"""
Turns a raw captured session (console errors, network failures, click/keystroke
sequence) into three entity types Cognee's knowledge graph can reason over:

  Error       — a console error, runtime exception, or failed network call
  Action      — a click or keystroke that happened in the session
  RootCause   — Cognee's best guess, via graph-completion search, at which
                Action(s) most plausibly led to a given Error

Design note: entity *extraction* (Error/Action) is done deterministically in
this file, not left to Cognee's LLM extraction — the events are already
structured, so re-deriving them via free-text NER would just add noise and
cost. Cognee's job is the part that's actually hard to hand-roll: correlating
an error with the action sequence that plausibly caused it, using the
knowledge graph it builds from the session narrative.
"""
from __future__ import annotations

import asyncio
import json
import os
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import certifi

# --- deterministic entity shapes -------------------------------------------------


@dataclass
class ErrorEntity:
    id: str
    kind: str  # console_error | runtime_error | unhandled_rejection | network_failure
    message: str
    url: str
    t: int
    context_before: list[dict] = field(default_factory=list)  # last N events before it


@dataclass
class ActionEntity:
    id: str
    kind: str  # click | keystroke
    target: dict | None
    url: str
    t: int


@dataclass
class RootCauseEntity:
    error_id: str
    explanation: str
    candidate_action_ids: list[str]


@dataclass
class SessionEntities:
    session_id: str
    errors: list[ErrorEntity]
    actions: list[ActionEntity]
    root_causes: list[RootCauseEntity]


ERROR_KINDS = {"console_error", "runtime_error", "unhandled_rejection", "network_failure"}
ACTION_KINDS = {"click", "keystroke"}
CONTEXT_WINDOW = 5  # actions immediately preceding an error, used as candidates


def _hosted_cognee_config() -> tuple[str, str, str] | None:
    base_url = os.environ.get("COGNEE_API_URL", "").rstrip("/")
    api_key = os.environ.get("COGNEE_API_KEY", "")
    tenant_id = os.environ.get("COGNEE_TENANT_ID", "")
    if not (base_url and api_key and tenant_id):
        return None
    if base_url.endswith("/api/v1"):
        base_url = base_url[: -len("/api/v1")]
    return base_url, api_key, tenant_id


def _hosted_request(path: str, payload: dict) -> dict | list:
    config = _hosted_cognee_config()
    if config is None:
        raise RuntimeError("hosted Cognee configuration is incomplete")
    base_url, api_key, tenant_id = config
    request = Request(
        f"{base_url}/api/v1/{path.lstrip('/')}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Api-Key": api_key,
            "X-Tenant-Id": tenant_id,
        },
        method="POST",
    )
    try:
        # macOS framework Python does not always inherit the system trust
        # store. Use certifi's maintained CA bundle; never disable TLS checks.
        tls_context = ssl.create_default_context(cafile=certifi.where())
        with urlopen(request, timeout=45, context=tls_context) as response:
            body = response.read(2 * 1024 * 1024)
    except HTTPError as exc:
        # Do not include response bodies: hosted errors can echo source text.
        raise RuntimeError(f"Cognee {path} returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"Cognee {path} was unavailable") from exc
    if not body:
        return {}
    value = json.loads(body)
    if not isinstance(value, (dict, list)):
        raise RuntimeError(f"Cognee {path} returned an invalid response")
    return value


def _result_text(value: dict | list) -> str:
    """Extract a displayable answer without depending on one hosted response version."""
    queue: list[object] = [value]
    while queue:
        current = queue.pop(0)
        if isinstance(current, str) and current.strip():
            return current[:2000]
        if isinstance(current, list):
            queue.extend(current[:20])
        elif isinstance(current, dict):
            for key in ("text", "answer", "result", "results", "data", "search_result"):
                if key in current:
                    queue.append(current[key])
    return "No graph context found for this error."


def _extract_deterministic_entities(session_id: str, events: list[dict]) -> tuple[list[ErrorEntity], list[ActionEntity]]:
    events = sorted(events, key=lambda e: e.get("t", 0))
    actions: list[ActionEntity] = []
    errors: list[ErrorEntity] = []

    for i, e in enumerate(events):
        kind = e.get("kind")
        if kind in ACTION_KINDS:
            actions.append(
                ActionEntity(
                    id=f"{session_id}-a{i}",
                    kind=kind,
                    target=e.get("target"),
                    url=e.get("url", ""),
                    t=e.get("t", 0),
                )
            )
        elif kind in ERROR_KINDS:
            preceding_actions = [
                {"kind": a.kind, "target": a.target, "t": a.t}
                for a in actions[-CONTEXT_WINDOW:]
            ]
            message = e.get("message") or f"{e.get('method', '')} {e.get('url', '')} -> {e.get('status')}".strip()
            errors.append(
                ErrorEntity(
                    id=f"{session_id}-e{i}",
                    kind=kind,
                    message=message[:500],
                    url=e.get("url", ""),
                    t=e.get("t", 0),
                    context_before=preceding_actions,
                )
            )
    return errors, actions


def _session_narrative(session_id: str, errors: list[ErrorEntity], actions: list[ActionEntity]) -> str:
    """Render the session as ordered plain-language lines so cognee's graph
    extraction has clean subject/verb/object structure to work with."""
    lines = [f"Session {session_id} timeline:"]
    timeline = [("action", a) for a in actions] + [("error", e) for e in errors]
    timeline.sort(key=lambda pair: pair[1].t)
    for kind, item in timeline:
        if kind == "action":
            target = item.target or {}
            label = target.get("label") or target.get("name") or target.get("id") or target.get("tag")
            lines.append(f"- User performed {item.kind} on {label or 'an element'} at {item.url}.")
        else:
            lines.append(f"- A {item.kind} occurred: \"{item.message}\" at {item.url}.")
    return "\n".join(lines)


async def _cognify_and_infer_root_causes(
    session_id: str, narrative: str, errors: list[ErrorEntity]
) -> list[RootCauseEntity]:
    dataset_name = f"session_{session_id}"

    hosted = _hosted_cognee_config() is not None
    if hosted:
        _hosted_request("add_text", {
            "textData": [narrative],
            "datasetName": dataset_name,
            "nodeSet": [session_id, "capture-memory"],
        })
        _hosted_request("cognify", {
            "datasets": [dataset_name],
            # Hosted graph construction can take longer than the live patch
            # request. Starting the real Cognee job is the durable boundary;
            # deterministic extraction below keeps the demo pipeline moving.
            "runInBackground": True,
            "customPrompt": (
                "Extract observed Error, UIComponent, and UserAction entities. Preserve timeline order "
                "with PRECEDES relationships. Treat a proposed cause as a hypothesis, not a verified fix."
            ),
        })
    else:
        # Import lazily so a hosted-only deployment does not need local Cognee
        # configuration or an LLM_API_KEY merely to start the API process.
        import cognee

        await cognee.add(narrative, dataset_name=dataset_name)
        await cognee.cognify(datasets=[dataset_name])

    root_causes: list[RootCauseEntity] = []
    for err in errors:
        query = (
            f'In this session, what user action most likely caused this error: "{err.message}"? '
            "Answer with the specific preceding action and why."
        )
        try:
            if hosted:
                explanation = (
                    "Cognee graph construction accepted in the background; "
                    "the nearest preceding actions are retained as candidate causes."
                )
            else:
                from cognee import SearchType

                results = await cognee.search(
                    query_text=query,
                    query_type=SearchType.GRAPH_COMPLETION,
                    datasets=[dataset_name],
                )
                explanation = results[0] if results else "No graph context found for this error."
        except Exception as exc:  # cognee needs an LLM provider configured; degrade gracefully
            explanation = f"Root-cause inference unavailable ({exc}); falling back to nearest preceding actions."

        root_causes.append(
            RootCauseEntity(
                error_id=err.id,
                explanation=str(explanation),
                candidate_action_ids=[a["kind"] for a in err.context_before],
            )
        )
    return root_causes


def build_entities_for_session(session_id: str, events: list[dict]) -> SessionEntities:
    errors, actions = _extract_deterministic_entities(session_id, events)
    narrative = _session_narrative(session_id, errors, actions)
    root_causes = asyncio.run(_cognify_and_infer_root_causes(session_id, narrative, errors))
    return SessionEntities(session_id=session_id, errors=errors, actions=actions, root_causes=root_causes)
