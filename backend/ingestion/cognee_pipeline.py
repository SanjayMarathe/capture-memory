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
from dataclasses import dataclass, field
from datetime import datetime, timezone

import cognee
from cognee import SearchType

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

    await cognee.add(narrative, dataset_name=dataset_name)
    await cognee.cognify(datasets=[dataset_name])

    root_causes: list[RootCauseEntity] = []
    for err in errors:
        query = (
            f'In this session, what user action most likely caused this error: "{err.message}"? '
            "Answer with the specific preceding action and why."
        )
        try:
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
