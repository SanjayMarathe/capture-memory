"""
Capture + Memory backend.

Receives event batches streamed by the Chrome extension over a WebSocket,
appends them to a per-session JSONL log, and exposes an endpoint to kick off
the Cognee -> HydraDB ingestion pipeline for a finished session.

Run:
    uvicorn main:app --reload --port 8000
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import json
import hashlib
import logging
from pathlib import Path
import re
from threading import Lock
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from ingestion.cognee_pipeline import build_entities_for_session
from ingestion.hydra_store import recall_entities, store_entities

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("capture-memory")

SESSIONS_DIR = Path(__file__).parent / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)
RECEIPTS_DIR = Path(__file__).parent / "receipts"
RECEIPTS_DIR.mkdir(exist_ok=True)

_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]+=*", re.IGNORECASE)
_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|authorization|password|secret)\b\s*[:=]\s*[^\s,;]+"
)
_TARGET_FIELDS = ("tag", "id", "name", "type", "role", "testId", "label")
_EVENT_KINDS = {
    "click", "keystroke", "console_error", "runtime_error", "unhandled_rejection", "network_failure"
}
_ingest_lock = Lock()


class MemoryRecallRequest(BaseModel):
    query: str = Field(min_length=1, max_length=1000)
    session_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,128}$")
    kinds: list[str] = Field(default_factory=list, max_length=3)
    max_results: int = Field(default=10, ge=1, le=25)

app = FastAPI(title="Capture + Memory backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # extension origins are chrome-extension://<id>; loosen for demo
    allow_methods=["*"],
    allow_headers=["*"],
)

# session_id -> live event count, kept in memory just for the /sessions listing
_session_counts: dict[str, int] = {}


def _session_path(session_id: str) -> Path:
    if not _SESSION_ID_RE.fullmatch(session_id):
        raise HTTPException(status_code=400, detail="invalid session id")
    return SESSIONS_DIR / f"{session_id}.jsonl"


def _receipt_path(session_id: str) -> Path:
    if not _SESSION_ID_RE.fullmatch(session_id):
        raise HTTPException(status_code=400, detail="invalid session id")
    return RECEIPTS_DIR / f"{session_id}.json"


def _redact_text(value: Any, limit: int = 500) -> str:
    text = str(value or "")[:limit]
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = _BEARER_RE.sub("Bearer [redacted]", text)
    return _SECRET_RE.sub(lambda match: f"{match.group(1)}=[redacted]", text)


def _safe_url(value: Any) -> str:
    """Keep origin/path for diagnosis while dropping query strings/fragments."""
    try:
        parts = urlsplit(str(value or "")[:2048])
        return urlunsplit((parts.scheme, parts.netloc, parts.path[:768], "", ""))
    except ValueError:
        return ""


def _safe_target(value: Any) -> dict[str, str | None] | None:
    if not isinstance(value, dict):
        return None
    return {
        field: (_redact_text(value[field], 120) if value.get(field) is not None else None)
        for field in _TARGET_FIELDS
    }


def _privacy_safe_event(event: dict[str, Any]) -> dict[str, Any]:
    """Return only the capture fields Person 2 needs; never return key content."""
    kind = _redact_text(event.get("kind"), 40)
    safe: dict[str, Any] = {
        "kind": kind,
        "t": int(event.get("t", 0)) if str(event.get("t", 0)).isdigit() else 0,
        "url": _safe_url(event.get("url")),
    }
    if kind in {"click", "keystroke"}:
        safe["target"] = _safe_target(event.get("target"))
    if kind == "keystroke":
        safe["keyCategory"] = event.get("keyCategory") if event.get("keyCategory") in {
            "char", "Enter", "Tab", "Escape", "Backspace", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "other"
        } else "other"
        modifiers = event.get("modifiers") if isinstance(event.get("modifiers"), dict) else {}
        safe["modifiers"] = {key: bool(modifiers.get(key)) for key in ("ctrl", "meta", "alt", "shift")}
    elif kind in {"console_error", "runtime_error", "unhandled_rejection"}:
        safe["message"] = _redact_text(event.get("message"), 500)
        if event.get("source"):
            safe["source"] = _safe_url(event.get("source"))
        for field in ("line", "col"):
            if isinstance(event.get(field), int):
                safe[field] = event[field]
    elif kind == "network_failure":
        safe.update({
            "method": _redact_text(event.get("method"), 12),
            "requestUrl": _safe_url(event.get("requestUrl")),
            "status": int(event.get("status", 0)) if str(event.get("status", 0)).isdigit() else 0,
            "statusText": _redact_text(event.get("statusText"), 160),
            "durationMs": max(0, int(event.get("durationMs", 0))) if str(event.get("durationMs", 0)).isdigit() else 0,
        })
    return safe


def _load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            log.warning("ignored malformed event line in %s", path.name)
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def _append_events(session_id: str, events: list[dict[str, Any]]) -> None:
    path = _session_path(session_id)
    safe_events = [
        _privacy_safe_event(event)
        for event in events[:100]
        if isinstance(event, dict) and event.get("kind") in _EVENT_KINDS
    ]
    if not safe_events:
        return
    with path.open("a", encoding="utf-8") as f:
        for event in safe_events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")
    _session_counts[session_id] = _session_counts.get(session_id, 0) + len(safe_events)


@app.websocket("/ws/ingest")
async def ws_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    log.info("extension connected")
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                batch = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("dropped malformed batch")
                continue
            session_id = batch.get("sessionId")
            events = batch.get("events", [])
            if (
                not isinstance(session_id, str)
                or not _SESSION_ID_RE.fullmatch(session_id)
                or not isinstance(events, list)
                or not events
            ):
                log.warning("dropped invalid capture batch")
                continue
            _append_events(session_id, events)
    except WebSocketDisconnect:
        log.info("extension disconnected")


@app.get("/sessions")
def list_sessions() -> dict[str, int]:
    # Reconstruct counts from the durable JSONL files after a backend restart;
    # the in-memory counters are only a fast path while capture is active.
    counts = dict(_session_counts)
    for path in SESSIONS_DIR.glob("*.jsonl"):
        if _SESSION_ID_RE.fullmatch(path.stem) and path.stem not in counts:
            counts[path.stem] = len(_load_events(path))
    return counts


@app.get("/sessions/{session_id}")
def get_session(
    session_id: str,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    """Person 1 -> Person 2 handoff. Query parameters and key content are removed."""
    path = _session_path(session_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"no session log for {session_id}")
    all_events = _load_events(path)
    events = [_privacy_safe_event(event) for event in all_events[-limit:]]
    terminal = [
        {
            "kind": event["kind"],
            "t": event["t"],
            "url": event["url"],
            "summary": event.get("message") or f"{event.get('method', '')} {event.get('status', '')}".strip(),
        }
        for event in events
        if event["kind"] in {"console_error", "runtime_error", "unhandled_rejection", "network_failure"}
    ]
    return {
        "session_id": session_id,
        "event_count": len(all_events),
        "events": events,
        "terminal_incidents": terminal,
        "truncated": len(all_events) > len(events),
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/memory/recall")
def recall_memory(request: MemoryRecallRequest) -> dict[str, Any]:
    """SDK-backed retrieval handoff; credentials remain server-side."""
    allowed_kinds = {"error", "action", "root_cause"}
    if not set(request.kinds).issubset(allowed_kinds):
        raise HTTPException(status_code=400, detail="invalid entity kind")
    return recall_entities(
        request.query,
        session_id=request.session_id,
        kinds=request.kinds or None,
        max_results=request.max_results,
    )


@app.post("/sessions/{session_id}/ingest")
def ingest_session(session_id: str) -> dict[str, Any]:
    """
    Runs the second half of this project's job for a captured session:
      1. Load the raw JSONL event log.
      2. Cognee: cognify() the session into Error / Action / RootCause entities.
      3. HydraDB: store the resulting entities + graph relations for retrieval.
    """
    path = _session_path(session_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"no session log for {session_id}")

    raw = path.read_bytes()
    fingerprint = hashlib.sha256(raw).hexdigest()
    receipt_path = _receipt_path(session_id)

    # FastAPI runs sync endpoints in a worker pool. Serialize this small demo
    # critical section so concurrent patch clicks cannot double-ingest a log.
    with _ingest_lock:
        if receipt_path.exists():
            try:
                previous = json.loads(receipt_path.read_text(encoding="utf-8"))
                if previous.get("session_fingerprint") == fingerprint:
                    previous["idempotent_replay"] = True
                    return previous
            except (json.JSONDecodeError, OSError):
                log.warning("ignored unreadable ingest receipt for %s", session_id)

        events = _load_events(path)
        if not events:
            raise HTTPException(status_code=400, detail="session log is empty")

        entities = build_entities_for_session(session_id, events)
        store_result = store_entities(session_id, entities)
        result = {
            "session_id": session_id,
            "session_fingerprint": fingerprint,
            "event_count": len(events),
            "entities": {
                "errors": len(entities.errors),
                "actions": len(entities.actions),
                "root_causes": len(entities.root_causes),
            },
            "hydra": store_result,
            "idempotent_replay": False,
        }
        temporary = receipt_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        temporary.replace(receipt_path)
        return result
