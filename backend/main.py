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
import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ingestion.cognee_pipeline import build_entities_for_session
from ingestion.hydra_store import store_entities

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("capture-memory")

SESSIONS_DIR = Path(__file__).parent / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

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
    # session ids come from the extension's own uuid-ish generator; still
    # strip path separators defensively before touching the filesystem.
    safe_id = "".join(c for c in session_id if c.isalnum() or c in "-_")
    return SESSIONS_DIR / f"{safe_id}.jsonl"


def _append_events(session_id: str, events: list[dict[str, Any]]) -> None:
    path = _session_path(session_id)
    with path.open("a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    _session_counts[session_id] = _session_counts.get(session_id, 0) + len(events)


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
            if not session_id or not events:
                continue
            _append_events(session_id, events)
    except WebSocketDisconnect:
        log.info("extension disconnected")


@app.get("/sessions")
def list_sessions() -> dict[str, int]:
    return _session_counts


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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

    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not events:
        raise HTTPException(status_code=400, detail="session log is empty")

    entities = build_entities_for_session(session_id, events)
    store_result = store_entities(session_id, entities)

    return {
        "session_id": session_id,
        "event_count": len(events),
        "entities": {
            "errors": len(entities.errors),
            "actions": len(entities.actions),
            "root_causes": len(entities.root_causes),
        },
        "hydra": store_result,
    }
