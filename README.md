# Capture + Memory

Person 1's half: get real browser session data in, and structure it into a
knowledge graph another teammate's agent can reason over.

```
extension/          Chrome MV3 extension — captures + streams events
backend/main.py      WebSocket ingest endpoint + session buffering
backend/ingestion/    Cognee (structuring) + HydraDB (storage) pipeline
```

## How data flows

1. **`extension/capture.js`** runs in every page. It hooks `console.error`,
   `window.onerror`, `unhandledrejection`, `fetch`/`XMLHttpRequest`, and
   `click`/`keydown`, and batches everything into small JSON events every
   1.5s (or every 50 events).
2. **`extension/background.js`** is the only thing holding a WebSocket to the
   backend. It queues batches in `chrome.storage.session` if the socket is
   down, so a service-worker restart doesn't lose events.
3. **`backend/main.py`** accepts the socket at `/ws/ingest`, appends each
   batch to `backend/sessions/<session_id>.jsonl`, and exposes
   `POST /sessions/{id}/ingest` to run the structuring step on demand (call
   this when a session ends, or on a timer/webhook — that wiring is a
   5-minute add depending on how the rest of the team wants to trigger it).
4. **`backend/ingestion/cognee_pipeline.py`** deterministically splits a
   session's events into `Error` and `Action` entities (this part doesn't
   need an LLM — the events are already structured), renders the session as
   an ordered narrative, and hands that to Cognee's `add()` + `cognify()` to
   build a knowledge graph. It then asks Cognee's `GRAPH_COMPLETION` search,
   per error, "what action most likely caused this" to produce `RootCause`
   entities.
5. **`backend/ingestion/hydra_store.py`** pushes all three entity types into
   HydraDB as `memory`-type records, scoped to
   `database="capture-memory"`, `collection=<session_id>`, so they're
   queryable later via `client.query(..., type="memory")` without re-reading
   raw logs.

## Privacy design choice worth flagging to the team

Keystrokes are captured as **category only** (`char`, `Enter`, `Backspace`,
etc.), never the actual character typed. This is enough to reconstruct "user
typed into the search box, then hit Enter, then got a 500" without the
extension ever recording passwords or personal data — worth keeping even
under hackathon time pressure since it's the difference between a demo and a
liability.

## Running it

```bash
# backend
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in LLM_API_KEY (for cognee) and HYDRA_DB_API_KEY
uvicorn main:app --reload --port 8000
```

```bash
# extension
chrome://extensions -> Developer mode -> Load unpacked -> select extension/
```

Browse around with the extension loaded, trigger an error or two, then:

```bash
curl -X POST http://localhost:8000/sessions/<session_id>/ingest
```

(session ids show up under `GET /sessions`, or in the extension's
`sessionStorage.__capture_memory_session_id` per tab).

## Known gaps / handoff notes for whoever's building the reasoning half

- `background.js` points at `ws://localhost:8000/ws/ingest` — swap for the
  deployed backend URL before the demo.
- Cognee's `cognify()` needs an LLM provider key set per
  [their docs](https://docs.cognee.ai/setup-configuration/llm-providers);
  without it, root-cause inference degrades to "no graph context found"
  rather than crashing.
- `hydra_store.py` assumes the `capture-memory` HydraDB database already
  exists or can be auto-created; database creation is async on HydraDB's
  side, so the very first ingest after creating it may need a retry.
- Ingestion currently runs synchronously when `/sessions/{id}/ingest` is
  hit. Fine for a hackathon demo; for anything longer-running, move it to a
  background task.
