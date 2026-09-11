"""
Stores the Error/Action/RootCause entities Cognee produced into HydraDB, so
whoever builds the "what should we fix" half of this project can query them
back out with client.query(...) instead of re-reading raw session logs.

Each entity becomes one HydraDB "memory" (type="memory"), tagged with
additional_metadata so they can be filtered by session / entity kind later.
"""
from __future__ import annotations

import json
import logging
import os
import time
from functools import lru_cache

from hydra_db import HydraDB
from hydra_db.errors.not_found_error import NotFoundError

from .cognee_pipeline import SessionEntities

log = logging.getLogger("capture-memory")

HYDRA_DATABASE = os.environ.get("HYDRA_DATABASE", "capture-memory")

# HydraDB indexes freshly-ingested memory sources asynchronously; calling
# update_source_metadata immediately after ingest() can 404 until indexing
# catches up, so tagging retries with backoff instead of assuming readiness.
_METADATA_TAG_ATTEMPTS = 8
_METADATA_TAG_RETRY_DELAY_S = 0.5


@lru_cache
def _client() -> HydraDB:
    api_key = os.environ["HYDRA_DB_API_KEY"]  # fail loudly if missing — no silent no-op storage
    return HydraDB(token=api_key)


def _ensure_database(client: HydraDB) -> None:
    existing = client.databases.list()
    names = getattr(existing.data, "tenant_ids", None) or getattr(existing.data, "ids", [])
    if HYDRA_DATABASE not in names:
        client.databases.create(database=HYDRA_DATABASE)
        # Provisioning is async on HydraDB's side; a hackathon demo can just
        # eat the first request's latency rather than polling databases.status().


def _memory_payload(session_id: str, kind: str, obj: dict) -> dict:
    return {
        "text": json.dumps(obj, default=str),
        # extra fields land in additional_metadata via document_metadata-style
        # tagging so later queries can filter by session/kind without a full scan
    }


def store_entities(session_id: str, entities: SessionEntities) -> dict:
    client = _client()
    _ensure_database(client)

    memories = []
    metadata = []
    for e in entities.errors:
        memories.append(_memory_payload(session_id, "error", vars(e)))
        metadata.append({"session_id": session_id, "kind": "error", "entity_id": e.id})
    for a in entities.actions:
        memories.append(_memory_payload(session_id, "action", vars(a)))
        metadata.append({"session_id": session_id, "kind": "action", "entity_id": a.id})
    for r in entities.root_causes:
        memories.append(_memory_payload(session_id, "root_cause", vars(r)))
        metadata.append({"session_id": session_id, "kind": "root_cause", "entity_id": r.error_id})

    if not memories:
        return {"stored": 0}

    resp = client.context.ingest(
        database=HYDRA_DATABASE,
        collection=session_id,
        memories=json.dumps(memories),
        type="memory",
    )

    # Best-effort per-item metadata tagging; skip silently if the SDK/API
    # shape doesn't line up — storage having happened is what matters for the demo.
    results = getattr(resp.data, "results", None) or []
    source_ids = [item.id for item in results if item.id]
    for source_id, meta in zip(source_ids, metadata):
        for attempt in range(1, _METADATA_TAG_ATTEMPTS + 1):
            try:
                client.context.update_source_metadata(
                    id=source_id,
                    database=HYDRA_DATABASE,
                    collection=session_id,
                    additional_metadata=meta,
                )
                break
            except NotFoundError:
                if attempt == _METADATA_TAG_ATTEMPTS:
                    log.warning("gave up tagging metadata for %s after %d attempts", source_id, attempt)
                    break
                time.sleep(_METADATA_TAG_RETRY_DELAY_S)
            except Exception:
                log.exception("failed to tag metadata for %s", source_id)
                break

    return {"stored": len(memories), "database": HYDRA_DATABASE, "collection": session_id}
