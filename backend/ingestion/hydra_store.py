"""HydraDB v2 persistence and retrieval for the Person 1 -> Person 2 handoff.

HydraDB's v2 names are ``database`` and ``collection``. The hackathon account
already provisions ``default-tenant``; all captured entities live in one
``capture-memory`` collection so Person 2 can query across sessions.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

from hydra_db import HydraDB

from .cognee_pipeline import SessionEntities

HYDRA_DATABASE = os.environ.get("HYDRA_DATABASE", "default-tenant")
HYDRA_COLLECTION = os.environ.get("HYDRA_COLLECTION", "capture-memory")


@lru_cache
def _client() -> HydraDB:
    # Prefer the shared Person 2 variable while retaining Person 1's legacy
    # name. Never include either value in responses or logs.
    api_key = os.environ.get("HYDRA_API_KEY") or os.environ.get("HYDRA_DB_API_KEY")
    if not api_key:
        raise RuntimeError("HYDRA_API_KEY is required")
    return HydraDB(token=api_key)


def _payload(response: Any) -> Any:
    """Unwrap Fern's runtime handler envelope when present.

    hydradb-sdk 2.1.4's generated high-level annotations and its runtime
    object disagree for some endpoints. The live API returns
    ``HandlerEnvelope*.data`` while test doubles and future SDKs may expose
    the inner payload directly.
    """
    data = getattr(response, "data", None)
    return data if data is not None else response


def _memory_payload(session_id: str, kind: str, entity_id: str, obj: dict[str, Any]) -> dict[str, Any]:
    return {
        "text": json.dumps(obj, default=str, sort_keys=True),
        "title": f"{kind}:{entity_id}",
        "infer": False,
        "additional_metadata": {
            "session_id": session_id,
            "kind": kind,
            "entity_id": entity_id,
            "schema_version": "1",
        },
    }


def store_entities(session_id: str, entities: SessionEntities) -> dict[str, Any]:
    """Queue one HydraDB memory per Cognee-derived entity.

    Per-item errors are checked before an ingestion receipt is returned.
    """
    memories: list[dict[str, Any]] = []
    memories.extend(
        _memory_payload(session_id, "error", entity.id, vars(entity))
        for entity in entities.errors
    )
    memories.extend(
        _memory_payload(session_id, "action", entity.id, vars(entity))
        for entity in entities.actions
    )
    memories.extend(
        _memory_payload(session_id, "root_cause", entity.error_id, vars(entity))
        for entity in entities.root_causes
    )

    if not memories:
        return {
            "stored": 0,
            "processing_status": "empty",
            "database": HYDRA_DATABASE,
            "collection": HYDRA_COLLECTION,
            "source_ids": [],
        }

    response = _client().context.ingest(
        database=HYDRA_DATABASE,
        collection=HYDRA_COLLECTION,
        memories=json.dumps(memories),
        type="memory",
        upsert="true",
    )
    results = list(getattr(_payload(response), "results", None) or [])
    failures = [item for item in results if getattr(item, "error", None)]
    if failures:
        raise RuntimeError(f"HydraDB rejected {len(failures)} entity item(s)")

    source_ids = [str(item.id) for item in results if getattr(item, "id", None)]
    return {
        "stored": len(source_ids),
        "queued": len(memories),
        "processing_status": "queued",
        "database": HYDRA_DATABASE,
        "collection": HYDRA_COLLECTION,
        "source_ids": source_ids,
    }


def recall_entities(
    query: str,
    *,
    session_id: str | None = None,
    kinds: list[str] | None = None,
    max_results: int = 10,
) -> dict[str, Any]:
    """Retrieve bounded, structured incident context via HydraDB v2 ``/query``."""
    additional_metadata: dict[str, Any] = {"schema_version": "1"}
    if session_id:
        additional_metadata["session_id"] = session_id
    if kinds:
        additional_metadata["kind"] = kinds

    response = _client().query(
        query=query,
        database=HYDRA_DATABASE,
        collection=HYDRA_COLLECTION,
        type="memory",
        query_by="hybrid",
        mode="fast",
        graph_context=True,
        max_results=max_results,
        metadata_filters={"additional_metadata": additional_metadata},
    )

    result = _payload(response)
    chunks = []
    for rank, chunk in enumerate(getattr(result, "chunks", None) or [], start=1):
        content = getattr(chunk, "chunk_content", None) or ""
        try:
            entity = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            entity = {"text": str(content)[:1000]}
        chunks.append({
            "rank": rank,
            "source_id": getattr(chunk, "id", None),
            "score": getattr(chunk, "relevancy_score", None),
            "collection": getattr(chunk, "collection", None),
            "metadata": getattr(chunk, "additional_metadata", None) or {},
            "entity": entity,
        })

    return {
        "database": HYDRA_DATABASE,
        "collection": HYDRA_COLLECTION,
        "count": len(chunks),
        "chunks": chunks,
        "graph_context_present": getattr(result, "graph_context", None) is not None,
    }
