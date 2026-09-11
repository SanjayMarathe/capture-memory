from __future__ import annotations

import importlib
import json
import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch


class FakeContext:
    def __init__(self):
        self.ingest_kwargs = None

    def ingest(self, **kwargs):
        self.ingest_kwargs = kwargs
        items = json.loads(kwargs["memories"])
        return SimpleNamespace(data=SimpleNamespace(
            results=[SimpleNamespace(id=f"src-{index}", error=None) for index, _ in enumerate(items)]
        ))


class FakeHydraDB:
    last = None

    def __init__(self, *, token):
        self.token = token
        self.context = FakeContext()
        self.query_kwargs = None
        FakeHydraDB.last = self

    def query(self, **kwargs):
        self.query_kwargs = kwargs
        return SimpleNamespace(data=SimpleNamespace(
            chunks=[SimpleNamespace(
                chunk_content='{"kind":"console_error"}', id="src-0",
                relevancy_score=0.91, collection="capture-memory",
                additional_metadata={"kind": "error"},
            )],
            graph_context=SimpleNamespace(),
        ))


def _import_store():
    hydra_module = types.ModuleType("hydra_db")
    hydra_module.HydraDB = FakeHydraDB
    sys.modules["hydra_db"] = hydra_module
    pipeline_module = types.ModuleType("ingestion.cognee_pipeline")
    pipeline_module.SessionEntities = object
    sys.modules["ingestion.cognee_pipeline"] = pipeline_module
    sys.modules.pop("ingestion.hydra_store", None)
    return importlib.import_module("ingestion.hydra_store")


class HydraStoreTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {"HYDRA_API_KEY": "test-key"}, clear=True)
        self.environment.start()
        self.store = _import_store()
        self.store._client.cache_clear()

    def tearDown(self):
        self.store._client.cache_clear()
        self.environment.stop()

    def test_v2_ingest_uses_default_database_and_atomic_metadata(self):
        entities = SimpleNamespace(
            errors=[SimpleNamespace(id="e1", message="boom")],
            actions=[SimpleNamespace(id="a1", kind="click")],
            root_causes=[SimpleNamespace(error_id="e1", explanation="after click")],
        )
        result = self.store.store_entities("session-1", entities)

        kwargs = FakeHydraDB.last.context.ingest_kwargs
        memories = json.loads(kwargs["memories"])
        self.assertEqual(kwargs["database"], "default-tenant")
        self.assertEqual(kwargs["collection"], "capture-memory")
        self.assertEqual(memories[0]["additional_metadata"]["session_id"], "session-1")
        self.assertEqual(result["stored"], 3)

    def test_v2_query_is_scoped_and_bounded(self):
        result = self.store.recall_entities(
            "undefined receipt after checkout", session_id="session-1", kinds=["error"], max_results=5
        )

        kwargs = FakeHydraDB.last.query_kwargs
        self.assertEqual(kwargs["database"], "default-tenant")
        self.assertEqual(kwargs["query_by"], "hybrid")
        self.assertEqual(kwargs["max_results"], 5)
        self.assertEqual(kwargs["metadata_filters"]["additional_metadata"]["session_id"], "session-1")
        self.assertEqual(result["chunks"][0]["entity"]["kind"], "console_error")


if __name__ == "__main__":
    unittest.main()
