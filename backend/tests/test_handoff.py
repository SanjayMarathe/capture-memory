from __future__ import annotations

import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def _import_main():
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    sys.modules.setdefault("dotenv", dotenv)

    cognee_pipeline = types.ModuleType("ingestion.cognee_pipeline")
    cognee_pipeline.build_entities_for_session = lambda session_id, events: SimpleNamespace(
        errors=[SimpleNamespace(id=f"{session_id}-e1")],
        actions=[SimpleNamespace(id=f"{session_id}-a1")],
        root_causes=[SimpleNamespace(error_id=f"{session_id}-e1")],
    )
    hydra_store = types.ModuleType("ingestion.hydra_store")
    hydra_store.store_entities = lambda session_id, entities: {
        "stored": 3,
        "database": "default-tenant",
        "collection": "capture-memory",
        "source_ids": ["one", "two", "three"],
    }
    hydra_store.recall_entities = lambda *args, **kwargs: {"count": 0, "chunks": []}
    sys.modules["ingestion.cognee_pipeline"] = cognee_pipeline
    sys.modules["ingestion.hydra_store"] = hydra_store
    sys.modules.pop("main", None)
    return importlib.import_module("main")


class HandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = _import_main()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.sessions = root / "sessions"
        self.receipts = root / "receipts"
        self.sessions.mkdir()
        self.receipts.mkdir()
        self.sessions_patch = patch.object(self.main, "SESSIONS_DIR", self.sessions)
        self.receipts_patch = patch.object(self.main, "RECEIPTS_DIR", self.receipts)
        self.sessions_patch.start()
        self.receipts_patch.start()

    def tearDown(self):
        self.sessions_patch.stop()
        self.receipts_patch.stop()
        self.temporary.cleanup()

    def test_session_handoff_is_bounded_and_redacted(self):
        events = [
            {
                "kind": "keystroke",
                "t": 1,
                "url": "https://demo.test/form?email=person@example.com#private",
                "target": {"tag": "input", "name": "email"},
                "keyCategory": "char",
                "key": "x",
            },
            {
                "kind": "console_error",
                "t": 2,
                "url": "https://demo.test/form?token=abc",
                "message": "failed for person@example.com password=hunter2",
                "stack": "must not leave the backend",
            },
        ]
        path = self.sessions / "safe-session.jsonl"
        path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")

        result = self.main.get_session("safe-session", limit=1)

        self.assertTrue(result["truncated"])
        self.assertEqual(result["event_count"], 2)
        self.assertNotIn("?", result["events"][0]["url"])
        self.assertNotIn("person@example.com", result["events"][0]["message"])
        self.assertNotIn("hunter2", result["events"][0]["message"])
        self.assertNotIn("stack", result["events"][0])

    def test_ingest_reuses_receipt_for_unchanged_log(self):
        path = self.sessions / "repeatable.jsonl"
        path.write_text(json.dumps({"kind": "click", "t": 1}), encoding="utf-8")

        first = self.main.ingest_session("repeatable")
        second = self.main.ingest_session("repeatable")

        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(first["session_fingerprint"], second["session_fingerprint"])

    def test_invalid_session_id_is_rejected(self):
        with self.assertRaises(self.main.HTTPException) as raised:
            self.main.get_session("../escape", limit=10)
        self.assertEqual(raised.exception.status_code, 400)

    def test_append_persists_only_privacy_safe_event_fields(self):
        self.main._append_events("captured", [{
            "kind": "keystroke",
            "t": 1,
            "url": "https://demo.test/?token=secret",
            "target": {"tag": "input", "name": "email"},
            "keyCategory": "char",
            "key": "sensitive-character",
            "body": "must-not-persist",
        }])

        stored = json.loads((self.sessions / "captured.jsonl").read_text(encoding="utf-8"))
        self.assertNotIn("key", stored)
        self.assertNotIn("body", stored)
        self.assertEqual(stored["keyCategory"], "char")
        self.assertNotIn("?", stored["url"])


if __name__ == "__main__":
    unittest.main()
