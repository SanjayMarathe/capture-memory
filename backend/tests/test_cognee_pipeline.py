from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch


pipeline = importlib.import_module("ingestion.cognee_pipeline")


class HostedCogneeTests(unittest.IsolatedAsyncioTestCase):
    async def test_hosted_add_cognify_and_search_are_used(self):
        calls: list[tuple[str, dict]] = []

        def fake_request(path: str, payload: dict):
            calls.append((path, payload))
            if path == "search":
                return {"results": [{"text": "Checkout click preceded the failure."}]}
            return {"pipeline_run_id": "run-1"}

        error = pipeline.ErrorEntity(
            id="session-1-e2",
            kind="console_error",
            message="undefined receipt",
            url="https://demo.test/checkout",
            t=2,
            context_before=[{"kind": "click", "target": {"id": "place-order"}, "t": 1}],
        )
        environment = {
            "COGNEE_API_URL": "https://cognee.example/api/v1",
            "COGNEE_API_KEY": "secret",
            "COGNEE_TENANT_ID": "tenant",
        }
        with patch.dict(os.environ, environment, clear=True), patch.object(
            pipeline, "_hosted_request", side_effect=fake_request
        ):
            roots = await pipeline._cognify_and_infer_root_causes("session-1", "timeline", [error])

        self.assertEqual([call[0] for call in calls], ["add_text", "cognify", "search"])
        self.assertEqual(calls[2][1]["searchType"], "GRAPH_COMPLETION")
        self.assertEqual(calls[2][1]["datasets"], ["session_session-1"])
        self.assertIn("preceded", roots[0].explanation)

    def test_api_v1_suffix_is_normalized_once(self):
        with patch.dict(os.environ, {
            "COGNEE_API_URL": "https://cognee.example/api/v1/",
            "COGNEE_API_KEY": "secret",
            "COGNEE_TENANT_ID": "tenant",
        }, clear=True):
            self.assertEqual(pipeline._hosted_cognee_config()[0], "https://cognee.example")


if __name__ == "__main__":
    unittest.main()
