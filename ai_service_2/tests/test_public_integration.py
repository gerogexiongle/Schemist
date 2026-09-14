"""HTTP contracts across the public UI, skills registry, MCP and query history."""
from collections import deque
import json
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as service
import feishu_bot_api
from skills import sql_pipeline


class PublicHttpIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(service.app)
        self.addCleanup(self.client.close)
        for context in (
            patch.dict(os.environ, {"MCP_ENABLED": "1", "MCP_API_TOKEN": ""}),
            patch.object(service, "_query_history", deque(maxlen=500)),
            patch.object(service, "_save_history"),
        ):
            context.start()
            self.addCleanup(context.stop)

    def rpc(self, method, params=None, headers=None):
        return self.client.post("/mcp", json={
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params or {},
        }, headers=headers or {})

    def test_mcp_initialization_and_tools(self):
        response = self.rpc("initialize", {"protocolVersion": "2025-11-25"})
        self.assertEqual(200, response.status_code)
        self.assertEqual("schemist", response.json()["result"]["serverInfo"]["name"])
        tools = self.rpc("tools/list").json()["result"]["tools"]
        self.assertEqual({"ask_data", "generate_sql", "execute_readonly_sql", "service_health"},
                         {tool["name"] for tool in tools})

    def test_mcp_token_and_disabled_endpoint(self):
        with patch.dict(os.environ, {"MCP_API_TOKEN": "local-test-token"}):
            self.assertEqual(401, self.rpc("ping").status_code)
            self.assertEqual(401, self.client.get("/mcp").status_code)
            self.assertEqual(401, self.client.delete("/mcp").status_code)
            response = self.rpc("ping", headers={"Authorization": "Bearer local-test-token"})
            self.assertEqual({}, response.json()["result"])
        with patch.dict(os.environ, {"MCP_ENABLED": "0"}):
            self.assertEqual(503, self.rpc("ping").status_code)

    def test_mcp_rejects_invalid_arguments_before_database_execution(self):
        with patch.object(service, "execute_sql") as execute:
            result = self.rpc("tools/call", {
                "name": "execute_readonly_sql", "arguments": {"sql": "SELECT 1"},
            }).json()["result"]
        self.assertTrue(result["isError"])
        execute.assert_not_called()

    def test_mcp_rejects_write_sql(self):
        with patch("skills.sql_executor.subprocess.Popen") as spark, \
                patch("skills.sql_executor._create_trino_connection") as trino:
            response = self.rpc("tools/call", {
                "name": "execute_readonly_sql",
                "arguments": {"question": "删除示例表", "sql": "DROP TABLE example.events"},
            }).json()["result"]
        self.assertTrue(response["isError"])
        spark.assert_not_called()
        trino.assert_not_called()

    def test_mcp_pipeline_preserves_sql_trace_history_and_result(self):
        generated = {"sql": "SELECT 1 AS value", "explanation": "constant", "tables_used": [],
                     "query_time": 0.01, "semantic_complete": True}
        executed = (True, ["value"], [{"value": 1}], None, 0.01, "test-query-id", {})
        with patch.object(sql_pipeline, "generate_sql", return_value=generated), \
                patch.object(sql_pipeline, "execute_sql", return_value=executed), \
                patch.object(sql_pipeline, "record_success_feedback_from_sql", return_value=None):
            response = self.rpc("tools/call", {
                "name": "ask_data", "arguments": {"question": "返回常数 1", "include_analysis": False},
            }).json()["result"]
        self.assertFalse(response["isError"])
        payload = json.loads(response["content"][0]["text"])
        self.assertTrue(payload["workflow"]["data_returned"])
        verification = payload["verification"]
        self.assertEqual("SELECT 1 AS value", verification["final_sql"])
        self.assertEqual("trino", verification["engine"])
        self.assertEqual("test-query-id", verification["query_id"])
        self.assertEqual(1, verification["row_count"])
        tid = verification["trace_id"]
        history = self.client.get("/api/query-history", params={"source": "mcp", "trace_id": tid}).json()
        self.assertEqual({"generate", "execute"}, {item["type"] for item in history["items"]})
        self.assertEqual(1, len({item["question_id"] for item in history["items"]}))
        trace = self.client.get("/api/pipeline-trace/" + tid).json()
        self.assertEqual("SELECT 1 AS value", trace["snapshot"]["sql"])
        self.assertGreater(trace["event_count"], 0)

    def test_public_skills_render_and_feishu_automatic_matching(self):
        skills = self.client.get("/api/skills").json()["items"]
        self.assertEqual(
            {"schema-exploration", "sql-rewrite", "data-insights"},
            {skill["id"] for skill in skills},
        )
        for skill in skills:
            with self.subTest(skill=skill["id"]):
                rendered = self.client.post("/api/skills/" + skill["id"] + "/render", json={
                    "user_query": "统计昨天的点击率", "engine": "trino", "today": "2026-09-14",
                })
                self.assertEqual(200, rendered.status_code)
                self.assertIn("统计昨天的点击率", rendered.text)
                self.assertNotIn("{{user_query}}", rendered.text)
        reg = service._get_skills_registry()
        examples = {
            "有哪些表和字段可以查询": "schema-exploration",
            "优化 SQL SELECT 1": "sql-rewrite",
        }
        for question, expected in examples.items():
            with self.subTest(question=question):
                picked = feishu_bot_api._pick_best_auto_skill_id(reg, question)
                self.assertIsNotNone(picked)
                self.assertEqual(expected, picked[0])

    def test_index_uses_schemist_brand_and_server_side_llm_proxy(self):
        with patch.object(service, "LLM_API_KEY", "private-key-must-stay-server-side"):
            response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        self.assertIn("<title>Schemist", response.text)
        self.assertIn("/api/page-agent-proxy/v1", response.text)
        self.assertNotIn("private-key-must-stay-server-side", response.text)
        self.assertIn("/api/query-history", response.text)
        self.assertIn("/api/run-pipeline", response.text)
