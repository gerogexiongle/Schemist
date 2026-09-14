#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import threading
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import patch

import app as app_module
import agents.base_agent as base_agent_module
import feishu_bot_api
from fastapi import Request
from fastapi.responses import Response
from agents.base_agent import DeepAgent, Skill
from agents import sql_agent
import mcp_api
from skills import schema_skill
from skills import sql_pipeline
from skills import sql_executor
from skills.sql_validator import (
    fix_trino_date_type_error,
    fix_cte_missing_columns,
    fix_order_by_before_set_operator,
    validate_engine_sql,
)


class _FakeMessage(object):
    def __init__(self, content="", tool_name=None, arguments=None, call_id="call-1"):
        self.content = content
        self.tool_calls = []
        if tool_name:
            self.tool_calls = [SimpleNamespace(
                id=call_id,
                function=SimpleNamespace(
                    name=tool_name,
                    arguments=json.dumps(arguments or {}),
                ),
            )]

    def model_dump(self):
        out = {"role": "assistant", "content": self.content}
        if self.tool_calls:
            out["tool_calls"] = [{
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                },
            } for tc in self.tool_calls]
        return out


class _FakeResponse(object):
    def __init__(self, message, finish_reason):
        self.choices = [SimpleNamespace(message=message, finish_reason=finish_reason)]


class _FakeCompletions(object):
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class DeepAgentReliabilityTests(unittest.TestCase):
    def test_finalization_applies_timeout_and_disables_sdk_retries(self):
        completions = _FakeCompletions([
            _FakeResponse(_FakeMessage(content='{"sql":"SELECT 1"}'), "stop"),
        ])

        class FakeClient(object):
            def __init__(self):
                self.chat = SimpleNamespace(completions=completions)
                self.options = []

            def with_options(self, **kwargs):
                self.options.append(kwargs)
                return self

        base_agent_module.LLM_API_URL = "http://127.0.0.1:1/v1/chat/completions"
        base_agent_module.LLM_API_KEY = "test"
        agent = DeepAgent(name="test")
        agent.client = FakeClient()
        agent.last_messages = [{"role": "user", "content": "question"}]

        result = agent.finalize_last_run(
            prompt="Return SQL",
            request_timeout=60,
            max_retries=0,
        )

        self.assertEqual('{"sql":"SELECT 1"}', result)
        self.assertEqual([{"timeout": 60, "max_retries": 0}], agent.client.options)

    def test_limits_duplicate_calls_and_forces_tool_free_finalization(self):
        executions = []
        responses = [
            _FakeResponse(_FakeMessage(tool_name="search_tables", arguments={"keyword": "daily"}, call_id="1"), "tool_calls"),
            _FakeResponse(_FakeMessage(tool_name="search_tables", arguments={"keyword": "daily"}, call_id="2"), "tool_calls"),
            _FakeResponse(_FakeMessage(tool_name="search_tables", arguments={"keyword": "revenue"}, call_id="3"), "tool_calls"),
            _FakeResponse(_FakeMessage(tool_name="search_tables", arguments={"keyword": "retention"}, call_id="4"), "tool_calls"),
            _FakeResponse(_FakeMessage(content='{"sql":"SELECT 1"}'), "stop"),
        ]
        completions = _FakeCompletions(responses)

        base_agent_module.LLM_API_URL = "http://127.0.0.1:1/v1/chat/completions"
        base_agent_module.LLM_API_KEY = "test"
        agent = DeepAgent(
            name="test",
            skills=[Skill("search_tables", "search", lambda **kw: executions.append(kw) or kw)],
            max_iterations=4,
            force_finalize_on_limit=True,
            finalization_prompt="Return final JSON",
            skill_call_limits={"search_tables": 2},
            max_total_skill_calls=2,
            block_duplicate_skill_calls=True,
        )
        agent.client = SimpleNamespace(chat=SimpleNamespace(completions=completions))

        result = agent.run("question")

        self.assertEqual('{"sql":"SELECT 1"}', result)
        self.assertEqual(2, len(executions))
        self.assertEqual(1, agent.last_run_info["duplicate_hits"])
        self.assertEqual(1, agent.last_run_info["limit_hits"])
        self.assertTrue(agent.last_run_info["forced_finalization"])
        self.assertEqual(5, len(completions.calls))
        self.assertIn("tools", completions.calls[3])
        self.assertNotIn("tools", completions.calls[4])
        self.assertNotIn("tool_choice", completions.calls[4])


class SqlAgentReliabilityTests(unittest.TestCase):
    def test_schema_fix_skips_alias_reused_by_cte(self):
        sql = """WITH payment_amount_agg AS (
  SELECT dt, SUM(payment_amount) AS total_payment_amount,
         COUNT(DISTINCT user_id) AS payment_amount_uv
  FROM example.payment_stats m
  GROUP BY dt
)
SELECT m.total_payment_amount, m.payment_amount_uv
FROM payment_amount_agg m"""
        with patch.object(
            schema_skill,
            "extract_tables_from_sql",
            return_value=["example.payment_stats"],
        ), patch.object(
            schema_skill,
            "get_table_columns",
            return_value={"dt", "user_id", "payment_amount"},
        ):
            fixed = schema_skill.fix_columns_by_schema(sql)

        self.assertIsNone(fixed)
        self.assertIn("m.total_payment_amount", sql)
        self.assertIn("m.payment_amount_uv", sql)

    def test_schema_fix_keeps_unambiguous_physical_alias_support(self):
        sql = "SELECT m.payment_amount_uv FROM example.payment_stats m"
        with patch.object(
            schema_skill,
            "extract_tables_from_sql",
            return_value=["example.payment_stats"],
        ), patch.object(
            schema_skill,
            "get_table_columns",
            return_value={"dt", "user_id", "payment_amount"},
        ):
            fixed = schema_skill.fix_columns_by_schema(sql)

        self.assertEqual(
            "SELECT m.payment_amount FROM example.payment_stats m",
            fixed,
        )

    def test_trino_date_type_error_is_fixed_deterministically(self):
        sql = (
            "SELECT * FROM retention_base r LEFT JOIN retention_next_day n "
            "ON r.user_id = n.user_id AND n.next_dt = "
            "DATE_ADD('day', 1, CAST(r.anchor_dt AS TIMESTAMP))"
        )
        error = "Cannot apply operator: varchar = timestamp(3)"

        fixed = fix_trino_date_type_error(sql, error)

        self.assertIn("CAST(n.next_dt AS DATE)", fixed)
        self.assertIn("DATE_ADD('day', 1, CAST(r.anchor_dt AS DATE))", fixed)
        self.assertNotIn("AS TIMESTAMP", fixed)

    def test_trino_date_add_varchar_argument_is_fixed_deterministically(self):
        sql = (
            "SELECT * FROM retention_base r LEFT JOIN retention_next_day n "
            "ON r.user_id = n.user_id AND n.next_dt = DATE_ADD('day', 1, r.anchor_dt)"
        )
        error = (
            "Unexpected parameters (varchar(3), integer, varchar) for function date_add"
        )

        fixed = fix_trino_date_type_error(sql, error)

        self.assertIn("CAST(n.next_dt AS DATE)", fixed)
        self.assertIn("DATE_ADD('day', 1, CAST(r.anchor_dt AS DATE))", fixed)

    def test_union_date_cte_with_later_join_is_not_a_select_list_error(self):
        sql = """WITH dates AS (
  SELECT '2026-09-03' AS dt UNION ALL
  SELECT '2026-09-04'
), daily AS (
  SELECT d.dt, COUNT(x.user_id) AS users
  FROM dates d
  LEFT JOIN db.events x ON x.dt = d.dt
  GROUP BY d.dt
)
SELECT * FROM daily"""
        issues = validate_engine_sql(sql, engine="trino")
        self.assertFalse(any("JOIN keyword appears" in item for item in issues))

    def test_actual_join_inside_select_list_is_still_rejected(self):
        sql = "SELECT a.user_id, LEFT JOIN db.b b ON a.user_id = b.user_id FROM db.a a"
        issues = validate_engine_sql(sql, engine="trino")
        self.assertTrue(any("JOIN keyword appears" in item for item in issues))

    def test_order_by_before_union_is_removed_and_revalidated(self):
        sql = "SELECT dt FROM daily_stats ORDER BY dt UNION ALL SELECT dt FROM total_stats"
        self.assertTrue(any("ORDER BY appears before" in item for item in validate_engine_sql(sql, "trino")))

        fixed = fix_order_by_before_set_operator(sql)

        self.assertEqual("SELECT dt FROM daily_stats\nUNION ALL SELECT dt FROM total_stats", fixed)
        self.assertFalse(any("ORDER BY appears before" in item for item in validate_engine_sql(fixed, "trino")))

    def test_nested_branch_order_by_is_not_changed(self):
        sql = (
            "(SELECT dt FROM daily_stats ORDER BY dt LIMIT 1) "
            "UNION ALL SELECT dt FROM total_stats ORDER BY dt"
        )
        self.assertIsNone(fix_order_by_before_set_operator(sql))

    def test_comment_prefixed_cte_does_not_duplicate_existing_columns(self):
        sql = """WITH base AS (
  -- source fields used by the outer query
  SELECT user_id, dt
  FROM example.events
)
SELECT b.user_id, b.dt FROM base b"""
        self.assertIsNone(fix_cte_missing_columns(sql))

    def test_comment_prefixed_cte_still_adds_a_truly_missing_column(self):
        sql = """WITH base AS (
  -- source fields
  SELECT user_id
  FROM example.events
)
SELECT b.user_id, b.dt FROM base b"""
        fixed = fix_cte_missing_columns(sql)
        self.assertIsNotNone(fixed)
        self.assertIn("dt", fixed)

    def test_relevant_catalog_is_capped(self):
        result = {
            "tables": [
                {"table": "db.table_{}".format(i), "comment": "table {}".format(i)}
                for i in range(30)
            ]
        }
        with patch.object(sql_agent, "skill_search_tables", return_value=result):
            catalog = sql_agent._build_relevant_table_catalog("daily users", limit=15)
        self.assertEqual(15, len(catalog.splitlines()))
        self.assertIn("db.table_14", catalog)
        self.assertNotIn("db.table_15", catalog)

    def test_postprocess_revalidates_after_auto_fix(self):
        with patch.object(sql_agent, "fix_columns_by_schema", return_value="SELECT 1"), \
                patch.object(sql_agent, "fix_cte_missing_columns", return_value=None), \
                patch.object(sql_agent, "validate_generated_sql", return_value=[]), \
                patch.object(sql_agent, "validate_engine_sql", return_value=[]) as validate_engine, \
                patch.object(sql_agent, "list_unknown_tables_in_sql", return_value=[]):
            result = sql_agent.postprocess_sql("SELECT old_column", engine="trino")
        self.assertEqual("SELECT 1", result["sql"])
        self.assertGreaterEqual(validate_engine.call_count, 2)
        self.assertEqual([], result["validation_issues"])

    def test_empty_cheap_output_uses_one_cheap_finalize_then_one_strong_fallback(self):
        class FakeAgent(object):
            def __init__(self):
                self.last_run_info = {"forced_finalization": False}
                self.models = []
                self.calls = []

            def run(self, **kwargs):
                return "No SQL yet"

            def finalize_last_run(self, prompt, model, **kwargs):
                self.models.append(model)
                self.calls.append(kwargs)
                if len(self.models) == 1:
                    return "Still no SQL"
                return '{"sql":"SELECT 1 AS value","explanation":"ok","tables_used":[]}'

        fake_agent = FakeAgent()
        with patch.object(sql_agent, "create_sql_agent", return_value=fake_agent), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "strong-model"):
            result = sql_agent.generate_sql("one value", llm_model="cheap-model")

        self.assertEqual(["cheap-model", "strong-model"], fake_agent.models)
        self.assertEqual("SELECT 1 AS value", result["sql"])
        self.assertTrue(result["fallback_used"])
        self.assertIn("cheap_finalization_failed", result["fallback_reason"])
        self.assertEqual(sql_agent.SQL_AGENT_FINAL_FALLBACK_TIMEOUT, fake_agent.calls[1]["request_timeout"])
        self.assertEqual(sql_agent.SQL_AGENT_FALLBACK_MAX_RETRIES, fake_agent.calls[1]["max_retries"])

    def test_selected_strong_model_skips_complex_sql_fallback(self):
        complex_sql = (
            "SELECT a.id FROM db.a a "
            "JOIN db.b b ON a.id = b.id "
            "JOIN db.c c ON b.id = c.id"
        )

        class FakeAgent(object):
            def __init__(self):
                self.last_run_info = {"forced_finalization": False}

            def run(self, **kwargs):
                return json.dumps({"sql": complex_sql, "explanation": "ok", "tables_used": []})

            def finalize_last_run(self, **kwargs):
                raise AssertionError("a selected strong model must not invoke another fallback")

        processed = {"sql": complex_sql, "explanation": "ok", "validation_issues": []}
        with patch.object(sql_agent, "create_sql_agent", return_value=FakeAgent()), \
                patch.object(sql_agent, "postprocess_sql", return_value=processed), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "fallback-model"), \
                patch.object(sql_agent, "SQL_AGENT_STRONG_MODELS", frozenset({"selected-strong-model"})):
            result = sql_agent.generate_sql("complex query", llm_model="selected-strong-model")

        self.assertIn("complex_multi_table_join", result["fallback_reason"])
        self.assertFalse(result["fallback_used"])

    def test_repair_keeps_selected_strong_model(self):
        constructor_options = []

        class FakeRepairAgent(object):
            def __init__(self, **kwargs):
                constructor_options.append(kwargs)

            def simple_chat(self, **kwargs):
                return '{"sql":"SELECT 1","explanation":"fixed","tables_used":[]}'

        processed = {"sql": "SELECT 1", "explanation": "fixed", "validation_issues": []}
        with patch.object(sql_agent, "DeepAgent", FakeRepairAgent), \
                patch.object(sql_agent, "_build_repair_schema_context", return_value=""), \
                patch.object(sql_agent, "postprocess_sql", return_value=processed), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "fallback-model"), \
                patch.object(sql_agent, "SQL_AGENT_STRONG_MODELS", frozenset({"selected-strong-model"})):
            result = sql_agent.repair_sql(
                original_question="question",
                failed_sql="SELECT bad",
                error_message="invalid column",
                engine="trino",
                llm_model="selected-strong-model",
                use_fallback_model=True,
            )

        options = constructor_options[0]
        self.assertEqual("selected-strong-model", options["model"])
        self.assertIsNone(options["request_timeout"])
        self.assertIsNone(options["max_retries"])
        self.assertFalse(result["fallback_used"])

    def test_repair_fallback_uses_short_independent_timeout(self):
        constructor_options = []

        class FakeRepairAgent(object):
            def __init__(self, **kwargs):
                constructor_options.append(kwargs)

            def simple_chat(self, **kwargs):
                return '{"sql":"SELECT 1","explanation":"fixed","tables_used":[]}'

        processed = {"sql": "SELECT 1", "explanation": "fixed", "validation_issues": []}
        with patch.object(sql_agent, "DeepAgent", FakeRepairAgent), \
                patch.object(sql_agent, "_build_repair_schema_context", return_value=""), \
                patch.object(sql_agent, "postprocess_sql", return_value=processed), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "strong-model"), \
                patch.object(sql_agent, "SQL_AGENT_STRONG_MODELS", frozenset({"strong-model"})):
            result = sql_agent.repair_sql(
                original_question="question",
                failed_sql="SELECT bad",
                error_message="invalid column",
                engine="trino",
                llm_model="cheap-model",
                use_fallback_model=True,
            )

        options = constructor_options[0]
        self.assertEqual(sql_agent.SQL_AGENT_REPAIR_FALLBACK_TIMEOUT, options["request_timeout"])
        self.assertEqual(sql_agent.SQL_AGENT_FALLBACK_MAX_RETRIES, options["max_retries"])
        self.assertTrue(result["fallback_used"])

    def test_valid_simple_sql_does_not_use_strong_fallback(self):
        class FakeAgent(object):
            def __init__(self):
                self.last_run_info = {"forced_finalization": False}

            def run(self, **kwargs):
                return '{"sql":"SELECT 1 AS value","explanation":"ok","tables_used":[]}'

            def finalize_last_run(self, **kwargs):
                raise AssertionError("simple valid SQL must not invoke fallback")

        with patch.object(sql_agent, "create_sql_agent", return_value=FakeAgent()), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "strong-model"):
            result = sql_agent.generate_sql("one value", llm_model="cheap-model")
        self.assertEqual("SELECT 1 AS value", result["sql"])
        self.assertFalse(result["fallback_used"])


class SqlPipelineFallbackTests(unittest.TestCase):
    def test_trino_date_error_uses_deterministic_repair_before_llm(self):
        failed_sql = (
            "SELECT * FROM retention_base r LEFT JOIN retention_next_day n "
            "ON r.user_id = n.user_id AND n.next_dt = DATE_ADD('day', 1, r.anchor_dt)"
        )
        execution_results = [
            (
                False,
                [],
                [],
                "Trino EXPLAIN validation failed: Unexpected parameters "
                "(varchar(3), integer, varchar) for function date_add",
                0.1,
                "query-1",
                {"error_kind": "trino_preflight"},
            ),
            (True, ["value"], [{"value": 1}], None, 0.2, "query-2", {}),
        ]
        with patch.object(sql_pipeline, "collect_pre_execution_issues", return_value=[]), \
                patch.object(sql_pipeline, "execute_sql", side_effect=execution_results), \
                patch.object(sql_pipeline, "repair_sql") as llm_repair, \
                patch.object(sql_pipeline, "_publish_pipeline_snapshot"):
            result = sql_pipeline.run_sql_pipeline(
                query="次留",
                engine="trino",
                pre_sql=failed_sql,
                max_retries=2,
            )

        self.assertTrue(result["success"])
        self.assertEqual(2, len(result["attempts"]))
        self.assertEqual(
            "deterministic_trino_date_type",
            result["attempts"][0]["repair_strategy"],
        )
        llm_repair.assert_not_called()

    def test_unchanged_repair_is_rejected_without_another_attempt(self):
        repairs = []

        def fake_repair(**kwargs):
            repairs.append(kwargs)
            return {"sql": "  SELECT   1; "}

        with patch.object(sql_pipeline, "collect_pre_execution_issues", return_value=["invalid"]), \
                patch.object(sql_pipeline, "repair_sql", side_effect=fake_repair), \
                patch.object(sql_pipeline, "_publish_pipeline_snapshot"):
            result = sql_pipeline.run_sql_pipeline(
                query="question",
                pre_sql="SELECT 1",
                max_retries=2,
            )

        self.assertEqual(1, len(repairs))
        self.assertFalse(result["success"])
        self.assertTrue(result["attempts"][0]["repair_no_change"])

    def test_second_failed_validation_repair_uses_fallback_model(self):
        fallback_flags = []

        def fake_repair(**kwargs):
            fallback_flags.append(kwargs.get("use_fallback_model"))
            return {"sql": "SELECT {}".format(len(fallback_flags) + 1)}

        with patch.object(sql_pipeline, "collect_pre_execution_issues", return_value=["invalid"]), \
                patch.object(sql_pipeline, "repair_sql", side_effect=fake_repair), \
                patch.object(sql_pipeline, "_publish_pipeline_snapshot"):
            result = sql_pipeline.run_sql_pipeline(
                query="question",
                pre_sql="SELECT 1",
                max_retries=2,
            )

        self.assertEqual([False, True], fallback_flags)
        self.assertFalse(result["success"])


class ApiExecutorOffloadTests(unittest.TestCase):
    def test_generate_sql_runs_in_worker_thread(self):
        caller_thread = threading.get_ident()
        worker_threads = []

        def fake_generate_sql(**kwargs):
            worker_threads.append(threading.get_ident())
            return {
                "sql": "SELECT 1",
                "explanation": "ok",
                "tables_used": [],
                "execution_plan": "",
                "query_time": 0.01,
            }

        request = app_module.SQLGenerationRequest(query="one value")
        with patch.object(app_module, "generate_sql", side_effect=fake_generate_sql), \
                patch.object(app_module, "record_query"):
            response = asyncio.run(app_module.api_generate_sql(request))

        self.assertEqual("SELECT 1", response.sql)
        self.assertEqual(1, len(worker_threads))
        self.assertNotEqual(caller_thread, worker_threads[0])

    def test_execute_sql_runs_in_worker_thread(self):
        caller_thread = threading.get_ident()
        worker_threads = []

        def fake_execute_sql(**kwargs):
            worker_threads.append(threading.get_ident())
            return True, ["value"], [{"value": 1}], None, 0.01, "query-1", {}

        request = app_module.SQLExecutionRequest(sql="SELECT 1", question="one value")
        with patch.object(app_module, "execute_sql", side_effect=fake_execute_sql), \
                patch.object(app_module, "record_query"), \
                patch.object(app_module, "record_success_feedback_from_sql", return_value=None), \
                patch.object(app_module, "put_query_result"):
            response = asyncio.run(app_module.api_execute_sql(request))

        self.assertTrue(response.success)
        self.assertEqual(1, response.row_count)
        self.assertEqual(1, len(worker_threads))
        self.assertNotEqual(caller_thread, worker_threads[0])


class TrinoPreflightTests(unittest.TestCase):
    def test_explain_validation_failure_prevents_query_execution(self):
        class FakeCursor(object):
            description = None

            def __init__(self):
                self.calls = []

            def execute(self, sql):
                self.calls.append(sql)
                raise RuntimeError("Cannot apply operator: varchar = timestamp(3)")

            def close(self):
                pass

        class FakeConnection(object):
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

            def close(self):
                pass

        cursor = FakeCursor()
        with patch.object(
            sql_executor,
            "_create_trino_connection",
            return_value=FakeConnection(cursor),
        ):
            result = sql_executor.execute_trino_sql("SELECT 1", max_rows=100)

        self.assertFalse(result[0])
        self.assertEqual(["EXPLAIN (TYPE VALIDATE) SELECT 1"], cursor.calls)
        self.assertIn("Trino EXPLAIN validation failed", result[3])
        self.assertEqual("trino_preflight", result[6]["error_kind"])

    def test_valid_query_executes_after_explain_validation(self):
        class FakeCursor(object):
            description = [("value",)]

            def __init__(self):
                self.calls = []

            def execute(self, sql):
                self.calls.append(sql)

            def fetchall(self):
                return [(True,)]

            def fetchmany(self, _limit):
                return [(1,)]

            def close(self):
                pass

        class FakeConnection(object):
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

            def close(self):
                pass

        cursor = FakeCursor()
        with patch.object(
            sql_executor,
            "_create_trino_connection",
            return_value=FakeConnection(cursor),
        ), patch.object(sql_executor, "open", create=True), \
                patch.object(sql_executor.os.path, "join", return_value="/tmp/result.csv"):
            result = sql_executor.execute_trino_sql("SELECT 1 AS value", max_rows=100)

        self.assertTrue(result[0])
        self.assertEqual(
            ["EXPLAIN (TYPE VALIDATE) SELECT 1 AS value", "SELECT 1 AS value"],
            cursor.calls,
        )
        self.assertEqual([{"value": 1}], result[2])


class McpWorkflowContractTests(unittest.TestCase):
    def test_default_engine_is_trino(self):
        self.assertEqual("trino", mcp_api._engine({}))
        engine_defaults = [
            tool["inputSchema"]["properties"]["engine"]["default"]
            for tool in mcp_api.TOOLS
            if "engine" in tool["inputSchema"].get("properties", {})
        ]
        self.assertEqual(["trino", "trino", "trino"], engine_defaults)

    def test_generate_sql_guidance_preserves_flexible_tool_chaining(self):
        payload = mcp_api._attach_verification(
            {"sql": "SELECT 1", "semantic_complete": True},
            "mcp_trace",
            "generate_sql",
            "查询一条数据",
            "trino",
            "SELECT 1",
        )

        self.assertFalse(payload["workflow"]["data_returned"])
        self.assertEqual(
            "execute_readonly_sql",
            payload["workflow"]["recommended_next_tool"],
        )
        self.assertTrue(payload["workflow"]["allow_agent_tool_choice"])


class QueryHistoryClientTests(unittest.TestCase):
    def test_mcp_request_context_records_client_ip_and_agent(self):
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/mcp",
            "raw_path": b"/mcp",
            "query_string": b"",
            "root_path": "",
            "headers": [(b"user-agent", b"codex-desktop/test")],
            "client": ("198.51.100.27", 43120),
            "server": ("127.0.0.1", 8889),
        }
        request = Request(scope)
        history = deque(maxlen=500)

        async def call_next(_request):
            app_module.record_query("generate", "trino", question="mcp question", source="mcp")
            return Response(status_code=200)

        with patch.object(app_module, "_query_history", history), \
                patch.object(app_module, "_save_history"):
            response = asyncio.run(app_module.pipeline_http_middleware(request, call_next))

        self.assertEqual(200, response.status_code)
        self.assertEqual("198.51.100.27", history[0]["client_ip"])
        self.assertEqual("codex-desktop/test", history[0]["client_agent"])

    def test_query_history_can_filter_by_client_ip(self):
        history = deque([
            {"id": "one", "client_ip": "198.51.100.27"},
            {"id": "two", "client_ip": "203.0.113.8"},
            {"id": "legacy"},
        ], maxlen=500)
        with patch.object(app_module, "_query_history", history):
            result = asyncio.run(app_module.api_query_history(
                limit=50,
                engine=None,
                query_type=None,
                source=None,
                trace_id=None,
                question_id=None,
                client_ip="198.51.100.27",
            ))

        self.assertEqual(1, result["total"])
        self.assertEqual("one", result["items"][0]["id"])

    def test_feishu_recorder_writes_shared_history_with_trace_id(self):
        history = deque(maxlen=500)
        with patch.object(app_module, "_query_history", history), \
                patch.object(app_module, "_save_history"):
            feishu_bot_api.set_query_recorder(app_module.record_query)
            recorded = feishu_bot_api._record_query_safely(
                query_type="execute",
                engine="spark",
                question="飞书查询最近7天点击率",
                sql="SELECT 1",
                success=True,
                duration=1.2,
                row_count=1,
                source="feishu",
                trace_id="fs_test_trace",
                query_id="query-1",
            )
            result = asyncio.run(app_module.api_query_history(
                limit=50,
                engine=None,
                query_type=None,
                source="feishu",
                trace_id=None,
                question_id=None,
                client_ip=None,
            ))

        self.assertTrue(recorded)
        self.assertEqual(1, result["total"])
        self.assertEqual("fs_test_trace", result["items"][0]["trace_id"])
        self.assertEqual("query-1", result["items"][0]["query_id"])

    def test_feishu_shared_report_services_are_bound_without_importing_app(self):
        feishu_bot_api.set_shared_report_services(
            app_module.persist_shared_report,
            app_module.build_shared_table_html,
            app_module.markdown_report_to_share_html,
        )

        self.assertIs(app_module.persist_shared_report, feishu_bot_api._shared_report_persister)
        self.assertIs(app_module.build_shared_table_html, feishu_bot_api._shared_table_builder)
        self.assertIs(app_module.markdown_report_to_share_html, feishu_bot_api._shared_markdown_renderer)
        with open(feishu_bot_api.__file__, "r", encoding="utf-8") as source_file:
            self.assertNotIn("from app import", source_file.read())

if __name__ == "__main__":
    unittest.main()
