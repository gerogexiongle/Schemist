#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from pathlib import Path
import tempfile
import skills.complex_query_retrieval as retrieval
import unittest
from unittest.mock import patch

from agents import sql_agent
from skills import schema_skill, sql_pipeline
from skills.complex_query_retrieval import (
    build_complex_query_plan,
    build_complex_retrieval_context,
    evaluate_semantic_coverage,
)


def setUpModule():
    global _mapping_patch, _mapping_directory
    # Independent synthetic stages exercise the extension without deployment rules.
    mapping = {
        "activation": {
            "min_stages": 3,
            "activate_without_marker_stages": 4,
            "markers": ["漏斗"],
        },
        "stages": [
            {
                "id": "stage_" + suffix,
                "label": label,
                "match_any": [label],
                "search_query": label,
                "pinned_tables": ["test_data.stage_" + suffix],
                "business_rules": [label + "使用对应测试表中的事件记录。"],
                "coverage_any": [{
                    "table": "test_data.stage_" + suffix,
                    "all_markers": ["event_" + suffix],
                }],
            }
            for suffix, label in [("a", "阶段甲"), ("b", "阶段乙"), ("c", "阶段丙")]
        ],
    }
    _mapping_directory = tempfile.TemporaryDirectory(prefix="schemist-stage-fixture-")
    mapping_path = Path(_mapping_directory.name) / "mapping.json"
    mapping_path.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
    _mapping_patch = patch.object(
        retrieval, "COMPLEX_QUERY_MAPPING_PATH",
        str(mapping_path),
    )
    _mapping_patch.start()
    retrieval._mapping_cache = None
    retrieval._mapping_mtime = None


def tearDownModule():
    _mapping_patch.stop()
    _mapping_directory.cleanup()
    retrieval._mapping_cache = None
    retrieval._mapping_mtime = None


FUNNEL_QUERY = "统计阶段甲、阶段乙、阶段丙的最近7天漏斗数据"

SCOPED_SUPPLEMENT_QUERY = FUNNEL_QUERY + (
    "\n补充生成，仅针对本轮已确认缺失的阶段丙记录数"
    "（不要重复生成阶段甲、阶段乙查询）。"
)

PARTIAL_SQL = (
    "SELECT COUNT(*) FROM test_data.stage_c "
    "WHERE event_name = 'event_c' AND dt >= '2026-09-03'"
)

COMPLETE_SQL = """SELECT 'stage_a' AS stage, COUNT(*) AS total
FROM test_data.stage_a WHERE event_name = 'event_a'
UNION ALL
SELECT 'stage_b' AS stage, COUNT(*) AS total
FROM test_data.stage_b WHERE event_name = 'event_b'
UNION ALL
SELECT 'stage_c' AS stage, COUNT(*) AS total
FROM test_data.stage_c WHERE event_name = 'event_c'"""


class ComplexFunnelRecognitionTests(unittest.TestCase):
    def test_exact_query_activates_three_stage_extension(self):
        plan = build_complex_query_plan(FUNNEL_QUERY)

        self.assertTrue(plan["active"])
        self.assertEqual("stage_funnel_extension", plan["strategy"])
        self.assertEqual([
            "stage_a",
            "stage_b",
            "stage_c",
        ], [stage["id"] for stage in plan["stages"]])

    def test_normal_query_keeps_default_strategy(self):
        plan = build_complex_query_plan("查询最近7天日活和充值收入")

        self.assertFalse(plan["active"])
        self.assertEqual("default", plan["strategy"])

    def test_explicit_supplement_scope_does_not_restore_full_funnel(self):
        plan = build_complex_query_plan(SCOPED_SUPPLEMENT_QUERY)
        coverage = evaluate_semantic_coverage(
            SCOPED_SUPPLEMENT_QUERY,
            PARTIAL_SQL,
            plan=plan,
        )

        self.assertTrue(plan["active"])
        self.assertTrue(plan["scope_applied"])
        self.assertEqual(
            ["stage_c"],
            [stage["id"] for stage in plan["stages"]],
        )
        self.assertTrue(coverage["complete"])
        self.assertEqual([], coverage["missing_stages"])

    def test_unrelated_scope_does_not_inherit_any_funnel_stage(self):
        query = FUNNEL_QUERY + (
            "\n本轮只补处理耗时"
            "（不要重复恢复完整阶段甲、阶段乙、阶段丙漏斗）。"
        )

        plan = build_complex_query_plan(query)

        self.assertTrue(plan["scope_applied"])
        self.assertFalse(plan["active"])
        self.assertEqual("default", plan["strategy"])
        self.assertEqual([], plan["stages"])

    def test_verified_tables_are_pinned_before_stage_search(self):
        def fake_search(keyword, limit):
            return {"tables": [
                {"table": "db.stage_candidate", "comment": "candidate", "score": 1.0}
            ]}

        with patch("skills.schema_skill.skill_search_tables", side_effect=fake_search), \
                patch("skills.schema_skill.load_table_schema", return_value={}), \
                patch("skills.schema_skill.find_query_related_feedback_tables", return_value=[]):
            context = build_complex_retrieval_context(FUNNEL_QUERY)

        pinned = {
            item["table"] for item in context["candidates"]
            if item["source"] == "verified_mapping"
        }
        self.assertEqual({
            "test_data.stage_a",
            "test_data.stage_b",
            "test_data.stage_c",
        }, pinned)
        self.assertIn("阶段甲", context["prompt"])
        self.assertIn("阶段乙", context["prompt"])

    def test_partial_sql_is_incomplete_and_complete_sql_passes(self):
        partial = evaluate_semantic_coverage(FUNNEL_QUERY, PARTIAL_SQL)
        complete = evaluate_semantic_coverage(FUNNEL_QUERY, COMPLETE_SQL)

        self.assertFalse(partial["complete"])
        self.assertIn("stage_a", partial["missing_stages"])
        self.assertIn("stage_b", partial["missing_stages"])
        self.assertTrue(complete["complete"])
        self.assertEqual([], complete["missing_stages"])


class ComplexFunnelAgentTests(unittest.TestCase):
    def test_complex_budget_is_isolated_from_default_budget(self):
        constructor_calls = []

        class FakeDeepAgent(object):
            def __init__(self, **kwargs):
                constructor_calls.append(kwargs)

        with patch.object(sql_agent, "DeepAgent", FakeDeepAgent), \
                patch.object(sql_agent, "_build_relevant_table_catalog", return_value="catalog"), \
                patch.object(sql_agent, "build_complex_retrieval_context", return_value={
                    "active": True, "prompt": "stage context"
                }):
            normal = sql_agent.create_sql_agent(query="查询最近7天日活")
            complex_agent = sql_agent.create_sql_agent(query=FUNNEL_QUERY)

        normal_options, complex_options = constructor_calls
        self.assertEqual(sql_agent.SQL_AGENT_MAX_TOOL_ROUNDS, normal_options["max_iterations"])
        self.assertEqual(
            sql_agent.SQL_AGENT_SEARCH_TABLES_LIMIT,
            normal_options["skill_call_limits"]["search_tables"],
        )
        self.assertEqual(
            sql_agent.SQL_AGENT_COMPLEX_MAX_TOOL_ROUNDS,
            complex_options["max_iterations"],
        )
        self.assertEqual(
            sql_agent.SQL_AGENT_COMPLEX_SEARCH_TABLES_LIMIT,
            complex_options["skill_call_limits"]["search_tables"],
        )
        self.assertEqual("default", normal.complex_query_plan["strategy"])
        self.assertEqual("stage_funnel_extension", complex_agent.complex_query_plan["strategy"])

    def test_semantic_recovery_then_single_strong_fallback(self):
        class FakeAgent(object):
            def __init__(self):
                self.last_run_info = {"forced_finalization": False}
                self.models = []

            def run(self, **kwargs):
                return json.dumps({"sql": PARTIAL_SQL, "explanation": "partial"})

            def finalize_last_run(self, model, **kwargs):
                self.models.append(model)
                if model == "cheap-model":
                    return json.dumps({"sql": PARTIAL_SQL, "explanation": "still partial"})
                return json.dumps({"sql": COMPLETE_SQL, "explanation": "complete"})

        fake_agent = FakeAgent()

        def passthrough(sql, engine="spark", explanation=""):
            return {"sql": sql, "explanation": explanation, "validation_issues": []}

        with patch.object(sql_agent, "create_sql_agent", return_value=fake_agent), \
                patch.object(sql_agent, "postprocess_sql", side_effect=passthrough), \
                patch.object(sql_agent, "build_semantic_recovery_context", return_value="context"), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "strong-model"), \
                patch.object(sql_agent, "SQL_AGENT_STRONG_MODELS", frozenset({"strong-model"})):
            result = sql_agent.generate_sql(FUNNEL_QUERY, engine="trino", llm_model="cheap-model")

        self.assertEqual(["cheap-model", "strong-model"], fake_agent.models)
        self.assertTrue(result["semantic_complete"])
        self.assertTrue(result["fallback_used"])
        self.assertIn("semantic_incomplete", result["fallback_reason"])

    def test_selected_strong_model_never_switches_model(self):
        class FakeAgent(object):
            def __init__(self):
                self.last_run_info = {"forced_finalization": False}
                self.models = []

            def run(self, **kwargs):
                return json.dumps({"sql": PARTIAL_SQL, "explanation": "partial"})

            def finalize_last_run(self, model, **kwargs):
                self.models.append(model)
                return json.dumps({"sql": PARTIAL_SQL, "explanation": "still partial"})

        fake_agent = FakeAgent()

        def passthrough(sql, engine="spark", explanation=""):
            return {"sql": sql, "explanation": explanation, "validation_issues": []}

        with patch.object(sql_agent, "create_sql_agent", return_value=fake_agent), \
                patch.object(sql_agent, "postprocess_sql", side_effect=passthrough), \
                patch.object(sql_agent, "build_semantic_recovery_context", return_value="context"), \
                patch.object(sql_agent, "SQL_AGENT_FALLBACK_MODEL", "strong-model"), \
                patch.object(sql_agent, "SQL_AGENT_STRONG_MODELS", frozenset({"strong-model"})):
            result = sql_agent.generate_sql(FUNNEL_QUERY, engine="trino", llm_model="strong-model")

        self.assertEqual(["strong-model"], fake_agent.models)
        self.assertFalse(result["semantic_complete"])
        self.assertFalse(result["fallback_used"])


class ComplexFunnelPipelineTests(unittest.TestCase):
    def test_pipeline_does_not_execute_semantically_incomplete_sql(self):
        with patch.object(sql_pipeline, "execute_sql") as execute, \
                patch.object(sql_pipeline, "_publish_pipeline_snapshot"):
            result = sql_pipeline.run_sql_pipeline(
                query=FUNNEL_QUERY,
                pre_sql=PARTIAL_SQL,
                engine="trino",
                max_retries=0,
            )

        execute.assert_not_called()
        self.assertFalse(result["success"])
        self.assertEqual("semantic_incomplete", result["error_kind"])
        self.assertEqual("semantic_precheck", result["attempts"][0]["phase"])
        self.assertEqual(PARTIAL_SQL, result["sql"])


class QueryFeedbackTests(unittest.TestCase):
    def test_feedback_matches_query_text_instead_of_global_popularity(self):
        payload = {"table_success_stats": {
            "db.relevant": {
                "success_score": 3,
                "recent_queries": ["阶段甲到阶段乙的漏斗统计"],
            },
            "db.popular_but_unrelated": {
                "success_score": 999,
                "recent_queries": ["服务器磁盘容量分布"],
            },
        }}
        with patch.object(schema_skill, "_load_feedback_data", return_value=payload), \
                patch.object(schema_skill, "load_table_schema", return_value={}):
            result = schema_skill.find_query_related_feedback_tables(
                "阶段甲到阶段乙的漏斗统计", limit=5
            )

        self.assertEqual(["db.relevant"], [item["table"] for item in result])


if __name__ == "__main__":
    unittest.main()
