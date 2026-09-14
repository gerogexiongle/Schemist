#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL 全流程编排：generate → precheck → execute → repair（可重试）→ optional analyze
"""
import logging
import re
import time
from typing import Dict, List, Optional

from agents.analysis_agent import analyze_data
from agents.sql_agent import generate_sql, repair_sql
from config.settings import QUERY_RESULT_MAX_ROWS, SQL_EXECUTOR_TIMEOUT
from skills.complex_query_retrieval import evaluate_semantic_coverage
from skills.pipeline_trace import log as pipeline_log, update_trace_snapshot
from skills.query_result_cache import put_query_result
from skills.schema_skill import list_unknown_tables_in_sql, record_success_feedback_from_sql
from skills.sql_execution_errors import (
    format_execution_error_for_user,
    is_permission_denied_error,
)
from skills.sql_executor import execute_sql
from skills.sql_validator import (
    fix_trino_date_type_error,
    format_sql_error_context,
    validate_engine_sql,
)

logger = logging.getLogger("sql_pipeline")

DEFAULT_MAX_RETRIES = 2

ZERO_ROWS_HINT = '查询成功但结果集为 0 行。请核对日期分区是否有数据、WHERE 枚举值及字段类型是否与实际数据一致；必要时对目标字段做 DISTINCT 抽样，并核对 JOIN 条件。'

NON_RETRYABLE_PATTERNS = [
    r"\bpermission denied\b",
    r"\baccess denied\b",
    r"\bPERMISSION_DENIED\b",
    r"Cannot select from table",
    r"Cannot insert into table",
    r"Cannot delete from table",
    r"\bnot authorized\b",
    r"\bauthorization failed\b",
    r"\bquery was cancelled\b",
    r"\bcancelled by user\b",
    r"\bquery killed\b",
    r"无权限",
    r"没有权限",
]


def is_retryable_error(error_message: str, repair_reason: str = "execute") -> bool:
    if is_permission_denied_error(error_message):
        return False
    if repair_reason in ("precheck", "zero_rows"):
        return True
    if not error_message:
        return False
    for pat in NON_RETRYABLE_PATTERNS:
        if re.search(pat, error_message, re.IGNORECASE):
            return False
    return True


def collect_pre_execution_issues(sql: str, engine: str) -> List[str]:
    issues = []
    if not sql or not sql.strip():
        issues.append("SQL is empty")
        return issues
    for t in list_unknown_tables_in_sql(sql):
        issues.append("unknown table not in schema index: {}".format(t))
    issues.extend(validate_engine_sql(sql, engine=engine))
    return issues


def _normalized_sql(sql: str) -> str:
    """Normalize formatting only; literal case changes remain meaningful."""
    return re.sub(r"\s+", " ", (sql or "").strip().rstrip(";")).strip()


def _is_meaningful_repair(old_sql: str, new_sql: str) -> bool:
    return bool((new_sql or "").strip()) and _normalized_sql(old_sql) != _normalized_sql(new_sql)


def _log_repair_exception(phase: str, error: Exception) -> None:
    text = str(error)
    error_name = type(error).__name__
    if "timeout" in error_name.lower() or "timed out" in text.lower():
        logger.warning("pipeline %s repair timed out: %s", phase, text or error_name)
    else:
        logger.exception("pipeline %s repair failed: %s", phase, error)


def _append_attempt(attempts, attempt_num, phase, sql, success, error=None, repaired=False, **extra):
    item = {
        "attempt": attempt_num,
        "phase": phase,
        "sql_chars": len(sql or ""),
        "success": success,
        "error": (error or "")[:2000] if error else None,
        "repaired": repaired,
    }
    item.update(extra)
    attempts.append(item)


def _publish_pipeline_snapshot(stage: str, sql: str = None, explanation: str = None, tables_used=None,
                               execution_plan=None, generate_time: float = None, **extra) -> None:
    fields = {"stage": stage}
    if sql is not None:
        fields["sql"] = sql
    if explanation is not None:
        fields["explanation"] = explanation
    if tables_used is not None:
        fields["tables_used"] = list(tables_used or [])
    if execution_plan is not None:
        fields["execution_plan"] = execution_plan
    if generate_time is not None:
        fields["generate_time"] = generate_time
    fields.update(extra)
    try:
        update_trace_snapshot(**fields)
    except Exception as e:
        logger.warning("pipeline snapshot publish failed: %s", e)


def run_sql_pipeline(
    query: str,
    history=None,
    engine: str = "spark",
    llm_model=None,
    temperature: float = 0.3,
    max_tokens: int = 4000,
    max_retries: int = DEFAULT_MAX_RETRIES,
    max_rows: int = None,
    timeout: int = None,
    include_analyze: bool = False,
    retry_zero_rows: bool = True,
    chart_type: Optional[str] = None,
    pre_sql: Optional[str] = None,
    pre_explanation: str = "",
    pre_tables_used: Optional[List] = None,
    pre_execution_plan=None,
    pre_generate_time: float = 0,
) -> Dict:
    """
    生成 SQL 并自动执行，失败时最多重试 max_retries 次（默认 2）。
    返回统一结构供 Web / 飞书 / API 消费。
    """
    if max_rows is None:
        max_rows = QUERY_RESULT_MAX_ROWS
    if timeout is None:
        timeout = SQL_EXECUTOR_TIMEOUT

    pipeline_start = time.time()
    attempts = []
    max_retries = max(0, int(max_retries or 0))
    max_attempts = max_retries + 1

    pipeline_log(logger, "pipeline.enter", engine=engine, q_chars=len(query or ""), max_retries=max_retries)

    pre_sql_stripped = (pre_sql or "").strip()
    if pre_sql_stripped:
        sql = pre_sql_stripped
        explanation = pre_explanation or ""
        tables_used = list(pre_tables_used or [])
        execution_plan = pre_execution_plan
        generate_time = float(pre_generate_time or 0)
        semantic_coverage = evaluate_semantic_coverage(query, sql)
        pipeline_log(logger, "pipeline.generate.skip", sql_chars=len(sql), sec=generate_time)
        _publish_pipeline_snapshot(
            "generate_done", sql=sql, explanation=explanation, tables_used=tables_used,
            execution_plan=execution_plan, generate_time=generate_time,
        )
    else:
        pipeline_log(logger, "pipeline.generate.start", engine=engine)
        gen = generate_sql(
            query=query,
            history=history,
            engine=engine,
            temperature=temperature,
            max_tokens=max_tokens,
            llm_model=llm_model,
        )
        sql = (gen.get("sql") or "").strip()
        explanation = gen.get("explanation") or ""
        tables_used = gen.get("tables_used") or []
        execution_plan = gen.get("execution_plan")
        generate_time = gen.get("query_time") or 0
        semantic_coverage = gen.get("semantic_coverage") or evaluate_semantic_coverage(
            query, sql
        )

        if not sql:
            pipeline_log(logger, "pipeline.generate.empty")
            _publish_pipeline_snapshot("failed", sql="", error="Failed to generate SQL", explanation=explanation)
            return {
                "success": False,
                "error": "Failed to generate SQL",
                "sql": "",
                "explanation": explanation,
                "tables_used": tables_used,
                "execution_plan": execution_plan,
                "generate_time": generate_time,
                "attempts": attempts,
                "execution": None,
                "analysis": None,
                "pipeline_time": time.time() - pipeline_start,
                "semantic_complete": bool(semantic_coverage.get("complete", True)),
                "semantic_coverage": semantic_coverage,
                "missing_stages": semantic_coverage.get("missing_stages") or [],
                "retrieval_strategy": gen.get("retrieval_strategy", "default"),
            }

        pipeline_log(logger, "pipeline.generate.done", sql_chars=len(sql), sec=generate_time)
        _publish_pipeline_snapshot(
            "generate_done", sql=sql, explanation=explanation, tables_used=tables_used,
            execution_plan=execution_plan, generate_time=generate_time,
        )

    execution_result = None

    for attempt_num in range(1, max_attempts + 1):
        semantic_coverage = evaluate_semantic_coverage(query, sql)
        if semantic_coverage.get("applicable") and not semantic_coverage.get("complete"):
            missing_labels = [
                item.get("label") for item in semantic_coverage.get("stage_results", [])
                if not item.get("covered")
            ]
            error_message = "SQL 语义完整性检查未通过，缺失阶段：{}".format(
                "、".join([x for x in missing_labels if x]) or "未知"
            )
            pipeline_log(
                logger, "pipeline.semantic_precheck.fail", attempt=attempt_num,
                missing=",".join(semantic_coverage.get("missing_stages") or []),
            )
            _append_attempt(
                attempts, attempt_num, "semantic_precheck", sql,
                success=False, error=error_message,
                missing_stages=semantic_coverage.get("missing_stages") or [],
            )
            _publish_pipeline_snapshot(
                "failed", sql=sql, explanation=explanation,
                tables_used=tables_used, error=error_message,
                error_kind="semantic_incomplete",
                semantic_coverage=semantic_coverage,
            )
            return {
                "success": False,
                "error": error_message,
                "error_kind": "semantic_incomplete",
                "sql": sql,
                "explanation": explanation,
                "tables_used": tables_used,
                "execution_plan": execution_plan,
                "generate_time": generate_time,
                "attempts": attempts,
                "execution": None,
                "analysis": None,
                "pipeline_time": time.time() - pipeline_start,
                "semantic_complete": False,
                "semantic_coverage": semantic_coverage,
                "missing_stages": semantic_coverage.get("missing_stages") or [],
                "retrieval_strategy": "stage_funnel_extension",
            }
        issues = collect_pre_execution_issues(sql, engine)
        if issues:
            issue_text = "; ".join(issues)
            pipeline_log(logger, "pipeline.precheck.fail", attempt=attempt_num, n=len(issues))
            _append_attempt(
                attempts, attempt_num, "precheck", sql, success=False, error=issue_text,
            )
            if attempt_num <= max_retries:
                try:
                    repaired = repair_sql(
                        original_question=query,
                        failed_sql=sql,
                        error_message=issue_text,
                        engine=engine,
                        repair_reason="precheck",
                        tables_used=tables_used,
                        llm_model=llm_model,
                        use_fallback_model=(attempt_num >= 2),
                    )
                except Exception as e:
                    _log_repair_exception("precheck", e)
                    repaired = {}
                new_sql = (repaired.get("sql") or "").strip()
                if _is_meaningful_repair(sql, new_sql):
                    sql = new_sql
                    explanation = repaired.get("explanation") or explanation
                    tables_used = repaired.get("tables_used") or tables_used
                    attempts[-1]["repaired"] = True
                    pipeline_log(logger, "pipeline.repair.done", reason="precheck", attempt=attempt_num)
                    _publish_pipeline_snapshot(
                        "repaired", sql=sql, explanation=explanation, tables_used=tables_used,
                        repair_reason="precheck", attempt=attempt_num,
                    )
                    continue
                if new_sql:
                    attempts[-1]["repair_no_change"] = True
                    pipeline_log(
                        logger, "pipeline.repair.no_change",
                        reason="precheck", attempt=attempt_num,
                    )
            execution_result = {
                "success": False,
                "headers": [],
                "result": [],
                "error": "Pre-check failed: " + issue_text,
                "execution_time": 0,
                "row_count": 0,
                "query_id": None,
                "debug_info": {},
            }
            break

        pipeline_log(logger, "pipeline.execute.start", attempt=attempt_num, sql_chars=len(sql))
        success, headers, results, err, exec_time, query_id, debug_info = execute_sql(
            sql=sql,
            engine=engine,
            max_rows=max_rows,
            timeout=timeout,
        )

        if success:
            row_count = len(results or [])
            pipeline_log(
                logger, "pipeline.execute.done",
                attempt=attempt_num, ok=True, rows=row_count, sec=exec_time,
            )
            _append_attempt(
                attempts, attempt_num, "execute", sql, success=True,
                row_count=row_count, execution_time=exec_time,
            )

            if row_count == 0 and retry_zero_rows and attempt_num <= max_retries:
                try:
                    repaired = repair_sql(
                        original_question=query,
                        failed_sql=sql,
                        error_message=ZERO_ROWS_HINT,
                        engine=engine,
                        repair_reason="zero_rows",
                        tables_used=tables_used,
                        llm_model=llm_model,
                        use_fallback_model=(attempt_num >= 2),
                    )
                except Exception as e:
                    _log_repair_exception("zero_rows", e)
                    repaired = {}
                new_sql = (repaired.get("sql") or "").strip()
                if _is_meaningful_repair(sql, new_sql):
                    sql = new_sql
                    explanation = repaired.get("explanation") or explanation
                    tables_used = repaired.get("tables_used") or tables_used
                    attempts[-1]["repaired"] = True
                    pipeline_log(logger, "pipeline.repair.done", reason="zero_rows", attempt=attempt_num)
                    _publish_pipeline_snapshot(
                        "repaired", sql=sql, explanation=explanation, tables_used=tables_used,
                        repair_reason="zero_rows", attempt=attempt_num,
                    )
                    continue
                if new_sql:
                    attempts[-1]["repair_no_change"] = True
                    pipeline_log(
                        logger, "pipeline.repair.no_change",
                        reason="zero_rows", attempt=attempt_num,
                    )

            di = dict(debug_info or {})
            if row_count == 0:
                di["zero_rows_hint"] = ZERO_ROWS_HINT
            execution_result = {
                "success": True,
                "headers": headers or [],
                "result": results or [],
                "error": None,
                "execution_time": exec_time,
                "row_count": row_count,
                "query_id": query_id,
                "debug_info": di,
            }
            _publish_pipeline_snapshot(
                "execute_done", sql=sql, query_id=query_id or "", row_count=row_count,
                execution_time=exec_time, execution_success=True, attempt=attempt_num,
            )
            break

        raw_err = err or "Unknown execution error"
        if is_permission_denied_error(raw_err) or (debug_info or {}).get("error_kind") == "permission_denied":
            err_full = raw_err
            if not err_full.startswith("【权限不足"):
                err_full = format_execution_error_for_user(raw_err, sql=sql, engine=engine)
            pipeline_log(logger, "pipeline.execute.permission_denied", attempt=attempt_num, err=err_full[:200])
            _append_attempt(
                attempts, attempt_num, "permission_denied", sql, success=False, error=err_full,
            )
            execution_result = {
                "success": False,
                "headers": [],
                "result": [],
                "error": err_full,
                "execution_time": exec_time,
                "row_count": 0,
                "query_id": query_id,
                "debug_info": dict(debug_info or {}, error_kind="permission_denied"),
            }
            _publish_pipeline_snapshot(
                "failed", sql=sql, error=err_full, error_kind="permission_denied", attempts=attempt_num,
            )
            break

        err_full = raw_err
        ctx = format_sql_error_context(sql, err_full)
        if ctx:
            err_full = err_full + "\n\n" + ctx

        pipeline_log(logger, "pipeline.execute.fail", attempt=attempt_num, err=err_full[:200])
        _append_attempt(
            attempts, attempt_num, "execute", sql, success=False, error=err_full,
        )

        if attempt_num <= max_retries and is_retryable_error(err_full, "execute"):
            deterministic_sql = (
                fix_trino_date_type_error(sql, err_full)
                if engine == "trino"
                else None
            )
            if _is_meaningful_repair(sql, deterministic_sql):
                sql = deterministic_sql
                repair_note = (
                    "Deterministic Trino date repair: converted varchar date operands "
                    "to DATE before DATE_ADD/comparison."
                )
                explanation = repair_note + ("\n\n" + explanation if explanation else "")
                attempts[-1]["repaired"] = True
                attempts[-1]["repair_strategy"] = "deterministic_trino_date_type"
                pipeline_log(
                    logger,
                    "pipeline.repair.done",
                    reason="execute",
                    attempt=attempt_num,
                    strategy="deterministic_trino_date_type",
                )
                _publish_pipeline_snapshot(
                    "repaired",
                    sql=sql,
                    explanation=explanation,
                    tables_used=tables_used,
                    repair_reason="execute",
                    repair_strategy="deterministic_trino_date_type",
                    attempt=attempt_num,
                )
                continue
            try:
                repaired = repair_sql(
                    original_question=query,
                    failed_sql=sql,
                    error_message=err_full,
                    engine=engine,
                    repair_reason="execute",
                    tables_used=tables_used,
                    llm_model=llm_model,
                    use_fallback_model=(attempt_num >= 2),
                )
            except Exception as e:
                _log_repair_exception("execute", e)
                repaired = {}
            new_sql = (repaired.get("sql") or "").strip()
            if _is_meaningful_repair(sql, new_sql):
                sql = new_sql
                repair_note = repaired.get("explanation") or ""
                if repair_note:
                    explanation = repair_note + "\n\n" + explanation
                tables_used = repaired.get("tables_used") or tables_used
                attempts[-1]["repaired"] = True
                pipeline_log(logger, "pipeline.repair.done", reason="execute", attempt=attempt_num)
                _publish_pipeline_snapshot(
                    "repaired", sql=sql, explanation=explanation, tables_used=tables_used,
                    repair_reason="execute", attempt=attempt_num,
                )
                continue
            if new_sql:
                attempts[-1]["repair_no_change"] = True
                pipeline_log(
                    logger, "pipeline.repair.no_change",
                    reason="execute", attempt=attempt_num,
                )

        execution_result = {
            "success": False,
            "headers": [],
            "result": [],
            "error": err_full,
            "execution_time": exec_time,
            "row_count": 0,
            "query_id": query_id,
            "debug_info": debug_info or {},
        }
        break

    if not execution_result or not execution_result.get("success"):
        err_msg = (execution_result or {}).get("error") or "Pipeline execution failed"
        pipeline_log(logger, "pipeline.exit", ok=False, attempts=len(attempts))
        err_kind = ((execution_result or {}).get("debug_info") or {}).get("error_kind")
        _publish_pipeline_snapshot(
            "failed", sql=sql, error=err_msg, error_kind=err_kind or "", attempts=len(attempts),
        )
        return {
            "success": False,
            "error": err_msg,
            "error_kind": err_kind,
            "sql": sql,
            "explanation": explanation,
            "tables_used": tables_used,
            "execution_plan": execution_plan,
            "generate_time": generate_time,
            "attempts": attempts,
            "execution": execution_result,
            "analysis": None,
            "pipeline_time": time.time() - pipeline_start,
        }

    if execution_result["row_count"] > 0:
        try:
            fb = record_success_feedback_from_sql(
                sql=sql,
                query=query,
                engine=engine,
                row_count=execution_result["row_count"],
            )
            if fb:
                execution_result["debug_info"]["schema_feedback"] = fb
        except Exception as fe:
            logger.warning("pipeline schema feedback failed: %s", fe)

    qid = execution_result.get("query_id")
    if qid and execution_result.get("result") is not None:
        try:
            put_query_result(
                qid,
                headers=execution_result.get("headers") or [],
                result=execution_result.get("result") or [],
                row_count=execution_result.get("row_count") or 0,
                execution_time=execution_result.get("execution_time") or 0,
                sql=sql,
                engine=engine,
                success=True,
            )
        except Exception as ce:
            logger.warning("pipeline query_result_cache put failed: %s", ce)

    analysis_result = None
    if include_analyze and execution_result["row_count"] > 0:
        pipeline_log(logger, "pipeline.analyze.start", rows=execution_result["row_count"])
        _publish_pipeline_snapshot("analyze_running", sql=sql)
        try:
            analysis_result = analyze_data(
                original_question=query,
                sql=sql,
                headers=execution_result["headers"],
                data=execution_result["result"],
                chart_type=chart_type,
                max_rows=max_rows,
                llm_model=llm_model,
            )
            pipeline_log(
                logger, "pipeline.analyze.done",
                ok=bool(analysis_result.get("success")),
                sec=analysis_result.get("analysis_time"),
            )
            _publish_pipeline_snapshot(
                "analyze_done",
                sql=sql,
                report=(analysis_result.get("report") or "") if analysis_result.get("success") else "",
                analysis_time=analysis_result.get("analysis_time") or 0,
                analysis_success=bool(analysis_result.get("success")),
            )
        except Exception as e:
            logger.exception("pipeline analyze failed: %s", e)
            analysis_result = {
                "success": False,
                "report": "",
                "error": str(e),
                "analysis_time": 0,
            }

    pipeline_log(
        logger, "pipeline.exit", ok=True,
        attempts=len(attempts), rows=execution_result["row_count"],
    )
    _publish_pipeline_snapshot("done", sql=sql, attempts=len(attempts))
    return {
        "success": True,
        "error": None,
        "sql": sql,
        "explanation": explanation,
        "tables_used": tables_used,
        "execution_plan": execution_plan,
        "generate_time": generate_time,
        "attempts": attempts,
        "execution": execution_result,
        "analysis": analysis_result,
        "pipeline_time": time.time() - pipeline_start,
        "semantic_complete": bool(semantic_coverage.get("complete", True)),
        "semantic_coverage": semantic_coverage,
        "missing_stages": semantic_coverage.get("missing_stages") or [],
        "retrieval_strategy": (
            "stage_funnel_extension" if semantic_coverage.get("applicable") else "default"
        ),
    }
