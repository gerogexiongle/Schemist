#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Additive retrieval and semantic checks for configured multi-stage funnels."""
import json
import logging
import os
import re
from typing import Dict, List, Optional

from config.settings import (
    COMPLEX_QUERY_MAPPING_PATH,
    SQL_AGENT_COMPLEX_CATALOG_MAX,
    SQL_AGENT_COMPLEX_FEEDBACK_TOP_N,
    SQL_AGENT_COMPLEX_STAGE_TOP_N,
)

logger = logging.getLogger("complex_query_retrieval")

_mapping_cache = None
_mapping_mtime = None

_EXPLICIT_STAGE_SCOPE_MARKERS = (
    "仅针对",
    "只针对",
    "仅补充",
    "只补充",
    "仅补",
    "只补",
)
_NEGATED_SCOPE_PREFIXES = ("不要", "不能", "不应")


def _load_mapping_config() -> Dict:
    global _mapping_cache, _mapping_mtime
    path = COMPLEX_QUERY_MAPPING_PATH
    if not path or not os.path.isfile(path):
        return {"activation": {}, "stages": []}
    try:
        mtime = os.path.getmtime(path)
        if _mapping_cache is not None and _mapping_mtime == mtime:
            return _mapping_cache
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("activation", {})
        payload.setdefault("stages", [])
        _mapping_cache = payload
        _mapping_mtime = mtime
        return payload
    except Exception as exc:
        logger.warning("complex query mapping load failed: %s", exc)
        return {"activation": {}, "stages": []}


def _contains(text: str, term: str) -> bool:
    return str(term or "").strip().lower() in text


def _extract_explicit_stage_scope(text: str) -> str:
    """Return the latest explicitly limited metric scope, excluding negated items."""
    marker_index = -1
    marker_value = ""
    for marker in _EXPLICIT_STAGE_SCOPE_MARKERS:
        index = text.rfind(marker)
        if index < 0:
            continue
        prefix = text[max(0, index - 2):index]
        if any(prefix.endswith(item) for item in _NEGATED_SCOPE_PREFIXES):
            continue
        if index > marker_index or (index == marker_index and len(marker) > len(marker_value)):
            marker_index = index
            marker_value = marker
    if marker_index < 0:
        return ""

    scope = text[marker_index + len(marker_value):]
    scope = re.split(r"[。；;\n]", scope, maxsplit=1)[0]
    scope = re.sub(
        r"[（(][^）)]*(?:不要|无需|不再|不必|不补|排除|不包含|不含)[^）)]*[）)]",
        " ",
        scope,
    )
    scope = re.sub(
        r"(?:不要|无需|不再|不必|不补|排除|不包含|不含)[^。；;\n]*",
        " ",
        scope,
    )
    return " ".join(scope.split()).strip()


def _stage_matches(text: str, stage: Dict) -> bool:
    match_any = stage.get("match_any") or []
    if match_any and not any(_contains(text, item) for item in match_any):
        return False
    match_all = stage.get("match_all") or []
    for alternatives in match_all:
        values = alternatives if isinstance(alternatives, list) else [alternatives]
        if not any(_contains(text, item) for item in values):
            return False
    return bool(match_any or match_all)


def build_complex_query_plan(query: str) -> Dict:
    """Recognize configured funnel stages without changing the general query path."""
    config = _load_mapping_config()
    text = str(query or "").lower()
    scope_text = _extract_explicit_stage_scope(text)
    match_text = scope_text or text
    stages = [
        dict(stage) for stage in (config.get("stages") or [])
        if isinstance(stage, dict) and _stage_matches(match_text, stage)
    ]
    activation = config.get("activation") or {}
    min_stages = max(1, int(activation.get("min_stages", 3) or 3))
    no_marker_stages = max(
        min_stages,
        int(activation.get("activate_without_marker_stages", 4) or 4),
    )
    markers = activation.get("markers") or []
    marker_hit = next((m for m in markers if _contains(match_text, m)), "")
    if scope_text:
        marker_hit = "explicit_scope"
        active = bool(stages)
    else:
        active = len(stages) >= min_stages and bool(marker_hit or len(stages) >= no_marker_stages)
    return {
        "active": active,
        "strategy": "stage_funnel_extension" if active else "default",
        "marker": marker_hit,
        "scope_applied": bool(scope_text),
        "scope_text": scope_text,
        "stage_count": len(stages),
        "stages": stages if active else [],
    }


def _append_candidate(candidates: List[Dict], seen: set, table: str, stage: Dict,
                      source: str, comment: str = "", score: float = 0.0) -> bool:
    table_name = str(table or "").strip()
    key = table_name.lower()
    if not table_name or "." not in table_name or key in seen:
        return False
    seen.add(key)
    candidates.append({
        "table": table_name,
        "stage_id": stage.get("id", ""),
        "stage_label": stage.get("label", ""),
        "source": source,
        "comment": str(comment or "")[:240],
        "score": float(score or 0.0),
    })
    return True


def build_complex_retrieval_context(query: str, plan: Optional[Dict] = None) -> Dict:
    """Retrieve Top N candidates per requested stage and merge them deterministically."""
    from skills.schema_skill import (
        find_query_related_feedback_tables,
        load_table_schema,
        skill_search_tables,
    )

    plan = plan or build_complex_query_plan(query)
    if not plan.get("active"):
        return {"active": False, "strategy": "default", "candidates": [], "prompt": ""}

    candidates = []
    seen = set()
    stage_rows = []
    top_n = SQL_AGENT_COMPLEX_STAGE_TOP_N
    for stage in plan.get("stages") or []:
        stage_candidates = []
        stage_seen = set()
        for table in stage.get("pinned_tables") or []:
            if load_table_schema(table) is None:
                logger.warning("configured funnel table missing from schema index: %s", table)
                continue
            item = {
                "table": table,
                "comment": "已验证业务映射",
                "source": "verified_mapping",
                "score": 1000.0,
            }
            stage_candidates.append(item)
            stage_seen.add(table.lower())
        try:
            result = skill_search_tables(
                keyword=str(stage.get("search_query") or stage.get("label") or "")[:2000],
                limit=max(top_n * 2, top_n),
            )
        except Exception as exc:
            logger.warning("stage table retrieval failed for %s: %s", stage.get("id"), exc)
            result = {}
        for item in result.get("tables", []) if isinstance(result, dict) else []:
            table = str(item.get("table") or "")
            if not table or table.lower() in stage_seen:
                continue
            stage_candidates.append({
                "table": table,
                "comment": item.get("comment") or "",
                "source": "stage_search",
                "score": item.get("score") or 0.0,
            })
            stage_seen.add(table.lower())
            if len(stage_candidates) >= top_n:
                break
        stage_candidates = stage_candidates[:top_n]
        stage_rows.append({"stage": stage, "candidates": stage_candidates})
        for item in stage_candidates:
            _append_candidate(
                candidates, seen, item.get("table"), stage, item.get("source"),
                comment=item.get("comment") or "", score=item.get("score") or 0.0,
            )

    if SQL_AGENT_COMPLEX_FEEDBACK_TOP_N > 0:
        for item in find_query_related_feedback_tables(
            query, limit=SQL_AGENT_COMPLEX_FEEDBACK_TOP_N
        ):
            _append_candidate(
                candidates, seen, item.get("table"),
                {"id": "feedback", "label": "相似历史问题"},
                "query_feedback", comment=item.get("last_query") or "",
                score=item.get("similarity") or 0.0,
            )

    candidates = candidates[:SQL_AGENT_COMPLEX_CATALOG_MAX]
    lines = [
        "【复杂漏斗扩展已启用】",
        "必须覆盖用户点名的每个阶段；已验证映射置顶，普通检索候选仅用于补充。",
    ]
    allowed = {item["table"].lower() for item in candidates}
    for row in stage_rows:
        stage = row["stage"]
        lines.append("\n阶段 {}（{}）：".format(stage.get("label"), stage.get("id")))
        for rule in stage.get("business_rules") or []:
            lines.append("- 已验证规则：{}".format(rule))
        for item in row["candidates"]:
            if item.get("table", "").lower() not in allowed:
                continue
            lines.append("- 候选 [{}] {}{}".format(
                item.get("source"), item.get("table"),
                " -- {}".format(item.get("comment")) if item.get("comment") else "",
            ))
    feedback_items = [x for x in candidates if x.get("source") == "query_feedback"]
    if feedback_items:
        lines.append("\n与当前问题文本相似的成功查询用表（仅辅助，不替代已验证映射）：")
        for item in feedback_items:
            lines.append("- {} -- similarity={:.3f}".format(item["table"], item["score"]))

    prompt = "\n".join(lines)
    logger.info(
        "Complex funnel retrieval: stages=%d candidates=%d chars=%d",
        len(stage_rows), len(candidates), len(prompt),
    )
    return {
        "active": True,
        "strategy": "stage_funnel_extension",
        "plan": plan,
        "candidates": candidates,
        "prompt": prompt,
    }


def evaluate_semantic_coverage(query: str, sql: str, plan: Optional[Dict] = None) -> Dict:
    """Check requested funnel stages against table and business markers in final SQL."""
    plan = plan or build_complex_query_plan(query)
    if not plan.get("active"):
        return {
            "applicable": False,
            "complete": True,
            "covered_stages": [],
            "missing_stages": [],
            "stage_results": [],
        }

    normalized_sql = re.sub(r"[`\"]", "", str(sql or "").lower())
    stage_results = []
    covered = []
    missing = []
    for stage in plan.get("stages") or []:
        matched = False
        for alternative in stage.get("coverage_any") or []:
            table = str(alternative.get("table") or "").lower()
            markers = [str(x).lower() for x in (alternative.get("all_markers") or [])]
            if table and table not in normalized_sql:
                continue
            if all(marker in normalized_sql for marker in markers):
                matched = True
                break
        item = {
            "id": stage.get("id", ""),
            "label": stage.get("label", ""),
            "covered": matched,
        }
        stage_results.append(item)
        if matched:
            covered.append(item["id"])
        else:
            missing.append(item["id"])
    return {
        "applicable": True,
        "complete": not missing,
        "covered_stages": covered,
        "missing_stages": missing,
        "stage_results": stage_results,
    }


def build_semantic_recovery_context(plan: Dict, missing_stage_ids: List[str]) -> str:
    """Build a tool-free repair context containing only missing verified stages and schemas."""
    from skills.schema_skill import skill_get_table_info

    missing = set(missing_stage_ids or [])
    lines = [
        "【语义完整性修复】",
        "当前 SQL 未覆盖以下用户明确要求的阶段。必须基于当前 SQL 补齐，不得删除已覆盖阶段。",
    ]
    emitted_tables = set()
    for stage in plan.get("stages") or []:
        if stage.get("id") not in missing:
            continue
        lines.append("\n缺失阶段：{}（{}）".format(stage.get("label"), stage.get("id")))
        for rule in stage.get("business_rules") or []:
            lines.append("- {}".format(rule))
        for table in stage.get("pinned_tables") or []:
            key = table.lower()
            if key in emitted_tables:
                continue
            emitted_tables.add(key)
            info = skill_get_table_info(table_name=table, full_detail=True)
            if not isinstance(info, dict) or info.get("error"):
                continue
            columns = []
            for col in info.get("columns") or []:
                if isinstance(col, dict) and col.get("name"):
                    columns.append("{} {}".format(col.get("name"), col.get("type") or ""))
            lines.append("- 表结构 {}: {}".format(table, ", ".join(columns[:120])))
    lines.append(
        "\n输出一条完整可执行 SQL JSON。所有请求阶段必须有独立统计字段；漏斗去重键、日期粒度和事件口径必须使用配置及真实表结构确认的字段。"
    )
    return "\n".join(lines)
