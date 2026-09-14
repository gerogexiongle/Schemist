#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schema skill: load table structures from local hivebrain JSON for Agent retrieval.
Supports BM25+倒排检索、同义词扩展、按库裁剪目录、get_table_info 摘要/全量两档。
"""
import json
import logging
import math
import os
import re
import time
from typing import Dict, List, Set, Tuple

from config.settings import (
    SCHEMA_BASE_DIRS,
    SCHEMA_ALLOWED_DBS,
    SCHEMA_ALIAS_MAP_PATH,
    SCHEMA_SEARCH_USE_BM25,
    SCHEMA_SEARCH_BM25_WEIGHT,
    SCHEMA_TABLE_PRIORITY_BOOST,
    SCHEMA_CATALOG_MODE,
    SCHEMA_GET_TABLE_INFO_DEFAULT_FULL,
)
from skills.schema_search import BM25Index, tokenize

logger = logging.getLogger("schema_skill")

_schema_cache = {}
_table_index = None
_table_catalog_cache = {}
_bm25_index = None
_alias_config = None
_alias_config_mtime = None
_feedback_data = None
_feedback_mtime = None

_SCHEMA_SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
_SERVICE_DIR = os.path.dirname(_SCHEMA_SKILL_DIR)
_DEFAULT_FEEDBACK_PATH = os.path.join(_SERVICE_DIR, "data", "schema_table_feedback.json")
_SCHEMA_FEEDBACK_PATH = os.environ.get("SCHEMA_FEEDBACK_PATH", _DEFAULT_FEEDBACK_PATH).strip()


class TableIndexEntry(object):
    __slots__ = ("full_name", "db", "table", "table_comment",
                 "col_names", "col_comments", "search_text",
                 "table_priority", "domain")

    def __init__(self, full_name, db, table, table_comment, col_names, col_comments,
                 table_priority=0, domain=""):
        self.full_name = full_name
        self.db = db
        self.table = table
        self.table_comment = table_comment
        self.col_names = col_names
        self.col_comments = col_comments
        self.table_priority = float(table_priority or 0.0)
        self.domain = domain or ""
        self.search_text = " ".join([
            full_name,
            table_comment,
            " ".join(col_names),
            " ".join(col_comments),
            self.domain,
        ]).lower()


def _extract_table_comment(schema):
    """
    Prefer top-level table_comment; fallback to properties.Comment.
    This avoids losing comments when upstream JSON only fills properties.Comment.
    """
    if not isinstance(schema, dict):
        return ""
    c = (schema.get("table_comment", "") or "").strip()
    if c:
        return c
    props = schema.get("properties", {}) or {}
    if isinstance(props, dict):
        # Keep case-insensitive lookup for compatibility with different exporters.
        for k in ("Comment", "comment"):
            v = props.get(k, "")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _allowed_db_filter():
    if not SCHEMA_ALLOWED_DBS:
        return None
    return set(str(d).strip() for d in SCHEMA_ALLOWED_DBS if str(d).strip())


def _normalize_table_name(table_full_name: str) -> str:
    return str(table_full_name or "").strip().lower()


def _default_feedback_payload():
    return {
        "version": 1,
        "table_success_stats": {},
    }


def _load_feedback_data():
    """
    Lightweight persistence for table success feedback.
    JSON format:
    {
      "version": 1,
      "table_success_stats": {
        "db.table": {"success_count": 3, "last_success_ts": 1710000000.0}
      }
    }
    """
    global _feedback_data, _feedback_mtime
    path = _SCHEMA_FEEDBACK_PATH
    if not path:
        _feedback_data = _default_feedback_payload()
        _feedback_mtime = None
        return _feedback_data
    if not os.path.isfile(path):
        _feedback_data = _default_feedback_payload()
        _feedback_mtime = None
        return _feedback_data
    try:
        mtime = os.path.getmtime(path)
        if _feedback_data is not None and _feedback_mtime == mtime:
            return _feedback_data
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            data = _default_feedback_payload()
        stats = data.get("table_success_stats", {})
        if not isinstance(stats, dict):
            stats = {}
        data["table_success_stats"] = stats
        data["version"] = int(data.get("version", 1) or 1)
        _feedback_data = data
        _feedback_mtime = mtime
    except Exception as e:
        logger.warning("schema feedback load failed: %s", e)
        _feedback_data = _default_feedback_payload()
        _feedback_mtime = None
    return _feedback_data


def _save_feedback_data(payload):
    global _feedback_data, _feedback_mtime
    path = _SCHEMA_FEEDBACK_PATH
    if not path:
        return False
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        _feedback_data = payload
        _feedback_mtime = os.path.getmtime(path)
        return True
    except Exception as e:
        logger.warning("schema feedback save failed: %s", e)
        return False


def _feedback_priority_boost(table_full_name: str) -> float:
    """
    Convert success_count to a bounded priority boost.
    log1p keeps growth stable and avoids runaway reinforcement.
    """
    data = _load_feedback_data()
    stats = data.get("table_success_stats", {}) if isinstance(data, dict) else {}
    item = stats.get(_normalize_table_name(table_full_name), {})
    try:
        score = float(item.get("success_score", item.get("success_count", 0)) or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    if score <= 0:
        return 0.0
    # Max around 8 points after enough successful executions.
    return min(8.0, math.log1p(score) * 2.6)


def _invalidate_search_caches():
    global _table_index, _bm25_index, _table_catalog_cache
    _table_index = None
    _bm25_index = None
    _table_catalog_cache = {}


def _build_table_index():
    """Scan hivebrain dirs and build a rich index with comments + BM25."""
    global _table_index, _bm25_index
    if _table_index is not None:
        return _table_index

    allowed = _allowed_db_filter()
    entries = []
    for base_dir in SCHEMA_BASE_DIRS:
        if not os.path.isdir(base_dir):
            continue
        for db_name in os.listdir(base_dir):
            if allowed is not None and db_name not in allowed:
                continue
            json_dir = os.path.join(base_dir, db_name, "json")
            if not os.path.isdir(json_dir):
                continue
            for fname in os.listdir(json_dir):
                if not fname.endswith(".json") or "." not in fname[:-5]:
                    continue
                json_path = os.path.join(json_dir, fname)
                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        schema = json.load(f)
                    table_full = fname[:-5]
                    _schema_cache[table_full] = schema

                    tbl_comment = _extract_table_comment(schema)
                    col_names = []
                    col_comments = []
                    for col in schema.get("columns", []):
                        cn = col.get("name", "").strip()
                        cc = col.get("comment", "").strip()
                        if cn:
                            col_names.append(cn)
                        if cc:
                            col_comments.append(cc)
                    for col in schema.get("partition_columns", []):
                        if isinstance(col, dict):
                            cn = col.get("name", "").strip()
                            cc = col.get("comment", "").strip()
                        else:
                            cn = str(col).strip()
                            cc = ""
                        if cn:
                            col_names.append(cn)
                        if cc:
                            col_comments.append(cc)

                    tp = 0
                    try:
                        tp = int(schema.get("table_priority", 0) or 0)
                    except (TypeError, ValueError):
                        tp = 0
                    tp = float(tp) + _feedback_priority_boost(table_full)
                    domain = schema.get("domain", "") or schema.get("subject", "") or ""

                    entries.append(TableIndexEntry(
                        full_name=table_full,
                        db=db_name,
                        table=table_full.split(".", 1)[1] if "." in table_full else table_full,
                        table_comment=tbl_comment,
                        col_names=col_names,
                        col_comments=col_comments,
                        table_priority=tp,
                        domain=domain,
                    ))
                except Exception as e:
                    logger.warning("Failed to index %s: %s", fname, e)

    _table_index = sorted(entries, key=lambda e: e.full_name)
    if SCHEMA_SEARCH_USE_BM25 and _table_index:
        doc_tokens = [tokenize(e.search_text) for e in _table_index]
        _bm25_index = BM25Index(doc_tokens)
    else:
        _bm25_index = None
    logger.info("Schema index: %d tables (allowed_dbs=%s, bm25=%s)",
                len(_table_index), list(allowed) if allowed else "all", _bm25_index is not None)
    return _table_index


def record_table_success_feedback(
    tables: List[str],
    query: str = "",
    engine: str = "",
    row_count: int = 0,
    table_weights: Dict[str, float] = None,
) -> Dict:
    """
    Record successful table usage and persist to local JSON.
    Called after SQL execute success (preferably with row_count > 0).
    """
    unique_tables = []
    seen = set()
    normalized_weights = {}
    for k, v in (table_weights or {}).items():
        try:
            normalized_weights[_normalize_table_name(k)] = max(0.0, float(v))
        except (TypeError, ValueError):
            continue

    for t in (tables or []):
        tn = _normalize_table_name(t)
        if not tn or "." not in tn or tn in seen:
            continue
        if load_table_schema(tn) is None:
            continue
        seen.add(tn)
        unique_tables.append(tn)

    if not unique_tables:
        return {"updated": 0, "tables": [], "saved": False}

    payload = _load_feedback_data()
    stats = payload.setdefault("table_success_stats", {})
    now_ts = float(time.time())
    updated = 0
    for t in unique_tables:
        item = stats.get(t, {})
        old_cnt = int(item.get("success_count", 0) or 0)
        old_score = float(item.get("success_score", old_cnt) or 0.0)
        weight = normalized_weights.get(t, 1.0)
        item["success_count"] = old_cnt + 1
        item["success_score"] = round(old_score + weight, 4)
        item["last_success_ts"] = now_ts
        if engine:
            item["last_engine"] = str(engine).strip().lower()
        if query:
            query_text = str(query)[:300]
            item["last_query"] = query_text
            recent_queries = item.get("recent_queries", [])
            if not isinstance(recent_queries, list):
                recent_queries = []
            # Keep a small per-table query history for query-specific retrieval.
            recent_queries = [
                str(value)[:300] for value in recent_queries
                if value and str(value) != query_text
            ]
            item["recent_queries"] = ([query_text] + recent_queries)[:8]
        item["last_row_count"] = int(row_count or 0)
        stats[t] = item
        updated += 1

    saved = _save_feedback_data(payload)
    if saved and updated:
        _invalidate_search_caches()
        logger.info(
            "schema feedback updated: %d tables, engine=%s, row_count=%s, tables=%s",
            updated, engine, row_count, ",".join(unique_tables[:8])
        )
    return {"updated": updated, "tables": unique_tables, "saved": saved}


def _scan_all_tables():
    """Return list of all table full names."""
    index = _build_table_index()
    return [e.full_name for e in index]


def load_table_schema(table_full_name):
    if table_full_name in _schema_cache:
        return _schema_cache[table_full_name]

    parts = table_full_name.split(".")
    if len(parts) != 2:
        return None
    db_name = parts[0]

    for base_dir in SCHEMA_BASE_DIRS:
        json_path = os.path.join(base_dir, db_name, "json", table_full_name + ".json")
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    schema = json.load(f)
                _schema_cache[table_full_name] = schema
                return schema
            except Exception as e:
                logger.warning("Failed to load schema %s: %s", json_path, e)
    return None


def get_table_columns(table_full_name):
    schema = load_table_schema(table_full_name)
    if schema is None:
        return None

    columns = set()
    for col in schema.get("columns", []):
        col_name = col.get("name", "").strip().lower()
        if col_name:
            columns.add(col_name)
    for col in schema.get("partition_columns", []):
        col_name = col.get("name", "").strip().lower() if isinstance(col, dict) else str(col).strip().lower()
        if col_name:
            columns.add(col_name)
    if "dt" not in columns:
        columns.add("dt")
    return columns


def get_table_columns_detail(table_full_name):
    schema = load_table_schema(table_full_name)
    if schema is None:
        return None
    result = []
    for col in schema.get("columns", []):
        result.append({
            "name": col.get("name", ""),
            "type": col.get("type", ""),
            "comment": col.get("comment", ""),
        })
    for col in schema.get("partition_columns", []):
        if isinstance(col, dict):
            result.append({
                "name": col.get("name", ""),
                "type": col.get("type", "string"),
                "comment": col.get("comment", "partition"),
            })
        else:
            result.append({"name": str(col), "type": "string", "comment": "partition"})
    return result


def _load_alias_config():
    """Load synonym_groups + term_expansions from JSON (reload if file mtime changes)."""
    global _alias_config, _alias_config_mtime
    path = SCHEMA_ALIAS_MAP_PATH
    if not path or not os.path.isfile(path):
        _alias_config = {"synonym_groups": [], "term_expansions": {}}
        _alias_config_mtime = None
        return _alias_config
    try:
        mtime = os.path.getmtime(path)
        if _alias_config is not None and _alias_config_mtime == mtime:
            return _alias_config
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _alias_config = {
            "synonym_groups": data.get("synonym_groups", []),
            "term_expansions": data.get("term_expansions", {}),
        }
        _alias_config_mtime = mtime
    except Exception as e:
        logger.warning("schema_aliases load failed: %s", e)
        _alias_config = {"synonym_groups": [], "term_expansions": {}}
    return _alias_config


def expand_query_terms(keyword: str) -> List[str]:
    """Split keyword + apply synonym_groups / term_expansions for retrieval."""
    cfg = _load_alias_config()
    raw = re.split(r'[\s,，、;；]+', (keyword or "").strip())
    tokens = [t for t in raw if t]
    expanded: Set[str] = set()
    for t in tokens:
        expanded.add(t)
        if t.isascii():
            expanded.add(t.lower())

    kw_full = (keyword or "").lower()
    for group in cfg.get("synonym_groups", []):
        if not group:
            continue
        hit = False
        for g in group:
            gs = g.lower() if g.isascii() else g
            if gs in kw_full or g in (keyword or ""):
                hit = True
                break
        if not hit:
            for g in group:
                for tok in tokens:
                    if tok == g or (tok.isascii() and g.isascii() and tok.lower() == g.lower()):
                        hit = True
                        break
                if hit:
                    break
        if hit:
            for g in group:
                expanded.add(g.lower() if g.isascii() else g)

    tex = cfg.get("term_expansions", {})
    for t in list(tokens):
        for key, vals in tex.items():
            if t == key or (t.isascii() and key.isascii() and t.lower() == key.lower()):
                seq = vals if isinstance(vals, (list, tuple)) else ([vals] if vals is not None else [])
                for x in seq:
                    if x is None:
                        continue
                    xs = str(x)
                    expanded.add(xs if not xs.isascii() else xs.lower())

    return list(expanded)


def _heuristic_table_score(entry: TableIndexEntry, terms: List[str]) -> float:
    if not terms:
        return 0.0
    score = 0.0
    fn = entry.full_name.lower()
    tn = entry.table.lower()
    tc = (entry.table_comment or "").lower()
    for k in terms:
        kl = k.lower() if k.isascii() else k
        if kl in fn:
            score += 10.0
        if kl in tn:
            score += 8.0
        if tc and kl in tc:
            score += 6.0
        col_hit_c = False
        col_hit_n = False
        for cc in entry.col_comments:
            if kl in cc.lower():
                col_hit_c = True
                break
        for cn in entry.col_names:
            if kl in cn.lower():
                col_hit_n = True
                break
        if col_hit_c:
            score += 3.0
        if col_hit_n:
            score += 2.0
    score += float(entry.table_priority) * SCHEMA_TABLE_PRIORITY_BOOST
    return score


def _detect_query_intent(keyword: str, all_terms: List[str]) -> str:
    """
    Rule-based intent classification:
    - metrics: 指标聚合
    - detail: 行为明细
    - experiment: 实验分析
    - dimension: 维度拆解
    """
    text = " ".join([keyword or ""] + (all_terms or [])).lower()
    metrics_keys = ["ctr", "点击率", "曝光", "点击", "转化率", "大盘", "趋势", "汇总", "总量", "最近", "7天", "日"]
    detail_keys = ["明细", "日志", "request", "request_id", "行为", "样本", "raw", "dtl", "log"]
    exp_keys = ["实验", "ab", "exp", "exp_id", "对照组", "实验组", "显著", "pvalue", "p-value"]
    dim_keys = ["按", "维度", "分组", "拆解", "渠道", "内容", "版本", "recall_type", "scene_id", "item_id"]

    def _count(keys: List[str]) -> int:
        return sum(1 for k in keys if k in text)

    scores = {
        "metrics": _count(metrics_keys),
        "detail": _count(detail_keys),
        "experiment": _count(exp_keys),
        "dimension": _count(dim_keys),
    }
    # Default to metrics for short KPI-like questions.
    return max(scores.items(), key=lambda x: x[1])[0] if any(scores.values()) else "metrics"


def _rewrite_query_terms(keyword: str, intent: str, all_terms: List[str]) -> List[str]:
    """
    Lightweight query rewrite (no external model):
    add canonical retrieval hints by intent and scene words.
    """
    text = (keyword or "").lower()
    extra: List[str] = []

    if intent == "metrics":
        extra += ["指标", "聚合", "summary", "metrics", "total", "ctr", "views", "clicks"]
    elif intent == "detail":
        extra += ["明细", "日志", "detail", "log", "request_id", "is_view", "is_click"]
    elif intent == "experiment":
        extra += ["实验", "ab", "exp_list", "exp_id", "对照组", "实验组"]
    elif intent == "dimension":
        extra += ["分组", "维度", "group by", "scene_id", "recall_type", "item_id"]

    # KPI phrase expansion
    if ("曝光" in text or "click" in text or "点击" in text) and ("ctr" in text or "点击率" in text):
        extra += ["views", "clicks", "ctr", "曝光量", "点击量", "点击率"]
    if "大盘" in text:
        extra += ["total", "summary", "汇总", "总表"]

    merged = list(dict.fromkeys((all_terms or []) + extra))
    return merged


def _table_type_feature_score(entry: TableIndexEntry, intent: str) -> float:
    """
    Intent-aware table type scoring (generic):
    - metrics intent boosts *_metrics_* / total / ctr/views/clicks columns
    - detail intent boosts behavior/log/dtl/request_id/is_view/is_click
    - experiment intent boosts exp/ab features
    - dimension intent boosts *_mapid / scene_id / recall_type / groupable fields
    """
    tn = entry.table.lower()
    fn = entry.full_name.lower()
    cols = set([c.lower() for c in (entry.col_names or [])])
    score = 0.0

    metrics_name_keys = ["metrics", "total", "summary", "ratio", "ctr"]
    metrics_col_keys = ["ctr", "views", "clicks", "reco", "total_view", "total_click"]
    detail_name_keys = ["behavior", "log", "dtl", "detail"]
    detail_col_keys = ["request_id", "user_id", "is_view", "is_click", "is_reco"]
    exp_name_keys = ["ab", "exp", "experiment"]
    exp_col_keys = ["exp", "exp_list", "exp_id", "group"]
    dim_name_keys = ["mapid", "dim", "dimension"]
    dim_col_keys = ["scene_id", "recall_type", "item_id", "app_version", "channel"]

    if intent == "metrics":
        score += sum(2.2 for k in metrics_name_keys if k in tn or k in fn)
        score += sum(1.5 for k in metrics_col_keys if any(k in c for c in cols))
        score -= sum(1.2 for k in detail_name_keys if k in tn or k in fn)
    elif intent == "detail":
        score += sum(2.2 for k in detail_name_keys if k in tn or k in fn)
        score += sum(1.5 for k in detail_col_keys if any(k in c for c in cols))
        score -= sum(1.0 for k in metrics_name_keys if k in tn or k in fn)
    elif intent == "experiment":
        score += sum(2.0 for k in exp_name_keys if k in tn or k in fn)
        score += sum(1.6 for k in exp_col_keys if any(k in c for c in cols))
    elif intent == "dimension":
        score += sum(1.8 for k in dim_name_keys if k in tn or k in fn)
        score += sum(1.4 for k in dim_col_keys if any(k in c for c in cols))

    return score


def _search_tables_ranked(keyword, limit=20):
    """Return ranked tuples: (final_score, entry, base_mix, rerank_bonus, intent, confidence_gap)."""
    index = _build_table_index()
    terms = expand_query_terms(keyword)
    kw_tokens = [t for t in tokenize(keyword or "") if len(t) >= 2]
    all_terms = list(dict.fromkeys((terms or []) + kw_tokens))
    if not all_terms:
        return []

    intent = _detect_query_intent(keyword or "", all_terms)
    all_terms = _rewrite_query_terms(keyword or "", intent, all_terms)

    bm25_w = SCHEMA_SEARCH_BM25_WEIGHT if SCHEMA_SEARCH_USE_BM25 and _bm25_index else 0.0
    qtoks = []
    for t in all_terms:
        qtoks.extend(tokenize(t))
    qtoks = list(dict.fromkeys(qtoks))

    cand_ids: Set[int] = set()
    bm25_scores: Dict[int, float] = {}
    if bm25_w > 0 and _bm25_index and qtoks:
        ranked = _bm25_index.search(qtoks, topn=max(limit * 6, limit))
        for doc_id, sc in ranked:
            cand_ids.add(doc_id)
            bm25_scores[doc_id] = sc

    heur_scores: Dict[int, float] = {}
    for i, entry in enumerate(index):
        hs = _heuristic_table_score(entry, all_terms)
        if hs > 0:
            cand_ids.add(i)
            heur_scores[i] = hs

    if not cand_ids:
        for i, entry in enumerate(index):
            cand_ids.add(i)
            heur_scores[i] = _heuristic_table_score(entry, all_terms)

    for i in cand_ids:
        if i not in heur_scores:
            heur_scores[i] = _heuristic_table_score(index[i], all_terms)

    max_b = max(bm25_scores.values()) if bm25_scores else 0.0
    max_h = max(heur_scores.values()) if heur_scores else 0.0
    if max_h <= 0:
        max_h = 1e-6

    combined: List[Tuple[float, int, TableIndexEntry, float]] = []
    for i in cand_ids:
        entry = index[i]
        nb = (bm25_scores.get(i, 0.0) / max_b) if max_b > 0 else 0.0
        nh = heur_scores.get(i, 0.0) / max_h
        if bm25_w >= 1.0:
            mix = nb
        elif bm25_w <= 0.0:
            mix = nh
        else:
            mix = bm25_w * nb + (1.0 - bm25_w) * nh
        rerank_bonus = _table_type_feature_score(entry, intent)
        # Two-stage rerank: base mix + intent-aware bonus (small, stable).
        final_score = mix + 0.08 * rerank_bonus
        combined.append((final_score, i, entry, rerank_bonus))

    combined.sort(key=lambda x: (-x[0], x[2].full_name))
    # Confidence by top1-top2 gap after rerank
    gap = 0.0
    if len(combined) >= 2:
        gap = combined[0][0] - combined[1][0]
    ranked = [(sc, e, sc - 0.08 * rb, rb, intent, gap) for sc, _, e, rb in combined[:limit]]
    return ranked


def search_tables(keyword, limit=20):
    """Hybrid BM25 + heuristic + intent-aware rerank."""
    ranked = _search_tables_ranked(keyword, limit=limit)
    return [e.full_name for _, e, __, ___, ____, _____ in ranked]


def get_table_ddl(table_full_name):
    schema = load_table_schema(table_full_name)
    if schema is None:
        return None
    return schema.get("create_statement", None)


def build_table_catalog(catalog_mode=None):
    """
    为 LLM 注入的表目录文本（按 SCHEMA_CATALOG_MODE 控制 token）：
    - minimal: 仅 db.table（最省）
    - standard: 表名 + 表注释一行
    - full: 表注释 + 列注释摘要（与原行为接近）
    """
    global _table_catalog_cache
    mode = (catalog_mode or SCHEMA_CATALOG_MODE or "minimal").lower()
    if mode not in ("minimal", "standard", "full"):
        mode = "minimal"

    allowed = _allowed_db_filter()
    cache_key = (mode, tuple(sorted(allowed)) if allowed else ())
    if cache_key in _table_catalog_cache:
        return _table_catalog_cache[cache_key]

    index = _build_table_index()
    lines = []
    current_db = None
    for entry in index:
        if entry.db != current_db:
            current_db = entry.db
            lines.append("\n## {}".format(current_db))

        if mode == "minimal":
            lines.append("  {}".format(entry.full_name))
            continue

        comment_part = ""
        if entry.table_comment:
            comment_part = " -- {}".format(entry.table_comment)
        elif mode == "full":
            key_comments = [c for c in entry.col_comments[:5] if c]
            if key_comments:
                comment_part = " -- 含: {}".format(", ".join(key_comments[:3]))

        if mode == "standard" and not comment_part and entry.table_priority:
            comment_part = " -- priority={}".format(entry.table_priority)

        lines.append("  {}{}".format(entry.full_name, comment_part))

    text = "\n".join(lines)
    _table_catalog_cache[cache_key] = text
    logger.info("Table catalog [%s]: %d chars, %d tables", mode, len(text), len(index))
    return text


def skill_search_tables(keyword, limit=10):
    """Agent skill: search tables by keyword (BM25+启发式+同义词扩展)."""
    ranked = _search_tables_ranked(keyword, limit=limit)
    tables = [e.full_name for _, e, __, ___, ____, _____ in ranked]
    if not tables:
        sub_keywords = re.split(r'[\s_]+', (keyword or "").lower())
        sub_keywords = [k for k in sub_keywords if len(k) >= 2]
        for sub_kw in sub_keywords:
            ranked = _search_tables_ranked(sub_kw, limit=limit)
            tables = [e.full_name for _, e, __, ___, ____, _____ in ranked]
            if tables:
                break

    result_details = []
    rank_map = {e.full_name: (score, base, bonus, intent, gap)
                for score, e, base, bonus, intent, gap in ranked}
    for t in tables:
        schema = load_table_schema(t)
        comment = ""
        if schema:
            comment = _extract_table_comment(schema)
            if not comment:
                cols = schema.get("columns", [])[:5]
                col_hints = ["{} ({})".format(c.get("name", ""), c.get("comment", "")) for c in cols if c.get("comment")]
                if col_hints:
                    comment = "主要字段: " + ", ".join(col_hints)
        rank_info = rank_map.get(t, (0.0, 0.0, 0.0, "metrics", 0.0))
        result_details.append({
            "table": t,
            "comment": comment,
            "score": round(rank_info[0], 4),
            "base_mix": round(rank_info[1], 4),
            "rerank_bonus": round(rank_info[2], 4),
        })

    exp = expand_query_terms(keyword)
    intent = ranked[0][4] if ranked else _detect_query_intent(keyword or "", exp)
    confidence_gap = ranked[0][5] if ranked else 0.0
    low_confidence = confidence_gap < 0.06
    hint = "对候选表务必 get_table_info；列不全时再 full_detail=true"
    if low_confidence:
        hint = "Top1/Top2 分差较小，建议二次检索或澄清（如：要大盘指标表还是行为明细表）"
    return {
        "tables": result_details,
        "count": len(result_details),
        "expanded_terms": exp[:25],
        "intent": intent,
        "confidence_gap": round(confidence_gap, 4),
        "hint": hint,
    }


def _build_column_summary(detail, schema, max_cols=40):
    """分区列 + 高价值列优先，支持 JSON 列上可选 tier=core / primary。"""
    if not detail:
        return [], {"truncated": False, "total_columns": 0}

    part_names = set()
    if schema:
        for col in schema.get("partition_columns", []):
            if isinstance(col, dict):
                part_names.add(col.get("name", "").strip())
            else:
                part_names.add(str(col).strip())

    raw_cols = schema.get("columns", []) if schema else []
    tier_by_name = {}
    for rc in raw_cols:
        nm = rc.get("name", "").strip()
        tier = (rc.get("tier") or rc.get("tag") or "").strip().lower()
        if nm:
            tier_by_name[nm] = tier

    hints = ("id", "dt", "time", "date", "cnt", "num", "amt", "amount", "fee", "rate", "gmv", "pay", "imp", "uv", "pv", "sku", "user")

    scored = []
    for i, c in enumerate(detail):
        name = c.get("name", "")
        comment = (c.get("comment", "") or "").lower()
        nlow = name.lower()
        is_part = name in part_names or "partition" in (c.get("comment") or "").lower()
        tier = tier_by_name.get(name, "")
        if is_part:
            pri = 1000
        elif tier in ("core", "primary", "key"):
            pri = 500
        elif any(h in nlow or h in comment for h in hints):
            pri = 200
        else:
            pri = 50 + i
        scored.append((pri, i, c))

    scored.sort(key=lambda x: (-x[0], x[1]))
    picked = [x[2] for x in scored[:max_cols]]
    truncated = len(detail) > len(picked)
    return picked, {"truncated": truncated, "total_columns": len(detail)}


def skill_get_table_info(table_name, full_detail=None):
    """
    Agent skill: 表结构。默认摘要列（省 token）；full_detail=true 返回全部列。
    full_detail 缺省时使用 SCHEMA_GET_TABLE_INFO_DEFAULT_FULL。
    """
    if full_detail is None:
        full_detail = SCHEMA_GET_TABLE_INFO_DEFAULT_FULL

    detail = get_table_columns_detail(table_name)
    if detail is None:
        all_tables = _scan_all_tables()
        partial = table_name.lower()
        candidates = [t for t in all_tables if partial in t.lower()][:5]
        return {
            "error": "table '{}' not found".format(table_name),
            "suggestions": candidates,
        }

    schema = load_table_schema(table_name)
    if full_detail:
        return {
            "table": table_name,
            "comment": schema.get("table_comment", "") if schema else "",
            "columns": detail,
            "column_count": len(detail),
            "full_detail": True,
        }

    summary, meta = _build_column_summary(detail, schema, max_cols=40)
    out = {
        "table": table_name,
        "comment": schema.get("table_comment", "") if schema else "",
        "columns": summary,
        "column_count": len(summary),
        "full_detail": False,
        "schema_meta": meta,
    }
    if meta.get("truncated"):
        out["hint"] = (
            "尚有 {} 列未列出；需要 JOIN/全字段时请再调用 get_table_info(table_name=\"{}\", full_detail=true)"
        ).format(meta["total_columns"] - len(summary), table_name)
    return out


SQL_KEYWORDS = {
    'select', 'from', 'where', 'and', 'or', 'not', 'in', 'on', 'join',
    'inner', 'left', 'right', 'full', 'outer', 'cross', 'group', 'order',
    'by', 'having', 'limit', 'union', 'all', 'insert', 'into', 'values',
    'update', 'set', 'delete', 'create', 'drop', 'alter', 'table',
    'with', 'as', 'case', 'when', 'then', 'else', 'end', 'between',
    'like', 'is', 'null', 'true', 'false', 'cast', 'distinct', 'exists',
    'asc', 'desc', 'offset', 'fetch', 'partition', 'over', 'window',
}


def extract_tables_from_sql(sql):
    pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return list(set(m.lower() for m in matches))


def extract_table_hits_from_sql(sql: str) -> List[Tuple[str, str]]:
    """
    Ordered table hits with role:
    - from: main source table(s)
    - join: joined table(s)
    """
    text = sql or ""
    pattern = re.compile(
        r'\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
        re.IGNORECASE
    )
    hits: List[Tuple[str, str]] = []
    for kw, table in pattern.findall(text):
        role = "from" if str(kw).lower() == "from" else "join"
        hits.append((_normalize_table_name(table), role))
    return hits


def record_success_feedback_from_sql(sql: str, query: str = "", engine: str = "", row_count: int = 0) -> Dict:
    hits = extract_table_hits_from_sql(sql or "")
    if not hits:
        return {"updated": 0, "tables": [], "saved": False}
    tables = []
    weights: Dict[str, float] = {}
    seen = set()
    for table, role in hits:
        if table not in seen:
            tables.append(table)
            seen.add(table)
        # Main FROM tables get strong reinforcement; JOIN tables get weak reinforcement.
        weights[table] = max(weights.get(table, 0.0), 1.0 if role == "from" else 0.35)
    return record_table_success_feedback(
        tables=tables,
        query=query,
        engine=engine,
        row_count=row_count,
        table_weights=weights,
    )


def get_schema_feedback_stats(limit: int = 50) -> Dict:
    """Read-only stats for UI/debugging of schema feedback learning."""
    n = max(1, min(int(limit or 50), 500))
    payload = _load_feedback_data()
    stats = payload.get("table_success_stats", {}) if isinstance(payload, dict) else {}
    items = []
    total_score = 0.0
    total_count = 0
    for table, item in stats.items():
        if not isinstance(item, dict):
            continue
        try:
            count = int(item.get("success_count", 0) or 0)
        except (TypeError, ValueError):
            count = 0
        try:
            score = float(item.get("success_score", count) or 0.0)
        except (TypeError, ValueError):
            score = float(count)
        try:
            ts = float(item.get("last_success_ts", 0) or 0.0)
        except (TypeError, ValueError):
            ts = 0.0
        boost = _feedback_priority_boost(table)
        total_score += score
        total_count += max(count, 0)
        items.append({
            "table": table,
            "success_count": count,
            "success_score": round(score, 4),
            "priority_boost": round(boost, 4),
            "last_success_ts": ts,
            "last_engine": item.get("last_engine", "") or "",
            "last_query": item.get("last_query", "") or "",
            "recent_queries": item.get("recent_queries", [])
            if isinstance(item.get("recent_queries", []), list) else [],
            "last_row_count": int(item.get("last_row_count", 0) or 0),
        })

    items.sort(key=lambda x: (-x["success_score"], -x["last_success_ts"], x["table"]))
    top_items = items[:n]
    max_score = top_items[0]["success_score"] if top_items else 0.0
    now_ts = float(time.time())
    for x in top_items:
        age_sec = int(max(0, now_ts - (x["last_success_ts"] or 0.0))) if x["last_success_ts"] else None
        x["age_seconds"] = age_sec
        x["score_pct"] = int(round((x["success_score"] / max_score) * 100)) if max_score > 0 else 0

    return {
        "feedback_file": _SCHEMA_FEEDBACK_PATH,
        "total_tables": len(items),
        "total_success_count": total_count,
        "total_success_score": round(total_score, 4),
        "items": top_items,
    }


def find_query_related_feedback_tables(query: str, limit: int = 5) -> List[Dict]:
    """
    Return tables learned from semantically similar successful queries.

    This is intentionally separate from the global table-priority score. Callers
    opt in (currently only the complex-funnel extension), so normal ranking stays
    backward compatible.
    """
    n = max(0, min(int(limit or 0), 20))
    if n == 0 or not str(query or "").strip():
        return []
    query_tokens = {token for token in tokenize(query) if len(token) >= 2}
    if not query_tokens:
        return []

    payload = _load_feedback_data()
    stats = payload.get("table_success_stats", {}) if isinstance(payload, dict) else {}
    ranked = []
    for table, item in stats.items():
        if not isinstance(item, dict) or load_table_schema(table) is None:
            continue
        try:
            success_score = float(item.get("success_score", 0) or 0.0)
        except (TypeError, ValueError):
            success_score = 0.0
        # A single diagnostic/accidental query is too weak to become a candidate.
        if success_score < 2.5:
            continue
        history = item.get("recent_queries", [])
        if not isinstance(history, list):
            history = []
        last_query = item.get("last_query", "") or ""
        if last_query and last_query not in history:
            history = [last_query] + history

        best_score = 0.0
        best_query = ""
        for old_query in history[:8]:
            if re.search(r"核验|枚举|抽样|表结构|describe\b|show\b", str(old_query), re.IGNORECASE):
                continue
            old_tokens = {token for token in tokenize(str(old_query)) if len(token) >= 2}
            if not old_tokens:
                continue
            common = query_tokens & old_tokens
            if not common:
                continue
            coverage = len(common) / float(max(1, min(len(query_tokens), len(old_tokens))))
            jaccard = len(common) / float(max(1, len(query_tokens | old_tokens)))
            score = 0.7 * coverage + 0.3 * jaccard
            if score > best_score:
                best_score = score
                best_query = str(old_query)
        # One generic shared phrase should not influence retrieval.
        if best_score < 0.12:
            continue
        ranked.append({
            "table": table,
            "similarity": round(best_score, 4),
            "last_query": best_query[:300],
            "success_score": success_score,
        })

    ranked.sort(key=lambda x: (-x["similarity"], -x["success_score"], x["table"]))
    return ranked[:n]


def list_unknown_tables_in_sql(sql):
    """生成后校验：SQL 中出现的 db.table 是否在本地 Schema 索引中。"""
    unknown = []
    for t in extract_tables_from_sql(sql):
        if load_table_schema(t) is None:
            unknown.append(t)
    return unknown


def _find_table_alias(sql, table_full_name):
    pattern = re.compile(
        r'(?:FROM|JOIN)\s+' + re.escape(table_full_name) + r'\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)',
        re.IGNORECASE
    )
    m = pattern.search(sql)
    if m:
        candidate = m.group(1)
        if candidate.lower() not in SQL_KEYWORDS:
            return candidate
    return None


def _find_alias_sources(sql, alias):
    """Return all FROM/JOIN sources bound to an alias in this statement."""
    if not sql or not alias:
        return set()
    clean = re.sub(r'/\*[\s\S]*?\*/', ' ', sql)
    clean = re.sub(r'--[^\n\r]*', ' ', clean)
    source_alias_pattern = re.compile(
        r'\b(?:FROM|JOIN)\s+'
        r'([a-zA-Z_][a-zA-Z0-9_$]*(?:\.[a-zA-Z_][a-zA-Z0-9_$]*){0,2})'
        r'\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)',
        re.IGNORECASE,
    )
    sources = set()
    for match in source_alias_pattern.finditer(clean):
        bound_alias = match.group(2)
        if bound_alias.lower() == alias.lower() and bound_alias.lower() not in SQL_KEYWORDS:
            sources.add(match.group(1).lower())
    return sources


def _alias_uniquely_binds_table(sql, alias, table_full_name):
    """Guard global alias rewrites against CTE/nested-scope alias reuse."""
    sources = _find_alias_sources(sql, alias)
    if len(sources) != 1:
        return False
    source = next(iter(sources))
    table_name = table_full_name.lower()
    return (
        source == table_name
        or source.endswith('.' + table_name)
        or table_name.endswith('.' + source)
    )


def _find_best_column_match(wrong_col, valid_columns):
    """给错误/缺失列名找一个"最接近"的真实列名。

    注意：必须跳过过短的候选，否则 "u"(表别名) 会被当成 "button_state" 的子串匹配，
    把 `WHERE u = ...` 里的表别名误改为 `WHERE button_state = ...`。
    """
    wc = wrong_col.lower()
    if wc in valid_columns:
        return None
    if len(wc) < 3:
        return None
    no_underscore = wc.replace("_", "")
    for vc in valid_columns:
        if vc.replace("_", "") == no_underscore:
            return vc
    for vc in valid_columns:
        vcl = vc.lower()
        if len(vcl) < 3:
            continue
        if wc in vcl or vcl in wc:
            if len(wc) <= 3 and len(vcl) > len(wc) * 3:
                continue
            return vc
    return None


def fix_columns_by_schema(sql):
    tables = extract_tables_from_sql(sql)
    if not tables:
        return None

    fixed = sql
    changed = False
    multi_table = len(tables) > 1

    for table_full in tables:
        columns = get_table_columns(table_full)
        if columns is None:
            continue
        alias = _find_table_alias(sql, table_full)

        if alias:
            if not _alias_uniquely_binds_table(sql, alias, table_full):
                logger.warning(
                    "schema fix skipped ambiguous alias: alias=%s table=%s sources=%s",
                    alias,
                    table_full,
                    sorted(_find_alias_sources(sql, alias)),
                )
                continue
            pattern = re.compile(
                r'(?<![a-zA-Z0-9_])' + re.escape(alias) + r'\.([a-zA-Z_][a-zA-Z0-9_]*)',
                re.IGNORECASE
            )
            for m in pattern.finditer(fixed):
                ref_col = m.group(1).lower()
                if ref_col not in columns:
                    best = _find_best_column_match(ref_col, columns)
                    if best:
                        old_ref = alias + "." + m.group(1)
                        new_ref = alias + "." + best
                        p2 = re.compile(
                            r'(?<![a-zA-Z0-9_])' + re.escape(old_ref) + r'(?![a-zA-Z0-9_])',
                            re.IGNORECASE
                        )
                        if p2.search(fixed):
                            fixed = p2.sub(new_ref, fixed)
                            changed = True
                            logger.info("schema fix: %s -> %s", old_ref, new_ref)

        if not multi_table and not alias:
            bare_col_pattern = re.compile(
                r'(?:WHERE|AND|OR|SELECT|,|ON)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:IN|=|>|<|>=|<=|!=|LIKE|IS|BETWEEN)',
                re.IGNORECASE
            )
            for m in bare_col_pattern.finditer(fixed):
                col_name = m.group(1).lower()
                if col_name in SQL_KEYWORDS:
                    continue
                if len(col_name) <= 2:
                    continue
                if col_name not in columns:
                    best = _find_best_column_match(col_name, columns)
                    if best:
                        col_p = re.compile(
                            r'(?<![a-zA-Z0-9_.])' + re.escape(col_name) + r'(?![a-zA-Z0-9_])',
                            re.IGNORECASE
                        )
                        fixed = col_p.sub(best, fixed)
                        changed = True
                        logger.info("schema fix: %s -> %s", col_name, best)

    return fixed if changed else None
