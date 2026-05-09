#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全局配置: LLM、SQL引擎、Schema路径等

路径约定（开源仓库 Schemist/）::
  - 本文件位于 ai_service_2/config/settings.py
  - 仓库根 = 上级目录的上级：`../..` relative to config → Schemist 根目录
  - 默认本地知识库：`../hivebrain`（与各库 ``<db>/json/*.json`` 结构一致）
密钥与租户地址 **禁止**写死在仓库中；请使用环境变量或 `ai_service_2/.env`（参见 `.env.example`）。
"""
import json
import os
from typing import List, Optional

# 仓库根目录（Schemist/）：含 hivebrain、ai_service_2 等
_SERVICE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_SERVICE_ROOT)
# 与历史变量名 SERVICE_DIR 一致，指向 ai_service_2 目录
SERVICE_DIR = _SERVICE_ROOT


def _parse_schema_base_dirs() -> List[str]:
    """
    Schema 检索目录列表。环境变量 SCHEMA_BASE_DIRS 可覆盖（分号分隔多个路径）。
    默认：<repo>/hivebrain 与 <repo>/table_filter_schemas（结构与 hivebrain 相同：``<db>/json/*.json``）。
    """
    raw = (os.environ.get("SCHEMA_BASE_DIRS") or "").strip()
    if raw:
        return [p.strip() for p in raw.replace("\n", ";").split(";") if p.strip()]
    return [
        os.path.join(_REPO_ROOT, "hivebrain"),
        os.path.join(_REPO_ROOT, "table_filter_schemas"),
    ]


SCHEMA_BASE_DIRS = _parse_schema_base_dirs()


# ======================== LLM 配置 ========================
# OpenAI 兼容 Chat Completions 完整 URL（末尾可带 /chat/completions）；须与网关一致
LLM_API_URL = os.environ.get("LLM_API_URL", "").strip()
LLM_API_KEY = os.environ.get("LLM_API_KEY", "").strip()
# 默认模型；可用环境变量 LLM_MODEL 覆盖（须与下方可选列表中的 id 一致）
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini").strip()
LLM_TEMPERATURE = 0.3
LLM_MAX_TOKENS = 4000
LLM_TIMEOUT = 120


def _default_llm_model_choices():
    # (模型 id, 界面展示名) — 可用 LLM_MODEL_CHOICES_JSON 覆盖为自建网关路由
    return [
        ("gpt-4o-mini", "GPT-4o mini"),
        ("gpt-4o", "GPT-4o"),
        ("gpt-4-turbo", "GPT-4 Turbo"),
    ]


def _load_llm_model_choices():
    """
    下拉可选模型列表。可用环境变量 LLM_MODEL_CHOICES_JSON 覆盖，例如:
    [["moonshotai/kimi-k2.5","Kimi"],["deepseek-ai/DeepSeek-V3-0324","DeepSeek"]]
    或 [{"id":"...","label":"..."}, ...]
    """
    raw = (os.environ.get("LLM_MODEL_CHOICES_JSON") or "").strip()
    if not raw:
        return _default_llm_model_choices()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _default_llm_model_choices()
    out = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                out.append((str(item[0]).strip(), str(item[1]).strip()))
            elif isinstance(item, dict) and item.get("id"):
                out.append((str(item["id"]).strip(), str(item.get("label") or item["id"]).strip()))
    return out if out else _default_llm_model_choices()


LLM_MODEL_CHOICES = _load_llm_model_choices()


def resolve_llm_model(requested: Optional[str]) -> str:
    """
    解析请求中的 llm_model：空则用 LLM_MODEL；否则必须在 LLM_MODEL_CHOICES 的 id 中。
    非法值抛出 ValueError（由 API 转为 400）。
    """
    if requested is None or not str(requested).strip():
        rid = LLM_MODEL
    else:
        rid = str(requested).strip()
    allowed = {pair[0] for pair in LLM_MODEL_CHOICES}
    if rid not in allowed:
        raise ValueError(
            "不支持的 llm_model: {}。可选: {}".format(rid, ", ".join(sorted(allowed)))
        )
    return rid

# ======================== SQL 引擎配置 ========================
SQL_ENGINE_DEFAULT = "spark"  # "spark" 或 "trino"

# Spark SQL
SPARK_SQL_CMD = "spark-sql"
SPARK_EXECUTOR_MEMORY = "4g"
SPARK_EXECUTOR_CORES = "2"
SPARK_EXECUTOR_INSTANCES = "10"
SPARK_DRIVER_MEMORY = "6g"
SPARK_QUEUE = os.environ.get("SPARK_QUEUE", "default").strip() or "default"
# Dynamic Allocation（等价于 spark-submit 的 --conf spark.dynamicAllocation.*）
# 默认关闭，与 sql_executor 原先行为一致。强烈建议线上开启，让 yarn 按需扩 executor，
# 处理 11 天以上窗口 / 多表 join / GroupBy DISTINCT 等重负载。开启示例:
#   export SPARK_DYNAMIC_ALLOCATION_ENABLED=true
#   export SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=80
SPARK_DYNAMIC_ALLOCATION_ENABLED = (
    os.environ.get("SPARK_DYNAMIC_ALLOCATION_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
)
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = (
    os.environ.get("SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS") or ""
).strip()
# 任意额外 --conf，分号或换行分隔。这里设置的是 spark-submit 启动级别 conf，例如:
#   export SPARK_EXTRA_CONFS='spark.dynamicAllocation.minExecutors=8;spark.speculation=true'
# 注意：SQL 级别的 spark.sql.* tuning 推荐改用 SPARK_SQL_SET_OVERRIDES（per-query 注入 SET）。
_SPARK_EXTRA_RAW = (os.environ.get("SPARK_EXTRA_CONFS") or "").strip()


def _parse_spark_extra_confs(raw: str):
    if not raw:
        return []
    pairs = []
    for part in raw.replace("\r\n", "\n").replace("\r", "\n").replace("\n", ";").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, _, v = part.partition("=")
        k, v = k.strip(), v.strip()
        if k and v is not None:
            pairs.append((k, v))
    return pairs


SPARK_EXTRA_CONFS = _parse_spark_extra_confs(_SPARK_EXTRA_RAW)

# SQL 执行超时（秒）：客户端 wait subprocess.communicate / Trino fetch 的硬上限
# 历史默认 300s 对 11 天以上窗口的明细 JOIN 易超时，且 spark-sql CLI 冷启动占 ~30s。
# 通过 SQL_EXECUTOR_TIMEOUT 可整体调大；前端 / 飞书 / sql_executor 三处共享同一默认值。
#
# 相关环境变量统一说明：
#   SQL_EXECUTOR_TIMEOUT          客户端等 spark-sql/Trino 完成的硬超时（秒），默认 900；最低 60。
#   SPARK_DYNAMIC_ALLOCATION_ENABLED / SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS
#                                 见上方 Dynamic Allocation 说明。
#   SPARK_EXTRA_CONFS             启动级 --conf。形如 'spark.speculation=true;spark.dynamicAllocation.minExecutors=8'
#   SPARK_SQL_SET_OVERRIDES       per-query SET 覆盖。形如 'spark.sql.autoBroadcastJoinThreshold=128m;spark.sql.shuffle.partitions=600'
#                                 sql_executor 已默认注入（仅 SQL 可调项）：
#                                   - spark.sql.autoBroadcastJoinThreshold=256m   (注意：与 driver memory 配套；过大会 OOM)
#                                   - spark.sql.shuffle.partitions=400
#                                   - spark.sql.adaptive.enabled=true
#                                   - spark.sql.adaptive.coalescePartitions.enabled=true
#                                   - spark.sql.adaptive.skewJoin.enabled=true
#                                 静态配置（如 spark.driver.maxResultSize）不能写在 SET 里，请用 SPARK_EXTRA_CONFS，例如:
#                                   SPARK_EXTRA_CONFS='spark.driver.maxResultSize=2g'
#                                 通过此变量可覆盖任意上述项（按 ; 或换行分隔）。
def _read_sql_executor_timeout() -> int:
    raw = (os.environ.get("SQL_EXECUTOR_TIMEOUT") or "").strip()
    if not raw:
        return 900
    try:
        v = int(raw)
        return max(60, v)
    except ValueError:
        return 900


SQL_EXECUTOR_TIMEOUT = _read_sql_executor_timeout()

# Trino（凭据勿提交仓库）
def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost").strip()
TRINO_PORT = _env_int("TRINO_PORT", 8080)
TRINO_USER = os.environ.get("TRINO_USER", "schemist").strip()
TRINO_PASSWORD = os.environ.get("TRINO_PASSWORD", "").strip()
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "hive").strip() or "hive"
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "default").strip() or "default"

# ======================== Schema 配置 ========================
# SCHEMA_BASE_DIRS 见文件顶部 `_parse_schema_base_dirs()`

# 仅索引这些库；空列表表示不过滤（全部库）
SCHEMA_ALLOWED_DBS = []  # 例: ["dwd", "dim"]

# 注入 System Prompt 的表目录粒度: minimal=仅 db.table；standard=表注释一行；full=表注释+列注释摘要（最耗 token）
SCHEMA_CATALOG_MODE = "minimal"

# 业务同义词 / 检索扩展（JSON）；不存在则忽略
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_ALIAS_MAP_PATH = os.path.join(_CONFIG_DIR, "schema_aliases.json")

# 表检索: True=BM25+倒排+启发式混合；False=仅原启发式线性扫描
SCHEMA_SEARCH_USE_BM25 = True

# BM25 与启发式混合权重 (0~1)，越大越信 BM25
SCHEMA_SEARCH_BM25_WEIGHT = 0.55

# JSON 中可选字段 table_priority（整数），检索时加分: score += priority * SCHEMA_TABLE_PRIORITY_BOOST
SCHEMA_TABLE_PRIORITY_BOOST = 0.35

# get_table_info 默认是否返回全列；False 时默认摘要列（省 token），需全列时显式 full_detail=true
SCHEMA_GET_TABLE_INFO_DEFAULT_FULL = False

# 可选：预计算表向量索引 JSON 的路径（未实现加载器时保留占位）
SCHEMA_VECTOR_INDEX_PATH = ""

# SQL Agent 传入 DeepAgent 的历史：最多保留几条 user/assistant（不含当前轮）；0 表示不截断条数仅截断长度
SQL_AGENT_HISTORY_MAX_TURNS = 4
# 单条 history 内容超过该字符则截断（避免 tool 大 payload 撑爆上下文）
SQL_AGENT_HISTORY_MAX_CHARS = 3500

# ======================== 服务配置 ========================
SERVICE_HOST = "0.0.0.0"
SERVICE_PORT = 8889
# 对外可访问的根 URL（含协议与端口），用于飞书等场景拼完整分享链接；未设则只返回相对路径 /shared/xxx
SERVICE_PUBLIC_ORIGIN = (
    os.environ.get("SERVICE_PUBLIC_ORIGIN") or os.environ.get("SQL_AI_PUBLIC_BASE_URL") or ""
).strip().rstrip("/")
# 临时 SQL/CSV：默认可写目录为 ai_service_2 下，也可用 SCHEMIST_WORKSPACE_DIR 指定（如 Docker 挂载卷）
WORKSPACE_DIR = (
    os.environ.get("SCHEMIST_WORKSPACE_DIR") or os.environ.get("SQL_AI_WORKSPACE_DIR") or ""
).strip() or _SERVICE_ROOT

TEMP_SQL_DIR = os.path.join(WORKSPACE_DIR, "temp_sql_v2")
TEMP_CSV_DIR = os.path.join(TEMP_SQL_DIR, "csv")

# ======================== 查询结果规模（统一上限） ========================
# SQL 执行默认 max_rows、前端结果表分页预览、数据分析报告样本、分享页嵌入表格行数等均使用此值。
# 覆盖示例: export QUERY_RESULT_MAX_ROWS=3000
QUERY_RESULT_MAX_ROWS = int(os.environ.get("QUERY_RESULT_MAX_ROWS", "2000"))
