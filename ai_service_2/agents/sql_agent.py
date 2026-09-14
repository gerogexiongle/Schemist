#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQL generation agent: uses DeepAgent + schema skills to generate SQL"""
import json
import logging
import re
import time

from agents.base_agent import DeepAgent, Skill
from skills.schema_skill import (
    skill_search_tables,
    skill_get_table_info,
    fix_columns_by_schema,
    extract_tables_from_sql,
    list_unknown_tables_in_sql,
)
from config.settings import (
    SQL_AGENT_CATALOG_TOP_N,
    SQL_AGENT_COMPLEX_GET_TABLE_INFO_LIMIT,
    SQL_AGENT_COMPLEX_MAX_TOOL_ROUNDS,
    SQL_AGENT_COMPLEX_MAX_TOTAL_TOOL_CALLS,
    SQL_AGENT_COMPLEX_SEARCH_TABLES_LIMIT,
    SQL_AGENT_COMPLEX_TABLE_THRESHOLD,
    SQL_AGENT_FALLBACK_MODEL,
    SQL_AGENT_FALLBACK_MAX_RETRIES,
    SQL_AGENT_FALLBACK_TIMEOUT,
    SQL_AGENT_FINAL_FALLBACK_TIMEOUT,
    SQL_AGENT_GET_TABLE_INFO_LIMIT,
    SQL_AGENT_HISTORY_MAX_CHARS,
    SQL_AGENT_HISTORY_MAX_TURNS,
    SQL_AGENT_MAX_TOOL_ROUNDS,
    SQL_AGENT_MAX_TOTAL_TOOL_CALLS,
    SQL_AGENT_REPAIR_FALLBACK_TIMEOUT,
    SQL_AGENT_SEARCH_TABLES_LIMIT,
    SQL_AGENT_STRONG_MODELS,
)
from skills.complex_query_retrieval import (
    build_complex_query_plan,
    build_complex_retrieval_context,
    build_semantic_recovery_context,
    evaluate_semantic_coverage,
)
from skills.sql_validator import (
    auto_fix_agg_sql,
    fix_cte_missing_columns,
    fix_order_by_before_set_operator,
    format_sql_error_context,
    validate_engine_sql,
    validate_generated_sql,
)
from skills.pipeline_trace import log as pipeline_log

logger = logging.getLogger("sql_agent")

_STUB_SQL_RE = re.compile(
    r"上\s*述\s*SQL|上\s*面\s*(?:的)?\s*SQL|见\s*上|如\s*上|\(\s*略\s*\)|略\s*\)"
    r"|省\s*略|此\s*处\s*SQL|placeholder\s*sql"
    r"|完\s*整\s*SQL|完\s*整\s*sql|complete\s*sql|full\s*sql"
    r"|\.\.\.\s*\w*\s*SQL\s*\w*\s*\.\.\.|\.\.\.\s*\w*\s*sql\s*\w*\s*\.\.\."
    r"|\.{3,}",
    re.IGNORECASE,
)

REPAIR_SYSTEM_PROMPT = """你是 SQL 修复专家。根据预检问题、执行错误或 0 行结果，在保持原分析意图的前提下修复 SQL。

【修复纪律】
1. 字段名、表名必须来自下方提供的表结构上下文，禁止臆造
2. 遵守当前引擎方言（Spark SQL 3.3.1 或 Trino 425），禁止混用
3. 最终 sql 必须是**一条**可执行语句；中间禁止分号
4. 仅做最小必要修改以解决报告的问题，不要重写无关逻辑
5. 枚举字段必须依据真实类型、样例值或用户确认的口径过滤，不猜测编码映射。

【输出格式 — 必须遵守】
优先输出 JSON：
{
  "sql": "完整可执行 SQL 原文",
  "explanation": "说明修复了什么问题",
  "tables_used": ["db.table1"]
}
也可用单独 ```sql 代码块；禁止在 sql 字段里用「上述SQL / 如上 / (略)」等占位。
"""


def _find_json_objects(text):
    """Find balanced JSON object boundaries in text, returning (start, end) pairs."""
    results = []
    i = 0
    while i < len(text):
        if text[i] == '{':
            depth = 0
            in_string = False
            escape = False
            for j in range(i, len(text)):
                ch = text[j]
                if escape:
                    escape = False
                    continue
                if ch == '\\' and in_string:
                    escape = True
                    continue
                if ch == '"' and not escape:
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        results.append((i, j))
                        break
        i += 1
    return results


SQL_SYSTEM_PROMPT = """你是一位专业的SQL分析师和数据研发工程师，负责基于企业数据仓库知识生成SQL查询。
你能根据业务需求找到相应的表结构、字段含义，并生成准确的SQL查询语句来满足分析需求。

你可以使用以下工具来查找表结构信息：
- search_tables: 按关键词检索表（同义词扩展 + 相关性排序），返回候选表与 expanded_terms
- get_table_info: 获取表字段；默认「摘要列」（分区+核心/常用指标列，省 token）。JOIN 或缺列时对同一表再调一次并设 full_detail=true

【工具使用纪律】：
1. search_tables 同一问题内最多调用 2 次
2. 最终 SQL 用到的每个 db.table 都必须至少调用 1 次 get_table_info（可先摘要再按需全量）
3. 字段名、类型、分区列必须来自 get_table_info，禁止臆造
4. 若返回 schema_meta.truncated=true 且所需列未列出，必须 full_detail=true 再拉一次
5. 表名须为真实存在的名称；分析查询优先用分区字段(如 dt)限制扫描

【业务字段取值】
- 枚举值必须来自表结构、样例或用户确认的口径；不能把口语编号直接当成落库值。
- 不确定时明确说明需要核对枚举，必要时提供 DISTINCT 抽样 SQL；禁止擅自扩大过滤范围。

【SQL语法强制规则】：
6. 聚合：SELECT 含聚合函数时，非聚合列须在 GROUP BY 中
7. JOIN 类型不一致须 CAST
8. CTE 须包含外层引用列
9. 字段名用工具返回的原始名

【引擎版本差异（必须专精）】：
当前只支持 Apache Spark SQL 3.3.1 与 Trino 425。生成 SQL 前必须按当前引擎选择语法，禁止混用。

Spark SQL 3.3.1：
- 别名/中文别名用反引号：`AS `别名``；不要用 ANSI 双引号别名。
- 字符串拼接用 `CONCAT(a,b,...)`；不要用 `||`。
- 日期格式化用 Java pattern：`date_format(dt, 'yyyy-MM-dd')`；不要用 Trino/MySQL 的 `'%Y-%m-%d'`。
- 数组长度用 `size(arr)`；字符串拆分展开用 `LATERAL VIEW explode(split(col, ',')) t AS x`。

Trino 425：
- 别名/中文别名用双引号：`AS "别名"`；不要用反引号。
- 字符串拼接可用 `||`，`CONCAT()` 参数尽量显式 `CAST(... AS VARCHAR)`。
- 日期格式化用 MySQL pattern：`date_format(ts, '%Y-%m-%d')`，**第一个参数必须是 TIMESTAMP**。
  - 若列是字符串分区（常见 `dt='yyyy-MM-dd'`），**禁止** `date_format(dt, '%Y-%m-%d')`。
  - 字符串分区过滤优先直接比较字符串：`dt >= '2026-04-21' AND dt < '2026-04-28'`。
  - 仅在确实需要时间格式化时才写：`date_format(CAST(dt AS TIMESTAMP), '%Y-%m-%d')`。
- Trino 日期运算优先：
  - `date_add('day', -7, current_date)`（不要用 Spark `date_sub`）
  - `date_diff('day', start_dt, end_dt)`（参数顺序与 Spark `datediff` 相反）
- 数组长度用 `CARDINALITY(arr)`；**禁止** `ARRAY_LENGTH(arr)`、`SIZE(arr)`。
- 字符串拆分展开用 `CROSS JOIN UNNEST(SPLIT(col, ',')) AS t(x)`；**禁止** `LATERAL VIEW` / `EXPLODE`。
- Trino **整数除法陷阱**：BIGINT/BIGINT 可能截断，占比与 CTR 必须写 `CAST(x AS DOUBLE) * 100.0 / NULLIF(y, 0)` 或 `x * 1.0 / NULLIF(y,0)`。
- Trino 类型纪律：`varchar = bigint`、`varchar = int` 这类比较必须显式 CAST 到同一类型后再比较/JOIN。
- Trino 子查询纪律：FROM/JOIN 中的子查询必须有别名。

通用结构规则：
- 最终 `sql` 必须是**一条**可执行语句；禁止在一个 SQL 字段里输出多条 `SELECT ...; SELECT ...`。
- 除末尾可选分号外，SQL 中间禁止出现分号。若要做多个分析，必须用 CTE 汇总成一个最终 `SELECT`。
- JOIN 只能写在 `FROM ... JOIN ... ON ...` 区域，严禁把 `LEFT JOIN/INNER JOIN` 放进 SELECT 字段列表。
- `GROUP BY` 只能放非聚合表达式，严禁 `GROUP BY COUNT(*) / SUM(...)`。
- `UNION/UNION ALL` 每个 SELECT 的列数、顺序、类型、业务含义必须一致；不要把不同分析结果强行 UNION 成含义错位的列。
- 长分析优先拆成一个“窄而准”的最终结果表；不要生成多个彼此无关的大段结果再 UNION。

【最终输出 — 必须遵守】
全部工具调用结束后，下一轮回复必须包含可解析的 SQL，禁止空回复、禁止只写说明不写 SQL。
优先输出 JSON（execution_plan 可 ""）；也可用单独 ```sql 代码块。
**严禁在 JSON 的 `sql` 字段里用「上述SQL / 如上 / 见上 / (略) / 省略 / ...」等占位短语代指；
无论长短都必须写完整、可直接执行的 SQL 原文。**只写 ```sql 代码块时，也禁止出现占位短语。
若用户已给出完整库表名（如 db.table），应优先 get_table_info 该表再写 SQL。

最终 JSON 示例：
{
  "sql": "SELECT ... FROM ... WHERE ...",
  "explanation": "这个SQL查询的目的是...",
  "tables_used": ["database.table1", "database.table2"],
  "execution_plan": ""
}"""

SQL_FINALIZATION_PROMPT = """工具检索阶段已经结束。现在禁止调用任何工具，也不要继续解释检索过程。
请只根据前面已经取得的候选表与字段信息，立即输出一个 JSON 对象：
{
  "sql": "一条完整、可直接执行的 SQL 原文",
  "explanation": "简要说明口径与关键过滤条件",
  "tables_used": ["db.table"],
  "execution_plan": ""
}
禁止省略 SQL，禁止使用“如上/上述SQL/略/...”，禁止输出多个 SQL 语句。"""


def _is_strong_sql_model(model):
    model_id = str(model or "").strip().lower()
    if not model_id:
        return False
    return model_id in SQL_AGENT_STRONG_MODELS or model_id == SQL_AGENT_FALLBACK_MODEL.lower()


def _build_skills():
    search_skill = Skill(
        name="search_tables",
        description="按关键词搜表（BM25+注释匹配+同义词扩展），返回候选表、comment、expanded_terms",
        func=skill_search_tables,
        parameters={
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "搜索关键词，如 user、order、ad、click 等"
                },
                "limit": {
                    "type": "integer",
                    "description": "最大返回数量",
                    "default": 10
                },
            },
            "required": ["keyword"],
        },
    )

    info_skill = Skill(
        name="get_table_info",
        description=(
            "获取表字段。默认摘要列（分区+核心列，省 token）；"
            "需要全部列或 JOIN 缺列时设 full_detail=true"
        ),
        func=skill_get_table_info,
        parameters={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "完整表名 database.table_name",
                },
                "full_detail": {
                    "type": "boolean",
                    "description": "false=仅摘要列（默认）；true=全部列",
                    "default": False,
                },
            },
            "required": ["table_name"],
        },
    )

    return [search_skill, info_skill]


def _trim_sql_agent_history(history):
    """限制历史条数与单条长度，避免 tool 大 payload 撑爆上下文。"""
    if not history:
        return history
    h = list(history)
    mt = SQL_AGENT_HISTORY_MAX_TURNS
    if mt and mt > 0 and len(h) > mt:
        h = h[-mt:]
    out = []
    for msg in h:
        m = dict(msg) if isinstance(msg, dict) else {"role": "user", "content": str(msg)}
        c = m.get("content", "")
        if isinstance(c, str) and SQL_AGENT_HISTORY_MAX_CHARS and len(c) > SQL_AGENT_HISTORY_MAX_CHARS:
            m["content"] = c[:SQL_AGENT_HISTORY_MAX_CHARS] + "\n...[truncated]"
        out.append(m)
    return out


def _build_relevant_table_catalog(query, limit=None):
    """Build a small local-retrieval catalog instead of injecting every known table."""
    top_n = max(1, min(int(limit or SQL_AGENT_CATALOG_TOP_N), 80))
    explicit_tables = _extract_explicit_db_tables(query)
    try:
        search_result = skill_search_tables(keyword=(query or "")[:4000], limit=top_n)
    except Exception as e:
        logger.warning("Initial table retrieval failed: %s", e)
        search_result = {}

    by_name = {}
    ordered = []
    for item in search_result.get("tables", []) if isinstance(search_result, dict) else []:
        if not isinstance(item, dict) or not item.get("table"):
            continue
        table_name = str(item["table"])
        by_name[table_name.lower()] = item
        ordered.append(table_name)

    candidates = []
    seen = set()
    for table_name in explicit_tables + ordered:
        key = table_name.lower()
        if key in seen:
            continue
        seen.add(key)
        item = by_name.get(key, {})
        candidates.append({
            "table": table_name,
            "comment": (item.get("comment") or "用户明确指定的表")[:240],
        })
        if len(candidates) >= top_n:
            break

    lines = []
    for item in candidates:
        suffix = " -- {}".format(item["comment"]) if item.get("comment") else ""
        lines.append("- {}{}".format(item["table"], suffix))
    catalog = "\n".join(lines) if lines else "(本地检索未命中候选表，请使用 search_tables 定位)"
    logger.info(
        "Relevant table catalog: query_chars=%d tables=%d chars=%d",
        len(query or ""), len(candidates), len(catalog),
    )
    return catalog


def create_sql_agent(engine="spark", llm_model=None, query=""):
    from config.settings import LLM_MODEL

    engine_hint = "Spark SQL" if engine == "spark" else "Trino SQL"
    extra = "\n当前SQL引擎: {}。请确保生成的SQL语法兼容该引擎。".format(engine_hint)
    if engine == "trino":
        extra += (
            "\nTrino注意: 使用标准ANSI SQL语法，不支持Hive特有语法如LATERAL VIEW。"
            "\nTrino高危点: date_format 的第1参数必须是 timestamp；若 dt 是字符串分区，优先直接按字符串比较，不要写 date_format(dt, '%Y-%m-%d')。"
            "\n生成占比/点击率时禁止 BIGINT 直接相除；用 * 100.0、CAST(... AS DOUBLE) 或分子 * 1.0 保证结果为小数。"
            "\n句末分号可有可无：本服务执行 Trino 时会自动去掉尾部分号（Python DBAPI 单条语句不接受结尾 ;）。"
        )

    catalog = _build_relevant_table_catalog(query)
    extra += (
        "\n\n【本问题相关候选表（本地检索 Top {}，仅作定位）】"
        "\n不要把候选表当成字段依据；最终字段仍须由 get_table_info 确认：\n{}"
    ).format(SQL_AGENT_CATALOG_TOP_N, catalog)

    complex_plan = build_complex_query_plan(query)
    complex_context = build_complex_retrieval_context(query, plan=complex_plan)
    if complex_context.get("active"):
        extra += "\n\n" + complex_context.get("prompt", "")
        extra += (
            "\n本问题属于复杂漏斗，工具预算例外调整为：search_tables 最多 {} 次、"
            "get_table_info 最多 {} 次、总轮次最多 {}；完全相同的调用仍禁止重复。"
        ).format(
            SQL_AGENT_COMPLEX_SEARCH_TABLES_LIMIT,
            SQL_AGENT_COMPLEX_GET_TABLE_INFO_LIMIT,
            SQL_AGENT_COMPLEX_MAX_TOOL_ROUNDS,
        )

    max_iterations = (
        SQL_AGENT_COMPLEX_MAX_TOOL_ROUNDS
        if complex_plan.get("active") else SQL_AGENT_MAX_TOOL_ROUNDS
    )
    search_limit = (
        SQL_AGENT_COMPLEX_SEARCH_TABLES_LIMIT
        if complex_plan.get("active") else SQL_AGENT_SEARCH_TABLES_LIMIT
    )
    table_info_limit = (
        SQL_AGENT_COMPLEX_GET_TABLE_INFO_LIMIT
        if complex_plan.get("active") else SQL_AGENT_GET_TABLE_INFO_LIMIT
    )
    total_tool_limit = (
        SQL_AGENT_COMPLEX_MAX_TOTAL_TOOL_CALLS
        if complex_plan.get("active") else SQL_AGENT_MAX_TOTAL_TOOL_CALLS
    )

    agent = DeepAgent(
        name="SQLGeneratorAgent",
        system_prompt=SQL_SYSTEM_PROMPT + extra,
        skills=_build_skills(),
        model=llm_model or LLM_MODEL,
        temperature=0.3,
        max_tokens=4000,
        max_iterations=max_iterations,
        force_finalize_on_limit=True,
        finalization_prompt=SQL_FINALIZATION_PROMPT,
        skill_call_limits={
            "search_tables": search_limit,
            "get_table_info": table_info_limit,
        },
        max_total_skill_calls=total_tool_limit,
        block_duplicate_skill_calls=True,
    )
    agent.complex_query_plan = complex_plan
    agent.complex_retrieval_context = complex_context
    return agent


def _extract_explicit_db_tables(text):
    """从问题里抓取 db.table 形式表名，便于优先 get_table_info。"""
    if not text:
        return []
    found = re.findall(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b",
        text,
    )
    seen = []
    for t in found:
        if t not in seen:
            seen.append(t)
    return seen


def parse_sql_from_llm_response(raw_response):
    """从 LLM 回复中解析 sql / explanation / tables_used。"""
    sql = ""
    explanation = ""
    tables_used = []
    execution_plan = None

    if not raw_response:
        return {
            "sql": sql,
            "explanation": explanation,
            "tables_used": tables_used,
            "execution_plan": execution_plan,
        }

    try:
        cleaned = re.sub(r"```json\s*\n?|\n?\s*```", "", raw_response).strip()
        parsed = None

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        if not parsed:
            brace_positions = _find_json_objects(raw_response)
            for start, end in brace_positions:
                candidate = raw_response[start:end + 1]
                try:
                    obj = json.loads(candidate)
                    if isinstance(obj, dict) and "sql" in obj:
                        parsed = obj
                        break
                except Exception:
                    continue

        if parsed and isinstance(parsed, dict):
            sql = parsed.get("sql", "")
            explanation = parsed.get("explanation", "")
            tables_used = parsed.get("tables_used", [])
            execution_plan = parsed.get("execution_plan", None)

        sql_too_short = bool(sql) and len(sql) < 800
        fence_matches = re.findall(r"```sql\s*\n(.*?)\n\s*```", raw_response, re.DOTALL | re.IGNORECASE)
        fence_sql_candidate = fence_matches[0].strip() if fence_matches else ""
        should_fallback = (
            (sql and _STUB_SQL_RE.search(sql))
            or (sql_too_short and fence_sql_candidate and len(fence_sql_candidate) > len(sql) * 2)
        )
        if should_fallback and fence_sql_candidate:
            logger.warning(
                "JSON.sql 疑似占位/截断 (%r, len=%d)，改用 ```sql 代码块 (len=%d)",
                sql[:80] if sql else "", len(sql or ""), len(fence_sql_candidate),
            )
            sql = fence_sql_candidate

        if not sql:
            sql_matches = re.findall(r"```sql\s*\n(.*?)\n\s*```", raw_response, re.DOTALL)
            if sql_matches:
                sql = sql_matches[0].strip()

        if not sql:
            select_match = re.search(
                r'((?:WITH\s+\w+\s+AS\s*\(|SELECT)\s[\s\S]+?)(?:\n\n|\Z|```)',
                raw_response, re.IGNORECASE
            )
            if select_match:
                sql = select_match.group(1).strip().rstrip(';')
                logger.info("Extracted SQL via regex fallback")

        if not explanation and not sql:
            explanation = raw_response

        if sql:
            logger.info("Parsed SQL: %s", sql[:200])
            miss = list_unknown_tables_in_sql(sql)
            if miss:
                logger.warning("SQL references tables not in local schema index: %s", miss)
        else:
            logger.warning("Failed to extract SQL from response")

    except Exception as e:
        logger.warning("Response parsing error: %s", e)
        explanation = raw_response

    return {
        "sql": sql,
        "explanation": explanation,
        "tables_used": tables_used,
        "execution_plan": execution_plan,
    }


def postprocess_sql(sql, engine="spark", explanation=""):
    """生成/修复后的统一后处理，并在每次自动修改后重新校验。"""
    if not sql:
        return {"sql": sql, "explanation": explanation, "validation_issues": ["SQL is empty"]}

    auto_fixes = []

    schema_fixed = fix_columns_by_schema(sql)
    if schema_fixed:
        logger.info("Schema auto-fix applied")
        sql = schema_fixed
        auto_fixes.append("schema columns")
        logger.info("Schema auto-fix revalidation: %s", validate_engine_sql(sql, engine=engine) or "passed")

    cte_fixed = fix_cte_missing_columns(sql)
    if cte_fixed:
        logger.info("CTE missing columns auto-fixed")
        sql = cte_fixed
        auto_fixes.append("CTE columns")
        logger.info("CTE auto-fix revalidation: %s", validate_engine_sql(sql, engine=engine) or "passed")

    set_order_fixed = fix_order_by_before_set_operator(sql)
    if set_order_fixed:
        sql = set_order_fixed
        auto_fixes.append("set operation ORDER BY")
        logger.info(
            "Set-operation ORDER BY auto-fix revalidation: %s",
            validate_engine_sql(sql, engine=engine) or "passed",
        )

    warnings = validate_generated_sql(sql)
    engine_warnings = validate_engine_sql(sql, engine=engine)
    if engine_warnings:
        warnings.extend(engine_warnings)
    if warnings:
        warning_text = "; ".join(warnings)
        logger.warning("SQL validation warnings: %s", warning_text)
        fixed_sql = auto_fix_agg_sql(sql)
        if fixed_sql:
            logger.info("Aggregation auto-fix applied")
            sql = fixed_sql
            auto_fixes.append("aggregation")
            logger.info("Aggregation auto-fix revalidation: %s", validate_engine_sql(sql, engine=engine) or "passed")
        else:
            explanation = "Warning: {}\n\n{}".format(warning_text, explanation)

    final_issues = []
    final_issues.extend(
        "unknown table not in schema index: {}".format(t)
        for t in list_unknown_tables_in_sql(sql)
    )
    final_issues.extend(validate_generated_sql(sql))
    final_issues.extend(validate_engine_sql(sql, engine=engine))
    final_issues = list(dict.fromkeys(final_issues))
    if final_issues:
        logger.warning("SQL validation after auto-fix: %s", "; ".join(final_issues))
        issue_text = "; ".join(final_issues)
        if "Warning: {}".format(issue_text) not in explanation:
            explanation = "Warning: {}\n\n{}".format(issue_text, explanation)
    elif auto_fixes:
        explanation = "Auto-fixed and revalidated: {}\n\n{}".format(", ".join(auto_fixes), explanation)

    return {
        "sql": sql,
        "explanation": explanation,
        "validation_issues": final_issues,
        "auto_fixes": auto_fixes,
    }


def _build_repair_schema_context(sql, tables_used=None):
    """为 repair 注入相关表的全量字段信息。"""
    from skills.schema_skill import extract_tables_from_sql

    tables = []
    for t in (tables_used or []):
        if t and t not in tables:
            tables.append(t)
    for t in extract_tables_from_sql(sql or ""):
        if t and t not in tables:
            tables.append(t)

    blocks = []
    for table_name in tables[:8]:
        try:
            info = skill_get_table_info(table_name=table_name, full_detail=True)
            if isinstance(info, dict) and info.get("columns"):
                cols = info.get("columns") or []
                col_lines = []
                for c in cols[:120]:
                    if isinstance(c, dict):
                        col_lines.append(
                            "  - {} ({})".format(c.get("name", "?"), c.get("type", "?"))
                        )
                    else:
                        col_lines.append("  - {}".format(c))
                blocks.append(
                    "表 {}:\n  comment: {}\n  columns:\n{}".format(
                        table_name,
                        (info.get("comment") or "")[:200],
                        "\n".join(col_lines) if col_lines else "  (无)",
                    )
                )
        except Exception as e:
            logger.warning("repair schema context failed for %s: %s", table_name, e)
    return "\n\n".join(blocks) if blocks else "(无表结构上下文，请仅根据错误信息做最小修复)"


def repair_sql(
    original_question,
    failed_sql,
    error_message,
    engine="spark",
    repair_reason="execute",
    tables_used=None,
    llm_model=None,
    temperature=0.15,
    max_tokens=4000,
    use_fallback_model=False,
):
    """
    根据预检/执行错误/0 行结果修复 SQL。
    repair_reason: precheck | execute | zero_rows
    """
    from config.settings import LLM_MODEL

    start_time = time.time()
    engine_hint = "Spark SQL 3.3.1" if engine == "spark" else "Trino 425"
    schema_ctx = _build_repair_schema_context(failed_sql, tables_used=tables_used)
    err_ctx = format_sql_error_context(failed_sql, error_message or "")

    reason_map = {
        "precheck": "SQL 未执行，预检发现以下问题",
        "execute": "SQL 执行失败，错误信息如下",
        "zero_rows": "SQL 执行成功但返回 0 行，请检查过滤条件/分区/枚举取值",
    }
    reason_text = reason_map.get(repair_reason, "需要修复 SQL")

    user_msg = (
        "{reason_text}：\n{error_message}\n\n"
        "【原始分析需求】\n{question}\n\n"
        "【当前引擎】{engine_hint}\n\n"
        "【失败 SQL】\n```sql\n{failed_sql}\n```\n\n"
        "【相关表结构 — 字段必须来自此处】\n{schema_ctx}\n"
    ).format(
        reason_text=reason_text,
        error_message=(error_message or "")[:6000],
        question=(original_question or "")[:4000],
        engine_hint=engine_hint,
        failed_sql=(failed_sql or "")[:50000],
        schema_ctx=schema_ctx[:30000],
    )
    if err_ctx:
        user_msg += "\n【错误行上下文】\n{}\n".format(err_ctx[:4000])

    primary_model = llm_model or LLM_MODEL
    selected_model = (
        SQL_AGENT_FALLBACK_MODEL
        if use_fallback_model and SQL_AGENT_FALLBACK_MODEL and not _is_strong_sql_model(primary_model)
        else primary_model
    )
    agent = DeepAgent(
        name="SQLRepairAgent",
        system_prompt=REPAIR_SYSTEM_PROMPT + "\n当前SQL引擎: {}。".format(engine_hint),
        skills=[],
        model=selected_model,
        temperature=temperature,
        max_tokens=max_tokens,
        max_iterations=1,
        request_timeout=(
            SQL_AGENT_REPAIR_FALLBACK_TIMEOUT if selected_model != primary_model else None
        ),
        max_retries=(SQL_AGENT_FALLBACK_MAX_RETRIES if selected_model != primary_model else None),
    )

    pipeline_log(
        logger,
        "agent.repair_sql.start",
        reason=repair_reason,
        sql_chars=len(failed_sql or ""),
        engine=engine,
        fallback_model=bool(selected_model != primary_model),
    )
    try:
        raw_response = agent.simple_chat(
            user_message=user_msg,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except Exception as e:
        pipeline_log(logger, "agent.repair_sql.fail", reason=repair_reason, err=str(e)[:180])
        raise

    parsed = parse_sql_from_llm_response(raw_response)
    sql = (parsed.get("sql") or "").strip()
    explanation = parsed.get("explanation") or ""
    repaired_tables = parsed.get("tables_used") or tables_used or []

    if sql:
        pp = postprocess_sql(sql, engine=engine, explanation=explanation)
        sql = pp["sql"]
        explanation = pp["explanation"]

    elapsed = time.time() - start_time
    pipeline_log(
        logger,
        "agent.repair_sql.done",
        reason=repair_reason,
        ok=bool(sql),
        sql_chars=len(sql or ""),
        sec=elapsed,
    )
    return {
        "sql": sql,
        "explanation": explanation,
        "tables_used": repaired_tables,
        "repair_reason": repair_reason,
        "fallback_used": bool(selected_model != primary_model),
        "query_time": elapsed,
    }


def generate_sql(query, history=None, engine="spark", temperature=0.3, max_tokens=4000, llm_model=None):
    from config.settings import LLM_MODEL

    start_time = time.time()
    primary_model = llm_model or LLM_MODEL

    agent = create_sql_agent(engine, llm_model=primary_model, query=query)
    complex_plan = getattr(agent, "complex_query_plan", None) or build_complex_query_plan(query)
    retrieval_strategy = complex_plan.get("strategy", "default")
    user_msg = "请专注于SQL生成任务。我的问题是：{}".format(query)
    explicit_tables = _extract_explicit_db_tables(query)
    if explicit_tables:
        user_msg += "\n\n【用户已点名的表（请优先依次 get_table_info，再写 SQL）】" + "、".join(explicit_tables)

    try:
        raw_response = agent.run(
            user_message=user_msg,
            history=_trim_sql_agent_history(history),
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except Exception as e:
        pipeline_log(logger, "agent.generate_sql.fail", err=str(e)[:180])
        logger.exception("Agent run failed: %s", e)
        raise

    logger.info("Raw LLM response (first 1000 chars): %s", raw_response[:1000])

    parsed = parse_sql_from_llm_response(raw_response)
    if not parsed.get("sql") and not agent.last_run_info.get("forced_finalization"):
        pipeline_log(logger, "agent.generate_sql.cheap_finalize.start", model=primary_model)
        try:
            raw_response = agent.finalize_last_run(
                prompt=SQL_FINALIZATION_PROMPT,
                model=primary_model,
                temperature=min(temperature, 0.1),
                max_tokens=max_tokens,
            )
            parsed = parse_sql_from_llm_response(raw_response)
            agent.last_run_info["forced_finalization"] = True
            pipeline_log(
                logger, "agent.generate_sql.cheap_finalize.done",
                ok=bool(parsed.get("sql")), chars=len(raw_response or ""),
            )
        except Exception as e:
            logger.warning("Cheap forced finalization failed: %s", e)
            pipeline_log(logger, "agent.generate_sql.cheap_finalize.fail", err=str(e)[:180])

    sql = parsed.get("sql") or ""
    explanation = parsed.get("explanation") or ""
    tables_used = parsed.get("tables_used") or []
    execution_plan = parsed.get("execution_plan")

    validation_issues = []
    if sql:
        pp = postprocess_sql(sql, engine=engine, explanation=explanation)
        sql = pp["sql"]
        explanation = pp["explanation"]
        validation_issues = pp.get("validation_issues") or []

    semantic_coverage = evaluate_semantic_coverage(query, sql, plan=complex_plan)
    semantic_recovery_used = False
    if semantic_coverage.get("applicable") and not semantic_coverage.get("complete"):
        missing = semantic_coverage.get("missing_stages") or []
        recovery_context = build_semantic_recovery_context(complex_plan, missing)
        recovery_prompt = (
            "禁止调用工具。当前 SQL 未完整覆盖用户要求的漏斗阶段，请定向补齐缺失阶段。\n"
            "【当前 SQL】\n```sql\n{sql}\n```\n\n{context}\n\n{format_prompt}"
        ).format(
            sql=(sql or "(当前没有可用 SQL)")[:50000],
            context=recovery_context[:30000],
            format_prompt=SQL_FINALIZATION_PROMPT,
        )
        pipeline_log(
            logger, "agent.generate_sql.semantic_recovery.start",
            missing=",".join(missing), model=primary_model,
        )
        try:
            recovery_raw = agent.finalize_last_run(
                prompt=recovery_prompt,
                model=primary_model,
                temperature=min(temperature, 0.1),
                max_tokens=max_tokens,
            )
            recovery_parsed = parse_sql_from_llm_response(recovery_raw)
            recovery_sql = (recovery_parsed.get("sql") or "").strip()
            recovery_issues = ["SQL is empty"]
            recovery_coverage = evaluate_semantic_coverage(query, recovery_sql, plan=complex_plan)
            if recovery_sql:
                recovery_pp = postprocess_sql(
                    recovery_sql,
                    engine=engine,
                    explanation=recovery_parsed.get("explanation") or "",
                )
                recovery_sql = recovery_pp["sql"]
                recovery_issues = recovery_pp.get("validation_issues") or []
                recovery_coverage = evaluate_semantic_coverage(
                    query, recovery_sql, plan=complex_plan
                )
            old_count = len(semantic_coverage.get("covered_stages") or [])
            new_count = len(recovery_coverage.get("covered_stages") or [])
            adopt_recovery = (
                bool(recovery_sql)
                and len(recovery_issues) <= len(validation_issues)
                and (recovery_coverage.get("complete") or new_count > old_count)
            )
            if adopt_recovery:
                sql = recovery_sql
                explanation = recovery_pp["explanation"]
                tables_used = recovery_parsed.get("tables_used") or tables_used
                execution_plan = recovery_parsed.get("execution_plan") or execution_plan
                validation_issues = recovery_issues
                semantic_coverage = recovery_coverage
                semantic_recovery_used = True
            pipeline_log(
                logger, "agent.generate_sql.semantic_recovery.done",
                adopted=adopt_recovery,
                complete=bool(recovery_coverage.get("complete")),
                covered=new_count,
            )
        except Exception as e:
            logger.warning("Semantic SQL recovery failed; continuing to fallback: %s", e)
            pipeline_log(
                logger, "agent.generate_sql.semantic_recovery.fail", err=str(e)[:180]
            )

    sql_tables = extract_tables_from_sql(sql or "")
    join_count = len(re.findall(r"\bJOIN\b", sql or "", re.IGNORECASE))
    fallback_reasons = []
    if not sql:
        fallback_reasons.append("cheap_finalization_failed")
    if validation_issues:
        fallback_reasons.append("validation_failed")
    if semantic_coverage.get("applicable") and not semantic_coverage.get("complete"):
        fallback_reasons.append("semantic_incomplete")
    if (
        len(sql_tables) >= SQL_AGENT_COMPLEX_TABLE_THRESHOLD
        and join_count >= SQL_AGENT_COMPLEX_TABLE_THRESHOLD - 1
    ):
        fallback_reasons.append("complex_multi_table_join")

    fallback_used = False
    primary_is_strong = _is_strong_sql_model(primary_model)
    if fallback_reasons and primary_is_strong:
        pipeline_log(
            logger, "agent.generate_sql.fallback.skip",
            reasons=",".join(fallback_reasons), primary_model=primary_model,
            skip_reason="primary_model_already_strong",
        )
    if (
        fallback_reasons
        and not primary_is_strong
        and SQL_AGENT_FALLBACK_MODEL
        and SQL_AGENT_FALLBACK_MODEL != primary_model
    ):
        reason_text = ",".join(fallback_reasons)
        fallback_prompt = (
            "你是最终 SQL 审核器。禁止调用工具。请根据完整对话中的需求、候选表和字段结果，"
            "生成或修正最终 SQL。触发原因：{reasons}。\n"
            "当前候选 SQL：\n```sql\n{sql}\n```\n"
            "当前校验问题：{issues}\n\n{semantic_context}\n\n{format_prompt}"
        ).format(
            reasons=reason_text,
            sql=(sql or "(便宜模型未生成 SQL)")[:50000],
            issues="; ".join(validation_issues) if validation_issues else "无",
            semantic_context=(
                build_semantic_recovery_context(
                    complex_plan, semantic_coverage.get("missing_stages") or []
                )[:30000]
                if "semantic_incomplete" in fallback_reasons else ""
            ),
            format_prompt=SQL_FINALIZATION_PROMPT,
        )
        pipeline_log(
            logger, "agent.generate_sql.fallback.start",
            reasons=reason_text, model=SQL_AGENT_FALLBACK_MODEL,
        )
        try:
            fallback_raw = agent.finalize_last_run(
                prompt=fallback_prompt,
                model=SQL_AGENT_FALLBACK_MODEL,
                temperature=0.1,
                max_tokens=max_tokens,
                request_timeout=SQL_AGENT_FINAL_FALLBACK_TIMEOUT,
                max_retries=SQL_AGENT_FALLBACK_MAX_RETRIES,
            )
            fallback_parsed = parse_sql_from_llm_response(fallback_raw)
            fallback_sql = (fallback_parsed.get("sql") or "").strip()
            fallback_issues = ["SQL is empty"]
            if fallback_sql:
                fallback_pp = postprocess_sql(
                    fallback_sql,
                    engine=engine,
                    explanation=fallback_parsed.get("explanation") or "",
                )
                fallback_sql = fallback_pp["sql"]
                fallback_issues = fallback_pp.get("validation_issues") or []

            fallback_coverage = evaluate_semantic_coverage(
                query, fallback_sql, plan=complex_plan
            )
            semantic_improved = (
                fallback_coverage.get("complete")
                or len(fallback_coverage.get("covered_stages") or [])
                > len(semantic_coverage.get("covered_stages") or [])
            )

            if fallback_sql and (
                (
                    semantic_coverage.get("applicable")
                    and semantic_improved
                    and len(fallback_issues) <= len(validation_issues)
                )
                or (
                    not semantic_coverage.get("applicable")
                    and (not sql or len(fallback_issues) <= len(validation_issues))
                )
            ):
                sql = fallback_sql
                explanation = fallback_pp["explanation"]
                tables_used = fallback_parsed.get("tables_used") or tables_used
                execution_plan = fallback_parsed.get("execution_plan") or execution_plan
                validation_issues = fallback_issues
                semantic_coverage = fallback_coverage
                fallback_used = True
            pipeline_log(
                logger, "agent.generate_sql.fallback.done",
                adopted=fallback_used, sql_chars=len(fallback_sql or ""), issues=len(fallback_issues),
            )
        except Exception as e:
            logger.warning("Strong-model SQL fallback failed; keeping cheap-model result: %s", e)
            pipeline_log(logger, "agent.generate_sql.fallback.fail", err=str(e)[:180])

    semantic_complete = bool(semantic_coverage.get("complete", True))
    if not semantic_complete:
        missing_labels = [
            item.get("label") for item in semantic_coverage.get("stage_results", [])
            if not item.get("covered")
        ]
        warning = "语义完整性检查未通过，缺失阶段：{}。该 SQL 不会由自动 Pipeline 执行。".format(
            "、".join([x for x in missing_labels if x]) or "未知"
        )
        explanation = warning + ("\n\n" + explanation if explanation else "")

    elapsed = time.time() - start_time
    pipeline_log(
        logger,
        "agent.generate_sql.done",
        sql_chars=len(sql or ""),
        tables=len(tables_used or []),
        sec=elapsed,
        fallback_used=fallback_used,
        validation_issues=len(validation_issues),
        semantic_complete=semantic_complete,
        missing_stages=",".join(semantic_coverage.get("missing_stages") or []),
        retrieval_strategy=retrieval_strategy,
    )
    return {
        "sql": sql,
        "explanation": explanation,
        "tables_used": tables_used,
        "execution_plan": execution_plan,
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reasons,
        "validation_issues": validation_issues,
        "semantic_complete": semantic_complete,
        "semantic_coverage": semantic_coverage,
        "missing_stages": semantic_coverage.get("missing_stages") or [],
        "retrieval_strategy": retrieval_strategy,
        "semantic_recovery_used": semantic_recovery_used,
        "query_time": elapsed,
    }
