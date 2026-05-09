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
    build_table_catalog,
    list_unknown_tables_in_sql,
)
from config.settings import SQL_AGENT_HISTORY_MAX_TURNS, SQL_AGENT_HISTORY_MAX_CHARS
from skills.sql_validator import (
    auto_fix_agg_sql,
    fix_cte_missing_columns,
    validate_engine_sql,
    validate_generated_sql,
)
from skills.pipeline_trace import log as pipeline_log

logger = logging.getLogger("sql_agent")


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

【业务取值 — api_type】
- 表 dws_cn.dws_behavior_map_game_reco_i_d 等推荐行为明细中，业务常说「API 4001 / 监控数 4001」，但落库 api_type 常为五位字符串 **'40001'**。
- 若用户写 4001，必须在 get_table_info 后按真实类型与样例值写过滤；禁止想当然写成 '4001' 导致 0 行。不确定时可生成「DISTINCT api_type 抽样」辅助 SQL 或在主 SQL 中用 IN ('4001','40001') 并在 explanation 中说明。

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


def create_sql_agent(engine="spark", llm_model=None):
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

    catalog = build_table_catalog()
    extra += "\n\n【可用数据表目录 — 仅作定位】用 search_tables 与 get_table_info 确认真实字段；目录可能不含列信息：\n"
    extra += catalog

    agent = DeepAgent(
        name="SQLGeneratorAgent",
        system_prompt=SQL_SYSTEM_PROMPT + extra,
        skills=_build_skills(),
        model=llm_model or LLM_MODEL,
        temperature=0.3,
        max_tokens=4000,
        max_iterations=8,
    )
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


def generate_sql(query, history=None, engine="spark", temperature=0.3, max_tokens=4000, llm_model=None):
    start_time = time.time()

    agent = create_sql_agent(engine, llm_model=llm_model)
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

    sql = ""
    explanation = ""
    tables_used = []
    execution_plan = None

    logger.info("Raw LLM response (first 1000 chars): %s", raw_response[:1000])

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
                candidate = raw_response[start:end+1]
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

        # 反偷懒：模型有时正文给了完整 ```sql 代码块，又在 JSON 里用 "上述SQL / 如上 / (略) / 完整SQL"
        # 等中文/英文占位代指，这会导致抽到的 sql 只有几十个字符、带 "(上述SQL)" 等串且不可执行。
        # 发现这类占位时，回落到 ```sql 代码块。
        _STUB_RE = re.compile(
            r"上\s*述\s*SQL|上\s*面\s*(?:的)?\s*SQL|见\s*上|如\s*上|\(\s*略\s*\)|略\s*\)"
            r"|省\s*略|此\s*处\s*SQL|placeholder\s*sql"
            r"|完\s*整\s*SQL|完\s*整\s*sql|complete\s*sql|full\s*sql"  # 新增：避免 LLM 抄 SKILL.md 里的 <完整 SQL> 占位
            r"|\.\.\.\s*\w*\s*SQL\s*\w*\s*\.\.\.|\.\.\.\s*\w*\s*sql\s*\w*\s*\.\.\."  # 新增：匹配 ...xxxSQL... 这种省略号包夹形式
            r"|\.{3,}",  # 新增：任何 SQL 里出现 3 个以上点号（合法 SQL 永远不会有 ...）
            re.IGNORECASE,
        )
        # 兜底长度判断：完整 AB / 多 CTE 业务 SQL 至少 800 字符；
        # 若 JSON.sql 长度 < 800 且原文含 markdown 代码块，几乎可以肯定 JSON 占位
        sql_too_short = bool(sql) and len(sql) < 800
        fence_matches = re.findall(r"```sql\s*\n(.*?)\n\s*```", raw_response, re.DOTALL | re.IGNORECASE)
        fence_sql_candidate = fence_matches[0].strip() if fence_matches else ""
        should_fallback = (
            (sql and _STUB_RE.search(sql))
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

    # Post-processing: schema fix, CTE fix, aggregation fix
    if sql:
        schema_fixed = fix_columns_by_schema(sql)
        if schema_fixed:
            logger.info("Schema auto-fix applied")
            sql = schema_fixed

        cte_fixed = fix_cte_missing_columns(sql)
        if cte_fixed:
            logger.info("CTE missing columns auto-fixed")
            sql = cte_fixed

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
                explanation = "Warning: {} -> Auto-fixed\n\n{}".format(warning_text, explanation)
                sql = fixed_sql
            else:
                explanation = "Warning: {}\n\n{}".format(warning_text, explanation)

    elapsed = time.time() - start_time
    pipeline_log(
        logger,
        "agent.generate_sql.done",
        sql_chars=len(sql or ""),
        tables=len(tables_used or []),
        sec=elapsed,
    )
    return {
        "sql": sql,
        "explanation": explanation,
        "tables_used": tables_used,
        "execution_plan": execution_plan,
        "query_time": elapsed,
    }
