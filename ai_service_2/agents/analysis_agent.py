#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Data analysis agent: generate Markdown analysis reports from query results"""
import json
import logging
import time

from agents.base_agent import DeepAgent
from config.settings import LLM_MODEL, QUERY_RESULT_MAX_ROWS
from skills.pipeline_trace import log as pipeline_log

logger = logging.getLogger("analysis_agent")

ANALYSIS_SYSTEM_PROMPT = """你是一位专业的数据分析师，擅长对结构化查询结果做解读，输出清晰、可核对、可落地的业务洞察。
用户会在消息中提供【用户问题】、【数据表头】、【数据内容】与【图表类型】（图表类型仅作背景，勿臆造图中未给出的数值）。

你必须严格基于【数据内容】中的真实行与列作答：禁止编造行、列、日期、指标值或因果；所有定量结论须能在给出的表中找到依据。
若用户问题与数据列不完全匹配，先说明「数据能回答什么、不能直接回答什么」，再在给定数据范围内分析。
若输入中出现【程序预计算累计（权威）】区块：涉及“累计/全周期/多天汇总”的数字必须优先引用该区块；`ctr/rate/ratio/avg/*_per_*` 等比率类指标按分子分母重算，禁止逐日相加。

按下列 Markdown 结构输出（四个主节 `## 一…`～`## 四…` 标题与顺序固定，勿改字样、勿插入新的 `##` 主节；可在节内用 `###` 细分）。

# [根据分析主题拟定的报告标题，一行]

在 `#` 标题与 `## 一、数据概览` 之间，**必须**增加一段「核心结论」导读（便于读者 10 秒内抓住结论），格式如下（勿用新的 `##` 标题）：
- 用 2～4 行无序列表 `- `，每行不超过 45 字；
- 每行须含**至少一个表中可查的数字或百分比**；
- 第一行优先直接回应【用户问题】是/否、程度如何；若数据不足以回答，第一行须明确写「在现有样本下无法判断…」；
- 避免与后文「二、关键发现」逐字重复，此处是高度概括，后文是展开。

## 一、数据概览
- 数据规模：行数、列数；若消息中说明「仅展示前 N 行」，须写明「结论基于样本前 N 行，完整结果可能不同」。
- 时间范围与粒度：仅当表中存在日期/时间类列时归纳；无则写「无时间列，为截面/汇总结果」。
- 分组维度与核心指标：用表头原名列出主要维度与指标（如 recall_type、占比、CTR 等），一句话说明与用户问题的对应关系。

## 二、关键发现
列出 3～5 条，每条格式固定为：`- **短标题（2～8 字）**：叙述`，且叙述中**至少包含一个可查表的数字或百分比**（禁止空泛结论）。
**排序**：把与【用户问题】关联最强、或业务影响最大的一条放在**第一条**；其余按重要性递减。
优先写：极值、TOP、跨行对比、异常波动、与业务问题直接相关的结构（如某通路占比与 CTR 的反差）。
对比时写清对象与时间/分组（例：「A 通路本周推荐占比 x%，上周 y%」）；勿使用占位符或模糊表述。
若存在「与直觉相反」或「需决策」的点，在该条末用括号补一句**含义**（如「需结合流量结构判断是否调权」），仍须基于数据表述。

## 三、深度分析
在「二」的基础上展开因果与业务含义，可分段或小标题 `###`，并**适当使用 Markdown 表格**（列名须来自真实表头，单元格为表中可查值）。
可讨论：可能原因、与其他指标的一致性、需要注意的口径（如占比分母、是否含零曝光）。
若数据不足以支持强结论，明确写「样本内无法区分…建议补充…」，不要强行断言。
在「三」的末尾（最后一个 `###` 或小节后），**可选**用一行引用收束本节，格式：`> **小结**：用一句话概括本节对业务的含义（须仍可从表中印证）。`

## 四、建议
给出 2～5 条可执行建议，每条尽量包含「动作 + 预期 + 如何验证」（例如再看哪类 SQL/哪几天分区）。
建议须与前文发现挂钩；无数据支撑时不要写具体目标数值。
**写法**：建议 1 对应「二」中优先级最高的发现；可用「针对上文 **XX**…」点明对应关系，便于执行与复盘。
**第一条**优先写「若资源有限、只推进一项」时最该做的事（仍须可验证、非空话）。

---

输出纪律（在遵守上述四节结构前提下）：
- 全文使用标准 Markdown；输出纯文本 Markdown，不要 JSON、不要 SQL、不要整段代码块包一层「报告」。
- **禁止**用 Markdown「代码围栏」（连续三个反引号起止）包裹正文、列表或管道表格；表格直接写管道行与 `|---|---|` 形式的分隔行即可。围栏仅用于少量真实代码片段。
- 数值与单位、百分比写法尽量与表中呈现一致；需要换算时在括号内写明依据。
- 语言简洁，避免重复堆砌同义句；避免「综上所述」等空话。
- 【图表类型】仅帮助语气侧重（趋势/对比/分布），不得虚构图表中的数字。

可选增强（非必须；HTML 须整段独占一行或多行，勿放进 Markdown 表格或 ``` 代码块）：
- 核心结论条：`> **要点**：……`
- KPI 卡片区：`<div class="report-kpi-row"><div class="report-kpi-card"><span class="v">数值含单位</span><span class="l">指标说明</span></div></div>`（可复制多个 card）
- 归因语气框：`class="report-callout report-callout-risk|ok|note|info"` 的 div 包裹一段话即可。"""


def _prompt_table_row_cap(num_columns, n_available):
    """明细表写入 prompt 的最大行数（列越多则越少，控制 token；n_available 为已截断后的可用行数）。"""
    if n_available <= 0:
        return 0
    if num_columns <= 5:
        cap = 400
    elif num_columns <= 10:
        cap = 250
    elif num_columns <= 15:
        cap = 150
    elif num_columns <= 25:
        cap = 100
    elif num_columns <= 40:
        cap = 60
    else:
        cap = 40
    return min(n_available, cap)


def _calculate_basic_stats(headers, data):
    if not data:
        return ""
    stats_lines = []
    for header in headers:
        values = [row.get(header) for row in data if row.get(header) is not None]
        if not values:
            continue
        try:
            numeric_values = []
            for v in values:
                if isinstance(v, (int, float)):
                    numeric_values.append(float(v))
                elif isinstance(v, str):
                    cleaned = v.replace(',', '').replace('%', '')
                    if cleaned.replace('.', '').replace('-', '').isdigit():
                        numeric_values.append(float(cleaned))

            if numeric_values and len(numeric_values) > len(values) * 0.5:
                min_val = min(numeric_values)
                max_val = max(numeric_values)
                avg_val = sum(numeric_values) / len(numeric_values)

                def fmt(n):
                    if abs(n) >= 1000000:
                        return "{:.2f}M".format(n/1000000)
                    elif abs(n) >= 1000:
                        return "{:.2f}K".format(n/1000)
                    elif abs(n) >= 1:
                        return "{:.2f}".format(n)
                    elif abs(n) >= 0.01:
                        return "{:.4f}".format(n)
                    elif abs(n) > 0:
                        return "{:.6f}".format(n)
                    return "0"

                stats_lines.append("- **{}**: min={}, max={}, avg={}".format(header, fmt(min_val), fmt(max_val), fmt(avg_val)))
            else:
                unique_count = len(set(str(v) for v in values))
                if unique_count <= 10:
                    uv = list(set(str(v) for v in values))[:5]
                    stats_lines.append("- **{}**: {} unique values ({})".format(header, unique_count, ', '.join(uv)))
                else:
                    stats_lines.append("- **{}**: {} unique values".format(header, unique_count))
        except Exception:
            continue
    return '\n'.join(stats_lines) if stats_lines else ""


def _format_data_table(headers, data, total_rows):
    if not data:
        return "No data"
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in data:
        row_vals = []
        for h in headers:
            val = row.get(h, "")
            if isinstance(val, float):
                if val >= 1000000:
                    val = "{:.2f}M".format(val/1000000)
                elif val >= 1000:
                    val = "{:,.0f}".format(val)
                elif abs(val) >= 1:
                    val = "{:.2f}".format(val)
                elif abs(val) >= 0.01:
                    val = "{:.4f}".format(val)
                elif abs(val) > 0:
                    val = "{:.6f}".format(val)
                else:
                    val = "0"
            row_vals.append(str(val))
        lines.append("| " + " | ".join(row_vals) + " |")
    if len(data) < total_rows:
        lines.append("\n*... total {} rows, showing first {} ...*".format(total_rows, len(data)))
    return '\n'.join(lines)


def _to_num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "").replace("%", "")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def _norm_name(name):
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(name or "")).strip("_")


def _is_date_like_col(name):
    n = _norm_name(name)
    return any(k in n for k in ("dt", "date", "day", "week", "month", "time", "hour"))


def _is_ratio_like_col(name):
    n = _norm_name(name)
    keys = ("ctr", "rate", "ratio", "pct", "percent", "avg", "mean", "per_", "_per", "arpu", "arppu")
    return any(k in n for k in keys)


def _is_numeric_column(col, data):
    vals = [row.get(col) for row in data if isinstance(row, dict)]
    if not vals:
        return False
    total = 0
    ok = 0
    for v in vals:
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        total += 1
        if _to_num(v) is not None:
            ok += 1
    return total > 0 and ok >= max(1, int(total * 0.7))


def _pick_group_columns(headers, data, numeric_cols):
    # 优先常见分组列；其次选一个低基数字段（排除日期列）
    preferred = (
        "exp_tag",
        "group",
        "variant",
        "bucket",
        "recall_type",
        "app_version",
        "scene",
        "channel",
        "source",
        "strategy",
        "version",
    )
    norm_to_header = {_norm_name(h): h for h in headers}
    for p in preferred:
        if p in norm_to_header:
            return [norm_to_header[p]]

    nrows = max(1, len(data))
    cands = []
    for h in headers:
        if h in numeric_cols:
            continue
        if _is_date_like_col(h):
            continue
        uniq = len(set(str((row or {}).get(h, "")).strip() for row in data if isinstance(row, dict)))
        if 2 <= uniq <= min(12, max(3, nrows // 2 + 1)):
            cands.append((uniq, len(str(h)), h))
    if cands:
        cands.sort(key=lambda x: (x[0], -x[1]))
        return [cands[0][2]]
    return []


def _pick_col_by_keywords(cols, *keywords):
    ks = tuple(_norm_name(k) for k in keywords if k)
    if not ks:
        return None
    for c in cols:
        n = _norm_name(c)
        if all(k in n for k in ks):
            return c
    # 仅单关键词时允许宽松匹配，避免 ("total","user") 误命中任意 total_* 列
    if len(ks) == 1:
        for c in cols:
            n = _norm_name(c)
            if ks[0] in n:
                return c
    return None


def _infer_ratio_formula(metric_col, additive_cols):
    """为比率类列自动推导 (num_col, den_col, formula_desc)；无法推导则返回 (None, None, '')."""
    n = _norm_name(metric_col)
    cols = list(additive_cols)

    if "ctr" in n:
        num = _pick_col_by_keywords(cols, "click") or _pick_col_by_keywords(cols, "clk")
        den = _pick_col_by_keywords(cols, "view") or _pick_col_by_keywords(cols, "expose") or _pick_col_by_keywords(cols, "pv")
        if num and den:
            return num, den, "{}/{}".format(num, den)

    if n.startswith("avg_"):
        base = n[4:]
        # avg_x_per_user -> total_x / uv(user_cnt)
        if "_per_" in base:
            left, _, right = base.partition("_per_")
            num = _pick_col_by_keywords(cols, "total", left) or _pick_col_by_keywords(cols, "sum", left) or _pick_col_by_keywords(cols, left)
            if right in ("user", "users", "uv", "person", "people"):
                den = (
                    _pick_col_by_keywords(cols, "uv")
                    or _pick_col_by_keywords(cols, "user", "cnt")
                    or _pick_col_by_keywords(cols, "user", "count")
                    or _pick_col_by_keywords(cols, "user")
                )
            elif right in ("view", "views", "expose", "pv"):
                den = (
                    _pick_col_by_keywords(cols, "total", "view")
                    or _pick_col_by_keywords(cols, "view")
                    or _pick_col_by_keywords(cols, "expose")
                    or _pick_col_by_keywords(cols, "pv")
                )
            elif right in ("click", "clicks", "clk"):
                den = _pick_col_by_keywords(cols, "total", "click") or _pick_col_by_keywords(cols, "click") or _pick_col_by_keywords(cols, "clk")
            else:
                den = _pick_col_by_keywords(cols, "total", right) or _pick_col_by_keywords(cols, right)
        else:
            num = _pick_col_by_keywords(cols, "total", base) or _pick_col_by_keywords(cols, "sum", base) or _pick_col_by_keywords(cols, base)
            den = _pick_col_by_keywords(cols, "uv") or _pick_col_by_keywords(cols, "user", "cnt") or _pick_col_by_keywords(cols, "user")
        if num and den:
            return num, den, "{}/{}".format(num, den)

    if "_per_" in n:
        left, _, right = n.partition("_per_")
        num = _pick_col_by_keywords(cols, "total", left) or _pick_col_by_keywords(cols, left)
        if right in ("user", "users", "uv", "person", "people"):
            den = (
                _pick_col_by_keywords(cols, "uv")
                or _pick_col_by_keywords(cols, "user", "cnt")
                or _pick_col_by_keywords(cols, "user", "count")
                or _pick_col_by_keywords(cols, "user")
            )
        elif right in ("view", "views", "expose", "pv"):
            den = (
                _pick_col_by_keywords(cols, "total", "view")
                or _pick_col_by_keywords(cols, "view")
                or _pick_col_by_keywords(cols, "expose")
                or _pick_col_by_keywords(cols, "pv")
            )
        elif right in ("click", "clicks", "clk"):
            den = _pick_col_by_keywords(cols, "total", "click") or _pick_col_by_keywords(cols, "click") or _pick_col_by_keywords(cols, "clk")
        else:
            den = _pick_col_by_keywords(cols, "total", right) or _pick_col_by_keywords(cols, right)
        if num and den:
            return num, den, "{}/{}".format(num, den)

    if n.endswith("_rate") or n.endswith("_ratio") or n.endswith("_pct"):
        base = n.rsplit("_", 1)[0]
        num = _pick_col_by_keywords(cols, base, "cnt") or _pick_col_by_keywords(cols, base, "num")
        den = _pick_col_by_keywords(cols, base, "total") or _pick_col_by_keywords(cols, "total", base)
        if num and den:
            return num, den, "{}/{}".format(num, den)

    return None, None, ""


def _build_authoritative_group_aggregate(headers, data):
    """
    通用累计口径块：
    - 自动识别分组列（优先 exp_tag/group/recall_type/app_version 等）
    - 可加总指标直接 SUM
    - 比率/均值类指标尝试按分子分母重算（而非逐日相加）
    """
    if not data:
        return ""

    has_dt = "dt" in headers
    numeric_cols = [h for h in headers if _is_numeric_column(h, data)]
    ratio_cols = [h for h in numeric_cols if _is_ratio_like_col(h)]
    additive_cols = [h for h in numeric_cols if h not in ratio_cols]
    if not additive_cols and not ratio_cols:
        return ""

    group_cols = _pick_group_columns(headers, data, numeric_cols)

    groups = {}

    for row in data:
        if not isinstance(row, dict):
            continue
        key = tuple(str(row.get(c, "")).strip() for c in group_cols) if group_cols else ("ALL",)
        if key not in groups:
            groups[key] = {"_rows": 0, "_dts": set()}
            for c in additive_cols:
                groups[key][c] = 0.0
        acc = groups[key]
        acc["_rows"] += 1
        if has_dt:
            dt = row.get("dt")
            if dt is not None and str(dt).strip():
                acc["_dts"].add(str(dt).strip())
        for c in additive_cols:
            n = _to_num(row.get(c))
            if n is not None:
                acc[c] += n

    if not groups:
        return ""

    # 推导可重算的比率列
    recalc_specs = []
    for c in ratio_cols:
        num_col, den_col, formula = _infer_ratio_formula(c, additive_cols)
        if num_col and den_col:
            recalc_specs.append((c, num_col, den_col, formula))

    # 控制输出宽度，避免 prompt 过大
    additive_show = additive_cols[:10]
    recalc_show = recalc_specs[:6]

    lines = []
    lines.append("【程序预计算累计（权威）】")
    lines.append("以下累计值由程序直接聚合；比率/均值类按分子分母重算，严禁逐日相加。")
    lines.append("")

    head = []
    if group_cols:
        head.extend(group_cols)
    else:
        head.append("group")
    if has_dt:
        head.append("days")
    head.append("rows")
    head.extend(["{}__sum".format(c) for c in additive_show])
    head.extend(["{}__recalc".format(c) for c, _, _, _ in recalc_show])
    lines.append("| " + " | ".join(head) + " |")
    lines.append("| " + " | ".join(["---"] * len(head)) + " |")

    for key in sorted(groups.keys()):
        acc = groups[key]
        row = []
        if group_cols:
            row.extend(list(key))
        else:
            row.append("ALL")
        if has_dt:
            row.append(str(len(acc["_dts"])))
        row.append(str(int(acc["_rows"])))
        for c in additive_show:
            row.append("{:.6f}".format(acc[c]))
        for _, num_col, den_col, _ in recalc_show:
            den = acc.get(den_col, 0.0)
            val = (acc.get(num_col, 0.0) / den) if den else 0.0
            row.append("{:.6f}".format(val))
        lines.append("| " + " | ".join(row) + " |")

    if recalc_show:
        lines.append("")
        lines.append("重算公式：")
        for metric_col, num_col, den_col, formula in recalc_show:
            lines.append("- {} = {}（分组累计后重算）".format(metric_col, formula or "{}/{}".format(num_col, den_col)))

    return "\n".join(lines)


def _build_analysis_user_message(
    original_question,
    sql,
    headers,
    data_to_analyze,
    total_rows,
    table_display_rows,
    chart_type,
):
    headers_str = ", ".join(headers)
    stats_summary = _calculate_basic_stats(headers, data_to_analyze)
    aggregate_block = _build_authoritative_group_aggregate(headers, data_to_analyze)
    slice_for_table = data_to_analyze[:table_display_rows]
    data_summary = _format_data_table(headers, slice_for_table, total_rows)
    used_rows = len(data_to_analyze)
    shown_rows = len(slice_for_table)
    stats_block = "\n基础统计:\n{}\n".format(stats_summary) if stats_summary else ""
    aggregate_text = "\n{}\n".format(aggregate_block) if aggregate_block else ""
    return (
        "【用户问题】\n{question}\n\n"
        "【数据表头】\n{headers}\n\n"
        "【数据内容】\n"
        "执行的SQL:\n{sql}\n"
        "数据规模: 查询返回共 {total} 行；本次分析对其中的前 {used} 行计算基础统计。"
        "下方 Markdown 明细表展示其中前 {shown} 行（列较多时会自动减少展示行数以控制长度）。\n"
        "若 {total} > {used}，未纳入分析样本的尾部行可能包含重要信息，结论需谨慎外推。\n"
        "{stats}"
        "{agg}"
        "\n数据明细:\n{data}\n\n"
        "【图表类型】\n{chart}\n\n"
        "请严格基于上述真实数据进行分析，必须引用具体数值，禁止使用占位符。"
    ).format(
        question=original_question,
        headers=headers_str,
        sql=sql,
        total=total_rows,
        used=used_rows,
        shown=shown_rows,
        stats=stats_block,
        agg=aggregate_text,
        data=data_summary,
        chart=chart_type or "自动",
    )


def analyze_data(original_question, sql, headers, data, chart_type=None, max_rows=QUERY_RESULT_MAX_ROWS, llm_model=None):
    """Generate analysis report from query results. Returns dict with success, report, error, analysis_time.

    llm_model: 可选，指定调用哪个大模型；为空时使用配置默认值。
    """
    start_time = time.time()

    if not data or not headers:
        return {
            "success": False,
            "error": "No data to analyze",
            "report": "",
            "analysis_time": time.time() - start_time,
        }

    data_to_analyze = data[:max_rows]
    total_rows = len(data)
    num_columns = len(headers)
    table_display_rows = _prompt_table_row_cap(num_columns, len(data_to_analyze))

    user_message = _build_analysis_user_message(
        original_question,
        sql,
        headers,
        data_to_analyze,
        total_rows,
        table_display_rows,
        chart_type,
    )

    agent = DeepAgent(
        name="AnalysisAgent",
        system_prompt=ANALYSIS_SYSTEM_PROMPT,
        model=llm_model or LLM_MODEL,
        temperature=0.5,
        max_tokens=4000,
    )

    max_retries = 3
    for attempt in range(max_retries):
        try:
            report = agent.simple_chat(
                user_message=user_message,
                system_prompt=ANALYSIS_SYSTEM_PROMPT,
            )

            if not report:
                if attempt < max_retries - 1:
                    table_display_rows = max(2, table_display_rows // 2)
                    user_message = _build_analysis_user_message(
                        original_question,
                        sql,
                        headers,
                        data_to_analyze,
                        total_rows,
                        table_display_rows,
                        chart_type,
                    )
                    continue
                return {
                    "success": False,
                    "error": "AI returned empty content",
                    "report": "",
                    "analysis_time": time.time() - start_time,
                }

            # Check if AI accidentally returned SQL JSON instead of report
            stripped = report.strip()
            if stripped.startswith('{') and '"sql"' in stripped:
                logger.warning("Analysis agent returned SQL format instead of report")
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, dict) and "sql" in parsed:
                        report = (
                            "## Data Analysis Report\n\n"
                            "### Analysis\n{}\n\n"
                            "### Execution Suggestion\n{}\n"
                        ).format(
                            parsed.get("explanation", ""),
                            parsed.get("execution_plan", ""),
                        )
                except Exception:
                    pass

            return {
                "success": True,
                "report": report,
                "error": None,
                "analysis_time": time.time() - start_time,
            }

        except Exception as e:
            logger.error("Analysis attempt %d failed: %s", attempt + 1, e)
            if attempt >= max_retries - 1:
                return {
                    "success": False,
                    "error": str(e),
                    "report": "",
                    "analysis_time": time.time() - start_time,
                }

    return {
        "success": False,
        "error": "Max retries exceeded",
        "report": "",
        "analysis_time": time.time() - start_time,
    }
