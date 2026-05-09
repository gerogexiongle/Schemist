---
id: data-insights
name: 数据洞察
description: 一句话出 SQL，并生成带业务解读的分析报告；适合常规业务问数
icon: 📊
placeholder: 例如：近 7 天 API 4001 的推荐量与 CTR 趋势
enabled: true
order: 10
tags:
  - 问数
  - 报告
---

请基于数仓完成以下数据分析任务。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}
【今天日期】{{today}}

【通用口径与约束】
- 优先使用 dws 分层表，必须加 dt 分区范围。
- 占比 / CTR / 转化率必须写浮点除法：分子 `* 1.0` 或 `CAST(... AS DOUBLE) * 100.0`，避免 BIGINT 整数截断。
- 业务口头 `API 4001` / `监控 4001` 在落库里常为五位字符串 `'40001'`；不确定时先抽样 `SELECT DISTINCT api_type ... LIMIT 50` 核对，或 `api_type IN ('4001','40001')` 兼容两种口径。
- 结果行数较多时用 LIMIT 控制，TOP 场景显式 ORDER BY。

【⚠️ 引擎方言强制纪律（违反 → pre-check 直接拒）】

`dt` 在多数表中是 `varchar 'yyyy-MM-dd'` 分区，过滤时需要把日期算术结果再格式化回字符串。**两套引擎不能混用**：

| 用途 | Trino（`{{engine}}=trino`） | Spark（`{{engine}}=spark`） |
|---|---|---|
| 字符串 → 日期 | `DATE '2026-04-27'` 或 `CAST(CAST('2026-04-27' AS TIMESTAMP) AS DATE)` | `to_date('2026-04-27')` |
| 减 N 天 | `DATE '{{today}}' - INTERVAL '7' DAY` | `date_sub(to_date('{{today}}'), 7)` |
| 日期 → `'yyyy-MM-dd'` 字符串 | `date_format(d, '%Y-%m-%d')`（**MySQL 风格 `%Y-%m-%d`**） | `date_format(d, 'yyyy-MM-dd')`（**Java 风格 `yyyy-MM-dd`**） |
| 表名写法 | **`<db>.<table>`**（如 `dws_cn.xxx`；**不要** `hive.` / `trino.` 前缀） | 同上 |
| 字符串拼接 | `concat(...)` 或 `||` | `concat(...)`（不支持 `||`） |
| 数字格式化 | `format('%.2f', x)` | `format_number(x, 2)` |
| 数组炸开 | `CROSS JOIN UNNEST(SPLIT(c, ','))` | `LATERAL VIEW EXPLODE(SPLIT(c, ','))` |

- 近 7 天分区过滤的标准写法：
  - **Trino**：
    ```sql
    WHERE dt BETWEEN date_format(DATE '{{today}}' - INTERVAL '6' DAY, '%Y-%m-%d')
                 AND date_format(DATE '{{today}}'                   , '%Y-%m-%d')
    ```
  - **Spark**：
    ```sql
    WHERE dt BETWEEN date_format(date_sub(to_date('{{today}}'), 6), 'yyyy-MM-dd')
                 AND date_format(to_date('{{today}}')              , 'yyyy-MM-dd')
    ```
- **常见踩雷**（务必避免）：
  - 在 Spark 上写 `'%Y-%m-%d'` → 报 `Spark date_format pattern should use Java style like 'yyyy-MM-dd'`；
  - 在 Trino 上写 `date_sub('2026-04-27', 7)` → Trino 无此重载，需用 `INTERVAL '7' DAY` 或 `date_add('day', -7, ...)`；
  - 在 SQL 里随意加 `hive.<db>.<table>` 等 catalog 前缀 → 与本环境默认 catalog 不一致时**找不到表**；统一只写 `dws_cn.xxx` / `dwd_cn.xxx`；
  - 把 BIGINT 直接相除做 CTR → 整数截断结果恒为 0。

【输出要求】
- 产出**可直接执行**的 SQL，**严格匹配 `{{engine}}` 的方言**。
- 在 explanation 里用 2–3 句话解释 SQL 的业务含义与口径，并标注所用引擎。
- 若用户问题无法只凭数仓回答（如涉及埋点以外数据），明确指出并给出退化方案。
