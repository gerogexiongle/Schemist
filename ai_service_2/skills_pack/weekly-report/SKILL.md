---
id: weekly-report
name: 联机推荐大厅分发周报
description: 联机大厅推荐页 (api_type=40001) 各召回通路 3 周环比 — 推荐量占比 / 曝光量占比 / 点击率
icon: 📅
placeholder: 例如：以 2026-04-12 为本周结束日，出联机推荐大厅 3 周分发周报
enabled: true
order: 40
tags:
  - 周报
  - 联机大厅
  - 召回通路
  - 3周环比
  - api_type=40001
---

【任务】产出「**联机推荐大厅分发周报**」——联机大厅推荐页 (`api_type='40001'`) 各召回通路最近 3 周对比。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}
【今天日期】{{today}}

---

## 固定业务信息（已在生产验证，**不要改也不要再去搜其他表**）

- **表**：`ads_algo_cn.ads_algo_map_game_reco_metrics_s_d`（推荐指标日表；`dt` 为分区，`yyyy-MM-dd` 字符串）
- **场景过滤**：`api_type = '40001'` ← 业务口头「联机大厅推荐页」/「API 4001」在此表落库为**五位字符串 '40001'**，千万不要写成 '4001'
- **维度**：`recall_type`（召回通路）
- **指标原字段**：`recos`（推荐量）、`views`（曝光量）、`clicks`（点击量）

> 本技能**禁止再调 `search_tables` / `get_table_info`**（表结构已固化）；直接按下面骨架替换 `end_date` 即可。

---

## 3 周窗口口径

以「本周结束日」为 `end_date`。解析规则：
- 若用户问题里给了具体日期（例如 `2026-04-12`），用它。
- 否则用 `{{today}}` 作默认。

| 窗口 | CTE | 区间 | 显示位置 |
|---|---|---|---|
| 本周 | `current_7d` | `[end_date - 6 天, end_date]` 共 7 天 | 最左列 |
| 上周 | `prev_7d` | `[end_date - 13 天, end_date - 7 天]` | 中间列 |
| 上上周 | `prev2_7d` | `[end_date - 20 天, end_date - 14 天]` | 最右列 |

explanation 必须**原文列出 3 段具体日期**（例："本周 2026-04-06 ~ 2026-04-12；上周 2026-03-30 ~ 2026-04-05；上上周 2026-03-23 ~ 2026-03-29"）。

---

## 指标口径

每个召回通路每周输出 3 个百分比字符串，**保留 3 位小数**，格式 `"xx.xxx%"`，NULL → `'N/A'`，分母为 0 → `'0.000%'`：

1. **推荐量占比** = 本通路该周 SUM(recos) ÷ 该周全通路 SUM(recos) × 100%
2. **曝光量占比** = 本通路该周 SUM(views) ÷ 该周全通路 SUM(views) × 100%
3. **点击率 CTR** = 本通路该周 SUM(clicks) ÷ **同通路同周** SUM(views) × 100%

**排序**：按本周推荐量占比降序（`ORDER BY COALESCE(c.current_recos, 0.0) / tc.total_recos_current DESC`）。

输出固定 10 列（顺序不可打乱）：
`recall_type | current_recos_ratio | current_views_ratio | current_ctr | prev_recos_ratio | prev_views_ratio | prev_ctr | prev2_recos_ratio | prev2_views_ratio | prev2_ctr`

---

## ★ Trino 引擎 SQL 骨架（已生产验证）

把 `SELECT CAST(CAST('{{today}}' AS TIMESTAMP) AS DATE) AS end_date` 里的日期字符串替换为用户指定的「本周结束日」即可。

```sql
WITH params AS (
    -- 【替换点】若用户指定了具体日期（如 2026-04-12），把下面 '{{today}}' 改为该日期
    SELECT CAST(CAST('{{today}}' AS TIMESTAMP) AS DATE) AS end_date
),
current_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS current_recos,
        CAST(SUM(clicks) AS DOUBLE) AS current_clicks,
        CAST(SUM(views)  AS DOUBLE) AS current_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(end_date - INTERVAL '6'  DAY, '%Y-%m-%d')
                 AND date_format(end_date                 , '%Y-%m-%d')
    GROUP BY recall_type
),
prev_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS prev_recos,
        CAST(SUM(clicks) AS DOUBLE) AS prev_clicks,
        CAST(SUM(views)  AS DOUBLE) AS prev_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(end_date - INTERVAL '13' DAY, '%Y-%m-%d')
                 AND date_format(end_date - INTERVAL '7'  DAY, '%Y-%m-%d')
    GROUP BY recall_type
),
prev2_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS prev2_recos,
        CAST(SUM(clicks) AS DOUBLE) AS prev2_clicks,
        CAST(SUM(views)  AS DOUBLE) AS prev2_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(end_date - INTERVAL '20' DAY, '%Y-%m-%d')
                 AND date_format(end_date - INTERVAL '14' DAY, '%Y-%m-%d')
    GROUP BY recall_type
),
total_current AS (SELECT SUM(current_recos) AS total_recos_current, SUM(current_views) AS total_views_current FROM current_7d),
total_prev    AS (SELECT SUM(prev_recos)    AS total_recos_prev,    SUM(prev_views)    AS total_views_prev    FROM prev_7d),
total_prev2   AS (SELECT SUM(prev2_recos)   AS total_recos_prev2,   SUM(prev2_views)   AS total_views_prev2   FROM prev2_7d),
all_types AS (
    SELECT recall_type FROM current_7d UNION
    SELECT recall_type FROM prev_7d    UNION
    SELECT recall_type FROM prev2_7d
)
SELECT a.recall_type,
    -- 本周（最左）
    CASE WHEN c.current_recos IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', c.current_recos  * 100.0 / tc.total_recos_current ), '%') END AS current_recos_ratio,
    CASE WHEN c.current_views IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', c.current_views  * 100.0 / tc.total_views_current ), '%') END AS current_views_ratio,
    CASE WHEN c.current_views IS NULL THEN 'N/A'
         WHEN c.current_views = 0     THEN '0.000%'
         ELSE concat(format('%.3f', c.current_clicks * 100.0 / c.current_views        ), '%') END AS current_ctr,
    -- 上周（中间）
    CASE WHEN p.prev_recos    IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', p.prev_recos     * 100.0 / tp.total_recos_prev    ), '%') END AS prev_recos_ratio,
    CASE WHEN p.prev_views    IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', p.prev_views     * 100.0 / tp.total_views_prev    ), '%') END AS prev_views_ratio,
    CASE WHEN p.prev_views    IS NULL THEN 'N/A'
         WHEN p.prev_views    = 0     THEN '0.000%'
         ELSE concat(format('%.3f', p.prev_clicks    * 100.0 / p.prev_views           ), '%') END AS prev_ctr,
    -- 上上周（最右）
    CASE WHEN p2.prev2_recos  IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', p2.prev2_recos   * 100.0 / tp2.total_recos_prev2  ), '%') END AS prev2_recos_ratio,
    CASE WHEN p2.prev2_views  IS NULL THEN 'N/A'
         ELSE concat(format('%.3f', p2.prev2_views   * 100.0 / tp2.total_views_prev2  ), '%') END AS prev2_views_ratio,
    CASE WHEN p2.prev2_views  IS NULL THEN 'N/A'
         WHEN p2.prev2_views  = 0     THEN '0.000%'
         ELSE concat(format('%.3f', p2.prev2_clicks  * 100.0 / p2.prev2_views         ), '%') END AS prev2_ctr
FROM all_types a
    LEFT JOIN current_7d c  ON a.recall_type = c.recall_type
    LEFT JOIN prev_7d    p  ON a.recall_type = p.recall_type
    LEFT JOIN prev2_7d   p2 ON a.recall_type = p2.recall_type
    CROSS JOIN total_current tc
    CROSS JOIN total_prev    tp
    CROSS JOIN total_prev2   tp2
ORDER BY COALESCE(c.current_recos, 0.0) / tc.total_recos_current DESC
```

---

## ★ Spark SQL 引擎 SQL 骨架

与 Trino 逻辑 1:1 等价，仅替换引擎方言：
- `date_format(..., '%Y-%m-%d')` → `date_format(..., 'yyyy-MM-dd')`
- `CAST(CAST('..' AS TIMESTAMP) AS DATE)` → `to_date('..')`
- `end_date - INTERVAL 'N' DAY` → `date_sub(end_date, N)`
- `format('%.3f', x)` → `format_number(x, 3)`

```sql
WITH params AS (
    -- 【替换点】若用户指定了具体日期（如 2026-04-12），把下面 '{{today}}' 改为该日期
    SELECT to_date('{{today}}') AS end_date
),
current_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS current_recos,
        CAST(SUM(clicks) AS DOUBLE) AS current_clicks,
        CAST(SUM(views)  AS DOUBLE) AS current_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(date_sub(end_date, 6), 'yyyy-MM-dd')
                 AND date_format(end_date,              'yyyy-MM-dd')
    GROUP BY recall_type
),
prev_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS prev_recos,
        CAST(SUM(clicks) AS DOUBLE) AS prev_clicks,
        CAST(SUM(views)  AS DOUBLE) AS prev_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(date_sub(end_date, 13), 'yyyy-MM-dd')
                 AND date_format(date_sub(end_date, 7),  'yyyy-MM-dd')
    GROUP BY recall_type
),
prev2_7d AS (
    SELECT recall_type,
        CAST(SUM(recos)  AS DOUBLE) AS prev2_recos,
        CAST(SUM(clicks) AS DOUBLE) AS prev2_clicks,
        CAST(SUM(views)  AS DOUBLE) AS prev2_views
    FROM ads_algo_cn.ads_algo_map_game_reco_metrics_s_d
    CROSS JOIN params
    WHERE api_type = '40001'
      AND dt BETWEEN date_format(date_sub(end_date, 20), 'yyyy-MM-dd')
                 AND date_format(date_sub(end_date, 14), 'yyyy-MM-dd')
    GROUP BY recall_type
),
total_current AS (SELECT SUM(current_recos) AS total_recos_current, SUM(current_views) AS total_views_current FROM current_7d),
total_prev    AS (SELECT SUM(prev_recos)    AS total_recos_prev,    SUM(prev_views)    AS total_views_prev    FROM prev_7d),
total_prev2   AS (SELECT SUM(prev2_recos)   AS total_recos_prev2,   SUM(prev2_views)   AS total_views_prev2   FROM prev2_7d),
all_types AS (
    SELECT recall_type FROM current_7d UNION
    SELECT recall_type FROM prev_7d    UNION
    SELECT recall_type FROM prev2_7d
)
SELECT a.recall_type,
    -- 本周（最左）
    CASE WHEN c.current_recos IS NULL THEN 'N/A'
         ELSE concat(format_number(c.current_recos  * 100.0 / tc.total_recos_current, 3), '%') END AS current_recos_ratio,
    CASE WHEN c.current_views IS NULL THEN 'N/A'
         ELSE concat(format_number(c.current_views  * 100.0 / tc.total_views_current, 3), '%') END AS current_views_ratio,
    CASE WHEN c.current_views IS NULL THEN 'N/A'
         WHEN c.current_views = 0     THEN '0.000%'
         ELSE concat(format_number(c.current_clicks * 100.0 / c.current_views, 3),        '%') END AS current_ctr,
    -- 上周（中间）
    CASE WHEN p.prev_recos    IS NULL THEN 'N/A'
         ELSE concat(format_number(p.prev_recos     * 100.0 / tp.total_recos_prev, 3),    '%') END AS prev_recos_ratio,
    CASE WHEN p.prev_views    IS NULL THEN 'N/A'
         ELSE concat(format_number(p.prev_views     * 100.0 / tp.total_views_prev, 3),    '%') END AS prev_views_ratio,
    CASE WHEN p.prev_views    IS NULL THEN 'N/A'
         WHEN p.prev_views    = 0     THEN '0.000%'
         ELSE concat(format_number(p.prev_clicks    * 100.0 / p.prev_views, 3),           '%') END AS prev_ctr,
    -- 上上周（最右）
    CASE WHEN p2.prev2_recos  IS NULL THEN 'N/A'
         ELSE concat(format_number(p2.prev2_recos   * 100.0 / tp2.total_recos_prev2, 3),  '%') END AS prev2_recos_ratio,
    CASE WHEN p2.prev2_views  IS NULL THEN 'N/A'
         ELSE concat(format_number(p2.prev2_views   * 100.0 / tp2.total_views_prev2, 3),  '%') END AS prev2_views_ratio,
    CASE WHEN p2.prev2_views  IS NULL THEN 'N/A'
         WHEN p2.prev2_views  = 0     THEN '0.000%'
         ELSE concat(format_number(p2.prev2_clicks  * 100.0 / p2.prev2_views, 3),         '%') END AS prev2_ctr
FROM all_types a
    LEFT JOIN current_7d c  ON a.recall_type = c.recall_type
    LEFT JOIN prev_7d    p  ON a.recall_type = p.recall_type
    LEFT JOIN prev2_7d   p2 ON a.recall_type = p2.recall_type
    CROSS JOIN total_current tc
    CROSS JOIN total_prev    tp
    CROSS JOIN total_prev2   tp2
ORDER BY COALESCE(c.current_recos, 0.0) / tc.total_recos_current DESC
```

---

## LLM 执行步骤（严格按此）

1. **解析本周结束日**：从「用户问题」里提取日期（如 `2026-04-12`）；若没给，用 `{{today}}`。
2. **选引擎**：当前是 `{{engine}}`。`trino` 取上面 Trino 骨架；`spark` 取 Spark 骨架。
3. **替换 end_date**：把 `params` CTE 里 `'{{today}}'` 替换为步骤 1 得到的日期字符串（**只改这一处**）。
4. **输出 JSON**（禁止用「上述 SQL / 如上 / (略)」占位短语代指，必须完整写出）：
   ```
   {
     "sql": "<完整 SQL>",
     "explanation": "3 段日期范围 + 口径说明",
     "tables_used": ["ads_algo_cn.ads_algo_map_game_reco_metrics_s_d"],
     "execution_plan": ""
   }
   ```

---

## 分析报告阶段要求（analyze-data）

- 「一、数据概览」第一句写明 3 周具体日期范围 + **本周 TOP 3 推荐量占比**的召回通路。
- 「二、关键发现」至少覆盖：
  1. 本周 vs 上周 vs 上上周 **推荐量占比涨跌幅 TOP 3** 的通路（含数值）。
  2. 本周 **CTR 最高 / 最低** 的通路（并看 3 周 CTR 趋势）。
  3. 若某通路本周占比掉量级（如 12% → 1%）或 CTR 翻倍/腰斩，**单独点名** + 业务假设。
- 结尾一句"下一步可追问"（例如：按 app_version 细分、按小时波动、按 exp_id 细分）。
