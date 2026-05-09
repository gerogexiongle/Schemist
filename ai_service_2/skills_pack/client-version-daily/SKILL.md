---
id: client-version-daily
name: 客户端版本差异分析日报
description: 首页大地图卡（FRONT_PAGE_FEED.MapCard）分 app_version 的曝光/点击/UV/CTR 日报
icon: 📱
placeholder: 例如：分析 2026-04-23 首页地图卡各客户端版本 CTR 差异（不填默认昨天）
enabled: true
order: 45
engine_hint: trino
tags:
  - 日报
  - 客户端版本
  - app_version
  - FRONT_PAGE_FEED
  - MapCard
  - CTR
---

【任务】产出「**客户端版本差异分析日报**」——首页大地图卡（`card_id='FRONT_PAGE_FEED'` + `comp_id='MapCard'`）按客户端版本 `app_version` 聚合的 **曝光 / 点击 / UV / CTR** 日指标，用于定位不同 app 版本间的分发与点击率差异。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}
【今天日期】{{today}}
【默认分析日期】{{yesterday}}（数据 T-1 就绪；除非用户指定其他日期，否则统计这一天）

---

## 固定业务信息（已在 Trino 生产验证，**不要改也不要再调 `search_tables` / `get_table_info` 查别的表**）

- **明细表**：`dwd_cn.dwd_client_70000_scene_18_dtl_i_d`（客户端前端打点明细；`dt` 为分区，`yyyy-MM-dd` 字符串）
  - ⚠️ **不要带 `hive.` / `trino.` 前缀**，直接 `dwd_cn.dwd_client_70000_scene_18_dtl_i_d`
- **场景过滤（固定，不要动）**：
  - 曝光：`card_id = 'FRONT_PAGE_FEED' AND comp_id = 'MapCard' AND event_code = 'view'`
  - 点击：`card_id = 'FRONT_PAGE_FEED' AND comp_id IN ('MapCard','JoinButton') AND event_code = 'click'`
- **曝光 cid 需要拆分**：`cid` 字段在 view 事件里是**逗号分隔的多个 map_id**（一次请求可能曝光多张地图卡），需用 `CROSS JOIN UNNEST(SPLIT(cid, ','))`（Trino）或 `LATERAL VIEW EXPLODE(SPLIT(cid, ','))`（Spark）炸开；**click 事件里 `cid` 就是单个 map_id**，不需要 split。
- **去重键**：view / click 都按 `(uin, dt, request_id, app_version, map_id)` DISTINCT，避免日志重复。
- **JOIN 条件**：view 表 LEFT JOIN click 表，ON `(uin, dt, request_id, map_id)` 四键同值。⚠️ 不要把 `app_version` 放进 ON —— 点击事件的 app_version 理论上和 view 一致，但为了避免极小概率的设备升级回写差异丢匹配，只用四键。
- **维度**：`dt`（分区日期）、`app_version`（客户端版本号）。

---

## 时间窗口口径

以「分析日期」为单日（日报）。解析规则：

1. 若用户问题里明确给了具体日期（如 `2026-04-23`、`昨天`、`前天`、日期区间），用它；
2. 否则默认 `{{yesterday}}`（即 T-1，数据通常在凌晨就绪）；
3. 若用户指定的是**区间**（如「最近 3 天」「4-20 到 4-22」），`params` CTE 用 `start_date` / `end_date` 两列，`dt BETWEEN start_date AND end_date` 即可，**其他逻辑完全不变**，最终 GROUP BY 仍带 `dt`。

**explanation 必须原文列出**分析日期或日期区间（例："分析日期：2026-04-23（单日）"）。

---

## 指标口径（输出固定 6 列，顺序不可打乱）

| # | 列名 | 含义 |
|---|---|---|
| 1 | `dt` | 分区日期 |
| 2 | `app_version` | 客户端版本号 |
| 3 | `view_cnt` | 去重后曝光条数（`COUNT(*)` on `view_dedup`） |
| 4 | `click_cnt` | 命中点击的曝光条数（`SUM(is_clicked)`） |
| 5 | `uv` | 独立曝光用户数（`COUNT(DISTINCT uin)`） |
| 6 | `ctr` | 点击率 = `click_cnt / view_cnt * 100%`；保留 **2 位小数**；分母 0 输出 `'0.00%'` |

**排序**：`ORDER BY dt ASC, view_cnt DESC`（按日期升序，同一天内按曝光量倒序，方便一眼看到大盘版本）。

---

## ★ Trino 引擎 SQL 骨架（用户生产已验证，**默认直接用**）

把 `params` CTE 里的 `start_date` / `end_date` 换成用户指定日期即可；单日日报两者相同。

```sql
WITH params AS (
    -- 【替换点】单日默认 {{yesterday}}；用户指定区间时改这两行
    SELECT DATE '{{yesterday}}' AS start_date,
           DATE '{{yesterday}}' AS end_date
),
view_exploded AS (
    SELECT t.uin,
           t.dt,
           t.request_id,
           t.app_version,
           TRIM(m.map_id) AS map_id
    FROM dwd_cn.dwd_client_70000_scene_18_dtl_i_d AS t
    CROSS JOIN params
    CROSS JOIN UNNEST(SPLIT(t.cid, ',')) AS m(map_id)
    WHERE t.dt BETWEEN date_format(start_date, '%Y-%m-%d')
                   AND date_format(end_date,   '%Y-%m-%d')
      AND t.card_id = 'FRONT_PAGE_FEED'
      AND t.comp_id = 'MapCard'
      AND t.event_code = 'view'
      AND t.cid IS NOT NULL AND t.cid <> ''
),
view_dedup AS (
    SELECT DISTINCT uin, dt, request_id, app_version, map_id
    FROM view_exploded
    WHERE map_id IS NOT NULL AND map_id <> ''
),
click_dedup AS (
    SELECT DISTINCT t.uin, t.dt, t.request_id, t.app_version, t.cid AS map_id
    FROM dwd_cn.dwd_client_70000_scene_18_dtl_i_d AS t
    CROSS JOIN params
    WHERE t.dt BETWEEN date_format(start_date, '%Y-%m-%d')
                   AND date_format(end_date,   '%Y-%m-%d')
      AND t.card_id = 'FRONT_PAGE_FEED'
      AND t.comp_id IN ('MapCard', 'JoinButton')
      AND t.event_code = 'click'
      AND t.cid IS NOT NULL AND t.cid <> ''
),
joined AS (
    SELECT v.dt,
           v.app_version,
           v.uin,
           v.request_id,
           v.map_id,
           CASE WHEN c.uin IS NOT NULL THEN 1 ELSE 0 END AS is_clicked
    FROM view_dedup AS v
    LEFT JOIN click_dedup AS c
      ON v.uin = c.uin
     AND v.dt  = c.dt
     AND v.request_id = c.request_id
     AND v.map_id = c.map_id
)
SELECT j.dt                                            AS dt,
       j.app_version                                   AS app_version,
       COUNT(*)                                        AS view_cnt,
       SUM(j.is_clicked)                               AS click_cnt,
       COUNT(DISTINCT j.uin)                           AS uv,
       CASE WHEN COUNT(*) = 0 THEN '0.00%'
            ELSE CONCAT(
                   FORMAT('%.2f',
                       CAST(SUM(j.is_clicked) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0)),
                   '%')
       END                                              AS ctr
FROM joined AS j
GROUP BY j.dt, j.app_version
ORDER BY j.dt ASC, view_cnt DESC
```

---

## ★ Spark SQL 引擎 SQL 骨架

与 Trino 逻辑 1:1 等价，仅替换方言：

- `CROSS JOIN UNNEST(SPLIT(...))` → `LATERAL VIEW EXPLODE(SPLIT(...))`
- `DATE '...'` → `to_date('...')`
- `date_format(x, '%Y-%m-%d')` → `date_format(x, 'yyyy-MM-dd')`
- `FORMAT('%.2f', x)` → `format_number(x, 2)`

```sql
WITH params AS (
    -- 【替换点】单日默认 {{yesterday}}；用户指定区间时改这两行
    SELECT to_date('{{yesterday}}') AS start_date,
           to_date('{{yesterday}}') AS end_date
),
view_exploded AS (
    SELECT t.uin,
           t.dt,
           t.request_id,
           t.app_version,
           TRIM(m.map_id) AS map_id
    FROM dwd_cn.dwd_client_70000_scene_18_dtl_i_d AS t
    CROSS JOIN params p
    LATERAL VIEW EXPLODE(SPLIT(t.cid, ',')) m AS map_id
    WHERE t.dt BETWEEN date_format(p.start_date, 'yyyy-MM-dd')
                   AND date_format(p.end_date,   'yyyy-MM-dd')
      AND t.card_id = 'FRONT_PAGE_FEED'
      AND t.comp_id = 'MapCard'
      AND t.event_code = 'view'
      AND t.cid IS NOT NULL AND t.cid <> ''
),
view_dedup AS (
    SELECT DISTINCT uin, dt, request_id, app_version, map_id
    FROM view_exploded
    WHERE map_id IS NOT NULL AND map_id <> ''
),
click_dedup AS (
    SELECT DISTINCT t.uin, t.dt, t.request_id, t.app_version, t.cid AS map_id
    FROM dwd_cn.dwd_client_70000_scene_18_dtl_i_d AS t
    CROSS JOIN params p
    WHERE t.dt BETWEEN date_format(p.start_date, 'yyyy-MM-dd')
                   AND date_format(p.end_date,   'yyyy-MM-dd')
      AND t.card_id = 'FRONT_PAGE_FEED'
      AND t.comp_id IN ('MapCard', 'JoinButton')
      AND t.event_code = 'click'
      AND t.cid IS NOT NULL AND t.cid <> ''
),
joined AS (
    SELECT v.dt,
           v.app_version,
           v.uin,
           v.request_id,
           v.map_id,
           CASE WHEN c.uin IS NOT NULL THEN 1 ELSE 0 END AS is_clicked
    FROM view_dedup AS v
    LEFT JOIN click_dedup AS c
      ON v.uin = c.uin
     AND v.dt  = c.dt
     AND v.request_id = c.request_id
     AND v.map_id = c.map_id
)
SELECT j.dt                                                           AS dt,
       j.app_version                                                  AS app_version,
       COUNT(*)                                                       AS view_cnt,
       SUM(j.is_clicked)                                              AS click_cnt,
       COUNT(DISTINCT j.uin)                                          AS uv,
       CASE WHEN COUNT(*) = 0 THEN '0.00%'
            ELSE concat(
                   format_number(
                       CAST(SUM(j.is_clicked) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0), 2),
                   '%')
       END                                                            AS ctr
FROM joined AS j
GROUP BY j.dt, j.app_version
ORDER BY j.dt ASC, view_cnt DESC
```

---

## LLM 执行步骤（严格按此）

1. **解析分析日期**：
   - 用户明确给日期 → 用它；
   - 用户说「昨天 / T-1 / 最近一天」→ 用 `{{yesterday}}`；
   - 用户说「今天」→ 用 `{{today}}`（数据可能未全量就绪，在 explanation 里提示）；
   - 用户说区间（例："最近 3 天"/"4-21 到 4-23"）→ `start_date` / `end_date` 分别填上下界；
   - 什么都没给 → 默认 `{{yesterday}}`，单日。
2. **选引擎**：当前是 `{{engine}}`。`trino` 取 Trino 骨架；`spark` 取 Spark 骨架。**不要混用方言**。
3. **替换日期**：只改 `params` CTE 里的两处日期，其他 CTE / JOIN / GROUP BY / ORDER BY 一律保持原样。
4. **禁止随意修改**：不要改表名、不要加 `hive.` 前缀、不要删 `NULLIF`、不要把 `app_version` 加到 LEFT JOIN 的 ON 里、不要把 `COUNT(DISTINCT uin)` 和 `COUNT(*)` 顺序打乱。
5. **输出 JSON**（禁止写「上述 SQL / 如上 / (略)」之类占位短语，必须把 SQL 完整写出）：
   ```
   {
     "sql": "<完整 SQL>",
     "explanation": "分析日期 + 口径说明 + 执行引擎",
     "tables_used": ["dwd_cn.dwd_client_70000_scene_18_dtl_i_d"],
     "execution_plan": ""
   }
   ```

---

## 分析报告阶段要求（analyze-data）

- 「一、数据概览」第一句明确分析日期 + 数据范围 + **本日曝光量 TOP 3 的 app_version**（含具体数字）。
- 「二、关键发现」至少覆盖：
  1. **CTR 最高 / 最低** 的 `app_version`（给数值，至少 Top3 + Bottom3），并**忽略曝光量过小**（例如 `view_cnt < 1000`）的长尾版本，避免误导。
  2. **新版本 vs 老版本** 的 CTR 对比（按 app_version 字符串可大致判断新老；若有明显新版本曝光占比高但 CTR 偏低，要点名）。
  3. 任何 **CTR 异常跳变**（与大盘偏离 ±20% 以上）的版本单独列出，给业务假设（例：新版本发布未完、埋点改版、机型适配问题等）。
- 「三、结论与建议」：给 1~3 条可执行建议，例如"把 CTR 明显偏低的新版本样本拉出来看埋点 / 推送下线老版本 / 灰度回滚"。
- 结尾一句「下一步可追问」，给出跨维度下钻建议（例：按 `map_id` 细分、按 `exp_id` / `ab_group` 细分、改为区间环比、按小时级波动）。
