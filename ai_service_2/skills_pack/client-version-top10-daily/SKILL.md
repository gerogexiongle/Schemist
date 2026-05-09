---
id: client-version-top10-daily
name: 客户端版本每日 TOP10 趋势
description: 基于 DWS 聚合表，分析多天每日曝光量 TOP10 客户端版本的曝光/点击/UV/CTR/版本占比
icon: 📈
placeholder: 例如：分析最近 7 天客户端版本 TOP10 的 CTR 与曝光占比变化（不填默认最近 7 天）
enabled: true
order: 46
engine_hint: trino
tags:
  - 多日趋势
  - 客户端版本
  - app_version
  - DWS
  - TOP10
  - 版本迁移
  - CTR
  - 曝光占比
---

【任务】产出「**客户端版本每日 TOP10 趋势分析**」——基于 DWS 聚合表 `dws_cn.dws_client_70000_front_page_feed_i_d`（**Trino / Spark 均直接写库表名，不要加 `hive.` 前缀**），按 `dt + app_version` 取每天**曝光量 TOP10** 的客户端版本，输出 **曝光 / 点击 / 曝光UV / 点击UV / CTR / UV-CTR / 版本曝光占比 / 版本人数占比** 8 个核心指标 + 当天排名 `rn`，用于跟踪 **多日版本迁移趋势**（哪个版本在涨、哪个在退、新版本 CTR 是否健康）。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}（**Trino 与 Spark 双引擎都支持**；按下方骨架选择即可）
【今天日期】{{today}}
【默认分析窗口】最近 7 天（`end_dt = {{yesterday}}`、`start_dt = end_dt - 6 天`）

---

## 固定业务信息（已生产验证，**不要改也不要再调 `search_tables` / `get_table_info` 查别的表**）

- **DWS 聚合表（已按用户/版本/日预聚合）**：`dws_cn.dws_client_70000_front_page_feed_i_d`
  - ⚠️ **Trino / Spark 统一**：表名一律 `dws_cn.dws_client_70000_front_page_feed_i_d`，**不要**加 `hive.` / `trino.` catalog 前缀（与本服务执行环境默认 catalog 一致）
  - `dt` 是分区列，类型 `varchar`，格式 `'yyyy-MM-dd'`
- **核心字段**：`dt`、`app_version`、`uin`、`view_time`、`click_time`
  - `view_time > 0` 视为**有效曝光** → 计 `expose_cnt`、`view_uv`
  - `click_time > 0` 视为**有效点击** → 计 `click_cnt`、`click_uv`
  - 一个 `uin` 当天只属于一个 `app_version`，所以日级 UV 直接 `COUNT(DISTINCT uin)` 不会双算
- **维度**：`dt`（分区日期）、`app_version`（客户端版本号）
- **TOP 口径**：每天按 `expose_cnt DESC` 取 `rn <= 10`（**曝光量 TOP10**，不是 UV TOP10，与用户原始 SQL 一致）

---

## 时间窗口口径

1. 用户明确给区间（如 `2026-04-20 到 2026-04-26`、「最近 14 天」）→ 用它，`start_dt` / `end_dt` 替换为对应日期；
2. 用户给单日 → `start_dt = end_dt = 该日`；
3. 用户说「最近 N 天」/「过去 N 天」 → `end_dt = {{yesterday}}`、`start_dt = end_dt - INTERVAL 'N-1' DAY`；
4. 什么都没给 → 默认 **最近 7 天**（`end_dt = {{yesterday}}`、`start_dt = {{yesterday}} - INTERVAL '6' DAY`）。

⚠️ **explanation 必须原文列出**实际日期区间（例："分析窗口：2026-04-20 ~ 2026-04-26（共 7 天）"）。

---

## 指标口径（输出固定 11 列，顺序不可打乱）

| # | 列名 | 含义 | 备注 |
|---|---|---|---|
| 1 | `dt` | 分区日期 | varchar 'yyyy-MM-dd' |
| 2 | `app_version` | 客户端版本号 | |
| 3 | `expose_cnt` | 当日该版本曝光次数 | `SUM(view_time>0)` |
| 4 | `click_cnt` | 当日该版本点击次数 | `SUM(click_time>0)` |
| 5 | `view_uv` | 当日该版本曝光独立用户数 | `COUNT(DISTINCT uin where view_time>0)` |
| 6 | `click_uv` | 当日该版本点击独立用户数 | `COUNT(DISTINCT uin where click_time>0)` |
| 7 | `ctr` | 次数口径 CTR = click_cnt / expose_cnt | DECIMAL(18,4) 0~1 比率 |
| 8 | `uv_ctr` | 人数口径 CTR = click_uv / view_uv | DECIMAL(18,4) 0~1 比率 |
| 9 | `expose_ratio` | 该版本曝光次数占当天总曝光的比例 | DECIMAL(18,4) 0~1 比率（窗口聚合自当日 TOP-N 之前的全量） |
| 10 | `view_uv_ratio` | 该版本曝光 UV 占当天总曝光 UV 的比例 | DECIMAL(18,4) 0~1 比率 |
| 11 | `rn` | 当天按 expose_cnt DESC 的排名 | 1~10 |

**排序**：`ORDER BY dt DESC, rn ASC`（与用户原始 SQL 一致：最新一天的 TOP1 在最上面，方便直接看现状；多日趋势靠下翻）。

⚠️ 比率字段一律用 `CAST(CAST(分子 AS DOUBLE) / NULLIF(分母, 0) AS DECIMAL(18, 4))`：先 `DOUBLE` 强制浮点除法，再 `DECIMAL(18,4)` 固定 4 位小数，避免 Trino DECIMAL 精度规则把 scale 截到 1 位。**不要改成百分号字符串**（`0.0823` 这种数值更利于前端图表渲染与 `expose_ratio` 在 0~1 范围内的可视化）。

---

## ★ Trino 引擎 SQL 骨架（生产已验证，**当 `{{engine}} = trino` 时直接用**）

把 `date_range` CTE 里的两处日期换成实际窗口即可；**其它一行都不要动**。

```sql
WITH date_range AS (
    -- 【替换点】用户指定区间 / 默认最近 7 天
    -- 用 DATE 类型存日期边界，方便做相对日期算术（如 end_dt - INTERVAL 'N' DAY）
    -- 与 varchar 分区列 dt 比较时用 date_format 转回 'yyyy-MM-dd' 字符串，保证分区裁剪
    SELECT CAST(CAST('{{yesterday}}' AS TIMESTAMP) AS DATE) - INTERVAL '6' DAY AS start_dt,
           CAST(CAST('{{yesterday}}' AS TIMESTAMP) AS DATE)                    AS end_dt
),
-- 事实表仅扫描一次：先按 dt + app_version 聚合
agg AS (
    SELECT dt,
           app_version,
           SUM(CASE WHEN view_time  > 0 THEN 1 ELSE 0 END)        AS expose_cnt,
           SUM(CASE WHEN click_time > 0 THEN 1 ELSE 0 END)        AS click_cnt,
           COUNT(DISTINCT CASE WHEN view_time  > 0 THEN uin END)  AS view_uv,
           COUNT(DISTINCT CASE WHEN click_time > 0 THEN uin END)  AS click_uv
    FROM dws_cn.dws_client_70000_front_page_feed_i_d
    CROSS JOIN date_range
    WHERE dt BETWEEN date_format(start_dt, '%Y-%m-%d')
                 AND date_format(end_dt,   '%Y-%m-%d')
    GROUP BY dt, app_version
),
-- 在聚合后的小结果上，用窗口函数派生当天总量 + 排名（不再二次扫事实表）
-- 一个 uin 当天只属于一个版本，所以 SUM(view_uv) OVER (PARTITION BY dt) 即全天去重曝光人数
ranked AS (
    SELECT dt,
           app_version,
           expose_cnt,
           click_cnt,
           view_uv,
           click_uv,
           SUM(expose_cnt) OVER (PARTITION BY dt) AS total_expose_cnt,
           SUM(view_uv)    OVER (PARTITION BY dt) AS total_view_uv,
           ROW_NUMBER()    OVER (PARTITION BY dt ORDER BY expose_cnt DESC) AS rn
    FROM agg
)
SELECT dt,
       app_version,
       expose_cnt,
       click_cnt,
       view_uv,
       click_uv,
       -- CTR（次数口径，原始比率，0~1，固定 4 位小数）
       CAST(CAST(click_cnt AS DOUBLE) / NULLIF(expose_cnt, 0) AS DECIMAL(18, 4)) AS ctr,
       -- UV CTR（人数口径，原始比率，0~1，固定 4 位小数）
       CAST(CAST(click_uv  AS DOUBLE) / NULLIF(view_uv,    0) AS DECIMAL(18, 4)) AS uv_ctr,
       -- 每个版本曝光占比（次数口径，原始比率，0~1）
       CAST(CAST(expose_cnt AS DOUBLE) / NULLIF(total_expose_cnt, 0) AS DECIMAL(18, 4)) AS expose_ratio,
       -- view_uv 人数占比（曝光总人数口径，原始比率，0~1）
       CAST(CAST(view_uv    AS DOUBLE) / NULLIF(total_view_uv,    0) AS DECIMAL(18, 4)) AS view_uv_ratio,
       rn
FROM ranked
WHERE rn <= 10
ORDER BY dt DESC, rn
```

---

## ★ Spark 引擎 SQL 骨架（**当 `{{engine}} = spark` 时用这个**）

与 Trino 逻辑 1:1 等价，**仅替换方言差异**：

| Trino | Spark |
|---|---|
| 表名 `dws_cn.xxx`（不加 catalog 前缀） | 同上 |
| `CAST(CAST('...' AS TIMESTAMP) AS DATE)` | `to_date('...')` |
| `... - INTERVAL '6' DAY` | `date_sub(end_dt, 6)` |
| `date_format(x, '%Y-%m-%d')` | `date_format(x, 'yyyy-MM-dd')` |
| `DOUBLE` | `DOUBLE`（一致） |
| `DECIMAL(18,4)` | `DECIMAL(18,4)`（一致） |
| `NULLIF` / `ROW_NUMBER()` | 一致 |

```sql
WITH date_range AS (
    -- 【替换点】用户指定区间 / 默认最近 7 天
    SELECT date_sub(to_date('{{yesterday}}'), 6) AS start_dt,
           to_date('{{yesterday}}')               AS end_dt
),
agg AS (
    SELECT t.dt,
           t.app_version,
           SUM(CASE WHEN t.view_time  > 0 THEN 1 ELSE 0 END)        AS expose_cnt,
           SUM(CASE WHEN t.click_time > 0 THEN 1 ELSE 0 END)        AS click_cnt,
           COUNT(DISTINCT CASE WHEN t.view_time  > 0 THEN t.uin END)  AS view_uv,
           COUNT(DISTINCT CASE WHEN t.click_time > 0 THEN t.uin END)  AS click_uv
    FROM dws_cn.dws_client_70000_front_page_feed_i_d AS t
    CROSS JOIN date_range AS p
    WHERE t.dt BETWEEN date_format(p.start_dt, 'yyyy-MM-dd')
                   AND date_format(p.end_dt,   'yyyy-MM-dd')
    GROUP BY t.dt, t.app_version
),
ranked AS (
    SELECT dt,
           app_version,
           expose_cnt,
           click_cnt,
           view_uv,
           click_uv,
           SUM(expose_cnt) OVER (PARTITION BY dt) AS total_expose_cnt,
           SUM(view_uv)    OVER (PARTITION BY dt) AS total_view_uv,
           ROW_NUMBER()    OVER (PARTITION BY dt ORDER BY expose_cnt DESC) AS rn
    FROM agg
)
SELECT dt,
       app_version,
       expose_cnt,
       click_cnt,
       view_uv,
       click_uv,
       CAST(CAST(click_cnt AS DOUBLE) / NULLIF(expose_cnt, 0) AS DECIMAL(18, 4)) AS ctr,
       CAST(CAST(click_uv  AS DOUBLE) / NULLIF(view_uv,    0) AS DECIMAL(18, 4)) AS uv_ctr,
       CAST(CAST(expose_cnt AS DOUBLE) / NULLIF(total_expose_cnt, 0) AS DECIMAL(18, 4)) AS expose_ratio,
       CAST(CAST(view_uv    AS DOUBLE) / NULLIF(total_view_uv,    0) AS DECIMAL(18, 4)) AS view_uv_ratio,
       rn
FROM ranked
WHERE rn <= 10
ORDER BY dt DESC, rn
```

---

## LLM 执行步骤（严格按此）

1. **选引擎骨架**（**最重要**）：
   - `{{engine}} = trino` → 用上面的 Trino 骨架（`%Y-%m-%d`、`INTERVAL '6' DAY`、`CAST(... AS DATE)`；表名 `dws_cn.xxx` **不加** `hive.`）；
   - `{{engine}} = spark` → 用上面的 Spark 骨架（`yyyy-MM-dd`、`date_sub` 替代 `INTERVAL`、`to_date` 替代 `CAST AS DATE`；表名同上）；
   - **绝对不要把两套方言混用**，否则 pre-check 会拒（例如 `Spark date_format pattern should use Java style like 'yyyy-MM-dd', not '%Y-%m-%d'`）。
2. **解析时间窗口**：
   - 用户明确给区间或单日 → 直接替换 `date_range` 里的两行；
   - 用户说「最近 N 天 / 过去 N 周」 → `end_dt = {{yesterday}}`，Trino 用 `INTERVAL 'N-1' DAY`、Spark 用 `date_sub(end_dt, N-1)`；
   - 什么都没给 → 默认 **最近 7 天**（已在两套骨架里写好，直接用即可）。
3. **不要改表名**：表本体一律是 `dws_cn.dws_client_70000_front_page_feed_i_d`，**Trino / Spark 都不要加 catalog 前缀**；不要换成 DWD 表 `dwd_client_70000_scene_18_dtl_i_d`。
4. **不要改 CTE 结构**：`date_range → agg → ranked → 最终 SELECT` 三段必须保留；不要把窗口函数改写成自连接；不要把 `ROW_NUMBER` 换成 `RANK`（用户原始口径就是 ROW_NUMBER）。
5. **不要把比率改为百分号字符串**：`ctr / uv_ctr / expose_ratio / view_uv_ratio` 都是 `DECIMAL(18,4)` 的 0~1 数值，前端可视化层会自动按列名识别为百分比并加 `%` 显示。
6. **保留 `rn <= 10`**：用户口径就是每天 TOP10。如果用户明确说"TOP20"或"全部版本"，把这里改成对应数字或去掉这一行（同时 explanation 里要说明改动）。
7. **输出 JSON**（禁止"上述 SQL / 如上"等占位短语，必须把 SQL 完整写出，且 `tables_used` 要与所选引擎下的实际表名一致）：
   ```
   {
     "sql": "<完整 SQL>",
     "explanation": "引擎 + 时间窗口 + 口径说明",
     "tables_used": ["dws_cn.dws_client_70000_front_page_feed_i_d"],
     "execution_plan": ""
   }
   ```

---

## 推荐可视化（前端图表面板会读这些建议）

- **首选：组合图（柱+线 双Y轴）** — X 轴 `dt`，柱状轴选 `expose_cnt`（量级），折线轴选 `ctr` 或 `uv_ctr`（比率）。一眼看大盘曝光量与 CTR 的关系。
- **次选：分组折线图** — X 轴 `dt`，Y 轴 `ctr`，分组列 `app_version`。每个版本一条线，配合"末位/数据未满分组虚线突出"开关，可清晰看到**新版本 CTR 是否健康**。
- **第三：分组折线图** — X 轴 `dt`，Y 轴 `expose_ratio`，分组列 `app_version`。直接看**版本曝光迁移**（老版本占比下降、新版本占比爬升）。

---

## 分析报告阶段要求（analyze-data）

「一、数据概览」（必含）：
1. 明确分析窗口（起止日期 + 共 N 天）；
2. 窗口内**累计去重 app_version 数量**与**最新一天 TOP10 版本列表**（含 expose_ratio 数值）；
3. 全量 `expose_cnt` / `view_uv` 大盘量级（万 / 亿口径自动）。

「二、关键发现」（至少覆盖以下 4 条）：
1. **版本迁移**：相比窗口起始日，最新一天**曝光占比涨幅 TOP3 / 跌幅 TOP3** 的 `app_version`（给具体的 `expose_ratio` 起止值与百分点差）。
2. **CTR 高低分布**：在曝光占比 ≥ 1% 的版本里，`ctr` 最高 / 最低 各 Top3（百分比形式 `0.05 → 5.00%`）；**忽略曝光过小（`expose_cnt < 1000`）的长尾版本**避免误导。
3. **新版本健康度**：识别 `expose_ratio` 持续上升（≥3 天连涨）的版本；若其 `ctr` 显著低于大盘均值（≥10% 的相对差），点名**重点关注**，给出业务假设（新版本灰度未完、埋点改版、推荐策略变更等）。
4. **量价关系**：在最新一天，对比每个版本的 `view_uv_ratio` 与 `ctr`——**人数占比高但 CTR 偏低**的版本属于"老粘性老玩家但匹配度下降"，**人数占比低但 CTR 偏高**的版本可能是"小众新版本，体验更好但量未铺开"，分别列举 1~2 个。

「三、结论与建议」：给 1~3 条可执行建议，例如"推荐策略针对低 CTR 老版本做兼容回滚 / 新版本灰度比例下调一档观察 / 推送下线 expose_ratio < 1% 的尾部版本"。

「四、下一步可追问」：
- 按 `app_version × dt × 渠道` 三维细分，定位是哪个渠道的版本下滑；
- 基于 `dwd_cn.dwd_client_70000_scene_18_dtl_i_d` 切到 `card_id` / `comp_id` 维度，看版本间的**功能模块** CTR 差异；
- 对涨幅 TOP3 新版本做留存/次留拉一拉，看用户是否真的留下来了。
