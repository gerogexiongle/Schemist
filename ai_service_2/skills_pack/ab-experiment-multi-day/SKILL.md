---
id: ab-experiment-multi-day
name: 实验对照多日归因分析
description: AB 实验多日全链路对比：推荐→曝光→点击→时长→迷你币收入；支持任意实验ID与标签
icon: 🧪
placeholder: 例如：分析 2026-04-20 到 2026-04-27，对照组(10127) vs 实验组(10128)
enabled: true
order: 47
tags:
  - AB实验
  - 多日趋势
  - 归因
  - 推荐
  - 曝光
  - 点击
  - 时长
  - 迷你币收入
  - exp_list
---

【任务】产出「**实验对照多日全链路归因分析**」——基于 DWS 推荐表 + DWD 时长表 + 迷你币收入表，按 `dt × 实验组` 输出 **UV / 推荐 / 曝光 / 点击 / CTR / 时长 / 时长人均 / 迷你币 / 迷你币人均** 全链路 **16 个指标**。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}（**Trino 与 Spark 双引擎都支持**；按下方骨架严格选择）
【今天日期】{{today}}
【默认分析窗口】最近 7 天（`end_dt = {{yesterday}}`、`start_dt = end_dt - 6 天`）

---

## 🚨 黄金法则（**违反任意一条直接判错，先于其它所有规则**）

1. **CTE 链路必须完整且按此顺序**：`reco_for_join → reco_agg → clicked → duration_agg → coin_agg → detail → 最终 SELECT`。**少任意一段都不允许**。（`reco_for_join` 是性能优化用中间层，**必须保留**；不再使用 `date_range` CTE + `CROSS JOIN`。）
2. **🚫 严禁用字面量 `0` / `NULL` 替代任何聚合列**。哪怕你"不认识 `mnv_ads_ugc_cn.map_consumption_details_day` 这张表"——这张表是**生产真实表**，已上线 1+ 年，必须**原样使用**。写 `0 AS total_coin` / `0 AS coin_tx_cnt` / `0 AS avg_coin_per_user` / `0 AS avg_tx_per_user` 是**严重幻觉**，会被 pre-check 拒绝。
3. **三张表全部出现在 `tables_used`**：`dws_cn.dws_behavior_map_game_reco_i_d`、`dwd_cn.dwd_user_game_retention_dtl_i_d`、`mnv_ads_ugc_cn.map_consumption_details_day`——**少一张就是错**。
4. **16 个输出列一个不能少**：`dt, exp_tag, uv, total_reco, total_view, total_click, ctr, total_game_duration, avg_game_duration, avg_click_per_user, avg_duration_per_view, total_coin, coin_tx_cnt, avg_coin_per_user, avg_tx_per_user`——后 4 个是迷你币相关，**必须从 `coin_agg` JOIN 计算得出，不能写死成 0**。
5. **不允许"我不熟悉这张表所以省略"的偷懒** —— 业务方已确认 `mnv_ads_ugc_cn.map_consumption_details_day` 字段：`dt, uin, map_id, pay_cnt, event_code, valid` 全部存在，按下方骨架照抄即可。
6. **大日期窗口预警**：若用户窗口 **> 7 天**，必须在 `explanation` 首句写明「窗口共 N 天，明细链路数据量大，查询可能较慢；建议后续沉淀 ADS 预聚合表」——**但不允许因此删减 CTE 或写 0 占位**。

> 💡 LLM 心理预警：如果你正在考虑"这张迷你币表我不太熟，要不写 0 算了"——**停下**。这是被业务专家审核过的固定骨架，照抄就对了。

---

## 固定业务信息（已生产验证；**不要改也不要再调 `search_tables` / `get_table_info`**）

| 角色 | 表名（**Trino / Spark 统一**，不要 `hive.` / `trino.` catalog 前缀） |
|---|---|
| 推荐 / 曝光 / 点击 | `dws_cn.dws_behavior_map_game_reco_i_d` |
| 游戏时长 | `dwd_cn.dwd_user_game_retention_dtl_i_d` |
| 迷你币收入 | `mnv_ads_ugc_cn.map_consumption_details_day` |

⚠️ **表名一律 `库.表` 两段式**；与本服务执行环境默认 catalog 一致，**不要**写 `hive.dws_cn.xxx`。

- **场景过滤**（推荐表）：`api_type = '40001'`（联机大厅推荐页；千万不要写成 `'4001'`）
- **收入过滤**：`event_code = 'coin_pay' AND valid = '1'`
- **归因锚点**：以"当天有点击的 (dt, uin, map_id)"作为锚点；时长表与收入表都用 `INNER JOIN clicked` 关联，避免把没点击产生的时长/收入也算进来
- **去重**：`reco_agg` 先按 `(dt, uin, map_id, exp_list)` 一次聚合，避免请求级重复
- **分区裁剪**：`WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'` 使用**字面量**日期字符串（`yyyy-MM-dd`），**禁止** `CROSS JOIN date_range` 再 `BETWEEN p.start_dt AND p.end_dt`（Spark 侧易丢分区裁剪，导致全表扫）。
- **JOIN 键类型**：
  - **Spark**：`uin` / `map_id` / `cid` 若元数据已为同类型，**禁止** `CAST(... AS STRING)` 包一层再 JOIN（会强制大表 shuffle + 丢统计信息）。
  - **Trino**：JOIN 比较两侧类型必须一致；若一侧 `varchar` 一侧 `bigint`，用 `CAST(... AS VARCHAR)` **仅在 Trino 骨架**中出现（见下方 Trino 模板）。
- **JOIN 策略（仅依赖 AQE，不写 hint）**：
  - 服务端已在 `sql_executor` 注入：`spark.sql.adaptive.enabled=true`、`spark.sql.autoBroadcastJoinThreshold=256m`、`adaptive.coalescePartitions.enabled=true`、`adaptive.skewJoin.enabled=true`。
  - **禁止**在 SQL 里写 `/*+ BROADCAST(c) */` / `/*+ MERGE(...) */` 等任何 join hint：实测中 `clicked` 在 **AB 实验 7 天以上窗口** 下行数可达千万级（`distinct (dt, uin, map_id)`），强制 broadcast 会触发 `Store broadcast fail` + `Multiple failures in stage materialization`，反而比 sort-merge join 还慢甚至直接失败。让 AQE 按 runtime 数据量自动选 BHJ / SMJ 即可。
  - **Trino**：同样**不要写任何 `/*+ ... */` 内联 hint**，Trino 425 不支持；如需特殊调优请用 session 变量。
  - ⚠️ **不要在 SQL 模板里写 `SET spark.sql.* = ...;` 等多语句**——服务侧会把多语句裁剪到第一段。性能相关 tuning（如 `autoBroadcastJoinThreshold`、`shuffle.partitions`、`adaptive.skewJoin`）已由 `sql_executor` 在 temp file 头自动注入；如需 per-query 临时覆盖，使用环境变量 `SPARK_SQL_SET_OVERRIDES`（见服务端 README）。

---

## ⚠️ 实验配置（LLM 必须从 `{{user_query}}` 中提取）

用户会描述若干实验组，每组包含【实验名称 + 实验id】。常见写法（示例）：
- "对照组(10127) vs 实验组(10128)"
- "对照组 10127、实验组 10128"
- "10120 对照组、10121 CTR1、10122 Duration1"
- "实验 10127 对比 10128"

LLM 需要解析出 EXPS = [(label, exp_id), ...]：
- 例 1：`[("对照组","10127"), ("实验组","10128")]`
- 例 2：`[("对照组","10120"), ("CTR1","10121"), ("Duration1","10122")]`

然后**三处替换必须全部一致**（filter 里的正则、detail SELECT 内 CASE WHEN、detail GROUP BY 内 CASE WHEN）：
1. **正则模式**（filter）：用 `|` 拼接所有 `exp_id` →  `'10127|10128'`
2. **CASE WHEN exp_tag**：每组一行
3. **GROUP BY 的 CASE WHEN**：必须与 SELECT 的 CASE WHEN **完全一致**

如果 user_query 完全没给实验ID，**不要瞎编**，在 explanation 里报错并请用户补充：
> "请提供实验组列表，格式如：对照组(10127), 实验组(10128)"

---

## 时间窗口口径

1. 用户明确给区间（`2026-04-20 ~ 2026-04-27`）→ 用它
2. 用户给单日 → `start_dt = end_dt = 该日`
3. 用户说「最近 N 天」 → `end_dt = {{yesterday}}`、`start_dt = end_dt - (N-1) 天`
4. 没给 → 默认最近 7 天（骨架默认值）

⚠️ explanation 必须**原文列出**实际日期区间（例："分析窗口：2026-04-21 ~ 2026-04-27（共 7 天）"）。

⚠️ **替换方式**：把下方骨架里所有 `'<start_dt>'` / `'<end_dt>'` 字面量（**每个引擎骨架各 3 处**，出现在 `reco_for_join` / `duration_agg` / `coin_agg` 的 `WHERE t.dt BETWEEN ...`）**全部替换为同一对用户日期**；单日则两者相同。**不要**再引入 `date_range` CTE。

---

## ✅ 最小正确规则（Spark 3.3.1 ↔ Trino 425）

> 以下规则优先级高于其它描述；若与历史经验冲突，以此处为准。

1. **日期格式**
   - Spark: `date_format(d, 'yyyy-MM-dd')`
   - Trino: `date_format(d, '%Y-%m-%d')`
   - 禁止混用（否则 pre-check 直接失败）

2. **日期运算**
   - Spark: `date_sub(to_date(x), n)`
   - Trino: `DATE 'yyyy-MM-dd' - INTERVAL 'n' DAY` 或 `date_add('day', -n, DATE 'yyyy-MM-dd')`

3. **正则匹配**
   - Spark: `col RLIKE 'pat'`
   - Trino: `regexp_like(col, 'pat')`

4. **类型比较（关键）**
   - Trino 不容忍 `varchar = bigint` 隐式比较
   - `api_type` / `valid` 与字面量比较时：
     - Spark：`CAST(t.api_type AS STRING) = '40001'`、`CAST(t.valid AS STRING) = '1'`
     - Trino：`CAST(t.api_type AS VARCHAR) = '40001'`、`CAST(t.valid AS VARCHAR) = '1'`
   - `uin` / `map_id` / `cid` 的 JOIN：**Spark 骨架用原生类型直连**；**Trino 骨架在 JOIN ON 上统一 `CAST(... AS VARCHAR)`**（见模板，勿混用）。

5. **除法与比例**
   - Spark 可直接 `ROUND(a / b, n)`
   - Trino 必须 `ROUND(CAST(a AS DOUBLE) / NULLIF(b, 0), n)`，避免整数截断

6. **库表名**
   - 两个引擎统一使用 `db.table`（如 `dws_cn.xxx`）
   - 不要写 `hive.db.table` / `trino.db.table`

7. **子查询/CTE 别名**
   - Trino 要求更严格，所有子查询必须有别名
   - 本 skill 固定别名：`r / dur / cn`，不要改

8. **禁止占位输出**
   - 禁止在 `sql` 字段写「如上 / 略 / 完整SQL」等占位话术
   - 必须输出可直接执行的完整 SQL（从 `WITH` 到 `ORDER BY`）

---

## 输出列（**固定 16 列，顺序不可打乱，列名一字不改**）

`dt, exp_tag, uv, total_reco, total_view, total_click, ctr, total_game_duration, avg_game_duration, avg_click_per_user, avg_duration_per_view, total_coin, coin_tx_cnt, avg_coin_per_user, avg_tx_per_user`

排序：`ORDER BY dt, exp_tag`
末尾过滤：`WHERE exp_tag IS NOT NULL`（过滤未匹配上任何实验组的脏行）

---

## ★ Spark 引擎 SQL 骨架（**当 `{{engine}} = spark` 时严格用此**）

把 **每个引擎骨架各 3 处** `'<start_dt>'` / `'<end_dt>'` 换成实际窗口；把 `RLIKE` / `regexp_like` 模式 + 三处 `CASE WHEN` 分支替换为 EXPS 实际值。**CTE 名 / JOIN 结构 / 列名一行都不要动**（仅允许改日期字面量、正则、CASE 标签）。

```sql
WITH
-- ① 轻量层：只扫推荐表 + 过滤实验，供 reco_agg / clicked 复用（避免 duration / coin 重复扫全表）
reco_for_join AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           t.exp_list,
           t.is_reco,
           t.is_view,
           t.is_click
    FROM dws_cn.dws_behavior_map_game_reco_i_d AS t
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
      AND CAST(t.api_type AS STRING) = '40001'
      -- 【替换点 B1】RLIKE 模式 = 所有 exp_id 用 | 拼接
      AND t.exp_list RLIKE '10127|10128'
),
reco_agg AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           t.exp_list,
           SUM(t.is_reco)  AS total_reco,
           SUM(t.is_view)  AS total_view,
           SUM(t.is_click) AS total_click
    FROM reco_for_join AS t
    GROUP BY t.dt, t.uin, t.map_id, t.exp_list
),
-- ② 归因锚点：当天该 map 发生过点击（用源表 is_click 直接过滤，比 SUM 后再筛更快）
clicked AS (
    SELECT DISTINCT t.dt, t.uin, t.map_id
    FROM reco_for_join AS t
    WHERE t.is_click > 0
),
duration_agg AS (
    SELECT t.dt,
           t.uin,
           t.cid AS map_id,
           SUM(t.game_duration) AS total_game_duration
    FROM dwd_cn.dwd_user_game_retention_dtl_i_d AS t
    INNER JOIN clicked AS c
        ON t.dt = c.dt AND t.uin = c.uin AND t.cid = c.map_id
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
    GROUP BY t.dt, t.uin, t.cid
),
coin_agg AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           SUM(t.pay_cnt) AS total_coin,
           COUNT(*)       AS coin_tx_cnt
    FROM mnv_ads_ugc_cn.map_consumption_details_day AS t
    INNER JOIN clicked AS c
        ON t.dt = c.dt AND t.uin = c.uin AND t.map_id = c.map_id
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
      AND t.event_code = 'coin_pay'
      AND CAST(t.valid AS STRING) = '1'
    GROUP BY t.dt, t.uin, t.map_id
),
detail AS (
    SELECT
        r.dt,
        -- 【替换点 B2】CASE WHEN exp_tag —— 每个实验组一行（SELECT 处）
        CASE
            WHEN r.exp_list RLIKE '10127' THEN '对照组'
            WHEN r.exp_list RLIKE '10128' THEN '实验组'
        END AS exp_tag,
        COUNT(DISTINCT r.uin) AS uv,
        SUM(r.total_reco)  AS total_reco,
        SUM(r.total_view)  AS total_view,
        SUM(r.total_click) AS total_click,
        COALESCE(SUM(dur.total_game_duration), 0) AS total_game_duration,
        COALESCE(SUM(cn.total_coin), 0)  AS total_coin,
        COALESCE(SUM(cn.coin_tx_cnt), 0) AS coin_tx_cnt
    FROM reco_agg AS r
    LEFT JOIN duration_agg AS dur
        ON r.dt = dur.dt AND r.uin = dur.uin AND r.map_id = dur.map_id
    LEFT JOIN coin_agg AS cn
        ON r.dt = cn.dt AND r.uin = cn.uin AND r.map_id = cn.map_id
    GROUP BY
        r.dt,
        -- 【替换点 B3】GROUP BY 的 CASE WHEN —— 必须与 SELECT 处完全一致！
        CASE
            WHEN r.exp_list RLIKE '10127' THEN '对照组'
            WHEN r.exp_list RLIKE '10128' THEN '实验组'
        END
)
SELECT
    dt,
    exp_tag,
    uv,
    total_reco,
    total_view,
    total_click,
    CASE WHEN total_view > 0 THEN ROUND(total_click / total_view, 4) ELSE 0 END AS ctr,
    total_game_duration,
    CASE WHEN uv > 0 THEN ROUND(total_game_duration / uv, 2) ELSE 0 END AS avg_game_duration,
    CASE WHEN uv > 0 THEN ROUND(total_click / uv, 4) ELSE 0 END AS avg_click_per_user,
    CASE WHEN total_view > 0 THEN ROUND(total_game_duration / total_view, 2) ELSE 0 END AS avg_duration_per_view,
    total_coin,
    coin_tx_cnt,
    CASE WHEN uv > 0 THEN ROUND(total_coin / uv, 2) ELSE 0 END AS avg_coin_per_user,
    CASE WHEN uv > 0 THEN ROUND(coin_tx_cnt / uv, 4) ELSE 0 END AS avg_tx_per_user
FROM detail
WHERE exp_tag IS NOT NULL
ORDER BY dt, exp_tag
```

---

## ★ Trino 引擎 SQL 骨架（**当 `{{engine}} = trino` 时严格用此**）

与 Spark **结构 1:1 一致**；仅以下方言差异：

1. `regexp_like(col, pat)` ↔ `col RLIKE pat`
2. `CAST(... AS VARCHAR)` ↔ `CAST(... AS STRING)`
3. 最终 `ROUND` 必须 `CAST AS DOUBLE` + `NULLIF`
4. **不写任何 `/*+ ... */` hint**（Spark 已撤掉 BROADCAST hint，依赖 AQE 自动选择；Trino 不支持内联 hint，如需特殊调优请在 session 层设 `join_distribution_type='BROADCAST'`）

```sql
WITH
reco_for_join AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           t.exp_list,
           t.is_reco,
           t.is_view,
           t.is_click
    FROM dws_cn.dws_behavior_map_game_reco_i_d AS t
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
      AND CAST(t.api_type AS VARCHAR) = '40001'
      AND regexp_like(t.exp_list, '10127|10128')
),
reco_agg AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           t.exp_list,
           SUM(t.is_reco)  AS total_reco,
           SUM(t.is_view)  AS total_view,
           SUM(t.is_click) AS total_click
    FROM reco_for_join AS t
    GROUP BY t.dt, t.uin, t.map_id, t.exp_list
),
clicked AS (
    SELECT DISTINCT t.dt, t.uin, t.map_id
    FROM reco_for_join AS t
    WHERE t.is_click > 0
),
duration_agg AS (
    SELECT t.dt,
           t.uin,
           t.cid AS map_id,
           SUM(t.game_duration) AS total_game_duration
    FROM dwd_cn.dwd_user_game_retention_dtl_i_d AS t
    INNER JOIN clicked AS c
        ON t.dt = c.dt
       AND CAST(t.uin AS VARCHAR) = CAST(c.uin AS VARCHAR)
       AND CAST(t.cid AS VARCHAR) = CAST(c.map_id AS VARCHAR)
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
    GROUP BY t.dt, t.uin, t.cid
),
coin_agg AS (
    SELECT t.dt,
           t.uin,
           t.map_id,
           SUM(t.pay_cnt) AS total_coin,
           COUNT(*)       AS coin_tx_cnt
    FROM mnv_ads_ugc_cn.map_consumption_details_day AS t
    INNER JOIN clicked AS c
        ON t.dt = c.dt
       AND CAST(t.uin AS VARCHAR) = CAST(c.uin AS VARCHAR)
       AND CAST(t.map_id AS VARCHAR) = CAST(c.map_id AS VARCHAR)
    WHERE t.dt BETWEEN '<start_dt>' AND '<end_dt>'
      AND t.event_code = 'coin_pay'
      AND CAST(t.valid AS VARCHAR) = '1'
    GROUP BY t.dt, t.uin, t.map_id
),
detail AS (
    SELECT
        r.dt,
        CASE
            WHEN regexp_like(r.exp_list, '10127') THEN '对照组'
            WHEN regexp_like(r.exp_list, '10128') THEN '实验组'
        END AS exp_tag,
        COUNT(DISTINCT r.uin) AS uv,
        SUM(r.total_reco)  AS total_reco,
        SUM(r.total_view)  AS total_view,
        SUM(r.total_click) AS total_click,
        COALESCE(SUM(dur.total_game_duration), 0) AS total_game_duration,
        COALESCE(SUM(cn.total_coin), 0)  AS total_coin,
        COALESCE(SUM(cn.coin_tx_cnt), 0) AS coin_tx_cnt
    FROM reco_agg AS r
    LEFT JOIN duration_agg AS dur
        ON r.dt = dur.dt
       AND CAST(r.uin AS VARCHAR) = CAST(dur.uin AS VARCHAR)
       AND CAST(r.map_id AS VARCHAR) = CAST(dur.map_id AS VARCHAR)
    LEFT JOIN coin_agg AS cn
        ON r.dt = cn.dt
       AND CAST(r.uin AS VARCHAR) = CAST(cn.uin AS VARCHAR)
       AND CAST(r.map_id AS VARCHAR) = CAST(cn.map_id AS VARCHAR)
    GROUP BY
        r.dt,
        CASE
            WHEN regexp_like(r.exp_list, '10127') THEN '对照组'
            WHEN regexp_like(r.exp_list, '10128') THEN '实验组'
        END
)
SELECT
    dt,
    exp_tag,
    uv,
    total_reco,
    total_view,
    total_click,
    CASE WHEN total_view > 0
         THEN ROUND(CAST(total_click AS DOUBLE) / NULLIF(total_view, 0), 4)
         ELSE 0 END AS ctr,
    total_game_duration,
    CASE WHEN uv > 0
         THEN ROUND(CAST(total_game_duration AS DOUBLE) / NULLIF(uv, 0), 2)
         ELSE 0 END AS avg_game_duration,
    CASE WHEN uv > 0
         THEN ROUND(CAST(total_click AS DOUBLE) / NULLIF(uv, 0), 4)
         ELSE 0 END AS avg_click_per_user,
    CASE WHEN total_view > 0
         THEN ROUND(CAST(total_game_duration AS DOUBLE) / NULLIF(total_view, 0), 2)
         ELSE 0 END AS avg_duration_per_view,
    total_coin,
    coin_tx_cnt,
    CASE WHEN uv > 0
         THEN ROUND(CAST(total_coin AS DOUBLE) / NULLIF(uv, 0), 2)
         ELSE 0 END AS avg_coin_per_user,
    CASE WHEN uv > 0
         THEN ROUND(CAST(coin_tx_cnt AS DOUBLE) / NULLIF(uv, 0), 4)
         ELSE 0 END AS avg_tx_per_user
FROM detail
WHERE exp_tag IS NOT NULL
ORDER BY dt, exp_tag
```

---

## ⚠️ 常见踩坑（**LLM 必读，违反任何一条都会被 pre-check 直接拒**）

| # | 错误写法 | 正确写法 | 原因 |
|---|---|---|---|
| 1 | `SUM(dur.game_duration)` | `SUM(dur.total_game_duration)` | `duration_agg` 已经把原始列 `game_duration` 聚合成 `total_game_duration`；**外层 detail 必须引用聚合后列名**，不要把内层 CTE 里的 `t.game_duration` 当成 `dur.game_duration` 抄出来 |
| 2 | `SUM(cn.pay_cnt)` | `SUM(cn.total_coin)` | 同上：`coin_agg` 已聚合 `pay_cnt → total_coin`，外层引用 `cn.total_coin` |
| 3 | `LEFT JOIN duration_agg d` + `d.game_duration` | `LEFT JOIN duration_agg dur` + `dur.total_game_duration` | 别名 `d` 在 dwd 原表里也用过，容易触发 LLM 别名混淆。**外层固定用 `dur` / `cn`** |
| 4 | Spark 写 `regexp_like(...)` 或 Trino 写 `RLIKE` | 严格按引擎选 | 方言混用 pre-check 必报错 |
| 5 | Trino 写 `ROUND(total_click / total_view, 4)` | `ROUND(CAST(total_click AS DOUBLE) / NULLIF(total_view, 0), 4)` | Trino 中 BIGINT/BIGINT = 整除（结果都是 0） |
| 6 | SELECT 与 GROUP BY 的 CASE WHEN 不一致 | 字符级一致 | Spark / Trino 都报 "GROUP BY expression is not present in select" |
| 7 | 漏写 `WHERE exp_tag IS NOT NULL` | 末尾必须有 | 没匹配上的脏行会破坏 ORDER BY |
| 8 | **省略 `coin_agg` CTE，把 `total_coin` / `coin_tx_cnt` 写成 `0`** | **必须保留 `coin_agg` 和 `cn` join，引用 `cn.total_coin` / `cn.coin_tx_cnt`** | 这是**最严重的偷懒**。`mnv_ads_ugc_cn.map_consumption_details_day` 是真实生产表，**不要因为"看起来陌生"就跳过它**——业务方已经验证过 |
| 9 | `0 AS total_coin, 0 AS coin_tx_cnt, 0 AS avg_coin_per_user, 0 AS avg_tx_per_user` | `total_coin, coin_tx_cnt, ROUND(...total_coin/uv...) AS avg_coin_per_user, ROUND(...coin_tx_cnt/uv...) AS avg_tx_per_user`（全部从 detail CTE 引用） | 字面量 0 = 直接放弃归因；这条规则**先于其它任何方言规则**，任何场景都不允许 |
| 10 | Trino：`t.api_type = '40001'` / `t.valid = '1'` / `t.cid = c.map_id`（隐式类型） | Trino：`CAST(t.api_type AS VARCHAR) = '40001'`、`CAST(t.valid AS VARCHAR) = '1'`、**JOIN ON 上** `CAST(... AS VARCHAR) = CAST(... AS VARCHAR)`；Spark：`CAST(t.api_type AS STRING) = '40001'`、`CAST(t.valid AS STRING) = '1'`，**JOIN ON 上 `uin/map_id/cid` 用原生类型直连**（**禁止**再包 `CAST(... AS STRING)`） | Trino **无** Hive 式隐式转换；Spark 侧对 JOIN 键强行 STRING cast 会丢统计 + 放大 shuffle，易触发超时 |

**列名速查表（外层 detail 中允许引用的列）：**
- `r.*`：`r.dt, r.uin, r.map_id, r.exp_list, r.total_reco, r.total_view, r.total_click`
- `dur.*`：`dur.dt, dur.uin, dur.map_id, dur.total_game_duration` ← **没有 `dur.game_duration`**
- `cn.*`：`cn.dt, cn.uin, cn.map_id, cn.total_coin, cn.coin_tx_cnt` ← **没有 `cn.pay_cnt`**

---

## LLM 执行步骤（**严格按此，禁止幻觉**）

1. **解析实验配置**：从 `{{user_query}}` 提取所有 (label, exp_id)，至少 2 组。如缺失，**不要瞎编**，在 explanation 里报错请求补充。
2. **解析时间窗口**：用户明确给 → 用它；说「最近 N 天」→ `end_dt = {{yesterday}}`、`start_dt = end_dt - (N-1) 天`；没给 → `end_dt = {{yesterday}}`、`start_dt = end_dt - 6 天`（默认 7 天）。把算出的 `start_dt/end_dt` **字符串化**为 `yyyy-MM-dd`，替换骨架里 **全部 3 处** `'<start_dt>'` / `'<end_dt>'`。
3. **选引擎骨架**（**最重要**）：
   - `{{engine}} = spark` → 用上面的 **Spark 骨架**（`RLIKE`、`CAST(... AS STRING)` 仅用于 `api_type/valid` 字面量比较、**`ROUND` 可直接 `BIGINT/BIGINT`**）；
   - `{{engine}} = trino` → 用上面的 **Trino 骨架**（`regexp_like`、`CAST(... AS VARCHAR)`、`ROUND` **必须** `CAST AS DOUBLE` + `NULLIF`）；表名与 Spark **相同**，**不要**加 `hive.`；
   - **绝对不要混用方言**——`RLIKE` 是 Spark 专属，`regexp_like` 是 Trino 专属。混用会被 pre-check 直接拒绝。
4. **三处 EXPS 替换必须一致**：
   - ① `reco_for_join` filter 的 `RLIKE` / `regexp_like` 模式（用 `|` 拼接所有 exp_id）；
   - ② detail SELECT 内的 `CASE WHEN ... THEN '<label>'`（每个组一行）；
   - ③ detail GROUP BY 内的 `CASE WHEN ...`（必须**字符级**与 SELECT 一致，否则 Spark / Trino 都会报 "GROUP BY expression is not present in select"）。
5. **保留 `WHERE exp_tag IS NOT NULL`**：过滤未匹配上任何实验的脏行。
6. **保留所有 `COALESCE(... , 0)`**：未点击用户的时长/收入应为 0 而非 NULL。
7. **不要改表名 / CTE 结构 / 列名 / 排序**：表本体三个、`WITH` 内 **6 个命名 CTE**（`reco_for_join → reco_agg → clicked → duration_agg → coin_agg → detail`）+ **外层最终 `SELECT`**、16 个输出列、`ORDER BY dt, exp_tag` 全部保留。
8. **输出 JSON**——`sql` 字段必须包含**从 `WITH` 到最后一行 `ORDER BY`** 的全部文本（**至少 ≥ 1500 字符**），**绝对禁止任何形式的占位代指**：
   - ❌ 禁止写 `"sql": "上述 SQL"` / `"如上"` / `"(略)"`
   - ❌ 禁止在 `sql` 字段里重新发明 `date_range` CTE / `CROSS JOIN date_range`
   - ❌ 禁止在 `sql` 字段里输出「未完成 SQL」（只写了前半段、没有写到 `ORDER BY dt, exp_tag` 最后一行）
   - ❌ 禁止写 `"sql": "见上面 sql 代码块"`
   - ✅ 正确做法：把上面 Spark 或 Trino 骨架的 SQL **逐字符复制**到 `sql` 字段（注意 JSON 字符串需要把换行 escape 成 `\n`，引号 escape 成 `\"`）

   输出格式（示意，**实际 sql 字段必须是完整 SQL 字符串**）：

   ```json
   {
     "sql": "<PASTE_FULL_SQL_HERE>",
     "explanation": "引擎 + 实验组列表（label/exp_id）+ 日期窗口 + 关键口径说明",
     "tables_used": ["dws_cn.dws_behavior_map_game_reco_i_d", "dwd_cn.dwd_user_game_retention_dtl_i_d", "mnv_ads_ugc_cn.map_consumption_details_day"],
     "execution_plan": ""
   }
   ```

   ⚠️ `tables_used` 与 SQL 正文中的表名一致：`dws_cn.xxx` / `dwd_cn.xxx` / `mnv_ads_ugc_cn.xxx`，**均不要** `hive.` 前缀。
   ⚠️ 上面 `"sql": "<PASTE_FULL_SQL_HERE>"` 只是**格式示意**；你真实输出时必须把 `<PASTE_FULL_SQL_HERE>` 整段替换成**完整可执行 SQL**（从 `WITH` 到 `ORDER BY dt, exp_tag`），并在 JSON 字符串里正确转义换行与引号；**禁止**在 `sql` 字段里写「省略 / 略 / 见上」或任何未闭合的半截语句。

---

## ✅ 输出前自检清单（**LLM 在 return 之前必须逐条核对**）

把生成的 SQL 文本对照下面清单，**任意一条不通过就重写**：

- [ ] **CTE 数量 = 6**：`reco_for_join`、`reco_agg`、`clicked`、`duration_agg`、`coin_agg`、`detail`（**清点 `WITH ... AS (` 出现次数应该 = 6**）
- [ ] **`reco_for_join` 不能缺**：且 `duration_agg` / `coin_agg` 必须 `INNER JOIN clicked AS c` 关联到锚点
- [ ] **任何引擎都不能出现 `/*+` hint**：SQL 正文里**禁止出现** `/*+ BROADCAST(...)`、`/*+ MERGE(...)` 等 hint 字符串。让 AQE 按 runtime 自动选择 join 策略
- [ ] **禁止 `date_range` / `CROSS JOIN date_range`**：搜索 SQL 文本里**不能出现** `date_range AS` / `CROSS JOIN date_range`
- [ ] **`coin_agg` 不能缺**：搜索 SQL 文本里有没有 `coin_agg AS (`、`mnv_ads_ugc_cn.map_consumption_details_day`、`event_code = 'coin_pay'`——**三个 anchor 必须都在**
- [ ] **`LEFT JOIN coin_agg cn` 不能缺**：detail CTE 必须有这一行
- [ ] **不能有字面量 0 占位**：搜索 SQL 文本里**不能出现** `0 AS total_coin`、`0 AS coin_tx_cnt`、`0 AS avg_coin_per_user`、`0 AS avg_tx_per_user`（这 4 个字符串都是禁词）
- [ ] **最终 SELECT 16 列齐全**：从 `dt` 到 `avg_tx_per_user` 一个不少
- [ ] **`tables_used` 必须含 3 张表**：缺 `mnv_ads_ugc_cn.map_consumption_details_day` 任何一项都是错
- [ ] **表名**：Trino / Spark 生成的 SQL **均不得**出现 `hive.` / `trino.` catalog 前缀
- [ ] **方言一致（仅检查最终 `sql` 正文）**：Spark 不能出现 `regexp_like` / `%Y-%m-%d`；Trino 不能出现 `RLIKE` / `yyyy-MM-dd` / `date_sub(` / `to_date(`（日期运算请只在 **explanation** 里用文字描述，**不要**写进 Spark SQL）
- [ ] **CASE WHEN 三处一致**：filter 的正则 + SELECT 的 CASE + GROUP BY 的 CASE，三处实验ID 列表与标签必须**字符级**一致

如果发现任意 ❌ 项，**不要嘴硬，重新生成完整 SQL**。

---

## 推荐可视化（前端图表面板会读这些建议）

- **首选：分组折线图** — X 轴 `dt`，Y 轴 `ctr`，分组列 `exp_tag`。一条线一个实验组，看跨天 CTR 走势对比。
- **次选：分组折线图** — X 轴 `dt`，Y 轴 `avg_coin_per_user`，分组列 `exp_tag`。看人均收入差异（变现链路）。
- **第三：组合图（柱+线 双Y轴）** — X 轴 `dt`，柱状轴 `total_view`（量级），折线轴 `ctr`。看大盘曝光量与 CTR 关系（仅当只有 1 个实验组时使用）。

---

## 分析报告阶段要求（analyze-data）

「一、数据概览」（必含）：
1. 明确分析窗口（起止日期 + 共 N 天）；
2. 实验组列表（`label(exp_id)` + 累计 UV + 占总 UV 比例），用于检查灰度均衡性；
3. 全量 `total_view` / `total_click` 大盘量级（万 / 亿口径自动）。

「二、关键发现」（至少覆盖以下 4 条）：
1. **CTR 对比**：每组 `ctr` 窗口均值；与对照组比较的**百分点差**与**相对涨跌幅 %**。点名涨幅 / 跌幅显著的组（≥5% 相对差视为显著）。
2. **人均时长**：`avg_game_duration` 实验 vs 对照组差异；CTR 涨而时长跌 = 推荐质量下降的红旗；CTR 跌而时长涨 = 推荐更精准但召回收窄。
3. **人均迷你币**：`avg_coin_per_user` 与 `avg_tx_per_user`；变现链路是否被影响（CTR 涨但人均收入跌 → 推送了不付费的"看客"）。
4. **跨天稳定性**：实验效果是否多天一致；如某天突跳，给出业务假设（节假日 / 灰度调整 / 数据延迟）。

「三、结论与建议」：1~3 条可执行建议，例如"实验组 CTR 稳定 +8% 且 avg_coin_per_user 同向增长，建议放量到 50%"或"CTR 涨但收入跌，建议先暂停灰度排查推荐链路"。

「四、下一步可追问」：
- 按 `map_id` 切片，看哪些地图卡贡献了实验差异；
- 按 `app_version` 切片，看版本敏感性（某些版本 SDK 有兼容问题）；
- 时长 → 收入的 AB 弹性回归（`avg_game_duration` 增加 1 分钟带来多少 `avg_coin_per_user` 增量）。
