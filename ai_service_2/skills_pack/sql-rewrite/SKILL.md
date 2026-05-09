---
id: sql-rewrite
name: SQL 改写
description: 把给定 SQL 改写为更高效 / 兼容目标引擎的版本，含优化说明
icon: ♻️
placeholder: 贴上要改写的 SQL，并说明目标（例如：改成 Trino、去除 BIGINT 整除、加 dt 分区过滤）
enabled: true
order: 30
tags:
  - 优化
  - 改写
---

【任务】对用户给出的 SQL 做**语义等价**改写，兼顾正确性、性能与引擎兼容。

【用户输入（含原 SQL 与改写目标）】
{{user_query}}

【当前 SQL 引擎】{{engine}}

【改写纪律】
- 优先保证**语义不变**；若不得不改语义（例如口径错误），必须在 explanation 里高亮告知。
- Spark ↔ Trino 互转：别名引号、CONCAT / ||、LATERAL VIEW / UNNEST、日期函数需逐一处理。
- Trino 必须避免 BIGINT 直接相除导致整数截断；推荐使用 `x * 1.0 / NULLIF(y, 0)`、`CAST(... AS DOUBLE)`。
- 分区字段（如 dt）必须在 WHERE 过滤；`OR` 条件不要跨越分区键，避免全表扫描。
- JOIN 顺序让小表在右；能下推到子查询的过滤不要留在外层。

【输出要求】
- 给出改写后的 SQL（可直接执行）。
- explanation 列出**至少 2 条具体优化点**（如：`将 a/b 改为 CAST(a AS DOUBLE)/b`，`将 WHERE dt BETWEEN … 改为 = 近一日`）。
- tables_used 与原 SQL 保持一致（除非用户明确要求替换表）。
