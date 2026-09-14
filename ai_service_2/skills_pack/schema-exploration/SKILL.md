---
id: schema-exploration
name: 库表探索
description: 只帮我找相关表与核心字段，不急着写完整 SQL
icon: 🔎
placeholder: 例如：推荐算法实验分流相关的表有哪些，包含哪些核心字段
enabled: true
order: 20
tags:
- 探索
- 元数据
---

【目标】帮用户在当前数仓中定位与问题相关的 **库表、核心字段、典型用法**，不要急于写最终 SQL。

【用户问题】
{{user_query}}

【当前 SQL 引擎】{{engine}}

【执行策略】
1. 先用 `search_tables` 按关键词（含同义词扩展）检索最相关的 5–10 张表。
2. 对 TOP 2–3 张表调 `get_table_info`，列出：
   - 分区字段
   - 主维度列（user_id、game_id、exp_id、scene_id …）
   - 核心指标列（推荐数、曝光、点击、时长…）
   - 注释中提到的口径或业务含义
3. 给出**如果要进一步分析，可以继续问**的 3 条引导式问题。

【输出要求】
- 以 Markdown 表格列出：`表名 | 简要注释 | 关键字段 | 分区 | 适用场景`。
- **不需要完整 SQL**，可给 1–2 个示例 `SELECT ... LIMIT 10` 用于验证表里的值。
- 最后一节「建议追问」：给 3 条面向该用户问题的下一步问法。
