---
name: ask-data
description: 使用Schemist MCP 处理自然语言数据查询、Text2SQL、Spark/Trino 只读 SQL 执行和结果分析。用户询问业务指标、趋势、明细、分组对比、数据口径或明确要求使用Schemist时使用。
---

# Schemist

## 工作流

1. 根据任务灵活选择工具：`ask_data` 适合直接完成生成、执行和分析；需要先检查或编辑 SQL 时可先调用 `generate_sql`；已有 SQL 时直接调用 `execute_readonly_sql`。不要为了形式固定使用某一种工具。
2. 调用任何问数工具时，把用户给出的指标口径、维度、筛选条件和日期范围完整写入 `question`。
3. `generate_sql` 只生成 SQL，绝不代表已经取到数据。若用户目标是取数，生成成功且语义完整后，应检查 `verification.final_sql`，必要时做最小修复，然后调用 `execute_readonly_sql`。
4. `execute_readonly_sql` 仅接受只读语句，并且必须把完整的原始用户问题传入 `question`，让后台把问题与 SQL 关联起来。
5. 用户未指定引擎时用 `trino`；明确要求 Spark 时用 `spark`。同一问题的生成和执行必须使用同一个引擎。
6. 不用完全相同的参数连续调用 `generate_sql`。只有出现具体校验错误、执行错误或 `missing_stages` 时，才允许携带这些新信息重新生成一次。
7. `execute_readonly_sql` 失败后不得原样重试同一 SQL；先根据错误做最小修改，无法可靠修复时再调用一次 `generate_sql`。
8. 读取工具返回的 `workflow`：`data_returned=false` 时不得向用户声称已经取得数据；结合 `recommended_next_tool`、错误和用户意图决定下一步，Codex 保留工具选择权。
9. 一般查询先用 `max_rows=100`。只有用户确实需要更多明细时才提高，最大 500。
10. 工具失败时保留原始错误，说明失败发生在生成、校验、执行还是分析阶段。不要编造结果。

## 回答要求

- 先给结论，再给关键数据和口径。
- 成功调用 `ask_data` 或 `execute_readonly_sql` 后，除非用户明确要求隐藏 SQL，否则必须原样展示 `verification.final_sql`，使用带 `sql` 标识的代码块，不得改写、截断或用重新生成的 SQL 替代。
- SQL 后必须列出 `verification.engine`、`verification.trace_id`、`verification.query_id` 和 `verification.row_count`，方便在Schemist后台定位同一次执行。
- 若一个结论使用了多条实际执行 SQL，逐条展示对应 SQL 和核验信息。
- 结果为 0 行时说明是查询成功但没有命中数据，并建议核对日期分区和枚举值。
- 只有 `ask_data` 或 `execute_readonly_sql` 返回 `workflow.data_returned=true`，才能基于结果给出数据结论。
- 指标、日期范围或分组维度有实质歧义时，先向用户确认。
- 不把查询结果描述成实时数据，除非工具结果明确支持这一结论。
- 所有数据库操作均视为只读，不建议或尝试绕过服务端限制。
