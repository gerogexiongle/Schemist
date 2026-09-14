# Codex MCP 接入

服务在现有 FastAPI 进程内提供无状态 Streamable HTTP MCP：

```text
POST /mcp
```

它直接复用 Web/飞书的请求模型和处理函数，因此 SQL 生成、执行限制、重试和分析口径保持一致。

## MCP 工具

| 工具 | 行为 | 是否执行 SQL |
|---|---|---|
| `ask_data` | 自然语言转 SQL、执行、可选分析 | 是，只读 |
| `generate_sql` | 生成 SQL 与说明 | 否 |
| `execute_readonly_sql` | 执行已有 SQL | 是，只读 |
| `service_health` | 返回服务健康状态 | 否 |

`ask_data` 和 `execute_readonly_sql` 默认最多向 Codex 返回 100 行，允许范围为 1 到 500 行。SQL 执行层继续只允许 `SELECT`、`WITH`、`EXPLAIN`、`SHOW`、`DESCRIBE` 和 `DESC` 前缀。

`ask_data` 和 `execute_readonly_sql` 的返回中包含 `verification`，其中保留实际执行 SQL、引擎、`trace_id`、`query_id` 和返回行数。插件要求 Codex 默认把这些核验信息展示给用户；只有用户明确要求隐藏 SQL 时才省略。

`execute_readonly_sql` 必须同时传入原始 `question`。后台查询历史保存 `source=mcp`、原始问题、稳定的 `question_id`、单次调用 `trace_id` 和数据库 `query_id`，用于把一次问数的生成、执行及补充验证关联起来。历史页可按 MCP 来源筛选。

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `MCP_ENABLED` | `1` | 设为 `0`、`false`、`no` 或 `off` 可关闭 POST 入口 |
| `MCP_API_TOKEN` | 空 | 非空时要求 `Authorization: Bearer <token>` |

配置 Token 后，Codex 使用环境变量传递凭据：

```bash
codex mcp add schemist \
  --url http://127.0.0.1:8889/mcp \
  --bearer-token-env-var SCHEMIST_MCP_TOKEN
```

## 协议自检

初始化：

```bash
curl -sS http://127.0.0.1:8889/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
```

列出工具：

```bash
curl -sS http://127.0.0.1:8889/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

分发文件位于 `codex_mcp_package/`。修改公网 MCP 地址时同步更新：

```text
codex_mcp_package/plugins/schemist/.mcp.json
```
