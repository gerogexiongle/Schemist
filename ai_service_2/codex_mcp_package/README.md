# Schemist Codex MCP 包

这个包把桌面 Codex 连接到Schemist服务：

- MCP 地址：`http://127.0.0.1:8889/mcp`
- Web 地址：`http://127.0.0.1:8889/`
- 能力：自然语言问数、生成 SQL、执行只读 SQL、结果分析、完整 SQL 核验、后台调用追踪
- 引擎：Trino（MCP 默认）和 Spark；用户明确指定 Spark 时自动切换

## 安装

把整个 `codex_mcp_package` 目录放到电脑上，然后执行对应脚本：

先编辑 `plugins/schemist/.mcp.json`，将 URL 改为自己的 Schemist 服务地址（示例为 `http://127.0.0.1:8889/mcp`），再进入 `codex_mcp_package` 目录执行安装。远程服务应填写客户端可访问的地址。

```bash
# macOS / Linux
bash install.sh
```

```powershell
# Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

脚本会把本目录注册为本地 Codex 插件市场，并安装 `schemist`。安装后重启桌面 Codex，打开新对话即可使用。

安装后，通过插件发起的成功问数默认会在回答末尾展示实际执行 SQL、引擎、追踪号、查询号和返回行数。服务后台的“完整查询历史”也会记录 MCP 来源、原始问题、问题编号和追踪号，可以按 MCP 来源筛选并对应核查。升级已有插件时请重新运行安装脚本，再重启 Codex 并打开新对话，避免旧会话继续使用缓存规则。

也可以不安装完整插件，只注册 MCP：

```bash
codex mcp add schemist --url http://127.0.0.1:8889/mcp
```

## 使用示例

- `用Schemist查询昨天各渠道的活跃用户数，并解释变化。`
- `先生成最近 7 天推荐点击率趋势的 Spark SQL，不要执行。`
- `用 Trino 查询本周和上周各召回类型的推荐量占比。`

Codex 会按需调用以下 MCP 工具：

- `ask_data`：自然语言转 SQL、执行、可选分析
- `generate_sql`：只生成 SQL，不执行
- `execute_readonly_sql`：执行已有的只读 SQL
- `service_health`：检查服务状态

Codex 可以根据任务灵活组合工具。直接问数可使用 `ask_data`；需要先审查或调整 SQL 时可使用
`generate_sql → execute_readonly_sql`。若目标是取数，只有执行工具返回
`workflow.data_returned=true` 才表示真正取得数据；不得重复提交完全相同的生成参数或失败 SQL。

## 鉴权

服务端未设置 `MCP_API_TOKEN` 时，MCP 与当前 Web API 一样无需鉴权。公网正式使用建议配置 Token：

```bash
MCP_API_TOKEN=replace-with-a-long-random-token
```

启用 Token 后，不要把 Token 写入插件包。请在桌面端设置环境变量，例如 `SCHEMIST_MCP_TOKEN`，再用以下配置注册：

```bash
codex mcp add schemist \
  --url http://127.0.0.1:8889/mcp \
  --bearer-token-env-var SCHEMIST_MCP_TOKEN
```

示例地址指向本机。连接远程服务时，把插件内 `.mcp.json` 的 URL 改为自己的 HTTPS 服务地址；插件应在修改地址后安装。
