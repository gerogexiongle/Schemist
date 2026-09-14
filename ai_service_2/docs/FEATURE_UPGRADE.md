# Schemist 功能与配置升级

本次升级覆盖 2026 年 9 月的查询引擎、Web、飞书和 MCP 功能。项目名称、网页标题、分析报告、MCP 服务与插件统一使用 Schemist。

## 功能清单

| 能力 | 实现及行为 |
|---|---|
| 统一执行 | `skills/sql_pipeline.py` 编排生成、预校验、执行、修复和分析；Web/飞书/MCP 复用 |
| 并发响应 | 生成和执行等同步任务通过线程执行，追踪上下文跨线程传递 |
| 生成预算 | 普通查询限制工具轮数、搜表次数、取表次数与总调用数；拦截重复调用并强制收口 |
| 模型兜底 | 收口失败、校验失败或复杂 JOIN 可调用一次配置的强模型；已选择强模型时避免再次切换 |
| 复杂漏斗 | 根据配置识别多阶段问题，逐阶段检索、置顶已确认映射，校验阶段覆盖并补齐缺失阶段 |
| 补充范围 | 用户明确要求“仅针对/只补充”时，阶段检查依据补充范围，避免恢复整段历史漏斗 |
| Trino | 配置角色请求头、可用角色探测、直连代理处理、EXPLAIN 预检与日期类型确定性修复 |
| 错误处理 | 权限不足及取消不自动重试；无变化的修复 SQL 不重复执行；保留错误分类与修复记录 |
| 查询追踪 | `question_id` 关联相同问题，`trace_id` 关联一次调用，`query_id` 对应一次执行；保存原始问题及 SQL |
| Web | 统一全流程调用、状态快照轮询、完整查询历史、来源/IP/问题/追踪号筛选与 SQL 复制 |
| 飞书 | 共用流水线、历史记录与报告生成；保留多轮上下文、技能匹配、交互卡片与云文档 |
| MCP | `ask_data`、`generate_sql`、`execute_readonly_sql`、`service_health`；返回 SQL 核验与下一步提示 |

## 已有部署升级

1. 安装 `requirements.txt` 中的依赖；本次补充了 Skills Pack 注册表直接依赖的 PyYAML。使用 Trino 时另安装 `trino`。
2. 保留自己的 `.env`，按 `.env.example` 补充需要启用的配置。默认模型列表沿用 Schemist 的通用配置，可用 `LLM_MODEL_CHOICES_JSON` 指定网关实际支持的模型。
3. `SQL_AGENT_FALLBACK_MODEL` 默认 `gpt-4o`，应改为自己网关可用的模型；设为空可关闭兜底。`SQL_AGENT_STRONG_MODELS` 是无需再次升级的模型 ID 集合。
4. 元数据目录默认是仓库内的 `hivebrain/` 和 `table_filter_schemas/`；临时 SQL/CSV 默认在 `ai_service_2/temp_sql_v2/`，支持 `SCHEMIST_WORKSPACE_DIR`。
5. 需要 MCP 鉴权时配置 `MCP_API_TOKEN`；客户端地址与凭据配置见 [CODEX_MCP.md](./CODEX_MCP.md)。
6. 完成配置后由部署者重启服务。查询历史保留在 `logs/query_history.json`，内存中的追踪快照和会话缓存会在重启后清空。

普通工具预算默认 4 轮、2 次搜表、6 次取表、8 次总调用。复杂漏斗使用独立预算，配置项为 `SQL_AGENT_COMPLEX_*`。最终兜底默认超时 120 秒，执行错误修复兜底默认 60 秒。

## 接入自己的复杂漏斗

仓库仅提供 `config/complex_query_mappings.example.json` 的空配置结构，不携带业务阶段、表映射或指标口径。按需复制后在本地填写自己的阶段定义；`stages` 为空时不启用漏斗扩展。

```bash
cp config/complex_query_mappings.example.json config/complex_query_mappings.json
```

编辑本地配置，逐项确认：

| 字段 | 用途 |
|---|---|
| `activation` | 最少阶段数、无漏斗关键词时的阈值、触发词 |
| `id` / `label` | 阶段标识与展示名 |
| `match_any` / `match_all` | 自然语言匹配；后者各组之间为 AND，组内为 OR |
| `search_query` | 该阶段的搜表关键词 |
| `pinned_tables` | 已确认表；仅在本地 Schema 中存在时置顶 |
| `business_rules` | 已确认的去重、日期、事件与指标规则，注入生成和修复上下文 |
| `coverage_any` | 任一候选满足表名和 `all_markers` 时视为覆盖该阶段 |

也可用 `COMPLEX_QUERY_MAPPING_PATH` 指向仓库外的 JSON。默认本地配置文件已由 `.gitignore` 排除。映射修改后按文件修改时间刷新缓存；更改环境变量需重启服务。

覆盖检查基于 SQL 中的表名和标记匹配，不等价于完整 SQL 语义证明。应把必要的阶段/指标标记写入配置，并通过自己的数据验证口径。

## 通用技能模板

| 技能 ID | 能力 |
|---|---|
| `schema-exploration` | 表与字段探索 |
| `sql-rewrite` | SQL 改写和方言转换 |
| `data-insights` | 通用数据分析 |

开源保留技能注册、管理、渲染和匹配机制。部署者可按自己的业务在本地创建技能；新增目录默认由 `.gitignore` 排除，仅上述 3 个通用模板在允许列表中。飞书按技能名称、ID、标签和描述进行匹配，保留库表探索与 SQL 改写的通用识别规则。

业务阶段映射、技能口径、真实元数据和运行日志应保留在部署环境。贡献新的通用模板时，应检查其内容并明确更新 Git 允许列表。

## 验证

在 `ai_service_2/` 下运行 `python tests/run_tests.py`。测试副本不携带本机 `.env`、运行数据或业务映射，使用伪造模型和数据库结果，并阻止网络连接。

测试覆盖工具预算、兜底、漏斗语义检查、SQL 修复、线程执行、Trino 预检、飞书记录，以及真实 ASGI 路由上的 MCP 初始化、鉴权、参数校验、写语句拒绝、问数结果核验、历史关联和模板渲染。真实数据准确性仍需在部署环境接入自己的 LLM 与数仓后验证。
