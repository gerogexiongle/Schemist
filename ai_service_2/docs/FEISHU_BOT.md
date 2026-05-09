# 飞书（Lark）机器人对接 — Schemist `ai_service_2`

在飞书中 @ 机器人或私聊发送自然语言问题，由本服务完成 **Text2SQL → 在 Spark/Trino 上执行 → 生成 Markdown 分析报告**，并将结果回复到该条消息下。

实现入口：`feishu_bot_api.py`（FastAPI 路由与全流程编排）、`skills/feishu_client.py`（飞书 OpenAPI、消息解析、文本/卡片回复）。与 Web 共用 `generate_sql` / `execute_sql` / `analyze_data` 及 `config/settings.py` 中的引擎与资源限制，**不引入额外的「数据源」切换层**。

飞书开放平台要求回调在约 **3 秒内返回 HTTP 200**。本服务在校验通过后 **立即返回 `{"msg":"ok"}`**，实际问数在 **`asyncio.create_task` 后台任务**中执行，避免超时。

---

## 1. 功能说明

### 1.1 全流程在做什么

| 步骤 | 说明 |
|------|------|
| ① 收消息 | 订阅事件 `im.message.receive_v1`，解析文本（支持简单 `text` 与 `post` 富文本）。群聊里 @ 机器人后的内容会去掉开头的 `@xxx`。 |
| ② 生成 SQL | 调用与 Web 相同的 `generate_sql`（DeepAgent + Schema 工具），使用服务默认 LLM 模型；可带**本会话多轮历史**（见 §1.4）。 |
| ③ 执行 SQL | 默认模式下调用 `execute_sql`，`max_rows` 为 `QUERY_RESULT_MAX_ROWS`，`timeout` 为 `SQL_EXECUTOR_TIMEOUT`（与 Web/API 一致）。 |
| ④ 分析报告 | 调用 `analyze_data`，基于查询结果生成 Markdown 报告。 |
| ⑤ 执行成功后的副作用 | 与 Web 一致：可写入 **Schema 反馈学习**（`record_success_feedback_from_sql`）；并尝试生成 **网页分享报告**（见 §1.2）。 |
| ⑥ 回复飞书 | 默认优先发送 **交互卡片**（Markdown + 结果表格组件，见 `FEISHU_REPLY_INTERACTIVE`）；失败时回退为**纯文本**。内容过长时按 `FEISHU_REPLY_MAX_CHARS` 等分段/截断。 |

### 1.2 回复形态：卡片与纯文本

环境变量 **`FEISHU_REPLY_INTERACTIVE`**（默认开启）：为 `1` / 未关闭时，全流程成功等路径会调用 `reply_interactive_card`（飞书消息卡片，支持 lark_md 与表格预览）；失败或卡片发送失败时回退 `reply_message` 纯文本。

相关变量（`skills/feishu_client.py`）：

| 变量 | 作用 |
|------|------|
| `FEISHU_CARD_SCHEMA` | 卡片结构版本，默认 `2`（与飞书客户端版本要求以官方文档为准）。 |
| `FEISHU_CARD_MD_CHUNK` | 卡片内 Markdown 分段长度，默认 `3800`。 |

因此与旧版「仅一条长纯文本」不同：**默认以卡片为主、文本为兜底**；若你希望始终纯文本，可设置 `FEISHU_REPLY_INTERACTIVE=0`。

### 1.3 网页分享报告

全流程成功且分析完成后，服务端会调用与 Web 相同的 `persist_shared_report`，在回复中追加 **网页分享链接**（相对路径或带 `SERVICE_PUBLIC_ORIGIN` 的完整 URL）。链接有效期由分享模块配置（回复文案中常见为约 7 天）。

若未配置 **`SERVICE_PUBLIC_ORIGIN`**，分享行可能仅为相对路径 `/shared/...`，在飞书内点击需自行补全可访问的服务根地址。

### 1.4 与 Web 端的差异

- **无「数据源:ID」**：引擎与连接由服务端 `settings` 决定，不按消息切换业务库连接串。
- **LLM 模型**：飞书链路当前调用 `generate_sql` 时**未透传** `llm_model`，使用服务默认模型；与 Web 上按次选模型不同。
- **完整可视化**：飞书侧以卡片/文本为主；图表与完整 HTML 体验仍以 Web 为准。

### 1.5 事件去重

飞书可能重复投递同一事件。服务用 `header.event_id`（没有则用 `message_id`）在约 **180 秒**内去重（`_FEISHU_DEDUP_TTL_SEC`）。多 Worker 时各进程内存独立，若仍重复投递可考虑单实例或后续接入集中式去重。

### 1.6 会话与多轮

同一会话键：`feishu_{app_id}_{chat_id}`（`app_id` 中的 `/` 会替换为 `_`）。最近约 **10 条** user/assistant 消息注入 `generate_sql` 的 `history`。**进程重启后历史清空**。

### 1.7 Skills Pack（与 Web 对齐）

1. **显式套技能**：消息以 **`使用技能`** 开头（不区分大小写），紧接技能的**显示名**或 **id**（与 `skills_pack` 注册一致），其后为参数；渲染逻辑与 Web 选择技能后提交等价。  
2. **自动匹配技能**（默认开启）：根据技能元数据与用户输入做轻量打分；需达到 **`FEISHU_AUTO_SKILL_MIN_SCORE`**（默认 `65`），且第一名与第二名分差 ≥ **`FEISHU_AUTO_SKILL_MARGIN`**（默认 `12`）才会自动套模板。可通过 **`FEISHU_AUTO_SKILL_MATCH=0`** 关闭。  
3. **排除列表**：默认将 `data-insights` 排除在自动匹配之外（过泛），可通过 **`FEISHU_AUTO_SKILL_EXCLUDE_IDS`** 配置（逗号分隔 id）。

### 1.8 前缀解析顺序（重要）

服务端顺序：**先整句解析「引擎」前缀，再对剩余部分解析「全流程 / 仅SQL」前缀**。

- 推荐：**引擎写在最前**，例如：`引擎:trino 仅SQL 最近7天各渠道曝光汇总`。
- 若写成 `仅SQL 引擎:trino …`，则「仅SQL」后的整段会当作自然语言交给模型，**不会再**解析出 `引擎:trino`。

---

## 2. 飞书开放平台配置（摘要）

详细步骤以 [飞书开放平台文档](https://open.feishu.cn/document) 为准，要点如下：

1. **创建企业自建应用**，取得 **App ID**、**App Secret**。
2. **开启「机器人」能力**。
3. **权限**：至少 **发送消息**；需能读取用户发给机器人的消息（单聊/群聊 @ 机器人，具体权限名以控制台为准）。
4. **事件订阅**：「将事件发送至开发者服务器」，**请求 URL** 为公网可达地址，路径为：  
   `https://<你的域名>/feishu/event`  
   若前有反向代理，需保证最终到达本服务的路径仍为 **`/feishu/event`**。
5. **订阅事件**：`im.message.receive_v1`（接收消息 v2.0）。
6. **版本发布**：保存权限与事件后创建版本并发布。
7. **使用**：单聊直接发；群聊需 **@机器人** 再输入问题。

保存订阅 URL 时，飞书会 POST `type: url_verification`，本服务返回 `{"challenge":"..."}` 完成校验。

---

## 3. 消息写法与模式

### 3.1 默认：全流程（`full`）

直接发业务问题即可。流程：**生成 SQL → 执行 → 分析报告 → 回复**（含 SQL、说明、结果预览、报告节选；可能含分享链接）。

可选显式前缀（与默认等价）：`全流程`、`自动执行`、`自动` + 空格 + 问题。

### 3.2 仅生成 SQL（`sql_only`）

句首以下**任一**前缀 + 空格 + 问题：`仅SQL`、`只生成SQL`、`只要SQL`、`只生成`。

### 3.3 指定 SQL 引擎

在**整句最前面**（中英文冒号均可）：

- `引擎:trino …` / `引擎:spark …`
- `engine:trino …`（不区分大小写）

未指定时使用 `config.settings.SQL_ENGINE_DEFAULT`。

### 3.4 默认改为「仅 SQL」

```bash
export FEISHU_BOT_DEFAULT_PIPELINE=sql_only
```

未设置或非 `sql_only` 时，默认仍为全流程。

### 3.5 回复长度与分段

- `FEISHU_REPLY_MAX_CHARS`（默认 `18000`）：整体回复字符上限相关逻辑见 `prepare_feishu_reply_text` 与卡片组装。
- 卡片内 Markdown 另受 `FEISHU_CARD_MD_CHUNK` 控制。

---

## 4. 环境变量一览

| 变量 | 必填 | 说明 |
|------|------|------|
| `FEISHU_APP_ID` | 单应用时必填 | 飞书应用 App ID（`cli_xxx`） |
| `FEISHU_APP_SECRET` | 单应用时必填 | 应用 Secret |
| `FEISHU_APP_MAPPINGS` | 否 | 多应用共用一个回调：JSON **数组**，每项含 `app_id`、`app_secret`；事件头 `app_id` 须能匹配一条。未配置则只用单应用变量。 |
| `FEISHU_BOT_DEFAULT_PIPELINE` | 否 | `full`（默认）或 `sql_only` |
| `FEISHU_REPLY_MAX_CHARS` | 否 | 单条回复相关上限，默认 `18000` |
| `FEISHU_REPLY_INTERACTIVE` | 否 | 默认 `1`：全流程等优先发交互卡片；`0`/`false`/`off` 则只用纯文本 |
| `FEISHU_CARD_SCHEMA` | 否 | 卡片 schema，默认 `2` |
| `FEISHU_CARD_MD_CHUNK` | 否 | 卡片 Markdown 分块大小，默认 `3800` |
| `FEISHU_AUTO_SKILL_MATCH` | 否 | 默认 `1` 开启自动技能匹配；`0`/`false`/`off` 关闭 |
| `FEISHU_AUTO_SKILL_MIN_SCORE` | 否 | 自动匹配最低分，默认 `65` |
| `FEISHU_AUTO_SKILL_MARGIN` | 否 | 第一名与第二名最小分差，默认 `12` |
| `FEISHU_AUTO_SKILL_EXCLUDE_IDS` | 否 | 逗号分隔的技能 id，不参与自动匹配；默认含 `data-insights` |
| `SERVICE_PUBLIC_ORIGIN` | 否 | 分享报告完整 URL 前缀，如 `https://sql.example.com`（无尾斜杠） |

**导出飞书云文档**（Web 上的「生成飞书文档」等）另需 `FEISHU_DOC_FOLDER_TOKEN` 等，见 `app.py` 中相关接口与 `.env.example`。

修改环境变量后需**重启**服务进程。

---

## 5. 依赖与安装

本模块依赖 **`httpx`**（已在 `ai_service_2/requirements.txt` 中声明）。在运行服务的同一 Python 环境中安装项目依赖即可，例如：

```bash
cd ai_service_2
pip install -r requirements.txt
```

---

## 6. HTTP 路由

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/feishu/event` | 探测用，返回 JSON 说明 |
| POST | `/feishu/event` | 事件回调：`url_verification` 返回 `challenge`；`im.message.receive_v1` 立即 `200` 且 `{"msg":"ok"}`，后台异步处理 |

可在服务 **`/docs`** OpenAPI 中确认路由与标签 `feishu`。

---

## 7. 日志与排查

- 日志 logger：`feishu_bot`、`skills.feishu_client` 等；pipeline 打点关键字含 `feishu.pipeline`、`feishu.phase`（若启用链路追踪 UI，可与 Web 侧配合查看）。
- 仅有 `url_verification`、无业务事件：检查机器人、应用发布状态、事件订阅、群聊是否 @ 机器人。
- `get_tenant_access_token failed`：核对 `FEISHU_APP_ID` / `SECRET` 或 `FEISHU_APP_MAPPINGS` 是否与当前应用一致。
- 执行/分析失败：错误信息中会含引擎返回摘要，可与 Web 同问题对照；同时确认 `SQL_EXECUTOR_TIMEOUT`、`QUERY_RESULT_MAX_ROWS` 等是否与预期一致。

---

## 8. 设计说明（无第三方项目对照）

本实现遵循常见 **Webhook 长任务**模式：校验与入队尽快返回 200，重逻辑异步执行，避免飞书侧读超时。与 Web 共用同一套 SQL 生成、执行与分析代码，便于行为一致与回归测试；飞书专属逻辑集中在 **`feishu_bot_api.py`** 与 **`skills/feishu_client.py`**，便于按需裁剪或替换为其他 IM 通道。
