# Schemist

**Schema-Guided SQL Assistant** — 在本地表结构（JSON 元数据）约束下，用自然语言生成 **Spark SQL** 或 **Trino SQL**，可选执行、分析与分享。

本仓库是面向开源发布的版本：核心服务代码在 `ai_service_2/`，示例/占位级 Hive 表描述在 `hivebrain/`（请替换为你自己的元数据，勿将生产库表结构直接公开）。

## 功能概览

- **Text2SQL**：自研轻量多轮 Agent（`DeepAgent`）+ 工具调用：`search_tables`、`get_table_info`，结合 BM25/规则检索与同义词扩展。
- **双引擎执行**：只读 SQL → `spark-sql`（YARN 等由环境配置）或 Trino 连接器；校验、方言修复、超时与 Spark 会话级 SET 可配置。
- **Skills Pack**：`skills_pack/*/SKILL.md`（YAML frontmatter + Markdown 模板），前端或管理 API 套模板后走同一套生成链路；内置若干示例技能可仿写。
- **Web UI + OpenAPI**：FastAPI、Jinja2 单页；健康检查、查询历史与统计、报告分享、可选 LLM 模型列表等 REST 接口。
- **飞书 / Lark（可选）**：事件订阅 **`POST /feishu/event`** 走与 Web 相同的生成 → 执行 → 分析链路；支持 **交互卡片** 回复、全流程成功后的 **网页分享报告**链接、与 Web 一致的 **Skills Pack**（`使用技能` / 自动匹配）。Web 端可将分析报告 **导出为飞书云文档**（需配置开放平台凭证）。对接步骤与全部环境变量见 **[飞书机器人对接说明](./ai_service_2/docs/FEISHU_BOT.md)**；飞书侧应用、权限与事件订阅以官方文档为准：[飞书开放平台 · 文档中心](https://open.feishu.cn/document)。

更完整的设计说明见仓库根目录 **[技术架构设计.md](./技术架构设计.md)**（与代码目录 `ai_service_2/` 对齐的开源版架构文档）。

## 仓库结构

| 路径 | 说明 |
|------|------|
| `ai_service_2/` | FastAPI 应用、Agent、skills、模板、启动脚本与 `config/` |
| `hivebrain/` | 表 JSON 目录布局（`库名/json/*.json`）；**库内 `.json` 与 `markdown/` 由 `.gitignore` 排除**，不入库真实数仓元数据；本地用 `get_spark_table_shecme_filter.py` 生成后仅供本机 `SCHEMA_BASE_DIRS` 使用 |
| `技术架构设计.md` | 架构、模块、API、配置项说明（开源脱敏版） |
| `ai_service_2/docs/FEISHU_BOT.md` | 飞书机器人：路由、模式、环境变量与排查 |
| `get_spark_table_shecme_filter.py` | 根目录：**PySpark + spark-submit** 离线拉 Hive 元数据、清洗结构化后写入 **`hivebrain/<库名>/`**（见 README「离线生成」） |

## 快速开始

1. **Python**：**3.7+**（依赖见 `ai_service_2/requirements.txt`：其中为低于 3.8 的解释器声明了 `importlib-metadata`；核心 Agent 代码亦按 3.7 兼容编写）。维护者在 **3.7.4** 上可正常运行；**3.8 及以上**一般更省心，是否采用以本机 `pip install -r requirements.txt` 与启动、`/docs` 自检无报错为准。
2. **安装依赖**（在 `ai_service_2/` 下）：

   ```bash
   cd ai_service_2
   pip install -r requirements.txt
   # Trino 执行路径需额外：pip install trino
   ```

3. **配置**：复制 `ai_service_2/.env.example` 为 `.env`，填写 LLM 网关、Spark/Trino、Schema 目录等（勿将 `.env` 提交到 Git）。若启用飞书机器人或云文档导出，在同一 `.env` 中填写 `FEISHU_*` 等变量，详见 [FEISHU_BOT.md](./ai_service_2/docs/FEISHU_BOT.md) 与 `.env.example` 注释。

4. **启动**：

   ```bash
   cd ai_service_2
   ./start_service.sh
   # 或 ./run.sh 用于本地调试
   ```

   默认监听地址与端口见 `config/settings.py` 中的 `SERVICE_HOST` / `SERVICE_PORT`（常见为 `8889`）。停止：`./stop_service.sh`。

5. **浏览器**：打开服务根 URL（如 `http://127.0.0.1:8889/`），在界面中选择引擎、可选技能与模型后提问。

OpenAPI：`/docs`、`/redoc`。

### 离线生成 `hivebrain/` 元数据（可选）

仓库根目录 **`get_spark_table_shecme_filter.py`** 需在具备 **PySpark**、且 Spark 能访问 **Hive Metastore** 的环境运行（典型为 **`spark-submit get_spark_table_shecme_filter.py ...`**，脚本内使用 `SparkSession.builder...enableHiveSupport()`）。通过 `SHOW CREATE TABLE`、`DESCRIBE EXTENDED` 等拉取表结构，经 `--only-with-comments` / `--check-data` 等参数过滤后，输出 **JSON**（及可选 **Markdown**）与中文注释统计。

将 **`--output` 指向 `hivebrain/<数据库名>/`**（或与 `SCHEMA_BASE_DIRS` 中某一库目录一致），会在其下创建 **`json/`**（文件名为 `<库>.<表>.json`）等子目录，与 `ai_service_2` 的 **`库名/json/*.json`** 约定一致，供本地 Schema 索引使用。默认输出目录为 `./table_filter_schemas`，接入本仓库时请显式改到 `hivebrain/...`。

勿将含生产库表命名的导出直接提交到公开 Git；发布前请脱敏或改用自建示例库。

## 配置要点（摘要）

- **LLM**：OpenAI 兼容接口（`LLM_API_URL`、`LLM_API_KEY`、`LLM_MODEL` 等）。
- **Schema**：`SCHEMA_BASE_DIRS` 指向含 `库名/json/*.json` 的目录；可用 `SCHEMA_ALLOWED_DBS`、`schema_aliases.json` 等同义词与检索配置。
- **执行**：`SQL_EXECUTOR_TIMEOUT`、`SPARK_*`、`TRINO_*` 等见 `config/settings.py` 与 **技术架构设计.md** §5。

## 参与贡献与合规

- 提交前请确认未包含密钥、内网地址、真实业务数据导出。
- 欢迎 Issue / PR；较大改动建议先对照 **技术架构设计.md** 中的模块边界，避免破坏「工具层 / 执行层 / 模板层」分离。

## 许可证

若根目录未包含 `LICENSE` 文件，以你后续补充的许可证条款为准；在补充前请勿默认视为公共领域。
