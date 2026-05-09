# hivebrain — 本地标准化 Schema 知识库

本目录存放 **Schemist `ai_service_2`** 使用的 **Hive 表结构 JSON**（及可选 Markdown 导出），供 Agent 的 `search_tables` / `get_table_info` 等工具索引。**目录名与层级可提交 Git**；**各库下的 `.json` 与 `markdown/` 已在仓库根 `.gitignore` 中忽略**，避免把真实数仓元数据推进远程仓库。本地生成后保留在工作区即可。

---

## 1. 目录约定（与 `schema_skill` 一致）

每个 Hive **库**对应一个子目录，表级 JSON 放在该库下的 **`json/`** 中：

```text
hivebrain/
├── README.md                 # 本说明（可提交）
├── <数据库名>/
│   ├── json/
│   │   └── <库名>.<表名>.json   # 单表一份结构化元数据
│   └── markdown/             # 可选：脚本生成的 Markdown（整目录被 gitignore）
└── …
```

服务侧通过环境变量 **`SCHEMA_BASE_DIRS`**（见 `ai_service_2/.env.example`）指向 **`hivebrain`** 的父路径或本目录，使索引能扫到 `<库>/json/*.json`。

---

## 2. 如何生成并写入本目录

使用仓库根目录脚本 **`get_spark_table_shecme_filter.py`**：需在能访问 **Hive Metastore** 的环境用 **PySpark** 运行（一般为 **`spark-submit`**，脚本内 `enableHiveSupport()`）。

在 **Schemist 仓库根目录** 执行（路径相对当前工作目录）：

```bash
# 单库全表（示例）
spark-submit get_spark_table_shecme_filter.py \
  --database <你的库名> \
  --table "*" \
  --only-with-comments \
  --output hivebrain/<你的库名>

# 仅 JSON（不需要 Markdown 时可加 --format json）
spark-submit get_spark_table_shecme_filter.py \
  --database <你的库名> \
  --table "ods_*" \
  --output hivebrain/<你的库名> \
  --format json

# 多库：--database "*" 与脚本内逻辑见脚本 --help 及文件头注释
```

要点：

- **`--output`** 必须指到 **`hivebrain/<数据库名>/`**（或与 `SCHEMA_BASE_DIRS` 中某一库根路径一致），脚本会在其下创建 **`json/`**（及默认的 **`markdown/`**），文件名形如 **`<库>.<表>.json`**。
- 默认输出根目录为 `./table_filter_schemas`；接入本仓库时请**显式**写成 `hivebrain/...`，否则会生成在仓库外或错误位置。
- 可选参数 **`--only-with-comments`**、**`--check-data`** 等用于过滤无中文注释或近期无数据的表，详见脚本内说明。

更多上下文见仓库根目录 **[README.md](../README.md)** 中「离线生成 `hivebrain/` 元数据」一节。

---

## 3. 与 `ai_service_2` 联调

1. 生成或拷贝 JSON 到 **`hivebrain/<库>/json/`**。  
2. **`SCHEMA_BASE_DIRS`**：未设置环境变量时，`config/settings.py` 默认已包含 **仓库根下的 `hivebrain/`**（以及可选的 `table_filter_schemas/`）。若你放在其他盘符或路径，再在 `ai_service_2/.env` 中设置 **`SCHEMA_BASE_DIRS`**，多个根目录用 **分号** 分隔（见 `.env.example`）。  
3. 重启服务后，在 Web 或 API 中提问，确认能检索到表。

---

## 4. Git 与开源合规

- **可提交**：本 `README.md`、目录占位、脱敏后的示例 SQL 等（不以 `hivebrain/**/*.json` 形式存在即可）。  
- **勿提交**：从生产/测试仓拉取的 **JSON**、**markdown/** 下的导出（已被 `.gitignore` 忽略；若历史上曾误提交，需在仓库根对相应路径执行 `git rm --cached` 后再推送）。  
- 对外开源前请确认 JSON 内无真实业务表名/字段敏感描述，或仅使用自建假数据。

---

## 5. 命名说明

仓库根脚本文件名为 **`get_spark_table_shecme_filter.py`**（历史拼写保留）；功能为从 Spark/Hive 拉取表结构并清洗为文档化 JSON。
