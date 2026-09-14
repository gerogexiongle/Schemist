#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual SQL engine executor: Spark SQL + Trino
"""
import csv
import logging
import os
import re
import subprocess
import uuid
import time

from config.settings import (
    SPARK_SQL_CMD, SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_CORES,
    SPARK_EXECUTOR_INSTANCES, SPARK_DRIVER_MEMORY, SPARK_QUEUE,
    SPARK_DYNAMIC_ALLOCATION_ENABLED, SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS,
    SPARK_EXTRA_CONFS,
    SQL_EXECUTOR_TIMEOUT,
    TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_PASSWORD, TRINO_CATALOG, TRINO_SCHEMA,
    TRINO_ROLES, TRINO_HIVE_ROLE_PRIORITY, TRINO_SET_HIVE_ROLE_FALLBACK,
    TEMP_SQL_DIR, TEMP_CSV_DIR, QUERY_RESULT_MAX_ROWS,
)
from skills.sql_validator import (
    fix_spark_sql_syntax,
    fix_trino_sql_syntax,
    format_sql_error_context,
    validate_engine_sql,
)
from skills.pipeline_trace import log as pipeline_log
from skills.sql_execution_errors import format_execution_error_for_user, is_permission_denied_error

logger = logging.getLogger("sql_executor")

running_queries = {}


def _strip_trailing_semicolon_trino(sql):
    """Trino DBAPI execute() submits one statement; a trailing ';' triggers SYNTAX_ERROR (expects EOF)."""
    s = sql.rstrip()
    while s.endswith(";"):
        s = s[:-1].rstrip()
    return s


def _keep_first_executable_statement(sql):
    """
    Keep only the first SQL statement when multiple statements are present.
    This protects Trino pre-check (single executable statement) from LLM outputs like:
    `SELECT ...; -- optional breakdown\\nSELECT ...`
    """
    if not sql:
        return sql
    s = sql
    in_sq = False
    in_dq = False
    in_bt = False
    escape = False
    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if ch == "\\" and in_sq:
            escape = True
            continue
        if ch == "'" and not in_dq and not in_bt:
            in_sq = not in_sq
            continue
        if ch == '"' and not in_sq and not in_bt:
            in_dq = not in_dq
            continue
        if ch == '`' and not in_sq and not in_dq:
            in_bt = not in_bt
            continue
        if ch == ";" and not in_sq and not in_dq and not in_bt:
            tail = s[i + 1:]
            # Strip comments + whitespace in tail; if anything remains, it's multi-statement.
            tail_no_block = re.sub(r"/\*[\s\S]*?\*/", " ", tail)
            tail_no_line = re.sub(r"--[^\n\r]*", " ", tail_no_block)
            if tail_no_line.strip():
                return s[:i].rstrip()
    return s


def _is_allowed_query_type(sql):
    """Allow SELECT/WITH/EXPLAIN/SHOW/DESC even if leading comments exist."""
    if not sql:
        return False
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl == -1:
                return False
            s = s[nl + 1:].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/", 2)
            if end == -1:
                return False
            s = s[end + 2:].lstrip()
            continue
        break
    return bool(re.match(r"^(select|with|explain|show|describe|desc)\b", s, flags=re.IGNORECASE))


def _should_trino_explain(sql):
    """Only SELECT/WITH statements need Trino's type-aware validation."""
    if not sql:
        return False
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl == -1:
                return False
            s = s[nl + 1:].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/", 2)
            if end == -1:
                return False
            s = s[end + 2:].lstrip()
            continue
        break
    return bool(re.match(r"^(select|with)\b", s, flags=re.IGNORECASE))

os.makedirs(TEMP_SQL_DIR, exist_ok=True)
os.makedirs(TEMP_CSV_DIR, exist_ok=True)


def convert_value(value):
    if value == "NULL" or value is None:
        return None
    try:
        if isinstance(value, str):
            if value.isdigit():
                return int(value)
            if '.' in value:
                try:
                    return float(value)
                except ValueError:
                    pass
        return value
    except (ValueError, TypeError):
        return value


def _extract_outer_select(sql):
    """Extract the outer SELECT clause from SQL (skipping CTEs)."""
    upper = sql.upper()
    depth = 0
    i = 0
    while i < len(upper):
        if upper[i] == '(':
            depth += 1
        elif upper[i] == ')':
            depth -= 1
        elif depth == 0 and upper[i:i+6] == 'SELECT':
            rest = sql[i+6:]
            from_pos = None
            d = 0
            for j, c in enumerate(rest):
                if c == '(':
                    d += 1
                elif c == ')':
                    d -= 1
                elif d == 0 and rest[j:j+4].upper() == 'FROM' and (j == 0 or not rest[j-1].isalnum()):
                    from_pos = j
                    break
            if from_pos is not None:
                return rest[:from_pos].strip()
            return rest.strip()
        i += 1
    return None


def _extract_sql_columns(sql):
    """Extract column names/aliases from the SQL SELECT clause."""
    try:
        columns_text = _extract_outer_select(sql)
        if not columns_text:
            match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not match:
                return []
            columns_text = match.group(1)

        column_specs = []
        current = ""
        depth = 0
        for ch in columns_text:
            if ch == '(':
                depth += 1
                current += ch
            elif ch == ')':
                depth -= 1
                current += ch
            elif ch == ',' and depth == 0:
                column_specs.append(current.strip())
                current = ""
            else:
                current += ch
        if current.strip():
            column_specs.append(current.strip())

        names = []
        for spec in column_specs:
            spec = spec.strip()
            alias = None
            # Spark: AS `任意字符含中文、括号、横线`；Trino: AS "..."
            m_bt = re.search(r"\bAS\s+`([^`]+)`\s*$", spec, re.IGNORECASE | re.DOTALL)
            if m_bt:
                alias = m_bt.group(1).strip()
            if alias is None:
                m_dq = re.search(r'\bAS\s+"([^"]+)"\s*$', spec, re.IGNORECASE | re.DOTALL)
                if m_dq:
                    alias = m_dq.group(1).strip()
            if alias is None:
                m_word = re.search(r"\bAS\s+(\w+)\s*$", spec, re.IGNORECASE | re.DOTALL)
                if m_word:
                    alias = m_word.group(1)
            if alias is not None:
                names.append(alias)
                continue

            parts = spec.split('.')
            simple = parts[-1].strip().strip('`"')
            if '(' in simple:
                func_match = re.match(r'(\w+)\(', simple)
                if func_match:
                    names.append(func_match.group(1).upper())
                else:
                    names.append("EXPR_{}".format(len(names) + 1))
            else:
                names.append(simple)
        return names
    except Exception:
        return []


def _compact_spark_error(stderr):
    """
    去掉常见 WARN/Session 噪音；若存在 'Error in query:' 只保留其后可读若干行，
    截断 Catalyst 解析计划长行，便于前端展示。
    """
    if not stderr:
        return ""
    noise_subs = (
        "WARN ", "WARNING", "Warning:", "Hive Session ID", "Spark master:",
        "Time taken:", "Fetched ", "[Stage", "Application Id:",
        "Ignoring non-Spark", "Info: ", "INFO ",
    )
    kept = []
    for line in stderr.split("\n"):
        s = line.strip()
        if not s:
            continue
        if any(x in s for x in noise_subs):
            continue
        kept.append(line.rstrip())

    text = "\n".join(kept).strip()
    if not text:
        text = stderr.strip()

    if "Error in query:" in text:
        idx = text.find("Error in query:")
        tail = text[idx:]
        out = []
        for ln in tail.split("\n"):
            t = ln.strip()
            if not t:
                continue
            if t.startswith("+-") or t.startswith("|--"):
                break
            if len(t) > 200 and any(k in t for k in ("'Project", "'Sort", "'Aggregate", "'Join")):
                break
            out.append(t)
            if len(out) >= 15:
                break
        if out:
            return "\n".join(out)

    return text


def _is_spark_cli_status_line(line):
    """仅过滤 Spark/Hive CLI 状态行，避免误伤列内容含 Fetched/Time taken 子串的数据行。"""
    s = line.strip()
    if not s:
        return True
    if s.startswith("Time taken:"):
        return True
    if re.match(r"^Fetched\s+\d+\s+row", s):
        return True
    if "Hive Session ID" in s and "=" in s:
        return True
    if s.startswith("Spark master:"):
        return True
    return False


def _looks_like_kv_config_line(line):
    """
    Hive 可能打印 key=value 配置行；含多列 tab 的视为数据行。
    避免仅用 '=' in line 误删合法数据行。
    """
    s = line.strip()
    if "=" not in s:
        return False
    if "\t" in s:
        parts = s.split("\t")
        if len(parts) >= 2:
            return False
    return re.match(r"^[a-zA-Z0-9_.]+\s*=", s) is not None


def _parse_spark_output(stdout, sql):
    """Parse spark-sql stdout into headers + rows"""
    param_pattern = re.compile(r"(spark\.|hive\.|mapreduce\.).*|true|false")

    set_output_pattern = re.compile(
        r"^(spark\.|hive\.|mapreduce\.|io\.compression|pyspark\.enabled|"
        r"yarn\.|fs\.|dfs\.|hadoop\.|javax\.|tez\.|orc\.|parquet\.)",
        re.IGNORECASE
    )

    valid_lines = []
    for line in stdout.strip().split("\n"):
        line_strip = line.strip()
        if not line_strip:
            continue
        if "[Stage" in line or line_strip.startswith("[Stage"):
            continue
        if _is_spark_cli_status_line(line):
            continue
        if line.startswith("Warning:"):
            continue
        if set_output_pattern.match(line_strip):
            continue
        if param_pattern.match(line_strip):
            continue
        parts_check = line_strip.split("\t")
        if len(parts_check) == 2 and set_output_pattern.match(parts_check[0].strip()):
            continue
        valid_lines.append(line_strip)

    clean_lines = []
    single_col_candidates = []
    for line in valid_lines:
        if "\t" in line or (len(re.split(r"\s{2,}", line)) > 1):
            clean_lines.append(line)
        else:
            single_col_candidates.append(line)

    if not clean_lines and single_col_candidates:
        clean_lines = single_col_candidates
    elif clean_lines and single_col_candidates:
        all_param = all(param_pattern.match(cl.split("\t")[0].strip()) for cl in clean_lines)
        if all_param:
            clean_lines = single_col_candidates

    if not clean_lines:
        return [], []

    if len(clean_lines) >= 2:
        first_line = clean_lines[0]
        if param_pattern.search(first_line) or _looks_like_kv_config_line(first_line):
            clean_lines = clean_lines[1:]
        if clean_lines and (param_pattern.search(clean_lines[0]) or _looks_like_kv_config_line(clean_lines[0])):
            clean_lines = clean_lines[1:]

    if not clean_lines:
        return [], []

    sql_columns = _extract_sql_columns(sql)

    potential_header = clean_lines[0]
    is_header_row = False
    if len(clean_lines) > 1:
        first_parts = re.split(r"\t|\s{2,}", potential_header)
        second_parts = re.split(r"\t|\s{2,}", clean_lines[1])
        if len(first_parts) != len(second_parts):
            is_header_row = True
        else:
            first_has_nums = any(p.replace(".", "", 1).replace("-", "", 1).isdigit() for p in first_parts)
            second_has_nums = any(p.replace(".", "", 1).replace("-", "", 1).isdigit() for p in second_parts)
            if not first_has_nums and second_has_nums:
                is_header_row = True

    if (
        not is_header_row
        and len(clean_lines) >= 2
        and sql_columns
        and len(sql_columns) == len(re.split(r"\t|\s{2,}", clean_lines[0]))
        and len(sql_columns) == len(re.split(r"\t|\s{2,}", clean_lines[1]))
    ):
        fp = re.split(r"\t|\s{2,}", clean_lines[0])
        sp = re.split(r"\t|\s{2,}", clean_lines[1])
        if all(
            a.strip().lower() == b.strip().lower() for a, b in zip(fp, sql_columns)
        ) and not all(
            a.strip().lower() == b.strip().lower() for a, b in zip(sp, sql_columns)
        ):
            is_header_row = True
            potential_header = clean_lines[0]

    if is_header_row:
        headers = [h.strip() for h in re.split(r"\t|\s{2,}", potential_header)]
        data_start = 1
    else:
        first_data = clean_lines[0]
        col_count = len(first_data.split("\t")) if "\t" in first_data else len(re.split(r"\s{2,}", first_data))
        if sql_columns and len(sql_columns) == col_count:
            parts0 = re.split(r"\t|\s{2,}", first_data)
            # 仅一行且与 SQL 列名完全一致 → Spark 只打印了表头、0 行数据
            if (
                len(clean_lines) == 1
                and len(parts0) == len(sql_columns)
                and all(
                    a.strip().lower() == b.strip().lower()
                    for a, b in zip(parts0, sql_columns)
                )
            ):
                return sql_columns, []
            headers = sql_columns
        else:
            headers = ["col_{}".format(i + 1) for i in range(col_count)]
        data_start = 0

    headers = [h.strip() for h in headers if h.strip()]
    if not headers:
        return [], []

    results = []
    for line in clean_lines[data_start:]:
        if param_pattern.match(line):
            continue
        if '\t' in line:
            parts = [p.strip() for p in line.split('\t')]
        else:
            parts = [p.strip() for p in re.split(r'\s{2,}', line)]

        if len(parts) > len(headers):
            parts = parts[:len(headers)]
        elif len(parts) < len(headers):
            parts.extend(["NULL"] * (len(headers) - len(parts)))

        row = {}
        for i, h in enumerate(headers):
            row[h] = convert_value(parts[i] if i < len(parts) else "NULL")
        results.append(row)

    return headers, results


def execute_spark_sql(sql, max_rows=QUERY_RESULT_MAX_ROWS, timeout=None):
    """Execute SQL via spark-sql CLI, return (success, headers, results, error, exec_time, query_id, debug)"""
    if timeout is None:
        timeout = SQL_EXECUTOR_TIMEOUT
    query_id = str(uuid.uuid4())
    start_time = time.time()

    sql = sql.strip()
    if not sql:
        return False, [], [], "SQL is empty", 0, query_id, {}

    if not _is_allowed_query_type(sql):
        return False, [], [], "Only SELECT/SHOW/DESCRIBE queries allowed", 0, query_id, {}

    original_sql = sql
    sql = fix_spark_sql_syntax(sql)
    if sql != original_sql:
        logger.info("Spark SQL auto-fixed: double-quote aliases -> backticks, || -> CONCAT()")

    sql_before_single = sql
    sql = _keep_first_executable_statement(sql)
    if sql != sql_before_single:
        logger.warning("Spark SQL auto-fixed: kept only first executable statement from multi-statement output")

    syntax_issues = validate_engine_sql(sql, engine="spark")
    if syntax_issues:
        msg = "Spark SQL 3.3.1 pre-check failed: " + "; ".join(syntax_issues)
        logger.warning("%s\nSQL:\n%s", msg, sql)
        return False, [], [], msg, 0, query_id, {}

    temp_file = os.path.join(TEMP_SQL_DIR, "spark_query_{}.sql".format(query_id))
    try:
        with open(temp_file, 'w') as f:
            f.write("SET hive.cli.print.header=true;\n")
            f.write("SET spark.sql.execution.arrow.pyspark.enabled=true;\n")
            # 性能 tuning：让 AQE 自动选择 BHJ/SMJ + 控制 shuffle 分区数 + skew join 自动拆分
            # 任意条可通过环境变量 SPARK_SQL_SET_OVERRIDES 覆盖（同名后置生效，per-query 临时设置）
            #
            # ⚠️ 不要把 autoBroadcastJoinThreshold 设得过大（如 512m+）+ driver 仅 4~6g：
            # 大窗口多表关联的中间结果序列化体积超过阈值时，
            # 强制 broadcast 会触发 driver 端 "Store broadcast fail / Multiple failures in stage materialization"。
            # 256m 是经验值：让 AQE 在小数据时仍走 BHJ，大数据自动 fallback SMJ，避免 driver OOM。
            f.write("SET spark.sql.autoBroadcastJoinThreshold=268435456;\n")  # 256MB
            f.write("SET spark.sql.shuffle.partitions=400;\n")
            f.write("SET spark.sql.adaptive.enabled=true;\n")
            f.write("SET spark.sql.adaptive.coalescePartitions.enabled=true;\n")
            f.write("SET spark.sql.adaptive.skewJoin.enabled=true;\n")
            # spark.driver.maxResultSize 等为静态配置，SQL 里 SET 会报
            # "Cannot modify the value of a Spark config"；需用 SPARK_EXTRA_CONFS 在 spark-sql 启动时传入。
            extra_set = (os.environ.get("SPARK_SQL_SET_OVERRIDES") or "").strip()
            if extra_set:
                for line in extra_set.replace("\r\n", "\n").replace("\r", "\n").replace(";", "\n").split("\n"):
                    line = line.strip()
                    if not line or "=" not in line:
                        continue
                    key = line.split("=", 1)[0].strip().lower()
                    # 静态 / 启动期配置，Spark SQL 会话内 SET 会失败（见 migration guide）
                    if key.startswith("spark.driver.") or key.startswith("spark.executor."):
                        logger.warning(
                            "Skipping SPARK_SQL_SET_OVERRIDES key %s (use SPARK_EXTRA_CONFS at spark-sql submit time)",
                            key,
                        )
                        continue
                    f.write("SET {};\n".format(line))
            f.write("\n")
            f.write(sql)
        os.chmod(temp_file, 0o755)
    except Exception as e:
        return False, [], [], "Failed to create temp file: {}".format(e), 0, query_id, {}

    cmd = [
        SPARK_SQL_CMD,
        "--master", "yarn",
        "--deploy-mode", "client",
        "--name", "SQL-V2-{}".format(query_id[:8]),
        "--executor-memory", SPARK_EXECUTOR_MEMORY,
        "--executor-cores", SPARK_EXECUTOR_CORES,
        "--num-executors", SPARK_EXECUTOR_INSTANCES,
        "--driver-memory", SPARK_DRIVER_MEMORY,
        "--queue", SPARK_QUEUE,
        "--conf", "spark.sql.adaptive.enabled=true",
    ]
    if SPARK_DYNAMIC_ALLOCATION_ENABLED:
        cmd.extend(["--conf", "spark.dynamicAllocation.enabled=true"])
        if SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS:
            cmd.extend(
                [
                    "--conf",
                    "spark.dynamicAllocation.maxExecutors={}".format(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS),
                ]
            )
    else:
        cmd.extend(["--conf", "spark.dynamicAllocation.enabled=false"])
    for _k, _v in SPARK_EXTRA_CONFS:
        cmd.extend(["--conf", "{}={}".format(_k, _v)])
    cmd.extend(["-f", temp_file])

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        running_queries[query_id] = {"process": process, "start_time": start_time, "temp_file": temp_file, "engine": "spark"}
        logger.info("Spark SQL started (ID: %s, PID: %d)", query_id, process.pid)

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            if process.poll() is None:
                process.kill()
            running_queries.pop(query_id, None)
            return False, [], [], "Timeout after {}s".format(timeout), time.time() - start_time, query_id, {}

        running_queries.pop(query_id, None)

        if process.returncode != 0:
            error_lines = []
            real_error = False
            for line in (stderr or "").split('\n'):
                line = line.strip()
                if not line:
                    continue
                if any(x in line for x in ['WARN', 'Warning:', 'Hive Session ID', 'Spark master', 'Time taken:',
                                            'Fetched', 'Stage', 'Application Id:', 'Ignoring non-Spark']):
                    continue
                real_error = True
                error_lines.append(line)
            if real_error:
                brief = _compact_spark_error(stderr)
                ctx = format_sql_error_context(sql, brief or stderr or "")
                if ctx:
                    logger.error(ctx)
                return False, [], [], brief or stderr or "Execution failed", time.time() - start_time, query_id, {}

        headers, results = _parse_spark_output(stdout, sql)

        if results and headers:
            csv_filename = "query_result_{}.csv".format(query_id[:8])
            csv_path = os.path.join(TEMP_CSV_DIR, csv_filename)
            try:
                with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for row in results:
                        writer.writerow({h: row.get(h, '') for h in headers})
            except Exception as e:
                logger.warning("CSV save failed: %s", e)
                csv_filename = None
        else:
            csv_filename = None

        total_rows = len(results)
        if max_rows > 0 and len(results) > max_rows:
            results = results[:max_rows]

        debug_info = {
            "csv_filename": csv_filename,
            "total_rows": total_rows,
            "returned_rows": len(results),
            "engine": "spark",
        }

        return True, headers, results, None, time.time() - start_time, query_id, debug_info

    except Exception as e:
        running_queries.pop(query_id, None)
        logger.exception("Spark SQL execution error: %s", e)
        ctx = format_sql_error_context(sql, str(e))
        if ctx:
            logger.error(ctx)
        return False, [], [], str(e), time.time() - start_time, query_id, {}
    finally:
        try:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
        except Exception:
            pass


def _parse_trino_roles(raw):
    """Parse TRINO_ROLES into catalog→role map (and optional bare ALL/NONE).

    Accepts:
      - ``hive=admin`` / ``hive:admin``
      - ``hive=admin,system=admin`` (multi catalog)
      - bare ``admin`` / ``ALL`` / ``NONE``
    """
    s = (raw or "").strip()
    if not s:
        return None, None
    if "," not in s and "=" not in s and ":" not in s:
        return None, s
    out = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
        elif ":" in part:
            k, v = part.split(":", 1)
        else:
            out["hive"] = part
            continue
        k, v = k.strip(), v.strip()
        if k and v:
            out[k] = v
    return (out if out else None), None


def _hive_role_priority_order():
    parts = tuple(
        p.strip().lower()
        for p in (TRINO_HIVE_ROLE_PRIORITY or "admin,analytics,public").split(",")
        if p.strip()
    )
    return parts or ("admin", "analytics", "public")


def _pick_best_hive_role(role_names):
    """Pick highest-priority role from applicable_roles ∩ TRINO_HIVE_ROLE_PRIORITY."""
    order = _hive_role_priority_order()
    allow = {x.lower() for x in order}
    picks = []
    seen = set()
    for r in role_names or []:
        n = str(r or "").strip()
        if n.lower().startswith("role{") and n.endswith("}"):
            n = n[5:-1].strip()
        if not n or not re.fullmatch(r"[A-Za-z0-9_]+", n):
            continue
        lk = n.lower()
        if lk not in allow or lk in seen:
            continue
        seen.add(lk)
        picks.append(n)
    if not picks:
        return None

    def _rank(name):
        try:
            return order.index(name.lower())
        except ValueError:
            return len(order) + 1

    return min(picks, key=_rank)


def _format_trino_role_token(role):
    """Build an X-Trino-Role value token: ``ROLE{name}`` (or ALL/NONE literal)."""
    r = (role or "").strip()
    if r.upper() in ("ALL", "NONE"):
        return r.upper()
    return "ROLE{%s}" % r


def _role_header_from_map(catalog_roles):
    """{'hive': 'admin'} → 'hive=ROLE{admin}' (comma-joined for multi catalog)."""
    if not catalog_roles:
        return None
    segs = []
    for cat, role in catalog_roles.items():
        cat = str(cat).strip()
        role = str(role).strip()
        if not cat or not role:
            continue
        segs.append("{}={}".format(cat, _format_trino_role_token(role)))
    return ",".join(segs) if segs else None


def _role_header_from_config():
    """X-Trino-Role from TRINO_ROLES env (e.g. ``hive=admin``)."""
    catalog_roles, bare = _parse_trino_roles(TRINO_ROLES)
    if catalog_roles:
        return _role_header_from_map(catalog_roles)
    if bare:
        return _role_header_from_map({"hive": bare})
    return None


def _discover_hive_role_header(base_conn):
    """Query applicable_roles and pick best hive role → 'hive=ROLE{admin}' (or None)."""
    if not TRINO_SET_HIVE_ROLE_FALLBACK:
        return None
    try:
        cur = base_conn.cursor()
        cur.execute(
            "SELECT role_name FROM hive.information_schema.applicable_roles "
            "WHERE lower(cast(grantee AS varchar)) = lower(cast(current_user AS varchar))"
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.warning("Trino applicable_roles query failed, skip role header: %s", e)
        return None
    names = [
        str(row[0]).strip()
        for row in rows
        if row and row[0] is not None and str(row[0]).strip()
    ]
    picked = _pick_best_hive_role(names)
    if not picked:
        logger.warning(
            "Trino applicable_roles has no overlap with TRINO_HIVE_ROLE_PRIORITY "
            "(sample=%s), skip role header",
            names[:20],
        )
        return None
    return _role_header_from_map({"hive": picked})


def _ensure_trino_host_bypasses_proxy():
    """内网 Trino 经 HTTP_PROXY 常 SSL 握手超时；把 TRINO_HOST 加入 NO_PROXY。"""
    host = (TRINO_HOST or "").strip()
    if not host:
        return
    extras = [host]
    # 若配置的是域名，同时加一份小写，避免大小写不一致
    if host.lower() != host:
        extras.append(host.lower())
    for key in ("NO_PROXY", "no_proxy"):
        cur = (os.environ.get(key) or "").strip()
        parts = [p.strip() for p in cur.split(",") if p.strip()]
        lower_set = {p.lower() for p in parts}
        changed = False
        for h in extras:
            if h.lower() not in lower_set:
                parts.append(h)
                lower_set.add(h.lower())
                changed = True
        if changed:
            os.environ[key] = ",".join(parts)


def _raw_trino_connect(trino_lib, http_headers=None):
    return trino_lib.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="https",
        auth=trino_lib.auth.BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
        verify=False,
        http_headers=http_headers or {},
    )


def _create_trino_connection():
    """Create a Trino DBAPI connection with the hive admin/high role enabled.

    Role is delivered via the ``X-Trino-Role`` request header (e.g.
    ``hive=ROLE{admin}``) rather than ``SET ROLE``: trino-python-client 0.305 does
    not persist a post-connect ``SET ROLE`` into subsequent statements, so the
    session would stay on ``public`` and hit Access Denied.
    """
    import warnings
    import trino as trino_lib
    warnings.filterwarnings('ignore', '.*InsecureRequestWarning*', Warning)
    warnings.filterwarnings('ignore', '.*Unverified HTTPS*', Warning)

    _ensure_trino_host_bypasses_proxy()

    # 1) 显式配置 TRINO_ROLES=hive=admin → 直接作为 X-Trino-Role 头
    role_header = _role_header_from_config()

    # 2) 兜底：查 applicable_roles，按优先级（默认 admin）选一个
    if not role_header:
        try:
            probe = _raw_trino_connect(trino_lib)
            role_header = _discover_hive_role_header(probe)
            try:
                probe.close()
            except Exception:
                pass
        except Exception as e:
            logger.warning("Trino role discovery connection failed: %s", e)

    headers = {}
    if role_header:
        headers["X-Trino-Role"] = role_header
        logger.info("Trino: using X-Trino-Role=%s", role_header)

    return _raw_trino_connect(trino_lib, http_headers=headers)


def execute_trino_sql(sql, max_rows=QUERY_RESULT_MAX_ROWS, timeout=None):
    """Execute SQL via Trino DBAPI connector, return same tuple as Spark"""
    if timeout is None:
        timeout = SQL_EXECUTOR_TIMEOUT
    query_id = str(uuid.uuid4())
    start_time = time.time()

    sql = sql.strip()
    if not sql:
        return False, [], [], "SQL is empty", 0, query_id, {}

    if not _is_allowed_query_type(sql):
        return False, [], [], "Only SELECT/SHOW/DESCRIBE queries allowed", 0, query_id, {}

    original_sql = sql
    sql = fix_trino_sql_syntax(sql)
    if sql != original_sql:
        logger.info("Trino SQL auto-fixed: backtick aliases -> double quotes")

    sql_before_single = sql
    sql = _keep_first_executable_statement(sql)
    if sql != sql_before_single:
        logger.warning("Trino SQL auto-fixed: kept only first executable statement from multi-statement output")

    sql_before_semi = sql
    sql = _strip_trailing_semicolon_trino(sql)
    if sql != sql_before_semi:
        logger.info("Trino SQL auto-fixed: removed trailing semicolon(s) for DBAPI")

    syntax_issues = validate_engine_sql(sql, engine="trino")
    if syntax_issues:
        msg = "Trino 425 pre-check failed: " + "; ".join(syntax_issues)
        logger.warning("%s\nSQL:\n%s", msg, sql)
        return False, [], [], msg, 0, query_id, {}

    conn = None
    cursor = None
    try:
        conn = _create_trino_connection()
        cursor = conn.cursor()
        running_queries[query_id] = {"cursor": cursor, "start_time": start_time, "engine": "trino"}

        if _should_trino_explain(sql):
            preflight_start = time.time()
            pipeline_log(
                logger,
                "agent.execute_sql.preflight.start",
                engine="trino",
                sql_chars=len(sql),
            )
            try:
                cursor.execute("EXPLAIN (TYPE VALIDATE) " + sql)
                cursor.fetchall()
            except Exception as e:
                running_queries.pop(query_id, None)
                err = "Trino EXPLAIN validation failed: {}".format(e)
                pipeline_log(
                    logger,
                    "agent.execute_sql.preflight.done",
                    engine="trino",
                    ok=False,
                    sec=time.time() - preflight_start,
                    err=str(e)[:160],
                )
                ctx = format_sql_error_context(sql, str(e))
                if ctx:
                    logger.error(ctx)
                return (
                    False,
                    [],
                    [],
                    err,
                    time.time() - start_time,
                    query_id,
                    {"error_kind": "trino_preflight"},
                )
            pipeline_log(
                logger,
                "agent.execute_sql.preflight.done",
                engine="trino",
                ok=True,
                sec=time.time() - preflight_start,
            )

        logger.info("Trino query started (ID: %s)", query_id)

        cursor.execute(sql)
        rows = cursor.fetchmany(max_rows + 1) if max_rows > 0 else cursor.fetchall()
        headers = [desc[0] for desc in cursor.description] if cursor.description else []

        running_queries.pop(query_id, None)

        total_rows = len(rows)
        if max_rows > 0 and len(rows) > max_rows:
            rows = rows[:max_rows]

        results = []
        for row in rows:
            row_dict = {}
            for i, h in enumerate(headers):
                val = row[i] if i < len(row) else None
                row_dict[h] = convert_value(str(val) if val is not None else "NULL")
            results.append(row_dict)

        csv_filename = None
        if results and headers:
            csv_filename = "query_result_{}.csv".format(query_id[:8])
            csv_path = os.path.join(TEMP_CSV_DIR, csv_filename)
            try:
                with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for row in results:
                        writer.writerow({h: row.get(h, '') for h in headers})
            except Exception:
                csv_filename = None

        return True, headers, results, None, time.time() - start_time, query_id, {
            "csv_filename": csv_filename,
            "total_rows": total_rows,
            "returned_rows": len(results),
            "engine": "trino",
        }

    except Exception as e:
        running_queries.pop(query_id, None)
        logger.exception("Trino execution error: %s", e)
        ctx = format_sql_error_context(sql, str(e))
        if ctx:
            logger.error(ctx)
        return False, [], [], str(e), time.time() - start_time, query_id, {}
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass


def execute_sql(sql, engine="spark", max_rows=QUERY_RESULT_MAX_ROWS, timeout=None):
    """Unified entry: dispatch to spark or trino"""
    if timeout is None:
        timeout = SQL_EXECUTOR_TIMEOUT
    pipeline_log(
        logger,
        "agent.execute_sql.enter",
        engine=engine,
        sql_chars=len(sql or ""),
        max_rows=max_rows,
        timeout_sec=float(timeout),
    )
    if engine == "trino":
        out = execute_trino_sql(sql, max_rows, timeout)
    else:
        out = execute_spark_sql(sql, max_rows, timeout)
    ok, headers, results, err, exec_time, qid, _dbg = out
    if not ok and err and is_permission_denied_error(err):
        err = format_execution_error_for_user(err, sql=sql, engine=engine)
        _dbg = dict(_dbg or {}, error_kind="permission_denied")
    pipeline_log(
        logger,
        "agent.execute_sql.done",
        ok=ok,
        exec_id=qid or "-",
        cols=len(headers or []),
        rows=len(results or []),
        sec=exec_time,
        err=(err[:120] if err else None),
    )
    return ok, headers, results, err, exec_time, qid, _dbg


def cancel_query(query_id):
    if query_id not in running_queries:
        return False, "Query {} not found".format(query_id)

    info = running_queries[query_id]
    process = info.get("process")
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        running_queries.pop(query_id, None)
        temp_file = info.get("temp_file")
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except Exception:
                pass
        return True, "Query cancelled"
    else:
        running_queries.pop(query_id, None)
        return False, "Query already finished"
