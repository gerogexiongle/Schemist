#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL validator skill: pre-check generated SQL for common errors (aggregation, CTE, etc.)
Ported from the original ai_service sql_generator_api.py validation logic.
"""
import logging
import re

logger = logging.getLogger("sql_validator")

AGG_FUNCS = ['CORR(', 'AVG(', 'SUM(', 'COUNT(', 'MIN(', 'MAX(',
             'STDDEV(', 'VARIANCE(', 'COVAR_POP(', 'COVAR_SAMP(']
GROUP_BY_CLAUSE_RE = (
    r"\bGROUP\s+BY\s+([\s\S]*?)"
    r"(?=\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|\)\s*,|\)\s*SELECT|$)"
)


def strip_leading_sql_comments(sql):
    """Return SQL without leading -- or /* */ comments; preserves inner comments."""
    if not sql:
        return sql
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl == -1:
                return ""
            s = s[nl + 1:].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/", 2)
            if end == -1:
                return s
            s = s[end + 2:].lstrip()
            continue
        return s


def _strip_sql_comments(sql):
    """Remove comments for lightweight syntax checks."""
    if not sql:
        return sql
    no_block = re.sub(r"/\*[\s\S]*?\*/", " ", sql)
    return re.sub(r"--[^\n\r]*", " ", no_block)


def _has_internal_statement_separator(sql):
    """True when there is a semicolon before more SQL tokens (DBAPI executes one statement)."""
    if not sql:
        return False
    s = _strip_sql_comments(sql)
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
            if s[i + 1:].strip():
                return True
    return False


def _line_context(sql, line_no, radius=5):
    lines = sql.splitlines()
    if line_no < 1 or line_no > len(lines):
        return ""
    start = max(1, line_no - radius)
    end = min(len(lines), line_no + radius)
    return "\n".join(
        "{:>4}: {}".format(i, lines[i - 1])
        for i in range(start, end + 1)
    )


def format_sql_error_context(sql, error_message, radius=6):
    """Extract Trino/Spark line number from an error and print nearby SQL lines."""
    if not sql or not error_message:
        return ""
    m = re.search(r"\bline\s+(\d+):(\d+)", str(error_message), re.IGNORECASE)
    if not m:
        return ""
    line_no = int(m.group(1))
    col_no = int(m.group(2))
    ctx = _line_context(sql, line_no, radius=radius)
    if not ctx:
        return ""
    return "SQL error near line {}, column {}:\n{}".format(line_no, col_no, ctx)


def _extract_outer_select(sql):
    sql_stripped = strip_leading_sql_comments(sql).strip()
    upper = sql_stripped.upper()
    outer_start = 0
    if upper.startswith('WITH'):
        depth = 0
        i = 0
        found_outer = False
        while i < len(sql_stripped):
            if sql_stripped[i] == '(':
                depth += 1
            elif sql_stripped[i] == ')':
                depth -= 1
            elif depth == 0 and upper[i:i+6] == 'SELECT' and i > 0:
                outer_start = i
                found_outer = True
                break
            i += 1
        if not found_outer:
            outer_start = 0

    outer_sql = sql_stripped[outer_start:]
    match = re.search(r'SELECT\s+(.*?)\s+FROM\s', outer_sql, re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else ""


def _split_select_columns(select_clause):
    cols = []
    current = ""
    depth = 0
    for ch in select_clause:
        if ch == '(':
            depth += 1
            current += ch
        elif ch == ')':
            depth -= 1
            current += ch
        elif ch == ',' and depth == 0:
            cols.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        cols.append(current.strip())
    return cols


def _is_agg_column(col):
    col_upper = col.upper().strip()
    if any(col_upper.startswith(f) or re.search(r'\b' + re.escape(f), col_upper) for f in AGG_FUNCS):
        return True
    if col_upper.startswith('ROUND(') and any(f in col_upper for f in AGG_FUNCS):
        return True
    return False


def validate_generated_sql(sql):
    warnings = []
    sql_check = _strip_sql_comments(sql)
    sql_upper = sql_check.upper()
    has_agg = any(func in sql_upper for func in AGG_FUNCS)
    if has_agg:
        select_clause = _extract_outer_select(sql_check)
        if select_clause:
            has_group_by = 'GROUP BY' in sql_upper
            cols = _split_select_columns(select_clause)
            non_agg_cols = [c for c in cols if not _is_agg_column(c) and c.upper().strip() != '*']
            agg_cols = [c for c in cols if _is_agg_column(c)]

            if agg_cols and non_agg_cols and not has_group_by:
                warnings.append(
                    "SELECT mixed aggregation with {} non-agg columns but missing GROUP BY".format(len(non_agg_cols))
                )
            if has_group_by and agg_cols:
                corr_funcs = [c for c in agg_cols if 'CORR(' in c.upper()]
                if corr_funcs and non_agg_cols:
                    warnings.append("CORR() with GROUP BY on {} fields may result in NULL".format(len(non_agg_cols)))

    # Common hallucination: GROUP BY 1,2,... accidentally includes aggregate columns.
    for m in re.finditer(GROUP_BY_CLAUSE_RE, sql_check, re.IGNORECASE):
        group_expr = m.group(1)
        if any(func in group_expr.upper() for func in AGG_FUNCS):
            warnings.append("GROUP BY contains aggregate function; remove COUNT/SUM/etc. from GROUP BY")

    return warnings


def validate_engine_sql(sql, engine="spark"):
    """Engine-specific lightweight syntax checks before execution."""
    issues = []
    if not sql:
        return ["SQL is empty"]

    engine = (engine or "spark").lower()
    cleaned = strip_leading_sql_comments(sql)
    no_comments = _strip_sql_comments(cleaned)
    lower = no_comments.lower()

    if _has_internal_statement_separator(cleaned):
        issues.append(
            "Multiple SQL statements detected; only one executable statement is supported. "
            "Rewrite as one SELECT/WITH query without internal semicolons."
        )

    select_clauses = []
    outer_select = _extract_outer_select(no_comments)
    if outer_select:
        select_clauses.append(outer_select)
    # Also scan CTE/subquery SELECT lists. This catches hallucinated "..., LEFT JOIN ..."
    # before the FROM of that SELECT, which Trino reports as "mismatched input 'LEFT'".
    select_clauses.extend(
        m.group(1)
        for m in re.finditer(r"\bSELECT\b([\s\S]{0,6000}?)\bFROM\b", no_comments, re.IGNORECASE)
    )
    if any(re.search(r"\b(left|right|inner|full|cross)\s+join\b", c, re.IGNORECASE) for c in select_clauses):
        issues.append(
            "JOIN keyword appears inside SELECT list before FROM; likely missing FROM/parenthesis/comma before JOIN"
        )

    for m in re.finditer(GROUP_BY_CLAUSE_RE, no_comments, re.IGNORECASE):
        if re.search(r"\b(count|sum|avg|min|max|corr|stddev|variance)\s*\(", m.group(1), re.IGNORECASE):
            issues.append("GROUP BY contains aggregate expression")
            break

    if engine == "trino":
        if re.search(r"\blateral\s+view\b|\bexplode\s*\(", no_comments, re.IGNORECASE):
            issues.append("Trino 425 does not support Spark LATERAL VIEW/EXPLODE; use CROSS JOIN UNNEST(...)")
        if re.search(r"\b(size|array_length)\s*\(", no_comments, re.IGNORECASE):
            issues.append("Trino 425 array length should use CARDINALITY(array_expr), not SIZE/ARRAY_LENGTH")
        if re.search(r"\bdate_format\s*\([^,]+,\s*'yyyy", no_comments, re.IGNORECASE):
            issues.append("Trino date_format pattern should use MySQL style like '%Y-%m-%d', not 'yyyy-MM-dd'")
        if "`" in no_comments:
            issues.append("Trino uses double quotes for identifiers/aliases, not backticks")
    else:
        if re.search(r"\bcross\s+join\s+unnest\b", no_comments, re.IGNORECASE):
            issues.append("Spark SQL 3.3.1 should use LATERAL VIEW explode(...) instead of CROSS JOIN UNNEST")
        if re.search(r"\bdate_format\s*\([^,]+,\s*'%Y", no_comments, re.IGNORECASE):
            issues.append("Spark date_format pattern should use Java style like 'yyyy-MM-dd', not '%Y-%m-%d'")
        # Avoid warning about string literals; only clear double-quoted aliases are invalid in Spark.
        if re.search(r"\bAS\s+\"[^\"]+\"", no_comments, re.IGNORECASE):
            issues.append("Spark SQL aliases should use backticks, not ANSI double quotes")
        if "||" in no_comments:
            issues.append("Spark SQL 3.3.1 should use CONCAT(...) for string concatenation, not ||")

    if lower.count("(") != lower.count(")"):
        issues.append("Unbalanced parentheses")

    return issues


def auto_fix_agg_sql(sql):
    sql_upper = sql.upper()
    has_agg = any(func in sql_upper for func in AGG_FUNCS)
    if not has_agg:
        return None

    select_clause = _extract_outer_select(sql)
    if not select_clause:
        return None

    has_group_by = 'GROUP BY' in sql_upper
    cols = _split_select_columns(select_clause)
    non_agg_cols = [c for c in cols if not _is_agg_column(c) and c.upper().strip() != '*']
    agg_cols = [c for c in cols if _is_agg_column(c)]

    need_fix = False
    if agg_cols and non_agg_cols and not has_group_by:
        need_fix = True
    if has_group_by and agg_cols:
        corr_funcs = [c for c in agg_cols if 'CORR(' in c.upper()]
        if corr_funcs and non_agg_cols:
            need_fix = True

    if not need_fix:
        return None

    new_cols = agg_cols + ["COUNT(*) AS sample_count"]
    new_select = ",\n    ".join(new_cols)

    fixed_sql = sql
    fixed_sql = re.sub(r'\s+GROUP\s+BY\s+.*?(?=ORDER\s+BY|LIMIT|HAVING|$)',
                       ' ', fixed_sql, flags=re.IGNORECASE | re.DOTALL)

    outer_start_idx = sql.upper().find('SELECT', sql.upper().find(select_clause.upper()[:20]) - 10)
    if outer_start_idx < 0:
        outer_start_idx = 0

    from_idx = sql.upper().find('\nFROM', outer_start_idx)
    if from_idx < 0:
        from_idx = sql.upper().find(' FROM', outer_start_idx)
    if from_idx < 0:
        return None

    select_keyword_end = sql.upper().find('SELECT', outer_start_idx) + len('SELECT')
    fixed_sql = sql[:select_keyword_end] + " \n    " + new_select + "\n" + sql[from_idx:]

    fixed_sql = re.sub(
        r'\s+GROUP\s+BY\s+[\s\S]*?(?=\s+ORDER\s+BY|\s+LIMIT|\s+HAVING|\s*;?\s*$)',
        ' ', fixed_sql, flags=re.IGNORECASE
    )

    logger.info("Auto-fixed SQL: removed %d non-agg cols, kept %d agg cols", len(non_agg_cols), len(agg_cols))
    return fixed_sql.strip()


# ==================== CTE fix logic ====================

def _find_keyword_at_depth0(text, keyword):
    upper = text.upper()
    kw_upper = keyword.upper()
    kw_len = len(keyword)
    depth = 0
    def _is_word_char(ch):
        return ch.isalnum() or ch == '_'
    for i in range(len(text)):
        if text[i] == '(':
            depth += 1
        elif text[i] == ')':
            depth -= 1
        elif depth == 0 and upper[i:i+kw_len] == kw_upper:
            if (i == 0 or not _is_word_char(text[i-1])) and \
               (i + kw_len >= len(text) or not _is_word_char(text[i+kw_len])):
                return i
    return -1


def _parse_cte_definitions(sql):
    text = sql.strip()
    upper = text.upper()
    if not upper.startswith('WITH'):
        return [], 0

    cte_list = []
    i = 4

    while i < len(text):
        while i < len(text) and text[i] in ' \t\n\r':
            i += 1
        if i >= len(text):
            break

        j = i
        while j < len(text) and (text[j].isalnum() or text[j] == '_'):
            j += 1
        cte_name = text[i:j]
        i = j
        if not cte_name:
            break

        while i < len(text) and text[i] in ' \t\n\r':
            i += 1
        if text[i:i+2].upper() != 'AS':
            break
        i += 2
        while i < len(text) and text[i] in ' \t\n\r':
            i += 1
        if i >= len(text) or text[i] != '(':
            break

        depth = 1
        body_start = i + 1
        i += 1
        while i < len(text) and depth > 0:
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
            i += 1
        body_end = i - 1
        body = text[body_start:body_end]
        cte_list.append((cte_name, body, body_start, body_end))

        while i < len(text) and text[i] in ' \t\n\r':
            i += 1
        if i < len(text) and text[i] == ',':
            i += 1
        else:
            break

    return cte_list, i


def _extract_cte_select_columns(cte_body):
    body_upper = cte_body.upper().strip()
    if not body_upper.startswith('SELECT'):
        return set()

    from_pos = _find_keyword_at_depth0(cte_body, 'FROM')
    if from_pos < 0:
        return set()

    select_start = cte_body.upper().index('SELECT') + 6
    select_clause = cte_body[select_start:from_pos].strip()
    cols = _split_select_columns(select_clause)

    col_names = set()
    for col in cols:
        col = col.strip()
        if col == '*':
            return None

        alias_m = re.search(r'\bAS\s+(\w+)\s*$', col, re.IGNORECASE)
        if alias_m:
            col_names.add(alias_m.group(1).lower())
        else:
            parts = re.findall(r'\b(\w+)\b', col)
            if parts:
                col_names.add(parts[-1].lower())
    return col_names


def fix_cte_missing_columns(sql):
    text = sql.strip()
    upper = text.upper()
    if not upper.startswith('WITH'):
        return None

    cte_list, main_start = _parse_cte_definitions(text)
    if not cte_list:
        return None

    main_query = text[main_start:]

    cte_info = {}
    for name, body, bs, be in cte_list:
        col_names = _extract_cte_select_columns(body)
        if col_names is None:
            continue
        from_pos = _find_keyword_at_depth0(body, 'FROM')
        if from_pos < 0:
            continue
        cte_info[name.lower()] = {
            "col_names": col_names,
            "body_start": bs,
            "from_pos_in_body": from_pos,
        }

    alias_to_cte = {}
    cte_names_lower = {name.lower() for name, _, _, _ in cte_list}
    alias_pattern = re.compile(
        r'\b(' + '|'.join(re.escape(n) for n, _, _, _ in cte_list) + r')\s+(\w+)\b',
        re.IGNORECASE
    )
    for am in alias_pattern.finditer(main_query):
        cte_name_match = am.group(1).lower()
        alias_match = am.group(2).lower()
        skip_words = {'on', 'inner', 'left', 'right', 'full', 'cross', 'join',
                      'where', 'group', 'order', 'having', 'limit', 'union',
                      'as', 'and', 'or', 'not', 'in', 'between', 'select'}
        if alias_match not in skip_words:
            alias_to_cte[alias_match] = cte_name_match
    for cte_name_lower in cte_names_lower:
        alias_to_cte[cte_name_lower] = cte_name_lower

    ref_pattern = re.compile(r'\b(\w+)\.(\w+)\b')
    missing_by_cte = {}
    for m in ref_pattern.finditer(main_query):
        alias = m.group(1).lower()
        col = m.group(2).lower()
        resolved_cte = alias_to_cte.get(alias)
        if resolved_cte and resolved_cte in cte_info and col not in cte_info[resolved_cte]["col_names"]:
            missing_by_cte.setdefault(resolved_cte, set()).add(col)

    if not missing_by_cte:
        return None

    fixes = []
    for cte_name_lower, missing_cols in missing_by_cte.items():
        info = cte_info[cte_name_lower]
        insert_pos = info["body_start"] + info["from_pos_in_body"]
        cols_str = ",\n    ".join(sorted(missing_cols))
        fixes.append((insert_pos, cols_str, cte_name_lower, missing_cols))
        logger.info("CTE '%s' missing cols: %s", cte_name_lower, missing_cols)

    fixes.sort(key=lambda x: x[0], reverse=True)
    fixed = text
    for insert_pos, cols_str, _, _ in fixes:
        fixed = fixed[:insert_pos] + ",\n    " + cols_str + "\n  " + fixed[insert_pos:]

    return fixed


def fix_spark_sql_syntax(sql):
    """Fix common syntax issues that break Spark SQL 3.3.1:
    1. Double-quoted aliases -> backtick-quoted (Spark uses backticks, not ANSI double quotes)
    2. String concatenation || -> CONCAT() (Spark doesn't support || operator)
    """
    if not sql:
        return sql

    fixed = sql
    changed = False

    # 1. Replace double-quoted identifiers with backtick-quoted
    # Match: AS "some alias" or AS "中文别名"
    dq_pattern = re.compile(r'''(\bAS\s+)"([^"]+)"''', re.IGNORECASE)
    if dq_pattern.search(fixed):
        fixed = dq_pattern.sub(r'\1`\2`', fixed)
        changed = True
        logger.info("Spark fix: replaced double-quoted aliases with backticks")

    # Also fix standalone double-quoted identifiers in SELECT (not after AS)
    # e.g., SELECT "col_name" FROM ...
    standalone_dq = re.compile(r'(?<!\w)"([a-zA-Z_\u4e00-\u9fff][\w\u4e00-\u9fff\-]*)"(?!\w)')
    if standalone_dq.search(fixed):
        fixed = standalone_dq.sub(r'`\1`', fixed)
        changed = True

    # 2. Replace string concatenation operator || with CONCAT()
    if '||' in fixed:
        new_fixed = _replace_concat_in_sql(fixed)
        if new_fixed != fixed:
            fixed = new_fixed
            changed = True
            logger.info("Spark fix: replaced || with CONCAT()")

    return fixed if changed else sql


def fix_trino_sql_syntax(sql):
    """Fix common syntax issues that break Trino 425:
    1. Backtick-quoted aliases -> double-quoted (Trino uses ANSI double quotes, not backticks)
    2. CONCAT() with non-varchar args -> cast to VARCHAR or convert to || with CAST
    3. Spark-ish function names (ARRAY_LENGTH / SIZE on array) -> CARDINALITY
    4. Spark LATERAL VIEW EXPLODE(split(...)) -> CROSS JOIN UNNEST(SPLIT(...))
    """
    if not sql:
        return sql

    fixed = sql
    changed = False

    # 1. Replace backtick-quoted identifiers with double-quoted
    bt_alias = re.compile(r'(\bAS\s+)`([^`]+)`', re.IGNORECASE)
    if bt_alias.search(fixed):
        fixed = bt_alias.sub(r'\1"\2"', fixed)
        changed = True
        logger.info("Trino fix: replaced backtick aliases with double quotes")

    standalone_bt = re.compile(r'(?<!\w)`([^`]+)`(?!\w)')
    if standalone_bt.search(fixed):
        fixed = standalone_bt.sub(r'"\1"', fixed)
        changed = True

    # 2. Fix CONCAT() with mixed types -> use || with CAST for non-string args
    if 'CONCAT(' in fixed.upper():
        new_fixed = _fix_trino_concat(fixed)
        if new_fixed != fixed:
            fixed = new_fixed
            changed = True
            logger.info("Trino fix: converted CONCAT() to || with CAST")

    # 3. ARRAY_LENGTH(arr) -> CARDINALITY(arr) (Trino < 415 doesn't have array_length)
    array_length_re = re.compile(r'\bARRAY_LENGTH\s*\(', re.IGNORECASE)
    if array_length_re.search(fixed):
        fixed = array_length_re.sub('CARDINALITY(', fixed)
        changed = True
        logger.info("Trino fix: ARRAY_LENGTH( -> CARDINALITY(")

    # 3b. Spark SIZE(array_expr) -> CARDINALITY(array_expr) — 只改作用在数组上的
    #     命中 SIZE(SPLIT(...)) 和 SIZE(ARRAY[...])；对 SIZE 当列名/别名的场景避让
    size_on_array_re = re.compile(
        r'\bSIZE\s*\(\s*('
        r'(?:SPLIT|SPLIT_TO_MAP|ARRAY|ELEMENT_AT|SLICE|TRANSFORM|FILTER|FLATTEN|SEQUENCE)'
        r'\s*\()',
        re.IGNORECASE,
    )
    if size_on_array_re.search(fixed):
        fixed = size_on_array_re.sub(lambda m: 'CARDINALITY(' + m.group(1), fixed)
        changed = True
        logger.info("Trino fix: SIZE(<array-fn>) -> CARDINALITY(<array-fn>)")

    # 4. Spark 展开数组写法 `LATERAL VIEW EXPLODE(x) tbl AS col`
    #    -> Trino 等价 `CROSS JOIN UNNEST(x) AS tbl(col)`
    lat_view_re = re.compile(
        r'\bLATERAL\s+VIEW\s+(?:OUTER\s+)?EXPLODE\s*\((.+?)\)\s+(\w+)\s+AS\s+(\w+)',
        re.IGNORECASE | re.DOTALL,
    )
    if lat_view_re.search(fixed):
        fixed = lat_view_re.sub(r'CROSS JOIN UNNEST(\1) AS \2(\3)', fixed)
        changed = True
        logger.info("Trino fix: LATERAL VIEW EXPLODE(...) -> CROSS JOIN UNNEST(...)")

    # 4b. 裸 EXPLODE(x) 出现在 SELECT 列表里（无 LATERAL VIEW 包裹）——Trino 没有
    #     EXPLODE 函数，提示用 UNNEST；此处不自动改结构，只在日志里警示
    if re.search(r'\bEXPLODE\s*\(', fixed, re.IGNORECASE) and 'UNNEST(' not in fixed.upper():
        logger.warning(
            "Trino fix: 检测到 EXPLODE(...) 残留，Trino 无此函数，请改写为 CROSS JOIN UNNEST(...)"
        )

    return fixed if changed else sql


def _is_string_literal(expr):
    """Check if an expression is a string literal (starts and ends with ')."""
    s = expr.strip()
    return len(s) >= 2 and s[0] == "'" and s[-1] == "'"


def _fix_trino_concat(sql):
    """Convert CONCAT(expr1, expr2, ...) to CAST-safe || chains for Trino.
    Trino CONCAT only accepts varchar; numeric expressions need CAST."""
    result = []
    i = 0
    upper = sql.upper()

    while i < len(sql):
        # Find CONCAT( pattern
        pos = upper.find('CONCAT(', i)
        if pos == -1:
            result.append(sql[i:])
            break

        # Make sure it's not part of another identifier (e.g. MY_CONCAT)
        if pos > 0 and (sql[pos-1].isalnum() or sql[pos-1] == '_'):
            result.append(sql[i:pos+7])
            i = pos + 7
            continue

        result.append(sql[i:pos])

        # Find the matching closing paren
        paren_start = pos + 6  # index of '('
        depth = 1
        j = paren_start + 1
        in_sq = False
        while j < len(sql) and depth > 0:
            ch = sql[j]
            if ch == "'" and not in_sq:
                in_sq = True
            elif ch == "'" and in_sq:
                in_sq = False
            elif not in_sq:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
            j += 1

        if depth != 0:
            result.append(sql[pos:j])
            i = j
            continue

        inner = sql[paren_start+1:j-1]  # content inside CONCAT(...)

        # Split arguments respecting nested parens and quotes
        args = _split_concat_args(inner)

        if len(args) >= 2:
            parts = []
            for arg in args:
                a = arg.strip()
                if _is_string_literal(a):
                    parts.append(a)
                else:
                    parts.append('CAST(' + a + ' AS VARCHAR)')
            result.append(' || '.join(parts))
        else:
            result.append(sql[pos:j])

        i = j

    return ''.join(result)


def _split_concat_args(inner):
    """Split CONCAT arguments by comma, respecting nested parens and quotes."""
    args = []
    depth = 0
    in_sq = False
    current = []
    for ch in inner:
        if ch == "'" :
            in_sq = not in_sq
            current.append(ch)
        elif not in_sq:
            if ch == '(':
                depth += 1
                current.append(ch)
            elif ch == ')':
                depth -= 1
                current.append(ch)
            elif ch == ',' and depth == 0:
                args.append(''.join(current))
                current = []
            else:
                current.append(ch)
        else:
            current.append(ch)
    if current:
        args.append(''.join(current))
    return args


def _replace_concat_in_sql(sql):
    """Replace all || concatenation with CONCAT(), handling nested function calls."""
    # Tokenize to find || positions not inside quotes
    pipe_positions = []
    in_sq = False
    in_bt = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_bt:
            in_sq = not in_sq
        elif ch == '`' and not in_sq:
            in_bt = not in_bt
        elif ch == '|' and i + 1 < len(sql) and sql[i+1] == '|' and not in_sq and not in_bt:
            pipe_positions.append(i)
            i += 2
            continue
        i += 1

    if not pipe_positions:
        return sql

    def skip_ws_left(text, pos):
        while pos >= 0 and text[pos] in ' \t\n\r':
            pos -= 1
        return pos

    def skip_ws_right(text, pos):
        while pos < len(text) and text[pos] in ' \t\n\r':
            pos += 1
        return pos

    def find_left_operand(text, pos):
        """Find start of left operand ending at pos (exclusive)."""
        p = skip_ws_left(text, pos - 1)
        if p < 0:
            return 0

        if text[p] == "'":
            j = p - 1
            while j >= 0 and text[j] != "'":
                j -= 1
            return max(j, 0)

        if text[p] == ')':
            depth = 1
            j = p - 1
            while j >= 0 and depth > 0:
                if text[j] == ')':
                    depth += 1
                elif text[j] == '(':
                    depth -= 1
                elif text[j] == "'" :
                    j -= 1
                    while j >= 0 and text[j] != "'":
                        j -= 1
                j -= 1
            j += 1
            while j > 0 and (text[j-1].isalnum() or text[j-1] in '_`.'):
                j -= 1
            return j

        j = p
        while j > 0 and (text[j-1].isalnum() or text[j-1] in '_`.'):
            j -= 1
        return j

    def find_right_operand(text, pos):
        """Find end of right operand starting at pos (exclusive, after ||)."""
        p = skip_ws_right(text, pos)
        if p >= len(text):
            return len(text)

        if text[p] == "'":
            j = p + 1
            while j < len(text) and text[j] != "'":
                j += 1
            return min(j + 1, len(text))

        j = p
        while j < len(text) and (text[j].isalnum() or text[j] in '_`.'):
            j += 1
        if j < len(text) and text[j] == '(':
            depth = 1
            j += 1
            while j < len(text) and depth > 0:
                if text[j] == '(':
                    depth += 1
                elif text[j] == ')':
                    depth -= 1
                elif text[j] == "'":
                    j += 1
                    while j < len(text) and text[j] != "'":
                        j += 1
                j += 1
            return j
        return j

    # Group chained || operators (e.g. A || B || C)
    groups = []
    used = set()
    for idx, pp in enumerate(pipe_positions):
        if pp in used:
            continue
        chain = [pp]
        used.add(pp)
        for pp2 in pipe_positions[idx+1:]:
            right_end = find_right_operand(sql, chain[-1] + 2)
            if pp2 >= right_end - 1 and pp2 <= right_end + 10:
                chain.append(pp2)
                used.add(pp2)
            else:
                break
        groups.append(chain)

    # Process from right to left to preserve positions
    result = sql
    for chain in reversed(groups):
        left_start = find_left_operand(result, chain[0])
        right_end = find_right_operand(result, chain[-1] + 2)

        expr = result[left_start:right_end]
        parts = []
        current = []
        in_sq2 = False
        j = 0
        while j < len(expr):
            c = expr[j]
            if c == "'":
                in_sq2 = not in_sq2
                current.append(c)
            elif c == '|' and j + 1 < len(expr) and expr[j+1] == '|' and not in_sq2:
                parts.append(''.join(current).strip())
                current = []
                j += 2
                continue
            else:
                current.append(c)
            j += 1
        parts.append(''.join(current).strip())

        if len(parts) >= 2:
            replacement = 'CONCAT(' + ', '.join(parts) + ')'
            result = result[:left_start] + replacement + result[right_end:]

    return result
