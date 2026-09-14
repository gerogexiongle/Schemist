# -*- coding: utf-8 -*-
"""SQL 执行错误分类与用户可读提示（权限、不可重试等）。"""
import os
import re
from typing import List, Optional

from config.settings import TRINO_USER

PERMISSION_DENIED_PATTERNS = [
    r"\bpermission denied\b",
    r"\baccess denied\b",
    r"\bPERMISSION_DENIED\b",
    r"Cannot select from table",
    r"Cannot insert into table",
    r"Cannot delete from table",
    r"Cannot update table",
    r"Cannot create table",
    r"Cannot drop table",
    r"not authorized",
    r"authorization failed",
    r"无权限",
    r"没有权限",
]

_TABLE_FROM_ERR_PATTERNS = [
    r"Cannot (?:select from|insert into|delete from|update) table ([A-Za-z0-9_.]+)",
    r"(?:from|join|into|update)\s+table\s+([A-Za-z0-9_.]+)",
    r"table\s+([A-Za-z0-9_.]+)",
]


def is_permission_denied_error(error_message: str) -> bool:
    if not error_message or not str(error_message).strip():
        return False
    text = str(error_message)
    for pat in PERMISSION_DENIED_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


def extract_denied_tables(error_message: str, sql: Optional[str] = None) -> List[str]:
    """从 Trino/Spark 报错或 SQL 中提取可能缺权限的表名。"""
    found: List[str] = []
    text = (error_message or "") + "\n" + (sql or "")
    for pat in _TABLE_FROM_ERR_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            t = (m.group(1) or "").strip().rstrip(",;")
            if t and t not in found and "." in t:
                found.append(t)
    if not found:
        for m in re.finditer(r"\b([a-z][a-z0-9_]*\.[a-z][a-z0-9_.]+)\b", error_message or "", re.IGNORECASE):
            t = m.group(1)
            if t not in found:
                found.append(t)
    return found


def get_execution_account(engine: str = "trino") -> str:
    """返回当前服务用于执行 SQL 的账号名（供权限申请提示）。"""
    if (engine or "").lower() == "trino":
        return (TRINO_USER or "schemist").strip() or "schemist"
    return (
        os.environ.get("HADOOP_USER_NAME")
        or os.environ.get("SPARK_USER")
        or os.environ.get("USER")
        or "当前 Spark 执行用户"
    ).strip()


def format_permission_denied_message(
    error_message: str,
    sql: Optional[str] = None,
    engine: str = "trino",
) -> str:
    engine_label = "Trino" if (engine or "").lower() == "trino" else "Spark"
    exec_account = get_execution_account(engine)
    tables = extract_denied_tables(error_message, sql)
    lines = [
        "【权限不足 — 已停止执行】",
        "当前 {} 查询账号「{}」无权访问所需数据表，流水线不会继续自动修复或重试。".format(
            engine_label, exec_account
        ),
        "请向数仓/平台管理员为执行账号「{}」申请对应表的 SELECT（查询）权限，审批通过后重新提问。".format(
            exec_account
        ),
    ]
    if tables:
        lines.append("")
        lines.append("涉及表（请优先申请以下权限）：")
        for t in tables[:20]:
            lines.append("  · {}".format(t))
        if len(tables) > 20:
            lines.append("  · … 共 {} 张表".format(len(tables)))
    lines.append("")
    lines.append("原始错误：")
    lines.append((error_message or "Permission denied")[:2000])
    return "\n".join(lines)


def format_execution_error_for_user(
    error_message: str,
    sql: Optional[str] = None,
    engine: str = "trino",
) -> str:
    if is_permission_denied_error(error_message):
        return format_permission_denied_message(error_message, sql=sql, engine=engine)
    return (error_message or "").strip()
