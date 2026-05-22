# -*- coding: utf-8 -*-
"""Web UI session snapshots for refresh-safe restore."""

from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Any, Dict, Optional

_STORE: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

_DEFAULT_TTL = int(os.environ.get("WEB_SESSION_TTL_SEC", "86400"))
_MAX_REPORT_HTML = int(os.environ.get("WEB_SESSION_MAX_REPORT_CHARS", "350000"))
_MAX_ENTRIES = int(os.environ.get("WEB_SESSION_MAX", "500"))


def _now() -> float:
    return time.time()


def new_session_id() -> str:
    return "ws_" + uuid.uuid4().hex[:16]


def _evict(now: Optional[float] = None) -> None:
    t = now if now is not None else _now()
    for k in [k for k, v in _STORE.items() if float(v.get("expires_at") or 0) <= t]:
        _STORE.pop(k, None)
    while len(_STORE) > _MAX_ENTRIES:
        oldest = min(_STORE.items(), key=lambda x: float(x[1].get("updated_at") or 0))[0]
        _STORE.pop(oldest, None)


def save_web_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    sid = (payload.get("session_id") or "").strip() or new_session_id()
    now = _now()
    ttl = int(payload.get("ttl_sec") or _DEFAULT_TTL)
    report_html = payload.get("report_html") or payload.get("reportHtml") or ""
    if isinstance(report_html, str) and len(report_html) > _MAX_REPORT_HTML:
        report_html = report_html[:_MAX_REPORT_HTML]

    entry = {
        "session_id": sid,
        "updated_at": now,
        "expires_at": now + max(ttl, 300),
        "step": int(payload.get("step") or 0),
        "status": str(payload.get("status") or "idle")[:32],
        "tid": str(payload.get("tid") or "")[:80],
        "question": str(payload.get("question") or "")[:8000],
        "sql": str(payload.get("sql") or "")[:50000],
        "headers": list(payload.get("headers") or []),
        "data": list(payload.get("data") or []),
        "report_html": report_html,
        "query_id": str(payload.get("query_id") or "")[:64],
        "share_id": str(payload.get("share_id") or "")[:32],
        "engine": str(payload.get("engine") or "")[:32],
        "smart_mode": bool(payload.get("smart_mode")),
        "chart_type": str(payload.get("chart_type") or "")[:32],
        "generate_meta": payload.get("generate_meta") if isinstance(payload.get("generate_meta"), dict) else {},
        "row_count": int(payload.get("row_count") or 0),
        "execution_time": float(payload.get("execution_time") or 0),
    }
    with _LOCK:
        _evict(now)
        _STORE[sid] = entry
    return {
        "session_id": sid,
        "updated_at": now,
        "expires_at": entry["expires_at"],
    }


def get_web_session(session_id: str) -> Optional[Dict[str, Any]]:
    sid = (session_id or "").strip()
    if not sid:
        return None
    with _LOCK:
        _evict()
        entry = _STORE.get(sid)
        if not entry:
            return None
        if float(entry.get("expires_at") or 0) <= _now():
            _STORE.pop(sid, None)
            return None
        return dict(entry)
