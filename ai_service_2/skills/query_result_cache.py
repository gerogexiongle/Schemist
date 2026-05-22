# -*- coding: utf-8 -*-
"""In-memory cache for SQL execute results (keyed by query_id)."""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional

_CACHE: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

_DEFAULT_TTL = int(os.environ.get("QUERY_RESULT_CACHE_TTL_SEC", "7200"))
_MAX_ENTRIES = int(os.environ.get("QUERY_RESULT_CACHE_MAX", "200"))


def _now() -> float:
    return time.time()


def _evict_expired(now: Optional[float] = None) -> None:
    t = now if now is not None else _now()
    expired = [k for k, v in _CACHE.items() if float(v.get("expires_at") or 0) <= t]
    for k in expired:
        _CACHE.pop(k, None)
    while len(_CACHE) > _MAX_ENTRIES:
        oldest = min(_CACHE.items(), key=lambda x: float(x[1].get("created_at") or 0))[0]
        _CACHE.pop(oldest, None)


def put_query_result(
    query_id: str,
    *,
    headers: List[str],
    result: List[Dict],
    row_count: int = 0,
    execution_time: float = 0.0,
    sql: str = "",
    engine: str = "",
    success: bool = True,
    ttl_sec: Optional[int] = None,
) -> None:
    qid = (query_id or "").strip()
    if not qid or not success:
        return
    ttl = int(ttl_sec if ttl_sec is not None else _DEFAULT_TTL)
    now = _now()
    entry = {
        "query_id": qid,
        "headers": list(headers or []),
        "result": list(result or []),
        "row_count": int(row_count),
        "execution_time": float(execution_time or 0),
        "sql": (sql or "")[:8000],
        "engine": (engine or "")[:32],
        "success": bool(success),
        "created_at": now,
        "expires_at": now + max(ttl, 60),
    }
    with _LOCK:
        _evict_expired(now)
        _CACHE[qid] = entry


def get_query_result(query_id: str) -> Optional[Dict[str, Any]]:
    qid = (query_id or "").strip()
    if not qid:
        return None
    with _LOCK:
        _evict_expired()
        entry = _CACHE.get(qid)
        if not entry:
            return None
        if float(entry.get("expires_at") or 0) <= _now():
            _CACHE.pop(qid, None)
            return None
        return dict(entry)
