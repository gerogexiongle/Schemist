# -*- coding: utf-8 -*-
"""
流水线调试日志：贯穿「HTTP / 飞书 → 生成 SQL → 执行 → 分析 → 回复 / 分享」。
环境变量 PIPELINE_TRACE_LOG=0 可关闭（默认开启）。
日志格式统一前缀：[PIPELINE] tid=... phase=...

log(..., tid=...) 可显式指定 tid（例如飞书在后台任务 set_trace_id 之前先在 webhook 打点）。
"""
from __future__ import annotations

import contextvars
import logging
import os
import threading
import time
import uuid
from collections import deque
from typing import Any, Callable, Dict, List, Optional

_TRACE_ID_CTX: contextvars.ContextVar[str] = contextvars.ContextVar("pipeline_trace_id", default="")

_events_lock = threading.Lock()
_events_store: Dict[str, deque] = {}
_events_last_seen: Dict[str, float] = {}
_events_seq: Dict[str, int] = {}
_MAX_EVENTS_PER_TID = 500
_MAX_TIDS = 200
_TTL_SEC = float((os.environ.get("PIPELINE_TRACE_UI_TTL_SEC", "3600") or "3600").strip() or "3600")

_recent_feishu_lock = threading.Lock()
_RECENT_FEISHU_SESSIONS: deque = deque(maxlen=40)


def events_enabled() -> bool:
    raw = (os.environ.get("PIPELINE_TRACE_UI", "1") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def enabled() -> bool:
    raw = (os.environ.get("PIPELINE_TRACE_LOG", "1") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def get_tid() -> str:
    tid = (_TRACE_ID_CTX.get() or "").strip()
    return tid if tid else "-"


def set_trace_id(tid: str) -> contextvars.Token:
    return _TRACE_ID_CTX.set((tid or "").strip()[:64] or "-")


def reset_trace_id(token: contextvars.Token) -> None:
    try:
        _TRACE_ID_CTX.reset(token)
    except Exception:
        pass


def _short(val: Any, maxlen: int = 200) -> str:
    if val is None:
        return ""
    s = str(val).replace("\n", " ").replace("\r", " ").strip()
    if len(s) > maxlen:
        return s[:maxlen] + "…"
    return s


def new_http_trace_id(header_value: Optional[str]) -> str:
    h = (header_value or "").strip()
    return h[:64] if h else "http_{}".format(uuid.uuid4().hex[:12])


def new_feishu_trace_id(message_id: str) -> str:
    mid = (message_id or "").strip().replace("/", "_")[:28]
    if mid:
        return "fs_{}_{}".format(mid, uuid.uuid4().hex[:6])
    return "fs_{}".format(uuid.uuid4().hex[:12])


def _jsonable_payload(kwargs: dict) -> dict:
    out = {}
    for k, v in sorted(kwargs.items()):
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int):
            out[k] = v
        elif isinstance(v, float):
            out[k] = v
        elif isinstance(v, str):
            out[k] = _short(v, 800)
        else:
            out[k] = _short(v, 800)
    return out


def _prune_store_locked(now: float) -> None:
    # Drop expired tids
    expired = [t for t, seen in _events_last_seen.items() if now - seen > _TTL_SEC]
    for t in expired:
        _events_store.pop(t, None)
        _events_last_seen.pop(t, None)
        _events_seq.pop(t, None)
    # Cap number of distinct tids (drop oldest by last_seen)
    while len(_events_store) > _MAX_TIDS:
        oldest_tid = min(_events_last_seen.keys(), key=lambda x: _events_last_seen.get(x, 0.0))
        _events_store.pop(oldest_tid, None)
        _events_last_seen.pop(oldest_tid, None)
        _events_seq.pop(oldest_tid, None)


def record_event(tid: str, phase: str, **kwargs: Any) -> None:
    """供 UI 轮询：与 log() 使用相同 tid，写入内存环形队列。"""
    if not events_enabled():
        return
    t = (tid or "").strip()[:64]
    if not t or t == "-":
        return
    now = time.time()
    with _events_lock:
        _prune_store_locked(now)
        if t not in _events_store:
            _events_store[t] = deque(maxlen=_MAX_EVENTS_PER_TID)
        seq = _events_seq.get(t, 0) + 1
        _events_seq[t] = seq
        _events_last_seen[t] = now
        _events_store[t].append(
            {
                "seq": seq,
                "ts": now,
                "phase": phase,
                "payload": _jsonable_payload(kwargs),
            }
        )


def get_trace_events(tid: str) -> List[dict]:
    t = (tid or "").strip()[:64]
    if not t:
        return []
    now = time.time()
    with _events_lock:
        if t not in _events_store:
            return []
        if now - _events_last_seen.get(t, 0) > _TTL_SEC:
            _events_store.pop(t, None)
            _events_last_seen.pop(t, None)
            _events_seq.pop(t, None)
            return []
        return list(_events_store[t])


def remember_feishu_pipeline_start(tid: str, message_id: str = "", chat_id: str = "", dedup: str = "") -> None:
    """飞书每条消息 enqueue 时登记，便于 Web 控制台拉「最近一条飞书链路」."""
    if not events_enabled():
        return
    t = (tid or "").strip()[:64]
    if not t or not t.startswith("fs_"):
        return
    now = time.time()
    row = {
        "tid": t,
        "ts": now,
        "message_id": _short(message_id or "", 80),
        "chat_id": _short(chat_id or "", 80),
        "dedup": _short(dedup or "", 80),
    }
    with _recent_feishu_lock:
        _RECENT_FEISHU_SESSIONS.append(row)


def list_recent_feishu_sessions(limit: int = 20) -> List[dict]:
    lim = max(1, min(int(limit or 20), 50))
    with _recent_feishu_lock:
        return list(_RECENT_FEISHU_SESSIONS)[-lim:][::-1]


def log(logger: logging.Logger, phase: str, tid: Optional[str] = None, **kwargs: Any) -> None:
    effective = ((tid or get_tid()) or "-").strip() or "-"
    if effective and effective != "-":
        record_event(effective, phase, **kwargs)
    if not enabled():
        return
    parts = ["[PIPELINE]", "tid={}".format(effective), "phase={}".format(phase)]
    for k in sorted(kwargs.keys()):
        v = kwargs[k]
        if v is None:
            continue
        if isinstance(v, bool):
            parts.append("{}={}".format(k, "1" if v else "0"))
        elif isinstance(v, float):
            parts.append("{}={:.4f}".format(k, v))
        elif isinstance(v, int):
            parts.append("{}={}".format(k, v))
        else:
            parts.append("{}={}".format(k, _short(v, 220)))
    logger.info(" ".join(parts))


async def run_sync_in_executor(loop, fn: Callable, *args, **kwargs):
    """在默认线程池中运行同步函数，并复制当前 ContextVar（含 trace_id）。"""
    ctx = contextvars.copy_context()

    def _runner():
        return ctx.run(lambda: fn(*args, **kwargs))

    return await loop.run_in_executor(None, _runner)
