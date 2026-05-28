#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Service V2 - FastAPI application
Refactored with DeepAgent framework + Skills architecture
Dual SQL engine support: Spark SQL + Trino
"""
import csv
import html
import json
import logging
import re
import os
import sys
import time
import uuid

import uvicorn
import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Dict, List, Optional

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    pass

from config.settings import (
    SERVICE_HOST, SERVICE_PORT, TEMP_SQL_DIR, TEMP_CSV_DIR,
    SQL_ENGINE_DEFAULT, QUERY_RESULT_MAX_ROWS,
    SQL_EXECUTOR_TIMEOUT,
    LLM_MODEL, LLM_MODEL_CHOICES, resolve_llm_model,
    LLM_TIMEOUT,
    LLM_API_URL, LLM_API_KEY,
    SERVICE_PUBLIC_ORIGIN,
)
from agents.sql_agent import generate_sql
from agents.analysis_agent import analyze_data
from skills.sql_executor import execute_sql, cancel_query
from skills.query_result_cache import get_query_result, put_query_result
from skills.web_session_store import get_web_session, save_web_session
from skills.pipeline_trace import (
    get_tid as pipeline_get_tid,
    log as pipeline_log,
    new_http_trace_id,
    reset_trace_id,
    set_trace_id,
)
from skills.schema_skill import get_schema_feedback_stats, record_success_feedback_from_sql
from skills.report_markdown_to_html import markdown_to_report_fragment_html
from skills.feishu_client import (
    create_feishu_doc_from_markdown,
    get_feishu_config,
    get_tenant_access_token,
)
from collections import deque
import threading
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("sql_service_v2")

app = FastAPI(
    title="Schemist · Schema-Guided SQL Assistant",
    description="NL→SQL with local schema tools, validation, Spark SQL + Trino execution, analysis; DeepAgent + Skills.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition", "X-Trace-Id"]
)

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(TEMPLATE_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMP_SQL_DIR, exist_ok=True)
os.makedirs(TEMP_CSV_DIR, exist_ok=True)

templates = Jinja2Templates(directory=TEMPLATE_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_PIPELINE_SKIP_PATHS = frozenset(
    {
        "/favicon.ico",
        "/api/health",
    }
)
_PIPELINE_SKIP_PREFIXES = ("/static/", "/docs", "/redoc", "/openapi.json")


def _build_page_agent_upstream_chat_url() -> str:
    """
    Build upstream OpenAI-compatible chat completions URL for PageAgent proxy.
    Accepts either:
      - full chat URL:  .../chat/completions
      - base v1 URL:    .../v1
      - base root URL:  ...
    """
    u = (LLM_API_URL or "").strip().rstrip("/")
    if not u:
        return ""
    if u.endswith("/chat/completions"):
        return u
    if u.endswith("/v1"):
        return u + "/chat/completions"
    return u + "/v1/chat/completions"


@app.middleware("http")
async def pipeline_http_middleware(request: Request, call_next):
    path = request.url.path
    # 轮询追踪接口若走同一套中间件会把大量 http.enter/exit 混入业务 tid；且不设置 trace ContextVar
    if path.startswith("/api/pipeline-trace") or path.startswith("/api/pipeline-feishu"):
        return await call_next(request)
    if path in _PIPELINE_SKIP_PATHS or any(path.startswith(p) for p in _PIPELINE_SKIP_PREFIXES):
        return await call_next(request)
    tid = new_http_trace_id(request.headers.get("x-request-id") or request.headers.get("X-Request-ID"))
    tok = set_trace_id(tid)
    try:
        pipeline_log(logger, "http.enter", path=path, method=request.method)
        response = await call_next(request)
        response.headers["X-Trace-Id"] = pipeline_get_tid()
        pipeline_log(logger, "http.exit", path=path, status=response.status_code)
        return response
    finally:
        reset_trace_id(tok)


# ======================== Pydantic Models ========================

class SQLGenerationRequest(BaseModel):
    query: str
    history: List[Dict[str, str]] = []
    max_tokens: int = 4000
    temperature: float = 0.3
    engine: str = SQL_ENGINE_DEFAULT
    llm_model: Optional[str] = None

class SQLGenerationResponse(BaseModel):
    sql: str
    explanation: str
    execution_plan: Optional[str] = None
    tables_used: List[str] = []
    query_time: float

class SQLExecutionRequest(BaseModel):
    sql: str
    max_rows: int = QUERY_RESULT_MAX_ROWS
    timeout: int = SQL_EXECUTOR_TIMEOUT
    engine: str = SQL_ENGINE_DEFAULT

class SQLExecutionResponse(BaseModel):
    success: bool
    result: List[Dict] = []
    headers: List[str] = []
    error: Optional[str] = None
    execution_time: float
    row_count: int = 0
    query_id: Optional[str] = None
    debug_info: Optional[Dict] = None

class SQLCancelRequest(BaseModel):
    query_id: str

class SQLCancelResponse(BaseModel):
    success: bool
    message: str


class WebSessionSaveRequest(BaseModel):
    session_id: Optional[str] = None
    step: int = 0
    status: str = "idle"
    tid: str = ""
    question: str = ""
    sql: str = ""
    headers: List[str] = []
    data: List[Dict] = []
    report_html: str = ""
    query_id: Optional[str] = None
    share_id: str = ""
    engine: str = ""
    smart_mode: bool = False
    chart_type: str = ""
    generate_meta: Optional[Dict] = None
    row_count: int = 0
    execution_time: float = 0.0


class WebSessionSaveResponse(BaseModel):
    session_id: str
    updated_at: float
    expires_at: float


class QueryResultCacheResponse(BaseModel):
    found: bool
    query_id: str = ""
    headers: List[str] = []
    result: List[Dict] = []
    row_count: int = 0
    execution_time: float = 0.0
    sql: str = ""
    engine: str = ""


class DataAnalysisRequest(BaseModel):
    original_question: str
    sql: str
    headers: List[str]
    data: List[Dict]
    chart_type: Optional[str] = None
    max_rows: int = QUERY_RESULT_MAX_ROWS
    llm_model: Optional[str] = None

class DataAnalysisResponse(BaseModel):
    success: bool
    report: str = ""
    error: Optional[str] = None
    analysis_time: float = 0


class FeishuDocMarkdownRequest(BaseModel):
    title: str = "SQL AI 数据分析报告"
    markdown: str
    folder_token: str = ""


class FeishuDocMarkdownResponse(BaseModel):
    success: bool
    document_id: str = ""
    url: str = ""
    error: Optional[str] = None
    block_count: int = 0


# ======================== Query History ========================

HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "query_history.json")
_query_history = deque(maxlen=500)
_history_lock = threading.Lock()


def _load_history():
    global _query_history
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                items = json.load(f)
            _query_history = deque(items[-500:], maxlen=500)
        except Exception:
            pass


def _save_history():
    try:
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(list(_query_history), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Failed to save history: %s", e)


def record_query(query_type, engine, question="", sql="", success=True, duration=0, error="", tables=None, row_count=0):
    entry = {
        "id": uuid.uuid4().hex[:8],
        "type": query_type,
        "engine": engine,
        "question": question[:200] if question else "",
        "sql": sql[:500] if sql else "",
        "success": success,
        "duration": round(duration, 2),
        "error": error[:200] if error else "",
        "tables": tables or [],
        "row_count": row_count,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with _history_lock:
        _query_history.append(entry)
        if len(_query_history) % 5 == 0:
            _save_history()


_load_history()


# ======================== Routes ========================

@app.get("/api/pipeline-trace/{tid}")
async def api_pipeline_trace(tid: str):
    """前端链路追踪面板轮询：返回该 tid 已记录的 pipeline 事件（内存，TTL 后清空）。"""
    from skills.pipeline_trace import get_trace_events

    ev = get_trace_events(tid)
    return {"tid": tid, "event_count": len(ev), "events": ev}


@app.get("/api/pipeline-feishu-recent")
async def api_pipeline_feishu_recent(limit: int = 20):
    """最近飞书入队的 pipeline tid（服务端内存）；供 Web「拉取最新飞书链路」."""
    from skills.pipeline_trace import list_recent_feishu_sessions

    return {"sessions": list_recent_feishu_sessions(limit)}


@app.get("/")
async def get_index(request: Request):
    # Keep real key server-side only; frontend uses proxy baseURL + public placeholder key.
    page_agent_base_url = "/api/page-agent-proxy/v1"
    page_agent_api_key = (os.environ.get("PAGE_AGENT_PUBLIC_KEY") or "page-agent-proxy").strip()
    page_agent_model = (os.environ.get("PAGE_AGENT_MODEL") or LLM_MODEL or "ws/kimi-k2.6").strip()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "query_result_max_rows": QUERY_RESULT_MAX_ROWS,
            "page_agent_base_url": page_agent_base_url,
            "page_agent_api_key": page_agent_api_key,
            "page_agent_model": page_agent_model,
        },
    )


@app.post("/api/page-agent-proxy/v1/chat/completions")
async def api_page_agent_proxy_chat(request: Request):
    """
    Browser-safe proxy for PageAgent:
    - Frontend calls this endpoint with a non-secret key.
    - Backend injects real LLM API key from .env and forwards to upstream.
    """
    upstream_url = _build_page_agent_upstream_chat_url()
    if not upstream_url:
        raise HTTPException(status_code=503, detail="LLM_API_URL not configured")
    if not (LLM_API_KEY or "").strip():
        raise HTTPException(status_code=503, detail="LLM_API_KEY not configured")

    try:
        raw_body = await request.body()
        content_type = request.headers.get("content-type", "application/json")
        timeout_sec = max(10, int(LLM_TIMEOUT))

        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            upstream_resp = await client.post(
                upstream_url,
                content=raw_body,
                headers={
                    "Content-Type": content_type,
                    "Authorization": f"Bearer {LLM_API_KEY}",
                },
            )
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            media_type=upstream_resp.headers.get("content-type"),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("page-agent proxy error: %s", e)
        raise HTTPException(status_code=502, detail=f"page-agent proxy failed: {e}")


@app.post("/api/generate-sql", response_model=SQLGenerationResponse)
async def api_generate_sql(request: SQLGenerationRequest):
    pipeline_log(
        logger,
        "api.generate_sql.enter",
        engine=request.engine,
        q_chars=len(request.query or ""),
        history_msgs=len(request.history or []),
    )
    try:
        llm_model = resolve_llm_model(request.llm_model)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    try:
        result = generate_sql(
            query=request.query,
            history=request.history,
            engine=request.engine,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            llm_model=llm_model,
        )
        pipeline_log(
            logger,
            "api.generate_sql.exit",
            ok=bool(result.get("sql")),
            sql_chars=len(result.get("sql") or ""),
            tables=len(result.get("tables_used") or []),
            sec=result.get("query_time"),
        )
        record_query(
            query_type="generate",
            engine=request.engine,
            question=request.query,
            sql=result.get("sql", ""),
            success=bool(result.get("sql")),
            duration=result.get("query_time", 0),
            tables=result.get("tables_used", []),
        )
        return SQLGenerationResponse(
            sql=result["sql"],
            explanation=result["explanation"],
            tables_used=result.get("tables_used", []),
            execution_plan=result.get("execution_plan"),
            query_time=result["query_time"],
        )
    except Exception as e:
        logger.exception("generate-sql error: %s", e)
        pipeline_log(logger, "api.generate_sql.exception", err=str(e)[:200])
        record_query(query_type="generate", engine=request.engine, question=request.query, success=False, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/execute-sql", response_model=SQLExecutionResponse)
async def api_execute_sql(request: SQLExecutionRequest):
    pipeline_log(
        logger,
        "api.execute_sql.enter",
        engine=request.engine,
        sql_chars=len(request.sql or ""),
        max_rows=request.max_rows,
    )
    try:
        success, headers, results, error, exec_time, query_id, debug_info = execute_sql(
            sql=request.sql,
            engine=request.engine,
            max_rows=request.max_rows,
            timeout=request.timeout,
        )
        record_query(
            query_type="execute",
            engine=request.engine,
            sql=request.sql,
            success=success,
            duration=exec_time,
            error=error or "",
            row_count=len(results),
        )
        di = dict(debug_info or {})
        feedback_info = None
        # Lightweight online learning: only reinforce tables after a successful execution with non-empty result.
        if success and len(results) > 0:
            try:
                feedback_info = record_success_feedback_from_sql(
                    sql=request.sql,
                    query="",
                    engine=request.engine,
                    row_count=len(results),
                )
            except Exception as fe:
                logger.warning("record schema feedback failed: %s", fe)
        if feedback_info:
            di["schema_feedback"] = feedback_info
        pipeline_log(
            logger,
            "api.execute_sql.exit",
            ok=success,
            exec_id=query_id,
            cols=len(headers or []),
            rows=len(results or []),
            sec=exec_time,
            err=(error or "")[:160],
        )
        if success and query_id and results is not None:
            try:
                put_query_result(
                    query_id,
                    headers=headers or [],
                    result=results or [],
                    row_count=len(results or []),
                    execution_time=exec_time,
                    sql=request.sql,
                    engine=request.engine,
                    success=success,
                )
            except Exception as ce:
                logger.warning("query_result_cache put failed: %s", ce)
        if success and len(results) == 0:
            di["zero_rows_hint"] = (
                "查询成功但结果集为 0 行（前端与接口正常）。请核对：① 分区 dt 在范围内是否有数据；"
                "② WHERE 条件是否与库里实际取值一致。常见情况：表 dws_cn.dws_behavior_map_game_reco_i_d "
                "的字段 api_type 落库多为五位字符串 '40001'，业务口语「API 4001」若写成 '4001'（四位）会查不到数据。"
                "可先执行：SELECT DISTINCT api_type FROM dws_cn.dws_behavior_map_game_reco_i_d WHERE dt='最近有数据的一天' LIMIT 50 核对枚举。"
            )
        return SQLExecutionResponse(
            success=success,
            result=results,
            headers=headers,
            error=error,
            execution_time=exec_time,
            row_count=len(results),
            query_id=query_id,
            debug_info=di,
        )
    except Exception as e:
        logger.exception("execute-sql error: %s", e)
        pipeline_log(logger, "api.execute_sql.exception", err=str(e)[:200])
        record_query(query_type="execute", engine=request.engine, sql=request.sql, success=False, error=str(e))
        return SQLExecutionResponse(
            success=False, error=str(e), execution_time=0, row_count=0
        )


@app.post("/api/cancel-sql", response_model=SQLCancelResponse)
async def api_cancel_sql(request: SQLCancelRequest):
    success, message = cancel_query(request.query_id)
    return SQLCancelResponse(success=success, message=message)


@app.post("/api/web-session", response_model=WebSessionSaveResponse)
async def api_save_web_session(request: WebSessionSaveRequest):
    try:
        payload = request.dict()
        if request.generate_meta is not None:
            payload["generate_meta"] = request.generate_meta
        out = save_web_session(payload)
        pipeline_log(
            logger,
            "api.web_session.save",
            session_id=out.get("session_id"),
            step=request.step,
            status=request.status,
            rows=len(request.data or []),
        )
        return WebSessionSaveResponse(
            session_id=out["session_id"],
            updated_at=float(out["updated_at"]),
            expires_at=float(out["expires_at"]),
        )
    except Exception as e:
        logger.exception("web-session save error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/web-session/{session_id}")
async def api_get_web_session(session_id: str):
    entry = get_web_session(session_id)
    if not entry:
        raise HTTPException(status_code=404, detail="session not found or expired")
    return entry


@app.get("/api/query-result/{query_id}", response_model=QueryResultCacheResponse)
async def api_get_query_result(query_id: str):
    entry = get_query_result(query_id)
    if not entry:
        return QueryResultCacheResponse(found=False, query_id=query_id or "")
    return QueryResultCacheResponse(
        found=True,
        query_id=entry.get("query_id") or query_id,
        headers=entry.get("headers") or [],
        result=entry.get("result") or [],
        row_count=int(entry.get("row_count") or 0),
        execution_time=float(entry.get("execution_time") or 0),
        sql=entry.get("sql") or "",
        engine=entry.get("engine") or "",
    )


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "timestamp": time.time(), "version": "2.0.0", "engine_default": SQL_ENGINE_DEFAULT}


@app.get("/api/llm-models")
async def api_llm_models():
    """返回前端可选的大模型列表与当前默认模型。"""
    models = [{"id": mid, "label": label} for mid, label in LLM_MODEL_CHOICES]
    return {
        "default": LLM_MODEL,
        "models": models,
    }


@app.post("/api/download-csv")
async def download_csv(request: Request):
    try:
        data = await request.json()
        headers = data.get("headers", [])
        result_data = data.get("data", [])
        filename = data.get("filename")

        if not headers or not result_data:
            raise HTTPException(status_code=400, detail="Missing headers or data")

        if not filename:
            filename = "query_result_{}.csv".format(uuid.uuid4().hex[:8])
        elif not filename.endswith('.csv'):
            filename += '.csv'

        file_path = os.path.join(TEMP_CSV_DIR, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in result_data:
                writer.writerow({h: row.get(h, '') for h in headers})

        return FileResponse(path=file_path, filename=filename, media_type='text/csv')
    except Exception as e:
        logger.exception("download-csv error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analyze-data", response_model=DataAnalysisResponse)
async def api_analyze_data(request: DataAnalysisRequest):
    pipeline_log(
        logger,
        "api.analyze_data.enter",
        rows=len(request.data or []),
        cols=len(request.headers or []),
        chart=request.chart_type or "",
    )
    try:
        llm_model = resolve_llm_model(request.llm_model)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    try:
        result = analyze_data(
            original_question=request.original_question,
            sql=request.sql,
            headers=request.headers,
            data=request.data,
            chart_type=request.chart_type,
            max_rows=request.max_rows,
            llm_model=llm_model,
        )
        pipeline_log(
            logger,
            "api.analyze_data.exit",
            ok=bool(result.get("success")),
            sec=result.get("analysis_time"),
            report_chars=len(result.get("report") or ""),
        )
        return DataAnalysisResponse(
            success=result["success"],
            report=result.get("report", ""),
            error=result.get("error"),
            analysis_time=result.get("analysis_time", 0),
        )
    except Exception as e:
        logger.exception("analyze-data error: %s", e)
        pipeline_log(logger, "api.analyze_data.exception", err=str(e)[:200])
        return DataAnalysisResponse(
            success=False, error=str(e), analysis_time=0
        )


@app.post("/api/feishu-doc-from-markdown", response_model=FeishuDocMarkdownResponse)
async def api_feishu_doc_from_markdown(request: FeishuDocMarkdownRequest):
    """Create a Feishu docx document from Markdown report content."""
    cfg = get_feishu_config()
    if not cfg.get("app_id") or not cfg.get("app_secret"):
        return FeishuDocMarkdownResponse(
            success=False,
            error="Feishu is not configured: FEISHU_APP_ID / FEISHU_APP_SECRET are required",
        )

    folder_token = (request.folder_token or os.getenv("FEISHU_DOC_FOLDER_TOKEN", "") or "").strip()
    try:
        async with httpx.AsyncClient() as client:
            token = await get_tenant_access_token(client, cfg["app_id"], cfg["app_secret"])
            if not token:
                return FeishuDocMarkdownResponse(success=False, error="Failed to get Feishu tenant_access_token")

            result = await create_feishu_doc_from_markdown(
                client=client,
                token=token,
                title=request.title,
                markdown=request.markdown,
                folder_token=folder_token,
            )
        return FeishuDocMarkdownResponse(
            success=bool(result.get("success")),
            document_id=result.get("document_id") or "",
            url=result.get("url") or "",
            error=result.get("error"),
            block_count=result.get("block_count") or 0,
        )
    except Exception as e:
        logger.exception("feishu-doc-from-markdown error: %s", e)
        return FeishuDocMarkdownResponse(success=False, error=str(e))


@app.get("/api/query-stats")
async def api_query_stats():
    """Return query statistics and recent history."""
    with _history_lock:
        history = list(_query_history)

    total = len(history)
    engine_stats = {}
    type_stats = {"generate": 0, "execute": 0}
    success_count = 0
    fail_count = 0
    table_freq = {}
    daily_counts = {}

    for entry in history:
        eng = entry.get("engine", "unknown")
        engine_stats[eng] = engine_stats.get(eng, 0) + 1

        qt = entry.get("type", "unknown")
        type_stats[qt] = type_stats.get(qt, 0) + 1

        if entry.get("success"):
            success_count += 1
        else:
            fail_count += 1

        for t in entry.get("tables", []):
            table_freq[t] = table_freq.get(t, 0) + 1

        day = entry.get("timestamp", "")[:10]
        if day:
            daily_counts[day] = daily_counts.get(day, 0) + 1

    top_tables = sorted(table_freq.items(), key=lambda x: -x[1])[:15]
    recent = list(reversed(history[-50:]))

    return {
        "total_queries": total,
        "engine_stats": engine_stats,
        "type_stats": type_stats,
        "success_count": success_count,
        "fail_count": fail_count,
        "top_tables": [{"table": t, "count": c} for t, c in top_tables],
        "daily_counts": daily_counts,
        "recent_queries": recent,
    }


@app.get("/api/schema-feedback-stats")
async def api_schema_feedback_stats(limit: int = Query(50, ge=1, le=500)):
    """Read-only stats for schema feedback learning."""
    try:
        return get_schema_feedback_stats(limit=limit)
    except Exception as e:
        logger.exception("schema-feedback-stats error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query-history")
async def api_query_history(limit: int = 50, engine: str = None, query_type: str = None):
    """Return filtered query history."""
    with _history_lock:
        history = list(_query_history)

    if engine:
        history = [h for h in history if h.get("engine") == engine]
    if query_type:
        history = [h for h in history if h.get("type") == query_type]

    return {"items": list(reversed(history[-limit:])), "total": len(history)}


# ======================== Report Sharing ========================

SHARE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared_reports")
os.makedirs(SHARE_DIR, exist_ok=True)

_REPORT_TABLE_RE = re.compile(r"(<table\b[^>]*>[\s\S]*?</table>)", re.IGNORECASE)


def build_shared_table_html(headers: List[str], rows: List[dict], max_rows: int = 500) -> str:
    """将查询结果转为分享页可用的 HTML 表格（无 JS 依赖）。"""
    if not headers:
        return ""
    n = max(0, int(max_rows or 0))
    esc = html.escape
    ths = "".join("<th>{}</th>".format(esc(str(h))) for h in headers)
    body_rows = []
    for row in (rows or [])[:n]:
        tds = "".join("<td>{}</td>".format(esc(str(row.get(h, "")) if row.get(h, "") is not None else "")) for h in headers)
        body_rows.append("<tr>{}</tr>".format(tds))
    return (
        '<table class="data-table">'
        "<thead><tr>{}</tr></thead><tbody>{}</tbody></table>"
    ).format(ths, "".join(body_rows))


def markdown_report_to_share_html(markdown: str, max_chars: int = 400000) -> str:
    """
    与 Web 端「分享报告」写入的 report_html 对齐：Markdown → HTML 片段
    （表格围栏展开、sanitize、report-table-scroll），非转义 pre。
    """
    return markdown_to_report_fragment_html(markdown, max_chars=max_chars)


def persist_shared_report(
    report_html: str,
    question: str = "",
    sql: str = "",
    engine: str = "spark",
    chart_image: str = "",
    table_html: str = "",
) -> Dict[str, str]:
    """
    写入 shared_reports 下的 JSON，与 POST /api/share-report 行为一致。
    成功返回 share_id、share_url（相对路径）；失败返回 error。
    """
    share_id = uuid.uuid4().hex[:12]
    chart_filename = ""
    if chart_image and chart_image.startswith("data:image/"):
        try:
            import base64

            header, b64data = chart_image.split(",", 1)
            ext = "png" if "png" in header else "jpg"
            chart_filename = "chart_{}.{}".format(share_id, ext)
            chart_path = os.path.join(SHARE_DIR, chart_filename)
            with open(chart_path, "wb") as cf:
                cf.write(base64.b64decode(b64data))
        except Exception as e:
            logger.warning("Failed to save chart image: %s", e)
            chart_filename = ""

    report_data = {
        "id": share_id,
        "report_html": report_html or "",
        "question": question or "",
        "sql": sql or "",
        "engine": engine or "spark",
        "chart_filename": chart_filename,
        "table_html": table_html or "",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
    }
    filepath = os.path.join(SHARE_DIR, "{}.json".format(share_id))
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        pipeline_log(
            logger,
            "share.persist.done",
            share_id=share_id,
            engine=engine,
            chart=bool(chart_filename),
            tbl_chars=len(table_html or ""),
            rpt_chars=len(report_html or ""),
        )
        return {"share_id": share_id, "share_url": "/shared/{}".format(share_id)}
    except Exception as e:
        logger.exception("persist_shared_report error: %s", e)
        return {"error": str(e)}


def _wrap_report_tables_in_html(html: str) -> str:
    """横向滚动 + 表头粘性；分享页无前端 JS，在服务端包一层。"""
    if not html or "<table" not in html.lower():
        return html
    # 前端已 enhanceReportTables 时勿重复包裹
    if "report-table-scroll" in html.lower():
        return html
    return _REPORT_TABLE_RE.sub(r'<div class="report-table-scroll">\1</div>', html)


class ShareReportRequest(BaseModel):
    report_html: str
    question: str = ""
    sql: str = ""
    engine: str = "spark"
    chart_image: str = ""
    table_html: str = ""


@app.post("/api/share-report")
async def api_share_report(request: ShareReportRequest):
    try:
        out = persist_shared_report(
            report_html=request.report_html,
            question=request.question,
            sql=request.sql,
            engine=request.engine,
            chart_image=request.chart_image or "",
            table_html=request.table_html or "",
        )
        if out.get("error"):
            return out
        return {"share_url": out["share_url"], "share_id": out["share_id"]}
    except Exception as e:
        logger.exception("share-report error: %s", e)
        return {"error": str(e)}


@app.get("/shared/charts/{filename}")
async def serve_shared_chart(filename: str):
    """Serve chart images for shared reports."""
    filepath = os.path.join(SHARE_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Image not found")
    media = "image/png" if filename.endswith(".png") else "image/jpeg"
    return FileResponse(path=filepath, media_type=media)


SHARED_REPORT_CSS = """
:root {
  --rp-bg: #eef1f6;
  --rp-card: #ffffff;
  --rp-ink: #1e293b;
  --rp-muted: #64748b;
  --rp-accent: #2563eb;
  --rp-line: #e2e8f0;
}
body { background: var(--rp-bg); margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'PingFang SC', 'Microsoft YaHei', sans-serif; color: var(--rp-ink); }
.report-page { max-width: 1100px; margin: 0 auto; padding: 24px 16px 48px; }
.report-header { background: linear-gradient(135deg, #1e3a5f 0%, #2c5282 52%, #3182ce 100%); color: #fff; padding: 28px 32px; border-radius: 16px 16px 0 0; box-shadow: 0 6px 24px rgba(30,58,95,.22); }
.report-header h4 { margin: 0 0 8px 0; font-size: 22px; font-weight: 700; letter-spacing: .02em; }
.meta-tag { display: inline-block; background: rgba(255,255,255,0.18); padding: 4px 14px; border-radius: 20px; font-size: 13px; margin-right: 8px; margin-top: 6px; border: 1px solid rgba(255,255,255,0.25); }
.report-content-card { background: var(--rp-card); padding: 28px 32px; border-radius: 0 0 16px 16px; box-shadow: 0 4px 24px rgba(15,23,42,.07); border: 1px solid var(--rp-line); border-top: none; }

.section-divider { border: none; border-top: 2px solid var(--rp-line); margin: 28px 0; }
.section-title { font-size: 15px; font-weight: 700; color: var(--rp-accent); margin-bottom: 14px; display: flex; align-items: center; gap: 8px; text-transform: none; letter-spacing: .02em; }

.chart-section img { max-width: 100%; height: auto; border-radius: 10px; border: 1px solid var(--rp-line); box-shadow: 0 2px 8px rgba(15,23,42,.06); }

.data-table { width: 100%; border-collapse: collapse; font-size: 13px; margin: 0; }
.data-table th { background: linear-gradient(180deg,#f1f5f9,#e2e8f0); font-weight: 600; border: 1px solid #cbd5e1; padding: 8px 12px; text-align: left; white-space: nowrap; }
.data-table td { border: 1px solid #e2e8f0; padding: 6px 12px; }
.data-table tr:nth-child(even) { background: #f8fafc; }
.data-table-wrapper { max-height: 420px; overflow: auto; border-radius: 10px; border: 1px solid var(--rp-line); }

.shared-table-toolbar { margin: 8px 0 12px; display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.shared-csv-btn {
  display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; font-size: 14px; font-weight: 600;
  color: #15803d; background: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; cursor: pointer;
}
.shared-csv-btn:hover { background: #dcfce7; border-color: #4ade80; }
.shared-csv-hint { font-size: 12px; color: var(--rp-muted); }

.sql-details summary { cursor: pointer; color: var(--rp-accent); font-weight: 600; font-size: 14px; padding: 8px 0; }
.sql-details pre { background: #1e293b; color: #e2e8f0; padding: 16px; border-radius: 10px; overflow-x: auto; font-size: 13px; margin-top: 8px; border: 1px solid #334155; }

/* AI 报告正文（与站内 .analysis-report-body 语义对齐） */
.report-body { line-height: 1.75; font-size: 0.95rem; color: var(--rp-ink); }
.report-body > h1:first-child { margin-top: 0; }
.report-body h1 { font-size: 1.32rem; font-weight: 700; margin: 1.5rem 0 1rem; padding: 1rem 1.25rem; background: linear-gradient(135deg, #1e3a5f, #2d4a6f); color: #fff !important; border-radius: 12px; box-shadow: 0 4px 16px rgba(30,58,95,.2); letter-spacing: .02em; }
.report-body h2 { font-size: 1.12rem; margin-top: 1.85rem; margin-bottom: 0.75rem; padding-bottom: 0.45rem; border-bottom: 2px solid var(--rp-line); color: #0f172a !important; font-weight: 700; display: flex; align-items: center; gap: 0.5rem; }
.report-body h2::before { content: ''; width: 4px; height: 1.05em; background: linear-gradient(180deg, var(--rp-accent), #60a5fa); border-radius: 2px; flex-shrink: 0; }
.report-body h3 { font-size: 1.03rem; color: #334155 !important; margin-top: 1.2rem; font-weight: 600; }
.report-body h4 { font-size: 0.95rem; color: var(--rp-muted) !important; font-weight: 600; }
.report-body p { margin: 0.65em 0; }
.report-body strong { color: #0f172a !important; font-weight: 600; }
.report-body ul, .report-body ol { padding-left: 1.35em; margin: 0.7em 0 1em; }
.report-body li { margin-bottom: 0.45em; }
.report-body li::marker { color: var(--rp-accent); }

.report-table-scroll { overflow-x: auto; margin: 1.15em 0; border-radius: 10px; border: 1px solid var(--rp-line); box-shadow: 0 1px 3px rgba(15,23,42,.06); background: #fff; -webkit-overflow-scrolling: touch; }
.report-table-scroll table { width: 100%; min-width: 560px; border-collapse: collapse; margin: 0; font-size: 0.88rem; }
.report-table-scroll thead th { position: sticky; top: 0; z-index: 2; background: linear-gradient(180deg, #f1f5f9, #e2e8f0) !important; font-weight: 600; color: #0f172a; border-bottom: 2px solid #cbd5e1; white-space: nowrap; box-shadow: 0 1px 0 #cbd5e1; }
.report-table-scroll th, .report-table-scroll td { border: 1px solid var(--rp-line); padding: 10px 12px; text-align: left; vertical-align: top; }
.report-table-scroll tbody tr:nth-child(even) { background: #f8fafc; }
.report-body > table { width: 100%; border-collapse: collapse; margin: 1em 0; font-size: 0.9rem; }
.report-body > table th, .report-body > table td { border: 1px solid var(--rp-line); padding: 8px 12px; }
.report-body > table th { background: #f1f5f9; font-weight: 600; }
.report-body > table tr:nth-child(even) { background: #f8fafc; }

.report-body code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 0.88em; color: #b91c1c; border: 1px solid #e2e8f0; }
.report-body pre { background: #1e293b; color: #e2e8f0; padding: 16px; border-radius: 10px; overflow-x: auto; font-size: 0.84rem; border: 1px solid #334155; }
.report-body blockquote { margin: 1rem 0; padding: 12px 16px 12px 18px; border-left: 4px solid var(--rp-accent); background: #eff6ff; border-radius: 0 10px 10px 0; color: #1e3a5f !important; font-style: normal; box-shadow: 0 1px 2px rgba(15,23,42,.04); }
.report-body blockquote strong { color: #1e40af !important; }

.report-body .highlight-positive { color: #15803d !important; font-weight: 600; }
.report-body .highlight-negative { color: #b91c1c !important; font-weight: 600; }
.report-body .highlight-neutral { color: #b45309 !important; font-weight: 600; }
.report-body .metric-box { display: inline-block; background: #f8fafc; border: 1px solid var(--rp-line); border-radius: 10px; padding: 10px 16px; margin: 4px; text-align: center; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
.report-body .metric-box .metric-value { font-size: 1.28em; font-weight: 700; color: #0f172a; }
.report-body .metric-box .metric-label { font-size: 0.78em; color: var(--rp-muted); margin-top: 2px; }

/* 可选：模型/手工嵌入的 HTML 片段（与站内一致） */
.report-body .report-callout { margin: 1rem 0; padding: 14px 18px; border-radius: 10px; border: 1px solid transparent; line-height: 1.65; }
.report-body .report-callout-risk { background: #fef2f2; border-color: #fecaca; color: #7f1d1d !important; }
.report-body .report-callout-ok { background: #f0fdf4; border-color: #bbf7d0; color: #14532d !important; }
.report-body .report-callout-note { background: #fffbeb; border-color: #fde68a; color: #78350f !important; }
.report-body .report-callout-info { background: #eff6ff; border-color: #bfdbfe; color: #1e3a5f !important; }
.report-body .report-kpi-row { display: flex; flex-wrap: wrap; gap: 12px; margin: 1rem 0; }
.report-body .report-kpi-card { flex: 1 1 150px; min-width: 130px; background: #fff; border: 1px solid var(--rp-line); border-radius: 12px; padding: 14px 16px; text-align: center; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.report-body .report-kpi-card .v { display: block; font-size: 1.32rem; font-weight: 700; color: #0f172a; line-height: 1.2; }
.report-body .report-kpi-card .l { display: block; font-size: 0.74rem; color: var(--rp-muted); margin-top: 6px; line-height: 1.35; }

.footer-bar { text-align: center; color: #94a3b8; font-size: 12px; padding: 20px 0 8px; margin-top: 8px; }
@media print {
  body { background: #fff; }
  .report-page { max-width: none; padding: 0; }
  .report-header, .report-content-card { box-shadow: none; border-radius: 0; }
  .report-table-scroll { break-inside: avoid; }
}
"""


@app.get("/shared/{share_id}")
async def view_shared_report(share_id: str, request: Request):
    filepath = os.path.join(SHARE_DIR, "{}.json".format(share_id))
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="报告不存在或已过期")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(status_code=500, detail="读取报告失败")

    try:
        expires = datetime.strptime(data.get("expires_at", ""), "%Y-%m-%d %H:%M:%S")
        if datetime.now() > expires:
            os.remove(filepath)
            raise HTTPException(status_code=410, detail="报告链接已过期")
    except (ValueError, TypeError):
        pass

    engine_label = "Spark SQL" if data.get("engine") == "spark" else "Trino"

    # Build optional sections
    question_block = ""
    if data.get("question"):
        question_block = '<p style="margin:8px 0 0;opacity:0.85;font-size:15px;"><i class="bi bi-chat-dots me-1"></i>{}</p>'.format(
            data["question"].replace("<", "&lt;").replace(">", "&gt;")
        )

    sql_block = ""
    if data.get("sql"):
        escaped_sql = data["sql"].replace("<", "&lt;").replace(">", "&gt;")
        sql_block = '<hr class="section-divider"><div class="section-title"><i class="bi bi-code-square"></i>SQL 查询</div><details class="sql-details" open><summary>点击收起/展开 SQL</summary><pre><code>{}</code></pre></details>'.format(escaped_sql)

    chart_block = ""
    if data.get("chart_filename"):
        chart_block = '<hr class="section-divider"><div class="section-title"><i class="bi bi-bar-chart-line"></i>数据图表</div><div class="chart-section"><img src="/shared/charts/{}" alt="数据图表"></div>'.format(data["chart_filename"])

    table_block = ""
    if data.get("table_html"):
        table_block = (
            '<hr class="section-divider">'
            '<div class="section-title"><i class="bi bi-table"></i>查询结果数据</div>'
            '<div class="shared-table-toolbar">'
            '<button type="button" class="shared-csv-btn" onclick="downloadSharedTableCsv()">'
            '<i class="bi bi-download"></i> 下载为 CSV</button>'
            '<span class="shared-csv-hint">根据下方表格导出（UTF-8 含 BOM，可用 Excel / WPS 打开）</span>'
            "</div>"
            '<div class="data-table-wrapper">{}</div>'
        ).format(data["table_html"])

    report_block = ""
    if data.get("report_html"):
        report_block = '<hr class="section-divider"><div class="section-title"><i class="bi bi-file-earmark-richtext"></i>AI 分析报告</div><div class="report-body">{}</div>'.format(
            _wrap_report_tables_in_html(data["report_html"])
        )

    html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>数据分析报告 - {question_title}</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css" rel="stylesheet">
<style>{css}</style>
</head>
<body>
<div class="report-page">
<div class="report-header">
<h4><i class="bi bi-file-earmark-richtext me-2"></i>数据分析报告</h4>
<div>
<span class="meta-tag"><i class="bi bi-calendar me-1"></i>{created_at}</span>
<span class="meta-tag"><i class="bi bi-cpu me-1"></i>{engine}</span>
</div>
{question_block}
</div>
<div class="report-content-card">
{sql_block}
{table_block}
{chart_block}
{report_block}
</div>
<div class="footer-bar">
Schemist · Schema-Guided SQL Assistant &middot; 分享报告 &middot; 有效期至 {expires_at}
</div>
</div>
<script>
function _sharedEscapeCsvCell(t) {{
  if (t == null || t === undefined) t = '';
  t = String(t).trim();
  if (/[",\\n\\r]/.test(t)) return '"' + t.replace(/"/g, '""') + '"';
  return t;
}}
function downloadSharedTableCsv() {{
  var wrap = document.querySelector('.data-table-wrapper');
  if (!wrap) {{ alert('当前页面无「查询结果数据」表格'); return; }}
  var table = wrap.querySelector('table');
  if (!table) {{ alert('未找到结果表'); return; }}
  var lines = [];
  var thead = table.querySelector('thead');
  var tbody = table.querySelector('tbody');
  if (thead) {{
    thead.querySelectorAll('tr').forEach(function(tr) {{
      var cells = [];
      tr.querySelectorAll('th,td').forEach(function(c) {{ cells.push(_sharedEscapeCsvCell(c.innerText)); }});
      if (cells.length) lines.push(cells.join(','));
    }});
  }}
  if (tbody) {{
    tbody.querySelectorAll('tr').forEach(function(tr) {{
      var cells = [];
      tr.querySelectorAll('td,th').forEach(function(c) {{ cells.push(_sharedEscapeCsvCell(c.innerText)); }});
      if (cells.length) lines.push(cells.join(','));
    }});
  }}
  if (!lines.length) {{ alert('表格无数据行'); return; }}
  var bom = '\\uFEFF';
  var blob = new Blob([bom + lines.join('\\n')], {{ type: 'text/csv;charset=utf-8' }});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'query_result_{share_file_id}.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(a.href);
}}
</script>
</body></html>""".format(
        question_title=data.get("question", "未命名报告")[:60].replace("<", "&lt;"),
        css=SHARED_REPORT_CSS,
        created_at=data.get("created_at", ""),
        engine=engine_label,
        question_block=question_block,
        sql_block=sql_block,
        table_block=table_block,
        chart_block=chart_block,
        report_block=report_block,
        expires_at=data.get("expires_at", ""),
        share_file_id=share_id.replace("/", "_"),
    )
    return HTMLResponse(content=html)


from feishu_bot_api import router as feishu_router

app.include_router(feishu_router)


# ======================== Skills Pack (提示词模板能力包) ========================
from skills_pack import (
    get_registry as _get_skills_registry,
    SkillNotFound,
    SkillValidationError,
)

SKILLS_ADMIN_TOKEN = os.environ.get("SKILLS_ADMIN_TOKEN", "").strip()


def _check_skills_admin(request: Request) -> None:
    """若设置了 SKILLS_ADMIN_TOKEN 环境变量，则写操作需要携带 X-Skills-Admin-Token。
    未设置时不做鉴权（内网部署默认开放，便于团队协作）。"""
    if not SKILLS_ADMIN_TOKEN:
        return
    token = request.headers.get("x-skills-admin-token") or request.headers.get("X-Skills-Admin-Token")
    if token != SKILLS_ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="缺少或错误的 X-Skills-Admin-Token")


class SkillMetaPayload(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    icon: Optional[str] = ""
    placeholder: Optional[str] = ""
    enabled: Optional[bool] = True
    order: Optional[int] = 100
    engine_hint: Optional[str] = ""
    llm_model_hint: Optional[str] = ""
    tags: Optional[List[str]] = None
    body: str = ""


class SkillRenderPayload(BaseModel):
    user_query: str = ""
    engine: Optional[str] = ""
    today: Optional[str] = ""
    variables: Optional[Dict[str, str]] = None


@app.get("/api/skills")
async def api_list_skills(include_disabled: bool = False):
    """返回技能列表（默认只返回 enabled=True 的技能，不含正文）。"""
    reg = _get_skills_registry()
    return {"items": reg.list(include_disabled=include_disabled, include_body=False)}


@app.get("/api/skills/{skill_id}")
async def api_get_skill(skill_id: str):
    reg = _get_skills_registry()
    try:
        return reg.get(skill_id)
    except SkillNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    except SkillValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/skills")
async def api_create_skill(payload: SkillMetaPayload, request: Request):
    _check_skills_admin(request)
    reg = _get_skills_registry()
    meta = payload.dict()
    body = meta.pop("body", "") or ""
    try:
        return reg.save(meta, body, create=True)
    except SkillValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/skills/{skill_id}")
async def api_update_skill(skill_id: str, payload: SkillMetaPayload, request: Request):
    _check_skills_admin(request)
    if payload.id != skill_id:
        raise HTTPException(status_code=400, detail="payload.id 与 URL 不一致")
    reg = _get_skills_registry()
    meta = payload.dict()
    body = meta.pop("body", "") or ""
    try:
        return reg.save(meta, body, create=False)
    except SkillNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    except SkillValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/skills/{skill_id}")
async def api_delete_skill(skill_id: str, request: Request):
    _check_skills_admin(request)
    reg = _get_skills_registry()
    try:
        reg.delete(skill_id)
        return {"ok": True, "id": skill_id}
    except SkillNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    except SkillValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/skills/{skill_id}/render")
async def api_render_skill(skill_id: str, payload: SkillRenderPayload):
    """调试/预览：用给定变量渲染模板，返回最终 prompt。"""
    reg = _get_skills_registry()
    try:
        skill = reg.get(skill_id)
    except SkillNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    now = datetime.now()
    today = payload.today or now.strftime("%Y-%m-%d")
    try:
        today_parsed = datetime.strptime(today, "%Y-%m-%d")
    except ValueError:
        today_parsed = now
    yesterday = (today_parsed - timedelta(days=1)).strftime("%Y-%m-%d")
    variables = dict(payload.variables or {})
    variables.update({
        "user_query": payload.user_query or "",
        "engine": payload.engine or SQL_ENGINE_DEFAULT,
        "today": today,
        "yesterday": yesterday,
    })
    from skills_pack.registry import render_template
    rendered = render_template(skill.get("body") or "", variables)
    return {
        "id": skill_id,
        "name": skill.get("name", ""),
        "rendered": rendered,
        "variables": variables,
    }


if __name__ == "__main__":
    routes_info = []
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            methods = ", ".join(route.methods)
            routes_info.append("{}: {}".format(methods, route.path))

    logger.info("Registered API routes:")
    for r in sorted(routes_info):
        logger.info("  %s", r)

    uvicorn.run(app, host=SERVICE_HOST, port=SERVICE_PORT)
