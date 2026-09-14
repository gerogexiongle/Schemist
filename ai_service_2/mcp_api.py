#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stateless Streamable HTTP MCP endpoint for Schemist."""

import hmac
import json
import os
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from skills.pipeline_trace import reset_trace_id, set_trace_id


SERVER_NAME = "schemist"
SERVER_VERSION = "1.2.0"
DEFAULT_PROTOCOL_VERSION = "2025-11-25"
SUPPORTED_PROTOCOL_VERSIONS = frozenset(
    {"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"}
)

SERVER_INSTRUCTIONS = (
    "Use these tools flexibly for business-data questions. The default engine is Trino unless the "
    "user explicitly requests Spark. ask_data performs an end-to-end workflow. generate_sql only "
    "creates SQL and never returns query data; when the user's goal is data, follow a successful "
    "generate_sql call with execute_readonly_sql. Codex may inspect or minimally edit generated SQL "
    "before execution. Never repeat generate_sql with identical arguments unless a concrete validation "
    "error or missing semantic stage is supplied, and never repeat a failed execute_readonly_sql call "
    "with unchanged SQL. Ask the user to clarify ambiguous metrics or date ranges. Never imply that "
    "returned data is fresher or more complete than the tool result states. All database operations "
    "exposed here are read-only. "
    "After every successful ask_data or execute_readonly_sql call, the final user-facing answer MUST "
    "include the exact executed SQL in a fenced sql block plus engine, trace_id, query_id, and row_count. "
    "Do not omit SQL unless the user explicitly asks to hide it. Pass the original user question in "
    "every execute_readonly_sql call so the audit history can bind the question to the SQL."
)

TOOLS = [
    {
        "name": "ask_data",
        "title": "Schemist",
        "description": (
            "用自然语言完成 Text2SQL、只读查询和可选数据分析。返回最终执行 SQL、引擎、追踪号和查询号；"
            "最终答复必须展示这些核验信息。问题应包含完整口径、日期范围和维度。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "完整的中文或英文业务问题。",
                    "minLength": 1,
                    "maxLength": 20000,
                },
                "engine": {
                    "type": "string",
                    "enum": ["spark", "trino"],
                    "default": "trino",
                    "description": "SQL 执行引擎；未指定时默认 Trino，用户明确要求时可选 Spark。",
                },
                "include_analysis": {
                    "type": "boolean",
                    "default": True,
                    "description": "是否在查询后生成分析报告。",
                },
                "max_rows": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 500,
                    "default": 100,
                    "description": "最多返回给 Codex 的结果行数。",
                },
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "generate_sql",
        "title": "生成 SQL",
        "description": (
            "根据自然语言问题生成 SQL 和口径说明，但绝不执行、也不返回查询数据。"
            "Codex 可按任务需要灵活使用；若用户目标是取数，生成成功后必须把 SQL 交给 "
            "execute_readonly_sql。除非有新的校验错误或缺失阶段，不要用相同参数重复生成。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "完整的业务问题。",
                    "minLength": 1,
                    "maxLength": 20000,
                },
                "engine": {
                    "type": "string",
                    "enum": ["spark", "trino"],
                    "default": "trino",
                },
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "execute_readonly_sql",
        "title": "执行只读 SQL",
        "description": (
            "执行已有的 SELECT/WITH/EXPLAIN/SHOW/DESCRIBE SQL，服务端会拒绝 DDL 和 DML。"
            "SQL 可以来自 generate_sql、Codex 的最小修复或用户输入。必须同时传入该 SQL 对应的"
            "原始用户问题，供后台审计绑定；执行失败后不得原样重复提交同一 SQL。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "该 SQL 所验证的原始用户问题，必须保持完整，不要只写“验证数据”。",
                    "minLength": 1,
                    "maxLength": 20000,
                },
                "sql": {
                    "type": "string",
                    "description": "要执行的只读 SQL。",
                    "minLength": 1,
                    "maxLength": 100000,
                },
                "engine": {
                    "type": "string",
                    "enum": ["spark", "trino"],
                    "default": "trino",
                },
                "max_rows": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 500,
                    "default": 100,
                },
            },
            "required": ["question", "sql"],
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "service_health",
        "title": "检查问数服务",
        "description": "检查Schemist服务是否在线，并返回默认 SQL 引擎。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
    },
]


def _jsonrpc_result(request_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _jsonrpc_error(request_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "dict"):
        return value.dict()
    raise TypeError("MCP handler returned an unsupported response type")


def _required_text(arguments: Dict[str, Any], field: str, max_length: int) -> str:
    value = arguments.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("{} must be a non-empty string".format(field))
    value = value.strip()
    if len(value) > max_length:
        raise ValueError("{} exceeds {} characters".format(field, max_length))
    return value


def _engine(arguments: Dict[str, Any]) -> str:
    value = arguments.get("engine", "trino")
    if value not in ("spark", "trino"):
        raise ValueError("engine must be spark or trino")
    return value


def _max_rows(arguments: Dict[str, Any]) -> int:
    value = arguments.get("max_rows", 100)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > 500:
        raise ValueError("max_rows must be an integer between 1 and 500")
    return value


def _tool_result(payload: Dict[str, Any], is_error: bool = False) -> Dict[str, Any]:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    return {
        "content": [{"type": "text", "text": text}],
        "isError": is_error,
    }


def _attach_verification(
    payload: Dict[str, Any],
    trace_id: str,
    tool_name: str,
    question: str,
    engine: str,
    final_sql: str,
) -> Dict[str, Any]:
    result = dict(payload or {})
    execution = result.get("execution") or {}
    if not isinstance(execution, dict):
        execution = {}
    query_id = result.get("query_id") or execution.get("query_id") or ""
    row_count = result.get("row_count")
    if row_count is None:
        row_count = execution.get("row_count")
    result["trace_id"] = trace_id
    result["source"] = "mcp"
    result["verification"] = {
        "tool": tool_name,
        "original_question": question,
        "engine": engine,
        "trace_id": trace_id,
        "query_id": query_id,
        "row_count": row_count,
        "final_sql": final_sql or "",
        "display_sql_in_final_answer": True,
    }
    if tool_name == "generate_sql":
        semantic_complete = bool(result.get("semantic_complete", True))
        result["workflow"] = {
            "state": "sql_generated" if semantic_complete else "sql_incomplete",
            "data_returned": False,
            "sql_executed": False,
            "allow_agent_tool_choice": True,
            "recommended_next_tool": "execute_readonly_sql" if semantic_complete else None,
            "do_not_repeat_same_call": True,
            "instruction": (
                "用户目标是取数时，可检查或最小修改 final_sql，然后调用 execute_readonly_sql；"
                "不要把 SQL 生成成功表述为查询成功。"
                if semantic_complete
                else "SQL 语义不完整，不要执行；根据 missing_stages 补充上下文后再决定下一工具。"
            ),
        }
    else:
        success = bool(result.get("success"))
        has_sql = bool(final_sql)
        result["workflow"] = {
            "state": "data_ready" if success else "query_failed",
            "data_returned": success,
            "sql_executed": success,
            "allow_agent_tool_choice": True,
            "recommended_next_tool": (
                None if success else ("execute_readonly_sql" if has_sql else "generate_sql")
            ),
            "do_not_repeat_same_call": True,
            "requires_sql_change": bool(not success and has_sql),
            "instruction": (
                "查询已执行，可依据 verification 和结果回答。"
                if success
                else (
                    "检查 error 并最小修复 final_sql 后再执行；无法可靠修复时，可带上具体错误重新生成一次。"
                    if has_sql
                    else "当前没有可执行 SQL，可补充口径后生成；不要重复完全相同的失败调用。"
                )
            ),
        }
    return result


def create_mcp_router(
    pipeline_handler: Callable[..., Awaitable[Any]],
    generate_handler: Callable[..., Awaitable[Any]],
    execute_handler: Callable[..., Awaitable[Any]],
    health_handler: Callable[[], Awaitable[Any]],
    pipeline_request_factory: Callable[..., Any],
    generate_request_factory: Callable[..., Any],
    execute_request_factory: Callable[..., Any],
) -> APIRouter:
    """Create the MCP router without importing ``app`` and creating a cycle."""
    router = APIRouter(tags=["mcp"])

    async def call_tool(name: str, arguments: Dict[str, Any], trace_id: str) -> Tuple[Dict[str, Any], bool]:
        try:
            if name == "service_health":
                payload = _as_dict(await health_handler())
                payload["service_engine_default"] = payload.get("engine_default")
                payload["engine_default"] = "trino"
                payload["trace_id"] = trace_id
                payload["source"] = "mcp"
                return payload, False
            if name == "ask_data":
                question = _required_text(arguments, "question", 20000)
                engine = _engine(arguments)
                include_analysis = arguments.get("include_analysis", True)
                if not isinstance(include_analysis, bool):
                    raise ValueError("include_analysis must be a boolean")
                request_model = pipeline_request_factory(
                    query=question,
                    engine=engine,
                    include_analyze=include_analysis,
                    max_rows=_max_rows(arguments),
                    source="mcp",
                )
                payload = _as_dict(await pipeline_handler(request_model))
                payload = _attach_verification(
                    payload, trace_id, name, question, engine, payload.get("sql") or ""
                )
                return payload, not bool(payload.get("success"))
            if name == "generate_sql":
                question = _required_text(arguments, "question", 20000)
                engine = _engine(arguments)
                request_model = generate_request_factory(
                    query=question,
                    engine=engine,
                    source="mcp",
                )
                payload = _as_dict(await generate_handler(request_model))
                payload = _attach_verification(
                    payload, trace_id, name, question, engine, payload.get("sql") or ""
                )
                return payload, not bool(payload.get("semantic_complete", True))
            if name == "execute_readonly_sql":
                question = _required_text(arguments, "question", 20000)
                sql = _required_text(arguments, "sql", 100000)
                engine = _engine(arguments)
                request_model = execute_request_factory(
                    sql=sql,
                    question=question,
                    engine=engine,
                    max_rows=_max_rows(arguments),
                    source="mcp",
                )
                payload = _as_dict(await execute_handler(request_model))
                payload = _attach_verification(payload, trace_id, name, question, engine, sql)
                return payload, not bool(payload.get("success"))
            return {"error": "Unknown tool: {}".format(name)}, True
        except ValueError as exc:
            return {"error": str(exc)}, True
        except Exception as exc:
            detail = getattr(exc, "detail", None)
            return {"error": str(detail if detail is not None else exc)}, True

    def is_authorized(request: Request) -> bool:
        expected = (os.environ.get("MCP_API_TOKEN") or "").strip()
        if not expected:
            return True
        header = request.headers.get("authorization") or ""
        prefix = "Bearer "
        if not header.startswith(prefix):
            return False
        return hmac.compare_digest(header[len(prefix):].strip(), expected)

    @router.get("/mcp")
    async def mcp_get(request: Request):
        if not is_authorized(request):
            return JSONResponse(
                {"error": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return JSONResponse(
            {"error": "This stateless MCP server accepts JSON-RPC requests with POST."},
            status_code=405,
            headers={"Allow": "POST, DELETE"},
        )

    @router.delete("/mcp")
    async def mcp_delete(request: Request):
        if not is_authorized(request):
            return JSONResponse(
                {"error": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return Response(status_code=204)

    @router.post("/mcp")
    async def mcp_post(request: Request):
        if (os.environ.get("MCP_ENABLED", "1") or "1").strip().lower() in (
            "0", "false", "no", "off"
        ):
            return JSONResponse({"error": "MCP endpoint is disabled"}, status_code=503)
        if not is_authorized(request):
            return JSONResponse(
                {"error": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        content_type = (request.headers.get("content-type") or "").lower()
        if "application/json" not in content_type:
            return JSONResponse({"error": "Content-Type must be application/json"}, status_code=415)
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(_jsonrpc_error(None, -32700, "Parse error"), status_code=400)
        if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0":
            return JSONResponse(_jsonrpc_error(None, -32600, "Invalid Request"), status_code=400)

        request_id: Optional[Any] = payload.get("id")
        method = payload.get("method")
        params = payload.get("params") or {}
        if not isinstance(method, str) or not isinstance(params, dict):
            return JSONResponse(_jsonrpc_error(request_id, -32600, "Invalid Request"))

        if request_id is None:
            return Response(status_code=202)

        if method == "initialize":
            requested_version = params.get("protocolVersion")
            protocol_version = (
                requested_version
                if requested_version in SUPPORTED_PROTOCOL_VERSIONS
                else DEFAULT_PROTOCOL_VERSION
            )
            result = {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                "instructions": SERVER_INSTRUCTIONS,
            }
            return JSONResponse(_jsonrpc_result(request_id, result))
        if method == "ping":
            return JSONResponse(_jsonrpc_result(request_id, {}))
        if method == "tools/list":
            return JSONResponse(_jsonrpc_result(request_id, {"tools": TOOLS}))
        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            if not isinstance(name, str) or not isinstance(arguments, dict):
                return JSONResponse(_jsonrpc_error(request_id, -32602, "Invalid tool arguments"))
            trace_id = "mcp_{}".format(uuid.uuid4().hex[:12])
            trace_token = set_trace_id(trace_id)
            try:
                tool_payload, is_error = await call_tool(name, arguments, trace_id)
            finally:
                reset_trace_id(trace_token)
            tool_payload.setdefault("trace_id", trace_id)
            tool_payload.setdefault("source", "mcp")
            return JSONResponse(_jsonrpc_result(request_id, _tool_result(tool_payload, is_error)))
        return JSONResponse(_jsonrpc_error(request_id, -32601, "Method not found"))

    return router
