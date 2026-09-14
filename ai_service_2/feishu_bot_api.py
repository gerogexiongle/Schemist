# -*- coding: utf-8 -*-
"""
飞书机器人事件回调：接收用户消息 → 生成 SQL →（可选）执行 →（可选）分析报告 → 回复飞书。
飞书事件订阅适配 FastAPI + Schemist 全流程。

事件订阅 URL: https://你的域名/feishu/event
飞书要求约 3 秒内返回 200，故先返回 ok，再在后台 asyncio.create_task 执行。
技能：显式 使用技能+名称或 id；或 FEISHU_AUTO_SKILL_MATCH 开启时按元数据与规则自适应选技能（FEISHU_AUTO_SKILL_MIN_SCORE 等）。
"""
import asyncio
import html
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from agents.analysis_agent import analyze_data
from agents.sql_agent import generate_sql
from config.settings import QUERY_RESULT_MAX_ROWS, SQL_ENGINE_DEFAULT, SQL_EXECUTOR_TIMEOUT
from skills.feishu_client import (
    FEISHU_REPLY_INTERACTIVE,
    create_feishu_doc_from_markdown,
    get_message_content,
    get_tenant_access_token,
    parse_engine_prefix,
    parse_pipeline_mode,
    parse_text_from_event_message_content,
    preview_table_markdown_pipe,
    reply_interactive_card,
    reply_message,
    resolve_feishu_config_for_event,
)
from skills.sql_pipeline import run_sql_pipeline, DEFAULT_MAX_RETRIES
from skills.sql_execution_errors import get_execution_account
from skills.pipeline_trace import (
    log as ptrace,
    new_feishu_trace_id,
    remember_feishu_pipeline_start,
    reset_trace_id,
    run_sync_in_executor,
    set_trace_id,
)

logger = logging.getLogger("feishu_bot")

router = APIRouter(tags=["feishu"])

_feishu_dedup_lock = asyncio.Lock()
_feishu_recent_event_keys: Dict[str, float] = {}
_FEISHU_DEDUP_TTL_SEC = 180.0

_history_lock = asyncio.Lock()
_feishu_sql_history: Dict[str, List[dict]] = {}
_FEISHU_HISTORY_MAX_MESSAGES = 10

_query_recorder: Optional[Callable[..., None]] = None
_shared_report_persister: Optional[Callable[..., dict]] = None
_shared_table_builder: Optional[Callable[..., str]] = None
_shared_markdown_renderer: Optional[Callable[..., str]] = None


def set_query_recorder(recorder: Callable[..., None]) -> None:
    """Bind Feishu history writes to the running application's history store."""
    global _query_recorder
    _query_recorder = recorder


def set_shared_report_services(
    persister: Callable[..., dict],
    table_builder: Callable[..., str],
    markdown_renderer: Callable[..., str],
) -> None:
    """Bind report helpers without importing the application module at runtime."""
    global _shared_report_persister, _shared_table_builder, _shared_markdown_renderer
    _shared_report_persister = persister
    _shared_table_builder = table_builder
    _shared_markdown_renderer = markdown_renderer


def _record_query_safely(**kwargs) -> bool:
    if not callable(_query_recorder):
        logger.warning("Feishu query history recorder is not configured")
        return False
    try:
        _query_recorder(**kwargs)
        return True
    except Exception as exc:
        logger.exception("Feishu query history write failed: %s", exc)
        return False


async def _feishu_claim_delivery(dedup_key: str) -> bool:
    if not dedup_key or not str(dedup_key).strip():
        return True
    key = str(dedup_key).strip()
    async with _feishu_dedup_lock:
        now = time.time()
        expired = [k for k, t in _feishu_recent_event_keys.items() if now - t > _FEISHU_DEDUP_TTL_SEC]
        for k in expired:
            del _feishu_recent_event_keys[k]
        if key in _feishu_recent_event_keys:
            logger.info("Feishu dedup skip key=%s", key[:80])
            return False
        _feishu_recent_event_keys[key] = now
        return True


def _session_chat_id(app_id: str, chat_id: str) -> str:
    app_safe = (app_id or "").strip().replace("/", "_")
    raw = (chat_id or "").strip()
    if raw and app_safe:
        return "feishu_{}_{}".format(app_safe, raw)
    if raw:
        return "feishu_{}".format(raw)
    return "feishu_orphan_{}".format(uuid.uuid4().hex[:12])


async def _get_history(session_key: str) -> List[dict]:
    async with _history_lock:
        return list(_feishu_sql_history.get(session_key) or [])


async def _append_history(session_key: str, user_text: str, assistant_text: str):
    async with _history_lock:
        h = _feishu_sql_history.setdefault(session_key, [])
        h.append({"role": "user", "content": user_text[:8000]})
        h.append({"role": "assistant", "content": assistant_text[:12000]})
        if len(h) > _FEISHU_HISTORY_MAX_MESSAGES:
            del h[: len(h) - _FEISHU_HISTORY_MAX_MESSAGES]


def _feishu_auto_skill_match_enabled() -> bool:
    v = (os.environ.get("FEISHU_AUTO_SKILL_MATCH", "1") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _feishu_auto_skill_exclude_ids() -> frozenset:
    raw = os.environ.get("FEISHU_AUTO_SKILL_EXCLUDE_IDS", "data-insights")
    return frozenset(s.strip() for s in (raw or "").split(",") if s.strip())


def _skill_auto_score(skill_meta: dict, query: str) -> int:
    """
    根据技能元数据与用户输入做轻量打分（子串 / 标签 / 描述片段 + 少量规则）。
    分高表示更像该技能场景；不参与自动匹配的技能在 exclude 集合中恒为 0。
    """
    q = (query or "").strip()
    if not q:
        return 0
    sid = (skill_meta.get("id") or "").strip()
    if sid in _feishu_auto_skill_exclude_ids():
        return 0
    score = 0
    name = (skill_meta.get("name") or "").strip()
    if len(name) >= 2 and name in q:
        score += 130 + min(len(name), 50)
    if sid and len(sid) >= 3 and sid in q:
        score += 95
    tag_hits = 0
    for tag in skill_meta.get("tags") or []:
        tg = (tag or "").strip()
        if len(tg) >= 2 and tg in q:
            tag_hits += 1
    score += min(tag_hits * 22, 66)
    desc = (skill_meta.get("description") or "").strip()
    if desc:
        for p in re.split(r"[,，。、;；\s\n]+", desc):
            p = p.strip()
            if 3 <= len(p) <= 28 and p in q:
                score += 14
    score = min(score, 260)

    if sid == "sql-rewrite":
        if re.search(r"(改写|重写|优化).{0,20}(sql|SQL)", q, re.I):
            score += 110
        if "SELECT" in q.upper() and any(k in q for k in ("改成", "改为", "转Trino", "转 Spark", "兼容")):
            score += 75
    if sid == "schema-exploration":
        if any(k in q for k in ("哪些表", "什么表", "库表", "字段有哪些", "表有哪些", "表结构", "元数据", "找表", "定位表")):
            score += 95
    return score


def _pick_best_auto_skill_id(reg, query: str) -> Optional[Tuple[str, int]]:
    """返回 (skill_id, score) 或 None（分差不足 / 低于阈值时不自动套技能）。"""
    if not _feishu_auto_skill_match_enabled():
        return None
    items = reg.list(include_disabled=False)
    scored = []
    for it in items:
        s = _skill_auto_score(it, query)
        if s > 0:
            sid = (it.get("id") or "").strip()
            if sid:
                scored.append((sid, s))
    scored.sort(key=lambda x: -x[1])
    if not scored:
        return None
    best_id, best_s = scored[0]
    second_s = scored[1][1] if len(scored) > 1 else 0
    try:
        min_s = int(os.environ.get("FEISHU_AUTO_SKILL_MIN_SCORE", "65"))
    except ValueError:
        min_s = 65
    try:
        margin = int(os.environ.get("FEISHU_AUTO_SKILL_MARGIN", "12"))
    except ValueError:
        margin = 12
    if best_s < min_s:
        logger.info(
            "Feishu auto skill below min_score top=%s/%s min=%s (runner=%s)",
            best_id,
            best_s,
            min_s,
            second_s,
        )
        return None
    if second_s > 0 and (best_s - second_s) < margin:
        logger.info(
            "Feishu auto skill ambiguous top=%s/%s runner=%s margin_need=%s",
            best_id,
            best_s,
            second_s,
            margin,
        )
        return None
    return best_id, best_s


def _compose_query_with_skills_pack(text: str, engine: str) -> Tuple[str, Optional[str]]:
    """
    1) 显式：以「使用技能」开头，紧接技能显示名或 id（与 Web 下拉一致）。
    2) 自适应：按名称 / id / 标签 / 描述子串 + 规则打分，取分差足够的最高分技能（见 FEISHU_AUTO_SKILL_*）。
       默认不参与自动匹配：data-insights（过泛），可通过 FEISHU_AUTO_SKILL_EXCLUDE_IDS 调整。
    """
    raw = (text or "").strip()
    if not raw:
        return raw, None
    try:
        from skills_pack import get_registry
        from skills_pack.registry import render_template
    except Exception as e:
        logger.warning("skills_pack import failed: %s", e)
        return raw, None
    reg = get_registry()
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    if re.match(r"^\s*使用技能\s*", raw, flags=re.IGNORECASE):
        rest = re.sub(r"^\s*使用技能\s*", "", raw, count=1, flags=re.IGNORECASE).strip()
        if not rest:
            return raw, None
        items = reg.list(include_disabled=False)
        candidates = []
        for it in items:
            sid = (it.get("id") or "").strip()
            name = (it.get("name") or "").strip()
            if name:
                candidates.append((name, sid))
            if sid and sid != name:
                candidates.append((sid, sid))
        candidates.sort(key=lambda x: len(x[0]), reverse=True)
        for needle, sid in candidates:
            if rest.startswith(needle):
                remainder = rest[len(needle) :].strip()
                try:
                    skill = reg.get(sid)
                except Exception:
                    continue
                body = skill.get("body") or ""
                variables = {
                    "user_query": remainder if remainder else raw,
                    "engine": engine or "",
                    "today": today,
                    "yesterday": yesterday,
                }
                rendered = render_template(body, variables)
                logger.info("Feishu skill applied id=%s remainder_len=%s", sid, len(remainder))
                return rendered, sid
        logger.info("Feishu 使用技能 未匹配任何技能，尝试自适应匹配")

    picked = _pick_best_auto_skill_id(reg, raw)
    if picked:
        sid, sc = picked
        try:
            skill = reg.get(sid)
            if skill.get("enabled") is False:
                return raw, None
            body = skill.get("body") or ""
            variables = {
                "user_query": raw,
                "engine": engine or "",
                "today": today,
                "yesterday": yesterday,
            }
            rendered = render_template(body, variables)
            logger.info("Feishu auto-matched skill id=%s score=%s", sid, sc)
            return rendered, sid
        except Exception as e:
            logger.warning("Feishu auto skill render failed: %s", e)

    return raw, None


async def _reply_text_or_interactive_card(
    client: httpx.AsyncClient,
    token: str,
    message_id: str,
    header_title: str,
    markdown_body: str,
    text_fallback: str,
    markdown_after_preview: Optional[str] = None,
    preview_table: Optional[dict] = None,
    markdown_v1_fallback: Optional[str] = None,
) -> None:
    if FEISHU_REPLY_INTERACTIVE:
        ok = await reply_interactive_card(
            client,
            token,
            message_id,
            header_title,
            markdown_body,
            markdown_after_preview=markdown_after_preview,
            preview_table=preview_table,
            markdown_v1_fallback=markdown_v1_fallback,
        )
        if ok:
            return
        logger.info("Feishu interactive card failed, fallback to text reply")
    await reply_message(client, token, message_id, text_fallback)


def _preview_table_markdown(headers: List[str], rows: List[dict], n: int = 10) -> str:
    return preview_table_markdown_pipe(headers, rows, n=n, total=len(rows) if rows else None)


def _feishu_run_analyze(original_question: str, sql_txt: str, hdrs: List[str], rows: List[dict]):
    return analyze_data(
        original_question=original_question,
        sql=sql_txt,
        headers=hdrs,
        data=rows,
        chart_type=None,
        max_rows=QUERY_RESULT_MAX_ROWS,
    )


async def _process_feishu_message(
    message_id: str,
    chat_id: str,
    config: dict,
    header_app_id: str,
    event_content: Optional[str] = None,
    trace_id: Optional[str] = None,
):
    tid = (trace_id or "").strip() or new_feishu_trace_id(message_id or "")
    trace_tok = set_trace_id(tid)
    logger.info("Feishu background start message_id=%s trace_id=%s", message_id, tid)
    ptrace(logger, "feishu.pipeline.enter", message_id=message_id, chat_id=(chat_id or "")[:40], app_id=(header_app_id or "")[:24])
    loop = asyncio.get_event_loop()
    session_key = _session_chat_id(header_app_id, chat_id)

    try:
        async with httpx.AsyncClient() as client:
            token = await get_tenant_access_token(client, config["app_id"], config["app_secret"])
            if not token:
                logger.warning("Feishu get_tenant_access_token failed")
                ptrace(logger, "feishu.phase.token_fail", message_id=message_id)
                return
            ptrace(logger, "feishu.phase.token_ok", message_id=message_id)

            query = None
            if event_content:
                query = parse_text_from_event_message_content(event_content)
                if query:
                    logger.info("Feishu query from event body len=%s", len(query))
            if not query:
                query = await get_message_content(client, token, message_id)
            logger.info("Feishu final query preview=%r", (query or "")[:200])

            if not query or not query.strip():
                hint = (
                    "请直接发送自然语言分析需求。\n\n"
                    "默认：**全流程**（生成 SQL → 执行 → 分析报告）。\n"
                    "· 仅生成 SQL：句首加 **仅SQL** 或 **只生成SQL** 再加空格和你的问题。\n"
                    "· 指定引擎：**引擎:trino** 或 **引擎:spark** 加空格再写问题（默认见服务配置）。\n"
                    "· 显式全流程：句首可加 **全流程** 或 **自动执行** 空格后写问题。\n"
                    "· 与 Web 端一致的技能模板：以 **使用技能** 开头，紧接技能 **显示名** 或 **技能 id**，"
                    "空格后写参数（与前台选择技能后输入的内容等价）。\n"
                    "· 若事件里拿不到正文且本接口拉消息失败，请在开放平台为应用开通 **im:message** 或 **im:message:readonly**。\n"
                )
                await reply_message(client, token, message_id, hint)
                ptrace(logger, "feishu.pipeline.early", reason="empty_query", message_id=message_id)
                return

            full_user = query.strip()
            eng_override, rest = parse_engine_prefix(full_user)
            mode, question = parse_pipeline_mode(rest)
            engine = eng_override or SQL_ENGINE_DEFAULT
            ptrace(
                logger,
                "feishu.phase.parsed",
                mode=mode,
                engine=engine,
                question_chars=len(question or ""),
            )

            if not question.strip():
                await reply_message(client, token, message_id, "问题内容为空，请重新输入。")
                return

            question_for_llm, feishu_skill_id = _compose_query_with_skills_pack(question.strip(), engine)
            if not (question_for_llm or "").strip():
                await reply_message(client, token, message_id, "问题内容为空，请重新输入。")
                return

            history = await _get_history(session_key)
            ptrace(logger, "feishu.phase.skill", skill_id=feishu_skill_id or "-", history_msgs=len(history or []))

            if mode == "sql_only":
                ptrace(logger, "feishu.phase.generate_sql.start", engine=engine)
                try:
                    gen = await run_sync_in_executor(
                        loop,
                        generate_sql,
                        question_for_llm.strip(),
                        history or None,
                        engine=engine,
                    )
                except Exception as e:
                    logger.exception("Feishu generate_sql error: %s", e)
                    ptrace(logger, "feishu.phase.generate_sql.fail", err=str(e)[:180])
                    await reply_message(client, token, message_id, "生成 SQL 失败：{}".format(str(e)[:2000]))
                    return

                ptrace(
                    logger,
                    "feishu.phase.generate_sql.done",
                    sql_chars=len(gen.get("sql") or ""),
                    tables=len(gen.get("tables_used") or []),
                    sec=gen.get("query_time"),
                )
                sql = (gen.get("sql") or "").strip()
                explanation = gen.get("explanation") or ""
                if not sql:
                    await reply_message(
                        client,
                        token,
                        message_id,
                        "未能从模型输出中解析出 SQL。\n说明：\n{}".format(explanation[:4000] or "(无)"),
                    )
                    return
                body = "**引擎**: {}\n\n**SQL**\n```sql\n{}\n```\n\n**说明**\n{}".format(
                    engine, sql, explanation[:8000] or "(无)"
                )
                card_title = "🧩 SQL 生成"
                if feishu_skill_id:
                    card_title = "🧩 SQL 生成（技能: {}）".format(feishu_skill_id)
                await _reply_text_or_interactive_card(
                    client, token, message_id, card_title, body, body
                )
                await _append_history(
                    session_key,
                    full_user,
                    "【仅SQL】\n```sql\n{}\n```\n{}".format(sql[:4000], explanation[:2000]),
                )
                _record_query_safely(
                    query_type="generate",
                    engine=engine,
                    question=question_for_llm.strip(),
                    sql=sql,
                    success=True,
                    duration=gen.get("query_time", 0),
                    tables=gen.get("tables_used") or [],
                    source="feishu",
                    trace_id=tid,
                )
                ptrace(logger, "feishu.pipeline.branch", branch="sql_only")
                return

            ptrace(logger, "feishu.phase.pipeline.start", engine=engine, max_retries=DEFAULT_MAX_RETRIES)
            try:
                pr = await run_sync_in_executor(
                    loop,
                    run_sql_pipeline,
                    question_for_llm.strip(),
                    history=history or None,
                    engine=engine,
                    max_retries=DEFAULT_MAX_RETRIES,
                    max_rows=QUERY_RESULT_MAX_ROWS,
                    timeout=SQL_EXECUTOR_TIMEOUT,
                    include_analyze=False,
                )
            except Exception as e:
                logger.exception("Feishu run_sql_pipeline error: %s", e)
                ptrace(logger, "feishu.phase.pipeline.fail", err=str(e)[:180])
                await reply_message(client, token, message_id, "全流程执行异常：{}".format(str(e)[:2000]))
                return

            sql = (pr.get("sql") or "").strip()
            explanation = pr.get("explanation") or ""
            gen = {
                "query_time": pr.get("generate_time") or 0,
                "tables_used": pr.get("tables_used") or [],
            }
            exec_block = pr.get("execution") or {}
            attempts = pr.get("attempts") or []

            ptrace(
                logger,
                "feishu.phase.pipeline.done",
                ok=pr.get("success"),
                sql_chars=len(sql),
                attempts=len(attempts),
                rows=exec_block.get("row_count") or 0,
                sec=pr.get("pipeline_time"),
            )

            if not sql:
                await reply_message(
                    client,
                    token,
                    message_id,
                    "未能从模型输出中解析出 SQL。\n说明：\n{}".format(explanation[:4000] or "(无)"),
                )
                return

            if not pr.get("success"):
                err = pr.get("error") or exec_block.get("error") or "unknown"
                exec_di = (exec_block.get("debug_info") or {}) if isinstance(exec_block, dict) else {}
                is_perm = (
                    pr.get("error_kind") == "permission_denied"
                    or exec_di.get("error_kind") == "permission_denied"
                    or "【权限不足" in (err or "")
                )
                if is_perm:
                    title = "**权限不足 — 已停止执行**"
                    exec_account = get_execution_account(engine)
                    retry_note = (
                        "\n\n请为执行账号 **{}** 申请上述表的 **SELECT** 权限，审批通过后重新提问。"
                    ).format(exec_account)
                else:
                    title = "**全流程执行失败**"
                    retry_note = ""
                    if attempts and len(attempts) > 1:
                        retry_note = "\n\n**自动修复重试**: 共 {} 次尝试".format(len(attempts))
                await reply_message(
                    client,
                    token,
                    message_id,
                    "{}\n{}\n\n**引擎** {}{}\n\n**SQL**\n```sql\n{}\n```".format(
                        title, (err or "unknown")[:4000], engine, retry_note, sql[:8000]
                    ),
                )
                _record_query_safely(
                    query_type="execute",
                    engine=engine,
                    question=question_for_llm.strip(),
                    sql=sql,
                    success=False,
                    duration=exec_block.get("execution_time") or 0,
                    error=err or "",
                    row_count=0,
                    source="feishu",
                    query_id=exec_block.get("query_id") or "",
                    trace_id=tid,
                )
                ptrace(logger, "feishu.pipeline.early", reason="pipeline_failed", err=(err or "")[:80])
                return

            headers = exec_block.get("headers") or []
            results = exec_block.get("result") or []
            exec_time = exec_block.get("execution_time") or 0
            qid = exec_block.get("query_id")

            preview = _preview_table_markdown(headers, results, n=10)
            ptrace(logger, "feishu.phase.analyze.start", rows=len(results), cols=len(headers or []))
            try:
                ar = await run_sync_in_executor(
                    loop,
                    _feishu_run_analyze,
                    question_for_llm.strip(),
                    sql,
                    headers,
                    results,
                )
            except Exception as e:
                logger.exception("Feishu analyze_data error: %s", e)
                ptrace(logger, "feishu.phase.analyze.fail", err=str(e)[:180])
                body = (
                    "**全流程（执行成功，分析阶段失败）**\n引擎: {}\n\n**SQL**\n```sql\n{}\n```\n\n**结果预览**\n{}\n\n**错误** {}".format(
                        engine, sql[:6000], preview, str(e)[:2000]
                    )
                )
                await reply_message(client, token, message_id, body)
                return

            ptrace(
                logger,
                "feishu.phase.analyze.done",
                ok=bool(ar.get("success")),
                sec=ar.get("analysis_time"),
                report_chars=len(ar.get("report") or ""),
            )
            report = (ar.get("report") or "").strip()
            if not ar.get("success"):
                report = "**分析未成功**\n{}\n\n---\n{}".format((ar.get("error") or "")[:2000], report)

            share_line = ""
            feishu_doc_line = ""
            feishu_share_id = ""
            try:
                from config.settings import SERVICE_PUBLIC_ORIGIN

                if not all(
                    callable(service)
                    for service in (
                        _shared_report_persister,
                        _shared_table_builder,
                        _shared_markdown_renderer,
                    )
                ):
                    raise RuntimeError("Feishu shared report services are not configured")
                th = _shared_table_builder(
                    headers,
                    results,
                    max_rows=min(500, max(len(results), 0) or 1),
                )
                try:
                    rh = _shared_markdown_renderer(report if report else "")
                except Exception as md_err:
                    logger.warning("Feishu share markdown render failed, fallback to escaped text: %s", md_err)
                    rh = '<div class="feishu-shared-md" style="white-space:pre-wrap;">{}</div>'.format(
                        html.escape(report if report else "")
                    )
                pr = _shared_report_persister(
                    report_html=rh,
                    question=question_for_llm.strip()[:8000],
                    sql=sql[:200000],
                    engine=engine,
                    chart_image="",
                    table_html=th,
                )
                if pr.get("share_id") and not pr.get("error"):
                    feishu_share_id = pr["share_id"]
                    path = pr.get("share_url") or ""
                    origin = (SERVICE_PUBLIC_ORIGIN or "").strip().rstrip("/")
                    full_u = "{}{}".format(origin, path) if origin else path
                    share_line = "\n\n---\n**网页分享报告**：\n{}\n（约 7 天内有效，浏览器打开）".format(full_u)
                    logger.info("Feishu share report created share_id=%s", feishu_share_id)
                    ptrace(logger, "feishu.phase.share", share_id=feishu_share_id)
                else:
                    logger.warning("Feishu share report not created: %s", pr.get("error"))
            except Exception as ex:
                logger.warning("Feishu share report failed: %s", ex)

            if report:
                try:
                    folder_token = (os.getenv("FEISHU_DOC_FOLDER_TOKEN", "") or "").strip()
                    q_prefix = question_for_llm.strip()[:60] or "Schemist 数据分析报告"
                    doc_title = "{} - {}".format(
                        q_prefix,
                        datetime.now().strftime("%Y-%m-%d %H:%M"),
                    )
                    doc_result = await create_feishu_doc_from_markdown(
                        client=client,
                        token=token,
                        title=doc_title,
                        markdown=report,
                        folder_token=folder_token,
                    )
                    if doc_result.get("success") and doc_result.get("url"):
                        feishu_doc_line = (
                            "\n\n**飞书云文档**：\n{}\n（飞书内打开）".format(doc_result["url"])
                        )
                        ptrace(
                            logger,
                            "feishu.phase.feishu_doc",
                            document_id=(doc_result.get("document_id") or "")[:24],
                        )
                    else:
                        logger.warning(
                            "Feishu doc not created: %s",
                            doc_result.get("error") or "unknown",
                        )
                except Exception as doc_ex:
                    logger.warning("Feishu doc creation failed: %s", doc_ex)

            intro = (
                "**Schemist 全流程完成**（生成 → 执行 → 分析）\n"
                "引擎: {}\n行数: {}\n耗时(执行): {:.2f}s\n\n"
                "**SQL**\n```sql\n{}\n```\n\n"
                "**模型说明**\n{}\n\n"
            ).format(
                engine,
                len(results),
                exec_time,
                sql[:12000],
                explanation[:3000] or "(无)",
            )
            report_section = "**分析报告**\n{}".format(
                report[:14000] if report else "(无报告正文)"
            ) + share_line + feishu_doc_line
            body = intro + "**结果预览**\n" + preview + "\n\n" + report_section

            card_title = "🧩 数据分析"
            if feishu_skill_id:
                card_title = "🧩 数据分析（技能: {}）".format(feishu_skill_id)
            ptrace(logger, "feishu.phase.reply_card", interactive=FEISHU_REPLY_INTERACTIVE)
            await _reply_text_or_interactive_card(
                client,
                token,
                message_id,
                card_title,
                intro,
                body,
                markdown_after_preview=report_section,
                preview_table={
                    "headers": headers,
                    "rows": results,
                    "max_rows": 10,
                    "total_rows": len(results),
                },
                markdown_v1_fallback=body,
            )

            hist_assistant = (
                "【SQL】\n```sql\n{}\n```\n【执行】行数={} 成功\n【分析摘要】\n{}".format(
                    sql[:3500],
                    len(results),
                    report[:2500] if report else "",
                )
            )
            await _append_history(session_key, full_user, hist_assistant)

            _record_query_safely(
                query_type="generate",
                engine=engine,
                question=question_for_llm.strip(),
                sql=sql,
                success=True,
                duration=gen.get("query_time", 0),
                tables=gen.get("tables_used") or [],
                source="feishu",
                trace_id=tid,
            )
            _record_query_safely(
                query_type="execute",
                engine=engine,
                question=question_for_llm.strip(),
                sql=sql,
                success=True,
                duration=exec_time,
                row_count=len(results),
                source="feishu",
                query_id=qid or "",
                trace_id=tid,
            )

            ptrace(
                logger,
                "feishu.pipeline.done",
                rows=len(results),
                skill_id=feishu_skill_id or "-",
                share_id=feishu_share_id or "-",
                message_id=message_id or "-",
            )
            logger.info(
                "Feishu pipeline done message_id=%s rows=%s skill=%s share=%s",
                message_id,
                len(results),
                feishu_skill_id or "-",
                feishu_share_id or "-",
            )

    except Exception as e:
        logger.exception("Feishu _process_feishu_message error: %s", e)
        ptrace(logger, "feishu.pipeline.exception", err=str(e)[:220])
    finally:
        reset_trace_id(trace_tok)


@router.get("/feishu/event")
async def feishu_event_get():
    return JSONResponse(
        {"msg": "Feishu callback endpoint. Configure POST im.message.receive_v1 to this URL."},
        status_code=200,
    )


@router.post("/feishu/event")
async def feishu_event(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}

    event_type = (body.get("header") or {}).get("event_type") or body.get("type") or "unknown"
    logger.info("Feishu POST /feishu/event type=%s", event_type)

    if body.get("type") == "url_verification":
        return JSONResponse({"challenge": body.get("challenge", "")})

    header = body.get("header") or {}
    if header.get("event_type") != "im.message.receive_v1":
        return JSONResponse({"msg": "ignored"}, status_code=200)

    config = resolve_feishu_config_for_event(header)
    if not config or not config.get("app_id") or not config.get("app_secret"):
        logger.warning("Feishu not configured (FEISHU_APP_ID / FEISHU_APP_SECRET or FEISHU_APP_MAPPINGS)")
        return JSONResponse({"msg": "not configured"}, status_code=200)

    event = body.get("event") or {}
    message = event.get("message") or {}
    message_id = message.get("message_id")
    chat_id = message.get("chat_id") or ""
    if not message_id:
        return JSONResponse({"msg": "no message_id"}, status_code=200)

    dedup_key = (header.get("event_id") or "").strip() or str(message_id)
    if not await _feishu_claim_delivery(dedup_key):
        return JSONResponse({"msg": "ok"}, status_code=200)

    event_content = message.get("content")
    header_app_id = (header.get("app_id") or config.get("app_id") or "").strip()

    fs_tid = new_feishu_trace_id(str(message_id))
    ptrace(
        logger,
        "feishu.webhook.enqueue",
        tid=fs_tid,
        message_id=message_id,
        chat_id=(chat_id or "")[:40],
        dedup=str(dedup_key)[:48],
    )
    remember_feishu_pipeline_start(
        fs_tid,
        message_id=str(message_id or ""),
        chat_id=str(chat_id or "")[:60],
        dedup=str(dedup_key)[:60],
    )
    asyncio.create_task(
        _process_feishu_message(
            message_id,
            chat_id,
            config,
            header_app_id,
            event_content=event_content,
            trace_id=fs_tid,
        )
    )
    return JSONResponse({"msg": "ok"}, status_code=200)
