# -*- coding: utf-8 -*-
"""
飞书开放平台 HTTP 客户端：tenant_access_token、拉取消息、回复消息、配置解析。
Schemist 飞书客户端：纯文本、交互卡片与云文档。
"""
import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

FEISHU_BASE = "https://open.feishu.cn/open-apis"
# 飞书单条文本消息不宜过长，预留余量
FEISHU_REPLY_MAX_CHARS = int(os.environ.get("FEISHU_REPLY_MAX_CHARS", "18000"))
# 交互卡片内 lark_md / v2 markdown 分段长度
FEISHU_CARD_MD_CHUNK = int(os.environ.get("FEISHU_CARD_MD_CHUNK", "3800"))
# 卡片 JSON：2.0 支持富文本 Markdown 表格与 table 组件（客户端需 ≥7.20）；1.0 为旧版 div+lark_md
FEISHU_CARD_SCHEMA = (os.environ.get("FEISHU_CARD_SCHEMA", "2") or "2").strip().lower()
# 全流程成功回复是否优先发消息卡片（失败仍用纯文本）
FEISHU_REPLY_INTERACTIVE = (os.environ.get("FEISHU_REPLY_INTERACTIVE", "1") or "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)


def get_feishu_doc_base_url() -> str:
    """Tenant doc URL base. Example: https://your-tenant.feishu.cn"""
    return (os.getenv("FEISHU_DOC_BASE_URL", "") or "https://www.feishu.cn").rstrip("/")


def _parse_feishu_content_to_text(content: dict) -> str:
    if not content:
        return ""
    text = (content.get("text") or "").strip()
    if text:
        return text
    parts = []
    for row in content.get("content") or []:
        for elem in row if isinstance(row, list) else []:
            if not isinstance(elem, dict):
                continue
            tag = elem.get("tag") or ""
            if tag == "text":
                parts.append((elem.get("text") or "").strip())
            elif tag == "at":
                name = (elem.get("user_name") or "").strip()
                if name:
                    parts.append("@{}".format(name))
    return " ".join(parts).strip()


def _strip_at_bot_prefix(text: str) -> str:
    if text.startswith("@"):
        idx = text.find(" ")
        if idx > 0:
            return text[idx + 1 :].strip()
    return text


def parse_text_from_event_message_content(event_content) -> Optional[str]:
    """从事件体 message.content（JSON 字符串或 dict）解析用户文本。"""
    if not event_content:
        return None
    try:
        content = json.loads(event_content) if isinstance(event_content, str) else event_content
        if not isinstance(content, dict):
            return None
        text = _parse_feishu_content_to_text(content)
        text = _strip_at_bot_prefix((text or "").strip())
        return text or None
    except Exception as e:
        logger.warning("parse_text_from_event_message_content failed: %s", e)
        return None


def prepare_feishu_reply_text(text: str, max_len: int = None) -> str:
    max_len = max_len if max_len is not None else FEISHU_REPLY_MAX_CHARS
    s = (text or "").strip()
    if len(s) <= max_len:
        return s if s else "(无内容)"
    return s[: max_len - 20] + "\n\n...(内容过长已截断)"


async def get_tenant_access_token(
    client: httpx.AsyncClient, app_id: str, app_secret: str
) -> Optional[str]:
    url = "{}/auth/v3/tenant_access_token/internal".format(FEISHU_BASE)
    payload = {"app_id": app_id, "app_secret": app_secret}
    try:
        r = await client.post(url, json=payload, timeout=20.0)
        data = r.json()
        if data.get("code") == 0:
            return data.get("tenant_access_token")
        logger.warning("Feishu token failed: %s", data)
        return None
    except Exception as e:
        logger.exception("get_tenant_access_token error: %s", e)
        return None


def _remove_readonly_fields(obj):
    """Remove readonly fields returned by Markdown conversion before block insertion."""
    if isinstance(obj, dict):
        obj.pop("merge_info", None)
        for v in obj.values():
            _remove_readonly_fields(v)
    elif isinstance(obj, list):
        for item in obj:
            _remove_readonly_fields(item)
    return obj


async def create_feishu_doc_from_markdown(
    client: httpx.AsyncClient,
    token: str,
    title: str,
    markdown: str,
    folder_token: str = "",
) -> dict:
    """Create a Feishu docx document and insert Markdown as converted document blocks."""
    safe_title = (title or "Schemist 数据分析报告").strip()[:800] or "Schemist 数据分析报告"
    content = (markdown or "").strip()
    if not content:
        return {"success": False, "error": "Markdown content is empty"}

    headers = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": "application/json; charset=utf-8",
    }

    create_payload = {"title": safe_title}
    if folder_token:
        create_payload["folder_token"] = folder_token.strip()
    create_url = "{}/docx/v1/documents".format(FEISHU_BASE)
    r = await client.post(create_url, headers=headers, json=create_payload, timeout=30.0)
    created = r.json()
    if created.get("code") != 0:
        logger.warning("Feishu create doc failed: %s", created)
        return {"success": False, "error": created.get("msg") or str(created)}

    document = (created.get("data") or {}).get("document") or {}
    document_id = document.get("document_id")
    if not document_id:
        return {"success": False, "error": "Feishu create doc returned no document_id"}

    convert_url = "{}/docx/v1/documents/blocks/convert".format(FEISHU_BASE)
    r = await client.post(
        convert_url,
        headers=headers,
        json={"content_type": "markdown", "content": content},
        timeout=60.0,
    )
    converted = r.json()
    if converted.get("code") != 0:
        logger.warning("Feishu markdown convert failed: %s", converted)
        return {"success": False, "error": converted.get("msg") or str(converted), "document_id": document_id}

    data = converted.get("data") or {}
    first_level_ids = data.get("first_level_block_ids") or []
    blocks = data.get("blocks") or []
    if not first_level_ids or not blocks:
        return {"success": False, "error": "Markdown converted to empty block list", "document_id": document_id}
    if len(blocks) > 1000:
        return {
            "success": False,
            "error": "Converted Markdown has too many blocks ({} > 1000); please shorten the report".format(len(blocks)),
            "document_id": document_id,
        }

    _remove_readonly_fields(blocks)
    insert_url = "{}/docx/v1/documents/{}/blocks/{}/descendant".format(FEISHU_BASE, document_id, document_id)
    r = await client.post(
        insert_url,
        headers=headers,
        params={"document_revision_id": -1},
        json={"children_id": first_level_ids, "descendants": blocks},
        timeout=60.0,
    )
    inserted = r.json()
    if inserted.get("code") != 0:
        logger.warning("Feishu insert doc blocks failed: %s", inserted)
        return {"success": False, "error": inserted.get("msg") or str(inserted), "document_id": document_id}

    return {
        "success": True,
        "document_id": document_id,
        "url": "{}/docx/{}".format(get_feishu_doc_base_url(), document_id),
        "block_count": len(blocks),
    }


async def get_message_content(
    client: httpx.AsyncClient, token: str, message_id: str
) -> Optional[str]:
    url = "{}/im/v1/messages/{}".format(FEISHU_BASE, message_id)
    headers = {"Authorization": "Bearer {}".format(token)}
    try:
        r = await client.get(url, headers=headers, timeout=20.0)
        data = r.json()
        if data.get("code") != 0:
            logger.warning("Feishu get message failed: %s", data)
            return None
        body = data.get("data", {}).get("body", {})
        content_str = body.get("content")
        if not content_str:
            logger.warning("Feishu get message body.content empty message_id=%s", message_id)
            return None
        content = json.loads(content_str) if isinstance(content_str, str) else content_str
        text = _parse_feishu_content_to_text(content)
        return _strip_at_bot_prefix(text) or None
    except Exception as e:
        logger.exception("get_message_content error: %s", e)
        return None


def _chunk_text_for_card(s: str, chunk: int) -> list:
    s = s or ""
    if chunk <= 100:
        chunk = 100
    out = []
    i = 0
    while i < len(s):
        out.append(s[i : i + chunk])
        i += chunk
    return out if out else [""]


def _feishu_column_key(header: str, index: int) -> str:
    raw = re.sub(r"[^0-9a-zA-Z_]", "_", (header or "").strip())
    raw = raw.strip("_")[:32] or "col"
    if not re.match(r"^[A-Za-z_]", raw):
        raw = "c_{}".format(raw)
    key = "{}_{}".format(raw, index) if len(raw) < 2 else raw
    return key[:40]


def preview_table_markdown_pipe(headers: List[str], rows: List[dict], n: int = 10, total: Optional[int] = None) -> str:
    """GitHub 风格 Markdown 表（用于卡片 1.0 / 文本回退）。"""
    if not headers:
        return "(无列)"
    sep = "| " + " | ".join(["---"] * len(headers)) + " |"
    head = "| " + " | ".join(headers) + " |"
    lines = [head, sep]
    for row in rows[:n]:
        vals = []
        for h in headers:
            v = row.get(h, "") if isinstance(row, dict) else ""
            s = str(v) if v is not None else ""
            if len(s) > 120:
                s = s[:117] + "..."
            vals.append(s.replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    tot = total if total is not None else len(rows)
    if tot > n:
        lines.append("\n(仅展示前 {} 行，共 {} 行)".format(n, tot))
    return "\n".join(lines)


def build_preview_table_element_v2(
    headers: List[str],
    rows: List[dict],
    max_rows: int = 10,
    element_id: str = "tblPreview",
) -> Optional[dict]:
    """
    飞书卡片 2.0 原生表格（结果预览）。列最多 50；行由 page_size 控制 [1,10]。
    """
    if not headers:
        return None
    max_rows = max(1, min(int(max_rows or 10), 10))
    headers_use = list(headers)[:50]
    keys = []
    columns = []
    for i, h in enumerate(headers_use):
        k = _feishu_column_key(str(h), i)
        base = k
        suf = 0
        while k in keys:
            suf += 1
            k = ("{}_{}".format(base, suf))[:40]
        keys.append(k)
        columns.append(
            {
                "name": k,
                "display_name": (str(h) if h is not None else "")[:80] or k,
                "width": "auto",
                "data_type": "text",
                "horizontal_align": "left",
            }
        )
    out_rows = []
    for row in rows[:max_rows]:
        if not isinstance(row, dict):
            continue
        r = {}
        for j, h in enumerate(headers_use):
            k = keys[j]
            v = row.get(h, "")
            s = str(v) if v is not None else ""
            if len(s) > 500:
                s = s[:497] + "..."
            r[k] = s
        out_rows.append(r)
    if not out_rows:
        return None
    eid = (element_id or "tblPreview").strip()
    if not re.match(r"^[A-Za-z]", eid):
        eid = "t" + eid
    eid = eid[:20]
    return {
        "tag": "table",
        "element_id": eid,
        "margin": "4px 0 8px 0",
        "page_size": max_rows,
        "row_height": "low",
        "freeze_first_column": len(columns) > 3,
        "header_style": {
            "text_align": "left",
            "text_size": "normal",
            "background_style": "grey",
            "text_color": "default",
            "bold": True,
            "lines": 1,
        },
        "columns": columns,
        "rows": out_rows,
    }


def _v2_markdown_elements(parts: List[str], id_prefix: str = "m") -> List[dict]:
    elements = []
    for i, part in enumerate(parts):
        eid = "{}{}".format(id_prefix, i)
        if not re.match(r"^[A-Za-z]", eid):
            eid = "m{}".format(i)
        elements.append(
            {
                "tag": "markdown",
                "element_id": eid[:20],
                "content": part,
                "margin": "0 0 8px 0",
                "text_align": "left",
                "text_size": "normal",
            }
        )
    return elements


def build_interactive_card_payload_v2(
    header_title: str,
    markdown_intro: str,
    footer_note: str = "完整报告与图表请登录 Web 端查看。",
    markdown_after_preview: Optional[str] = None,
    preview_table: Optional[Dict] = None,
) -> str:
    """
    飞书交互卡片 JSON 2.0：正文用 tag=markdown（支持 GFM 表格等），可选原生 table 展示结果预览。
    config.update_multi 须为 true（平台要求）。
    """
    title = (header_title or "数据分析").strip()[:200] or "数据分析"
    elements: List[dict] = []
    chunk = FEISHU_CARD_MD_CHUNK

    if preview_table and (preview_table.get("headers") or []):
        intro = prepare_feishu_reply_text(markdown_intro or "", max_len=FEISHU_REPLY_MAX_CHARS)
        elements.extend(_v2_markdown_elements(_chunk_text_for_card(intro, chunk), "mi"))
        elements.append(
            {
                "tag": "markdown",
                "element_id": "mPrevTitle",
                "content": "### 结果预览",
                "margin": "8px 0 4px 0",
                "text_size": "heading-4",
                "text_align": "left",
            }
        )
        pt = preview_table
        tbl = build_preview_table_element_v2(
            pt.get("headers") or [],
            pt.get("rows") or [],
            max_rows=int(pt.get("max_rows") or 10),
            element_id="tblPreview",
        )
        if tbl:
            elements.append(tbl)
        total = int(pt.get("total_rows") or 0)
        shown = min(int(pt.get("max_rows") or 10), len(pt.get("rows") or []))
        if total > shown:
            elements.append(
                {
                    "tag": "markdown",
                    "element_id": "mPrevNote",
                    "content": "*仅展示前 {} 行，共 {} 行。*".format(shown, total),
                    "margin": "4px 0 0 0",
                    "text_size": "notation",
                }
            )
        rest = markdown_after_preview or ""
        rest = prepare_feishu_reply_text(rest, max_len=FEISHU_REPLY_MAX_CHARS)
        elements.extend(_v2_markdown_elements(_chunk_text_for_card(rest, chunk), "mr"))
    else:
        full = markdown_intro or ""
        if markdown_after_preview:
            full = (full + "\n\n" + markdown_after_preview).strip()
        full = prepare_feishu_reply_text(full, max_len=FEISHU_REPLY_MAX_CHARS)
        elements.extend(_v2_markdown_elements(_chunk_text_for_card(full, chunk), "m"))

    fn = (footer_note or "").strip()
    if fn:
        elements.append({"tag": "hr", "element_id": "hrFooter"})
        elements.append(
            {
                "tag": "markdown",
                "element_id": "mFooter",
                "content": fn[:500],
                "text_size": "notation",
                "margin": "8px 0 0 0",
            }
        )

    card = {
        "schema": "2.0",
        "config": {
            "update_multi": True,
            "width_mode": "fill",
            "enable_forward": True,
        },
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {
            "direction": "vertical",
            "padding": "12px 12px 12px 12px",
            "elements": elements,
        },
    }
    return json.dumps(card, ensure_ascii=False)


def build_interactive_card_payload(
    header_title: str,
    markdown_body: str,
    footer_note: str = "完整报告与图表请登录 Web 端查看。",
) -> str:
    title = (header_title or "数据分析").strip()[:200] or "数据分析"
    body = prepare_feishu_reply_text(markdown_body or "", max_len=FEISHU_REPLY_MAX_CHARS)
    chunks = _chunk_text_for_card(body, FEISHU_CARD_MD_CHUNK)
    elements = []
    for part in chunks:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": part}})
    fn = (footer_note or "").strip()
    if fn:
        elements.append({"tag": "hr"})
        elements.append({"tag": "note", "elements": [{"tag": "plain_text", "content": fn[:500]}]})
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": elements,
    }
    return json.dumps(card, ensure_ascii=False)


def _use_feishu_card_v2() -> bool:
    return FEISHU_CARD_SCHEMA not in ("1", "1.0", "v1", "legacy")


async def reply_interactive_card(
    client: httpx.AsyncClient,
    token: str,
    message_id: str,
    header_title: str,
    markdown_body: str,
    footer_note: str = "完整报告与图表请登录 Web 端查看。",
    markdown_after_preview: Optional[str] = None,
    preview_table: Optional[Dict] = None,
    markdown_v1_fallback: Optional[str] = None,
) -> bool:
    url = "{}/im/v1/messages/{}/reply".format(FEISHU_BASE, message_id)
    headers = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": "application/json; charset=utf-8",
    }
    try:
        payload = None
        if _use_feishu_card_v2():
            try:
                payload = build_interactive_card_payload_v2(
                    header_title,
                    markdown_body,
                    footer_note=footer_note,
                    markdown_after_preview=markdown_after_preview,
                    preview_table=preview_table,
                )
            except Exception as e:
                logger.warning("Feishu card v2 build failed: %s", e)
                payload = None
        if payload and len(payload.encode("utf-8")) > 29000:
            # 压缩：去掉表格仅保留 Markdown（仍可能含报告内表格语法，客户端 2.0 可渲染）
            merged = (markdown_body or "").strip()
            if markdown_after_preview:
                merged = (merged + "\n\n" + markdown_after_preview).strip()
            merged = (merged or "")[:12000] + "\n\n…(卡片过大已截断)"
            try:
                payload = build_interactive_card_payload_v2(
                    header_title,
                    merged,
                    footer_note=footer_note,
                    markdown_after_preview=None,
                    preview_table=None,
                )
            except Exception:
                payload = None
        if payload and len(payload.encode("utf-8")) > 29000:
            payload = None

        if payload:
            req_body = {"msg_type": "interactive", "content": payload}
            r = await client.post(url, headers=headers, json=req_body, timeout=30.0)
            data = r.json()
            if data.get("code") == 0:
                return True
            logger.warning(
                "Feishu interactive v2 reply failed code=%s msg=%s data=%s",
                data.get("code"),
                data.get("msg"),
                data,
            )

        fb = markdown_v1_fallback
        if fb is None:
            if preview_table and markdown_after_preview is not None:
                pt = preview_table
                prev_md = preview_table_markdown_pipe(
                    pt.get("headers") or [],
                    pt.get("rows") or [],
                    n=int(pt.get("max_rows") or 10),
                    total=pt.get("total_rows"),
                )
                fb = (
                    (markdown_body or "").strip()
                    + "\n\n**结果预览**\n"
                    + prev_md
                    + "\n\n"
                    + (markdown_after_preview or "").strip()
                )
            else:
                fb = markdown_body
        payload = build_interactive_card_payload(header_title, fb or "", footer_note=footer_note)
        if len(payload.encode("utf-8")) > 29000:
            payload = build_interactive_card_payload(
                header_title,
                (fb or "")[:12000] + "\n\n…(卡片过大已截断)",
                footer_note=footer_note,
            )
        req_body = {"msg_type": "interactive", "content": payload}
        r = await client.post(url, headers=headers, json=req_body, timeout=30.0)
        data = r.json()
        if data.get("code") == 0:
            return True
        logger.warning(
            "Feishu interactive reply failed code=%s msg=%s data=%s",
            data.get("code"),
            data.get("msg"),
            data,
        )
        return False
    except Exception as e:
        logger.exception("reply_interactive_card error: %s", e)
        return False


async def reply_message(
    client: httpx.AsyncClient, token: str, message_id: str, text: str
) -> bool:
    url = "{}/im/v1/messages/{}/reply".format(FEISHU_BASE, message_id)
    headers = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": "application/json",
    }
    body = {
        "msg_type": "text",
        "content": json.dumps({"text": prepare_feishu_reply_text(text)}, ensure_ascii=False),
    }
    try:
        r = await client.post(url, headers=headers, json=body, timeout=30.0)
        data = r.json()
        if data.get("code") == 0:
            return True
        logger.warning("Feishu reply failed code=%s msg=%s data=%s", data.get("code"), data.get("msg"), data)
        return False
    except Exception as e:
        logger.exception("reply_message error: %s", e)
        return False


def get_feishu_config() -> dict:
    return {
        "app_id": os.getenv("FEISHU_APP_ID", "").strip(),
        "app_secret": os.getenv("FEISHU_APP_SECRET", "").strip(),
    }


def _parse_feishu_app_mappings_env() -> Optional[list]:
    raw = os.getenv("FEISHU_APP_MAPPINGS")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("FEISHU_APP_MAPPINGS invalid JSON: %s", e)
        return []
    if not isinstance(data, list):
        logger.error("FEISHU_APP_MAPPINGS must be a JSON array")
        return []
    return data


def _normalize_feishu_mapping_entry(m: dict) -> Optional[dict]:
    app_id = (m.get("app_id") or "").strip()
    app_secret = (m.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        return None
    return {"app_id": app_id, "app_secret": app_secret}


def resolve_feishu_config_for_event(header: Optional[dict]) -> Optional[dict]:
    header = header or {}
    header_app_id = (header.get("app_id") or "").strip()
    raw_list = _parse_feishu_app_mappings_env()
    if raw_list is not None:
        entries = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            n = _normalize_feishu_mapping_entry(item)
            if n:
                entries.append(n)
        if not entries:
            logger.error("FEISHU_APP_MAPPINGS has no valid entries")
            return None
        if not header_app_id:
            logger.warning("Feishu event header missing app_id, cannot match FEISHU_APP_MAPPINGS")
            return None
        for n in entries:
            if n["app_id"] == header_app_id:
                return n
        logger.warning("Feishu event app_id=%s not in FEISHU_APP_MAPPINGS", header_app_id)
        return None
    cfg = get_feishu_config()
    if not cfg.get("app_id") or not cfg.get("app_secret"):
        return None
    if header_app_id and cfg["app_id"] != header_app_id:
        logger.warning(
            "Feishu event app_id=%s differs from FEISHU_APP_ID=%s; still using single-app env (use FEISHU_APP_MAPPINGS for multi-app)",
            header_app_id,
            cfg["app_id"],
        )
    return cfg


def parse_engine_prefix(text: str) -> Tuple[Optional[str], str]:
    """返回 (engine 'trino'|'spark'|None, 剩余问题)."""
    if not text or not text.strip():
        return None, (text or "").strip()
    t = text.strip()
    m = re.match(r"^(?:引擎|engine)\s*[:：]\s*(trino|spark)\s+(.+)$", t, re.IGNORECASE)
    if m:
        return m.group(1).lower(), m.group(2).strip()
    return None, t


def parse_pipeline_mode(text: str) -> Tuple[str, str]:
    """
    返回 (mode, 剩余问题).
    mode: 'full' = 生成 SQL → 执行 → 分析报告；'sql_only' = 仅生成 SQL。
    默认 full（飞书侧「自动执行全流程」）；环境变量 FEISHU_BOT_DEFAULT_PIPELINE=sql_only 可改为默认仅 SQL。
    """
    default_full = (os.getenv("FEISHU_BOT_DEFAULT_PIPELINE", "full") or "full").strip().lower() != "sql_only"
    default_mode = "full" if default_full else "sql_only"
    t = text.strip()
    for pat, mode in (
        (r"^(?:仅SQL|只生成SQL|只要SQL|只生成)\s+", "sql_only"),
        (r"^(?:全流程|自动执行|自动)\s+", "full"),
    ):
        m = re.match(pat, t, re.IGNORECASE)
        if m:
            rest = t[m.end() :].strip()
            if rest:
                return mode, rest
    return default_mode, t
