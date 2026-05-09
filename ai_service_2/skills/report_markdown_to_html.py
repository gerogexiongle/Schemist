# -*- coding: utf-8 -*-
"""
与 Web 端「数据分析报告」展示对齐：Markdown → HTML（含表格围栏展开、样式清理、表格滚动容器）。
供飞书全流程分享、以及任意需与 index.html parseMarkdown 效果接近的服务端场景使用。
"""
import logging
import re

logger = logging.getLogger(__name__)

_UNWRAP_FENCE_RE = re.compile(r"```(\w*)\r?\n([\s\S]*?)```", re.MULTILINE)
_SEP_ROW_RE = re.compile(r"^\|?[\s\-:|]+\|\s*$")
_TABLE_RE = re.compile(r"(<table\b[^>]*>[\s\S]*?</table>)", re.IGNORECASE)


def unwrap_markdown_table_code_fences(md: str) -> str:
    """与 index.html unwrapMarkdownTableCodeFences 一致：误包在 ``` 里的管道表拆出再解析。"""
    if not md:
        return md

    def repl(m: re.Match) -> str:
        lang = m.group(1) or ""
        inner = m.group(2) or ""
        body = inner.replace("\r\n", "\n")
        low = lang.lower()
        if low in ("markdown", "md"):
            return "\n" + body.strip() + "\n"
        lines = [ln.strip() for ln in body.split("\n") if ln.strip()]
        if len(lines) < 2:
            return m.group(0)
        has_sep = any(_SEP_ROW_RE.match(ln) for ln in lines)
        pipe_rows = [ln for ln in lines if "|" in ln and ln.lstrip().startswith("|")]
        if has_sep and len(pipe_rows) >= 2:
            return "\n" + body.strip() + "\n"
        return m.group(0)

    return _UNWRAP_FENCE_RE.sub(repl, md)


def sanitize_report_rendered_html(html: str) -> str:
    """与 index.html sanitizeReportRenderedHtml 一致。"""
    if not html:
        return ""
    s = html
    s = re.sub(r'\sstyle\s*=\s*["\'][^"\']*["\']', "", s, flags=re.I)
    s = re.sub(r'\sbgcolor\s*=\s*["\'][^"\']*["\']', "", s, flags=re.I)
    s = re.sub(r"<font\b[^>]*>", "", s, flags=re.I)
    s = re.sub(r"</font>", "", s, flags=re.I)
    return s


def enhance_report_tables_html(html: str) -> str:
    """与 index.html enhanceReportTables 等效：每张表外包 report-table-scroll。"""

    def wrap_one(m: re.Match) -> str:
        block = m.group(1)
        if "report-table-scroll" in block.lower():
            return block
        return '<div class="report-table-scroll">{}</div>'.format(block)

    if not html or "<table" not in html.lower():
        return html
    if "report-table-scroll" in html.lower():
        return html
    return _TABLE_RE.sub(wrap_one, html)


def markdown_to_report_fragment_html(markdown_src: str, max_chars: int = 400000) -> str:
    """
    将分析 Agent 返回的 Markdown 转为与 Web 报告区相近的 HTML 片段（不含外层 report-body）。
    优先使用 Python-Markdown（与 Web 的简化 parseMarkdown 在表格/标题/围栏上等价度较高）；
    若未安装 markdown 包则回退为转义纯文本（旧行为）。
    """
    cap = max(1, int(max_chars or 400000))
    text = (markdown_src or "")[:cap]
    text = unwrap_markdown_table_code_fences(text)
    try:
        import markdown as md_lib
    except Exception as e:
        logger.warning("markdown import failed, fallback to escaped text: %s", e)
        import html as html_mod
        return '<div class="feishu-shared-md" style="white-space:pre-wrap;">{}</div>'.format(html_mod.escape(text))

    try:
        html_body = md_lib.markdown(
            text,
            extensions=[
                "markdown.extensions.extra",
                "markdown.extensions.nl2br",
            ],
        )
    except Exception as e:
        # Some older runtime combos fail on extension discovery
        # (e.g. "'EntryPoints' object has no attribute 'get'").
        logger.warning("markdown with extensions failed, fallback to basic markdown: %s", e)
        try:
            html_body = md_lib.markdown(text)
        except Exception as e2:
            logger.warning("basic markdown render failed, fallback to escaped text: %s", e2)
            import html as html_mod
            return '<div class="feishu-shared-md" style="white-space:pre-wrap;">{}</div>'.format(
                html_mod.escape(text)
            )
    html_body = sanitize_report_rendered_html(html_body)
    html_body = enhance_report_tables_html(html_body)
    return html_body
