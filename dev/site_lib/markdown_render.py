"""Zero-dependency, XSS-safe Markdown-to-HTML renderer for Culprit docs and release notes."""

from __future__ import annotations

import html
import re
from typing import Callable


def slugify_heading(text: str) -> str:
    clean = re.sub(r"`([^`]+)`", r"\1", text)
    clean = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", clean)
    clean = re.sub(r"[^a-zA-Z0-9\s-]", "", clean).strip().lower()
    return re.sub(r"[\s-]+", "-", clean) or "section"


def render_inline(text: str, link_rewriter: Callable[[str], str] | None = None) -> str:
    """Render inline code, links, bold, and emphasis while escaping all raw HTML."""
    tokens: list[str] = []

    def stash(fragment: str) -> str:
        idx = len(tokens)
        tokens.append(fragment)
        return f"\x00TOK{idx}\x00"

    # 1. Inline code spans `...`
    def repl_code(m: re.Match[str]) -> str:
        return stash(f"<code>{html.escape(m.group(1))}</code>")

    work = re.sub(r"`([^`\n]+)`", repl_code, text)

    # 2. Explicit HTML anchor markers in docs like <a id="verified-on"></a>
    def repl_anchor(m: re.Match[str]) -> str:
        aid = re.sub(r"[^a-zA-Z0-9_-]", "", m.group(1))
        return stash(f'<span id="{aid}"></span>')

    work = re.sub(r'<a\s+id=["\']([^"\']+)["\']\s*>\s*</a>', repl_anchor, work)

    # 2b. Markdown images ![alt](url) -> external link so strict img-src 'self' CSP stays clean
    def repl_image(m: re.Match[str]) -> str:
        alt_raw, url_raw = m.group(1).strip() or "Screenshot", m.group(2).strip()
        target_url = link_rewriter(url_raw) if link_rewriter else url_raw
        safe_url = html.escape(target_url, quote=True)
        ext = ' rel="noopener"' if target_url.startswith(("http://", "https://")) and "culpritsql.com" not in target_url else ""
        return stash(f'<a href="{safe_url}"{ext}>{html.escape(alt_raw)} ↗</a>')

    work = re.sub(r"!\[([^\]\n]*)\]\(([^\s)]+)\)", repl_image, work)

    # 3. Markdown links [label](url)
    def repl_link(m: re.Match[str]) -> str:
        label_raw, url_raw = m.group(1), m.group(2).strip()
        target_url = link_rewriter(url_raw) if link_rewriter else url_raw
        safe_url = html.escape(target_url, quote=True)
        # Render inline code inside link labels too
        label_html = html.escape(label_raw)
        label_html = re.sub(
            r"\x00TOK(\d+)\x00",
            lambda tm: tokens[int(tm.group(1))],
            label_html,
        )
        ext = ' rel="noopener"' if target_url.startswith(("http://", "https://")) and "culpritsql.com" not in target_url else ""
        return stash(f'<a href="{safe_url}"{ext}>{label_html}</a>')

    work = re.sub(r"\[([^\]\n]+)\]\(([^\s)]+)\)", repl_link, work)

    # 4. Bare https:// URLs in release notes (e.g. compare links)
    def repl_bare_url(m: re.Match[str]) -> str:
        raw_url = m.group(1).rstrip(".,);")
        trailing = m.group(1)[len(raw_url):]
        safe_url = html.escape(raw_url, quote=True)
        display = raw_url.replace("https://", "")
        if len(display) > 68:
            display = display[:65] + "..."
        ext = ' rel="noopener"' if "culpritsql.com" not in raw_url else ""
        return stash(f'<a href="{safe_url}"{ext}>{html.escape(display)}</a>') + html.escape(trailing)

    work = re.sub(r"(?<![\"'=(])(https://[^\s<>]+)", repl_bare_url, work)

    # Escape remaining plain text
    escaped = html.escape(work)

    # 5. Bold **...** and italic *...*
    escaped = re.sub(r"\*\*([^*\n]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", escaped)

    # Restore stashed HTML fragments
    return re.sub(r"\x00TOK(\d+)\x00", lambda m: tokens[int(m.group(1))], escaped)


def render_markdown(
    md_text: str,
    *,
    strip_h1: bool = True,
    demote_h2_to_h3: bool = False,
    link_rewriter: Callable[[str], str] | None = None,
) -> tuple[str, list[tuple[str, str]]]:
    """Convert Markdown text into clean semantic HTML and return (html, [(id, h2_label), ...])."""
    lines = md_text.replace("\r\n", "\n").split("\n")
    out: list[str] = []
    toc: list[tuple[str, str]] = []
    seen_ids: dict[str, int] = {}

    i = 0
    n = len(lines)
    skipped_first_h1 = False

    def unique_id(base: str) -> str:
        count = seen_ids.get(base, 0)
        seen_ids[base] = count + 1
        return base if count == 0 else f"{base}-{count + 1}"

    while i < n:
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        # Standalone anchor line <a id="..."></a>
        anchor_only = re.match(r'^<a\s+id=["\']([^"\']+)["\']\s*>\s*</a>$', stripped)
        if anchor_only:
            aid = re.sub(r"[^a-zA-Z0-9_-]", "", anchor_only.group(1))
            out.append(f'<div id="{aid}"></div>')
            i += 1
            continue

        # Fenced code block
        fence = re.match(r"^```([a-zA-Z0-9_-]*)\s*$", stripped)
        if fence:
            i += 1
            code_lines: list[str] = []
            while i < n and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            if i < n:
                i += 1
            code_escaped = html.escape("\n".join(code_lines))
            out.append(f"<pre><code>{code_escaped}</code></pre>")
            continue

        # Heading
        heading = re.match(r"^(#{1,4})\s+(.+?)\s*$", stripped)
        if heading:
            level = len(heading.group(1))
            raw_title = heading.group(2)
            if level == 1 and strip_h1 and not skipped_first_h1:
                skipped_first_h1 = True
                i += 1
                continue
            if demote_h2_to_h3:
                level = min(4, level + 1)
            elif level == 1:
                level = 2
            hid = unique_id(slugify_heading(raw_title))
            rendered_title = render_inline(raw_title, link_rewriter)
            plain_title = re.sub(r"<[^>]+>", "", rendered_title)
            if level == 2 and not demote_h2_to_h3:
                toc.append((hid, html.unescape(plain_title)))
            out.append(f'<h{level} id="{hid}">{rendered_title}</h{level}>')
            i += 1
            continue

        # GFM Table
        if "|" in stripped and i + 1 < n and re.match(r"^\s*\|?[\s:-]+\|[\s|:-]*$", lines[i + 1]):
            header_cells = [c.strip() for c in stripped.strip("|").split("|")]
            i += 2  # skip header and delimiter row
            body_rows: list[list[str]] = []
            while i < n and "|" in lines[i] and lines[i].strip():
                row_cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                body_rows.append(row_cells)
                i += 1
            thead = "".join(f'<th scope="col">{render_inline(c, link_rewriter)}</th>' for c in header_cells)
            tbody_parts: list[str] = []
            for r in body_rows:
                padded = r + [""] * max(0, len(header_cells) - len(r))
                tds = "".join(f"<td>{render_inline(c, link_rewriter)}</td>" for c in padded[: len(header_cells)])
                tbody_parts.append(f"<tr>{tds}</tr>")
            out.append(
                f'<div class="table-scroll"><table class="data-table"><thead><tr>{thead}</tr></thead>'
                f'<tbody>{"".join(tbody_parts)}</tbody></table></div>'
            )
            continue

        # Unordered list
        if re.match(r"^[-*]\s+", stripped):
            items: list[str] = []
            while i < n and re.match(r"^\s*[-*]\s+", lines[i]):
                item_lines = [re.sub(r"^\s*[-*]\s+", "", lines[i]).strip()]
                i += 1
                while i < n and lines[i].startswith("  ") and not re.match(r"^\s*[-*]\s+", lines[i]):
                    item_lines.append(lines[i].strip())
                    i += 1
                items.append(f"<li>{render_inline(' '.join(item_lines), link_rewriter)}</li>")
            out.append(f"<ul>{''.join(items)}</ul>")
            continue

        # Ordered list
        if re.match(r"^\d+\.\s+", stripped):
            items = []
            while i < n and re.match(r"^\s*\d+\.\s+", lines[i]):
                item_text = re.sub(r"^\s*\d+\.\s+", "", lines[i]).strip()
                items.append(f"<li>{render_inline(item_text, link_rewriter)}</li>")
                i += 1
            out.append(f"<ol>{''.join(items)}</ol>")
            continue

        # Paragraph (merge consecutive plain lines)
        para_lines: list[str] = []
        while (
            i < n
            and lines[i].strip()
            and not lines[i].strip().startswith(("```", "#"))
            and not re.match(r"^\s*[-*]\s+", lines[i])
            and not re.match(r"^\s*\d+\.\s+", lines[i])
            and not (
                "|" in lines[i]
                and i + 1 < n
                and re.match(r"^\s*\|?[\s:-]+\|[\s|:-]*$", lines[i + 1])
            )
        ):
            para_lines.append(lines[i].strip())
            i += 1
        joined = " ".join(para_lines)
        # Skip standalone "[Engine parity](../parity.md)" breadcrumb line at top of provider docs
        if joined == "[Engine parity](../parity.md)":
            continue
        out.append(f"<p>{render_inline(joined, link_rewriter)}</p>")

    return "\n".join(out), toc


def summarize_markdown(md_text: str, fallback: str, min_len: int = 115, max_len: int = 158) -> str:
    """Extract a clean plain-text SERP description from Markdown release notes."""
    text = re.sub(r"```[\s\S]*?```", " ", md_text)
    text = re.sub(r"^#+\s+.*$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*Full Changelog\*\*:.*$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[`*_>#-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 40:
        text = fallback
    elif len(text) < min_len:
        text = f"{text} {fallback}"
    if len(text) > max_len:
        cut = text[: max_len - 1].rsplit(" ", 1)[0].rstrip(".,;:")
        text = cut + "."
    return text
