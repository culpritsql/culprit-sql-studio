#!/usr/bin/env python3
"""Compile culpritsql.com into _site/:
- 9-locale platform pages (/, /studio/, /fleet/, /explain/, /ci/, /enterprise/, /databases/, /releases/)
- 7 canonical database provider HTML guides (/databases/<engine>/) + Markdown twins (/docs/providers/*.md)
- Automated GitHub Releases + CHANGELOG.md SEO pages (/releases/<slug>/, .md twins, /releases/rss.xml)
- Multilingual sitemap.xml with xhtml:link hreflang clusters, llms.txt, llms-full.txt
- Content-versioned static assets (?v=<sha256>) and strict SHA-256 Content-Security-Policy
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
from pathlib import Path
import re
import sys
import tomllib
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))

from site_lib.locales_meta import (
    LOCALE_BY_CODE,
    LOCALES,
    LOCALIZED_SLUGS,
    PROVIDER_URL_SLUGS,
    canonical_url,
    localized_path,
)
from site_lib.markdown_render import render_markdown
from site_lib.pages import (
    build_ci_page,
    build_databases_index_page,
    build_enterprise_page,
    build_explain_page,
    build_fleet_page,
    build_home_page,
    build_provider_detail_page,
    build_release_detail_page,
    build_releases_index_page,
    build_studio_page,
    render_footer,
    render_header,
)
from site_lib.releases import load_releases, write_releases_rss


def publish_documentation(root: Path, site: Path) -> tuple[list[dict[str, Any]], str]:
    """Validate and publish Markdown docs, copy provider SVGs, and render provider HTML sections."""
    parity = root / "docs/parity.md"
    parity_text = parity.read_text(encoding="utf-8")
    providers_raw = re.findall(r"^\| \[([^\]]+)\]\((providers/[a-z-]+\.md)\)", parity_text, re.MULTILINE)
    if not providers_raw or len({path for _, path in providers_raw}) != len(providers_raw):
        raise ValueError("Engine parity must list each provider once")

    sources = ["README.md", "docs/guide.md", "docs/agents.md", "docs/parity.md", "docs/ssh.md"]
    provider_sources = ["docs/" + path for _, path in providers_raw]
    sections = [
        "Connecting",
        "Permissions and setup",
        "Workload and plans",
        "Live activity and waits",
        "Schema and indexes",
        "Limits and evidence",
        "Verification",
    ]
    for (label, _), source in zip(providers_raw, provider_sources):
        text = (root / source).read_text(encoding="utf-8")
        if text.splitlines()[0] != f"# {label}" or re.findall(r"^## ([^\n]+)$", text, re.MULTILINE) != sections:
            raise ValueError(f"Provider page {source} must use the common title and seven-section structure")

    sources += provider_sources + ["docs/providers/coverage.md", "docs/ARCHITECTURE.md", "docs/DESIGN.md", "docs/sqlite.md"]
    destinations = {str((root / source).resolve()): source.lower() for source in sources}
    destinations[str((root / "README.md").resolve())] = "docs/readme.md"

    def publish_md_links(text: str, source: Path) -> str:
        def target(url: str) -> str:
            parts = urlsplit(url)
            if parts.scheme or parts.netloc or not parts.path:
                return url
            path = (source.parent / unquote(parts.path)).resolve()
            relative = path.relative_to(root).as_posix()
            if not path.exists():
                raise ValueError(f"Broken documentation link in {source.relative_to(root)}: {url}")
            destination = destinations.get(str(path))
            if destination:
                base = "/" + destination
            elif path.suffix.lower() in {".png", ".svg", ".jpg", ".webp"}:
                base = "https://raw.githubusercontent.com/culpritsql/culprit-sql-studio/main/" + relative
            else:
                kind = "tree" if path.is_dir() else "blob"
                base = f"https://github.com/culpritsql/culprit-sql-studio/{kind}/main/{relative}"
            parsed = urlsplit(base)
            return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parts.query, parts.fragment))

        def replace(match: re.Match[str]) -> str:
            return match[1] + target(match[2]) + match[3]

        text = re.sub(r"(!?\[[^\]\n]*\]\()([^\s)]+)(\))", replace, text)
        return re.sub(r'''(\b(?:src|href)=["'])([^"']+)(["'])''', replace, text)

    published: dict[str, str] = {}
    for source in sources:
        path = root / source
        text = publish_md_links(path.read_text(encoding="utf-8"), path)
        destination = site / destinations[str(path.resolve())]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")
        published[source] = text

    full_sources = ["README.md", "docs/guide.md", "docs/parity.md"] + provider_sources + ["docs/ssh.md", "docs/agents.md", "docs/providers/coverage.md"]
    full = "# Culprit SQL: full documentation\n\nSource: https://github.com/culpritsql/culprit-sql-studio · Website: https://culpritsql.com\n"
    for source in full_sources:
        full += "\n---\n\n" + published[source]
    (site / "llms-full.txt").write_text(full, encoding="utf-8")

    def html_doc_link_rewriter(source_file: Path):
        def rewrite(url: str) -> str:
            parts = urlsplit(url)
            if parts.scheme or parts.netloc or not parts.path:
                return url
            target_path = (source_file.parent / unquote(parts.path)).resolve()
            frag = f"#{parts.fragment}" if parts.fragment else ""
            if target_path == (root / "docs/parity.md").resolve():
                return f"/databases/{frag or '#parity'}"
            if target_path.parent == (root / "docs/providers").resolve() and target_path.stem in PROVIDER_URL_SLUGS:
                return f"/databases/{PROVIDER_URL_SLUGS[target_path.stem]}/{frag}"
            dest = destinations.get(str(target_path))
            if dest:
                return f"/{dest}{frag}"
            rel = target_path.relative_to(root).as_posix()
            kind = "tree" if target_path.is_dir() else "blob"
            return f"https://github.com/culpritsql/culprit-sql-studio/{kind}/main/{rel}{frag}"

        return rewrite

    logos = site / "images/providers"
    logos.mkdir(parents=True, exist_ok=True)
    providers_meta: list[dict[str, Any]] = []
    for label, provider_rel in providers_raw:
        raw_slug = Path(provider_rel).stem
        url_slug = PROVIDER_URL_SLUGS.get(raw_slug, raw_slug)
        logo = root / "assets/providers" / f"{raw_slug}.svg"
        (logos / logo.name).write_bytes(logo.read_bytes())
        row = next(line for line in parity_text.splitlines() if line.startswith(f"| [{label}]({provider_rel})"))
        summary = row.split("|")[3].strip().replace("`", "")
        src_path = root / "docs" / provider_rel
        rendered_html, toc = render_markdown(
            src_path.read_text(encoding="utf-8"),
            strip_h1=True,
            link_rewriter=html_doc_link_rewriter(src_path),
        )
        providers_meta.append(
            {
                "label": label,
                "raw_slug": raw_slug,
                "url_slug": url_slug,
                "summary": summary,
                "html": rendered_html,
                "toc": toc,
            }
        )

    # Extract table onwards from docs/parity.md (skipping duplicate "## Feature overview" heading)
    feat_idx = parity_text.find("| Feature |")
    parity_tail = parity_text[feat_idx:] if feat_idx >= 0 else parity_text
    parity_matrix_html, _ = render_markdown(
        parity_tail,
        strip_h1=True,
        link_rewriter=html_doc_link_rewriter(parity),
    )

    return providers_meta, parity_matrix_html


def write_page(site: Path, rel_url_path: str, content: str) -> None:
    clean = rel_url_path.strip("/")
    out_file = (site / clean / "index.html") if clean else (site / "index.html")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(content, encoding="utf-8")


def write_sitemap(
    site: Path,
    lastmod_default: str,
    providers: list[dict[str, Any]],
    releases: list[dict[str, Any]],
) -> None:
    entries: list[str] = []

    # 1. All 8 localized routes x 9 locales with full xhtml:link hreflang clusters
    for slug in LOCALIZED_SLUGS:
        alternates = [
            f'    <xhtml:link rel="alternate" hreflang="{html.escape(alt.hreflang)}" href="{html.escape(canonical_url(localized_path(alt.code, slug)))}" />'
            for alt in LOCALES
        ]
        alternates.append(
            f'    <xhtml:link rel="alternate" hreflang="x-default" href="{html.escape(canonical_url(localized_path("en", slug)))}" />'
        )
        alt_block = "\n".join(alternates)
        for loc in LOCALES:
            loc_url = canonical_url(localized_path(loc.code, slug))
            entries.append(
                f"  <url>\n    <loc>{html.escape(loc_url)}</loc>\n    <lastmod>{html.escape(lastmod_default)}</lastmod>\n{alt_block}\n  </url>"
            )

    # 2. 7 Canonical Database Provider Guides
    for p in providers:
        p_url = canonical_url(f"/databases/{p['url_slug']}/")
        entries.append(f"  <url><loc>{html.escape(p_url)}</loc><lastmod>{html.escape(lastmod_default)}</lastmod></url>")

    # 3. Dedicated Release Notes Permalinks
    for r in releases:
        if r.get("slug"):
            r_url = canonical_url(f"/releases/{r['slug']}/")
            entries.append(f"  <url><loc>{html.escape(r_url)}</loc><lastmod>{html.escape(r['dateShort'])}</lastmod></url>")

    sitemap_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    )
    (site / "sitemap.xml").write_text(sitemap_xml, encoding="utf-8")


def apply_csp(text: str) -> str:
    """Hash every inline <script> without src= and inject enforced Content-Security-Policy."""
    inline_scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", text, flags=re.DOTALL)
    hashes = " ".join(
        "'sha256-" + base64.b64encode(hashlib.sha256(script.encode("utf-8")).digest()).decode("ascii") + "'"
        for script in inline_scripts
        if script
    )
    policy = (
        "default-src 'none'; "
        f"script-src 'self' https://plausible.io/js/pa-r5EkaWqkhpILeZH2bizcF.js {hashes}; "
        "style-src 'self'; font-src 'self'; img-src 'self' data:; "
        "connect-src 'self' https://api.github.com/repos/culpritsql/culprit-sql-studio https://plausible.io/api/event; "
        "object-src 'none'; base-uri 'none'; form-action 'none'; frame-src 'none'"
    )
    return text.replace("__CONTENT_SECURITY_POLICY__", html.escape(policy, quote=True))


def build(root: Path, lastmod: str = "2026-11-27") -> None:
    site = root / "_site"
    providers, parity_matrix_html = publish_documentation(root, site)
    version = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))["workspace"]["package"]["version"]
    checks = len(json.loads((root / "fixtures/checks.sample.json").read_text(encoding="utf-8")))

    releases = load_releases(root)
    write_releases_rss(site, releases)

    # Render all 9 locales x 8 routes = 72 localized pages
    for locale in LOCALES:
        write_page(site, localized_path(locale.code, ""), build_home_page(locale, version=version, check_count=checks, providers=providers))
        write_page(site, localized_path(locale.code, "studio"), build_studio_page(locale, version=version, check_count=checks))
        write_page(site, localized_path(locale.code, "fleet"), build_fleet_page(locale, version=version))
        write_page(site, localized_path(locale.code, "explain"), build_explain_page(locale))
        write_page(site, localized_path(locale.code, "ci"), build_ci_page(locale))
        write_page(site, localized_path(locale.code, "enterprise"), build_enterprise_page(locale, version=version))
        write_page(site, localized_path(locale.code, "databases"), build_databases_index_page(locale, providers=providers, parity_matrix_html=parity_matrix_html))
        write_page(site, localized_path(locale.code, "releases"), build_releases_index_page(locale, releases=releases, studio_version=version))

    # Render 7 canonical /databases/<slug>/ guides
    for p in providers:
        write_page(site, f"/databases/{p['url_slug']}/", build_provider_detail_page(p))

    # Render substantive /releases/<slug>/ pages + Markdown twins
    rel_llms_lines = ["\n## Recent Releases\n"]
    for r in releases:
        slug = r.get("slug")
        if not slug:
            continue
        write_page(site, f"/releases/{slug}/", build_release_detail_page(r))
        md_content = f"# {r['name']} ({r['dateShort']})\n\nCanonical URL: https://culpritsql.com/releases/{slug}/\nGitHub Release: {r['url']}\n\n{r['body']}\n"
        (site / "releases" / f"{slug}.md").write_text(md_content, encoding="utf-8")
        if len(rel_llms_lines) <= 12:
            rel_llms_lines.append(f"- [{r['name']}](https://culpritsql.com/releases/{slug}.md): {r['summary']}")

    # Inject shared header/footer into 404.html
    en_locale = LOCALE_BY_CODE["en"]
    not_found = site / "404.html"
    if not_found.exists():
        nf_text = not_found.read_text(encoding="utf-8")
        nf_text = nf_text.replace("__SHARED_HEADER__", render_header(en_locale, "404")).replace("__SHARED_FOOTER__", render_footer(en_locale))
        not_found.write_text(nf_text, encoding="utf-8")

    # Write sitemap.xml and enrich llms.txt
    write_sitemap(site, lastmod, providers, releases)
    llms_path = site / "llms.txt"
    if llms_path.exists():
        llms_text = llms_path.read_text(encoding="utf-8").replace("__CHECK_COUNT__", str(checks))
        if "## Recent Releases" not in llms_text:
            llms_text = llms_text.rstrip() + "\n" + "\n".join(rel_llms_lines) + "\n"
        llms_path.write_text(llms_text, encoding="utf-8")

    # Version static assets and compute per-page SHA-256 CSP
    assets: dict[str, str] = {}

    def remember(path: Path) -> None:
        url = "/" + path.relative_to(site).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
        assets[url] = f"{url}?v={digest}"

    def references(text: str) -> str:
        for source, target in sorted(assets.items(), key=lambda pair: -len(pair[0])):
            text = re.sub(re.escape(source) + r"(?=[\s\"'<>),]|$)", lambda _: target, text)
        return text

    for path in sorted(site.rglob("*")):
        if path.is_file() and path.suffix in {".webp", ".woff2", ".js", ".svg", ".ico", ".png", ".jpg"}:
            remember(path)
    for path in sorted(site.rglob("*.css")):
        path.write_text(references(path.read_text(encoding="utf-8")), encoding="utf-8")
        remember(path)
    for path in sorted(site.rglob("*.html")):
        if path.is_relative_to(site / "blog"):
            continue
        versioned = references(path.read_text(encoding="utf-8"))
        path.write_text(apply_csp(versioned), encoding="utf-8")


def merge_blog(root: Path) -> None:
    """Append built blog routes from _site/blog/posts.json into sitemap.xml and llms.txt."""
    site = root / "_site"
    manifest_path = site / "blog/posts.json"
    if not manifest_path.exists():
        return
    posts = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not posts:
        return

    sitemap_path = site / "sitemap.xml"
    if sitemap_path.exists():
        latest = max((p.get("updatedDate") or p["pubDate"] for p in posts), default="2026-11-27")
        entries = [f"  <url><loc>https://culpritsql.com/blog/</loc><lastmod>{html.escape(latest)}</lastmod></url>"]
        for p in posts:
            lastmod = p.get("updatedDate") or p["pubDate"]
            entries.append(f"  <url><loc>{html.escape(p['url'])}</loc><lastmod>{html.escape(lastmod)}</lastmod></url>")
        sitemap_text = sitemap_path.read_text(encoding="utf-8")
        sitemap_path.write_text(sitemap_text.replace("</urlset>", "\n".join(entries) + "\n</urlset>"), encoding="utf-8")

    llms_path = site / "llms.txt"
    if llms_path.exists():
        llms_text = llms_path.read_text(encoding="utf-8")
        if "## Blog" not in llms_text:
            lines = ["\n## Blog\n"]
            for p in posts:
                lines.append(f"- [{p['title']}]({p['markdownUrl']}): {p['description']}")
            llms_path.write_text(llms_text.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")

    for path in sorted((site / "blog").rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        if "__CONTENT_SECURITY_POLICY__" in text:
            path.write_text(apply_csp(text), encoding="utf-8")


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    if "--merge-blog" in sys.argv[1:]:
        merge_blog(repo_root)
    else:
        lastmod_arg = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--lastmod=")), "2026-11-27")
        build(repo_root, lastmod=lastmod_arg)
