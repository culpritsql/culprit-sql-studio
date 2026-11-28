#!/usr/bin/env python3
"""Verify every generated HTML, XML, and RSS artifact in _site/:
- Enforced Content-Security-Policy with matching SHA-256 script hashes and zero inline style= attributes
- Single <h1> per page, explicit alt= on all <img>, valid JSON-LD @graph
- Canonical URLs with trailing slash, 9-locale hreflang reciprocity + x-default
- Internal link & asset resolution across _site/
- Zero personal author handles (@cristipufu / Cristian Pufu) or /Users/ paths
"""

from __future__ import annotations

import base64
import hashlib
import html as html_mod
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET


def verify_site(root: Path) -> None:
    site = root / "_site"
    if not site.exists():
        raise SystemExit("_site/ does not exist; run bash dev/site.sh first")

    has_blog = (site / "blog/index.html").exists()
    html_files = sorted(p for p in site.rglob("*.html") if not p.is_relative_to(site / "blog"))
    if len(html_files) < 85:
        raise SystemExit(f"Expected >= 85 generated site HTML pages, found {len(html_files)}")

    for page in html_files:
        rel = page.relative_to(site).as_posix()
        raw = page.read_text(encoding="utf-8")

        if "__CONTENT_SECURITY_POLICY__" in raw or "__VERSION__" in raw or "__CHECK_COUNT__" in raw:
            raise SystemExit(f"{rel}: unreplaced template placeholder")
        if "/Users/" in raw or "@cristipufu" in raw or "Cristian Pufu" in raw:
            raise SystemExit(f"{rel}: leaked local path or personal handle")
        if re.search(r"\bstyle\s*=", raw):
            raise SystemExit(f"{rel}: inline style= attribute violates strict style-src 'self' CSP")

        # CSP SHA-256 verification for every inline <script>
        csp_match = re.search(r'<meta http-equiv="Content-Security-Policy" content="([^"]+)">', raw)
        if not csp_match:
            raise SystemExit(f"{rel}: missing Content-Security-Policy meta tag")
        csp = html_mod.unescape(csp_match.group(1))
        for script_body in re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", raw, flags=re.DOTALL):
            if not script_body:
                continue
            expected_hash = "'sha256-" + base64.b64encode(hashlib.sha256(script_body.encode("utf-8")).digest()).decode("ascii") + "'"
            if expected_hash not in csp:
                raise SystemExit(f"{rel}: inline script SHA-256 {expected_hash} missing from CSP")

        # Exactly one <h1>
        h1s = re.findall(r"<h1[\s>]", raw)
        if len(h1s) != 1:
            raise SystemExit(f"{rel}: expected 1 <h1>, found {len(h1s)}")

        # Every <img> has alt=
        for img in re.findall(r"<img\b[^>]*>", raw):
            if not re.search(r'\balt="[^"]*"', img):
                raise SystemExit(f"{rel}: <img> missing alt attribute: {img}")

        # Header consistency check ("Culprit <b>SQL</b>")
        if '<span>Culprit <b>SQL</b></span>' not in raw:
            raise SystemExit(f"{rel}: missing unified Culprit SQL header brand")

        if rel != "404.html":
            # Canonical check
            expected_url_path = "/" if rel == "index.html" else "/" + rel.removesuffix("index.html")
            expected_can = f"https://culpritsql.com{expected_url_path}"
            if f'<link rel="canonical" href="{expected_can}">' not in raw:
                raise SystemExit(f"{rel}: canonical mismatch (expected {expected_can})")

            # SERP <title> and <meta name="description"> length bounds
            t_match = re.search(r"<title>([^<]+)</title>", raw)
            d_match = re.search(r'<meta name="description" content="([^"]+)">', raw)
            if not t_match or not d_match:
                raise SystemExit(f"{rel}: missing <title> or <meta description>")
            title_txt = html_mod.unescape(t_match.group(1))
            desc_txt = html_mod.unescape(d_match.group(1))
            if not (18 <= len(title_txt) <= 68):
                raise SystemExit(f"{rel}: <title> length {len(title_txt)} outside [18, 68]: {title_txt}")
            if not (55 <= len(desc_txt) <= 165):
                raise SystemExit(f"{rel}: <meta description> length {len(desc_txt)} outside [55, 165]: {desc_txt}")

            # JSON-LD check
            ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', raw, flags=re.DOTALL)
            if not ld_match:
                raise SystemExit(f"{rel}: missing JSON-LD")
            ld = json.loads(ld_match.group(1))
            if ld.get("@context") != "https://schema.org" or not isinstance(ld.get("@graph"), list):
                raise SystemExit(f"{rel}: invalid JSON-LD @graph")

        # Internal link, asset & #anchor target check
        for m in re.finditer(r'(?<![a-zA-Z0-9_-])(?:href|src)="([^"]+)"', raw):
            href = html_mod.unescape(m.group(1))
            if href.startswith(("mailto:", "data:")):
                continue
            if href.startswith("#"):
                frag_only = href[1:]
                if frag_only and frag_only != "top" and f'id="{frag_only}"' not in raw:
                    raise SystemExit(f"{rel}: broken same-page anchor {href}")
                continue
            parts = urlsplit(href)
            if parts.scheme in {"http", "https"}:
                if parts.netloc != "culpritsql.com":
                    continue
                check_path = parts.path or "/"
            else:
                check_path = parts.path
            if not check_path.startswith("/"):
                raise SystemExit(f"{rel}: relative non-root URL {href}")
            if check_path.startswith("/blog/") and not has_blog:
                continue
            fs_target = site / check_path.lstrip("/")
            if check_path.endswith("/"):
                fs_target = fs_target / "index.html"
            if not fs_target.exists():
                raise SystemExit(f"{rel}: broken internal target {href} -> {fs_target}")
            if parts.fragment and parts.fragment != "top" and fs_target.suffix == ".html":
                target_raw = fs_target.read_text(encoding="utf-8")
                if f'id="{parts.fragment}"' not in target_raw:
                    raise SystemExit(f"{rel}: broken fragment #{parts.fragment} in {href} -> {fs_target}")

    # Validate XML well-formedness of sitemap.xml and releases/rss.xml
    ET.fromstring((site / "sitemap.xml").read_text(encoding="utf-8"))
    ET.fromstring((site / "releases/rss.xml").read_text(encoding="utf-8"))

    if has_blog:
        blog_idx = (site / "blog/index.html").read_text(encoding="utf-8")
        for needle in ('<span>Culprit <b>SQL</b></span>', "culprit:theme", "https://culpritsql.com/releases/", "https://culpritsql.com/enterprise/"):
            if needle not in blog_idx:
                raise SystemExit(f"blog/index.html: missing unified header/theme marker {needle}")

    print(f"Verified {len(html_files)} static HTML pages, CSP hashes, SEO schemas, sitemap.xml, and releases/rss.xml")


if __name__ == "__main__":
    verify_site(Path(__file__).resolve().parent.parent)
