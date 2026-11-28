"""GitHub Releases + CHANGELOG.md ingestion, sanitization, and RSS / Markdown export."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime
import html
import json
import os
from pathlib import Path
import re
import urllib.request

from .markdown_render import summarize_markdown

REPOS: tuple[tuple[str, str, str, str], ...] = (
    # (repo_name, product_key, product_label, filter_group)
    ("culprit-sql-studio", "studio", "Culprit SQL Studio", "studio"),
    ("culprit-fleet", "fleet", "Culprit Fleet", "studio"),
    ("culprit-explain", "explain", "Culprit Explain", "wasm"),
    ("culprit-vscode", "vscode", "Culprit SQL for VS Code", "vscode"),
    ("culprit-ci", "ci", "@culpritsql/ci", "ci"),
    ("culprit-devtools", "devtools", "@culpritsql/devtools", "ci"),
    ("culprit-test", "test", "@culpritsql/test", "ci"),
    ("culprit-plans", "plans", "@culpritsql/plans", "wasm"),
    ("culprit-plan-viewer", "plan-viewer", "@culpritsql/plan-viewer", "wasm"),
)

GROUP_BY_PRODUCT: dict[str, str] = {prod: grp for _, prod, _, grp in REPOS}

HISTORICAL_TAG_DATES: dict[str, str] = {
    "v0.6.1": "2026-11-26T16:22:22Z",
    "v0.6.0": "2026-11-25T11:48:44Z",
}


def parse_semver(tag: str) -> tuple[int, int, int]:
    m = re.match(r"^v?(\d+)\.(\d+)\.(\d+)", tag.strip())
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)


def sanitize_release_body(body: str) -> str:
    """Strip GitHub auto-generated personal handles while preserving engineering notes and compare links."""
    cleaned = body.replace("\r\n", "\n").strip()
    cleaned = cleaned.replace("https://github.com/cristipufu/", "https://github.com/culpritsql/")
    cleaned = re.sub(r"\s+by\s+@[A-Za-z0-9_-]+(?=\s+in\s+https://)", "", cleaned)
    cleaned = re.sub(r"\n*## New Contributors[\s\S]*?(?=\n\*\*Full Changelog\*\*:|\Z)", "\n\n", cleaned)
    # When curated ### sections exist, drop redundant auto-generated PR list under "## What's Changed"
    if re.search(r"^###\s+", cleaned, flags=re.MULTILINE):
        cleaned = re.sub(
            r"\n+## What's Changed\n[\s\S]*?(?=\n+\*\*Full Changelog\*\*:|\Z)",
            "\n\n",
            cleaned,
        )
    return cleaned.strip()


def fetch_github_releases_live() -> list[dict]:
    """Fetch published releases across all culpritsql repos via GitHub REST API."""
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "culpritsql-site-builder",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    collected: list[dict] = []
    for repo, product_key, product_label, _ in REPOS:
        url = f"https://api.github.com/repos/culpritsql/{repo}/releases?per_page=50"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            items = json.loads(resp.read().decode("utf-8"))
        for r in items:
            if r.get("draft"):
                continue
            assets = [
                {
                    "name": a["name"],
                    "size": a["size"],
                    "url": a["browser_download_url"],
                }
                for a in r.get("assets", [])
            ]
            collected.append(
                {
                    "repo": repo,
                    "product": product_key,
                    "productLabel": product_label,
                    "tag": r["tag_name"],
                    "name": r.get("name") or f"{product_label} {r['tag_name']}",
                    "publishedAt": r.get("published_at") or r.get("created_at") or "",
                    "prerelease": bool(r.get("prerelease")),
                    "url": r["html_url"],
                    "body": sanitize_release_body(r.get("body") or ""),
                    "assets": assets,
                }
            )
    return collected


def parse_local_changelog(changelog_path: Path) -> dict[str, str]:
    """Parse CHANGELOG.md sections keyed by tag ('v0.6.3' -> markdown body)."""
    if not changelog_path.exists():
        return {}
    text = changelog_path.read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    matches = list(re.finditer(r"^##\s+([0-9]+\.[0-9]+\.[0-9]+[^\s]*)\s*$", text, flags=re.MULTILINE))
    for idx, m in enumerate(matches):
        ver = m.group(1).strip()
        start = m.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body:
            sections[f"v{ver}" if not ver.startswith("v") else ver] = body
    return sections


def release_slug(item: dict) -> str | None:
    """Return dedicated SEO permalink slug for substantive releases, or None for minor package tags."""
    tag = item["tag"]
    prod = item["product"]
    body = item["body"]
    if prod == "studio":
        return re.sub(r"[^a-zA-Z0-9._-]", "-", tag)
    if re.search(r"^###\s+", body, flags=re.MULTILINE) or len(body) >= 500:
        safe_tag = re.sub(r"[^a-zA-Z0-9._-]", "-", tag)
        return f"{prod}-{safe_tag}"
    return None


def load_releases(root: Path) -> list[dict]:
    """Load releases (live API when CULPRIT_FETCH_RELEASES=1, else seed JSON) merged with CHANGELOG.md."""
    seed_path = root / "site/data/releases.json"
    items: list[dict] = []

    if os.environ.get("CULPRIT_FETCH_RELEASES") == "1":
        try:
            items = fetch_github_releases_live()
        except Exception as exc:
            print(f"Warning: live GitHub release fetch failed ({exc}); using {seed_path}", flush=True)

    if not items and seed_path.exists():
        items = json.loads(seed_path.read_text(encoding="utf-8"))

    changelog_map = parse_local_changelog(root / "CHANGELOG.md")
    seen_studio_tags = {r["tag"] for r in items if r["product"] == "studio"}

    # Ensure Studio releases have clean curated bodies and any newly added CHANGELOG.md section appears
    for r in items:
        r["body"] = sanitize_release_body(r.get("body", ""))
        if r["product"] == "studio" and r["tag"] in changelog_map:
            # Keep Full Changelog compare footer if present on GitHub
            compare_match = re.search(r"(\*\*Full Changelog\*\*:[^\n]+)$", r["body"])
            curated = changelog_map[r["tag"]]
            if compare_match and "Full Changelog" not in curated:
                curated = f"{curated}\n\n{compare_match.group(1)}"
            r["body"] = sanitize_release_body(curated)

    max_published_studio = max(
        (parse_semver(r["tag"]) for r in items if r["product"] == "studio" and not r.get("prerelease")),
        default=(0, 0, 0),
    )
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    for tag, body in changelog_map.items():
        if tag not in seen_studio_tags:
            ver = tag.removeprefix("v")
            sem = parse_semver(tag)
            if tag in HISTORICAL_TAG_DATES:
                pub_at = HISTORICAL_TAG_DATES[tag]
            elif sem > max_published_studio:
                pub_at = today_iso
            else:
                next_dates = [
                    r["publishedAt"]
                    for r in items
                    if r["product"] == "studio" and parse_semver(r["tag"]) > sem and r.get("publishedAt")
                ]
                pub_at = min(next_dates) if next_dates else "1970-01-01T00:00:00Z"
            items.append(
                {
                    "repo": "culprit-sql-studio",
                    "product": "studio",
                    "productLabel": "Culprit SQL Studio",
                    "tag": tag,
                    "name": f"Culprit SQL Studio {ver}",
                    "publishedAt": pub_at,
                    "prerelease": False,
                    "url": f"https://github.com/culpritsql/culprit-sql-studio/releases/tag/{tag}",
                    "body": sanitize_release_body(body),
                    "assets": [],
                }
            )

    items.sort(
        key=lambda r: (
            r.get("publishedAt") or "",
            parse_semver(r["tag"]) if r["product"] == "studio" else (0, 0, 0),
        ),
        reverse=True,
    )

    for r in items:
        r["group"] = GROUP_BY_PRODUCT.get(r["product"], "ci")
        r["slug"] = release_slug(r)
        r["dateShort"] = (r.get("publishedAt") or "")[:10] or "2026-11-27"
        fallback = f"Official release notes, downloads, and SHA-256 checksums for {r['name']} ({r['tag']})."
        r["summary"] = summarize_markdown(r["body"], fallback)

    return items


def latest_by_product(releases: list[dict], studio_version: str) -> list[dict]:
    """Return the 8-tile current artifact matrix across the platform."""
    latest: dict[str, dict] = {}
    for r in releases:
        if r.get("prerelease"):
            continue
        prod = r["product"]
        prev = latest.get(prod)
        if prev is None or (parse_semver(r["tag"]), r.get("publishedAt") or "") > (
            parse_semver(prev["tag"]),
            prev.get("publishedAt") or "",
        ):
            latest[prod] = r

    studio_rel = latest.get("studio")
    studio_tag = studio_rel["tag"] if studio_rel else f"v{studio_version}"
    studio_date = studio_rel["dateShort"] if studio_rel else "2026-11-27"

    tiles = [
        {
            "title": "Culprit SQL Studio & CLI",
            "version": studio_tag,
            "meta": f"{studio_date} · macOS / Win / Linux",
            "href": f"/releases/{studio_rel['slug']}/" if studio_rel and studio_rel.get("slug") else "/studio/#download",
        },
        {
            "title": "Culprit Collector & Helm",
            "version": studio_tag,
            "meta": "GHCR multi-arch · OCI chart",
            "href": "/fleet/",
        },
        {
            "title": "Culprit Fleet Web",
            "version": "Live",
            "meta": "ADX & ClickHouse · 0 DB drivers",
            "href": "https://fleet.culpritsql.com",
        },
        {
            "title": "Culprit Explain",
            "version": "Wasm",
            "meta": "9 locales · Plans & .xel",
            "href": "https://explain.culpritsql.com",
        },
    ]

    for prod_key, default_title, meta_hint in (
        ("vscode", "Culprit SQL for VS Code", "VS Code Marketplace & .vsix"),
        ("ci", "@culpritsql/ci", "GitHub Action & SARIF CLI"),
        ("plans", "@culpritsql/plans", "Rust -> Wasm parser SDK"),
        ("test", "@culpritsql/test", "Vitest & Jest SQL assertions"),
    ):
        rel = latest.get(prod_key)
        tiles.append(
            {
                "title": default_title,
                "version": rel["tag"] if rel else "latest",
                "meta": f"{rel['dateShort']} · {meta_hint}" if rel else meta_hint,
                "href": rel["url"] if rel else f"https://github.com/culpritsql/culprit-{prod_key}",
            }
        )
    return tiles


def format_bytes(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    if size >= 1024:
        return f"{size / 1024:.0f} KB"
    return f"{size} B"


def write_releases_rss(site_dir: Path, releases: list[dict]) -> None:
    """Write valid RSS 2.0 feed to _site/releases/rss.xml."""
    rss_dir = site_dir / "releases"
    rss_dir.mkdir(parents=True, exist_ok=True)
    items_xml: list[str] = []
    for r in releases[:40]:
        link = f"https://culpritsql.com/releases/{r['slug']}/" if r.get("slug") else r["url"]
        try:
            dt = datetime.fromisoformat(r["publishedAt"].replace("Z", "+00:00"))
        except ValueError:
            dt = datetime(2026, 11, 27, tzinfo=timezone.utc)
        pub_rfc = format_datetime(dt)
        items_xml.append(
            "    <item>\n"
            f"      <title>{html.escape(r['name'])}</title>\n"
            f"      <link>{html.escape(link)}</link>\n"
            f"      <guid isPermaLink=\"true\">{html.escape(link)}</guid>\n"
            f"      <pubDate>{html.escape(pub_rfc)}</pubDate>\n"
            f"      <description>{html.escape(r['summary'])}</description>\n"
            "    </item>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
        "  <channel>\n"
        "    <title>Culprit SQL Releases</title>\n"
        "    <link>https://culpritsql.com/releases/</link>\n"
        "    <description>Official release notes across Culprit SQL Studio, Collector, VS Code, CI, and WebAssembly SDKs.</description>\n"
        "    <language>en-us</language>\n"
        '    <atom:link href="https://culpritsql.com/releases/rss.xml" rel="self" type="application/rss+xml" />\n'
        + "\n".join(items_xml)
        + "\n  </channel>\n</rss>\n"
    )
    (rss_dir / "rss.xml").write_text(xml, encoding="utf-8")
