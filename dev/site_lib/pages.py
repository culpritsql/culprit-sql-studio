"""HTML shell, unified Culprit SQL header/footer, JSON-LD, and route page renderers."""

from __future__ import annotations

import html
import json
from typing import Any

from .bundles_east import BUNDLES_EAST
from .bundles_west import BUNDLES_WEST
from .locales_meta import (
    LOCALE_BY_CODE,
    LOCALES,
    LocaleSpec,
    canonical_url,
    explain_url_for_locale,
    localized_path,
)
from .markdown_render import render_markdown
from .releases import format_bytes, latest_by_product

BUNDLES: dict[str, dict[str, Any]] = {**BUNDLES_WEST, **BUNDLES_EAST}

THEME_INIT_SCRIPT = (
    'try{var t=localStorage.getItem("culprit:theme")||localStorage.getItem("culprit-blog:theme");'
    'if(t==="dark"||t==="light")document.documentElement.dataset.theme=t;}catch(_){}'
)

GITHUB_SVG = (
    '<svg aria-hidden="true" viewBox="0 0 16 16" width="15" height="15">'
    '<path fill="currentColor" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 '
    "0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 "
    "1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15"
    "-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 "
    "2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07"
    '-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z"/></svg>'
)

GLOBE_SVG = (
    '<svg aria-hidden="true" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2">'
    '<circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 '
    '15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10Z"/></svg>'
)

CARET_SVG = (
    '<svg class="nav-caret" aria-hidden="true" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8">'
    '<path d="m4 6 4 4 4-4" stroke-linecap="round" stroke-linejoin="round"/></svg>'
)

SUN_MOON_SVG = (
    '<svg class="icon-sun" aria-hidden="true" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
    '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>'
    "</svg>"
    '<svg class="icon-moon" aria-hidden="true" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
    '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/></svg>'
)

MENU_SVG = (
    '<svg aria-hidden="true" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
    '<path d="M4 7h16M4 12h16M4 17h16" stroke-linecap="round"/></svg>'
)

SAME_AS_URLS = [
    "https://github.com/culpritsql",
    "https://github.com/culpritsql/culprit-sql-studio",
    "https://github.com/culpritsql/culprit-fleet",
    "https://github.com/culpritsql/culprit-explain",
    "https://github.com/culpritsql/culprit-vscode",
    "https://github.com/culpritsql/culprit-ci",
    "https://github.com/culpritsql/culprit-devtools",
    "https://github.com/culpritsql/culprit-test",
    "https://github.com/culpritsql/culprit-plans",
    "https://github.com/culpritsql/culprit-plan-viewer",
    "https://fleet.culpritsql.com/",
    "https://explain.culpritsql.com/",
]


def e(val: str) -> str:
    return html.escape(val, quote=True)


def themed_picture(
    stem: str,
    width: int,
    height: int,
    alt: str,
    *,
    sizes: str = "(max-width: 980px) calc(100vw - 48px), 640px",
    has_half: bool = False,
    eager: bool = False,
) -> str:
    half_dark = f"/images/{stem}-dark@0.5x.webp {width // 2}w, " if has_half else ""
    half_light = f"/images/{stem}-light@0.5x.webp {width // 2}w, " if has_half else ""
    dark_srcset = f"{half_dark}/images/{stem}-dark@1x.webp {width}w, /images/{stem}-dark@2x.webp {width * 2}w"
    light_srcset = f"{half_light}/images/{stem}-light@1x.webp {width}w, /images/{stem}-light@2x.webp {width * 2}w"
    load_attr = 'fetchpriority="high"' if eager else 'loading="lazy"'
    return (
        "<picture>"
        f'<source type="image/webp" media="(prefers-color-scheme: dark)" data-dark-srcset="1" srcset="{dark_srcset}" sizes="{e(sizes)}">'
        f'<img src="/images/{stem}-light@1x.webp" srcset="{light_srcset}" sizes="{e(sizes)}" '
        f'width="{width}" height="{height}" {load_attr} decoding="async" alt="{e(alt)}">'
        "</picture>"
    )


def render_header(
    locale: LocaleSpec,
    active_section: str,
    *,
    lang_switcher_slug: str | None = "",
) -> str:
    """Render the unified Culprit SQL 64px sticky header."""
    b = BUNDLES[locale.code]
    nav = b["nav"]
    home_href = localized_path(locale.code, "")
    studio_href = localized_path(locale.code, "studio")
    fleet_href = localized_path(locale.code, "fleet")
    explain_href = localized_path(locale.code, "explain")
    ci_href = localized_path(locale.code, "ci")
    ent_href = localized_path(locale.code, "enterprise")
    db_href = localized_path(locale.code, "databases")
    rel_href = localized_path(locale.code, "releases")
    cta_href = f"{studio_href}#download" if active_section != "studio" else "#download"

    prod_active = " active" if active_section in {"studio", "fleet", "explain", "ci"} else ""
    ent_active = ' class="active"' if active_section == "enterprise" else ""
    db_active = ' class="active"' if active_section == "databases" else ""
    rel_active = ' class="active"' if active_section == "releases" else ""
    blog_active = ' class="active"' if active_section == "blog" else ""

    lang_items: list[str] = []
    target_slug = lang_switcher_slug if lang_switcher_slug is not None else ""
    for alt in LOCALES:
        href = localized_path(alt.code, target_slug)
        cls = "lang-item active" if alt.code == locale.code else "lang-item"
        mark = "<span>✓</span>" if alt.code == locale.code else ""
        lang_items.append(f'<a class="{cls}" href="{e(href)}" hreflang="{e(alt.hreflang)}"><span>{e(alt.native_label)}</span>{mark}</a>')

    return f"""<a class="skip" href="#main">Skip to content</a>
  <header class="nav">
    <div class="wrap nav-row">
      <a class="brand" href="{e(home_href)}" aria-label="Culprit SQL">
        <img src="/favicon.svg" width="26" height="26" alt="Culprit SQL logo">
        <span>Culprit <b>SQL</b></span>
      </a>
      <nav class="nav-links" aria-label="Primary">
        <details class="nav-dropdown{prod_active}">
          <summary>{e(nav["products"])}{CARET_SVG}</summary>
          <div class="nav-panel">
            <a class="nav-prod" href="{e(studio_href)}">
              <span class="nav-prod-head"><span>{e(nav["prod_studio_title"])}</span><span class="nav-prod-tag">App · CLI · MCP</span></span>
              <p>{e(nav["prod_studio_desc"])}</p>
            </a>
            <a class="nav-prod" href="{e(fleet_href)}">
              <span class="nav-prod-head"><span>{e(nav["prod_fleet_title"])}</span><span class="nav-prod-tag">ADX · ClickHouse</span></span>
              <p>{e(nav["prod_fleet_desc"])}</p>
            </a>
            <a class="nav-prod" href="{e(explain_href)}">
              <span class="nav-prod-head"><span>{e(nav["prod_explain_title"])}</span><span class="nav-prod-tag">Wasm · .xel</span></span>
              <p>{e(nav["prod_explain_desc"])}</p>
            </a>
            <a class="nav-prod" href="{e(ci_href)}">
              <span class="nav-prod-head"><span>{e(nav["prod_ci_title"])}</span><span class="nav-prod-tag">SARIF · VS Code</span></span>
              <p>{e(nav["prod_ci_desc"])}</p>
            </a>
            <div class="nav-panel-foot">
              <span>{e(nav["panel_foot_left"])}</span>
              <span><a href="https://fleet.culpritsql.com">{e(nav["panel_foot_fleet"])}</a> · <a href="{e(explain_url_for_locale(locale.code))}">{e(nav["panel_foot_explain"])}</a></span>
            </div>
          </div>
        </details>
        <a href="{e(ent_href)}"{ent_active}>{e(nav["enterprise"])}</a>
        <a href="{e(db_href)}"{db_active}>{e(nav["databases"])}</a>
        <a href="{e(rel_href)}"{rel_active}>{e(nav["releases"])}</a>
        <a href="/blog/"{blog_active}>{e(nav["blog"])}</a>
      </nav>
      <div class="nav-cta">
        <details class="lang-menu">
          <summary aria-label="{e(nav['lang'])}">{GLOBE_SVG}<span class="lang-full">{e(locale.native_label)}</span><span>{CARET_SVG}</span></summary>
          <div class="lang-list">
            {"".join(lang_items)}
          </div>
        </details>
        <button type="button" class="icon-btn" id="theme-toggle" aria-label="{e(nav['theme'])}" title="{e(nav['theme'])}">
          {SUN_MOON_SVG}
        </button>
        <a class="btn ghost gh hide-md" href="https://github.com/culpritsql/culprit-sql-studio" rel="noopener" aria-label="Culprit SQL on GitHub">
          {GITHUB_SVG}<span class="gh-label">GitHub</span><span class="stars" data-stars hidden></span>
        </a>
        <a class="btn primary" href="{e(cta_href)}">{e(nav["cta"])}</a>
        <details class="mobile-menu">
          <summary class="icon-btn" aria-label="{e(nav['menu'])}">{MENU_SVG}</summary>
          <div class="mobile-drawer">
            <div class="mobile-drawer-group">{e(nav["products"])}</div>
            <a href="{e(studio_href)}"><span>{e(nav["prod_studio_title"])}</span><span class="nav-prod-tag">App · CLI</span></a>
            <a href="{e(fleet_href)}"><span>{e(nav["prod_fleet_title"])}</span><span class="nav-prod-tag">24/7</span></a>
            <a href="{e(explain_href)}"><span>{e(nav["prod_explain_title"])}</span><span class="nav-prod-tag">Wasm</span></a>
            <a href="{e(ci_href)}"><span>{e(nav["prod_ci_title"])}</span><span class="nav-prod-tag">SARIF</span></a>
            <div class="mobile-drawer-group">Culprit SQL</div>
            <a href="{e(ent_href)}">{e(nav["enterprise"])}</a>
            <a href="{e(db_href)}">{e(nav["databases"])}</a>
            <a href="{e(rel_href)}">{e(nav["releases"])}</a>
            <a href="/blog/">{e(nav["blog"])}</a>
            <a href="https://github.com/culpritsql" rel="noopener">GitHub (@culpritsql) ↗</a>
            <a class="mobile-cta" href="{e(cta_href)}">{e(nav["cta"])}</a>
          </div>
        </details>
      </div>
    </div>
  </header>"""


def render_footer(locale: LocaleSpec) -> str:
    b = BUNDLES[locale.code]
    foot = b["footer"]
    nav = b["nav"]
    home_href = localized_path(locale.code, "")
    return f"""<footer class="footer">
    <div class="wrap">
      <div class="foot-grid">
        <div class="foot-brand">
          <a class="brand" href="{e(home_href)}">
            <img src="/favicon.svg" width="24" height="24" alt="Culprit SQL logo">
            <span>Culprit <b>SQL</b></span>
          </a>
          <p>{e(foot["tagline"])}</p>
        </div>
        <div class="foot-col">
          <h3>{e(foot["col_platform"])}</h3>
          <ul>
            <li><a href="{e(localized_path(locale.code, 'studio'))}">{e(nav["prod_studio_title"])}</a></li>
            <li><a href="{e(localized_path(locale.code, 'fleet'))}">{e(nav["prod_fleet_title"])}</a></li>
            <li><a href="{e(localized_path(locale.code, 'explain'))}">{e(nav["prod_explain_title"])}</a></li>
            <li><a href="{e(localized_path(locale.code, 'ci'))}">{e(nav["prod_ci_title"])}</a></li>
            <li><a href="{e(localized_path(locale.code, 'enterprise'))}">{e(nav["enterprise"])}</a></li>
          </ul>
        </div>
        <div class="foot-col">
          <h3>{e(foot["col_developers"])}</h3>
          <ul>
            <li><a href="https://fleet.culpritsql.com">fleet.culpritsql.com ↗</a></li>
            <li><a href="{e(explain_url_for_locale(locale.code))}">explain.culpritsql.com ↗</a></li>
            <li><a href="https://github.com/culpritsql/culprit-vscode" rel="noopener">VS Code Extension</a></li>
            <li><a href="https://github.com/culpritsql/culprit-ci" rel="noopener">@culpritsql/ci</a></li>
            <li><a href="https://github.com/culpritsql/culprit-plans" rel="noopener">@culpritsql/plans</a></li>
          </ul>
        </div>
        <div class="foot-col">
          <h3>{e(foot["col_databases"])}</h3>
          <ul>
            <li><a href="/databases/sql-server/">SQL Server &amp; Azure SQL</a></li>
            <li><a href="/databases/postgresql/">PostgreSQL &amp; RDS</a></li>
            <li><a href="/databases/timescaledb/">TimescaleDB</a></li>
            <li><a href="/databases/yugabytedb/">YugabyteDB</a></li>
            <li><a href="/databases/mysql/">MySQL &amp; MariaDB</a></li>
            <li><a href="/databases/clickhouse/">ClickHouse</a></li>
            <li><a href="/databases/sqlite/">SQLite</a></li>
          </ul>
        </div>
        <div class="foot-col">
          <h3>{e(foot["col_resources"])}</h3>
          <ul>
            <li><a href="/blog/">{e(nav["blog"])}</a></li>
            <li><a href="{e(localized_path(locale.code, 'releases'))}">{e(nav["releases"])}</a></li>
            <li><a href="/releases/rss.xml">Releases RSS</a></li>
            <li><a href="/llms.txt">llms.txt</a></li>
            <li><a href="https://github.com/culpritsql/culprit-sql-studio/blob/main/LICENSE" rel="noopener">MIT License</a></li>
          </ul>
        </div>
      </div>
      <div class="foot-bottom">
        <span>{e(foot["copyright"])}</span>
        <span><a href="https://github.com/culpritsql" rel="noopener">github.com/culpritsql</a> · <a href="/llms-full.txt">llms-full.txt</a></span>
      </div>
    </div>
  </footer>
  <p class="sr-only" aria-live="polite" data-announce></p>"""


def render_document(
    *,
    locale: LocaleSpec,
    title: str,
    description: str,
    canonical_path: str,
    active_section: str,
    body_html: str,
    json_ld_graph: list[dict[str, Any]],
    localized_slug_for_hreflang: str | None = None,
    markdown_twin_path: str | None = None,
    og_type: str = "website",
) -> str:
    can_url = canonical_url(canonical_path)
    og_image = "https://culpritsql.com/og.jpg"

    hreflang_tags: list[str] = []
    og_alt_tags: list[str] = []
    if localized_slug_for_hreflang is not None:
        for alt in LOCALES:
            alt_url = canonical_url(localized_path(alt.code, localized_slug_for_hreflang))
            hreflang_tags.append(f'<link rel="alternate" hreflang="{e(alt.hreflang)}" href="{e(alt_url)}">')
            if alt.code != locale.code:
                og_alt_tags.append(f'<meta property="og:locale:alternate" content="{e(alt.og_locale)}">')
        x_def = canonical_url(localized_path("en", localized_slug_for_hreflang))
        hreflang_tags.append(f'<link rel="alternate" hreflang="x-default" href="{e(x_def)}">')

    md_link = (
        f'\n  <link rel="alternate" type="text/markdown" title="Markdown version" href="{e(canonical_url(markdown_twin_path))}">'
        if markdown_twin_path
        else ""
    )

    ld_payload = json.dumps({"@context": "https://schema.org", "@graph": json_ld_graph}, ensure_ascii=False, separators=(",", ":"))
    header_html = render_header(
        locale,
        active_section,
        lang_switcher_slug=localized_slug_for_hreflang if localized_slug_for_hreflang is not None else "",
    )
    footer_html = render_footer(locale)

    hreflang_block = ("\n  " + "\n  ".join(hreflang_tags)) if hreflang_tags else ""
    og_alt_block = ("\n  " + "\n  ".join(og_alt_tags)) if og_alt_tags else ""

    return f"""<!doctype html>
<html lang="{e(locale.html_lang)}">
<head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy" content="__CONTENT_SECURITY_POLICY__">
  <meta name="referrer" content="strict-origin-when-cross-origin">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1">
  <title>{e(title)}</title>
  <meta name="description" content="{e(description)}">
  <link rel="canonical" href="{e(can_url)}">{hreflang_block}{md_link}
  <link rel="alternate" type="application/rss+xml" title="Culprit SQL Releases RSS" href="https://culpritsql.com/releases/rss.xml">
  <link rel="alternate" type="application/rss+xml" title="Culprit SQL Blog RSS" href="https://culpritsql.com/blog/rss.xml">
  <meta name="color-scheme" content="light dark">
  <meta name="theme-color" content="#0f0e0c" media="(prefers-color-scheme: dark)">
  <meta name="theme-color" content="#fbfaf7" media="(prefers-color-scheme: light)">
  <meta property="og:type" content="{e(og_type)}">
  <meta property="og:site_name" content="Culprit SQL">
  <meta property="og:locale" content="{e(locale.og_locale)}">{og_alt_block}
  <meta property="og:url" content="{e(can_url)}">
  <meta property="og:title" content="{e(title)}">
  <meta property="og:description" content="{e(description)}">
  <meta property="og:image" content="{og_image}">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:image:type" content="image/jpeg">
  <meta property="og:image:alt" content="{e(title)}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{e(title)}">
  <meta name="twitter:description" content="{e(description)}">
  <meta name="twitter:image" content="{og_image}">
  <meta name="twitter:image:alt" content="{e(title)}">
  <link rel="icon" href="/favicon.ico" sizes="32x32">
  <link rel="icon" href="/favicon.svg" type="image/svg+xml">
  <link rel="apple-touch-icon" href="/apple-touch-icon.png">
  <link rel="preload" href="/fonts/Geist-Variable.woff2" as="font" type="font/woff2" crossorigin>
  <link rel="stylesheet" href="/styles.css">
  <script>{THEME_INIT_SCRIPT}</script>
  <script type="application/ld+json">{ld_payload}</script>
  <script src="/main.js" defer></script>
  <script defer src="https://plausible.io/js/pa-r5EkaWqkhpILeZH2bizcF.js"></script>
</head>
<body>
  {header_html}
  <main id="main">
{body_html}
  </main>
  {footer_html}
</body>
</html>
"""


def base_org_graph(locale: LocaleSpec) -> list[dict[str, Any]]:
    return [
        {
            "@type": "Organization",
            "@id": "https://culpritsql.com/#org",
            "name": "Culprit SQL",
            "url": "https://culpritsql.com/",
            "logo": "https://culpritsql.com/favicon.svg",
            "sameAs": SAME_AS_URLS,
        },
        {
            "@type": "WebSite",
            "@id": "https://culpritsql.com/#website",
            "url": "https://culpritsql.com/",
            "name": "Culprit SQL",
            "inLanguage": locale.html_lang,
            "publisher": {"@id": "https://culpritsql.com/#org"},
        },
    ]


def breadcrumb_node(items: list[tuple[str, str]]) -> dict[str, Any]:
    return {
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": idx + 1, "name": label, "item": canonical_url(path)}
            for idx, (label, path) in enumerate(items)
        ],
    }


def render_provider_cards(providers: list[dict[str, str]], locale: LocaleSpec) -> str:
    b = BUNDLES[locale.code]
    cov_label = b["common"]["coverage_setup"]
    cards: list[str] = []
    for p in providers:
        cards.append(
            f'<article class="engine" data-provider="{e(p["raw_slug"])}">'
            f'<span class="provider-logo" aria-hidden="true">'
            f'<img src="/images/providers/{e(p["raw_slug"])}.svg" alt="{e(p["label"])} logo" width="26" height="26" decoding="async">'
            f"</span>"
            f'<h3>{e(p["label"])}</h3>'
            f'<p>{e(p["summary"])}.</p>'
            f'<p class="engine-link"><a href="/databases/{e(p["url_slug"])}/">{e(cov_label)}</a></p>'
            f"</article>"
        )
    if len(providers) % 4 != 0:
        parity_href = f"{localized_path(locale.code, 'databases')}#parity"
        cards.append(
            '<article class="engine parity-card">'
            f'<span class="provider-logo" aria-hidden="true">{len(providers)}x18</span>'
            f'<h3>{e(b["databases"]["matrix_title"])}</h3>'
            f'<p>18 capability dimensions across {len(providers)} engines, plus pluggable Rust EngineSpec &amp; Check traits.</p>'
            f'<p class="engine-link"><a href="{e(parity_href)}">{e(b["common"]["learn_more"])}</a></p>'
            "</article>"
        )
    return "\n        ".join(cards)


def render_sample_diagnoses(locale: LocaleSpec, check_count: int) -> str:
    b = BUNDLES[locale.code]
    c = b["common"]
    h = b["home"]
    stats_html = "".join(
        f'<li><b>{e(val.replace("__CHECK_COUNT__", str(check_count)))}</b><span>{e(lbl)}</span></li>'
        for val, lbl in h["stats"]
    )
    return f"""    <section class="band-alt" aria-labelledby="diag-title">
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">{e(h["diag_kicker"])}</p>
          <h2 class="section-title" id="diag-title">{e(h["diag_title"])}</h2>
        </div>
        <div class="sample-tabs" role="tablist" aria-label="{e(h['diag_kicker'])}">
          <button type="button" role="tab" aria-selected="true" aria-controls="sample-loop" id="stab-loop">Loop join timeout · SQL Server</button>
          <button type="button" role="tab" aria-selected="false" aria-controls="sample-index" id="stab-index" tabindex="-1">Missing index · PostgreSQL</button>
          <button type="button" role="tab" aria-selected="false" aria-controls="sample-migration" id="stab-migration" tabindex="-1">Table-locking DDL · PR CI</button>
          <button type="button" role="tab" aria-selected="false" aria-controls="sample-blocking" id="stab-blocking" tabindex="-1">Idle blocker · Live</button>
        </div>
        <article class="finding-card" id="sample-loop" role="tabpanel" aria-labelledby="stab-loop">
          <div class="finding-head"><span class="sev">{e(c["high"])}</span><span class="share"><b>27%</b> DB time · <code>app/reports/order_summary.rb:84</code></span></div>
          <h3>A read from <code>order_items</code> times out on every run at ~15 s.</h3>
          <dl>
            <div><dt>{e(c["why"])}</dt><dd>The execution plan repeats an index lookup on <code>order_items</code> 1.5M times because <code>OPTION (LOOP JOIN)</code> overrides a single-pass hash join.</dd></div>
            <div><dt>{e(c["fix"])}</dt><dd>Remove <code>OPTION (LOOP JOIN)</code> so the optimizer selects a Hash Match join.</dd></div>
            <div><dt>{e(c["verify"])}</dt><dd><code>culprit verify prod</code> confirms 1 scan of <code>order_items</code> and sub-200 ms elapsed latency.</dd></div>
          </dl>
        </article>
        <article class="finding-card" id="sample-index" role="tabpanel" aria-labelledby="stab-index" hidden>
          <div class="finding-head"><span class="sev">{e(c["high"])}</span><span class="share"><b>19%</b> DB time · <code>public.orders</code> (480K rows)</span></div>
          <h3>Reads 480K rows of <code>orders</code> and discards 99.8% per customer lookup.</h3>
          <dl>
            <div><dt>{e(c["why"])}</dt><dd>No composite index covers <code>(customer_id, status)</code>; PostgreSQL executes 14,200 sequential scans per hour.</dd></div>
            <div><dt>{e(c["fix"])}</dt><dd><code>CREATE INDEX CONCURRENTLY orders_customer_id_status_idx ON public.orders (customer_id, status);</code></dd></div>
            <div><dt>{e(c["verify"])}</dt><dd><code>culprit explain prod --sql "…" --with-index "CREATE INDEX …"</code> proves an Index Scan via HypoPG at 0.04% cost before DDL runs.</dd></div>
          </dl>
        </article>
        <article class="finding-card" id="sample-migration" role="tabpanel" aria-labelledby="stab-migration" hidden>
          <div class="finding-head"><span class="sev">{e(c["high"])}</span><span class="share"><b>Blocks writes</b> on <code>orders</code> (12.4M rows, 340 writes/s) · <code>migrations/0042_add_idx.sql:3</code></span></div>
          <h3><code>CREATE INDEX</code> without <code>CONCURRENTLY</code> acquires a table lock blocking writes.</h3>
          <dl>
            <div><dt>{e(c["why"])}</dt><dd>Evaluated offline against the production snapshot: <code>orders</code> sustains 340 writes/s and 8.2 GB of heap pages.</dd></div>
            <div><dt>{e(c["fix"])}</dt><dd><code>culprit lint-sql prod --file migrations/ --write</code> rewrites the migration to <code>CREATE INDEX CONCURRENTLY</code>.</dd></div>
            <div><dt>{e(c["verify"])}</dt><dd><code>culprit lint-sql prod --file migrations/ --fail-on high --format sarif</code> exits 0 in GitHub Actions.</dd></div>
          </dl>
        </article>
        <article class="finding-card" id="sample-blocking" role="tabpanel" aria-labelledby="stab-blocking" hidden>
          <div class="finding-head"><span class="sev">{e(c["high"])}</span><span class="share"><b>2 sessions queued</b> · head wait 22 s on <code>public.accounts</code></span></div>
          <h3>Session 71 has been <code>idle in transaction</code> for 22 s, blocking checkout workers.</h3>
          <dl>
            <div><dt>{e(c["why"])}</dt><dd>Session 71 ran <code>UPDATE accounts SET balance = $1 WHERE id = $2</code> and held its row lock open across an external HTTP call.</dd></div>
            <div><dt>{e(c["fix"])}</dt><dd>Commit before external network calls and enforce <code>idle_in_transaction_session_timeout = '30s'</code>.</dd></div>
            <div><dt>{e(c["verify"])}</dt><dd><code>culprit blocking prod</code> confirms zero chains headed by <code>idle in transaction</code> sessions.</dd></div>
          </dl>
        </article>
        <ul class="stats">
          {stats_html}
        </ul>
      </div>
    </section>"""


def render_download_block(locale: LocaleSpec) -> str:
    c = BUNDLES[locale.code]["common"]
    return f"""      <div class="dl-grid">
        <article class="dl" data-os="mac">
          <span class="mono-badge amber">macOS 11+ · Universal</span>
          <h3 class="mt-10">macOS</h3>
          <p>Apple Silicon &amp; Intel · Signed and notarized by Apple</p>
          <a class="btn primary" href="https://github.com/culpritsql/culprit-sql-studio/releases/latest/download/Culprit-SQL-Studio-macOS.dmg">{e(c["download_mac"])} (.dmg)</a>
        </article>
        <article class="dl" data-os="windows">
          <span class="mono-badge">Windows 10 / 11 · x64</span>
          <h3 class="mt-10">Windows</h3>
          <p>Native WebView2 installer · 14 MB footprint</p>
          <a class="btn primary" href="https://github.com/culpritsql/culprit-sql-studio/releases/latest/download/Culprit-SQL-Studio-Windows-x64-setup.exe">{e(c["download_win"])} (.exe)</a>
        </article>
        <article class="dl" data-os="linux">
          <span class="mono-badge">Linux · x86_64</span>
          <h3 class="mt-10">Linux</h3>
          <p>Portable AppImage and Debian/Ubuntu package</p>
          <div class="dl-pair">
            <a class="btn primary" href="https://github.com/culpritsql/culprit-sql-studio/releases/latest/download/Culprit-SQL-Studio-Linux-x86_64.AppImage">AppImage</a>
            <a class="btn ghost" href="https://github.com/culpritsql/culprit-sql-studio/releases/latest/download/Culprit-SQL-Studio-Linux-amd64.deb">.deb</a>
          </div>
        </article>
      </div>
      <div class="terminal mt-24 text-left">
        <div class="term-bar">
          <span class="dots" aria-hidden="true"><i></i><i></i><i></i></span>
          <span class="label">culprit cli · macOS / Linux / Windows</span>
          <button class="copy" type="button" data-copy-target="cli-quick">{e(c["copy"])}</button>
        </div>
<pre id="cli-quick"><code><span class="p">$</span> culprit conn add prod --url 'postgres://monitor@db.internal:5432/orders'
<span class="p">$</span> culprit dashboard prod                                     <span class="c"># ranked root-cause summary</span>
<span class="p">$</span> culprit lint-sql prod --file migrations/ --format sarif    <span class="c"># offline PR migration gate</span>
<span class="p">$</span> claude mcp add culprit -- culprit mcp                      <span class="c"># 27 audited MCP agent tools</span></code></pre>
      </div>"""


def build_home_page(locale: LocaleSpec, *, version: str, check_count: int, providers: list[dict[str, str]]) -> str:
    b = BUNDLES[locale.code]
    h = b["home"]
    c = b["common"]
    nav = b["nav"]

    # Surface Switcher Tabs & Panels
    surface_visuals = {
        "studio": (
            f'<figure class="window"><div class="window-bar" aria-hidden="true"><i></i><i></i><i></i>'
            f"<span>Culprit SQL Studio v{e(version)} — mssql-prod</span></div>"
            + themed_picture(
                "dashboard",
                1389,
                868,
                "Culprit SQL Studio dashboard showing database time, plan regression marker, ranked findings, top queries, and waits",
                sizes="(max-width: 960px) calc(100vw - 64px), 640px",
                has_half=True,
                eager=True,
            )
            + "</figure>"
        ),
        "fleet": (
            '<div class="terminal"><div class="term-bar"><span class="dots" aria-hidden="true"><i></i><i></i><i></i></span>'
            '<span class="label">helm · culprit-collector -> ADX / ClickHouse</span>'
            f'<button class="copy" type="button" data-copy-target="surf-fleet">{e(c["copy"])}</button></div>'
            '<pre id="surf-fleet"><code><span class="p">$</span> helm upgrade --install culprit-collector \\\n'
            "    oci://ghcr.io/culpritsql/charts/culprit-collector \\\n"
            "    --namespace culprit --create-namespace \\\n"
            "    --set warehouse.driver=adx \\\n"
            "    --set recovery.provider=azure-blob\n\n"
            '<span class="c"># Query synthesized root-cause stream without touching prod DBs:</span>\n'
            '<span class="p">$</span> culprit resources --config archive.json --json\n'
            '<span class="p">$</span> culprit incident orders --around 2026-11-27T14:20:00Z --config archive.json</code></pre></div>'
        ),
        "ci": (
            '<div class="terminal"><div class="term-bar"><span class="dots" aria-hidden="true"><i></i><i></i><i></i></span>'
            '<span class="label">.github/workflows/sql-guardrails.yml</span>'
            f'<button class="copy" type="button" data-copy-target="surf-ci">{e(c["copy"])}</button></div>'
            '<pre id="surf-ci"><code>- uses: culpritsql/culprit-ci@v0.5.12\n'
            "  with:\n"
            "    snapshot: .culprit/prod-schema.json\n"
            "    files: migrations/**/*.sql\n"
            "    fail-on: high\n"
            "    sarif-file: culprit.sarif\n\n"
            '<span class="a">✓</span> <span class="out">SARIF 2.1.0 uploaded: blocked CREATE INDEX without CONCURRENTLY on public.orders (12.4M rows, 340 writes/s).</span></code></pre></div>'
        ),
        "explain": (
            f'<figure class="window"><div class="window-bar" aria-hidden="true"><i></i><i></i><i></i>'
            f"<span>explain.culpritsql.com — WebAssembly Plan Diff</span></div>"
            + themed_picture(
                "compare",
                869,
                541,
                "Side-by-side execution plan comparison highlighting 3,500x regression from Index Seek to Sequential Scan",
                sizes="(max-width: 960px) calc(100vw - 64px), 640px",
            )
            + "</figure>"
        ),
    }

    tab_buttons: list[str] = []
    tab_panels: list[str] = []
    for idx, surf in enumerate(h["surfaces"]):
        selected = "true" if idx == 0 else "false"
        tabindex = "0" if idx == 0 else "-1"
        hidden = "" if idx == 0 else " hidden"
        sid = surf["id"]
        tab_buttons.append(
            f'<button type="button" role="tab" aria-selected="{selected}" aria-controls="surf-panel-{sid}" '
            f'id="surf-tab-{sid}" tabindex="{tabindex}">'
            f'<span class="surface-num">{e(surf["num"])}</span>'
            f'<span class="surface-name">{e(surf["name"])}</span></button>'
        )
        bullets_html = "".join(
            f'<li>{e(item.replace("__CHECK_COUNT__", str(check_count)))}</li>' for item in surf["bullets"]
        )
        secondary_btn = ""
        if sid == "fleet":
            secondary_btn = f'<a class="btn ghost" href="https://fleet.culpritsql.com">{e(c["open_fleet"])}</a>'
        elif sid == "explain":
            secondary_btn = f'<a class="btn ghost" href="{e(explain_url_for_locale(locale.code))}">{e(c["open_explain"])}</a>'
        tab_panels.append(
            f'<div class="surface-panel" id="surf-panel-{sid}" role="tabpanel" aria-labelledby="surf-tab-{sid}"{hidden}>'
            f'<div class="surface-copy">'
            f'<h2>{e(surf["h2"])}</h2>'
            f'<p>{e(surf["p"])}</p>'
            f'<ul class="surface-bullets">{bullets_html}</ul>'
            f'<div class="surface-actions">'
            f'<a class="btn primary" href="{e(localized_path(locale.code, surf["cta_href"]))}">{e(surf["cta_label"])}</a>'
            f"{secondary_btn}"
            f"</div></div>"
            f'<div>{surface_visuals[sid]}</div>'
            f"</div>"
        )

    bp_stages_html = "".join(
        f'<div class="bp-stage"><span class="bp-step"><span>{e(st["step"])}</span>'
        f'<span class="bp-arrow" aria-hidden="true">{"→" if idx < 3 else "✓"}</span></span>'
        f'<h3>{e(st["title"])}</h3>'
        f'<p>{e(st["body"])}</p><div class="bp-tags">'
        + "".join(f"<span>{e(tg)}</span>" for tg in st["tags"])
        + "</div></div>"
        for idx, st in enumerate(h["bp_stages"])
    )
    bp_guarantees_html = "".join(f"<div><b>{e(k)}</b>{e(v)}</div>" for k, v in h["bp_guarantees"])
    ent_specs_html = "".join(
        f'<div class="spec-cell"><span class="spec-key">{e(cat)}</span><h3>{e(ttl)}</h3><p>{e(desc)}</p></div>'
        for cat, ttl, desc in h["ent_specs"]
    )
    faqs_html = "".join(
        f"<details><summary>{e(q)}</summary><p>{e(a)}</p></details>" for q, a in h["faqs"]
    )
    trust_html = "".join(f"<li>{e(t)}</li>" for t in h["trust"])

    studio_href = localized_path(locale.code, "studio")
    fleet_href = localized_path(locale.code, "fleet")
    explain_href = localized_path(locale.code, "explain")
    ci_href = localized_path(locale.code, "ci")

    body = f"""    <section class="hero" id="top">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="glow-layer" aria-hidden="true"><div class="glow"></div></div>
      <div class="wrap hero-grid">
        <p class="eyebrow"><span class="dot"></span> {e(h["eyebrow"])}</p>
        <h1>{e(h["h1"])}</h1>
        <p class="lede">{e(h["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="{e(studio_href)}#download" data-os-download
             data-label-mac="{e(c['download_mac'])}" data-label-windows="{e(c['download_win'])}" data-label-linux="{e(c['download_linux'])}">
            <span class="os-icon" aria-hidden="true"></span>
            <span data-os-label>{e(c["download_studio"])}</span>
          </a>
          <a class="btn ghost large" href="https://fleet.culpritsql.com">{e(c["open_fleet"])}</a>
          <a class="btn ghost large" href="{e(explain_url_for_locale(locale.code))}">{e(c["open_explain"])}</a>
          <a class="btn ghost large" href="https://github.com/culpritsql" rel="noopener">{e(c["view_github"])}</a>
        </div>
        <ul class="trust-pills">{trust_html}</ul>

        <div class="surface-deck">
          <div class="surface-tabs" role="tablist" aria-label="{e(nav['products'])}">
            {"".join(tab_buttons)}
          </div>
          {"".join(tab_panels)}
        </div>
      </div>
    </section>

    <section aria-labelledby="arch-title">
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">{e(h["arch_kicker"])}</p>
          <h2 class="section-title" id="arch-title">{e(h["arch_title"])}</h2>
          <p class="section-lede">{e(h["arch_lede"])}</p>
        </div>
        <div class="blueprint">
          <div class="blueprint-flow">{bp_stages_html}</div>
          <div class="blueprint-guarantees">{bp_guarantees_html}</div>
        </div>

        <div class="eco-grid mt-24">
          <a class="eco-cell" href="{e(studio_href)}">
            <span class="eco-cell-top"><h3>Culprit SQL Studio</h3><span class="mono-badge amber">v{e(version)}</span></span>
            <p>{e(nav["prod_studio_desc"])}</p>
            <span class="eco-cmd">macOS · Windows · Linux · CLI</span>
          </a>
          <a class="eco-cell" href="{e(fleet_href)}">
            <span class="eco-cell-top"><h3>Culprit Collector</h3><span class="mono-badge green">Helm · GHCR</span></span>
            <p>Bounded read-only pod streaming diagnoses to ADX or ClickHouse.</p>
            <span class="eco-cmd">oci://ghcr.io/culpritsql/charts/culprit-collector</span>
          </a>
          <a class="eco-cell" href="https://fleet.culpritsql.com">
            <span class="eco-cell-top"><h3>Culprit Fleet</h3><span class="mono-badge">0 DB Drivers</span></span>
            <p>{e(nav["prod_fleet_desc"])}</p>
            <span class="eco-cmd">fleet.culpritsql.com ↗</span>
          </a>
          <a class="eco-cell" href="{e(explain_url_for_locale(locale.code))}">
            <span class="eco-cell-top"><h3>Culprit Explain</h3><span class="mono-badge">Wasm · .xel</span></span>
            <p>{e(nav["prod_explain_desc"])}</p>
            <span class="eco-cmd">explain.culpritsql.com ↗</span>
          </a>
          <a class="eco-cell" href="{e(ci_href)}">
            <span class="eco-cell-top"><h3>VS Code Extension</h3><span class="mono-badge">IDE</span></span>
            <p>Inline SQL migration diagnostics, quick-fixes, and .sqlplan / .xel viewer.</p>
            <span class="eco-cmd">ext install culpritsql.culprit-sql</span>
          </a>
          <a class="eco-cell" href="https://github.com/culpritsql/culprit-ci" rel="noopener">
            <span class="eco-cell-top"><h3>@culpritsql/ci</h3><span class="mono-badge">SARIF 2.1</span></span>
            <p>Offline pull request migration &amp; query gate for GitHub Actions.</p>
            <span class="eco-cmd">uses: culpritsql/culprit-ci@v0.5.12</span>
          </a>
          <a class="eco-cell" href="https://github.com/culpritsql/culprit-test" rel="noopener">
            <span class="eco-cell-top"><h3>@culpritsql/test</h3><span class="mono-badge">Vitest · Jest</span></span>
            <p>Deterministic test matchers blocking N+1 loops and sequential scans.</p>
            <span class="eco-cmd">npm i -D @culpritsql/test</span>
          </a>
          <a class="eco-cell" href="https://github.com/culpritsql/culprit-plans" rel="noopener">
            <span class="eco-cell-top"><h3>@culpritsql/plans</h3><span class="mono-badge">npm SDK</span></span>
            <p>Embeddable Rust WebAssembly plan, diff, deadlock, and .xel parser.</p>
            <span class="eco-cmd">npm i @culpritsql/plans @culpritsql/plan-viewer</span>
          </a>
        </div>
      </div>
    </section>

{render_sample_diagnoses(locale, check_count)}

    <section aria-labelledby="ent-title">
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">{e(h["ent_kicker"])}</p>
          <h2 class="section-title" id="ent-title">{e(h["ent_title"])}</h2>
          <p class="section-lede">{e(h["ent_lede"])}</p>
        </div>
        <div class="spec-grid">{ent_specs_html}</div>
        <p class="more-link"><a href="{e(localized_path(locale.code, 'enterprise'))}">{e(c["learn_more"])}</a></p>
      </div>
    </section>

    <section class="band-alt" id="engines" aria-labelledby="db-title">
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">{e(h["db_kicker"])}</p>
          <h2 class="section-title" id="db-title">{e(h["db_title"])}</h2>
          <p class="section-lede">{e(h["db_lede"])}</p>
        </div>
        <div class="engine-grid">
          {render_provider_cards(providers, locale)}
        </div>
      </div>
    </section>

    <section class="faq" id="faq" aria-labelledby="faq-title">
      <div class="wrap faq-split">
        <div>
          <p class="section-kicker">Architecture &amp; Licensing</p>
          <h2 class="section-title" id="faq-title">{e(h["faq_title"])}</h2>
          <p class="section-lede">{e(h["ent_lede"])}</p>
          <p class="more-link"><a href="https://github.com/culpritsql/culprit-sql-studio/blob/main/docs/ARCHITECTURE.md" rel="noopener">ARCHITECTURE.md ↗</a></p>
        </div>
        <div class="faq-list">
          {faqs_html}
        </div>
      </div>
    </section>

    <section class="cta-band" id="download" aria-labelledby="cta-title">
      <div class="wrap">
        <h2 id="cta-title">{e(h["cta_title"])}</h2>
        <p>{e(h["cta_sub"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="{e(studio_href)}#download">{e(c["download_studio"])}</a>
          <a class="btn ghost large" href="https://fleet.culpritsql.com">{e(c["open_fleet"])}</a>
          <a class="btn ghost large" href="https://github.com/culpritsql" rel="noopener">{e(c["view_github"])}</a>
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [
        {
            "@type": "SoftwareApplication",
            "@id": "https://culpritsql.com/#app",
            "name": "Culprit SQL",
            "applicationCategory": "DeveloperApplication",
            "applicationSubCategory": "Database Performance & Root-Cause Engineering Platform",
            "operatingSystem": "macOS 11+, Windows 10/11, Linux, Kubernetes, WebAssembly",
            "softwareVersion": version,
            "isAccessibleForFree": True,
            "license": "https://opensource.org/licenses/MIT",
            "downloadUrl": "https://github.com/culpritsql/culprit-sql-studio/releases/latest",
            "releaseNotes": "https://culpritsql.com/releases/",
            "offers": {"@type": "Offer", "price": "0", "priceCurrency": "USD"},
            "publisher": {"@id": "https://culpritsql.com/#org"},
        },
        {
            "@type": "FAQPage",
            "inLanguage": locale.html_lang,
            "mainEntity": [
                {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}}
                for q, a in h["faqs"]
            ],
        },
    ]

    return render_document(
        locale=locale,
        title=h["title"],
        description=h["description"],
        canonical_path=localized_path(locale.code, ""),
        active_section="home",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="",
    )


def build_studio_page(locale: LocaleSpec, *, version: str, check_count: int) -> str:
    b = BUNDLES[locale.code]
    s = b["studio"]
    c = b["common"]
    eyebrow = s["eyebrow"].replace("__VERSION__", version)
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "studio")

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>Studio &amp; CLI</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(eyebrow)}</p>
        <h1>{e(s["h1"])}</h1>
        <p class="lede">{e(s["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="#download" data-os-download
             data-label-mac="{e(c['download_mac'])}" data-label-windows="{e(c['download_win'])}" data-label-linux="{e(c['download_linux'])}">
            <span class="os-icon" aria-hidden="true"></span>
            <span data-os-label>{e(c["download_studio"])}</span>
          </a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-sql-studio" rel="noopener">{e(c["view_github"])}</a>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="show">
          <div class="show-text">
            <p class="section-kicker">{check_count} Evidence-Gated Checks</p>
            <h2>Findings ranked by share of database time.</h2>
            <p>Runaway nested loops, parameter-sensitive plan regressions, missing or duplicate indexes, lock escalations, tempdb spills, subtransaction SLRU pressure, and table-locking DDL—each paired with concrete evidence, production-safe SQL, and verification.</p>
          </div>
          <figure class="shot">{themed_picture("finding", 660, 828, "Culprit SQL Studio finding detail showing share of DB time, root cause, safe fix, and verification")}</figure>
        </div>

        <div class="show flip">
          <div class="show-text">
            <p class="section-kicker">What Changed &amp; Regressions</p>
            <h2>Drag across any timeline window to isolate regressions.</h2>
            <p>Compare two intervals or saved snapshots and Culprit correlates plan switches, new query fingerprints, deployments, and statistics updates automatically.</p>
          </div>
          <figure class="shot">{themed_picture("what-changed", 660, 640, "Period comparison highlighting a 293x plan regression alongside deploy markers")}</figure>
        </div>

        <div class="show wide">
          <div class="show-text">
            <p class="section-kicker">Live Activity &amp; Blocking</p>
            <h2>Who is blocking whom right now, rendered as one causal tree.</h2>
            <p>Identifies the exact head blocker, open transaction age, lock mode, and queued victims—plus historical deadlock cycles from Query Store, ring buffers, or local <code>.xel</code> traces.</p>
          </div>
          <figure class="shot">{themed_picture("blocking", 1169, 298, "Live blocking tree showing session 71 idle in transaction for 22 seconds holding up downstream sessions", sizes="(max-width: 1200px) calc(100vw - 48px), 1169px")}</figure>
        </div>
      </div>
    </section>

    <section class="band-alt" id="agents">
      <div class="wrap">
        <div class="show">
          <div class="show-text">
            <p class="section-kicker">Model Context Protocol (MCP)</p>
            <h2>27 database performance tools for Claude Code, Codex &amp; Cursor.</h2>
            <p><code>culprit mcp</code> exposes findings, execution plans, HypoPG virtual index tests, offline migration linting, schema drift, and blocking trees. Connections default to stats-only; optional read-only SQL requires statement-by-statement human approval and logs to <code>culprit audit</code>.</p>
            <div class="mt-20">
              <figure class="shot">{themed_picture("approval", 697, 368, "Read-only SQL statement approval prompt for Claude Code with 200-row and 30-second caps")}</figure>
            </div>
          </div>
          <div>
            <div class="terminal">
              <div class="term-bar">
                <span class="dots" aria-hidden="true"><i></i><i></i><i></i></span>
                <div class="tabs" role="tablist" aria-label="Coding agent">
                  <button type="button" role="tab" aria-selected="true" aria-controls="setup-claude" id="tab-claude">Claude Code</button>
                  <button type="button" role="tab" aria-selected="false" aria-controls="setup-codex" id="tab-codex" tabindex="-1">Codex</button>
                  <button type="button" role="tab" aria-selected="false" aria-controls="setup-cursor" id="tab-cursor" tabindex="-1">Cursor</button>
                </div>
                <button class="copy" type="button" data-copy-target="setup">{e(c["copy"])}</button>
              </div>
<pre id="setup-claude" role="tabpanel" aria-labelledby="tab-claude" data-setup><code><span class="p">$</span> claude mcp add culprit -- culprit mcp
<span class="p">$</span> culprit agent init</code></pre>
<pre id="setup-codex" role="tabpanel" aria-labelledby="tab-codex" data-setup hidden><code><span class="c"># ~/.codex/config.toml</span>
[mcp_servers.culprit]
command = "culprit"
args = ["mcp"]</code></pre>
<pre id="setup-cursor" role="tabpanel" aria-labelledby="tab-cursor" data-setup hidden><code><span class="c">// ~/.cursor/mcp.json</span>
{{ "mcpServers": {{ "culprit": {{ "command": "culprit", "args": ["mcp"] }} }} }}</code></pre>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section id="download">
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">v{e(version)} · MIT License</p>
          <h2 class="section-title">Download Culprit SQL Studio &amp; CLI.</h2>
          <p class="section-lede">Checksums and third-party notices ship with every release on <a href="{e(localized_path(locale.code, 'releases'))}">{e(b["nav"]["releases"])}</a>.</p>
        </div>
{render_download_block(locale)}
      </div>
    </section>"""

    graph = base_org_graph(locale) + [
        breadcrumb_node([(c["home"], home_path), ("Studio & CLI", this_path)]),
        {
            "@type": "SoftwareApplication",
            "name": "Culprit SQL Studio",
            "applicationCategory": "DeveloperApplication",
            "operatingSystem": "macOS 11+, Windows 10/11, Linux",
            "softwareVersion": version,
            "downloadUrl": "https://github.com/culpritsql/culprit-sql-studio/releases/latest",
            "releaseNotes": "https://culpritsql.com/releases/",
            "isAccessibleForFree": True,
            "license": "https://opensource.org/licenses/MIT",
            "offers": {"@type": "Offer", "price": "0", "priceCurrency": "USD"},
        },
    ]
    return render_document(
        locale=locale,
        title=s["title"],
        description=s["description"],
        canonical_path=this_path,
        active_section="studio",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="studio",
    )


def build_fleet_page(locale: LocaleSpec, *, version: str) -> str:
    b = BUNDLES[locale.code]
    f = b["fleet"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "fleet")

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>Fleet &amp; Collector</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(f["eyebrow"])}</p>
        <h1>{e(f["h1"])}</h1>
        <p class="lede">{e(f["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="https://fleet.culpritsql.com">{e(c["open_fleet"])}</a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-fleet" rel="noopener">{e(c["view_github"])}</a>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="show">
          <div class="show-text">
            <p class="section-kicker">Zero-Replica Default · Bounded Budgets</p>
            <h2>Deploy inside your VPC with OCI Helm &amp; GHCR containers.</h2>
            <p><code>ghcr.io/culpritsql/culprit-collector</code> (<code>linux/amd64</code> &amp; <code>linux/arm64</code>) runs the exact same read-only native readers as Studio, seals captures to private Azure Blob or S3, and publishes synthesized tables to Azure Data Explorer or ClickHouse.</p>
          </div>
          <div class="terminal">
            <div class="term-bar"><span class="dots" aria-hidden="true"><i></i><i></i><i></i></span><span class="label">helm · culprit-collector v{e(version)}</span>
              <button class="copy" type="button" data-copy-target="fleet-helm">{e(c["copy"])}</button>
            </div>
<pre id="fleet-helm"><code><span class="p">$</span> helm install culprit-collector \\
    oci://ghcr.io/culpritsql/charts/culprit-collector \\
    --version {e(version)} \\
    --namespace culprit --create-namespace \\
    -f values.production.yaml

<span class="c"># Generate namespace-bound ADX / ClickHouse views &amp; Grafana pack:</span>
<span class="p">$</span> culprit collect queries --config archive.json --name Prod \\
    --history-from 2026-11-01T00:00:00Z --rules-version "$RULES" --analyzer-version "{e(version)}"</code></pre>
          </div>
        </div>

        <div class="section-head mt-48">
          <p class="section-kicker">Synthesized Warehouse Schema</p>
          <h2 class="section-title">Pre-computed diagnoses in your analytical cluster.</h2>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>Table</th><th>Purpose &amp; Verification Contract</th></tr></thead>
            <tbody>
              <tr><td><code>CulpritIncidentsV1</code></td><td>Recorded blocking chains, deadlock graphs, worker starvation, victim impact, evidenced culprit queries, and verification steps.</td></tr>
              <tr><td><code>CulpritFindingsV1</code></td><td>Ranked query, index, configuration, and cloud posture diagnoses with exact input capture digests and explicit check coverage.</td></tr>
              <tr><td><code>CulpritChangesV1</code></td><td>Plan switches, latency regressions, and schema changes across matching workload populations.</td></tr>
              <tr><td><code>CulpritCollectorConnectionsV1</code></td><td>Typed collector attempt telemetry, monotonic phase budgets, and quiescence proofs—with zero credentials or SQL payloads.</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="band-alt" id="dri-alerts">
      <div class="wrap">
        <div class="show">
          <div class="show-text">
            <p class="section-kicker">DRI On-Call Alerting · ADX &amp; ClickHouse</p>
            <h2>Page the DRI with the culprit SQL and fix already attached.</h2>
            <p><code>culprit collect queries</code> generates namespace-bound Kusto functions and ClickHouse <code>SQL SECURITY INVOKER</code> views over sealed diagnoses—plus an eight-panel Grafana dashboard and paused alert rules. Point Azure Monitor, Grafana, or <code>culprit notify</code> (PagerDuty, Microsoft Teams, Slack, webhooks) at your warehouse with zero live load on production databases.</p>
            <ul class="surface-bullets mt-20">
              <li><strong>Episode-scoped deduplication:</strong> group by <code>TargetId, OriginalScopeId, OriginalSource, EpisodeId, ControllerVersion</code> so follow-up enrichments never double-page on-call.</li>
              <li><strong>Replay-safe timestamps:</strong> backfilled captures preserve <code>FirstRecordedObservation</code> and never trigger false fresh-outage pages.</li>
              <li><strong>Separate availability guard:</strong> <code>ProdStatus</code> alerts on stale or partial evidence—zero incident rows alone never implies healthy collection.</li>
            </ul>
          </div>
          <div>
            <div class="terminal">
              <div class="term-bar">
                <span class="dots" aria-hidden="true"><i></i><i></i><i></i></span>
                <div class="tabs" role="tablist" aria-label="DRI alert queries">
                  <button type="button" role="tab" aria-selected="true" aria-controls="dri-adx-inc" id="tab-dri-inc">ADX · DRI Page</button>
                  <button type="button" role="tab" aria-selected="false" aria-controls="dri-adx-avail" id="tab-dri-avail" tabindex="-1">ADX · Availability</button>
                  <button type="button" role="tab" aria-selected="false" aria-controls="dri-ch-sql" id="tab-dri-ch" tabindex="-1">ClickHouse SQL</button>
                </div>
                <button class="copy" type="button" data-copy-target="active-tab">{e(c["copy"])}</button>
              </div>
<pre id="dri-adx-inc" role="tabpanel" aria-labelledby="tab-dri-inc"><code><span class="c">// Azure Monitor / Grafana Scheduled Query Rule (every 1m -> PagerDuty / Teams / Slack)</span>
<span class="c">// Deduplicate by: TargetId, OriginalScopeId, OriginalSource, EpisodeId, ControllerVersion</span>
ProdRecordedIncidentAlertsDuring("Orders production", ago(5m), now())
| project FirstRecordedObservation, TargetId, EpisodeId,
          Severity, Headline, CulpritSql, DeveloperAction</code></pre>
<pre id="dri-adx-avail" role="tabpanel" aria-labelledby="tab-dri-avail" hidden><code><span class="c">// Evidence freshness &amp; completeness guard (separate from incident alerts)</span>
ProdStatus("Orders production", now(), 30m)
| where State != "observed" or not(InputReadComplete) or array_length(ContextIssues) &gt; 0
| project State, InputReadComplete, ContextIssues</code></pre>
<pre id="dri-ch-sql" role="tabpanel" aria-labelledby="tab-dri-ch" hidden><code><span class="c">-- ClickHouse parameterized view (SQL SECURITY INVOKER)</span>
SELECT
  FirstRecordedObservation, TargetId, EpisodeId,
  Severity, Headline, CulpritSql, DeveloperAction
FROM ProdRecordedIncidentAlertsDuring(
  resource = 'Orders production',
  window_from = now64(9, 'UTC') - INTERVAL 5 MINUTE,
  window_until = now64(9, 'UTC')
);</code></pre>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="section-head">
          <p class="section-kicker">Architecture Comparison</p>
          <h2 class="section-title">MIT self-hosted telemetry vs. legacy per-instance DBM.</h2>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>Dimension</th><th>Culprit Fleet + Collector (MIT)</th><th>Legacy Commercial DBM / SaaS</th></tr></thead>
            <tbody>
              <tr><td>Licensing cost (100 DBs, 40 engineers)</td><td><strong>$0 license</strong> (MIT) + your own ClickHouse / ADX compute</td><td>$150,000 - $400,000+ / year in per-instance &amp; per-seat fees</td></tr>
              <tr><td>Web tier blast radius</td><td><strong>0 DB drivers compiled in web binary</strong> (enforced by CI)</td><td>Centralized SaaS or monolithic server holding live DB credentials</td></tr>
              <tr><td>Data residency</td><td>100% inside your VPC (ADX / ClickHouse + private Blob / S3)</td><td>Query texts, plans, and wait traces shipped to third-party cloud</td></tr>
              <tr><td>Coding agent integration</td><td>Built-in remote audited MCP endpoint + CLI JSON time-machine</td><td>Proprietary UI walls or un-audited API scraping</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [
        breadcrumb_node([(c["home"], home_path), ("Fleet & Collector", this_path)]),
        {
            "@type": "SoftwareApplication",
            "name": "Culprit Fleet",
            "url": "https://fleet.culpritsql.com/",
            "applicationCategory": "DeveloperApplication",
            "operatingSystem": "Linux, Kubernetes, Docker",
            "softwareVersion": version,
            "isAccessibleForFree": True,
            "license": "https://opensource.org/licenses/MIT",
            "offers": {"@type": "Offer", "price": "0", "priceCurrency": "USD"},
        },
    ]
    return render_document(
        locale=locale,
        title=f["title"],
        description=f["description"],
        canonical_path=this_path,
        active_section="fleet",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="fleet",
    )


def build_explain_page(locale: LocaleSpec) -> str:
    b = BUNDLES[locale.code]
    ex = b["explain"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "explain")
    live_explain = explain_url_for_locale(locale.code)

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>Explain &amp; Wasm SDKs</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(ex["eyebrow"])}</p>
        <h1>{e(ex["h1"])}</h1>
        <p class="lede">{e(ex["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="{e(live_explain)}">{e(c["open_explain"])}</a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-explain" rel="noopener">{e(c["view_github"])}</a>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="show">
          <div class="show-text">
            <p class="section-kicker">Air-Gapped Browser Execution</p>
            <h2>Plans, diffs, deadlocks, and binary <code>.xel</code> traces—zero uploads.</h2>
            <p>Drop a 40 MB SQL Server Extended Events <code>.xel</code> capture, an SSMS <code>.sqlplan</code>, PostgreSQL <code>EXPLAIN (ANALYZE, BUFFERS)</code>, MySQL/MariaDB JSON, or a deadlock graph. Everything executes inside your browser tab via WebAssembly with optional one-click SQL literal redaction for shareable <code>#s=z1</code> links.</p>
          </div>
          <figure class="shot">{themed_picture("compare", 869, 541, "Culprit Explain structural execution plan comparison")}</figure>
        </div>

        <div class="show flip">
          <div class="show-text">
            <p class="section-kicker">Embeddable TypeScript &amp; React SDKs</p>
            <h2>Ship the exact same viewer inside internal tools.</h2>
            <p><code>@culpritsql/plans</code> (Wasm parser, diff engine, deadlock &amp; <code>.xel</code> decoder) and <code>@culpritsql/plan-viewer</code> (interactive canvas &amp; operator inspector) publish to npm under MIT.</p>
          </div>
          <div class="terminal">
            <div class="term-bar"><span class="dots" aria-hidden="true"><i></i><i></i><i></i></span><span class="label">npm · @culpritsql/plans + @culpritsql/plan-viewer</span>
              <button class="copy" type="button" data-copy-target="wasm-sdk">{e(c["copy"])}</button>
            </div>
<pre id="wasm-sdk"><code><span class="p">$</span> npm install @culpritsql/plans @culpritsql/plan-viewer

<span class="c">// Parse plans or binary SQL Server .xel files in browser or Node:</span>
import {{ parsePlan, comparePlans, parseXel }} from "@culpritsql/plans";
import {{ PlanDiffViewer }} from "@culpritsql/plan-viewer";</code></pre>
          </div>
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [
        breadcrumb_node([(c["home"], home_path), ("Explain & Wasm", this_path)]),
        {
            "@type": "WebApplication",
            "name": "Culprit Explain",
            "url": live_explain,
            "applicationCategory": "DeveloperApplication",
            "operatingSystem": "Any (WebAssembly)",
            "isAccessibleForFree": True,
            "license": "https://opensource.org/licenses/MIT",
            "offers": {"@type": "Offer", "price": "0", "priceCurrency": "USD"},
        },
    ]
    return render_document(
        locale=locale,
        title=ex["title"],
        description=ex["description"],
        canonical_path=this_path,
        active_section="explain",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="explain",
    )


def build_ci_page(locale: LocaleSpec) -> str:
    b = BUNDLES[locale.code]
    ci_copy = b["ci"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "ci")

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>CI, IDE &amp; SDKs</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(ci_copy["eyebrow"])}</p>
        <h1>{e(ci_copy["h1"])}</h1>
        <p class="lede">{e(ci_copy["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="https://github.com/culpritsql/culprit-ci" rel="noopener">@culpritsql/ci on GitHub</a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-vscode" rel="noopener">VS Code Extension</a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-test" rel="noopener">@culpritsql/test</a>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="platform-grid">
          <article class="platform-card">
            <div class="platform-card-top"><span class="mono-badge amber">@culpritsql/ci</span><span class="mono-badge">SARIF 2.1.0</span></div>
            <h3>Offline Pull Request Migration Gates</h3>
            <p>Checks Flyway, Prisma, Rails, Django, Alembic, and raw SQL migrations against a saved catalog snapshot—catching missing <code>CONCURRENTLY</code>, volatile defaults, wide lock queues, and unindexed foreign keys without connecting CI runners to production.</p>
            <div class="platform-card-links"><a href="https://github.com/culpritsql/culprit-ci" rel="noopener">github.com/culpritsql/culprit-ci ↗</a></div>
          </article>
          <article class="platform-card">
            <div class="platform-card-top"><span class="mono-badge">VS Code</span><span class="mono-badge">Quick Fixes</span></div>
            <h3>Culprit SQL for VS Code</h3>
            <p>Inline squiggles on unsafe DDL and slow SQL patterns as you type, one-click <code>CONCURRENTLY</code> / lock-timeout rewrites, and integrated execution plan previews.</p>
            <div class="platform-card-links"><a href="https://github.com/culpritsql/culprit-vscode" rel="noopener">github.com/culpritsql/culprit-vscode ↗</a></div>
          </article>
          <article class="platform-card">
            <div class="platform-card-top"><span class="mono-badge">@culpritsql/test</span><span class="mono-badge">Vitest · Jest</span></div>
            <h3>Deterministic Query Regression Assertions</h3>
            <p>Wrap integration endpoints in <code>expectQueries()</code> to fail CI automatically when an ORM upgrade introduces N+1 query loops or sequential table scans.</p>
            <div class="platform-card-links"><a href="https://github.com/culpritsql/culprit-test" rel="noopener">github.com/culpritsql/culprit-test ↗</a></div>
          </article>
          <article class="platform-card">
            <div class="platform-card-top"><span class="mono-badge">@culpritsql/devtools</span><span class="mono-badge">Local HUD</span></div>
            <h3>In-App SQL Telemetry HUD</h3>
            <p>Drop-in development overlay showing per-request query counts, slow statements, duplicate calls, and 1-click plan inspection during local feature work.</p>
            <div class="platform-card-links"><a href="https://github.com/culpritsql/culprit-devtools" rel="noopener">github.com/culpritsql/culprit-devtools ↗</a></div>
          </article>
        </div>

        <div class="terminal mt-28">
          <div class="term-bar"><span class="dots" aria-hidden="true"><i></i><i></i><i></i></span><span class="label">GitHub Actions + Vitest Guardrails</span>
            <button class="copy" type="button" data-copy-target="ci-workflow">{e(c["copy"])}</button>
          </div>
<pre id="ci-workflow"><code><span class="c"># 1. Lint SQL migrations in GitHub Actions with SARIF annotations:</span>
<span class="p">$</span> npx @culpritsql/ci lint --snapshot .culprit/prod.json --files "migrations/**/*.sql" --sarif culprit.sarif

<span class="c"># 2. Assert query count and index usage in integration tests:</span>
<span class="p">$</span> npm install -D @culpritsql/test</code></pre>
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [breadcrumb_node([(c["home"], home_path), ("CI, IDE & SDKs", this_path)])]
    return render_document(
        locale=locale,
        title=ci_copy["title"],
        description=ci_copy["description"],
        canonical_path=this_path,
        active_section="ci",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="ci",
    )


def build_enterprise_page(locale: LocaleSpec, *, version: str) -> str:
    b = BUNDLES[locale.code]
    ent = b["enterprise"]
    h = b["home"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "enterprise")
    ent_specs_html = "".join(
        f'<div class="spec-cell"><span class="spec-key">{e(cat)}</span><h3>{e(ttl)}</h3><p>{e(desc)}</p></div>'
        for cat, ttl, desc in h["ent_specs"]
    )

    arch_svg = """        <div class="arch-diagram-frame">
          <div class="arch-diagram-bar">
            <span>Reference Production Topology · Collector -&gt; ADX / ClickHouse -&gt; Fleet</span>
            <span>0 Inbound DB Ports · 0 SQL Drivers in Web Binary</span>
          </div>
          <div class="arch-diagram-scroll">
            <svg class="arch-svg" viewBox="0 0 1180 316" role="img" aria-label="High-level architecture diagram showing production databases read by culprit-collector inside a private VPC, streaming synthesized diagnoses to Azure Data Explorer or ClickHouse and sealed captures to Blob or S3, consumed read-only by Culprit Fleet, Remote MCP, and DRI alerts">
              <defs>
                <marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="var(--accent)"/>
                </marker>
                <marker id="arr-muted" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="var(--text-3)"/>
                </marker>
              </defs>

              <!-- Zone 1: Private DB Subnet (Databases + Bounded Collector) -->
              <rect x="8" y="10" width="596" height="296" rx="12" fill="var(--bg-2)" stroke="var(--line-strong)" stroke-dasharray="5 4"/>
              <text x="24" y="32" class="svg-mono" font-size="10.5" font-weight="650" fill="var(--text-3)" letter-spacing="0.8">ZONE 1 · PRIVATE DATABASE VPC / VNET</text>

              <!-- Databases Stack -->
              <rect x="24" y="50" width="238" height="238" rx="10" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="40" y="74" font-size="13.5" font-weight="680" fill="var(--text)">Production Databases</text>
              <text x="40" y="92" class="svg-mono" font-size="10.5" fill="var(--text-3)">Zero host agents installed</text>

              <rect x="38" y="108" width="210" height="34" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="50" y="129" font-size="12" font-weight="600" fill="var(--text)">SQL Server &amp; Azure SQL</text>

              <rect x="38" y="150" width="210" height="34" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="50" y="171" font-size="12" font-weight="600" fill="var(--text)">PostgreSQL &amp; AWS RDS</text>

              <rect x="38" y="192" width="210" height="34" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="50" y="213" font-size="12" font-weight="600" fill="var(--text)">Timescale &amp; YugabyteDB</text>

              <rect x="38" y="234" width="210" height="34" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="50" y="255" font-size="11.5" font-weight="600" fill="var(--text)">MySQL, MariaDB &amp; ClickHouse</text>

              <!-- Arrow DB -> Collector -->
              <line x1="262" y1="169" x2="354" y2="169" stroke="var(--accent)" stroke-width="1.8" marker-end="url(#arr)"/>
              <text x="310" y="154" text-anchor="middle" class="svg-mono" font-size="9.5" font-weight="600" fill="var(--accent)">READ-ONLY</text>
              <text x="310" y="186" text-anchor="middle" class="svg-mono" font-size="9.5" fill="var(--text-3)">1s stmt cap</text>

              <!-- Collector Pod -->
              <rect x="358" y="50" width="230" height="238" rx="10" fill="var(--surface)" stroke="var(--brand)" stroke-width="1.8"/>
              <rect x="372" y="64" width="76" height="20" rx="4" fill="var(--brand-soft)"/>
              <text x="410" y="78" text-anchor="middle" class="svg-mono" font-size="10" font-weight="700" fill="var(--text)">COLLECTOR</text>
              <text x="372" y="104" font-size="14" font-weight="700" fill="var(--text)">culprit-collector</text>
              <text x="372" y="121" class="svg-mono" font-size="10.5" fill="var(--text-3)">Helm / K8s · 0-replica default</text>

              <rect x="372" y="136" width="202" height="40" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="384" y="154" font-size="11.5" font-weight="630" fill="var(--text)">Bounded Native Readers</text>
              <text x="384" y="168" class="svg-mono" font-size="10" fill="var(--text-3)">DMVs · pg_stat · Quiescence</text>

              <rect x="372" y="184" width="202" height="40" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="384" y="202" font-size="11.5" font-weight="630" fill="var(--text)">Pure Rust Diagnosis Engine</text>
              <text x="384" y="216" class="svg-mono" font-size="10" fill="var(--text-3)">Local synthesis · 0 DB load</text>

              <rect x="372" y="232" width="202" height="40" rx="6" fill="var(--bg-2)" stroke="var(--line)"/>
              <text x="384" y="250" font-size="11.5" font-weight="630" fill="var(--text)">Durable Local Outbox</text>
              <text x="384" y="264" class="svg-mono" font-size="9.5" fill="var(--text-3)">Survives restarts &amp; backfill</text>

              <!-- Outbound TLS Arrows Collector -> Storage -->
              <line x1="588" y1="122" x2="660" y2="122" stroke="var(--accent)" stroke-width="1.8" marker-end="url(#arr)"/>
              <line x1="588" y1="234" x2="660" y2="234" stroke="var(--text-3)" stroke-width="1.6" marker-end="url(#arr-muted)"/>
              <text x="634" y="112" text-anchor="middle" class="svg-mono" font-size="9.5" font-weight="600" fill="var(--accent)">HTTPS</text>

              <!-- Zone 2: Customer Warehouse & Object Store -->
              <rect x="664" y="10" width="250" height="296" rx="12" fill="var(--bg-2)" stroke="var(--line-strong)"/>
              <text x="680" y="32" class="svg-mono" font-size="10.5" font-weight="650" fill="var(--text-3)" letter-spacing="0.8">ZONE 2 · YOUR WAREHOUSE</text>

              <rect x="680" y="50" width="218" height="132" rx="10" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="694" y="74" font-size="13.5" font-weight="700" fill="var(--text)">ADX or ClickHouse</text>
              <text x="694" y="92" class="svg-mono" font-size="10.5" fill="var(--green)">Synthesized Gold Streams</text>
              <text x="694" y="114" class="svg-mono" font-size="11" fill="var(--text-2)">• CulpritIncidentsV1</text>
              <text x="694" y="132" class="svg-mono" font-size="11" fill="var(--text-2)">• CulpritFindingsV1</text>
              <text x="694" y="150" class="svg-mono" font-size="11" fill="var(--text-2)">• CulpritChangesV1</text>
              <text x="694" y="168" class="svg-mono" font-size="10.5" fill="var(--text-3)">+ 13 typed raw evidence tables</text>

              <rect x="680" y="196" width="218" height="92" rx="10" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="694" y="220" font-size="13.5" font-weight="700" fill="var(--text)">Azure Blob or AWS S3</text>
              <text x="694" y="238" class="svg-mono" font-size="10.5" fill="var(--text-3)">Conditional-object recovery</text>
              <text x="694" y="258" class="svg-mono" font-size="10.5" fill="var(--text-2)">Sealed captures, plans &amp; .xel</text>
              <text x="694" y="274" class="svg-mono" font-size="10" fill="var(--text-3)">Cryptographic EOF verification</text>

              <!-- Compile-Time Airwall Line -->
              <line x1="938" y1="16" x2="938" y2="300" stroke="var(--green)" stroke-width="1.5" stroke-dasharray="4 4"/>
              <line x1="898" y1="96" x2="958" y2="96" stroke="var(--accent)" stroke-width="1.8" marker-end="url(#arr)"/>
              <line x1="898" y1="172" x2="958" y2="172" stroke="var(--accent)" stroke-width="1.8" marker-end="url(#arr)"/>
              <line x1="898" y1="248" x2="958" y2="248" stroke="var(--accent)" stroke-width="1.8" marker-end="url(#arr)"/>

              <!-- Zone 3: Zero-Driver Triage & Alerting -->
              <rect x="962" y="10" width="210" height="296" rx="12" fill="var(--bg-2)" stroke="var(--line-strong)"/>
              <text x="976" y="32" class="svg-mono" font-size="10.5" font-weight="650" fill="var(--green)" letter-spacing="0.8">0 DB DRIVERS COMPILED</text>

              <rect x="976" y="50" width="182" height="72" rx="9" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="990" y="73" font-size="13" font-weight="700" fill="var(--text)">Culprit Fleet Web</text>
              <text x="990" y="90" class="svg-mono" font-size="10.5" fill="var(--text-2)">Entra OIDC · RBAC</text>
              <text x="990" y="106" class="svg-mono" font-size="10" fill="var(--accent)">fleet.culpritsql.com</text>

              <rect x="976" y="134" width="182" height="72" rx="9" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="990" y="157" font-size="13" font-weight="700" fill="var(--text)">Remote Audited MCP</text>
              <text x="990" y="174" class="svg-mono" font-size="10.5" fill="var(--text-2)">Claude · Codex · Cursor</text>
              <text x="990" y="190" class="svg-mono" font-size="10" fill="var(--text-3)">POST /mcp + audit log</text>

              <rect x="976" y="218" width="182" height="70" rx="9" fill="var(--surface)" stroke="var(--line-strong)"/>
              <text x="990" y="241" font-size="13" font-weight="700" fill="var(--text)">DRI On-Call Alerts</text>
              <text x="990" y="258" class="svg-mono" font-size="10.5" fill="var(--text-2)">ADX / Grafana / notify</text>
              <text x="990" y="274" class="svg-mono" font-size="10" fill="var(--text-3)">PagerDuty · Teams · Slack</text>
            </svg>
          </div>
        </div>"""

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>{e(b["nav"]["enterprise"])}</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(ent["eyebrow"])}</p>
        <h1>{e(ent["h1"])}</h1>
        <p class="lede">{e(ent["lede"])}</p>
        <div class="hero-cta">
          <a class="btn primary large" href="https://github.com/culpritsql/culprit-sql-studio/blob/main/docs/ARCHITECTURE.md" rel="noopener">Read ARCHITECTURE.md ↗</a>
          <a class="btn ghost large" href="https://github.com/culpritsql/culprit-sql-studio/releases/download/v{e(version)}/THIRD-PARTY-LICENSES.txt" rel="noopener">THIRD-PARTY-LICENSES.txt ↗</a>
        </div>
      </div>
    </section>

    <section>
      <div class="wrap">
{arch_svg}
        <div class="spec-grid">{ent_specs_html}</div>

        <div class="section-head mt-52">
          <p class="section-kicker">Trust Boundaries</p>
          <h2 class="section-title">Component isolation &amp; credential scope matrix.</h2>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>Component</th><th>Network Placement</th><th>Credentials Held</th><th>Database Write Capability</th></tr></thead>
            <tbody>
              <tr><td>Culprit SQL Studio / CLI</td><td>Engineer workstation / Bastion</td><td>OS Keychain, Entra CLI, AWS IAM (per-connection)</td><td>Read-only scans; editor requires explicit human approval on prod</td></tr>
              <tr><td><code>culprit collect</code></td><td>Private VPC / AKS / EKS pod</td><td>Scoped monitoring login + Outbox write token</td><td>None — catalog/stats readers only with 1 s statement timeout</td></tr>
              <tr><td>Culprit Fleet Web</td><td>Internal ingress / SSO</td><td>Read-only ADX or ClickHouse + Blob/S3 reader</td><td><strong>Impossible</strong> — zero SQL database drivers compiled into binary</td></tr>
              <tr><td>Culprit Explain</td><td>Browser tab (Wasm)</td><td>None</td><td><strong>Impossible</strong> — zero network sockets or database drivers</td></tr>
              <tr><td><code>@culpritsql/ci</code></td><td>GitHub Actions / CI runner</td><td>None (reads offline schema snapshot JSON)</td><td><strong>Impossible</strong> — offline static analysis only</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [breadcrumb_node([(c["home"], home_path), (b["nav"]["enterprise"], this_path)])]
    return render_document(
        locale=locale,
        title=ent["title"],
        description=ent["description"],
        canonical_path=this_path,
        active_section="enterprise",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="enterprise",
    )


def build_databases_index_page(
    locale: LocaleSpec,
    *,
    providers: list[dict[str, str]],
    parity_matrix_html: str,
) -> str:
    b = BUNDLES[locale.code]
    db = b["databases"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "databases")

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>{e(b["nav"]["databases"])}</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(db["eyebrow"])}</p>
        <h1>{e(db["h1"])}</h1>
        <p class="lede">{e(db["lede"])}</p>
      </div>
    </section>

    <section>
      <div class="wrap">
        <div class="engine-grid">
          {render_provider_cards(providers, locale)}
        </div>

        <div class="section-head mt-56" id="parity">
          <p class="section-kicker">✓ Implemented · ◐ Scoped / Alternative Source · — Not Supplied</p>
          <h2 class="section-title">{e(db["matrix_title"])}</h2>
        </div>
        <div class="prose">
          {parity_matrix_html}
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [breadcrumb_node([(c["home"], home_path), (b["nav"]["databases"], this_path)])]
    return render_document(
        locale=locale,
        title=db["title"],
        description=db["description"],
        canonical_path=this_path,
        active_section="databases",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="databases",
    )


def build_provider_detail_page(provider: dict[str, Any]) -> str:
    locale = LOCALE_BY_CODE["en"]
    c = BUNDLES["en"]["common"]
    url_slug = provider["url_slug"]
    raw_slug = provider["raw_slug"]
    label = provider["label"]
    this_path = f"/databases/{url_slug}/"
    md_twin = f"/docs/providers/{raw_slug}.md"

    toc_items = "".join(f'<li><a href="#{e(hid)}">{e(htitle)}</a></li>' for hid, htitle in provider["toc"])

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb">
          <a href="/">Culprit SQL</a><span>/</span><a href="/databases/">Databases</a><span>/</span><span>{e(label)}</span>
        </nav>
        <div class="doc-hero-badges" data-provider="{e(raw_slug)}">
          <span class="provider-logo"><img src="/images/providers/{e(raw_slug)}.svg" width="40" height="40" alt="{e(label)} logo"></span>
          <span class="mono-badge">Provider Guide · Read-Only</span>
          <a class="mono-badge" href="{e(md_twin)}">{e(c["raw_markdown"])} (.md)</a>
        </div>
        <h1>{e(label)}: Performance Diagnostics, Plans &amp; Setup</h1>
        <p class="lede">{e(provider["summary"])}.</p>
      </div>
    </section>

    <section>
      <div class="wrap doc-layout">
        <aside class="doc-toc" aria-label="{e(c['on_this_page'])}">
          <p class="doc-toc-title">{e(c["on_this_page"])}</p>
          <ul>{toc_items}</ul>
        </aside>
        <article class="prose">
          {provider["html"]}
        </article>
      </div>
    </section>"""

    desc = summarize_provider_desc(label, provider["summary"])
    graph = base_org_graph(locale) + [
        breadcrumb_node([("Home", "/"), ("Databases", "/databases/"), (label, this_path)]),
        {
            "@type": "TechArticle",
            "headline": f"{label} Performance Diagnostics, Setup & Coverage — Culprit SQL",
            "description": desc,
            "url": canonical_url(this_path),
            "author": {"@id": "https://culpritsql.com/#org"},
            "publisher": {"@id": "https://culpritsql.com/#org"},
        },
    ]
    return render_document(
        locale=locale,
        title=f"{label} Slow Query, Plan & Setup Guide · Culprit SQL",
        description=desc,
        canonical_path=this_path,
        active_section="databases",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang=None,
        markdown_twin_path=md_twin,
        og_type="article",
    )


def summarize_provider_desc(label: str, summary: str) -> str:
    base = f"{label} setup, required read-only permissions, execution plan coverage, live activity, and evidence rules in Culprit SQL ({summary})."
    if len(base) > 158:
        base = base[:155].rsplit(" ", 1)[0].rstrip(".,;(") + "."
    return base


def render_release_assets(assets: list[dict[str, Any]], github_url: str) -> str:
    chips: list[str] = []
    priority = [a for a in assets if not a["name"].endswith(".sig") and a["name"] != "latest.json"]
    for a in priority[:8]:
        chips.append(
            f'<a class="asset-chip" href="{e(a["url"])}" rel="noopener">'
            f'<span>{e(a["name"])}</span><span>({e(format_bytes(a["size"]))})</span></a>'
        )
    chips.append(f'<a class="asset-chip" href="{e(github_url)}" rel="noopener">GitHub Release ↗</a>')
    return '<div class="release-assets">' + "".join(chips) + "</div>"


def build_releases_index_page(locale: LocaleSpec, *, releases: list[dict[str, Any]], studio_version: str) -> str:
    b = BUNDLES[locale.code]
    rel_copy = b["releases"]
    c = b["common"]
    home_path = localized_path(locale.code, "")
    this_path = localized_path(locale.code, "releases")

    tiles_html: list[str] = []
    for tile in latest_by_product(releases, studio_version):
        ext = ' rel="noopener"' if tile["href"].startswith("https://") and "culpritsql.com" not in tile["href"] else ""
        tiles_html.append(
            f'<a class="latest-tile" href="{e(tile["href"])}"{ext}>'
            f'<span class="latest-tile-head"><h3>{e(tile["title"])}</h3><span class="latest-ver">{e(tile["version"])}</span></span>'
            f'<span class="latest-meta">{e(tile["meta"])}</span>'
            f"</a>"
        )

    entries_html: list[str] = []
    for r in releases:
        slug = r.get("slug")
        grp = r["group"]
        if slug:
            rendered_body, _ = render_markdown(r["body"], strip_h1=True, demote_h2_to_h3=True)
            perm_href = f"/releases/{slug}/"
            entries_html.append(
                f'<article class="release-entry" data-release-group="{e(grp)}" id="{e(r["product"])}-{e(r["tag"])}">'
                f'<div class="release-meta">'
                f'<span class="mono-badge amber">{e(r["productLabel"])}</span>'
                f'<span class="release-tag">{e(r["tag"])}</span>'
                f'<time class="release-date" datetime="{e(r["dateShort"])}">{e(r["dateShort"])}</time>'
                f'<a class="perm-link" href="{e(perm_href)}">{e(rel_copy["permalink_label"])}</a>'
                f"</div>"
                f'<div class="release-body prose">'
                f'<h2><a href="{e(perm_href)}">{e(r["name"])}</a></h2>'
                f"{rendered_body}"
                f'{render_release_assets(r.get("assets", []), r["url"])}'
                f"</div></article>"
            )
        else:
            entries_html.append(
                f'<article class="release-entry compact" data-release-group="{e(grp)}">'
                f'<div class="release-meta release-meta-row">'
                f'<span class="mono-badge">{e(r["product"])}</span>'
                f'<time class="release-date" datetime="{e(r["dateShort"])}">{e(r["dateShort"])}</time>'
                f"</div>"
                f'<div class="release-compact-row">'
                f'<strong><a href="{e(r["url"])}" rel="noopener">{e(r["name"])}</a></strong>'
                f'<span class="release-compact-tag">{e(r["tag"])} · <a href="{e(r["url"])}" rel="noopener">GitHub Release &amp; SHA256SUMS ↗</a></span>'
                f"</div></article>"
            )

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{e(home_path)}">Culprit SQL</a><span>/</span><span>{e(b["nav"]["releases"])}</span></nav>
        <p class="eyebrow"><span class="dot"></span> {e(rel_copy["eyebrow"])}</p>
        <h1>{e(rel_copy["h1"])}</h1>
        <p class="lede">{e(rel_copy["lede"])}</p>
      </div>
    </section>

    <section>
      <div class="wrap">
        <p class="section-kicker">{e(rel_copy["latest_kicker"])}</p>
        <div class="latest-matrix">
          {"".join(tiles_html)}
        </div>

        <div class="release-filters">
          <div class="filter-pills" role="group" aria-label="Filter releases">
            <button type="button" aria-pressed="true" data-release-filter="all">{e(rel_copy["filter_all"])}</button>
            <button type="button" aria-pressed="false" data-release-filter="studio">{e(rel_copy["filter_studio"])}</button>
            <button type="button" aria-pressed="false" data-release-filter="vscode">{e(rel_copy["filter_vscode"])}</button>
            <button type="button" aria-pressed="false" data-release-filter="ci">{e(rel_copy["filter_ci"])}</button>
            <button type="button" aria-pressed="false" data-release-filter="wasm">{e(rel_copy["filter_wasm"])}</button>
          </div>
          <a class="btn ghost" href="/releases/rss.xml">{e(c["rss_feed"])} (RSS)</a>
        </div>

        <div class="release-stream">
          {"".join(entries_html)}
        </div>
      </div>
    </section>"""

    graph = base_org_graph(locale) + [breadcrumb_node([(c["home"], home_path), (b["nav"]["releases"], this_path)])]
    return render_document(
        locale=locale,
        title=rel_copy["title"],
        description=rel_copy["description"],
        canonical_path=this_path,
        active_section="releases",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang="releases",
    )


def build_release_detail_page(release: dict[str, Any]) -> str:
    locale = LOCALE_BY_CODE["en"]
    slug = release["slug"]
    this_path = f"/releases/{slug}/"
    md_twin = f"/releases/{slug}.md"
    rendered_body, toc = render_markdown(release["body"], strip_h1=True)
    toc_html = (
        '<aside class="doc-toc" aria-label="Sections"><p class="doc-toc-title">Release Sections</p><ul>'
        + "".join(f'<li><a href="#{e(hid)}">{e(htitle)}</a></li>' for hid, htitle in toc)
        + '<li><a href="#artifacts">Downloads &amp; Checksums</a></li></ul></aside>'
    )

    body = f"""    <section class="hero subpage-hero">
      <div class="arch-grid" aria-hidden="true"></div>
      <div class="wrap hero-sub-grid">
        <nav class="breadcrumbs" aria-label="Breadcrumb">
          <a href="/">Culprit SQL</a><span>/</span><a href="/releases/">Releases</a><span>/</span><span>{e(release["tag"])}</span>
        </nav>
        <div class="doc-hero-badges">
          <span class="mono-badge amber">{e(release["productLabel"])}</span>
          <span class="mono-badge">{e(release["tag"])}</span>
          <time class="mono-badge" datetime="{e(release['dateShort'])}">{e(release["dateShort"])}</time>
          <a class="mono-badge" href="{e(md_twin)}">Markdown (.md)</a>
        </div>
        <h1>{e(release["name"])} Release Notes</h1>
        <p class="lede">{e(release["summary"])}</p>
      </div>
    </section>

    <section>
      <div class="wrap doc-layout">
        {toc_html}
        <article class="prose">
          {rendered_body}
          <h2 id="artifacts">Downloads &amp; SHA-256 Checksums</h2>
          {render_release_assets(release.get("assets", []), release["url"])}
          <p class="mt-24"><a href="/releases/">← All Culprit SQL releases</a></p>
        </article>
      </div>
    </section>"""

    page_title = f"{release['name']} Release Notes · Culprit SQL"
    if len(page_title) > 62:
        page_title = f"{release['productLabel']} {release['tag']} · Culprit SQL"

    graph = base_org_graph(locale) + [
        breadcrumb_node([("Home", "/"), ("Releases", "/releases/"), (release["tag"], this_path)]),
        {
            "@type": "TechArticle",
            "headline": f"{release['name']} Release Notes",
            "datePublished": release["dateShort"],
            "description": release["summary"],
            "url": canonical_url(this_path),
            "author": {"@id": "https://culpritsql.com/#org"},
            "publisher": {"@id": "https://culpritsql.com/#org"},
        },
    ]
    return render_document(
        locale=locale,
        title=page_title,
        description=release["summary"],
        canonical_path=this_path,
        active_section="releases",
        body_html=body,
        json_ld_graph=graph,
        localized_slug_for_hreflang=None,
        markdown_twin_path=md_twin,
        og_type="article",
    )
