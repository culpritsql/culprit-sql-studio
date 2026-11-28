"""Locale metadata and route registry matching explain.culpritsql.com (9 languages)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LocaleSpec:
    code: str
    hreflang: str
    html_lang: str
    og_locale: str
    native_label: str
    short_label: str


LOCALES: tuple[LocaleSpec, ...] = (
    LocaleSpec("en", "en", "en", "en_US", "English", "EN"),
    LocaleSpec("zh", "zh-Hans", "zh-Hans", "zh_CN", "简体中文", "中文"),
    LocaleSpec("ja", "ja", "ja", "ja_JP", "日本語", "日本語"),
    LocaleSpec("ko", "ko", "ko", "ko_KR", "한국어", "한국어"),
    LocaleSpec("ru", "ru", "ru", "ru_RU", "Русский", "RU"),
    LocaleSpec("de", "de", "de", "de_DE", "Deutsch", "DE"),
    LocaleSpec("es", "es", "es", "es_ES", "Español", "ES"),
    LocaleSpec("fr", "fr", "fr", "fr_FR", "Français", "FR"),
    LocaleSpec("pt", "pt-BR", "pt-BR", "pt_BR", "Português", "PT"),
)

LOCALE_BY_CODE: dict[str, LocaleSpec] = {loc.code: loc for loc in LOCALES}

# All routes pre-rendered across every locale (8 routes x 9 locales = 72 localized pages)
LOCALIZED_SLUGS: tuple[str, ...] = (
    "",
    "studio",
    "fleet",
    "explain",
    "ci",
    "enterprise",
    "databases",
    "releases",
)

# Canonical URL slugs for docs/providers/*.md
PROVIDER_URL_SLUGS: dict[str, str] = {
    "mssql": "sql-server",
    "postgres": "postgresql",
    "timescale": "timescaledb",
    "yugabyte": "yugabytedb",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "clickhouse": "clickhouse",
    "sqlite": "sqlite",
}


def localized_path(locale_code: str, slug: str = "") -> str:
    prefix = "" if locale_code == "en" else f"/{locale_code}"
    suffix = f"/{slug}/" if slug else "/"
    return f"{prefix}{suffix}"


def canonical_url(path: str) -> str:
    clean = "/" + path.lstrip("/")
    return f"https://culpritsql.com{clean}"


def explain_url_for_locale(locale_code: str, sub: str = "") -> str:
    lang_seg = "" if locale_code == "en" else f"{locale_code}/"
    sub_seg = f"{sub.strip('/')}/" if sub.strip("/") else ""
    return f"https://explain.culpritsql.com/{lang_seg}{sub_seg}"
