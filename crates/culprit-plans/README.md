# culprit-plans

Portable database plan and deadlock parsing, comparison and plan exports.

## What it does

- Reads SQL Server, PostgreSQL, MySQL, MariaDB, YugabyteDB and SQLite plans.
- Parses database deadlock text and graphs.
- Compares supplied plans using Culprit’s existing evidence rules.
- Exports plans as Markdown, self-contained HTML or native plan text.

## What it depends on

The shared Core model, XML parsing and JSON parsing. It opens no database connection, reads no credentials and performs no network or filesystem work. Database providers call these same parsers after collecting native data.

## Entry points

`parse_plan`, `parse_native_plan`, `compare_plans`, `parse_deadlocks`, and `export::export_plan`.

Exports receive their timestamp from the caller. Literal redaction stays enabled by default.

## Languages and WebAssembly

The default `locales` feature includes the available translations. Disable default features to embed English only. The `culprit-wasm` wrapper exposes the same operations to browser projects; see [`packages/culprit-wasm`](../../packages/culprit-wasm/README.md).

Before packaging WebAssembly, run `python3 dev/licenses.py` from the repository root. The browser package reuses its generated third-party notices.
