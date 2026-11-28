# SQLite

[Engine parity](../parity.md)

Culprit opens SQLite database files strictly read-only (`SQLITE_OPEN_READ_ONLY` + `PRAGMA query_only = ON`). Schema inspection, `sqlite_stat1` planner statistics, bounded `dbstat` storage analysis and `EXPLAIN QUERY PLAN` work immediately with zero application changes; statement workload history comes from an optional framework or JSONL query log (`crates/culprit-querylog`).

## Connecting

Select **SQLite** in Studio and pick a `.sqlite3` / `.db` file, or register the path on the CLI:

```sh
# Add a read-only SQLite file connection
culprit conn add app --env prod --url db/production.sqlite3

# Explicitly attach or disable an application query log
culprit conn set app --query-log log/production.log
culprit conn set app --no-query-log
```

File connections store local paths and require no network credentials. When `--query-log` is not set, Culprit automatically checks beside the database file, `./log/`, `./logs/`, and `../log/` for a matching log (`production.sqlite3` → `production.log`), while refusing to pair an environment-named database (`production.sqlite3`) with another environment's log (`development.log`).

## Permissions and setup

Culprit needs filesystem read permission on the database file (plus its `-wal` / `-shm` companions when WAL mode is active) and on any attached query log.

- **Zero application instrumentation required**: Culprit never installs hooks, triggers, extensions or driver wrappers, and never runs `WAL` checkpoints or `VACUUM`.
- **Connection-local pragmas**: Settings persisted in the database header (`journal_mode`, `auto_vacuum`, `page_size`, `user_version`) are read directly; per-connection pragmas (`synchronous`, `foreign_keys`, `cache_size`, `busy_timeout`) belong to your application's own connections, so Culprit explains how to verify them in application code rather than guessing from its own connection.

## Workload and plans

Because SQLite embeds no shared Query Store or `pg_stat_statements` memory across OS processes, Culprit reconstructs statement workload from your application's query log—fingerprinting SQL shapes, redacting literals, tallying calls/total/avg/max latency, and tracking structured `SQLITE_BUSY` / error counts separately from timed runs.

For execution plans, Culprit runs `EXPLAIN QUERY PLAN` read-only against the database schema (and imports sqlite3 CLI tree or pipe-delimited EQP text in Studio, CLI and [explain.culpritsql.com](https://explain.culpritsql.com)), surfacing full-table scans (`SCAN`), index searches (`SEARCH`), temporary B-tree sorts (`USE TEMP B-TREE`) and transient `AUTOMATIC COVERING INDEX` builds.

### If your framework already logs SQL

Culprit parses standard framework SQL logs out of the box with no format changes:

| Framework | Logger configuration |
|---|---|
| Ruby on Rails | Standard `debug` log level (default in development) |
| Django | `django.db.backends` logger at `DEBUG` |
| Entity Framework Core | `Microsoft.EntityFrameworkCore.Database.Command` at `Information` |
| Phoenix / Ecto | Standard Ecto `debug` query logger |

Point Culprit at any log file with `culprit conn set app --query-log /path/to/app.log` (or **Add query log** in Studio), or analyze a log standalone without a saved connection via `culprit logs`.

### Logging queries yourself

For applications using raw `sqlite3` without an ORM (FastAPI, Flask, Go, Rust or CLI tools), emit one JSON line (`JSONL`) per query:

```json
{"sql": "SELECT id, title FROM listings WHERE city = ? AND price < ? LIMIT ?", "ms": 4.2, "at": "2026-10-16T09:12:03.120+00:00"}
```

- `sql`: Parameterized SQL (`?` / `:name` placeholders keep user values out of the log file; Culprit also strips literals during ingestion).
- `ms`: Elapsed milliseconds across **both `execute()` and row iteration** (`sqlite3` steps lazily as rows are fetched).
- `at` *(optional)*: ISO 8601 timestamp with UTC offset (enables per-day rate calculations and window labels; without timestamps, Culprit shows percentage shares only).
- `request_id` *(optional)*: Correlates repeated statement shapes within one HTTP request/job for high-confidence N+1 detection.

```python
import json, time
from datetime import datetime, timezone

log = open("app.log", "a", buffering=1)

def query(conn, sql, params=()):
    start = time.perf_counter()
    rows = conn.execute(sql, params).fetchall()
    log.write(json.dumps({
        "sql": sql,
        "ms": round((time.perf_counter() - start) * 1000, 3),
        "at": datetime.now(timezone.utc).isoformat(),
    }) + "\n")
    return rows
```

### What the report says about a log

- **Ranked by total recorded time**: Distinct `WHERE` / `ORDER BY` shapes appear side by side with exact run counts and latency distributions.
- **Honest windowing**: Reports cite the exact span covered by the log (*"over the 50 minutes the Rails log covers"*), never claiming *"the last 50 minutes"*.
- **Streaming privacy**: Logs are streamed line by line; raw literals, free-form error text and raw `request_id` tokens never enter the saved report, and queries referencing tables absent from the opened SQLite file are excluded.

## Live activity and waits

SQLite is an in-process embedded library with no server session table, wait-event DMV or live lock graph. Studio and CLI mark live session/wait capabilities unavailable rather than displaying empty `0` dashboards, and keep the SQL editor strictly read-only on SQLite files.

## Schema and indexes

| Snapshot area | SQLite source | How Culprit reads it |
|---|---|---|
| Engine & header | `sqlite_version()`, `PRAGMA compile_options`, `page_size`, `journal_mode`, `auto_vacuum`, `user_version` | Reports persistent file header state alongside the linked SQLite library version |
| Tables & columns | `sqlite_schema`, `PRAGMA table_list`, `PRAGMA table_xinfo` | Preserves ordinary, `WITHOUT ROWID`, generated-column and virtual tables while excluding internal FTS/R-Tree shadow tables from standalone table checks |
| Row counts & sizes | Single-pass `dbstat` virtual table (within read budget) or `sqlite_stat1` | Never runs `SELECT COUNT(*)` on user tables; leaf cells count rowid rows, unknown stays `None` (never `0`) if over budget |
| Indexes & stats | `PRAGMA index_list`, `PRAGMA index_xinfo`, `sqlite_stat1` | Preserves partial-index `WHERE` clauses, expression indexes and per-index `ANALYZE` statistics coverage |
| Foreign keys | `PRAGMA foreign_key_list` | Detects unindexed child foreign keys that cause parent `DELETE` / `UPDATE` table scans |
| Free space & WAL | `PRAGMA page_count`, `freelist_count` + filesystem `.sqlite3-wal` stat | Measures freelist fragmentation and on-disk WAL size without checkpointing or truncating the WAL |

- **Schema & migration checks**: Flags unindexed foreign keys, duplicate/prefix-redundant indexes, missing `ANALYZE` (`sqlite_stat1`) statistics, freelist bloat and planner `AUTOMATIC INDEX` warnings. Because SQLite keeps no index-read counters, Culprit **never** flags an index as unused. `culprit lint-sql migrations/ --format sarif` lints SQLite schema migrations offline in CI and VS Code.

## Limits and evidence

- **Bounded `dbstat` read budgets (writers never wait)**: Full-file `dbstat` page inspection is capped at **4 GiB and 3 seconds** in WAL mode, or **128 MiB and 500 ms** with a rollback journal (where an open reader holds a shared lock that blocks writers). Files exceeding those bounds skip `dbstat` cleanly and report why.
- **Background telemetry (`culprit collect`)**: Exports **10 bounded `sqlite.*` metadata recipes** with a **250 ms** metadata ceiling—verifying file inode/replacement identity before and after each read, releasing the read snapshot before paging output, and skipping `dbstat` and default/DDL bodies. See [collector source coverage](../collector.md#other-provider-source-coverage) and [evidence rules](coverage.md).
- **N+1 attribution**: High call counts alone do not trigger an N+1 finding unless corroborated by explicit `request_id` correlation or repeated loop evidence.

## Verification

Verified against SQLite **3.53.2** using the reproducible fixture and framework query logs in [`dev/sqlite/README.md`](../../dev/sqlite/README.md)—covering WAL vs rollback-journal read budgets, inode replacement detection, `query_only` enforcement, EQP parsing and log correlation.

- Declared SQLite sources: [`culprit-sqlite/src/sources.rs`](../../crates/providers/culprit-sqlite/src/sources.rs)
- Query-log parser (`Rails`, `Django`, `EF Core`, `Ecto`, `JSONL`): [`crates/culprit-querylog`](../../crates/culprit-querylog)
- Native error codes: [SQLite result and extended error codes](https://www.sqlite.org/rescode.html)
