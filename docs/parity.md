# Engine parity

Culprit runs one analysis pipeline, workspace model, execution-plan viewer, migration linter, fleet collector and MCP toolset across seven database engines. What each scan or capture can prove depends on the measurements that engine records. Select a provider below for connection setup, read-only `GRANT` statements, workload and plan sources, live diagnostics, schema coverage, operational limits and verified releases.

## Providers

| Database | Connection target | Main evidence |
|---|---|---|
| [SQL Server](providers/mssql.md) | SQL Server, Azure SQL Database, Managed Instance and Amazon RDS | Query Store intervals, plan cache, DMVs, Showplan XML and Extended Events (`.xel` / XML) |
| [PostgreSQL](providers/postgres.md) | PostgreSQL 15–18, Amazon RDS, Aurora and Azure Flexible Server | `pg_stat_statements`, catalog and I/O views, `EXPLAIN`, `hypopg` and JSON/CSV logs |
| [TimescaleDB](providers/timescale.md) | PostgreSQL connection; `timescaledb` detected automatically | PostgreSQL sources plus logical hypertable storage, chunk compression and background job views |
| [YugabyteDB](providers/yugabyte.md) | Dedicated `yugabyte://` / YSQL over DocDB provider | Native YSQL/DocDB RPC counters, `DIST` plans, `yb_pg_stat_plans`, ASH and tablet metadata |
| [MySQL](providers/mysql.md) | MySQL 5.7–8.4, Amazon RDS, Aurora and Azure Flexible Server | `performance_schema` digests and histograms, `sys`, JSON/tree `EXPLAIN` and slow query log |
| [MariaDB](providers/mariadb.md) | MySQL / MariaDB connection; MariaDB detected automatically | `performance_schema`, `slow_log` table, `userstat`, `ANALYZE FORMAT=JSON` and Galera status |
| [SQLite](providers/sqlite.md) | Local read-only SQLite file (`query_only`) | Catalog pragmas, bounded `dbstat`, `sqlite_stat1`, `EXPLAIN QUERY PLAN` and framework query logs |

Every provider guide follows the same seven sections:

1. **Connecting** — URLs, TLS defaults, cloud IAM / Entra ID, SSH jump hosts and IDE connection import
2. **Permissions and setup** — Least-privilege read-only `GRANT` SQL and optional server telemetry setup
3. **Workload and plans** — Statement history, runtime spread, estimated vs measured plans and virtual indexes
4. **Live activity and waits** — Active sessions, lock chains, wait sampling, deadlocks and opt-in diagnostics
5. **Schema and indexes** — Catalog inspection, index advice, storage layout and offline SQL migration linting
6. **Limits and evidence** — Connection safety bounds, background collector coverage and strict attribution rules
7. **Verification** — Reproducible Docker/file fixtures, verified server versions and source declarations

## Feature overview

**✓** supported natively · **◐** partial, version-dependent or supplied by an alternate source · **—** not recorded by the engine.
Actual availability always depends on server version, enabled telemetry sources and login privileges.

| Feature | SQL Server | PostgreSQL | TimescaleDB | YugabyteDB | MySQL | MariaDB | SQLite |
|---|---|---|---|---|---|---|---|
| Statement workload | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ◐ query log |
| Timelines and period comparison | ✓ intervals | ◐ scans | ◐ scans | ◐ matching scans | ◐ scans / slow log | ◐ scans / slow log | — |
| Estimated plans / access paths | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ◐ EQP access paths |
| Explicitly measured execution plans | ✓ | ✓ | ✓ | ✓ DIST | ◐ 8.0.18+ | ✓ ANALYZE | — |
| Retained last actual execution plan | ◐ 2019+ opt-in | — | — | — | — | — | — |
| Virtual indexes (`--with-index`) | — | ✓ `hypopg` | ✓ `hypopg` | ✓ `hypopg` | — | — | — |
| Live sessions and blocking chains | ✓ | ✓ | ✓ | ◐ native scoped | ✓ | ✓ | — |
| Live statement / operation progress | ✓ operator profiles | ◐ progress views | ◐ progress views | ◐ backfill / COPY | ◐ plan / stages | ◐ SHOW ANALYZE | — |
| Recorded waits / activity | ✓ Query Store waits | ◐ timed I/O | ◐ PostgreSQL I/O | ◐ weighted ASH | ◐ counters / lock time | ◐ counters / lock time | — |
| Live wait sample | ✓ counter deltas | ✓ sessions | ✓ sessions | ✓ sessions | ◐ counters / sessions | ◐ counters / sessions | — |
| Deadlock evidence | ✓ `.xel` / XML / XE | ◐ count + JSON/CSV log | ◐ count + JSON/CSV log | — | ◐ latest InnoDB | ◐ latest InnoDB | — |
| Index usage and cleanup safety | ✓ | ✓ | ◐ ordinary tables | ◐ global views | ✓ (`INVISIBLE`) | ◐ `performance_schema` / `userstat` (`IGNORED`) | — |
| Health, storage and posture | ✓ | ✓ | ◐ logical + PG | ◐ DocDB / nodes | ✓ | ✓ + Galera | ◐ file + WAL |
| Offline SQL migration linting (SARIF) | ✓ | ✓ | ✓ hypertable-aware | ✓ DocDB-aware | ✓ | ✓ | ✓ |
| Offline log / `.xel` diagnostics | ✓ `.xel` & XML | ✓ JSON & CSV | ✓ JSON & CSV | — | — | — | ✓ framework / JSONL |
| Cloud auth & managed posture | ✓ Entra / Azure / RDS | ✓ AWS IAM / Entra | ◐ PostgreSQL auth | — | ✓ AWS IAM / Entra | ✓ AWS IAM / RDS | — |
| Background fleet collector (ADX / CH) | ✓ | ✓ (24 datasets) | ◐ PostgreSQL base | ✓ (16 datasets) | ✓ | ✓ + `mariadb.galera` | ✓ (10 datasets) |
| SSH jump hosts (`--ssh`) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |

Saved scans, offline replay, plan comparison, schema drift, SARIF migration linting (`culprit lint-sql`), Markdown/JSON exports and [MCP agent guardrails](agents.md) work identically across providers.

## Evidence and missing data

Unavailable or permission-denied sources never silently become `0` or a clean bill of health: Culprit locks only the affected checks and explains which view, extension or `GRANT` was missing. Cumulative counters, sampled sessions, retained event buffers and current storage allocations keep their own timestamps, reset epochs and units.

Per-day rates require at least one hour of dated window (labeled "a day" only after 20 hours), and unused-index verdicts require at least seven days of continuous index-usage counters across every read-serving replica or node. Two-period comparisons verify matching database identity, contributor roster and counter reset history before computing deltas. Read the shared [evidence rules](providers/coverage.md) for the exact thresholds behind every check.

<a id="verified-on"></a>
<a id="versions"></a>

## Verification

Every provider page documents its verified engine releases, disposable local fixture (`dev/*`), and authoritative source table (`src/sources.rs`). Version-specific fallbacks and managed-cloud restrictions are listed alongside the capabilities they govern.

For CLI commands, Studio workflows and exports, see the [user guide](guide.md). For the five-stage diagnostic pipeline (`sources → snapshot → prepare → detect → refine → rank`), see [architecture](ARCHITECTURE.md).
