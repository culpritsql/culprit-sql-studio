# TimescaleDB

[Engine parity](../parity.md)

Culprit detects TimescaleDB automatically through its PostgreSQL provider and queries the extension's public catalog views and size functions directly—keeping logical hypertables, compressed columnstore chunks, continuous aggregates and background jobs distinct from ordinary PostgreSQL heaps.

## Connecting

Choose **PostgreSQL** in Studio or on the CLI when connecting to self-hosted TimescaleDB or Tiger Cloud. Culprit detects the installed `timescaledb` extension and displays both the TimescaleDB and PostgreSQL versions.

```sh
# Self-hosted TimescaleDB or Tiger Cloud (TLS defaults to sslmode=verify-full)
culprit conn add metrics --env prod --url 'postgres://culprit_reader@tsdb.example.com:5432/metrics'

# Through an SSH jump host
culprit conn add metrics-priv --env prod --ssh bastion@10.0.1.5 --url 'postgres://culprit_reader@10.0.3.20:5432/metrics'
```

Connection store credentials, OS keychain secrets, SSH jump hosts (`--ssh`), IDE connection imports (`culprit conn import`), live sessions, `EXPLAIN` and saved-scan comparisons work identically to the [PostgreSQL provider](postgres.md).

## Permissions and setup

Grant PostgreSQL's read-only statistics role plus `USAGE` / `SELECT` on TimescaleDB's public informational views and size functions:

```sql
CREATE ROLE culprit_reader WITH LOGIN PASSWORD '<strong-password>';
GRANT CONNECT ON DATABASE metrics TO culprit_reader;
GRANT pg_read_all_stats TO culprit_reader;

-- Inside the target database (if workload history or virtual index testing is needed):
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;
```

- **Non-intrusive discovery**: Culprit inspects the existing `timescaledb` extension and its schema; if a view or size function is restricted, only that TimescaleDB capability is marked unavailable with a clear note. Culprit never installs extensions, alters compression/retention policies, or triggers background jobs.
- **Job history visibility**: Note that PostgreSQL's `pg_monitor` / `pg_read_all_stats` roles alone do not bypass TimescaleDB's owner filtering on some job history views—partial visibility is labeled explicitly rather than reported as zero failures.

## Workload and plans

Statement counters, query fingerprints, saved-scan deltas, virtual index testing (`hypopg`) and JSON/CSV server-log diagnostics use the [PostgreSQL workload sources](postgres.md#workload-and-plans). Counter windows reflect `pg_stat_statements` history—never an interval guessed from chunk creation dates.

- **Chunk-to-hypertable plan attribution**: During `EXPLAIN` and plan analysis, Culprit resolves physical `_timescaledb_internal` chunk scans back to their owning logical hypertable so findings and index candidates point to the table engineers actually maintain.
- **Safe unknowns**: If an internal chunk cannot be mapped to a readable hypertable catalog entry, Culprit withholds generated DDL rather than emitting an index script against an internal chunk table.

## Live activity and waits

Live sessions, lock-wait graphs, 1 Hz session wait sampling, progress views and non-production cancel/terminate actions use the [PostgreSQL live sources](postgres.md#live-activity-and-waits). Deadlock counts come from `pg_stat_database`, and full deadlock graphs can be decoded offline from PostgreSQL JSON/CSV logs via `culprit logs`.

### Background jobs

Opt into TimescaleDB background-job and continuous-aggregate diagnostics during a scan (`culprit scan metrics --collect-native-failures`) or a targeted capture (`culprit collect once metrics --collection jobs`):

| View / catalog | Captured facts and bounds |
|---|---|
| `timescaledb_information.jobs` & `job_stats` | Schedules, current state, total runs and exact failure counters (up to 256 job rows) |
| `job_history` & `job_errors` | Recent retained outcomes and error codes when installed on the server version (up to 256 run rows) |
| `continuous_aggregates` | View/source/materialization names plus `materialized_only`, compression and granular-refresh flags (up to 64 rows; [catalog reference](https://www.tigerdata.com/docs/reference/timescaledb/informational-views/continuous_aggregates)) |

- **Strict read budget**: Capped at 512 total rows across all views inside a 1-second statement timeout and 5-second total deadline. Culprit probes column availability before querying and never reads view SQL definitions, watermark functions or user rows.
- **Offline inspection**: View captured job health in Studio under **Health**, on the CLI via `--analysis jobs`, or through MCP's `waits` summary. Configuration flags describe current catalog settings—not refresh lag or worker ownership.

## Schema and indexes

Culprit queries `timescaledb_information.hypertables`, `dimensions` and `chunks`, together with `approximate_row_count()`, `hypertable_detailed_size()`, `chunks_detailed_size()` and `hypertable_index_size()` in the extension's schema:

- **Unified logical hypertable view**: Studio's **Schema** browser lists each hypertable once, aggregating estimated rows and storage across all rowstore and compressed columnstore chunks. Expand **Managed chunks** to inspect per-chunk sizes, row estimates and compression state (paginated 20 chunks at a time).
- **Hypertable-aware index DDL & `lint-sql`**: PostgreSQL rejects `CREATE INDEX CONCURRENTLY` on hypertables. Both the index advisor and `culprit lint-sql` emit `CREATE INDEX ... ON hypertable (...) WITH (timescaledb.transaction_per_chunk)` for non-unique hypertable indexes while keeping `CONCURRENTLY` for ordinary tables, and verify that unique indexes include all partitioning dimensions. Note that [per-chunk index builds](https://github.com/timescale/Tiger-Data-Docs/blob/main/src/content/docs/reference/timescaledb/hypertables/create_index.mdx) still lock the active chunk being built.

## Limits and evidence

- **No false "unused index" claims on hypertables**: Parent hypertable entries in `pg_stat_user_tables` and `pg_stat_user_indexes` stay at zero because reads/writes hit child chunks. Culprit marks logical hypertable scan/usage counters unknown instead of flagging parent indexes as unused or grading empty parent heaps for bloat.
- **Internal schemas excluded**: `_timescaledb_*` backing tables, compressed chunk relations and continuous-aggregate materialization tables are excluded from standalone schema/maintenance findings.
- **Graceful partial coverage**: If `timescaledb_information.hypertables` is unreadable, Culprit withholds table/index storage grades and suppresses `CONCURRENTLY` DDL on unresolved relations rather than treating hypertables as empty heaps.
- **DDL export & background collector**: Studio's DDL tab notes that recreating a hypertable requires `create_hypertable()` and policy exports rather than plain `CREATE TABLE`. Background `culprit collect` shares PostgreSQL's bounded wire reader for native relation metrics (without aggregating logical hypertable sizes in background polls); see [collector source coverage](../collector.md#other-provider-source-coverage) and the shared [evidence rules](coverage.md).

## Verification

Verified against **TimescaleDB 2.31.0** on **PostgreSQL 17.11** using `public.sensor_readings` (**120,000 rows across 14 chunks, 7 compressed**) alongside ordinary heap tables (`fixtures/timescale-demo.report.json`). Reproduce the database workload and background-job test cases locally:

- Disposable database & compression fixture: [`dev/timescale/README.md`](../../dev/timescale/README.md)
- Retained job-failure & permission test cases: [`dev/timescale/jobs.md`](../../dev/timescale/jobs.md)
- Extension reader implementation: [`culprit-postgres/src/timescale.rs`](../../crates/providers/culprit-postgres/src/timescale.rs)
- Live fixture screenshots: [TimescaleDB dashboard](../screenshots/timescale-dashboard.png) · [Hypertable with rowstore and compressed chunks](../screenshots/timescale-hypertable.png)
