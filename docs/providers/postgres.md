# PostgreSQL

[Engine parity](../parity.md)

Culprit inspects self-hosted PostgreSQL, Amazon RDS, Amazon Aurora PostgreSQL, Azure Database for PostgreSQL Flexible Server and Google Cloud SQL using read-only catalog and statistics views. Cumulative workload history comes from `pg_stat_statements` paired with Culprit's saved scans for clean time-window deltas.

## Connecting

Select **PostgreSQL** in Studio or supply a `postgres://` URL, `libpq` key-value string or JDBC URL on the CLI. Existing connections from DBeaver, DataGrip, pgAdmin or VS Code can be imported with `culprit conn import`.

```sh
# Standard connection (remote TLS defaults to sslmode=verify-full)
culprit conn add shop --env prod --url 'postgres://culprit_reader@db.example.com:5432/shop'

# Amazon RDS / Aurora PostgreSQL with short-lived AWS IAM auth token
culprit conn add shop-rds --env prod --auth aws-iam --url 'postgres://culprit_reader@shop.cluster-xyz.us-east-1.rds.amazonaws.com:5432/shop'

# Azure Database for PostgreSQL with Microsoft Entra ID
culprit conn add shop-az --env prod --auth entra --url 'postgres://culprit_reader@shop.postgres.database.azure.com:5432/shop'

# Through an SSH jump host
culprit conn add shop-priv --env prod --ssh ubuntu@bastion.example.com --url 'postgres://culprit_reader@10.0.4.12:5432/shop'
```

- **TLS & credentials**: Passwords stay in the OS keychain; AWS IAM (`--auth aws-iam`) and Microsoft Entra ID (`--auth entra`) mint short-lived tokens per session. Managed cloud posture (RDS parameters, CloudWatch metrics, unencrypted session checks via `pg_stat_ssl`) enriches health views when cloud access is configured.
- **Engine routing**: [TimescaleDB](timescale.md) uses this provider and is detected automatically when the `timescaledb` extension is installed. [YugabyteDB](yugabyte.md) uses its own dedicated provider (`--engine yugabyte`); connecting to YugabyteDB with the PostgreSQL provider stops cleanly before calling heap storage APIs.

## Permissions and setup

Grant PostgreSQL's built-in `pg_read_all_stats` role (or `pg_monitor`) so Culprit can read query texts across roles and inspect session/storage statistics without superuser access:

```sql
CREATE ROLE culprit_reader WITH LOGIN PASSWORD '<strong-password>';
GRANT CONNECT ON DATABASE shop TO culprit_reader;
GRANT pg_read_all_stats TO culprit_reader;

-- Run inside the target database for workload history and zero-cost virtual index testing:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;
```

- **Recommended server settings**: Enable `track_io_timing = on` (data-file and temp I/O timings) and `pg_stat_statements.track_planning = on` (planner overhead) when permitted. On managed services such as RDS where `ALTER SYSTEM` is unavailable, Culprit emits safe database-scoped tuning advice (`ALTER DATABASE shop SET ...`) where applicable.
- **Privilege-safe `EXPLAIN`**: Planning a query in PostgreSQL can invoke immutable/inlined SQL or PL/pgSQL functions. Culprit refuses elevated generic planning when untrusted roles' reachable functions could execute with the monitoring role's privileges, and always wraps plan checks in read-only transactions that roll back.

## Workload and plans

| Capability | PostgreSQL source and behavior |
|---|---|
| Statement statistics | Cumulative `pg_stat_statements` calls, total/min/max/mean/stddev time, rows, shared/local/temp blocks, I/O timings, WAL bytes, planning and JIT counters |
| Period comparison | Computes exact deltas between Culprit's saved scans (`culprit changes`) after verifying matching database identity and `pg_stat_statements_info.stats_reset` |
| Automatic plan capture | Safe generic plans via `EXPLAIN (GENERIC_PLAN)` on PostgreSQL 16+ or prepared-statement generic plans on PostgreSQL 12–15 |
| Measured plans | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` run on explicit user request inside a rolled-back read-only transaction |
| Virtual indexes (`hypopg`) | Tests candidate indexes in backend memory (`culprit explain shop --sql '...' --with-index 'CREATE INDEX ON orders (customer_id)'`) without touching disk or locking tables |
| Plan import | Renders JSON and text `EXPLAIN` plans (including `psql` output) in Studio, CLI and [explain.culpritsql.com](https://explain.culpritsql.com) |
| Statement families | Groups normalized queries that differ only by `IN (...)` or `VALUES (...)` list lengths |

Temporary file block reads/writes (`temp_blks_read`, `temp_blks_written`) and `temp_blk_read_time` / `temp_blk_write_time` are projected in the primary statement read with their exact block size—never extrapolated into guessed disk throughput or `work_mem` formulas without actual plan spill evidence.

## Live activity and waits

Live combines `pg_stat_activity`, `pg_blocking_pids()`, lock catalogs and `pg_stat_progress_*` (`vacuum`, `analyze`, `create_index`, `cluster`, `copy`, `basebackup`) to display active queries, idle-in-transaction blockers, live wait-event sampling (1 Hz over active sessions) and operation progress.

- **Blocking & lock closure**: Focused blocking analysis verifies PostgreSQL's system identifier, server address/port, PID, `backend_start`, `xact_start` and query token before and after capturing bounded lock wait chains.
- **Session actions**: Outside `prod` connections, Studio exposes both `pg_cancel_backend` (cancel current query) and `pg_terminate_backend` (close connection).
- **Deadlock history & structured logs**: `pg_stat_database.deadlocks` tracks cumulative count; full participant lock graphs and SQLSTATE failures are decoded offline from PostgreSQL JSON (`jsonlog`) or CSV (`csvlog`) server logs without needing database credentials or `pg_read_file`:

```sh
# Offline analysis of PostgreSQL 15+ jsonlog or PostgreSQL 14+ 26-column csvlog (use csv13 for PG 13)
culprit logs ./postgresql.jsonl --input-format json --json
culprit logs ./postgresql.csv --input-format csv
```

`culprit logs` classifies SQLSTATE conditions (`40P01` deadlocks with reconstructed participant lock edges, `53200` OOM, `57014` query canceled, `25P04` idle-in-transaction timeout, lock/connection exhaustion), redacts SQL literals, omits free-form detail text, and caps single-pass reads at 1 MiB / 1,024 records by default.

## Schema and indexes

Catalog and statistics readers inspect tables, partitions, columns, constraints, extended statistics, index definitions, `pg_stat_user_tables`, `pg_stat_user_indexes`, `pg_statio_user_*`, PostgreSQL 16+ `pg_stat_io`, and PostgreSQL 17+ `pg_stat_checkpointer`.

- **PostgreSQL-native index advice**: Candidate indexes come from plan filter/join/sort operators, unindexed foreign keys and sequential-scan evidence—validated in milliseconds when `hypopg` is installed. DDL uses `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` on ordinary tables, and distinguishes invalid (`!indisvalid`) remnants from active or disabled indexes.
- **Storage, vacuum & wraparound health**: Evaluates dead-tuple bloat, autovacuum lag and per-table freeze/MultiXact age (`relfrozenxid`, `relminmxid`), HOT update ratios blocked by indexed columns, sequence exhaustion (`pg_sequences`), replication slot `xmin` / `catalog_xmin` retention, WAL archiver failures (`pg_stat_archiver`), standby recovery conflicts and PostgreSQL 15+ logical subscription errors.
- **Offline SQL migration linting**: `culprit lint-sql migrations/ --format sarif` catches non-concurrent index builds, volatile column defaults, missing `lock_timeout` guards, unvalidated `ADD CONSTRAINT` table scans and unsafe type rewrites before deployment.

## Limits and evidence

### Connection safety and wire bounds

Every scan, probe, plan capture and collector poll shares one owned PostgreSQL wire reader with strict guardrails:

| Guardrail | Limit |
|---|---|
| Session posture | `SET default_transaction_read_only = on`, catalog-only `search_path`, `lock_timeout = '2s'`, parallel workers disabled for catalog queries |
| Statement timeouts | `20s` for scan/collector reads; `10s` for interactive enrichments |
| Result caps | `10,000` rows and `8 MiB` payload per query; `512` batches and `60s` ceiling across a full parallel snapshot |
| Cancellation fencing | Interrupted sessions are drained to `ReadyForQuery` or retired without automatic retry; unconfirmed shutdowns fence subsequent background polls |

### Background telemetry (`culprit collect`)

The fleet collector exports **24 `postgres.*` telemetry recipes** to Azure Data Explorer (ADX) or ClickHouse—covering statement counters (polled separately from redacted SQL text), bounded catalog fingerprints, table/index/I/O stats, persistent blocking activity and instance posture without running `EXPLAIN` or reading user table rows. See [PostgreSQL collector coverage](../collector.md#postgresql-source-coverage).

 PostgreSQL heap/buffer rules never carry over to engines that merely speak the PostgreSQL wire protocol. Cumulative counters are never treated as reset on simple postmaster restart unless `stats_reset` changed, per-day rates require dated history, and unused-index verdicts require at least 7 days of continuous counters. See the shared [evidence rules](coverage.md).

## Verification

Exercised across **PostgreSQL 15–18** (including Amazon RDS and saved PostgreSQL 18.6 fixtures). Reproduce workload, bloat, wraparound, foreign-key and lock-contention scenarios locally with [`dev/postgres/README.md`](../../dev/postgres/README.md).

All declared PostgreSQL sources live in [`culprit-postgres/src/sources.rs`](../../crates/providers/culprit-postgres/src/sources.rs). References: [PostgreSQL routine vacuuming & MultiXacts](https://www.postgresql.org/docs/18/routine-vacuuming.html), [`pg_sequences`](https://www.postgresql.org/docs/18/view-pg-sequences.html), and [JSON/CSV structured logging](https://www.postgresql.org/docs/18/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-JSONLOG).
