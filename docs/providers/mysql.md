# MySQL

[Engine parity](../parity.md)

Culprit inspects self-hosted MySQL (5.7–8.4), Amazon RDS for MySQL, Amazon Aurora MySQL, Azure Database for MySQL Flexible Server and Google Cloud SQL using `performance_schema`, `sys`, `information_schema` and `EXPLAIN`. When `performance_schema` digests are disabled, a readable `mysql.slow_log` table supplies a no-restart workload fallback. [MariaDB](mariadb.md) shares the wire transport with its own dedicated plan, statistics and Galera handling.

## Connecting

Choose **MySQL / MariaDB** in Studio or pass a `mysql://` URL, JDBC / ADO.NET string or host/port on the CLI. Existing profiles from DBeaver, DataGrip or VS Code can be imported via `culprit conn import`.

```sh
# Standard MySQL connection (remote TLS defaults to VERIFY_IDENTITY)
culprit conn add shop --env prod --url 'mysql://culprit_reader@db.example.com:3306/shop'

# Amazon RDS / Aurora MySQL with AWS IAM authentication token
culprit conn add shop-rds --env prod --auth aws-iam --url 'mysql://culprit_reader@shop.cluster-xyz.us-east-1.rds.amazonaws.com:3306/shop'

# Azure Database for MySQL Flexible Server with Microsoft Entra ID
culprit conn add shop-az --env prod --auth entra --url 'mysql://culprit_reader@shop.mysql.database.azure.com:3306/shop'

# Private instance through an SSH jump host
culprit conn add shop-priv --env prod --ssh ubuntu@bastion.example.com --url 'mysql://culprit_reader@10.0.5.21:3306/shop'
```

Passwords remain in the OS keychain; AWS IAM (`--auth aws-iam`) and Entra ID (`--auth entra`) generate short-lived tokens automatically. Cloud posture checks inspect RDS/Azure configuration and unencrypted client connections (`performance_schema.session_status` / threads) where readable.

## Permissions and setup

Grant read-only monitoring privileges (`PROCESS` plus `SELECT` on `performance_schema`, `sys` and the target schema):

```sql
CREATE USER 'culprit_reader'@'%' IDENTIFIED BY '<strong-password>';
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'culprit_reader'@'%';
GRANT SELECT ON performance_schema.* TO 'culprit_reader'@'%';
GRANT SELECT ON sys.* TO 'culprit_reader'@'%';
GRANT SELECT ON `shop`.* TO 'culprit_reader'@'%';

-- Optional: if using table-based slow query log fallback when performance_schema is off
GRANT SELECT ON mysql.slow_log TO 'culprit_reader'@'%';
```

To recover full SQL samples on older setups where digest sample columns are absent or empty, ensure statement-history consumers are enabled:

```sql
UPDATE performance_schema.setup_consumers
SET ENABLED = 'YES'
WHERE NAME IN ('events_statements_current', 'events_statements_history', 'events_statements_history_long');
```

Culprit never modifies `setup_consumers`, `setup_instruments` or global variables itself; when MySQL 8.0+ configuration changes are recommended, Culprit surfaces `SET PERSIST` statements for an operator to review.

## Workload and plans

| Capability | MySQL source and behavior |
|---|---|
| Statement statistics | `performance_schema.events_statements_summary_by_digest` (counts, timers, rows, tmp tables, sort passes, index/no-index scans), or grouped runs from `mysql.slow_log` |
| Period comparison | Saved-scan deltas across digest snapshots (`culprit changes`), or native run timestamps when reading `mysql.slow_log` |
| Captured SQL samples | Digest `QUERY_SAMPLE_TEXT` on MySQL 8.0.3+, falling back to statement-history consumers or `slow_log` samples |
| Estimated & measured plans | `EXPLAIN FORMAT=JSON` (both v1 and MySQL 8.0/8.4 JSON v2) and `FORMAT=TREE`; explicit measured runs use `EXPLAIN ANALYZE` on MySQL 8.0.18+ inside a rolled-back read-only transaction |
| Plan import | Renders JSON and `EXPLAIN ANALYZE` tree plans in Studio, CLI and [explain.culpritsql.com](https://explain.culpritsql.com) |
| Runtime spread & tails | Digest min/avg/max plus MySQL 8.0.19+ per-digest latency histograms (`events_statements_histogram_by_digest`) |

Digest quantiles, native latency histograms and `slow_log` samples keep their own independent sample counts and reset epochs. Internal temporary table counts, on-disk temporary table counts and sort-merge passes are projected directly from the digest summary without guessing spilled byte volumes or sizing `tmp_table_size`.

## Live activity and waits

Live combines `PROCESSLIST`, `information_schema.INNODB_TRX` and lock-wait views (`performance_schema.data_lock_waits` on MySQL 8.0+, `sys.innodb_lock_waits` / `INNODB_LOCK_WAITS` on 5.7) alongside `sys.schema_table_lock_waits` metadata-lock blocking.

- **Blocker SQL recovery**: When an idle-in-transaction blocker has an empty `PROCESSLIST` query, Culprit inspects `events_statements_history` to surface the earlier statement holding the lock.
- **Live plan & stage progress**: Uses `EXPLAIN FOR CONNECTION <id>` and `performance_schema` stage instruments where enabled. Outside `prod`, Studio exposes both `KILL QUERY` (cancel) and `KILL` (terminate).
- **Lock timers & deadlocks**: Digest `SUM_LOCK_TIME` (exported in exact picoseconds for Fleet, displayed in milliseconds in scans) reflects MySQL 8.0.28+'s corrected row + table lock accounting without claiming historical parity on 5.7. InnoDB exposes the latest detected deadlock (`SHOW ENGINE INNODB STATUS`), which Culprit parses into a participant lock graph without claiming complete incident retention.

### Opt-in diagnostic & replication readers

To keep routine scans lightweight, deeper `performance_schema` and cluster inspections run only when explicitly requested via `culprit scan <conn> <flag>` or `culprit collect once <conn> --collection <recipe>`:

| Opt-in reader | Flag / collector recipe | Supported versions & safety bounds |
|---|---|---|
| Tracked session memory | `--collect-session-memory` / `session-memory` | **MySQL 8.0.31+**: Reads current & high-water `TOTAL_MEMORY_ALLOC` / `CONTROLLED_MEMORY` from `performance_schema.threads` via in-memory thread join (never touching `sys.processlist`); 24-query, 2s/10s, 8 MiB cap |
| Retained error log | `--collect-native-failures` / `native-failures` | **MySQL 8.0.22+**: Reads up to 128 structured rows from `performance_schema.error_log` (UTC timestamp, error code, priority, subsystem) while omitting the free-form `DATA` column so user literals never leak; 2s/10s cap |
| Event Scheduler catalog | `--collect-native-failures` / `mysql.jobs` | **MySQL 5.7+**: Reads up to 128 visible `information_schema.EVENTS` schedules/statuses (omitting event SQL bodies and definers); 2s/10s, 512 KiB cap |
| Group Replication | `mysql.group-replication` (collector) | **MySQL 5.7.17+**: Captures `replication_group_members` and `replication_group_member_stats` (certification/applier queues, conflicts, member roles); 23 phases, 1s/5s, 128 rows/table cap |

## Schema and indexes

`information_schema`, `performance_schema.table_io_waits_summary_by_index_usage` and `sys` supply tables, columns, foreign keys, auto-increment utilization, column histograms and index usage counters.

- **Reversible `INVISIBLE` index workflow**: On MySQL 8.0+, cleanup advice for eligible unused indexes (after `>= 7` days of continuous counters, never touching primary, unique or foreign-key-backing indexes) recommends `ALTER TABLE ... ALTER INDEX ... INVISIBLE` first so optimizer impact can be verified with instant rollback before dropping. On MySQL 5.7, Culprit preserves the full `CREATE INDEX` definition first.
- **Online DDL & `lint-sql`**: Both the schema advisor and `culprit lint-sql migrations/ --format sarif` emit `ALGORITHM=INPLACE, LOCK=NONE` where supported and flag table-rebuilding column alterations, missing foreign-key indexes and auto-increment exhaustion risks in CI and VS Code.

## Limits and evidence

- **Independent counter resets**: Digest summaries, latency histograms, table I/O summaries and `slow_log` tables reset independently; Culprit verifies counter continuity before computing rates or deltas and never substitutes `0` when an instrument or consumer is disabled.
- **Shared instance metrics**: InnoDB redo-log waits, buffer pool counters and host/cloud metrics retain their instance-wide scope rather than being blamed on a single schema's queries.
- **Background telemetry (`culprit collect`)**: Shares the owned MySQL wire reader with strict statement timeouts, byte budgets and cancellation fencing—exporting exact picosecond counters, catalog fingerprints, persistent blockers and the latest InnoDB deadlock to ADX or ClickHouse without running application SQL. See [collector source coverage](../collector.md#other-provider-source-coverage) and [evidence rules](coverage.md).

## Verification

Verified against **MySQL 5.7.44** and **MySQL 8.4** (saved fixture: MySQL **8.4.11**), covering `performance_schema` digests, latency histograms, MySQL 5.7 lock-view fallbacks, `INVISIBLE` index handling, metadata-lock blocking and InnoDB deadlocks.

- Reproducible Docker fixture & workload: [`dev/mysql/README.md`](../../dev/mysql/README.md)
- Declared MySQL sources: [`culprit-mysql/src/sources.rs`](../../crates/providers/culprit-mysql/src/sources.rs)
- Native references: [MySQL 8.4 error-log table](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-log-table.html) · [metadata-lock waits](https://dev.mysql.com/doc/refman/8.4/en/sys-schema-table-lock-waits.html)
