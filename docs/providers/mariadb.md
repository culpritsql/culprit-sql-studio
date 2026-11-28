# MariaDB

[Engine parity](../parity.md)

Culprit detects MariaDB automatically over the MySQL wire protocol and switches to MariaDB-native execution plans (`ANALYZE FORMAT=JSON`), persistent column statistics, lock views, `IGNORED` index syntax, multi-source replication channels and Galera cluster telemetry—keeping every counter and version rule distinct from [MySQL](mysql.md).

## Connecting

Select **MySQL / MariaDB** in Studio or pass a `mysql://` URL on the CLI. Existing connections from DBeaver, DataGrip or VS Code can be imported via `culprit conn import`.

```sh
# Standard MariaDB connection (remote TLS verifies server identity by default)
culprit conn add shop --env prod --url 'mysql://culprit_reader@mariadb.example.com:3306/shop'

# Amazon RDS for MariaDB with short-lived AWS IAM token
culprit conn add shop-rds --env prod --auth aws-iam --url 'mysql://culprit_reader@shop.xyz.us-east-1.rds.amazonaws.com:3306/shop'

# Private MariaDB node through an SSH jump host
culprit conn add shop-priv --env prod --ssh ubuntu@bastion.example.com --url 'mysql://culprit_reader@10.0.6.14:3306/shop'
```

In JSON exports and SDK payloads, MariaDB uses `engine = "mysql"` while `server.product` / `latest_scan.product` identifies `"MariaDB"` and parsed plans carry `props.flavor = "MariaDB"`.

## Permissions and setup

Grant read-only monitoring privileges (`PROCESS`, `REPLICA MONITOR` / `BINLOG MONITOR` where applicable, and `SELECT` on monitoring catalogs):

```sql
CREATE USER 'culprit_reader'@'%' IDENTIFIED BY '<strong-password>';
GRANT PROCESS, REPLICA MONITOR ON *.* TO 'culprit_reader'@'%';
GRANT SELECT ON performance_schema.* TO 'culprit_reader'@'%';
GRANT SELECT ON mysql.slow_log TO 'culprit_reader'@'%';
GRANT SELECT ON `shop`.* TO 'culprit_reader'@'%';
```

### Zero-restart workload setup when `performance_schema` is off

Many MariaDB installations ship with `performance_schema = OFF` (which requires a restart to enable). Instead of demanding a production restart, Culprit prioritizes MariaDB's dynamic table-based slow query log and `userstat` counters when nothing is currently recorded:

```sql
-- Dynamic, zero-restart workload & index-usage capture (review overhead before lowering long_query_time):
SET GLOBAL log_output = 'TABLE';
SET GLOBAL slow_query_log = ON;
SET GLOBAL userstat = ON;
```

Culprit presents these commands as guidance in `server.missing[]` and Studio—scans never execute `SET GLOBAL` themselves.

## Workload and plans

| Capability | MariaDB source and behavior |
|---|---|
| Statement statistics | `performance_schema` digests (`workload.source = "performance_schema"`), or fingerprinted runs from `mysql.slow_log` (`workload.source = "slow query log"`) |
| Period comparison | Saved-scan deltas for digests, or native `slow_log` timestamps and `long_query_time` windowing (`"slow-log"`) |
| Estimated & measured plans | MariaDB `EXPLAIN FORMAT=JSON` and `ANALYZE FORMAT=JSON` (per-operator r_rows, r_loops, filtered %, r_total_time_ms and page reads inside a rolled-back read-only transaction) |
| Legacy JSON compatibility | Handles pre-11.0 MariaDB plans without native cost fields (using row estimates without conflating them with MySQL/PostgreSQL costs) and repeated `table` members alongside `nested_loop` arrays |
| Plan import & replay | Imports MariaDB `EXPLAIN` / `ANALYZE` JSON in Studio, CLI (`explain --query-id`), MCP `query_plan` and [explain.culpritsql.com](https://explain.culpritsql.com) |
| Runtime spread | Digest min/avg/max, or exact percentiles and histograms computed directly from retained `mysql.slow_log` runs |

## Live activity and waits

Live combines `information_schema.PROCESSLIST` (including MariaDB's sub-second `TIME_MS` precision and `STAGE` / `MAX_STAGE` progress for `ALTER TABLE` and `LOAD DATA`), `INNODB_TRX`, `INNODB_LOCK_WAITS` and `INNODB_LOCKS`.

- **Read-only transaction ID guard**: MariaDB 10.x assigns `trx_id = 0` to transactions that have not yet modified rows; Culprit matches waiting sessions through their requested lock descriptors rather than falsely linking unrelated `trx_id = 0` sessions.
- **Running query plans (`SHOW ANALYZE`)**: On MariaDB **10.9+**, Live can inspect an executing statement's live plan via `SHOW ANALYZE FORMAT=JSON <id>`. Outside `prod` connections, `KILL QUERY` and `KILL` are available.
- **Scheduled events (`mysql.jobs`)**: On MariaDB releases exposing `server_uid`, `--collect-native-failures` and `--collection jobs` capture up to 128 `information_schema.EVENTS` schedules without reading event bodies.

## Schema and indexes

Catalog views supply tables, columns, foreign keys, auto-increment bounds, indexes and MariaDB engine-independent persistent statistics (`mysql.column_stats` / `ANALYZE TABLE ... PERSISTENT FOR COLUMNS (...)`).

- **MariaDB 10.6+ `IGNORED` indexes**: When index-usage history establishes an eligible unused index, Culprit emits MariaDB's native `ALTER TABLE ... ALTER INDEX ... IGNORED` (reversible via `NOT IGNORED`)—never MySQL's `INVISIBLE` syntax. Note that `userstat`'s `INDEX_STATISTICS` table records no enable/reset timestamp, so Culprit never claims a 7-day unused verdict from `userstat` alone without dated scan baselines.
- **Online DDL & `lint-sql`**: `culprit lint-sql migrations/ --format sarif` validates MariaDB migrations for `ALGORITHM=INPLACE, LOCK=NONE` compatibility, unindexed foreign keys and unsafe column rewrites.

## Limits and evidence

### Background telemetry and operational scope

Background `culprit collect` shares the owned MySQL-family wire reader with MariaDB-specific timeout, lock and multi-source [`SHOW ALL SLAVES / REPLICAS STATUS`](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/show/show-replica-status) layouts. Server-wide counters (InnoDB redo waits, metadata-lock queues, replication channels) retain their instance scope rather than being attributed to a single database; see [collector source coverage](../collector.md#other-provider-source-coverage) and [evidence rules](coverage.md).

### Galera cluster observations (`mariadb.galera`)

Ordinary scans capture a lightweight 5-variable Galera readiness and flow-control summary (`wsrep_*`), treating `Donor/Desynced` state as operational context rather than an automatic outage. For deeper cluster troubleshooting on MariaDB nodes exposing native `server_uid`, run the explicit `mariadb.galera` collector recipe:

| `wsrep` metric group | Captured meaning and boundaries |
|---|---|
| Queue depths | Current receive/send write-set queues waiting to apply or transmit at capture time |
| Cumulative totals | Certification failures, local applier aborts, transaction replays, local commits and flow-control paused nanoseconds (kept with unknown reset epoch) |
| Flow-control fraction | `wsrep_flow_control_paused` covers the interval since the last `FLUSH STATUS` (Culprit never runs `FLUSH STATUS` and excludes read-relative min/max/avg variables) |
| Safety ceiling | Shared-instance scope; capped at 17 SQL statements total (including 3-point context verification), 17 allowlisted variables (`<= 32 KiB`), 1s statement timeout and 5s total budget ([Galera status reference](https://mariadb.com/docs/galera-cluster/reference/galera-cluster-status-variables)) |

## Verification

Verified across **MariaDB 10.5.29, 10.6.28, 10.11 and 11.8**—exercising `performance_schema` on and off, `mysql.slow_log` + `userstat` fallback, `ANALYZE FORMAT=JSON` plans, `IGNORED` indexes, blocking chains, InnoDB deadlocks and `SHOW ANALYZE` live progress.

- Reproducible Docker fixture: [`dev/mysql/README.md`](../../dev/mysql/README.md)
- Declared MySQL/MariaDB sources: [`culprit-mysql/src/sources.rs`](../../crates/providers/culprit-mysql/src/sources.rs)
