# YugabyteDB

[Engine parity](../parity.md)

Culprit ships a dedicated **YugabyteDB** provider (`crates/providers/culprit-yugabyte`) built specifically for YSQL over DocDB. While YSQL speaks the PostgreSQL wire protocol, Culprit never applies PostgreSQL heap, buffer-cache, `VACUUM` or wraparound assumptions to DocDB's distributed LSM/tablet storage.

## Connecting

Select **YugabyteDB** in Studio or pass `--engine yugabyte` (`ysql` and `yugabytedb` are accepted aliases) or a `yugabyte://` URL on the CLI. Defaults are port `5433` and user/database `yugabyte`.

```sh
# Direct YSQL node connection (remote TLS verifies server certificates by default)
culprit conn add shop --env prod --url 'yugabyte://culprit_reader@yb-1.example.com:5433/shop'

# Private YugabyteDB cluster through an SSH jump host
culprit conn add shop-priv --env prod --ssh ops@bastion.example.com --url 'yugabyte://culprit_reader@10.0.8.11:5433/shop'
```

- **Automatic engine guard**: If you accidentally connect to YugabyteDB using the PostgreSQL provider, Culprit detects YSQL during the lightweight probe (before touching storage APIs) and prompts you to switch to **YugabyteDB**.
- **Direct YSQL backend vs Connection Manager**: Session-state operations (such as virtual index testing with `hypopg` and targeted session actions) verify a direct YSQL backend and refuse multiplexed YugabyteDB Connection Manager workers. Cloud IAM/Entra bearer tokens are refused on this provider.

## Permissions and setup

Grant `pg_read_all_stats` for full statement, plan-cache and ASH visibility across roles, and optionally `yb_db_admin` if you want host CPU/memory and TServer memory-tracker metrics:

```sql
CREATE ROLE culprit_reader WITH LOGIN PASSWORD '<strong-password>';
GRANT CONNECT ON DATABASE shop TO culprit_reader;
GRANT pg_read_all_stats TO culprit_reader;

-- Optional: enables yb_servers_metrics() shared node CPU & TServer memory limits
GRANT yb_db_admin TO culprit_reader;
```

### Node-local vs cluster-wide statistics

By default, YSQL statistics (`pg_stat_statements`, `pg_stat_activity`, index counters) are **node-local** to the TServer you connect to, and Culprit labels that scope clearly. If an administrator has enabled YugabyteDB's [cluster-wide database views](https://docs.yugabyte.com/stable/launch-and-manage/monitor-and-alert/cluster-wide-db-views/) outside Culprit, the provider reads them automatically and verifies every node's contributor status and reset epoch.

To expose cluster-wide reset timestamps and metric settings alongside global statement views, an administrator can create these superuser-owned views on `yb_global_views_server`:

```sql
SELECT yb_create_global_view('public', 'pg_stat_statements_info');
SELECT yb_create_global_view('public', 'pg_settings');
```

**Important `yb_pg_stat_plans` privilege guard**: On verified YugabyteDB **2026.1.2.0-b137**, reading `yb_pg_stat_plans` without an **effective** (inherited) `pg_read_all_stats` or superuser privilege crashes that YSQL backend inside `yb_pg_stat_plans_get_all_entries`. Culprit checks effective privileges before touching the plan cache and skips it cleanly when unprivileged.

## Workload and plans

| Capability | YugabyteDB source and behavior |
|---|---|
| Statement workload | Database-filtered `pg_stat_statements` aggregated by query identity (up to 8 MiB streamed without full latency-histogram expansion) |
| DocDB native costs | Storage read/write RPCs, DocDB rows scanned, RocksDB/LSM traversal counters, retries and storage timings (when enabled across all contributing nodes) |
| Automatic & retained plans | Safe generic YSQL `EXPLAIN` estimates plus retained [Query Plan Management (`yb_pg_stat_plans`)](https://docs.yugabyte.com/stable/launch-and-manage/monitor-and-alert/query-tuning/query-plan-manage/) JSON trees, execution counts and first/last-use timestamps |
| Measured `DIST` plans | Explicit `EXPLAIN (ANALYZE, DIST, VERBOSE, FORMAT JSON)` inside a rolled-back transaction, normalizing per-loop `DIST` RPC and storage metrics |
| Virtual indexes (`hypopg`) | `culprit explain shop --sql '...' --with-index 'CREATE INDEX ON public.orders (customer_id ASC)'` on a direct YSQL backend; resets `hypopg` and closes the session afterward |
| Plan import | Automatically recognizes YugabyteDB `DIST` / storage markers in JSON plans in Studio, CLI and [explain.culpritsql.com](https://explain.culpritsql.com) |

Retained `yb_pg_stat_plans` entries keep their own independent cache lifetime and reset epoch (tagged Culprit queries opt out of QPM tracking). Selected cached plan bodies are embedded into saved `.culprit` scans so they remain inspectable offline even after cache eviction.

## Live activity and waits

| Capability | YugabyteDB source and behavior |
|---|---|
| Live sessions & locks | Current-database sessions paired with `yb_lock_status` contention evidence (capped at 1,000 sessions / 2,000 waits); session IDs are node-qualified (`<32-hex-node-uuid>/<pid>/<backend_start>`) |
| Recorded activity (ASH) | Reads [native Active Session History](https://docs.yugabyte.com/stable/launch-and-manage/monitor-and-alert/active-session-history-monitor/) inverse-sampling weights, wait components, observation nodes and query IDs (up to 10,000 samples / 8 MiB) |
| Unexpected terminations | Reads [`yb_terminated_queries`](https://docs.yugabyte.com/stable/explore/observability/yb-pg-stat-get-queries/) for backend crashes (`SIGKILL` / `SIGSEGV`) and temp-file limit aborts (up to 1,000 rows, 10s deadline; SQL/PIDs omitted) |
| Shared node resources | Calls `yb_servers_metrics()` (when granted `yb_db_admin`) for per-node CPU user/system fractions, host RAM and TServer root memory-tracker soft/hard limits |
| Live command progress | Tracks [index backfill](https://docs.yugabyte.com/stable/explore/ysql-language-features/indexes-constraints/index-backfill/) rows, `ANALYZE` phases and active `IN PROGRESS` `COPY` counters after verifying current SQL and backend identity |

- **ASH weight discipline**: ASH weights measure sampled active task presence—not elapsed query duration, exclusive wait share or host CPU %.
- **Distributed deadlocks**: Live `yb_lock_status` shows active lock waits, whereas retrospective distributed deadlocks surface as `40P01` errors in application / `yb-tserver` logs rather than PostgreSQL's heap deadlock counter.

## Schema and indexes

Catalog inspection covers tables, logical row estimates, columns, defaults, foreign keys, secondary indexes, **HASH vs ASC/DESC range key columns**, **tablet counts** and **colocation** metadata.

- **DocDB-aware index advice & `lint-sql`**: Distinguishes `HASH` sharding columns from ordered `ASC` / `DESC` range prefixes when detecting redundant indexes, covering lookups and schema drift. Repair scripts wait for active index backfills before touching invalid indexes, and `culprit lint-sql` validates YSQL migrations offline.
- **Unvalidated constraints**: Flags `NOT VALID` `CHECK` and foreign-key constraints on existing data with version-appropriate validation guidance.
- **Cluster-safe unused-index rules**: Culprit never calls an index unused from a single node's zero counter or a `NULL` global reset date—every contributing node in the cluster must prove at least 7 days of continuous counter history.

## Limits and evidence

| Boundary | How Culprit handles it on YugabyteDB |
|---|---|
| No PostgreSQL heap myths | Heap bloat, buffer hit ratios, dead tuples, HOT updates, autovacuum, XID/MultiXact wraparound and `ALTER SYSTEM` heap GUCs are disabled by design; database tuning suggestions are restricted to the 5 supported `ALTER DATABASE` YSQL settings |
| Sequence exhaustion | `pg_sequences.last_value` returns `NULL` in YSQL rather than reading DocDB sequence slabs, and Culprit never calls `nextval()`; sequence headroom stays unknown rather than guessed |
| Plan capture budgets | Capped at `20s` and `8 MiB` across estimated and retained `yb_pg_stat_plans` bodies (`--plans 0` skips bodies); uncalibrated YSQL optimizer costs never trigger findings without measured work |
| Multi-node comparisons | Saved-scan deltas (`culprit changes`) require identical contributing node sets and uninterrupted per-node reset timestamps; skipped or restarted nodes mark comparisons partial |
| Background telemetry | `culprit collect` ships **16 dedicated `yugabyte.*` recipes** (workload counters, QPM metadata, ASH, terminations, node resources, DocDB locks and catalog fingerprints) over the owned wire reader; see [collector source coverage](../collector.md#other-provider-source-coverage) and [evidence rules](coverage.md) |

## Verification

Verified against a three-node **YugabyteDB 2026.1.2.0-b137** cluster (`15.12-YB-2026.1.2.0-b0`), exercising global-view aggregation, independent node resets, paused/degraded node fault injection and recovery, `yb_db_admin` permission boundaries, `hypopg` virtual indexes and Connection Manager worker refusal.

- Reproducible 3-node cluster & workload suite: [`dev/yugabyte/README.md`](../../dev/yugabyte/README.md)
- Declared YugabyteDB sources: [`culprit-yugabyte/src/sources.rs`](../../crates/providers/culprit-yugabyte/src/sources.rs)
- Verified UI captures: [Dashboard](../screenshots/yugabyte-dashboard.png) · [Recorded ASH activity](../screenshots/yugabyte-activity.png) · [Terminated queries](../screenshots/yugabyte-terminations.png) · [Shared node resources](../screenshots/yugabyte-resources.png) · [DocDB query metrics](../screenshots/yugabyte-queries.png) · [Retained QPM plan history](../screenshots/yugabyte-history.png) · [Hash keys & tablet schema](../screenshots/yugabyte-schema.png)
