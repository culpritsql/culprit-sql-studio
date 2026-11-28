# SQL Server

[Engine parity](../parity.md)

Culprit connects to boxed SQL Server (2017–2022+), Azure SQL Database, Azure SQL Managed Instance and Amazon RDS for SQL Server. Query Store supplies time-windowed workload intervals, historical plans and per-query wait categories; when Query Store is off or unreadable, Culprit falls back to the plan cache and DMVs with explicit retention boundaries.

## Connecting

Select **SQL Server** in Culprit SQL Studio or pass an ADO.NET, JDBC or `mssql://` connection string to the CLI. You can also import existing profiles from Azure Data Studio, VS Code (`mssql`), DBeaver or DataGrip with `culprit conn import`.

```sh
# Standard SQL login (password stored in OS keychain)
culprit conn add shop --env prod --url 'Server=tcp:db.example.com,1433;Database=shop;User ID=culprit_reader;Encrypt=strict'

# Azure SQL Database with Microsoft Entra ID (az login, Managed Identity, Workload Identity or device code)
culprit conn add shop-az --env prod --auth entra --url 'Server=tcp:shop.database.windows.net,1433;Database=shop'

# Private subnet via SSH jump host
culprit conn add shop-priv --env prod --ssh bastion@10.0.1.5 --ssh-key ~/.ssh/id_ed25519 \
  --url 'Server=tcp:10.0.2.15,1433;Database=shop;User ID=culprit_reader'
```

| Capability | Behavior |
|---|---|
| TLS encryption | `Encrypt=true` by default on remote hosts; TDS 8.0 `Encrypt=strict` supported for SQL Server 2022 and Azure SQL |
| Cloud authentication | Microsoft Entra ID (`az` CLI, device-code flow, Managed Identity and Kubernetes Workload Identity). Amazon RDS SQL Server uses SQL authentication plus optional CloudWatch / RDS posture enrichment (IAM DB auth tokens are not offered by RDS SQL Server) |
| Cloud discovery | Discovers Azure SQL servers/databases/elastic pools and AWS RDS SQL Server instances without touching application tables |
| SSH jump hosts | Built-in SSH tunneling (`--ssh`, `--ssh-port`, `--ssh-key`); see [SSH jump hosts](../ssh.md) |
| Environment safety | Passwords live in the OS keychain; `KILL` and measured plan execution are blocked on `prod` connections unless explicitly permitted |

### Read-only replicas

Save read-scale or Availability Group secondaries as a separate connection with `ApplicationIntent=ReadOnly`. Routing intent asks the listener for a read-only member; it does not prove which physical replica answered, guarantee every secondary was visited, or prevent writes on its own.

| Evidence area | Scope on replicas and secondary populations |
|---|---|
| Query Store history | Shared database catalog with separate native replica groups when secondary Query Store capture (`READ_CAPTURE_SECONDARY`) is enabled. Ordinary scans read the shared history once and persist independent per-population reports |
| Selecting a population | Inspect a specific replica role/group with `culprit scan shop --workload-population <ID>`, `culprit changes shop --workload-population <ID>`, or Studio's **Query history** population picker. Reports never silently blend primary and secondary populations |
| Activity and waits | Reflects the connected replica's live requests, locks and wait counters. Hyperscale replica continuity checks native database/replica identity and process epoch before linking follow-up reads |
| Retained actual plans | `LAST_QUERY_PLAN_STATS` plans are attached only when native execution-group provenance matches; otherwise the captured plan remains inspectable with its limitation noted |
| Unused-index verdicts | Local `sys.dm_db_index_usage_stats` counters on one member never prove an index is unused on secondaries. Disable/drop advice is withheld unless every read-serving member is covered across at least 7 days |

See Microsoft's [read-scale monitoring](https://learn.microsoft.com/azure/azure-sql/database/read-scale-out#monitor-and-troubleshoot-read-only-replicas) and [Query Store for secondary replicas](https://learn.microsoft.com/sql/relational-databases/performance/query-store-for-secondary-replicas) guides for native replica capture requirements.

## Permissions and setup

Culprit probes effective permissions and Query Store state up front. Missing privileges never fail a scan: unavailable views become explicit coverage notes with the exact `GRANT` needed.

```sql
-- SQL Server 2022+ (least-privilege performance state + plan XML)
USE [master];
CREATE LOGIN [culprit_reader] WITH PASSWORD = '<strong-password>';
GRANT VIEW SERVER PERFORMANCE STATE TO [culprit_reader];

USE [shop];
CREATE USER [culprit_reader] FOR LOGIN [culprit_reader];
GRANT VIEW DATABASE PERFORMANCE STATE TO [culprit_reader];
GRANT SHOWPLAN TO [culprit_reader];
```

- **SQL Server 2017–2019**: Replace the `PERFORMANCE STATE` grants above with `GRANT VIEW SERVER STATE TO [culprit_reader];` in `master` and `GRANT VIEW DATABASE STATE TO [culprit_reader];` in the target database.
- **Azure SQL Database**: Grant `VIEW DATABASE PERFORMANCE STATE` (or `VIEW DATABASE STATE`) and `SHOWPLAN` inside the target user database.
- **Query Store (recommended)**: Enable Query Store with wait capture for time-windowed intervals, plan regression tracking and wait-category attribution:

```sql
ALTER DATABASE [shop] SET QUERY_STORE = ON (
    OPERATION_MODE = READ_WRITE,
    WAIT_STATS_CAPTURE_MODE = ON
);
```

## Workload and plans

| Capability | SQL Server source and behavior |
|---|---|
| Statement workload | `sys.query_store_runtime_stats` over the selected time window; falls back to `sys.dm_exec_query_stats` plan-cache counters when Query Store is off |
| Period comparison | Native Query Store interval slicing (`culprit scan --since`, `culprit changes`), or saved-scan deltas when reading the plan cache |
| Captured & imported plans | Stored Showplan XML from Query Store / plan cache, plus offline `.sqlplan` and XML import in Studio, CLI and [Culprit Explain](https://explain.culpritsql.com) |
| Estimated vs measured plans | Estimated Showplan never executes user SQL; explicit measured runs use `SET STATISTICS XML ON` inside a rolled-back transaction |
| Retained last actual plan | Reads `sys.dm_exec_query_plan_stats` on SQL Server 2019+ and Azure SQL when `LAST_QUERY_PLAN_STATS = ON` |
| Runtime spread & PSP | Tracks min, avg, max and stdev per plan, preserves SQL Server 2022 Parameter Sensitive Plan (`PSP`) dispatcher-to-variant relationships without double-counting executions, and surfaces Query Store plan-forcing state |
| Compilation & gateways | Reads Query Store compile counts, durations and working memory alongside `RESOURCE_SEMAPHORE_QUERY_COMPILE` waits and Resource Governor gateway thresholds |

Culprit keeps compilation counters, cached plan sizes, execution memory grants and runtime intervals on their own native clocks:

- **Query identity verification**: Compilation metrics link to SQL text only when `query_id`, initial compile timestamp, `query_hash` and context settings match; reused IDs or post-reset counters are never mixed across epochs.
- **Temporary space semantics**: Query Store's tempdb metric represents average 8-KiB pages used weighted by executions—not total written bytes, peak concurrent footprint, or proof of a sort/hash spill.

## Live activity and waits

Live combines `sys.dm_exec_sessions`, `sys.dm_exec_requests`, `sys.dm_tran_active_transactions`, `sys.dm_os_waiting_tasks` and lock views to render active queries, idle-in-transaction blockers, live operator progress (`sys.dm_exec_query_profiles`) and sampled server wait deltas.

- **Schema-lock blocking chains**: On verified SQL Server 2022+ writable primaries, focused blocking analysis traces queued DDL (`Sch-M` / `Sch-S`) back to the upstream transaction holding incompatible locks and lists downstream waiters blocked behind the queued schema change using native lock-resource and task pointers.
- **Wait category discipline**: Query Store wait categories, `THREADPOOL` worker starvation, CPU, compilation and memory-grant waits remain distinct; aggregate wait totals never invent a specific blocking session.
- **System health deadlocks (SQL Server 2017+ & Managed Instance)**: When `system_health` writes to a single absolute local/UNC file path, Culprit reads retained deadlock graphs via catalog metadata without querying `sys.dm_xe_session_targets` (which forces target flushes).
- **Session control**: Outside `prod` connections, Studio exposes `KILL <spid>` (SQL Server provides session termination rather than a separate query-cancel verb).

### Opt-in live & diagnostic readers

Heavy DMV projections are disabled during ordinary scans and Live polling so routine diagnostics stay fast and predictable. Request them explicitly on the CLI (`culprit scan <conn> <flag>`) or via `culprit collect once <conn> --collection <recipe>`:

| Opt-in reader | CLI scan flag / collector recipe | What it captures and safety bounds |
|---|---|---|
| Observed page-wait targets | `--collect-page-wait-targets` / `page-wait-targets` | Maps up to 8 distinct `PAGELATCH_*` / `PAGEIOLATCH_*` wait pages via `sys.dm_db_page_info(..., 'LIMITED')` (rowstore leaf/branch, heap, PFS/GAM/SGAM) plus up to 8 partition counter points; 18-batch, 1s statement / 5s total budget, gated on Azure SQL resource pressure |
| Current execution memory | `--collect-request-memory` / `current-request-memory` | Captures active/queued `sys.dm_exec_query_memory_grants` reservations, usage, workers and SQL Server 2017+ workload-group tempdb usage/limits; 200-row, 9-query, 3s/10s budget |
| Task-level tempdb space | `--collect-task-space` / `task-space` | Captures `sys.dm_db_task_space_usage` user/internal allocation & deallocation page counters per task; 200-row, 1s/5s budget (see details below) |
| Out-of-memory events | `--collect-memory-failures` / `memory-failures` | Reads up to 128 retained `sys.dm_os_out_of_memory_events` rows on Azure SQL Database & Managed Instance; 3s/10s budget (see details below) |
| Boxed process CPU | `mssql.process-cpu` (collector) | Captures SQLOS user/kernel CPU totals, Resource Governor pool CPU totals and latest scheduler-monitor ring-buffer sample; 10 dispatches max |
| MI resource history | `mssql.mi-resource-stats` (collector) | Streams up to 512 15-second intervals (max 2h window) from `sys.server_resource_stats` on Managed Instance; 1s/5s budget |

### Task-level temporary-space observations

`culprit scan shop --collect-task-space` (and `--collection task-space`) opts into [`sys.dm_db_task_space_usage`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-task-space-usage-transact-sql) when investigating active `tempdb` growth. Scans support SQL Server, Managed Instance and Azure SQL Database; fleet capture supports Azure SQL Database and boxed SQL Server 2022+ writable primaries.

- **Four exact 8-KiB page counters per task**: User allocations, user deallocations, internal allocations and internal deallocations per `(session_id, request_id, exec_context_id)`.
- **Strict attribution**: Cached temp tables, deferred drop cleanup and earlier statements in the same batch affect task counters. Checked current SQL is attached as an investigation candidate—not proof of peak footprint, spill duration or allocator ownership.
- **Hard ceiling**: Capped at 200 task rows + 1 overflow witness, 1-second statement timeout and 5-second total budget, with no server-side sort, plan XML fetch or OS stack walk.

### Azure SQL deadlock events

For **offline event analysis**—with zero database credentials or server changes—open binary `.xel` or exported XML files directly in **Studio** (**Plans → Open events XML**), drag-and-drop into **[explain.culpritsql.com](https://explain.culpritsql.com)**, run `culprit logs ./events.xel --input-format mssql-xe-xel`, or invoke MCP's `analyze_event_xel` / `analyze_event_xml` tools. Binary `.xel` framing, QPC-derived UTC timestamps, attention/timeout events, errors and multi-victim deadlock graphs are decoded locally in Rust/Wasm.

For **live Azure SQL Database scans**, Culprit **never** reads Azure SQL's internal `dl` session ([Microsoft advises against reading it](https://learn.microsoft.com/en-us/azure/azure-sql/database/xevent-db-diff-from-svr#best-practices) due to availability impact). Instead, an administrator can optionally create this bounded database-scoped ring buffer; Culprit only reads it if it already exists with these exact caps (`<= 1024` KiB, `<= 256` events):

```sql
CREATE EVENT SESSION [culprit_deadlocks] ON DATABASE
ADD EVENT sqlserver.database_xml_deadlock_report (
    ACTION (package0.event_sequence)
)
ADD TARGET package0.ring_buffer (
    SET max_memory = 1024, max_events_limit = 256
)
WITH (
    MAX_MEMORY = 2048 KB,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    STARTUP_STATE = ON
);
ALTER EVENT SESSION [culprit_deadlocks] ON DATABASE STATE = START;
```

Ring buffers clear on restart and overwrite oldest entries first, so an empty buffer means zero currently retained events—not proof that no deadlocks ever occurred.

### Retained out-of-memory observations

`culprit scan shop --collect-memory-failures` (and `--collection memory-failures`) reads [`sys.dm_os_out_of_memory_events`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-os-out-of-memory-events?view=azuresqldb-current) on **Azure SQL Database** and **Azure SQL Managed Instance** (boxed SQL Server is excluded because Microsoft marks the view unsupported there).

- **Shared hosting scope**: Reports instance-wide (Managed Instance) or elastic-pool / hosting-level memory events rather than blaming the current database or a specific query.
- **Bounded projection**: Reads at most 128 rows without server-side sorting, joins or JSON parsing inside a 3-second statement / 10-second total budget, keeping offset-free native event timestamps separate from UTC capture bounds.

## Schema and indexes

Catalog and DMV readers inspect tables, columns, constraints, statistics (`DBCC` is never executed), persisted columnstore configuration (compression delay, ordered columns), partition functions/schemes, triggers, index definitions, `sys.dm_db_index_usage_stats` and optimizer missing-index DMVs.

- **Reversible index cleanup**: Eligible unused nonclustered indexes (verified over `>= 7` days across all readable replicas, excluding clustered, unique and constraint-backing indexes) recommend `ALTER INDEX ... DISABLE` first—stopping write maintenance while keeping metadata intact so `ALTER INDEX ... REBUILD` can restore it instantly.
- **Offline SQL migration linting**: `culprit lint-sql migrations/ --format sarif` (and `--write` safe autofixes) checks T-SQL DDL for table-locking rewrites, missing `ONLINE = ON` / `LOCK_TIMEOUT` guards, unindexed foreign keys and destructive changes in CI or VS Code.
- **Opt-in schema & operational collector datasets**: Beyond core schema/workload polling, `culprit collect` supports bounded targeted captures for `objects`, `triggers`, `partitions`, `cached-definitions`, `retained-plans`, `query-compilation`, `cached-runtime`, `columnstore-rowgroups`, `backup-history`, `agent-jobs`, `cdc`, `change-feed`, `hyperscale-log-components`, `suspect-pages`, `full-text-populations`, `table-memory`, `numa-nodes` and `tempdb-metadata-memory` (`sys.dm_db_xtp_memory_consumers` on SQL Server 2022+). Every capture replay works offline via CLI and MCP `analyze_capture`.

## Limits and evidence

- **Session guardrails**: Every monitoring connection sets `LOCK_TIMEOUT 2000`, `DEADLOCK_PRIORITY LOW` and `READ UNCOMMITTED` where appropriate, tags every query with `/* culprit */` so Culprit's own SQL is excluded from workload findings, and never replays timed-out batches automatically.
- **Confirmed cancellation**: Background telemetry owns each TDS connection until completion or confirmed cancellation; if driver cancellation remains uncertain, the workspace fences further background polls rather than leaking orphaned sessions.
- **Azure SQL pressure admission**: Heavy background recipes verify database CPU, instance CPU, worker %, data I/O and log-write pressure before running and abort immediately when pressure is elevated.
- **Operational scope**: Transaction log occupancy (`log_reuse_wait_desc`), filegroup space, shared `tempdb` user/internal/version-store allocation, last recorded `DBCC CHECKDB` timestamp (`DATABASEPROPERTYEX`, never running `DBCC CHECKDB`), and identity/sequence exhaustion bounds keep their exact database vs instance scope. See the shared [evidence rules](coverage.md) and [collector source coverage](../collector.md).

## Verification

Exercised against **SQL Server 2022** and **Azure SQL Database**, with saved snapshots, `.sqlplan` / `.xel` corpora and offline golden replay tests. Spin up the local Docker fixture with planted indexing, parameter-sensitivity, blocking and deadlock scenarios in [`dev/mssql/README.md`](../../dev/mssql/README.md).

All declared SQL Server sources and capability locks live in [`culprit-mssql/src/sources.rs`](../../crates/providers/culprit-mssql/src/sources.rs).
