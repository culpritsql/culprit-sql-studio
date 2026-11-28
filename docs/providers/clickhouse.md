# ClickHouse

[Engine parity](../parity.md)

Culprit inspects ClickHouse servers over read-only HTTP(S), scoping each connection to one node and one database. Workload families, exceptions and execution profiles come from `system.query_log`, live requests come from `system.processes`, physical query steps come from `EXPLAIN PLAN json=1`, and MergeTree operational diagnostics cover parts, merges, mutations, replication queues, distributed send queues and asynchronous inserts.

## Connecting

Select **ClickHouse** in Culprit SQL Studio or register a `clickhouse://` HTTP(S) endpoint on the CLI:

```sh
# Production HTTPS endpoint (port 8443, TLS certificate & hostname verification enabled by default)
culprit conn add analytics --env prod --url 'clickhouse://culprit_reader@ch.example.com:8443/analytics' --password-env CULPRIT_SOURCE_PASSWORD
culprit scan analytics

# Local plaintext fixture over HTTP (explicit port + ?sslmode=disable)
culprit conn add ch-local --env dev --url 'clickhouse://culprit_reader@127.0.0.1:8123/analytics?sslmode=disable'
```

| Capability | ClickHouse provider behavior |
|---|---|
| Transport & TLS | Speaks ClickHouse's HTTP(S) interface (`8443` default, `sslmode=verify-full` with optional `sslrootcert`); native TCP ports `9000` / `9440` use a different wire protocol and are not used |
| Node & database scope | Each connection inspects **one node and one database**; for predictable replica coverage behind a load balancer, save a connection per node or use a session-affine endpoint |
| Authentication | Dedicated password (stored in OS keychain or `--password-env`) or explicitly configured passwordless reader; cloud IAM/Entra tokens and SSH jump-host forwarding (`--ssh`) are refused |
| Session safety | Sends read-only execution settings and per-query resource ceilings on every HTTP request and maintains a dedicated HTTP session ID through final identity verification |

## Permissions and setup

Create a least-privilege ClickHouse reader and grant catalog visibility on the monitored database plus `SELECT` on the `system.*` tables you want Culprit to inspect:

```sql
-- Core catalog and query-history visibility
GRANT SHOW TABLES, SHOW COLUMNS ON analytics.* TO culprit_reader;
GRANT SELECT ON system.databases TO culprit_reader;
GRANT SELECT ON system.tables TO culprit_reader;
GRANT SELECT ON system.columns TO culprit_reader;
GRANT SELECT ON system.query_log TO culprit_reader;

-- Live requests and MergeTree / cluster operational health
GRANT SELECT ON system.processes TO culprit_reader;
GRANT SELECT ON system.parts TO culprit_reader;
GRANT SELECT ON system.merges TO culprit_reader;
GRANT SELECT ON system.mutations TO culprit_reader;
GRANT SELECT ON system.replicas TO culprit_reader;
GRANT SELECT ON system.replication_queue TO culprit_reader;
GRANT SELECT ON system.distribution_queue TO culprit_reader;
GRANT SELECT ON system.asynchronous_insert_log TO culprit_reader;
GRANT SELECT ON system.part_log TO culprit_reader;
```

- **Settings profile constraints**: Because Culprit attaches a read-only flag and per-query resource bounds (`max_rows_to_read`, `max_bytes_to_read`, `max_memory_usage`, timeouts) to every HTTP query, a restrictive ClickHouse settings profile that marks those settings `CONST` / `READONLY` can reject requests before touching `system.*`. Configure bounded exceptions in the reader's profile where needed; see [ClickHouse settings constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings).
- **Non-intrusive log reads**: Running `EXPLAIN PLAN` or read-only editor queries additionally requires `SELECT` on the referenced tables. Culprit reads whatever `system.query_log` and `system.part_log` rows have already flushed—it never enables logging or issues `SYSTEM FLUSH LOGS`.

## Workload and plans

| Capability | ClickHouse source and behavior |
|---|---|
| Statement workload | Groups completed initial requests in `system.query_log` by `normalized_query_hash` where the query's native `databases` list names the selected database |
| Distributed queries | Counted once at the initiating node; `read_rows` counts ClickHouse rows read and elapsed duration can include remote shard work |
| Timelines & comparison | Time-bucketed execution and latency series plus two-period comparisons across logged initial requests |
| SQL sample redaction | Bounded SQL samples pass through Culprit's SQL literal redactor before export; oversized statements retain their exact counters with an explicit truncation note |
| Execution plans | Runs `EXPLAIN PLAN json=1` in Studio, CLI and the read-only editor (and imports ClickHouse plan JSON in [explain.culpritsql.com](https://explain.culpritsql.com)) to visualize physical pipeline steps |
| Unavailable plan metrics | ClickHouse `EXPLAIN PLAN` does not emit relational optimizer cost numbers, actual per-operator timings or retained historical plans |

## Live activity and waits

Live queries `system.processes` for visible foreground initial requests whose current session database matches the connection—displaying native `query_id`, user, elapsed duration, rows read and current SQL text.

- **Database context boundary**: A process's current database reflects session state rather than every cross-database table a running query might read.
- **Wait & progress model**: ClickHouse records query duration and resource/profile counters rather than relational lock-wait chains or an exclusive wait-category breakdown.
- **Session termination**: Outside `prod` connections, terminating an active request issues `KILL QUERY WHERE query_id = '...' SYNC;`.

## Schema and indexes

Schema inspection reads `system.databases`, `system.tables` and `system.columns` to report table engines (`MergeTree`, `ReplicatedMergeTree`, `Distributed`, etc.), row and on-disk byte totals, primary-key presence and typed column definitions.

- **MergeTree vs B-tree semantics**: MergeTree sorting keys (`ORDER BY` / sparse primary index) and data-skipping indexes operate on granules and parts rather than row-level B-tree pointers. Culprit never emits relational `CREATE INDEX` covering scripts or "unused index" drop verdicts against ClickHouse tables; instead, findings direct you to sorting keys, selected marks/parts and insert batching.

## Limits and evidence

| Source | What an investigator can establish |
|---|---|
| `system.query_log` | Recorded execution failures on the observed node—distinguishing memory limits, concurrency caps, timeouts, disk-space errors, Keeper errors and `TOO_MANY_PARTS` from benign cancellations |
| `system.parts` & `system.merges` | Active part counts per partition and in-flight background merges to contextualize insert/merge write pressure |
| `system.mutations` | Unfinished `ALTER TABLE ... UPDATE / DELETE` mutations with a recorded failed part and failure timestamp |
| `system.replicas` & `system.replication_queue` | Read-only or expired Keeper/ZooKeeper replica sessions, queues delayed `>= 5` minutes, and tasks failing with retry exceptions |
| `system.distribution_queue` | Blocked `Distributed` table send queues with pending on-disk batch files (exported with an opaque queue identity) |
| `system.asynchronous_insert_log` | Parsing and flush errors (`FlushError`) for asynchronous inserts |
| `system.query_log` & `system.part_log` | Targeted follow-up of one failed `query_id` plus concurrent part events on a named table during the 5 minutes preceding the failure (context, not causal blame) |

- **Per-request guardrails**: Native HTTP reads enforce an **8-second** statement timeout, **`2,000,000`** max rows read, **`128 MiB`** max bytes read / memory usage, and an **`8 MiB`** HTTP response ceiling. Unreadable tables and row caps appear explicitly in source coverage, single-node scans never claim cluster-wide health, and unconfirmed HTTP session shutdowns fence subsequent background collection.

### Background telemetry and focused investigations

`culprit collect` captures a 15-minute `system.query_log` window (including CPU/I/O profile counters, selected marks/parts and spill counters on failed queries) alongside current MergeTree, mutation, replication and queue state:

```sh
culprit collect outbox init ./outbox
culprit collect once analytics --collection operational --outbox ./outbox
```

To capture a targeted follow-up for a specific failed query (`system.query_log` + 5-minute `system.part_log` window), expand the failure under **Health → ClickHouse operations** in Studio and click **Copy collector selection**, then pass that selector file to `culprit collect once analytics --selection focus.json --outbox ./outbox`:

```json
{
  "kind": "click-house-investigation",
  "focus": {
    "target": {
      "engine": "clickhouse", "database": "analytics",
      "location": {"kind": "server", "host": "ch.example.com", "port": 8443, "instance": "node-a"},
      "incarnation": "637839ab-45c7-4091-b729-94717b33d2e4"
    },
    "request": {"node": "node-a", "database": "analytics", "query_id": "example-id", "started_us": 1735732800000000, "ended_us": 1735732802000000},
    "table": "events"
  }
}
```

Sealed ClickHouse captures publish to either **Azure Data Explorer (ADX)** or a **ClickHouse** telemetry warehouse using independent source, recovery and warehouse credentials. See [collector configuration](../collector.md) and the [Helm chart](../../deploy/helm/culprit-collector/README.md).

## Verification

`dev/smoke-clickhouse-source.py` boots a disposable ClickHouse container and verifies query-family aggregation, `TOO_MANY_PARTS` insert refusals, failed mutations, malformed asynchronous inserts and memory/group-by limit exceptions (`--provider-test` also exercises the HTTP provider, archive recipe, focused lookup, read-only editor and plan parser; pass `--image` to test specific ClickHouse versions).

- Disposable smoke suite: [`dev/smoke-clickhouse-source.py`](../../dev/smoke-clickhouse-source.py)
- Declared ClickHouse sources: [`culprit-clickhouse/src/sources.rs`](../../crates/providers/culprit-clickhouse/src/sources.rs)
