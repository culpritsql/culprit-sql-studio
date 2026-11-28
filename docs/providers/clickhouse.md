# ClickHouse

## Connecting

Choose **ClickHouse** in Studio or add a source connection to the CLI workspace. Use the server’s HTTP interface. The default is HTTPS on port 8443 with certificate and host verification.

```sh
culprit conn add analytics --url 'clickhouse://culprit_reader@ch.example.com:8443/analytics' --password-env CULPRIT_SOURCE_PASSWORD
culprit scan analytics
```

For a local plaintext fixture, specify its actual HTTP port and add `?sslmode=disable`. The native TCP ports 9000 and 9440 use a different protocol. Each scan follows one node and one database. For predictable replica coverage, configure a connection per node or a session-affine endpoint.

## Permissions and setup

Use a dedicated reader. Grant visibility of the monitored database and the system tables the role is allowed to inspect. For example, the following grants enable workload history and catalog inspection for an existing reader.

```sql
GRANT SHOW TABLES, SHOW COLUMNS ON analytics.* TO culprit_reader;
GRANT SELECT ON system.databases TO culprit_reader;
GRANT SELECT ON system.tables TO culprit_reader;
GRANT SELECT ON system.columns TO culprit_reader;
GRANT SELECT ON system.query_log TO culprit_reader;
```

Culprit sends a read-only setting and per-query resource limits with every request. A settings profile that fixes or forbids these settings can reject the read before the system table is accessed. Keep the account's grants restricted to the required reads and have its administrator configure the allowed values in its settings profile. ClickHouse supports bounded exceptions for individual settings. [ClickHouse settings constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings)

For health and investigations, grant `SELECT` individually on the system tables in the table below. Live uses `system.processes`. Editor queries and estimated plans additionally require the reader to access the selected objects. Native role visibility, query logging, retention and log flush timing determine what appears. Culprit reads the existing logs. It does not enable logging or issue `SYSTEM FLUSH LOGS`.

## Workload and plans

Query families group retained terminal events by `normalized_query_hash`. Counts include initial requests whose native database list names exactly the selected database. A distributed request is counted at its initiator. The desktop can show a bounded SQL sample for each family. The archive retains family hashes, intervals, runs, elapsed time, rows, bytes and the full-population totals used for shares. A complete SQL sample within the size limit passes through Culprit’s SQL redactor before export. Oversized samples retain an explicit reason alongside the counters. Rows read measure ClickHouse rows, and elapsed time can include work on other nodes.

Use Plans or the read-only editor for `EXPLAIN PLAN json=1`. Plans show ClickHouse’s physical steps. Estimated costs, actual operator timings and historical execution plans are unavailable from that output.

## Live activity and waits

Live lists visible foreground initial requests whose current database matches the connection. It displays the native query ID, user, elapsed time, read rows and current statement. A current database is session context. It does not identify every database a live query may touch.

## Schema and indexes

Schema reads table engines, available row and byte totals, the presence of a primary key and typed column metadata. MergeTree sorting keys and data-skipping indexes have different semantics from relational covering indexes. Missing native row counts remain unknown.

## Limits and evidence

| Source | What an investigator can establish |
|---|---|
| `system.query_log` | The reporting node recorded an execution failure. Named exceptions distinguish memory, concurrency, timeout, disk space, Keeper and too-many-parts failures. Cancellation remains history. |
| `system.parts` and `system.merges` | Active part counts per partition and current merge work. Counts provide context for write pressure. |
| `system.mutations` | Unfinished mutation with a recorded failed part and failure time. |
| `system.replicas` and `system.replication_queue` | Read-only or expired replica sessions, a queue with at least five minutes of native delay, and tasks with retry exceptions. |
| `system.distribution_queue` | A blocked distributed send queue with pending data files. The archive uses an opaque queue identity. |
| `system.asynchronous_insert_log` | Recorded parsing and flush failures for asynchronous inserts. |
| `system.query_log` and `system.part_log` | A focused lookup of the original failed execution and optional part events on a table it named during the five minutes ending at the failure. Concurrent part events establish activity on that table, not responsibility for the failure. |

The operational recipe collects a 15-minute query-log window plus current table state. For failed executions it also retains the available per-query CPU and I/O counters, selected marks and parts, and spill counters. These come from the original query log row in the same read. Native statement limits bound threads, elapsed time, memory and input size. Row caps and unreadable sources remain visible in coverage. One node cannot establish cluster-wide health. Collection requires its original ClickHouse HTTP session through the final identity check. An unconfirmed native shutdown retains the normal workspace collection fence.

Save the same named source in the collector workspace and select the operational recipe. The fleet’s recovery and analytical destination settings control publication. The following commands capture one database into a local outbox.

```sh
culprit collect outbox init ./outbox
culprit collect once analytics --collection operational --outbox ./outbox
```

For an exact execution follow-up, use `collect once analytics --selection focus.json --outbox ./outbox`. In Health, expand the original failure under ClickHouse operations and use **Copy collector selection** to get the JSON. It carries the database identity and native microsecond timestamps. The table is optional. A missing retained execution produces a retention gap.

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

Select ADX in the collector’s warehouse settings to publish ClickHouse observations there. The source reader and the ADX query and ingestion identities are configured independently. The same operational evidence can be published to a ClickHouse warehouse. See the [collector configuration](../collector.md) and [Helm chart](../../deploy/helm/culprit-collector/README.md).

## Verification

`dev/smoke-clickhouse-source.py` starts a disposable ClickHouse server and verifies native query families, a refused insert, a failed mutation, a malformed asynchronous insert and a query that exceeds the server’s group limit. Its `--provider-test` option also exercises the HTTP provider, finding input, archive recipe, focused lookup, editor and plan parser. Use `--image` to select a server version. Production resource cost depends on log volume, retention, table count and the server’s visibility policy.
