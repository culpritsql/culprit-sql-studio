# culprit-mssql

## Purpose

Read SQL Server and Azure SQL evidence for Culprit. The provider turns native facts into shared models; pure checks explain those facts elsewhere.

## Responsibilities

- Read database metadata, Query Store, supported cache entries, current activity and resource statistics.
- Keep query incarnations, execution populations, observation bounds and missing-source reasons explicit.
- Parse native Showplan XML and retain the evidence its operators expose.
- Supply bounded telemetry readers with query, row, byte, timeout and cancellation ownership.

## How it fits

- `culprit-engine` selects the provider and supplies connection settings and resolved credentials.
- `culprit-core` defines the models and provider contracts; `culprit-read` supplies shared reader lifecycle support.
- `culprit-checks` analyzes snapshots without opening database connections. The collector uses the provider's declared telemetry recipes.
- A bounded query is not proof of acceptable production cost. Recipe cost qualification and source availability remain separate requirements.

## Explicit telemetry scopes

These routes reuse the native readers used by scans. Query ceilings include session setup and preserve stricter
caller limits; they are not measured production-cost claims.

| Subject and selection | Observations | Query ceiling |
|---|---|---|
| SQL Server / Managed Instance instance: `wait-counters` | Shared native waits, fenced to the instance/process and held connection; no database attribution or inferred reset | 8 |
| SQL Server / Managed Instance database: `operational` | Selected-database files/filegroups, log space, file I/O, version store and persistent version store/ADR; unavailable families retain coverage gaps | 22 when all five families are available |
| Verified writable SQL Server 2016+ / Managed Instance database: `settings` | Four native catalog families, including 21 public Query Store options with versioned NULLs and separate configuration/state/measurement meaning | 18 |
| Azure SQL elastic pool: `operational` | Native pool-scoped clerks, schedulers, performance counters, member file I/O and shared tempdb allocation; topology/compiler admission remain unavailable | 22 |

Pool tempdb belongs to the observed pool replica, not the selected member database or query. Missing member,
process, role or permission evidence remains unavailable. All MSSQL recipes remain `Unqualified` for automatic
admission pending the released upstream Tiberius cancellation prerequisite and native revalidation. Local SQL
Server 16 correctness passes do not qualify Azure/MI/Hyperscale support, production cost or active cancellation.

## Main entry points

- [`src/lib.rs`](src/lib.rs): `MssqlProvider`, its provider implementation and `telemetry_recipes()`.
- [`src/collect.rs`](src/collect.rs): snapshot acquisition and shared population collection.
- [`src/sources.rs`](src/sources.rs): source declarations and coverage requirements.
- [`src/telemetry/recipes.rs`](src/telemetry/recipes.rs): telemetry registrations and native reader selection.
- [`src/read.rs`](src/read.rs): bounded native reads and shutdown ownership.
- [`src/plan_xml`](src/plan_xml): SQL Server plan parsing.

See [provider documentation](../../../docs/providers/mssql.md), [design](../../../docs/DESIGN.md) and [architecture](../../../docs/ARCHITECTURE.md) for supported behavior and evidence rules.
