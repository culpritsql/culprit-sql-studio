# culprit-postgres

## Purpose

Read PostgreSQL diagnostics and native plans for Culprit.

## Responsibilities

- Collect database catalogs, statement statistics, activity, waits and resource observations.
- Acquire native plans and retain their measured facts and limitations; pure parsing lives in `culprit-plans`.
- Share native projections between scans and optional telemetry captures.
- Bound native work and retain ownership through cancellation and connection close.

## How it fits

`culprit-engine` supplies connection settings and resolved credentials. This provider returns `culprit-core`
models; `culprit-checks` analyzes them without a connection. YugabyteDB reuses the public wire interface while
owning its own storage and source semantics.

## Main entry points

- [`PostgresProvider`](src/lib.rs): implementation of the core `Provider` interface.
- [`telemetry_recipes`](src/lib.rs): static declarations for implemented telemetry routes.
- [`culprit_plans::postgres`](../../culprit-plans/src/postgres/mod.rs): shared pure native JSON plan parsing.
- [`wire`](src/wire.rs): shared PostgreSQL transport, bounded readers and activity layouts.
- [`sources`](src/sources.rs): native sources and their permissions.

## Standard Settings collection

PostgreSQL 18 Standard collection includes Activity and guarded Settings. The Settings reader shares the scan's
allowlist and conversion, verifies its held native owner/role before and after the read, and seals six dispatches
including setup. The pure engine then runs the same `ConfigCheck` and publishes findings through the canonical
archive path. Raw values survive independently of advice: missing/session-only values, unknown roles, partial
coverage and standby-inapplicable maintenance checks remain explicit.

The guarded statement-counter route remains `Unqualified`. Counter-only evidence retains native identity/reset
facts but lacks the source-bound SQL, application origin and compatible workload denominator needed for application
findings. It does not acquire definitions from Activity or manufacture Query Store intervals.

Source credentials are resolved independently of warehouse and recovery credentials. See the
[collector's admission, budgets and Settings coverage](../../../docs/collector.md#configure-and-validate),
[PostgreSQL setup](../../../docs/providers/postgres.md) and the [architecture](../../../docs/ARCHITECTURE.md).
