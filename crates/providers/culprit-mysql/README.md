# culprit-mysql

## Purpose

Read MySQL and MariaDB diagnostics and native plans for Culprit.

## Responsibilities

- Collect catalogs, Performance Schema observations, available native statement history and live activity.
- Handle MySQL and MariaDB differences in plans, instrumentation, locks and timeouts.
- Reuse native layouts and decoders across scans and optional telemetry captures.
- Account for native query phases and retain cancellation ownership until responses settle.

## How it fits

`culprit-engine` supplies resolved connection inputs. This provider produces `culprit-core` facts for the pure
`culprit-checks` pipeline. Native feature and permission gaps remain explicit; a declared telemetry route does
not establish its availability or production cost on a particular server.

## Main entry points

- [`MysqlProvider`](src/lib.rs): implementation of the core `Provider` interface for both products.
- [`telemetry_recipes`](src/lib.rs): static declarations from the native dispatch catalog.
- [`culprit_plans::mysql`](../../culprit-plans/src/mysql/mod.rs): shared pure MySQL/MariaDB native plan parsing.
- [`read`](src/read.rs): bounded native reads, accounting and response retirement.
- [`sources`](src/sources.rs): native sources and their permissions.

## Standard Settings collection

Standard Settings admission covers MySQL 8.4 and MariaDB 11.8 in a writable native role. The same snapshot reader
uses a bounded `SHOW GLOBAL VARIABLES` allowlist and verifies actual product/version, member/process, database and
role on its held connection. Its normal path charges 18 dispatches including setup; stricter caller limits and
awaited cancellation/close still apply. Other versions and read-only roles do not inherit this admission.

The scan and retained engine share one checked conversion and `ConfigCheck`; automatic findings use the canonical
Gold/archive path. Values retain server scope, explicit units and catalog-default provenance. Unknown values are
not zero/default, and a completed read does not prove application-session configuration. Settings acquisition with
Performance Schema disabled does not enable instrumentation or qualify other sources. Counter-only analysis keeps
missing SQL-definition, application-origin, population and denominator limitations rather than a healthy verdict.

Source credentials are independent of warehouse and recovery credentials. See the
[collector's admission and budgets](../../../docs/collector.md#configure-and-validate),
[MySQL setup](../../../docs/providers/mysql.md) and [MariaDB setup](../../../docs/providers/mariadb.md).
