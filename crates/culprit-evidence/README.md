# culprit-evidence

## Purpose

Read and verify captured database evidence from a telemetry store and its recovery archive.

## Responsibilities

- Own the shared analytical schemas and projection contracts.
- Verify publication, row membership and original capture identity.
- Bound warehouse queries, proof reads and optional SQL or plan hydration.
- Expose authenticated originals to the existing pure analyzers.

## How it fits

ADX and ClickHouse adapters implement the same read contracts. Recovery adapters supply immutable objects. The engine composes these readers with independently supplied credentials; database providers and collector scheduling are separate dependencies.

## Main entry points

- [Evidence reader](src/analytical/evidence/mod.rs): bounded capture reads.
- [Read-store contract](src/analytical/read_store.rs): warehouse queries and publication checks.
- [Recovery reader](src/delivery/object_store/mod.rs): immutable recovery objects.
- [Original projection](src/original_projection.rs): verified facts for shared analysis.
