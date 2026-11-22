# culprit-mcp

## Purpose

Let coding agents inspect Culprit through the Model Context Protocol over standard input and output.

## Responsibilities

- Expose tools for connections, findings, queries, plans and schema.
- Apply connection-level agent permissions.
- Require explicit approval for the opt-in read-only query tool.
- Record tool calls in the audit log.
- Optionally analyze verified local collector captures without opening a database.
- Configure supported coding-agent clients.

## How it fits

The `culprit mcp` command hosts this server. Tools use the engine's `Workspace` API and the same analysis as the app.
`--evidence-outbox DIRECTORY` adds the local-capture capability. An embedding host can inject its reader directly;
the default server does not depend on the collector or warehouse adapters.

## Main entry points

- [Server](src/lib.rs): tool handling and audit ownership.
- [Tools](src/tools.rs): tool definitions and dispatch.
- [Client setup](src/init.rs): coding-agent configuration.


## Warehouse-only investigation

`culprit mcp --warehouse-evidence evidence.json` explicitly delegates read-only resource and incident lookup to one
configured warehouse and recovery namespace. It needs no saved source connection. The server advertises `resources`
and `incident`, with evidence-only instructions; forged native tools are rejected before provider admission even when
the local workspace contains agent-enabled production connections. Native `--connection` selection is incompatible
with this CLI mode. An explicitly installed local `--evidence-outbox` reader remains a separate offline capability.

Call `resources`, select a returned database resource, then pass its `selector` unchanged to
`incident({resource: selector, around: "2026-11-22T10:00:00Z"})`. Agents do not construct range hashes, storage paths,
SQL or credentials. Resource pagination reuses the returned `as_of`, original window and opaque `next` cursor. The
bounded listing describes authenticated published observations, not configured fleet inventory, current native
ownership, complete history or health. Returned native time and source coverage remain independent.

The Engine capability delegates to the same `EvidenceService` resources/incident assembler. The canonical reader
checks the selector against the selected contract and actual recovery namespace on the same cumulative action
budget; it does not reset the allowance before discovery or ancestry. Warehouse/recovery credentials are independent
of source identities. The existing terminal audit records the call; it carries no source connection or SQL for these
tools. Partiality, unknown inputs, source-only attribution and omitted presentation details stay explicit.

SDK hosts install `WarehouseDelegation::read_only(reader)` with `with_warehouse`. That grant permits resource and
incident reads throughout the injected reader's configured namespace; it does not inherit a native connection
allowlist or grant native actions. Its `revoke()` is monotonic and checked before work and before publishing a
successful result. Backend credentials retain their own independent restrictions. A host may deliberately use
`ToolMode::Standard` to combine native and archive capabilities, or `ToolMode::EvidenceOnly` to expose only installed
source-free capabilities. No built-in warehouse adapter or credential discovery is linked into the MCP crate.

The ordinary CLI follows the same source-free selection: `culprit resources --config evidence.json`, then
`culprit incident <observed-database-or-display-name> --around <RFC3339> --config evidence.json`. Both dispatch
before opening a workspace. Name lookup refuses incomplete, missing or ambiguous results and consumes the same
bounded action for resource pages, namespace verification and incident ancestry; it does not grant a fresh read
allowance after finding a name. Structured MCP selectors avoid human name ambiguity while keeping that same
namespace authority boundary.
