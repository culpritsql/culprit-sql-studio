<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plans/main/assets/hero-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plans/main/assets/hero-light.png">
  <img alt="@culpritsql/plans" src="https://raw.githubusercontent.com/culpritsql/culprit-plans/main/assets/hero-light.png" width="100%">
</picture>

<br />

[![npm](https://img.shields.io/npm/v/%40culpritsql%2Fplans?style=flat-square&label=npm&color=ffcf33&labelColor=181715)](https://www.npmjs.com/package/@culpritsql/plans)
[![Runtime: Rust + Wasm](https://img.shields.io/badge/runtime-Rust%20%2B%20Wasm-dea584?style=flat-square&labelColor=181715)](https://webassembly.org)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0-4cbb84?style=flat-square&labelColor=181715)](https://www.npmjs.com/package/@culpritsql/plans?activeTab=dependencies)
[![TypeScript](https://img.shields.io/badge/types-ts--rs-3178c6?style=flat-square&labelColor=181715)](https://github.com/culpritsql/culprit-sql-studio)
[![License: MIT](https://img.shields.io/badge/license-MIT-4cbb84?style=flat-square&labelColor=181715)](./LICENSE)

**Portable WebAssembly execution plan parser, structural differ, deadlock & Extended Events (`.xel`) parser, and exporter.**
<br />
Compiled directly from [Culprit SQL Studio](https://github.com/culpritsql/culprit-sql-studio) and paired with [`@culpritsql/plan-viewer`](https://github.com/culpritsql/culprit-plan-viewer).

[Quickstart](#quickstart) · [Supported Engines](#supported-engines) · [API Reference](#api-reference) · [Node.js & Bundlers](#nodejs--bundlers) · [Security & Limits](#security--limits)

</div>

---

## Quickstart

```sh
npm install @culpritsql/plans
```

Initialize the WebAssembly module once, then call the synchronous parser, comparison, deadlock, Extended Events, and export operations:

```ts
import {
  init,
  parsePlan,
  diffPlans,
  parseDeadlock,
  parseXel,
  parseExtendedEventsXml,
  exportPlan,
} from "@culpritsql/plans";

await init();

const before = parsePlan(beforePlanText);
const after = parsePlan(afterPlanText, "postgres");
const diff = diffPlans(before, "Before", after, "After");

const deadlocks = parseDeadlock(deadlockLogText);
const xelEvents = parseXel(new Uint8Array(await xelFile.arrayBuffer()));
const exported = exportPlan(after, "checkout-query", "markdown", {
  redact: true,
  notes: [],
});
```

> Rendering in React? Pass `Plan`, `PlanDiff`, `Deadlock`, and `ExtendedEventsBatch` directly to [`@culpritsql/plan-viewer`](https://www.npmjs.com/package/@culpritsql/plan-viewer). Both packages share the exact same generated `ts-rs` model types.

---

## Supported Engines

| Engine | Execution Plans | Deadlocks & Native Events |
| --- | --- | --- |
| **SQL Server** | `.sqlplan` Showplan XML | XML deadlock graphs, binary `.xel` & XML Extended Events (`xml_deadlock_report`, `error_reported`, `attention`) |
| **PostgreSQL** | `EXPLAIN (FORMAT JSON)`, `EXPLAIN (ANALYZE, BUFFERS)` text | Server log deadlock excerpts |
| **MySQL & MariaDB** | `EXPLAIN FORMAT=JSON`, `EXPLAIN ANALYZE` tree text | `SHOW ENGINE INNODB STATUS` |
| **YugabyteDB** | `EXPLAIN (FORMAT JSON)` + DocDB RPC/storage metrics | PostgreSQL-compatible logs |
| **SQLite** | `EXPLAIN QUERY PLAN` text | Not applicable |

---

## API Reference

| Export | Signature | Description |
| --- | --- | --- |
| `init` | `init(input?): Promise<void>` | Instantiates the Wasm module once; subsequent calls reuse the live instance |
| `parsePlan` | `parsePlan(input: string, engine?: Engine): Plan` | Auto-detects format or validates strictly against `"mssql" \| "postgres" \| "mysql" \| "yugabyte" \| "sqlite"` |
| `diffPlans` | `diffPlans(a: Plan, nameA: string, b: Plan, nameB: string, context?: CompareContext): PlanDiff` | Computes structural operator changes, access-path flips, cardinality shifts, and speedup/cost verdicts |
| `parseDeadlock` | `parseDeadlock(input: string): Deadlock[]` | Extracts victim selection, participant sessions, held/waited locks, and original timestamps |
| `parseXel` | `parseXel(input: Uint8Array, limits?: Partial<NativeLogLimits>): ExtendedEventsBatch` | Decodes binary SQL Server `.xel` captures (with SQL literal redaction and embedded deadlock graph extraction) |
| `parseExtendedEventsXml` | `parseExtendedEventsXml(input: string \| Uint8Array, limits?: Partial<NativeLogLimits>): ExtendedEventsBatch` | Decodes exported `<events>`, `<RingBufferTarget>`, or `<event>` XML records |
| `exportPlan` | `exportPlan(plan: Plan, title: string, format: ExportFormat, options: ExportOptions): ExportFile` | Exports `"markdown"`, `"html"`, or `"native"` with optional SQL literal redaction |
| `ParserError` | `class ParserError extends Error` | Structured refusal error (`error.error` carries `{ code, message, hint, detail }`) instead of Wasm traps |

### Redacted Exports

Setting `redact: true` scrubs SQL string and numeric literals from statements, filter predicates, and native Showplan XML parameter lists before returning the file:

```ts
const file = exportPlan(plan, "Order Lookup", "html", {
  redact: true,
  notes: [],
});
// file => { name: "order-lookup.html", format: "html", contents: "<!doctype html>..." }
```

### Structured Error Handling

Malformed XML/JSON, mismatched engine hints, DTD attempts, or over-depth trees throw `ParserError` cleanly without corrupting the Wasm instance:

```ts
import { parsePlan, ParserError } from "@culpritsql/plans";

try {
  const plan = parsePlan(untrustedInput, "postgres");
} catch (err) {
  if (err instanceof ParserError) {
    console.error(err.error.code, err.error.message, err.error.hint);
  }
}
```

---

## Node.js & Bundlers

In browsers, `init()` resolves `culprit_wasm_bg.wasm` relative to `import.meta.url` via `fetch`. In Node.js or custom bundler setups, pass `module_or_path` explicitly:

```ts
import { readFile } from "node:fs/promises";
import { init, parsePlan } from "@culpritsql/plans";

const wasmBytes = await readFile(
  new URL(
    "./node_modules/@culpritsql/plans/pkg/native/culprit_wasm_bg.wasm",
    import.meta.url,
  ),
);
await init({ module_or_path: wasmBytes });

const plan = parsePlan("QUERY PLAN\n`--SCAN orders", "sqlite");
```

With **Vite** or **Next.js** static asset URLs:

```ts
import wasmUrl from "@culpritsql/plans/pkg/native/culprit_wasm_bg.wasm?url";
import { init } from "@culpritsql/plans";

await init({ module_or_path: wasmUrl });
```

---

## Security & Limits

- **Zero runtime dependencies**: Pure Rust compiled to `wasm32-unknown-unknown` with zero npm dependencies, no database drivers, and no network calls.
- **Hardened XML & JSON bounds**: Rejects XML `<!DOCTYPE>` / entity expansion (`XXE`) and enforces deterministic depth ceilings (`1,024` XML elements, `56` plan operator levels) on a dedicated 32 MiB Wasm linear stack.
- **HTML-safe exports**: HTML plan exports escape every identifier, predicate, and title string.
- **CSP compatibility**: Serve `.wasm` assets with `Content-Type: application/wasm` and include `'wasm-unsafe-eval'` in `script-src` under strict Content Security Policies.
- **Single source of truth**: Synced automatically from [`culpritsql/culprit-sql-studio`](https://github.com/culpritsql/culprit-sql-studio) (`crates/culprit-plans`, `crates/culprit-wasm`, `packages/culprit-wasm`).

## License

MIT. Third-party Rust crate notices are distributed in `pkg/THIRD-PARTY-LICENSES.txt`.
