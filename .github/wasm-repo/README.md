# @culpritsql/plans

Portable WebAssembly execution plan parser, comparator, deadlock parser, and exporter extracted from [Culprit SQL Studio](https://github.com/culpritsql/culprit-sql-studio).

Runs locally in browsers and JavaScript runtimes with zero runtime npm dependencies, no database drivers, and no network calls. TypeScript declarations are generated directly from Culprit's Rust domain model (`ts-rs`).

## Supported Formats

| Engine | Execution Plans | Deadlocks |
| --- | --- | --- |
| SQL Server | XML Showplan (`.sqlplan`) | XML deadlock graphs |
| PostgreSQL | `EXPLAIN (FORMAT JSON)`, `EXPLAIN (ANALYZE, BUFFERS)` text | Server log deadlock excerpts |
| MySQL & MariaDB | `EXPLAIN FORMAT=JSON`, `EXPLAIN ANALYZE` tree text | `SHOW ENGINE INNODB STATUS` deadlocks |
| YugabyteDB | `EXPLAIN (FORMAT JSON)`, text plans | PostgreSQL-compatible deadlock logs |
| SQLite | `EXPLAIN QUERY PLAN` text | Not applicable |

## Install

```sh
npm install @culpritsql/plans
```

## Quickstart

Initialize the WebAssembly module once, then call the synchronous operations:

```ts
import {
  init,
  parsePlan,
  diffPlans,
  parseDeadlock,
  exportPlan,
  ParserError,
} from "@culpritsql/plans";

await init();

const before = parsePlan(beforePlanText);
const after = parsePlan(afterPlanText, "postgres");
const diff = diffPlans(before, "before", after, "after");

const deadlocks = parseDeadlock(deadlockLogText);
const exported = exportPlan(after, "checkout-query", "markdown", {
  redact: true,
  notes: [],
});
```

### Node.js and Custom Asset URLs

In browsers, `init()` resolves `culprit_wasm_bg.wasm` relative to `import.meta.url` via `fetch`. In Node.js, or when a bundler emits the `.wasm` asset at a custom URL, pass `module_or_path` explicitly:

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

With Vite or Next.js static asset imports:

```ts
import wasmUrl from "@culpritsql/plans/pkg/native/culprit_wasm_bg.wasm?url";
import { init } from "@culpritsql/plans";

await init({ module_or_path: wasmUrl });
```

## API

### `init(input?): Promise<void>`

Loads and instantiates the WebAssembly module. Safe to call more than once; subsequent calls reuse the initialized instance.

### `parsePlan(input: string, engine?: Engine): Plan`

Parses raw plan text into a typed `Plan` tree (`"mssql" | "postgres" | "mysql" | "yugabyte" | "sqlite"`). When `engine` is omitted, the format is detected from the input. When `engine` is provided, incompatible inputs are rejected.

### `diffPlans(a: Plan, nameA: string, b: Plan, nameB: string, context?: CompareContext): PlanDiff`

Compares two parsed plans structurally and across operator estimates, actual row counts, and costs. Optional `CompareContext` supplies explicit table statistics without touching a database connection.

### `parseDeadlock(input: string): Deadlock[]`

Parses native deadlock XML or server log text into typed `Deadlock` observations, preserving participant processes, held/waited locks, victim selection, and original timestamps when present.

### `exportPlan(plan: Plan, title: string, format: ExportFormat, options: ExportOptions): ExportFile`

Formats a `Plan` as `"markdown"`, `"html"`, or `"native"` (`ExportFile` with `name`, `format`, `contents`). Setting `options.redact = true` scrubs SQL string and numeric literals from statements, predicates, and native XML parameters, and HTML exports escape all user and plan strings.

### `ParserError`

Invalid, empty, malformed, over-depth, or mismatched inputs throw `ParserError` rather than trapping the WebAssembly instance. Inspect `.error` for the structured Culprit error (`{ code, message, hint, detail }`).

## Runtime and Security Notes

- Serve `.wasm` files with `Content-Type: application/wasm`.
- Under a strict Content Security Policy, include `'wasm-unsafe-eval'` in `script-src`.
- XML DTD declarations and entity expansions are rejected. Inputs are bounded to 1024 nested XML elements and 56 plan operator levels.
- Compressed payload size is ~1.41 MB gzip (~1.00 MB Brotli) with 108 generated TypeScript model declarations.

## Source Repository

This repository is synced automatically from [`culpritsql/culprit-sql-studio`](https://github.com/culpritsql/culprit-sql-studio) (`crates/culprit-plans`, `crates/culprit-wasm`, and `packages/culprit-wasm`).

- Website: [culpritsql.com](https://culpritsql.com)
- Parser and engine source: [github.com/culpritsql/culprit-sql-studio](https://github.com/culpritsql/culprit-sql-studio)

## License

MIT. Third-party Rust crate notices are distributed in `pkg/THIRD-PARTY-LICENSES.txt`.
