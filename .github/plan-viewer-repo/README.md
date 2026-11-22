<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/hero-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/hero-light.png">
  <img alt="@culpritsql/plan-viewer" src="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/hero-light.png" width="100%">
</picture>

<br />

[![npm](https://img.shields.io/npm/v/%40culpritsql%2Fplan-viewer?style=flat-square&label=npm&color=ffcf33&labelColor=181715)](https://www.npmjs.com/package/@culpritsql/plan-viewer)
[![React 18 | 19](https://img.shields.io/badge/react-18%20%7C%2019-61dafb?style=flat-square&labelColor=181715)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/types-ts--rs-3178c6?style=flat-square&labelColor=181715)](https://github.com/culpritsql/culprit-sql-studio)
[![License: MIT](https://img.shields.io/badge/license-MIT-4cbb84?style=flat-square&labelColor=181715)](./LICENSE)

**Interactive execution plan diagrams, structural diffs, and deadlock cycle graphs for React.**
<br />
Extracted directly from [Culprit SQL Studio](https://github.com/culpritsql/culprit-sql-studio) and paired with [`@culpritsql/plans`](https://github.com/culpritsql/culprit-plans).

[Quickstart](#quickstart) · [PlanViewer](#planviewer) · [PlanDiffViewer](#plandiffviewer) · [DeadlockViewer](#deadlockviewer) · [Theming](#theming--css-isolation) · [Primitives](#granular-primitives)

</div>

---

## Quickstart

```sh
npm install @culpritsql/plan-viewer @culpritsql/plans
```

```tsx
import { init, parsePlan } from "@culpritsql/plans";
import { PlanViewer } from "@culpritsql/plan-viewer";
import "@culpritsql/plan-viewer/styles.css";

await init();
const plan = parsePlan(explainJsonOrXml);

export function QueryExplainPanel() {
  return <PlanViewer plan={plan} title="Checkout Query" />;
}
```

---

## `PlanViewer`

Drop-in single-plan explorer with bottleneck insights, measured runtime strip, interactive layout switcher, and operator inspector.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-plan-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-plan-light.png">
  <img alt="PlanViewer preview" src="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-plan-light.png" width="100%">
</picture>

- **Ranked insights**: Highlights high-impact bottlenecks with plain-language explanations and copyable SQL fixes.
- **Three views**: Switch seamlessly between **Diagram**, **Tree**, and sortable **Operations** table.
- **Actual run metrics**: Surfaces elapsed duration, buffer reads, memory grants, disk spills, and parallel skew.
- **Operator inspector**: Compares estimated vs actual row counts, seek/filter predicates, and engine properties.

```tsx
<PlanViewer
  plan={plan}
  title="Recent Paid Orders"
  defaultLook="diagram" // "diagram" | "tree" | "operations"
  theme="system"        // "light" | "dark" | "system"
/>
```

---

## `PlanDiffViewer`

Side-by-side execution plan comparison for before/after index changes, query rewrites, and regressions.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-diff-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-diff-light.png">
  <img alt="PlanDiffViewer preview" src="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-diff-light.png" width="100%">
</picture>

- **Verdict banner**: Computes measured speedup or estimated cost ratio (`167x faster`, `3.2x costlier`).
- **Structural diff**: Marks access-path flips, operator swaps, and added or removed subtrees directly on cards.
- **Why it changed**: Surfaces index additions, cardinality shifts, parameter differences, and estimator changes.
- **Synchronized canvas**: Mirrors zoom and viewport scrolling across Plan A and Plan B.

```tsx
import { diffPlans } from "@culpritsql/plans";
import { PlanDiffViewer } from "@culpritsql/plan-viewer";

const diff = diffPlans(beforePlan, "Before", afterPlan, "After");

<PlanDiffViewer a={beforePlan} b={afterPlan} diff={diff} nameA="Before" nameB="After" />;
```

---

## `DeadlockViewer`

Visual lock-cycle graph and root-cause breakdown for SQL Server XML deadlocks, PostgreSQL logs, and InnoDB status dumps.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-deadlock-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-deadlock-light.png">
  <img alt="DeadlockViewer preview" src="https://raw.githubusercontent.com/culpritsql/culprit-plan-viewer/main/assets/preview-deadlock-light.png" width="100%">
</picture>

- **Cycle SVG graph**: Interactive `holds` and `wants` lock curves with hover focus and victim rollback badge.
- **Pattern badges**: Detects opposite lock ordering, key-lookup cycles, lock conversions, and escalations.
- **Actionable fixes**: Ordered remediation steps with syntax-highlighted SQL.
- **Session breakdown**: Displays application, login, isolation level, lock wait mode, and statement per participant.

```tsx
import { parseDeadlock } from "@culpritsql/plans";
import { DeadlockViewer } from "@culpritsql/plan-viewer";

const [deadlock] = parseDeadlock(serverDeadlockText);

<DeadlockViewer deadlock={deadlock} />;
```

---

## Theming & CSS Isolation

`@culpritsql/plan-viewer/styles.css` scopes every token, reset, and selector inside `.culprit-pv`. Importing it never alters your application's `html`, `body`, scrollbars, or global `<button>` styles.

| Mode | Usage |
| --- | --- |
| **System (default)** | Follows `prefers-color-scheme: dark` automatically |
| **Component prop** | `<PlanViewer plan={plan} theme="dark" />` or `theme="light"` |
| **Ancestor attribute** | Set `data-theme="dark"` or `data-theme="light"` on `<html>`, `<body>`, or any wrapper |

---

## Granular Primitives

Need custom layout composition? Every building block is exported individually with full TypeScript definitions:

| Export | Purpose |
| --- | --- |
| `PlanSurface` | Combined insights, DocDB/type metadata, and active plan view without outer toolbar |
| `PlanDiagram` | Interactive DAG layout with pan, zoom, cost strip, and collapsible cheap branches |
| `PlanView` | Collapsible tree view with inline warning chips and cost heat bars |
| `PlanOperations` | Searchable, keyboard-navigable (`j`/`k`), multi-column sortable operator table |
| `PlanInsights` | Expandable bottleneck cards with Markdown export |
| `NodeDetails` | Standalone operator property and cardinality inspector |
| `ActualSummary` | Measured runtime, I/O, memory grant, and spill callout strip |
| `DeadlockGraph` | Standalone interactive lock-cycle SVG figure |
| `Sql` | Lightweight zero-dependency SQL tokenizer and formatter |
| `bindPlanViewerI18n` | Plug in a custom host localization catalog (ships English out of the box) |
| `bindPlanViewerClipboard` | Plug in a custom clipboard + toast notification handler |

---

## Supported Engines

Pair with [`@culpritsql/plans`](https://www.npmjs.com/package/@culpritsql/plans) to parse native plans and deadlocks in the browser or Node.js:

| Engine | Plans | Deadlocks |
| --- | --- | --- |
| **SQL Server** | `.sqlplan` Showplan XML | XML deadlock graphs |
| **PostgreSQL** | `EXPLAIN (FORMAT JSON)`, `EXPLAIN (ANALYZE, BUFFERS)` text | Server log deadlock excerpts |
| **MySQL & MariaDB** | `EXPLAIN FORMAT=JSON`, `EXPLAIN ANALYZE` tree | `SHOW ENGINE INNODB STATUS` |
| **YugabyteDB** | `EXPLAIN (FORMAT JSON)` + DocDB RPC/storage metrics | PostgreSQL-compatible logs |
| **SQLite** | `EXPLAIN QUERY PLAN` | Not applicable |

---

## Security & Architecture

- **Text-only rendering**: Database identifiers, SQL literals, and plan properties are rendered strictly as React text nodes (never `dangerouslySetInnerHTML`).
- **Zero telemetry or network calls**: Pure client/SSR React components (`lucide-react` icons + peer `react`).
- **Single source of truth**: Synced automatically from [`culpritsql/culprit-sql-studio`](https://github.com/culpritsql/culprit-sql-studio) (`packages/culprit-plan-viewer`).

## License

MIT. Third-party notices are distributed in `pkg/THIRD-PARTY-LICENSES.txt`.
