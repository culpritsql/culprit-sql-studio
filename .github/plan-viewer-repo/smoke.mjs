#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { access, readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const requirePkg = process.argv.includes("--require-pkg");
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(await readFile(path.join(root, "package.json"), "utf8"));

assert.equal(manifest.name, "@culpritsql/plan-viewer");
assert.equal(manifest.private, undefined, "published manifest must not set private");
assert.equal(manifest.type, "module");
assert.equal(manifest.license, "MIT");
assert.deepEqual(manifest.files, ["pkg"]);
assert.deepEqual(manifest.exports, {
  ".": { types: "./pkg/index.d.ts", import: "./pkg/index.js" },
  "./styles.css": "./pkg/styles.css",
});
assert.equal(manifest.publishConfig?.access, "public");
assert.equal(manifest.publishConfig?.provenance, true);
assert.equal(manifest.repository?.url, "git+https://github.com/culpritsql/culprit-plan-viewer.git");
assert.equal(typeof manifest.dependencies?.["lucide-react"], "string");
assert.equal(manifest.peerDependencies?.react, "^18.0.0 || ^19.0.0");

const reportPath = path.join(root, "pkg/build-report.json");
let hasPkg = true;
try {
  await access(reportPath);
} catch {
  hasPkg = false;
}

if (!hasPkg) {
  if (requirePkg) {
    throw new Error("pkg/build-report.json is missing; build and sync pkg/ before publishing");
  }
  console.log(JSON.stringify({ status: "SKIP_PKG", reason: "awaiting initial CI artifact sync" }));
  process.exit(0);
}

const reportRaw = await readFile(reportPath, "utf8");
assert(!reportRaw.includes("/Users/"), "build-report.json must not contain macOS user paths");
assert(!reportRaw.includes("/home/"), "build-report.json must not contain Linux home paths");

const report = JSON.parse(reportRaw);
assert.equal(report.target, "es2022-react-esm");
assert.equal(report.native_or_database_work, false);

const notices = await stat(path.join(root, "pkg/THIRD-PARTY-LICENSES.txt"));
assert(notices.size > 0, "THIRD-PARTY-LICENSES.txt must not be empty");

const css = await readFile(path.join(root, "pkg/styles.css"), "utf8");
assert(css.includes(".culprit-pv"), "styles.css must scope styles under .culprit-pv");
assert(!/^\s*(?:html|body|\*)\s*\{/m.test(css), "styles.css must never emit unscoped html, body, or * selectors");

const sha256 = async (filePath) => createHash("sha256").update(await readFile(filePath)).digest("hex");

for (const [relativePath, meta] of Object.entries(report.artifacts)) {
  const fullPath = path.join(root, "pkg", relativePath);
  const info = await stat(fullPath);
  assert.equal(info.size, meta.bytes, `size mismatch for ${relativePath}`);
  assert.equal(await sha256(fullPath), meta.sha256, `sha256 mismatch for ${relativePath}`);
}

const modelFiles = (await readdir(path.join(root, "pkg/models"))).filter((f) => f.endsWith(".d.ts")).sort();
assert.equal(modelFiles.length, report.models.length, "packaged model count must match build report");
for (const model of report.models) {
  const modelPath = path.join(root, "pkg/models", `${model.name}.d.ts`);
  assert.equal(await sha256(modelPath), model.package_sha256, `model digest mismatch for ${model.name}`);
}

const React = (await import("react")).default;
const { renderToStaticMarkup } = await import("react-dom/server");
const viewer = await import(new URL("../pkg/index.js", import.meta.url));

for (const requiredExport of [
  "ActualSummary",
  "DeadlockGraph",
  "DeadlockViewer",
  "NodeDetails",
  "OpIcon",
  "PlanDiagram",
  "PlanDiffViewer",
  "PlanInsights",
  "PlanOperations",
  "PlanSurface",
  "PlanView",
  "PlanViewer",
  "Sql",
  "bindPlanViewerClipboard",
  "bindPlanViewerI18n",
]) {
  assert.equal(typeof viewer[requiredExport], "function", `missing runtime export: ${requiredExport}`);
}

const samplePlan = {
  engine: "postgres",
  kind: "actual",
  statement: "SELECT id FROM orders WHERE customer_id = 42 AND note <> '<script>evil()</script>'",
  total_cost: 120.5,
  warnings: ["misestimate"],
  props: {},
  raw: null,
  facts: {
    constants_redacted: false,
    estimated_cost_unavailable: false,
    docdb: null,
    current_type_schema: null,
  },
  insights: {
    note: null,
    items: [
      {
        rule: "seq_scan_filter",
        area: "index",
        severity: "high",
        title: "Full scan filters 98% of orders",
        detail: "Index `orders(customer_id)` avoids reading unrelated pages.",
        fix: "Create a supporting B-tree index.",
        fix_sql: "CREATE INDEX CONCURRENTLY ix_orders_customer ON public.orders (customer_id);",
        verify: "Re-run EXPLAIN (ANALYZE, BUFFERS) and confirm Index Scan.",
        impact: 0.92,
        nodes: [0],
      },
    ],
  },
  view: {
    kind: "actual",
    cost_basis: 120.5,
    cost_note: null,
    timed_total_ms: 48.2,
    time_note: null,
    hottest_cost: 0,
    hottest_time: 0,
    query: null,
    current_type_schema: null,
    actual: {
      elapsed_ms: 48.2,
      cpu_ms: null,
      logical_reads: 940,
      physical_reads: 12,
      rows: 14,
      memory_granted_kb: null,
      memory_used_kb: null,
      spilled: false,
      spill_pages: null,
      timed: true,
      slowest_node: 0,
      slowest_ms: 48.2,
      worst_estimate_node: 0,
      worst_estimate: 25,
      skew: null,
      last_run_at: null,
      workers: null,
      triggers: [],
    },
    insights: [{ rule: "seq_scan_filter", nodes: [0], share: 1 }],
    nodes: [
      {
        id: 0,
        cost_share: 1,
        subtree_cost_share: 1,
        time_share: 1,
        subtree_time_share: 1,
        own_ms: 48.2,
        layout_own_share: 1,
        layout_subtree_share: 1,
        estimated_rows: 350,
        estimated_rows_for_runs: 350,
        rows_out: 14,
        runs: 1,
        misestimate: -25,
        uncosted: false,
        guess: null,
        no_share_reason: null,
        finding_no_share_reason: null,
        page_server_io: null,
      },
    ],
  },
  root: {
    id: 0,
    op: "Seq Scan",
    kind: "scan",
    detail: null,
    object: "public.orders",
    index: null,
    predicate: "customer_id = 42",
    seek_predicate: null,
    output: [],
    est_rows: 350,
    act_rows: 14,
    rows_read: 18400,
    table_rows: null,
    est_executions: 1,
    act_executions: 1,
    self_cost: 120.5,
    subtree_cost: 120.5,
    act_time_ms: 48.2,
    warnings: ["misestimate"],
    props: {},
    facts: { estimated_rows_unavailable: false },
    children: [],
  },
};

const sampleDiff = {
  comparable: true,
  same_shape: false,
  cost_a: 120.5,
  cost_b: 8.4,
  cost_ratio: 8.4 / 120.5,
  time_a_ms: 48.2,
  time_b_ms: 1.4,
  headlines: ["After replaces Seq Scan on orders with an Index Scan (34.4x faster)."],
  matches: [{ a: 0, b: 0 }],
  regions: [{ a: [0], b: [0] }],
  only_a: [],
  only_b: [],
  changes: [
    {
      kind: "access",
      a: 0,
      b: 0,
      title: "Access on orders changed from Seq Scan to Index Scan",
      detail: "Uses ix_orders_customer",
    },
  ],
  scenarios: [],
};

const sampleDeadlock = {
  id: "dl-1",
  at: "2026-10-11T13:48:28Z",
  source: "pasted",
  pattern: "opposite-order",
  headline: "Two sessions locked orders and inventory in opposite order",
  explanation: "Session 74 updated orders then waited on inventory while Session 112 held inventory.",
  fixes: [
    {
      title: "Acquire locks in consistent table order",
      detail: "Update orders before inventory in every transaction.",
      sql: "BEGIN; UPDATE orders SET status = 'paid' WHERE id = $1; COMMIT;",
      finding_id: null,
    },
  ],
  resources: [
    {
      id: "r1",
      kind: "keylock",
      object: "public.orders",
      index: "orders_pkey",
      holders: [{ process: "p1", mode: "X" }],
      waiters: [{ process: "p2", mode: "X" }],
    },
  ],
  processes: [
    {
      id: "p1",
      session: "74",
      victim: true,
      application: "checkout",
      login: "app",
      host: "web-1",
      isolation: "read committed",
      wait_mode: "X",
      procedure: null,
      statement: "UPDATE orders SET note = '<script>bad()</script>' WHERE id = 1",
    },
    {
      id: "p2",
      session: "112",
      victim: false,
      application: "worker",
      login: "app",
      host: "job-1",
      isolation: "read committed",
      wait_mode: "X",
      procedure: null,
      statement: "UPDATE inventory SET qty = qty - 1 WHERE id = 1",
    },
  ],
};

const planHtml = renderToStaticMarkup(React.createElement(viewer.PlanViewer, { plan: samplePlan, title: "Checkout" }));
assert(planHtml.includes("culprit-pv"), "PlanViewer SSR output missing culprit-pv root");
assert(planHtml.includes("Full scan filters 98% of orders"), "PlanViewer SSR output missing insight title");
assert(!planHtml.includes("<script>evil()"), "PlanViewer SSR output failed to escape SQL script tag");

const diffHtml = renderToStaticMarkup(
  React.createElement(viewer.PlanDiffViewer, { a: samplePlan, b: samplePlan, diff: sampleDiff }),
);
assert(diffHtml.includes("cmp-verdict"), "PlanDiffViewer SSR output missing verdict section");

const deadlockHtml = renderToStaticMarkup(React.createElement(viewer.DeadlockViewer, { deadlock: sampleDeadlock }));
assert(deadlockHtml.includes("dl-graph"), "DeadlockViewer SSR output missing SVG graph");
assert(!deadlockHtml.includes("<script>bad()"), "DeadlockViewer SSR output failed to escape SQL script tag");

console.log(
  JSON.stringify({
    status: "PASS",
    version: manifest.version,
    models: modelFiles.length,
    messages: report.embedded_catalog?.message_count,
  }),
);
