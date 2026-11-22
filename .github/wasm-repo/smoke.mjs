#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { access, readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const requirePkg = process.argv.includes("--require-pkg");
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(await readFile(path.join(root, "package.json"), "utf8"));

assert.equal(manifest.name, "@culpritsql/culprit-wasm");
assert.equal(manifest.private, undefined, "published manifest must not set private");
assert.equal(manifest.type, "module");
assert.equal(manifest.license, "MIT");
assert.deepEqual(manifest.files, ["pkg"]);
assert.deepEqual(manifest.exports, { ".": { types: "./pkg/index.d.ts", import: "./pkg/index.js" } });
assert.equal(manifest.publishConfig?.access, "public");
assert.equal(manifest.repository?.url, "git+https://github.com/culpritsql/culprit-wasm.git");

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
assert(!reportRaw.includes("bindgen_executable"), "build-report.json must not expose host executable paths");

const report = JSON.parse(reportRaw);
assert.equal(report.target, "wasm32-unknown-unknown");
assert.equal(report.profile, "release");
assert.equal(report.native_or_database_work, false);
assert.equal(report.stack_policy?.linear_stack_bytes, 32 * 1024 * 1024);

const notices = await stat(path.join(root, "pkg/THIRD-PARTY-LICENSES.txt"));
assert(notices.size > 0, "THIRD-PARTY-LICENSES.txt must not be empty");

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

const parser = await import(new URL("../pkg/index.js", import.meta.url));
assert.deepEqual(
  Object.keys(parser).sort(),
  ["ParserError", "diffPlans", "exportPlan", "init", "parseDeadlock", "parsePlan"],
);

const wasmBytes = await readFile(path.join(root, "pkg/native/culprit_wasm_bg.wasm"));
await parser.init({ module_or_path: wasmBytes });

const canary = "smoke-secret-token";
const mssqlXml = `<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementType="SELECT" StatementText="SELECT '${canary}' AS token" StatementSubTreeCost="1" StatementEstRows="1"><QueryPlan><RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="1" EstimatedTotalSubtreeCost="1"><ConstantScan/></RelOp><ParameterList><ColumnReference Column="@p0" ParameterDataType="nvarchar(30)" ParameterCompiledValue="N'${canary}'"/></ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>`;
const pgJson = JSON.stringify([
  {
    Plan: {
      "Node Type": "Seq Scan",
      "Relation Name": "orders",
      Alias: "o",
      "Startup Cost": 0.0,
      "Total Cost": 42.5,
      "Plan Rows": 250,
      "Plan Width": 32,
    },
  },
]);
const mysqlJson = JSON.stringify({
  operation: "Table scan on orders",
  query_type: "select",
  access_type: "table",
  table_name: "orders",
  schema_name: "shop",
  estimated_rows: 100.0,
  estimated_total_cost: 12.5,
});
const sqliteText = "QUERY PLAN\n`--SEARCH orders USING INDEX orders_customer_idx (customer_id=?)";
const pgDeadlock = [
  "2026-10-11 13:48:28.887 UTC [539] ERROR:  deadlock detected",
  "2026-10-11 13:48:28.887 UTC [539] DETAIL:  Process 539 waits for ShareLock on transaction 838; blocked by process 545.",
  "\tProcess 545 waits for ShareLock on transaction 837; blocked by process 539.",
  "\tProcess 539: UPDATE customers SET id = id WHERE id = 1",
  "\tProcess 545: UPDATE orders SET id = id WHERE id = 1",
].join("\n");

const mssqlPlan = parser.parsePlan(mssqlXml, "mssql");
const pgPlan = parser.parsePlan(pgJson, "postgres");
const mysqlPlan = parser.parsePlan(mysqlJson, "mysql");
const sqlitePlan = parser.parsePlan(sqliteText, "sqlite");

assert.equal(mssqlPlan.engine, "mssql");
assert.equal(pgPlan.engine, "postgres");
assert.equal(mysqlPlan.engine, "mysql");
assert.equal(sqlitePlan.engine, "sqlite");

const diff = parser.diffPlans(pgPlan, "before", pgPlan, "after");
assert.equal(diff.same_shape, true);
assert.equal(diff.changes.length, 0);

const deadlocks = parser.parseDeadlock(pgDeadlock);
assert.equal(deadlocks.length, 1);
assert.equal(deadlocks[0].processes.length, 2);

for (const format of ["markdown", "html", "native"]) {
  const exported = parser.exportPlan(mssqlPlan, "check <script>", format, { redact: true, notes: [] });
  assert(!exported.contents.includes(canary), `${format} export leaked literal`);
  if (format === "html") assert(!exported.contents.includes("<script>"), "HTML export did not escape title");
}

for (const badCall of [
  () => parser.parsePlan(""),
  () => parser.parsePlan(pgJson, "mysql"),
  () => parser.parsePlan(`<!DOCTYPE ShowPlanXML [<!ENTITY x "y">]>${mssqlXml}`, "mssql"),
]) {
  assert.throws(badCall, (err) => err instanceof parser.ParserError && err.error.code === "invalid_input");
}

console.log(
  JSON.stringify({
    status: "PASS",
    version: manifest.version,
    models: modelFiles.length,
    wasmGzipBytes: report.artifacts["native/culprit_wasm_bg.wasm"].gzip_bytes,
  }),
);
