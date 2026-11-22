#!/usr/bin/env node
// Actual packaged browser runtime, with existing file fixtures and no app/dev-server/native connection.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { readFile, realpath, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "../app/node_modules/playwright/index.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packageRoot = path.join(root, "packages/culprit-wasm");
const fixtureRoot = path.join(root, "crates/culprit-engine/tests/plans");
const cases = [
  ["mssql-actual.sqlplan", "mssql"],
  ["pg-seqscan.json", "postgres"],
  ["pg-analyze.txt", "postgres"],
  ["mysql-v2.json", "mysql"],
  ["mysql-analyze.txt", "mysql"],
  ["mariadb-index.json", "mysql"],
  ["yugabyte-index.json", "yugabyte"],
  ["sqlite-index.txt", "sqlite"],
];
const fixtures = await Promise.all(cases.map(async ([name, engine]) => {
  const text = await readFile(path.join(fixtureRoot, name), "utf8");
  return { name, engine, text, sha256: createHash("sha256").update(text).digest("hex") };
}));
const deadlock = await readFile(path.join(root, "crates/culprit-wasm/tests/data/postgres-deadlock.log"), "utf8");
const report = JSON.parse(await readFile(path.join(packageRoot, "pkg/build-report.json"), "utf8"));
assert.equal(report.stack_policy?.linear_stack_bytes, 32 * 1024 * 1024, "use the explicit packaged XML stack policy");
const canonicalPackageRoot = await realpath(packageRoot);
const contentTypes = { ".js": "text/javascript", ".wasm": "application/wasm", ".json": "application/json" };
const server = createServer(async (request, response) => {
  response.setHeader("Content-Security-Policy", "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'");
  try {
    const pathname = decodeURIComponent(new URL(request.url, "http://127.0.0.1").pathname);
    if (pathname === "/") {
      response.writeHead(200, { "Content-Type": "text/html" });
      response.end("<!doctype html><meta charset=utf-8><title>Culprit WASM runtime smoke</title>");
      return;
    }
    if (!pathname.startsWith("/pkg/")) throw new Error("outside packaged output");
    const file = await realpath(path.join(packageRoot, pathname.slice(1)));
    if (!file.startsWith(`${canonicalPackageRoot}${path.sep}`) || !(await stat(file)).isFile()) throw new Error("outside package");
    response.writeHead(200, { "Content-Type": contentTypes[path.extname(file)] ?? "application/octet-stream" });
    response.end(await readFile(file));
  } catch {
    response.writeHead(404); response.end();
  }
});
let browser;
let failure;
let completed;
let listening = false;
try {
  await new Promise((resolve, reject) => {
    server.once("error", reject); server.listen(0, "127.0.0.1", resolve);
  });
  listening = true;
  const origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch();
  const page = await browser.newPage();
  const unexpected = [];
  const assets = [];
  await page.route("**/*", async (route) => {
    const request = route.request();
    if (new URL(request.url()).origin !== origin) { unexpected.push(request.url()); await route.abort(); }
    else await route.continue();
  });
  page.on("response", (response) => assets.push({ url: response.url(), status: response.status(), contentType: response.headers()["content-type"] }));
  await page.goto(origin);
  const actual = await page.evaluate(async ({ fixtures, deadlock, stackBytes }) => {
    const parser = await import("/pkg/index.js");
    const expectedExports = ["ParserError", "diffPlans", "exportPlan", "init", "parseDeadlock", "parsePlan"];
    if (JSON.stringify(Object.keys(parser).sort()) !== JSON.stringify(expectedExports)) {
      throw new Error("Standalone package exposed an unexpected runtime capability");
    }
    await parser.init();
    const ensure = (condition, message) => { if (!condition) throw new Error(message); };
    const binding = await import("/pkg/native/culprit_wasm.js");
    const instance = await binding.default();
    const initialMemoryBytes = instance.memory.buffer.byteLength;
    ensure(initialMemoryBytes >= stackBytes, "linked module lacks the packaged stack reserve");
    const invalid = (action, reason) => {
      try { action(); } catch (error) {
        ensure(error instanceof parser.ParserError, `${reason}: trap or transport exception instead of Core error`);
        ensure(error.error.code === "invalid_input" && typeof error.error.message === "string", `${reason}: Core error contract lost`);
        return error.error;
      }
      throw new Error(`${reason}: invalid input unexpectedly succeeded`);
    };
    const plans = fixtures.map((fixture) => {
      const plan = parser.parsePlan(fixture.text, fixture.engine);
      ensure(plan.engine === fixture.engine, `wrong engine: ${fixture.name}`);
      ensure(plan.root && typeof plan.root.id === "number", `missing native tree: ${fixture.name}`);
      ensure(plan.raw !== null, `native original lost: ${fixture.name}`);
      return plan;
    });
    // These pin the public Core ceilings: changing them requires reviewing the packaged stack acceptance.
    const maxXmlDepth = 1024;
    const maxPlanDepth = 56;
    const nativeCanary = "wasm-native-private-literal";
    const showplan = (operator) => `<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementType="SELECT" StatementText="SELECT '${nativeCanary}' AS token" StatementSubTreeCost="1" StatementEstRows="1"><QueryPlan>${operator}<ParameterList><ColumnReference Column="@token" ParameterDataType="nvarchar(30)" ParameterCompiledValue="N'${nativeCanary}'"/></ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>`;
    const node = (id, operator, inner) => `<RelOp NodeId="${id}" PhysicalOp="${operator}" LogicalOp="${operator}" EstimateRows="1" EstimatedTotalSubtreeCost="1">${inner}</RelOp>`;
    const deepDom = (depth) => showplan(node(0, "Constant Scan", `<ConstantScan/><InternalInfo>${"<m>".repeat(depth - 8)}${"</m>".repeat(depth - 8)}</InternalInfo>`));
    const nearDom = parser.parsePlan(deepDom(maxXmlDepth), "mssql");
    ensure(nearDom.root.id === 0 && nearDom.root.children.length === 0, "near-limit whole XML DOM did not produce its single operator");
    // Shared native redaction adds a wrapper and may fail closed at the exact DOM ceiling; it must not leak.
    ensure(!parser.exportPlan(nearDom, "portable", "native", { redact: true, notes: [] }).contents.includes(nativeCanary), "near-limit native export leaked its literal");
    const overDom = invalid(() => parser.parsePlan(deepDom(maxXmlDepth + 1), "mssql"), "XML depth");
    ensure(overDom.message.includes("nests elements more than"), "over-depth XML did not reach the iterative Core refusal");
    const dtd = invalid(() => parser.parsePlan(`<!DOCTYPE ShowPlanXML [<!ENTITY private "must-not-expand">]>${deepDom(9)}`, "mssql"), "DTD");
    ensure(dtd.message.includes("DTD"), "DTD did not reach the existing forbidden-DTD owner");
    invalid(() => parser.parsePlan('[{"Plan":{"Node Type":"Seq Scan",}}]', "postgres"), "malformed JSON");
    const operators = (depth) => {
      let operator = node(depth - 1, "Constant Scan", "<ConstantScan/>");
      for (let id = depth - 2; id >= 0; id--) operator = node(id, "Compute Scalar", `<ComputeScalar>${operator}</ComputeScalar>`);
      return showplan(operator);
    };
    let current = parser.parsePlan(operators(maxPlanDepth), "mssql").root;
    let observedDepth = 1;
    while (current.children.length) { ensure(current.children.length === 1, "controlled unary tree changed"); current = current.children[0]; observedDepth++; }
    ensure(observedDepth === maxPlanDepth, "near-limit operator tree was silently truncated");
    const overPlan = invalid(() => parser.parsePlan(operators(maxPlanDepth + 1), "mssql"), "operator depth");
    ensure(overPlan.message.includes("steps more than"), "operator depth did not reach the Core model refusal");
    const privateNative = parser.parsePlan(deepDom(9), "mssql");
    ensure(privateNative.raw.includes(nativeCanary), "native privacy positive control lost original literal");
    const visibleNative = parser.exportPlan(privateNative, "portable", "native", { redact: false, notes: [] });
    ensure(visibleNative.contents.includes(nativeCanary), "explicit unredacted native positive control failed");
    for (const format of ["markdown", "html", "native"]) {
      const hidden = parser.exportPlan(privateNative, "portable <script>", format, { redact: true, notes: [] });
      ensure(!hidden.contents.includes(nativeCanary), `${format}: native literal redaction failed`);
      if (format === "native") ensure(hidden.contents.includes("ShowPlanXML"), "native privacy was satisfied only by discarding the document");
      if (format === "html") ensure(!hidden.contents.includes("<script>"), "native HTML title was not escaped");
    }
    const diff = parser.diffPlans(plans[0], "before", plans[0], "after");
    ensure(diff.same_shape && diff.changes.length === 0, "identical original comparison changed");
    const observed = parser.parseDeadlock(deadlock);
    ensure(observed.length === 1 && observed[0].processes.length === 2, "deadlock observation vector lost");
    ensure(observed[0].at === "2026-10-11T13:48:28Z", "explicit native UTC clock changed");
    ensure(observed[0].source === "pasted", "pasted source changed");
    const privatePlan = structuredClone(plans[0]);
    privatePlan.statement = "SELECT * FROM orders WHERE token='wasm-private-literal'";
    privatePlan.raw = null;
    const options = { redact: true, notes: [] };
    const markdown = parser.exportPlan(privatePlan, "portable <script>", "markdown", options);
    const html = parser.exportPlan(privatePlan, "portable <script>", "html", options);
    for (const file of [markdown, html]) ensure(!file.contents.includes("wasm-private-literal"), "export redaction lost");
    ensure(!html.contents.includes("<script>"), "HTML title was not escaped");
    ensure(markdown.name.endsWith(".md") && html.name.endsWith(".html"), "file contract changed");
    const native = parser.exportPlan(plans[0], "portable", "native", options);
    ensure(native.name.endsWith(".sqlplan") && native.contents.includes("ShowPlanXML"), "native plan export lost");
    let refused = 4;
    for (const action of [
      () => parser.parsePlan(fixtures[1].text, "mysql"),
      () => parser.parsePlan(""),
      () => parser.parseDeadlock("no native deadlock here"),
      () => parser.exportPlan(privatePlan, "portable", "native", options),
    ]) { invalid(action, "existing typed refusal"); refused++; }
    ensure(parser.parsePlan(fixtures[1].text, "postgres").engine === "postgres", "module was corrupted after controlled refusals");
    return { parsedFormats: plans.length, engines: [...new Set(plans.map((plan) => plan.engine))], sameShape: diff.same_shape, deadlocks: observed.length, exportFormats: [markdown.format, html.format, native.format], refused, safety: { xmlDepth: maxXmlDepth, planDepth: observedDepth, dtdRefused: true, malformedJsonRefused: true, nativePrivacy: true, initialMemoryBytes } };
  }, { fixtures, deadlock, stackBytes: report.stack_policy.linear_stack_bytes });
  assert.equal(actual.parsedFormats, cases.length);
  assert.equal(actual.refused, 8);
  assert.deepEqual(unexpected, []);
  assert(assets.some((asset) => asset.url.endsWith(".wasm") && asset.status === 200 && asset.contentType === "application/wasm"));
  assert(assets.filter((asset) => asset.url.startsWith(`${origin}/pkg/`)).every((asset) => asset.status === 200));
  completed = { result: "PASS", actual, fixtures: fixtures.map(({ text, ...metadata }) => metadata), artifacts: report.artifacts, requests: assets };
} catch (error) {
  failure = error;
} finally {
  const cleanup = await Promise.allSettled([
    browser ? browser.close() : Promise.resolve(),
    listening ? new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())) : Promise.resolve(),
  ]);
  const errors = cleanup.filter((result) => result.status === "rejected").map((result) => result.reason);
  if (errors.length) failure = new AggregateError(failure ? [failure, ...errors] : errors, "Owned browser/server cleanup failed");
}
if (failure) throw failure;
console.log(JSON.stringify(completed, null, 2));
