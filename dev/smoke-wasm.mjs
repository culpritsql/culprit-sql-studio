#!/usr/bin/env node
// Actual packaged browser runtime, with existing file fixtures and no app/dev-server/native connection.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, readFile, realpath, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "../app/node_modules/playwright/index.mjs";

const screenshotFlagIndex = process.argv.indexOf("--screenshots");
const screenshotDir = screenshotFlagIndex >= 0 ? path.resolve(process.argv[screenshotFlagIndex + 1]) : null;

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

  if (screenshotDir) {
    await mkdir(screenshotDir, { recursive: true });
    const heroContext = await browser.newContext({ viewport: { width: 1280, height: 288 }, deviceScaleFactor: 2 });
    try {
      for (const theme of ["dark", "light"]) {
        const heroPage = await heroContext.newPage();
        const isDark = theme === "dark";
        const bg = isDark ? "#121211" : "#fafaf9";
        const card = isDark ? "#191917" : "#ffffff";
        const cardHead = isDark ? "#1f1e1b" : "#f5f4f0";
        const border = isDark ? "#2a2a27" : "#e5e4df";
        const edge = isDark ? "#3a3935" : "#d0cec7";
        const ink = isDark ? "#f2f1ed" : "#181715";
        const sub = isDark ? "#9e9b94" : "#64615b";
        const gridDot = isDark ? "rgba(255,255,255,0.06)" : "rgba(24,23,21,0.06)";
        const pillBg = isDark ? "#201f1d" : "#f2f1ed";
        const amberInk = isDark ? "#ffcf33" : "#996b00";
        const greenInk = isDark ? "#4cbb84" : "#1d7f51";
        const coralInk = isDark ? "#ea5f45" : "#c9432a";
        const blueInk = isDark ? "#6cb6ff" : "#1f6feb";
        await heroPage.setContent(`<!doctype html>
        <html><head><meta charset="utf-8"><style>
          * { box-sizing: border-box; margin: 0; }
          body {
            width: 1280px; height: 288px; overflow: hidden;
            display: flex; align-items: center; justify-content: space-between;
            padding: 0 64px;
            background-color: ${bg};
            background-image:
              radial-gradient(circle at 16% 50%, rgba(255, 207, 51, ${isDark ? "0.11" : "0.15"}), transparent 44%),
              radial-gradient(${gridDot} 1px, transparent 1px);
            background-size: 100% 100%, 20px 20px;
            font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif;
            color: ${ink};
            border: 1px solid ${border};
            border-radius: 16px;
          }
          .left { display: flex; align-items: center; gap: 26px; max-width: 660px; }
          .mark {
            width: 82px; height: 82px; flex: none; border-radius: 22px;
            background: ${isDark ? "#1b1a18" : "#181715"};
            border: 1px solid ${isDark ? "#35332d" : "#2c2a26"};
            display: grid; place-items: center;
            box-shadow: 0 12px 32px rgba(0,0,0,${isDark ? "0.45" : "0.14"});
          }
          .eyebrow {
            display: inline-flex; align-items: center; gap: 8px;
            padding: 4px 11px; border-radius: 999px;
            background: ${pillBg}; border: 1px solid ${border};
            font-size: 12px; font-weight: 600; letter-spacing: 0.03em; color: ${sub};
            margin-bottom: 11px;
          }
          .dot { width: 7px; height: 7px; border-radius: 50%; background: #ffcf33; }
          h1 { font-size: 37px; font-weight: 700; letter-spacing: -0.025em; line-height: 1.12; }
          h1 span { color: ${sub}; font-weight: 500; }
          p { margin-top: 9px; font-size: 16px; color: ${sub}; max-width: 510px; line-height: 1.45; }
          .pipeline {
            position: relative;
            width: 466px; height: 188px; flex: none;
          }
          .pipe-edges {
            position: absolute; inset: 0; width: 466px; height: 188px; pointer-events: none;
          }
          .src {
            position: absolute; left: 0; width: 176px; height: 46px;
            padding: 7px 12px; border-radius: 9px;
            background: ${card}; border: 1px solid ${border};
            box-shadow: 0 4px 14px rgba(0,0,0,${isDark ? "0.28" : "0.05"});
            white-space: nowrap;
          }
          .src.s1 { top: 12px; }
          .src.s2 { top: 71px; border-color: ${isDark ? "rgba(255,207,51,0.45)" : "rgba(204,148,0,0.45)"}; }
          .src.s3 { top: 130px; }
          .sk { font-family: ui-monospace, "SF Mono", monospace; font-size: 11.5px; font-weight: 600; color: ${ink}; }
          .sv { font-size: 10.5px; color: ${sub}; margin-top: 2px; }
          .out {
            position: absolute; left: 224px; top: 16px; width: 242px; height: 156px;
            border-radius: 12px; overflow: hidden;
            background: ${card}; border: 1px solid ${border};
            box-shadow: 0 8px 24px rgba(0,0,0,${isDark ? "0.36" : "0.07"});
          }
          .out-head {
            display: flex; align-items: center; justify-content: space-between;
            padding: 8px 13px; background: ${cardHead}; border-bottom: 1px solid ${border};
            font-family: ui-monospace, "SF Mono", monospace; font-size: 11px; color: ${sub};
          }
          .tag {
            padding: 2px 7px; border-radius: 5px;
            background: ${isDark ? "rgba(255,207,51,0.14)" : "rgba(255,207,51,0.24)"};
            color: ${amberInk}; font-weight: 600; font-size: 10.5px;
          }
          .out-body {
            padding: 9px 13px;
            display: flex; flex-direction: column; gap: 7px;
            font-family: ui-monospace, "SF Mono", monospace; font-size: 11.5px;
          }
          .fn-row { display: flex; align-items: center; justify-content: space-between; }
          .fn { color: ${ink}; font-weight: 600; }
          .ret { font-weight: 600; font-variant-numeric: tabular-nums; }
        </style></head><body>
          <div class="left">
            <div class="mark">
              <svg width="46" height="46" viewBox="0 0 64 64" fill="none">
                <path d="M18 16L34 8L50 16V32L34 40L18 32V16Z" fill="#FFCF33" fill-opacity="0.18" stroke="#FFCF33" stroke-width="3" stroke-linejoin="round"/>
                <path d="M34 22L48 30V46L34 54L20 46V30L34 22Z" fill="#FFCF33"/>
                <path d="M34 22V54M34 38L48 30M34 38L20 30" stroke="#181715" stroke-width="2.6" stroke-linecap="round"/>
              </svg>
            </div>
            <div>
              <div class="eyebrow"><span class="dot"></span> Culprit SQL Wasm</div>
              <h1><span>@culpritsql/</span>plans</h1>
              <p>Portable WebAssembly execution plan parser, structural differ, deadlock parser, and exporter.</p>
            </div>
          </div>
          <div class="pipeline">
            <svg class="pipe-edges" viewBox="0 0 466 188" fill="none">
              <path d="M 176 35 C 202 35, 198 94, 224 94" stroke="${edge}" stroke-width="1.6"/>
              <path d="M 176 94 L 224 94" stroke="#ffcf33" stroke-width="2.2" stroke-opacity="0.9"/>
              <path d="M 176 153 C 202 153, 198 94, 224 94" stroke="${edge}" stroke-width="1.6"/>
            </svg>
            <div class="src s1">
              <div class="sk">.sqlplan XML</div>
              <div class="sv">SQL Server Showplan</div>
            </div>
            <div class="src s2">
              <div class="sk">EXPLAIN JSON / Text</div>
              <div class="sv">Postgres · MySQL · YB · SQLite</div>
            </div>
            <div class="src s3">
              <div class="sk">Deadlock Dumps</div>
              <div class="sv">XML · PG Log · InnoDB</div>
            </div>
            <div class="out">
              <div class="out-head">
                <span>wasm32 · 0 deps</span>
                <span class="tag">ts-rs</span>
              </div>
              <div class="out-body">
                <div class="fn-row"><span class="fn">parsePlan()</span><span class="ret" style="color:${blueInk}">Plan</span></div>
                <div class="fn-row"><span class="fn">diffPlans()</span><span class="ret" style="color:${greenInk}">167x faster</span></div>
                <div class="fn-row"><span class="fn">parseDeadlock()</span><span class="ret" style="color:${coralInk}">Deadlock[]</span></div>
                <div class="fn-row"><span class="fn">exportPlan()</span><span class="ret" style="color:${sub}">MD · HTML · XML</span></div>
              </div>
            </div>
          </div>
        </body></html>`);
        await heroPage.screenshot({
          path: path.join(screenshotDir, `hero-${theme}.png`),
          clip: { x: 0, y: 0, width: 1280, height: 288 },
          omitBackground: true,
        });
        await heroPage.close();
      }
    } finally {
      await heroContext.close();
    }
  }

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
