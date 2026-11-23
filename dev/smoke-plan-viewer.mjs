#!/usr/bin/env node
// Packaged browser runtime verification (and optional 2x Retina dark/light preview renderer) for @culpritsql/plan-viewer.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "../app/node_modules/playwright/index.mjs";

const screenshotFlagIndex = process.argv.indexOf("--screenshots");
const screenshotDir = screenshotFlagIndex >= 0 ? path.resolve(process.argv[screenshotFlagIndex + 1]) : null;

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const viewerPkgRoot = path.join(root, "packages/culprit-plan-viewer/pkg");
const wasmPkgRoot = path.join(root, "packages/culprit-wasm/pkg");
const fixtureRoot = path.join(root, "crates/culprit-engine/tests/plans");

const mssqlXml = await readFile(path.join(fixtureRoot, "mssql-actual.sqlplan"), "utf8");
const pgBeforeJson = await readFile(path.join(fixtureRoot, "pg-seqscan.json"), "utf8");
const pgAfterTxt = await readFile(path.join(fixtureRoot, "pg-analyze.txt"), "utf8");
const pgDeadlockLog = await readFile(path.join(root, "crates/culprit-wasm/tests/data/postgres-deadlock.log"), "utf8");

// Rich showcase fixtures for crisp visual documentation screenshots when --screenshots is passed.
const showcaseBeforeJson = JSON.stringify([
  {
    Plan: {
      "Node Type": "Limit",
      "Startup Cost": 48210.4,
      "Total Cost": 48215.9,
      "Plan Rows": 50,
      "Plan Width": 96,
      "Actual Startup Time": 318.4,
      "Actual Total Time": 319.2,
      "Actual Rows": 50,
      "Actual Loops": 1,
      Plans: [
        {
          "Node Type": "Sort",
          "Sort Key": ["o.created_at DESC"],
          "Sort Method": "external merge",
          "Sort Space Used": 18432,
          "Sort Space Type": "Disk",
          "Startup Cost": 48210.4,
          "Total Cost": 48285.1,
          "Plan Rows": 29880,
          "Plan Width": 96,
          "Actual Startup Time": 318.1,
          "Actual Total Time": 318.9,
          "Actual Rows": 50,
          "Actual Loops": 1,
          Plans: [
            {
              "Node Type": "Hash Join",
              "Join Type": "Inner",
              "Hash Cond": "(o.customer_id = c.id)",
              "Startup Cost": 1420.0,
              "Total Cost": 44890.0,
              "Plan Rows": 29880,
              "Plan Width": 96,
              "Actual Startup Time": 24.6,
              "Actual Total Time": 274.5,
              "Actual Rows": 41280,
              "Actual Loops": 1,
              Plans: [
                {
                  "Node Type": "Seq Scan",
                  "Relation Name": "orders",
                  Schema: "public",
                  Alias: "o",
                  Filter: "((status = 'paid'::text) AND (created_at >= '2026-10-01'::timestamptz))",
                  "Rows Removed by Filter": 1458720,
                  "Startup Cost": 0.0,
                  "Total Cost": 39840.0,
                  "Plan Rows": 3100,
                  "Plan Width": 64,
                  "Actual Startup Time": 0.08,
                  "Actual Total Time": 218.4,
                  "Actual Rows": 41280,
                  "Actual Loops": 1,
                  "Shared Hit Blocks": 4120,
                  "Shared Read Blocks": 18420,
                },
                {
                  "Node Type": "Hash",
                  "Startup Cost": 980.0,
                  "Total Cost": 980.0,
                  "Plan Rows": 35200,
                  "Plan Width": 32,
                  "Actual Startup Time": 24.2,
                  "Actual Total Time": 24.2,
                  "Actual Rows": 35200,
                  "Actual Loops": 1,
                  Plans: [
                    {
                      "Node Type": "Seq Scan",
                      "Relation Name": "customers",
                      Schema: "public",
                      Alias: "c",
                      "Startup Cost": 0.0,
                      "Total Cost": 980.0,
                      "Plan Rows": 35200,
                      "Plan Width": 32,
                      "Actual Startup Time": 0.03,
                      "Actual Total Time": 11.8,
                      "Actual Rows": 35200,
                      "Actual Loops": 1,
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    "Planning Time": 0.84,
    "Execution Time": 321.6,
  },
]);

const showcaseAfterJson = JSON.stringify([
  {
    Plan: {
      "Node Type": "Limit",
      "Startup Cost": 0.86,
      "Total Cost": 84.2,
      "Plan Rows": 50,
      "Plan Width": 96,
      "Actual Startup Time": 0.19,
      "Actual Total Time": 1.84,
      "Actual Rows": 50,
      "Actual Loops": 1,
      Plans: [
        {
          "Node Type": "Nested Loop",
          "Join Type": "Inner",
          "Startup Cost": 0.86,
          "Total Cost": 3420.0,
          "Plan Rows": 2050,
          "Plan Width": 96,
          "Actual Startup Time": 0.18,
          "Actual Total Time": 1.79,
          "Actual Rows": 50,
          "Actual Loops": 1,
          Plans: [
            {
              "Node Type": "Index Scan",
              "Scan Direction": "Backward",
              "Index Name": "ix_orders_status_created_at",
              "Relation Name": "orders",
              Schema: "public",
              Alias: "o",
              "Index Cond": "((status = 'paid'::text) AND (created_at >= '2026-10-01'::timestamptz))",
              "Startup Cost": 0.43,
              "Total Cost": 1240.0,
              "Plan Rows": 41000,
              "Plan Width": 64,
              "Actual Startup Time": 0.11,
              "Actual Total Time": 0.62,
              "Actual Rows": 50,
              "Actual Loops": 1,
              "Shared Hit Blocks": 54,
            },
            {
              "Node Type": "Index Scan",
              "Scan Direction": "Forward",
              "Index Name": "customers_pkey",
              "Relation Name": "customers",
              Schema: "public",
              Alias: "c",
              "Index Cond": "(id = o.customer_id)",
              "Startup Cost": 0.43,
              "Total Cost": 0.53,
              "Plan Rows": 1,
              "Plan Width": 32,
              "Actual Startup Time": 0.02,
              "Actual Total Time": 0.02,
              "Actual Rows": 1,
              "Actual Loops": 50,
              "Shared Hit Blocks": 200,
            },
          ],
        },
      ],
    },
    "Planning Time": 0.42,
    "Execution Time": 1.92,
  },
]);

const showcaseDeadlockXml = `<deadlock>
  <victim-list>
    <victimProcess id="process201"/>
  </victim-list>
  <process-list>
    <process id="process201" spid="74" clientapp="checkout-worker" loginname="app_checkout" hostname="worker-03" isolationlevel="read committed (2)" waitresource="KEY: 6:72057594041204736 (81944f3284a0)" waittime="1840" status="suspended">
      <executionStack>
        <frame procname="adhoc" sqlhandle="0x02">UPDATE dbo.inventory SET reserved = reserved + @qty WHERE sku_id = @sku;</frame>
      </executionStack>
      <inputbuf>BEGIN TRAN; UPDATE dbo.orders SET status = 'reserved' WHERE id = 91824; UPDATE dbo.inventory SET reserved = reserved + 2 WHERE sku_id = 4410;</inputbuf>
    </process>
    <process id="process202" spid="112" clientapp="fulfillment-sync" loginname="app_fulfillment" hostname="sync-01" isolationlevel="read committed (2)" waitresource="KEY: 6:72057594039894016 (44019a01b2c1)" waittime="1825" status="suspended">
      <executionStack>
        <frame procname="adhoc" sqlhandle="0x03">UPDATE dbo.orders SET shipped_at = SYSUTCDATETIME() WHERE id = @order_id;</frame>
      </executionStack>
      <inputbuf>BEGIN TRAN; UPDATE dbo.inventory SET on_hand = on_hand - 2 WHERE sku_id = 4410; UPDATE dbo.orders SET shipped_at = SYSUTCDATETIME() WHERE id = 91824;</inputbuf>
    </process>
  </process-list>
  <resource-list>
    <keylock hobtid="72057594039894016" dbid="6" objectname="shop.dbo.orders" indexname="PK_orders" id="lock_orders" mode="X">
      <owner-list><owner id="process201" mode="X"/></owner-list>
      <waiter-list><waiter id="process202" mode="U" requestType="wait"/></waiter-list>
    </keylock>
    <keylock hobtid="72057594041204736" dbid="6" objectname="shop.dbo.inventory" indexname="PK_inventory" id="lock_inventory" mode="X">
      <owner-list><owner id="process202" mode="X"/></owner-list>
      <waiter-list><waiter id="process201" mode="X" requestType="wait"/></waiter-list>
    </keylock>
  </resource-list>
</deadlock>`;

const report = JSON.parse(await readFile(path.join(viewerPkgRoot, "build-report.json"), "utf8"));
assert.equal(report.target, "es2022-react-esm");
assert.equal(report.native_or_database_work, false);

const tempVendor = await mkdtemp(path.join(os.tmpdir(), "culprit-pv-vendor-"));
const canonicalViewerRoot = await realpath(viewerPkgRoot);
const canonicalWasmRoot = await realpath(wasmPkgRoot);
const canonicalVendorRoot = await realpath(tempVendor);

// Build browser ESM shims for peer/icon dependencies with --splitting so they share one React instance; /pkg/* stays raw & unbundled.
async function buildVendorShims() {
  const nm = path.join(root, "app/node_modules");
  const srcDir = path.join(tempVendor, "_src");
  await mkdir(srcDir, { recursive: true });
  execFileSync("ln", ["-s", nm, path.join(srcDir, "node_modules")]);
  const files = {
    "react.js": `import R from "react"; export default R; export const { Children, Component, Fragment, Profiler, PureComponent, StrictMode, Suspense, act, cache, cloneElement, createContext, createElement, createRef, forwardRef, isValidElement, lazy, memo, startTransition, use, useActionState, useCallback, useContext, useDebugValue, useDeferredValue, useEffect, useId, useImperativeHandle, useInsertionEffect, useLayoutEffect, useMemo, useOptimistic, useReducer, useRef, useState, useSyncExternalStore, useTransition, version } = R;\n`,
    "jsx-runtime.js": `import J from "react/jsx-runtime"; export default J; export const { Fragment, jsx, jsxs } = J;\n`,
    "react-dom-client.js": `import D from "react-dom/client"; export const { createRoot, hydrateRoot } = D;\n`,
    "lucide-react.js": `export * from "lucide-react";\n`,
  };
  const entryPaths = [];
  for (const [name, code] of Object.entries(files)) {
    const p = path.join(srcDir, name);
    await writeFile(p, code);
    entryPaths.push(p);
  }
  execFileSync("bun", ["build", ...entryPaths, "--splitting", "--format=esm", "--outdir", tempVendor], {
    cwd: srcDir,
    stdio: "pipe",
  });
}

const contentTypes = {
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
};

const htmlPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>@culpritsql/plan-viewer smoke</title>
  <script type="importmap">
  {
    "imports": {
      "react": "/vendor/react.js",
      "react/jsx-runtime": "/vendor/jsx-runtime.js",
      "react-dom/client": "/vendor/react-dom-client.js",
      "lucide-react": "/vendor/lucide-react.js"
    }
  }
  </script>
</head>
<body>
  <button id="host-btn" type="button">Host button</button>
  <div id="mount"></div>
</body>
</html>`;

async function serveCheckedFile(baseDir, canonicalBase, relPath, response) {
  const resolved = await realpath(path.join(baseDir, relPath));
  if (!resolved.startsWith(`${canonicalBase}${path.sep}`) || !(await stat(resolved)).isFile()) {
    throw new Error("outside root");
  }
  response.writeHead(200, { "Content-Type": contentTypes[path.extname(resolved)] ?? "application/octet-stream" });
  response.end(await readFile(resolved));
}

const server = createServer(async (request, response) => {
  response.setHeader(
    "Content-Security-Policy",
    "default-src 'none'; script-src 'self' 'wasm-unsafe-eval' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'",
  );
  try {
    const pathname = decodeURIComponent(new URL(request.url, "http://127.0.0.1").pathname);
    if (pathname === "/") {
      response.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      response.end(htmlPage);
      return;
    }
    if (pathname.startsWith("/pkg/")) {
      await serveCheckedFile(viewerPkgRoot, canonicalViewerRoot, pathname.slice(5), response);
      return;
    }
    if (pathname.startsWith("/wasm/")) {
      await serveCheckedFile(wasmPkgRoot, canonicalWasmRoot, pathname.slice(6), response);
      return;
    }
    if (pathname.startsWith("/vendor/")) {
      await serveCheckedFile(tempVendor, canonicalVendorRoot, pathname.slice(8), response);
      return;
    }
    response.writeHead(404);
    response.end();
  } catch {
    response.writeHead(404);
    response.end();
  }
});

let browser;
let failure;
let completed;
let listening = false;

try {
  await buildVendorShims();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  listening = true;
  const origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1180, height: 900 }, deviceScaleFactor: 2 });
  const page = await context.newPage();

  const unexpected = [];
  const pageErrors = [];
  await page.route("**/*", async (route) => {
    const url = route.request().url();
    if (!url.startsWith("data:") && new URL(url).origin !== origin) {
      unexpected.push(url);
      await route.abort();
    } else {
      await route.continue();
    }
  });
  page.on("pageerror", (err) => pageErrors.push(String(err)));

  await page.goto(origin);

  const smokeResult = await page.evaluate(
    async ({ mssqlXml, pgBeforeJson, pgAfterTxt, pgDeadlockLog }) => {
      const ensure = (cond, msg) => {
        if (!cond) throw new Error(msg);
      };

      // Capture host baseline styles BEFORE loading @culpritsql/plan-viewer/styles.css
      const beforeBodyOverflow = getComputedStyle(document.body).overflow;
      const beforeBodySelect = getComputedStyle(document.body).userSelect;
      const hostBtn = document.getElementById("host-btn");
      const beforeBtnBorder = getComputedStyle(hostBtn).borderTopStyle;

      await new Promise((resolve, reject) => {
        const link = document.createElement("link");
        link.rel = "stylesheet";
        link.href = "/pkg/styles.css";
        link.onload = resolve;
        link.onerror = reject;
        document.head.appendChild(link);
      });

      // Host isolation check: styles.css must never alter body overflow/user-select or external buttons
      ensure(getComputedStyle(document.body).overflow === beforeBodyOverflow, "styles.css mutated body overflow");
      ensure(getComputedStyle(document.body).userSelect === beforeBodySelect, "styles.css mutated body user-select");
      ensure(getComputedStyle(hostBtn).borderTopStyle === beforeBtnBorder, "styles.css mutated host button border");

      const parser = await import("/wasm/index.js");
      await parser.init();
      const viewer = await import("/pkg/index.js");
      const React = (await import("react")).default;
      const { flushSync } = await import("/vendor/react-dom-client.js").catch(() => ({}));
      const { createRoot } = await import("react-dom/client");

      const mount = document.getElementById("mount");
      const root = createRoot(mount);
      const renderSync = async (el) => {
        if (flushSync) flushSync(() => root.render(el));
        else root.render(el);
        await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
      };

      // 1. PlanViewer smoke + interactive tab switching
      const mssqlPlan = parser.parsePlan(mssqlXml, "mssql");
      await renderSync(
        React.createElement(viewer.PlanViewer, {
          plan: mssqlPlan,
          title: "Order Lookup",
          theme: "dark",
        }),
      );
      ensure(mount.querySelector(".culprit-pv.pv-root[data-theme='dark']"), "PlanViewer root missing dark theme");
      const diagramCards = mount.querySelectorAll(".plan-card").length;
      ensure(diagramCards >= 1, "PlanViewer diagram rendered zero operator cards");
      ensure(mount.querySelector(".nd"), "PlanViewer operator details panel missing");

      const tabs = [...mount.querySelectorAll(".look-toggle [role='tab']")];
      ensure(tabs.length === 5, `Expected 5 plan tabs (Diagram/Tree/Operations/Tables/Raw), got ${tabs.length}`);

      // Verify diagram search highlight and minimap toggle
      const searchInput = mount.querySelector(".plan-search input");
      ensure(searchInput, "Diagram search input missing");
      const minimapBtn = mount.querySelector(".zoom button:last-child");
      ensure(minimapBtn, "Minimap toggle button missing");
      minimapBtn.click();
      await new Promise((r) => requestAnimationFrame(r));

      tabs[1].click();
      await new Promise((r) => requestAnimationFrame(r));
      const treeRows = mount.querySelectorAll(".pnode").length;
      ensure(treeRows >= 1, "Tree view rendered zero .pnode rows");

      tabs[2].click();
      await new Promise((r) => requestAnimationFrame(r));
      const opRows = mount.querySelectorAll(".ops-table tbody tr").length;
      ensure(opRows >= 1, "Operations view rendered zero table rows");

      tabs[3].click();
      await new Promise((r) => requestAnimationFrame(r));
      const objectRows = mount.querySelectorAll(".plan-objects tbody tr").length;
      ensure(objectRows >= 1, "Tables view rendered zero table rollup rows");

      tabs[4].click();
      await new Promise((r) => requestAnimationFrame(r));
      const rawCode = mount.querySelector(".plan-raw-code");
      ensure(rawCode && rawCode.textContent.includes("ShowPlanXML"), "Raw view did not render native plan XML");

      // 1b. Pure PlanCanvas granular smoke (zero insights, zero header, zero NodeDetails drawer)
      await renderSync(
        React.createElement(viewer.PlanCanvas, {
          plan: mssqlPlan,
          lens: "cost",
          showInsights: false,
          showMinimap: true,
          height: 420,
          theme: "dark",
        }),
      );
      ensure(mount.querySelectorAll(".plan-card").length >= 1, "PlanCanvas rendered zero operator cards");
      ensure(!mount.querySelector(".plan-head"), "PlanCanvas unexpectedly rendered .plan-head");
      ensure(!mount.querySelector(".nd"), "PlanCanvas unexpectedly rendered NodeDetails");
      ensure(!mount.querySelector(".pc-flag"), "PlanCanvas with showInsights=false rendered insight flags");
      ensure(mount.querySelector(".diagram-minimap"), "PlanCanvas minimap did not render");

      // 2. PlanDiffViewer smoke
      const pgBefore = parser.parsePlan(pgBeforeJson, "postgres");
      const pgAfter = parser.parsePlan(pgAfterTxt, "postgres");
      const diff = parser.diffPlans(pgBefore, "Before", pgAfter, "After");
      await renderSync(
        React.createElement(viewer.PlanDiffViewer, {
          a: pgBefore,
          b: pgAfter,
          diff,
          nameA: "Before",
          nameB: "After",
        }),
      );
      ensure(mount.querySelector(".cmp-verdict"), "PlanDiffViewer verdict missing");
      ensure(mount.querySelectorAll(".compare-side").length === 2, "PlanDiffViewer must render 2 compare sides");
      const canvasBar = mount.querySelector(".cmp-canvas-bar");
      const compareSides = mount.querySelector(".compare-sides");
      ensure(canvasBar && compareSides, "PlanDiffViewer .cmp-canvas-bar or .compare-sides missing");
      ensure(
        canvasBar.getBoundingClientRect().bottom <= compareSides.getBoundingClientRect().top + 2,
        "Diagram/Tree toggle bar (.cmp-canvas-bar) must sit directly above .compare-sides",
      );

      // Switch to Tree view and verify zero text box overlap across wide (1100px), medium (740px), and mobile (360px) widths
      const treeBtn = [...canvasBar.querySelectorAll(".seg button")][1];
      ensure(treeBtn, "Tree toggle button missing in .cmp-canvas-bar");
      treeBtn.click();
      await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));

      for (const testWidth of [1100, 740, 360]) {
        mount.style.width = `${testWidth}px`;
        await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
        const pnodes = [...mount.querySelectorAll(".pnode")];
        ensure(pnodes.length >= 2, `Expected .pnode rows at ${testWidth}px`);
        for (const row of pnodes) {
          const labelRect = row.querySelector(".label")?.getBoundingClientRect();
          const rowsEl = row.querySelector(".rows");
          const costRect = row.querySelector(".cost")?.getBoundingClientRect();
          ensure(labelRect && costRect, "Missing .label or .cost rect");
          ensure(labelRect.right <= costRect.left + 1, `.pnode .label overlapped .cost at ${testWidth}px`);
          if (rowsEl && getComputedStyle(rowsEl).display !== "none" && rowsEl.textContent.trim().length > 0) {
            const rowsRect = rowsEl.getBoundingClientRect();
            const sameLine = Math.abs(rowsRect.top - labelRect.top) < 8;
            if (sameLine) {
              ensure(labelRect.right <= rowsRect.left + 1, `.pnode .label overlapped .rows at ${testWidth}px`);
              ensure(rowsRect.right <= costRect.left + 1, `.pnode .rows overlapped .cost at ${testWidth}px`);
            }
          }
        }
      }
      mount.style.width = "";

      const propTab = [...mount.querySelectorAll(".cmp-tabs [role='tab']")][2];
      propTab.click();
      await new Promise((r) => requestAnimationFrame(r));
      ensure(mount.querySelector(".cmp-empty, .cmp-props"), "PlanDiffViewer props tab did not render");

      // 3. DeadlockViewer smoke + XSS safety check
      const deadlocks = parser.parseDeadlock(pgDeadlockLog);
      ensure(deadlocks.length === 1, "Expected 1 parsed deadlock");
      const xssPlan = structuredClone(mssqlPlan);
      xssPlan.statement = "SELECT '<script>window.__xss=1</script><img src=x onerror=window.__xss=1>'";
      xssPlan.root.object = "dbo.<script>window.__xss=1</script>";
      await renderSync(React.createElement(viewer.PlanViewer, { plan: xssPlan }));
      ensure(window.__xss === undefined, "XSS payload executed in PlanViewer");
      ensure(mount.querySelectorAll("script, img").length === 0, "Raw HTML tags injected into DOM");

      await renderSync(React.createElement(viewer.DeadlockViewer, { deadlock: deadlocks[0] }));
      ensure(mount.querySelector(".dl-graph svg"), "DeadlockViewer SVG graph missing");
      ensure(mount.querySelectorAll(".dl-proc-card").length === 2, "DeadlockViewer process cards missing");

      root.unmount();
      return {
        diagramCards,
        treeRows,
        opRows,
        diffChanges: diff.changes.length,
        deadlockProcs: deadlocks[0].processes.length,
      };
    },
    { mssqlXml, pgBeforeJson, pgAfterTxt, pgDeadlockLog },
  );

  if (screenshotDir) {
    await mkdir(screenshotDir, { recursive: true });

    // Render crisp dark & light hero banners (1280x288 @ 2x) matching Culprit's Option 01 "Spotlight Prism" identity
    for (const theme of ["dark", "light"]) {
      const heroPage = await context.newPage();
      await heroPage.setViewportSize({ width: 1280, height: 288 });
      const isDark = theme === "dark";
      const bg = isDark ? "#121211" : "#fafaf9";
      const card = isDark ? "#191917" : "#ffffff";
      const border = isDark ? "#2a2a27" : "#e5e4df";
      const edge = isDark ? "#3a3935" : "#d0cec7";
      const ink = isDark ? "#f2f1ed" : "#181715";
      const sub = isDark ? "#9e9b94" : "#64615b";
      const gridDot = isDark ? "rgba(255,255,255,0.06)" : "rgba(24,23,21,0.06)";
      const pillBg = isDark ? "#201f1d" : "#f2f1ed";
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
        .left { display: flex; align-items: center; gap: 26px; max-width: 720px; }
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
        p { margin-top: 9px; font-size: 16px; color: ${sub}; max-width: 530px; line-height: 1.45; }
        .mini-dag {
          position: relative;
          width: 356px; height: 176px; flex: none;
        }
        .mini-edges {
          position: absolute; inset: 0; width: 356px; height: 176px; pointer-events: none;
        }
        .node {
          position: absolute;
          padding: 9px 12px 10px; border-radius: 10px;
          background: ${card}; border: 1px solid ${border};
          box-shadow: 0 4px 16px rgba(0,0,0,${isDark ? "0.32" : "0.06"});
        }
        .node.top { width: 196px; left: 80px; top: 8px; }
        .node.hot { width: 170px; left: 0; top: 108px; border-color: #ea5f45; }
        .node.hot::after {
          content: ""; position: absolute; left: 0; bottom: 0; height: 3px; width: 78%;
          background: #ea5f45; border-radius: 0 2px 0 9px;
        }
        .node.right { width: 170px; left: 186px; top: 108px; }
        .ntop { display: flex; justify-content: space-between; font-size: 12.5px; font-weight: 600; }
        .nshare { color: #ea5f45; font-variant-numeric: tabular-nums; }
        .nsub { font-size: 11px; color: ${sub}; font-family: ui-monospace, "SF Mono", monospace; margin-top: 3px; }
        .badge {
          position: absolute; top: -7px; right: -7px;
          width: 17px; height: 17px; border-radius: 9px;
          background: #e0484e; color: #fff; font-size: 10px; font-weight: 700;
          display: grid; place-items: center;
          box-shadow: 0 0 0 2px ${bg};
        }
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
            <div class="eyebrow"><span class="dot"></span> Culprit SQL UI</div>
            <h1><span>@culpritsql/</span>plan-viewer</h1>
            <p>Interactive execution plan diagrams, structural diffs, and deadlock cycle graphs for React.</p>
          </div>
        </div>
        <div class="mini-dag">
          <svg class="mini-edges" viewBox="0 0 356 176" fill="none">
            <path d="M 85 108 C 85 84, 152 84, 152 62" stroke="#ea5f45" stroke-width="2.4" stroke-opacity="0.85"/>
            <path d="M 271 108 C 271 84, 204 84, 204 62" stroke="${edge}" stroke-width="1.6"/>
          </svg>
          <div class="node top">
            <div class="ntop"><span>Hash Join</span><span style="color:${sub}">18%</span></div>
            <div class="nsub">41.3K rows · 32 ms</div>
          </div>
          <div class="node hot">
            <span class="badge">2</span>
            <div class="ntop"><span>Seq Scan</span><span class="nshare">78%</span></div>
            <div class="nsub">orders · 218 ms</div>
          </div>
          <div class="node right">
            <div class="ntop"><span>Index Scan</span><span style="color:${sub}">4%</span></div>
            <div class="nsub">customers_pkey</div>
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

    // Capture real component screenshots in dark and light themes
    for (const theme of ["dark", "light"]) {
      const bg = theme === "dark" ? "#0d0d0c" : "#f2f1ee";
      await page.setViewportSize({ width: 1120, height: 900 });
      await page.evaluate(
        async ({ theme, bg, showcaseBeforeJson, showcaseAfterJson, showcaseDeadlockXml }) => {
          const parser = await import("/wasm/index.js");
          await parser.init();
          const viewer = await import("/pkg/index.js");
          const React = (await import("react")).default;
          const { createRoot } = await import("react-dom/client");

          document.getElementById("host-btn")?.remove();
          document.body.style.margin = "0";
          document.body.style.padding = "20px";
          document.body.style.background = bg;

          const mount = document.getElementById("mount");
          mount.innerHTML = "";
          const root = createRoot(mount);

          const beforePlan = parser.parsePlan(showcaseBeforeJson, "postgres");
          beforePlan.statement =
            "SELECT o.id, o.total_cents, c.email FROM public.orders o JOIN public.customers c ON c.id = o.customer_id WHERE o.status = 'paid' AND o.created_at >= '2026-10-01' ORDER BY o.created_at DESC LIMIT 50;";

          const afterPlan = parser.parsePlan(showcaseAfterJson, "postgres");
          afterPlan.statement = beforePlan.statement;

          window.__pvRender = async (mode) => {
            if (mode === "plan") {
              root.render(
                React.createElement(viewer.PlanViewer, {
                  plan: beforePlan,
                  title: "Recent Paid Orders",
                  theme,
                }),
              );
            } else if (mode === "diff") {
              const diff = parser.diffPlans(beforePlan, "Before index", afterPlan, "After ix_orders_status_created_at");
              root.render(
                React.createElement(viewer.PlanDiffViewer, {
                  a: beforePlan,
                  b: afterPlan,
                  diff,
                  nameA: "Before index",
                  nameB: "After ix_orders_status_created_at",
                  theme,
                }),
              );
            } else if (mode === "deadlock") {
              const dls = parser.parseDeadlock(showcaseDeadlockXml);
              root.render(
                React.createElement(viewer.DeadlockViewer, {
                  deadlock: dls[0],
                  theme,
                }),
              );
            }
            await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
          };
        },
        { theme, bg, showcaseBeforeJson, showcaseAfterJson, showcaseDeadlockXml },
      );

      for (const mode of ["plan", "diff", "deadlock"]) {
        await page.evaluate((m) => window.__pvRender(m), mode);
        if (mode === "plan") {
          const firstInsight = page.locator(".ins-row").first();
          if ((await firstInsight.count()) > 0) {
            await firstInsight.click();
          }
        }
        await page.waitForTimeout(80);
        const box = await page.locator("#mount > *").first().boundingBox();
        assert(box, `missing bounding box for ${mode}-${theme}`);
        await page.setViewportSize({ width: 1120, height: Math.ceil(box.height + 40) });
        await page.waitForTimeout(40);
        await page.screenshot({
          path: path.join(screenshotDir, `preview-${mode}-${theme}.png`),
          fullPage: true,
        });
      }
    }
  }

  assert.deepEqual(unexpected, [], "unexpected external network requests");
  assert.deepEqual(pageErrors, [], `browser page errors: ${pageErrors.join("; ")}`);
  completed = {
    result: "PASS",
    actual: smokeResult,
    screenshots: screenshotDir ? "generated" : "skipped",
  };
} catch (error) {
  failure = error;
} finally {
  await Promise.allSettled([
    browser ? browser.close() : Promise.resolve(),
    listening ? new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve()))) : Promise.resolve(),
    rm(tempVendor, { recursive: true, force: true }),
  ]);
}

if (failure) throw failure;
console.log(JSON.stringify(completed, null, 2));
