import { expect, test } from "@playwright/test";
import captured from "../../fixtures/clickhouse-demo.json" with { type: "json" };
import type { Snapshot } from "../src/gen/Snapshot";
import { fromRust } from "../src/test/fixtures";
import { openConnection, waitForReport } from "./demo";

test("a ClickHouse failure opens its native evidence and copies a selection for that exact execution", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await openConnection(page, "mssql-demo");
  await waitForReport(page);
  await page.evaluate(async ({ path, snapshot }) => {
    const store = await import(path) as typeof import("../src/lib/store");
    const state = store.getState().report;
    if (state.status !== "ready") throw new Error("expected saved report");
    store.setState({ report: { ...state, report: { ...state.report, snapshot } } });
  }, { path: "/src/lib/store.ts", snapshot: fromRust<Snapshot>(captured) });

  await page.locator('[data-tile="health"]').click();
  const card = page.locator(".operational-card").filter({ has: page.getByRole("heading", { name: "ClickHouse operations" }) });
  await card.locator("summary").filter({ hasText: "system.query_log.exceptions" }).click();
  const original = captured.operations.clickhouse.find(row => row.kind === "failure" && row.observation.exception_name === "TOO_MANY_PARTS");
  if (!original) throw new Error("missing failed insert");
  const summary = card.getByText(`TOO_MANY_PARTS · ${original.observation.query_id}`, { exact: true });
  await summary.click();
  const failed = summary.locator("..");
  await failed.getByRole("combobox").selectOption("events");
  await failed.getByRole("button", { name: "Copy collector selection" }).click();
  const selection = JSON.parse(await page.evaluate(() => navigator.clipboard.readText()));
  expect(selection.focus.request.query_id).toBe(original.observation.query_id);
  expect(selection.focus.request.started_us).toBe(original.observation.started_us);
  expect(selection.focus.target.incarnation).toBe(captured.identity.incarnation);
  expect(selection.focus.table).toBe("events");
});
