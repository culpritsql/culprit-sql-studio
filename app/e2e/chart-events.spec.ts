import { expect, test } from "@playwright/test";
import { openConnection, waitForReport } from "./demo";

test("clustered database changes fit the window and can be read from first to last", async ({ page }, info) => {
  await page.setViewportSize({ width: 960, height: 600 });
  await openConnection(page, "mssql-demo");
  await waitForReport(page);
  const since = await page.evaluate(async () => {
    const apiPath = "/src/lib/api.ts", eventsPath = "/src/lib/events.ts", storePath = "/src/lib/store.ts";
    const { api } = await import(apiPath) as typeof import("../src/lib/api");
    const { eventsChanged } = await import(eventsPath) as typeof import("../src/lib/events");
    const { getState } = await import(storePath) as typeof import("../src/lib/store");
    const report = getState().report;
    const timeline = report.status === "ready" && report.report.snapshot.workload?.timeline;
    if (!timeline) throw new Error("Missing database time history");
    const at = timeline.starts.at(-1)!;
    (await api()).events = async (connection, req) => ({
      connection, start: req.start!, end: req.end!, notes: [],
      events: Array.from({ length: 32 }, (_, index) => ({
        id: `index-${index}`, kind: "index", source: "database", action: null, at, after: null, until: null,
        title: { text: `Index orders_${index + 1} was created on sales.orders` }, detail: null,
        table: "sales.orders", object: null, minor: false, by: null, job_execution: null,
      })),
    });
    eventsChanged();
    return at;
  });
  const marker = page.locator(".dash-chart .tc-ev").filter({ hasText: "32" });
  await expect(marker).toBeAttached();
  const moveMarker = (top: number) => marker.evaluate((button, y) => {
    button.closest(".dash")!.scrollTop += button.getBoundingClientRect().top - y;
  }, top);
  await moveMarker(115);
  await marker.hover();
  const tip = page.getByRole("tooltip");
  await expect(tip).toBeInViewport({ ratio: 1 });
  expect((await tip.boundingBox())!.y).toBeGreaterThan((await marker.boundingBox())!.y);
  const entries = tip.locator(".tc-ev-item");
  await expect(entries).toHaveCount(32);
  await expect(entries.first()).toBeInViewport({ ratio: 1 });
  await expect(entries.last()).not.toBeInViewport();
  await page.screenshot({ animations: "disabled", path: info.outputPath("chart-changes.png") });
  const scrollBefore = await page.locator(".dash").evaluate(el => el.scrollTop);
  await entries.first().hover();
  await page.mouse.wheel(0, 2400);
  await expect(entries.last()).toBeInViewport({ ratio: 1 });
  expect(await page.locator(".dash").evaluate(el => el.scrollTop)).toBe(scrollBefore);
  await page.keyboard.press("Escape");
  await expect(tip).toHaveCount(0);

  await moveMarker(510);
  await marker.focus();
  await expect(tip).toBeInViewport({ ratio: 1 });
  expect((await tip.boundingBox())!.y).toBeLessThan((await marker.boundingBox())!.y);
  await moveMarker(130);
  await expect.poll(async () => (await tip.boundingBox())?.y ?? 0).toBeGreaterThan(130);
  await marker.press("End");
  await expect(entries.last()).toBeInViewport({ ratio: 1 });
  await marker.press("Home");
  await expect(entries.first()).toBeInViewport({ ratio: 1 });
  await marker.press("Enter");
  await expect.poll(() => page.evaluate(async () => {
    const path = "/src/lib/store.ts";
    const { getState } = await import(path) as typeof import("../src/lib/store");
    return getState().changes?.since;
  })).toBe(new Date(since).toISOString());
});
