import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { WarehouseWindowQuery } from "../gen/WarehouseWindowQuery";
import { ArchiveWindow } from "../views/archives/ArchiveWindow";
import { warehouseWindowCase } from "../test/warehouse-windows";
import { matchesWindowRequest, nextWindowPage } from "./archive-window";
import { t } from "./i18n";
const request: WarehouseWindowQuery = { resource: { namespace_id: "a".repeat(64), database: "b".repeat(64) },
  window: { from: "2026-11-22T09:00:00Z", until: "2026-11-22T11:00:00Z" }, as_of: "2026-11-22T12:00:00Z",
  view: "published-answers", page_rows: 1, after: null };
test("copied continuation fixes resource namespace native window policy and cutoff without manufacturing authority", () => {
  const first = warehouseWindowCase(structuredClone(request));
  expect(matchesWindowRequest(structuredClone(first), request)).toBe(true);
  const next = nextWindowPage(first)!;
  expect(next).toEqual({ ...request, after: first.next });
  expect(next.resource).toEqual(request.resource); expect(next.window).toEqual(request.window);
  const last = warehouseWindowCase(next); expect(nextWindowPage(last)).toBeNull();
  expect(matchesWindowRequest(last, next)).toBe(true);
});
test("foreign namespaces changed windows cutoffs policies and incident resource responses cannot replace a selected page", () => {
  for (const mutate of [
    (value: ReturnType<typeof warehouseWindowCase>) => { value.request.resource.namespace_id = "f".repeat(64); },
    (value: ReturnType<typeof warehouseWindowCase>) => { value.request.window.from = "2026-11-22T08:00:00Z"; },
    (value: ReturnType<typeof warehouseWindowCase>) => { value.request.as_of = "2026-11-23T12:00:00Z"; },
    (value: ReturnType<typeof warehouseWindowCase>) => { value.request.view = "compatible-workload"; },
    (value: ReturnType<typeof warehouseWindowCase>) => { value.incidents.database = "c".repeat(64); },
  ]) { const result = warehouseWindowCase(structuredClone(request)); mutate(result); expect(matchesWindowRequest(result, request)).toBe(false); }
});
test("a published page renders findings changes retained resolved incidents and unknown lineage as read-only text", () => {
  const value = warehouseWindowCase(structuredClone(request));
  const html = renderToStaticMarkup(<ArchiveWindow value={value} />);
  for (const key of ["ui.window.page_scope", "window.reader.historical_guidance", "window.reader.workload_not_requested", "window.reader.more"]) expect(html).toContain(t(key));
  expect(html).toContain("Original schema context unavailable"); expect(html).toContain("Original native clock unavailable");
  expect(html).toContain("&lt;script&gt;finding&lt;/script&gt;");expect(html).toContain("&lt;img&gt;");
  expect(html).not.toContain("<script>"); expect(html).not.toContain("<img>");
  expect(html).not.toContain("run_query"); expect(html).not.toContain("interrupt_session"); expect(html).not.toContain("Apply fix");
  expect(html).toContain(value.incidents.incidents[0]!.entry.phase);
});
