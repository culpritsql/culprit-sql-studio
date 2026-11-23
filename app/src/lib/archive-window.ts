import type { WarehouseWindowQuery } from "../gen/WarehouseWindowQuery";
import type { WarehouseWindowReport } from "../gen/WarehouseWindowReport";
import type { PublishedResourceCursor } from "../gen/PublishedResourceCursor";
function sameCursor(a: PublishedResourceCursor | null, b: PublishedResourceCursor | null): boolean {
  return a == null || b == null ? a === b : a.selection_key === b.selection_key &&
    a.position.resource_id === b.position.resource_id && a.position.source_id === b.position.source_id && a.position.range_id === b.position.range_id;
}
/** A request/result consistency fence, never warehouse or native evidence authentication. */
export function matchesWindowRequest(report: WarehouseWindowReport, request: WarehouseWindowQuery): boolean {
  const actual = report.request;
  return actual.resource.namespace_id === request.resource.namespace_id && actual.resource.database === request.resource.database &&
    actual.window.from === request.window.from && actual.window.until === request.window.until && actual.as_of === request.as_of &&
    actual.view === request.view && actual.page_rows === request.page_rows && sameCursor(actual.after, request.after) &&
    report.incidents.database === request.resource.database &&
    (request.view !== "published-answers" || report.published.length <= request.page_rows) &&
    (request.view !== "compatible-workload" || report.next == null) &&
    (report.next == null || request.after == null || report.next.selection_key === request.after.selection_key);
}
/** Copy the service's exact fixed selection. A cursor is never rebuilt from display names or timestamps. */
export function nextWindowPage(report: WarehouseWindowReport): WarehouseWindowQuery | null {
  return report.request.view !== "published-answers" || report.next == null ? null : { ...report.request, after: report.next };
}
