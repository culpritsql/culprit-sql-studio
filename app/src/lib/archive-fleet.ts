import type { WarehouseFleetCursor } from "../gen/WarehouseFleetCursor";
import type { WarehouseFleetReport } from "../gen/WarehouseFleetReport";
import type { WarehouseFleetRequest } from "../gen/WarehouseFleetRequest";
import type { WarehouseFleetRow } from "../gen/WarehouseFleetRow";

export type FleetChip = "attention" | "incidents" | "regressions" | "high-findings" | "collector-health" | "all";

function sameFleetCursor(a: WarehouseFleetCursor | null, b: WarehouseFleetCursor | null): boolean {
  if (a == null || b == null) return a === b;
  return a.selection_key === b.selection_key && a.resource_id === b.resource_id;
}

/** Request/result consistency fence for fleet triage reads. */
export function matchesFleetRequest(report: WarehouseFleetReport, request: WarehouseFleetRequest): boolean {
  const actual = report.request;
  const sameWindow =
    actual.window == null || request.window == null
      ? actual.window === request.window
      : actual.window.from === request.window.from && actual.window.until === request.window.until;
  return (
    actual.as_of === request.as_of &&
    sameWindow &&
    actual.max_age_seconds === request.max_age_seconds &&
    actual.page_rows === request.page_rows &&
    actual.sort === request.sort &&
    actual.filter.attention_only === request.filter.attention_only &&
    actual.filter.engine === request.filter.engine &&
    actual.filter.environment === request.filter.environment &&
    actual.filter.shared_group === request.filter.shared_group &&
    sameFleetCursor(actual.after, request.after) &&
    report.rows.length <= request.page_rows
  );
}

export function rowNeedsAttention(row: WarehouseFleetRow): boolean {
  return (
    row.active_incidents > 0 ||
    row.uncertain_incidents > 0 ||
    row.high_findings > 0 ||
    row.recent_regressions > 0 ||
    row.freshness !== "observed"
  );
}

export function filterFleetRows(rows: WarehouseFleetRow[], chip: FleetChip, search: string): WarehouseFleetRow[] {
  const needle = search.trim().toLowerCase();
  return rows.filter((row) => {
    const chipMatches =
      chip === "all"
        ? true
        : chip === "attention"
          ? rowNeedsAttention(row)
          : chip === "incidents"
            ? row.active_incidents > 0 || row.uncertain_incidents > 0
            : chip === "regressions"
              ? row.recent_regressions > 0 || row.recent_changes > 0
              : chip === "high-findings"
                ? row.high_findings > 0
                : row.freshness !== "observed" || !row.input_read_complete;
    if (!chipMatches) return false;
    if (!needle) return true;
    const haystack = [
      row.database_name,
      row.connection_alias ?? "",
      row.engine,
      row.environment ?? "",
      row.shared_group?.label ?? "",
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(needle);
  });
}

export type FleetGroup = {
  key: string;
  label: string;
  rows: WarehouseFleetRow[];
};

export function groupFleetRows(rows: WarehouseFleetRow[], byShared: boolean, standaloneLabel: string): FleetGroup[] {
  if (!byShared) {
    return [{ key: "all", label: "", rows }];
  }
  const groups = new Map<string, FleetGroup>();
  for (const row of rows) {
    const key = row.shared_group ? `shared:${row.shared_group.member_id}` : "standalone";
    const label = row.shared_group ? row.shared_group.label : standaloneLabel;
    const existing = groups.get(key);
    if (existing) {
      existing.rows.push(row);
    } else {
      groups.set(key, { key, label, rows: [row] });
    }
  }
  return [...groups.values()];
}
