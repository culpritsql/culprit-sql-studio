import { describe, expect, test } from "bun:test";
import type { Plan } from "../gen/Plan";
import type { WarehouseArtifactReport } from "../gen/WarehouseArtifactReport";
import type { WarehouseFleetReport } from "../gen/WarehouseFleetReport";
import type { WarehouseFleetRequest } from "../gen/WarehouseFleetRequest";
import type { WarehouseFleetRow } from "../gen/WarehouseFleetRow";
import type { WarehouseWindowReport } from "../gen/WarehouseWindowReport";
import { filterFleetRows, groupFleetRows, matchesFleetRequest } from "./archive-fleet";
import { extractComparablePlans } from "./archive-plan-compare";
import { compareArchivedWindows } from "./archive-window-compare";

const HEX64 = "a".repeat(64);
const HEX64_B = "b".repeat(64);

function sampleRow(overrides: Partial<WarehouseFleetRow> = {}): WarehouseFleetRow {
  return {
    resource_id: HEX64,
    selector: { namespace_id: HEX64_B, database: "db-selector-1" as WarehouseFleetRow["selector"]["database"] },
    engine: "mssql",
    database_name: "orders",
    connection_alias: "orders-prod",
    environment: "production",
    shared_group: null,
    freshness: "observed",
    latest_collected_from: "2026-04-01T11:55:00Z",
    latest_collected_until: "2026-04-01T11:56:00Z",
    latest_observed_from: "2026-04-01T11:50:00Z",
    latest_observed_until: "2026-04-01T11:55:00Z",
    active_incidents: 0,
    uncertain_incidents: 0,
    high_findings: 0,
    actionable_findings: 1,
    recent_regressions: 0,
    recent_changes: 0,
    input_read_complete: true,
    limitations: [],
    ...overrides,
  };
}

describe("archive fleet triage", () => {
  test("validates request/report consistency fence", () => {
    const request: WarehouseFleetRequest = {
      as_of: "2026-04-01T12:00:00Z",
      window: null,
      max_age_seconds: 1800,
      filter: { environment: null, engine: null, attention_only: false, shared_group: null },
      sort: "attention",
      page_rows: 24,
      after: null,
    };
    const report: WarehouseFleetReport = {
      request,
      summary: {
        evaluated_resources: 1,
        attention_resources: 0,
        active_incidents: 0,
        high_finding_resources: 0,
        regression_resources: 0,
        stale_or_partial_resources: 0,
      },
      rows: [sampleRow()],
      next: null,
      discovery_complete: true,
      gaps: [],
      candidate_rows: 1,
      archive_work: {
        selected_rows: 0,
        queries: 1,
        pages: 1,
        returned_rows: 1,
        response_bytes: 64n,
        proof_bytes: 64n,
        recovery_objects: 0,
        recovery_bytes: 0n,
      },
      qualifications: [],
    };
    expect(matchesFleetRequest(report, request)).toBe(true);
    expect(matchesFleetRequest(report, { ...request, as_of: "2026-04-01T12:01:00Z" })).toBe(false);
  });

  test("filters by chips and search and groups by shared pool/instance", () => {
    const rows = [
      sampleRow({ resource_id: "1".repeat(64), database_name: "orders", active_incidents: 2 }),
      sampleRow({
        resource_id: "2".repeat(64),
        database_name: "billing",
        connection_alias: "billing-prod",
        high_findings: 3,
        shared_group: { member_id: "pool-1", member_kind: "pool", label: "Elastic Pool · commerce-pool" },
      }),
      sampleRow({
        resource_id: "3".repeat(64),
        database_name: "catalog",
        connection_alias: "catalog-prod",
        freshness: "stale",
        shared_group: { member_id: "pool-1", member_kind: "pool", label: "Elastic Pool · commerce-pool" },
      }),
      sampleRow({ resource_id: "4".repeat(64), database_name: "healthy", connection_alias: "healthy-prod" }),
    ];

    expect(filterFleetRows(rows, "attention", "").map((r) => r.database_name)).toEqual(["orders", "billing", "catalog"]);
    expect(filterFleetRows(rows, "incidents", "").map((r) => r.database_name)).toEqual(["orders"]);
    expect(filterFleetRows(rows, "high-findings", "").map((r) => r.database_name)).toEqual(["billing"]);
    expect(filterFleetRows(rows, "collector-health", "").map((r) => r.database_name)).toEqual(["catalog"]);
    expect(filterFleetRows(rows, "all", "commerce-pool").map((r) => r.database_name)).toEqual(["billing", "catalog"]);

    const grouped = groupFleetRows(rows, true, "Standalone databases");
    expect(grouped.length).toBe(2);
    expect(grouped.find((g) => g.key === "shared:pool-1")?.rows.length).toBe(2);
    expect(grouped.find((g) => g.key === "standalone")?.rows.length).toBe(2);
  });
});

describe("archived plan & window compare", () => {
  test("extractComparablePlans admits complete plans and refuses chunk/metadata slices", () => {
    const fakePlan = { engine: "mssql", kind: "estimated" } as unknown as Plan;
    const report = {
      query: {} as WarehouseArtifactReport["query"],
      records: [
        {
          reference: {
            capture: { observation_id: "obs-1", revision: null },
            manifest_sha256: HEX64,
            source_scope_id: HEX64,
            stream: "plan-definitions",
            batch_id: "b1",
            row_ordinal: 1,
            record_id: "rec-1",
            representation_version: 3,
            native_key: {} as WarehouseArtifactReport["records"][number]["reference"]["native_key"],
            payload_sha256: HEX64,
          },
          engine: "mssql",
          scope: { kind: "database", database: "db-1" as never, label: "orders" },
          source: "query-store",
          native_clock: { kind: "unknown" },
          collected_from: "2026-04-01T11:00:00Z",
          collected_until: "2026-04-01T11:05:00Z",
          linked_from: [],
          artifacts: [
            {
              location: "/record/plan/1",
              kind: "plan",
              label: "Plan 68",
              payload: { kind: "complete", payload: { state: "available", text: "<ShowPlanXML/>", sha256: HEX64, redaction_version: 1 } },
              matches_requested_sql: true,
              plan: fakePlan,
              limitations: [],
            },
            {
              location: "/record/plan/2",
              kind: "plan",
              label: "Plan 69 slice",
              payload: {
                kind: "chunk",
                sha256: HEX64,
                redaction_version: 1,
                from_character: 0,
                until_character: 512,
                total_characters: 4096,
                text: "<ShowPlan",
                next: null,
              },
              matches_requested_sql: true,
              plan: null,
              limitations: [],
            },
          ],
        },
      ],
      gaps: [],
      qualifications: [],
      archive_work: {} as WarehouseArtifactReport["archive_work"],
    } satisfies WarehouseArtifactReport;

    const result = extractComparablePlans(report);
    expect(result.complete.length).toBe(1);
    expect(result.complete[0]?.label).toBe("Plan 68");
    expect(result.refusedSlices).toBe(1);
  });

  test("compareArchivedWindows refuses mismatched selectors and diffs findings and slower queries", () => {
    const baseWindow = {
      request: {
        resource: { namespace_id: HEX64, database: "db-1" as never },
        window: { from: "2026-04-01T09:00:00Z", until: "2026-04-01T10:00:00Z" },
        as_of: "2026-04-01T12:00:00Z",
        view: "compatible-workload",
        page_rows: 1,
        after: null,
      },
      published: [
        {
          findings: [
            { finding: { finding: { id: "missing-index:old", title: "Old index", summary: "Old", inactive: false } } },
          ],
        },
      ],
      workload: {
        kind: "evaluated",
        analysis: {
          sections: [
            {
              analysis: {
                workload: {
                  queries: [{ id: "q1", text: "SELECT 1", executions: 20, mean_time_ms: 10 }],
                },
              },
            },
          ],
        },
      },
    } as unknown as WarehouseWindowReport;

    const currentWindow = {
      ...baseWindow,
      request: {
        ...baseWindow.request,
        window: { from: "2026-04-01T10:00:00Z", until: "2026-04-01T11:00:00Z" },
      },
      published: [
        {
          findings: [
            { finding: { finding: { id: "missing-index:new", title: "New index", summary: "New", inactive: false } } },
          ],
        },
      ],
      workload: {
        kind: "evaluated",
        analysis: {
          sections: [
            {
              analysis: {
                workload: {
                  queries: [{ id: "q1", text: "SELECT 1", executions: 20, mean_time_ms: 30 }],
                },
              },
            },
          ],
        },
      },
    } as unknown as WarehouseWindowReport;

    const diff = compareArchivedWindows(baseWindow, currentWindow);
    expect(diff).not.toBeNull();
    expect(diff?.newFindings.map((f) => f.finding.id)).toEqual(["missing-index:new"]);
    expect(diff?.resolvedFindings.map((f) => f.finding.id)).toEqual(["missing-index:old"]);
    expect(diff?.slowerQueries.length).toBe(1);
    expect(diff?.slowerQueries[0]?.ratio).toBe(3);

    const foreign = {
      ...currentWindow,
      request: { ...currentWindow.request, resource: { namespace_id: HEX64_B, database: "db-1" as never } },
    } as unknown as WarehouseWindowReport;
    expect(compareArchivedWindows(baseWindow, foreign)).toBeNull();
  });
});
