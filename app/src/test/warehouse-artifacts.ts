// Presentation-only synthetic artifact output. These references never enter backend authentication.
import type { WarehouseArtifactQuery } from "../gen/WarehouseArtifactQuery";
import type { WarehouseArtifactReport } from "../gen/WarehouseArtifactReport";
import type { Plan } from "../gen/Plan";
export function warehouseArtifactCase(query: WarehouseArtifactQuery, marker = "COMPLETE_RETAINED_BODY", plan: Plan | null = null): WarehouseArtifactReport {
  const report: WarehouseArtifactReport = { query: structuredClone(query), records: [{ reference: query.references[0]!, engine: "mssql",
    scope: { kind: "database", database: query.resource.database, label: "Presentation original" }, source: "presentation-only",
    native_clock: { kind: "unknown" }, collected_from: "2026-11-22T10:00:00Z", collected_until: "2026-11-22T10:00:01Z", linked_from: [],
    artifacts: [{ location: "/record/data/statement", kind: "sql", label: "SQL", matches_requested_sql: false, plan: null, limitations: [],
      payload: { kind: "complete", payload: { state: "available", text: `${marker} ${"retained_column,".repeat(220)} <script>retained-data</script>`, sha256: "e".repeat(64), redaction_version: 1 } } }],
  }], gaps: [], qualifications: ["Presentation-only original scope and unavailable ownership context"],
    archive_work: { selected_rows: 1, queries: 4, pages: 1, returned_rows: 1, response_bytes: 20n, proof_bytes: 30n, recovery_objects: 1, recovery_bytes: 5000n } };
  if (plan?.raw) report.records[0]!.artifacts.push({
    location: "/record/data/plan", kind: "plan", label: "Plan", matches_requested_sql: false,
    plan, limitations: [], payload: { kind: "complete", payload: { state: "available", text: plan.raw, sha256: "f".repeat(64), redaction_version: 1 } },
  });
  return report;
}
