// Presentation-only synthetic window records. No manifest/body proof is fabricated or sent to authentication.
// Rust EvidenceService controls own actual canonical archive/EOF tests; this fixture exercises only UI text/lifetimes.
import type { WarehouseWindowQuery } from "../gen/WarehouseWindowQuery";
import type { WarehouseWindowReport } from "../gen/WarehouseWindowReport";
import type { Finding } from "../gen/Finding";
import { partial } from "./fixtures";
import { warehouseIncidentCase } from "./warehouse-incidents";
export function warehouseWindowCase(request: WarehouseWindowQuery, marker = "window finding"): WarehouseWindowReport {
  const incident = warehouseIncidentCase("Window victim", request.resource.database);
  incident.report.from = request.window.from; incident.report.until = request.window.until;
  const activity = incident.report.incidents[0]!.affected_queries[0]!.evidence[0]!;
  const original = { capture: activity.capture, manifest_sha256: activity.manifest_sha256, source_scope_id: activity.source_scope_id };
  const gold = { capture: { observation_id: `presentation-gold:${request.after == null ? "first" : "second"}`, revision: { outbox_id: "presentation-only", owner_epoch: "1", sequence: "3" } }, manifest_sha256: "f".repeat(64), source_scope_id: "a".repeat(64) };
  const finding = partial<Finding>({ id: "presentation:window", check: "presentation", title: marker,
    summary: "Recorded <script>finding</script> text", severity: "medium", confidence: "medium",
    fix: { summary: "Historical action", sql: "SELECT '<script>fix</script>'", cost: null },
    verify: ["Verify against fresh original evidence"], caveats: [], metrics: [], queries: [], inactive: false });
  const placement = { time: { kind: "unknown" as const }, overlaps_window: null, read_complete: false,
    qualification: "Original native clock unavailable" };
  return { request, discovery_complete: request.after != null, originals: [], workload: { kind: "not-requested" },
    next: request.after == null ? { selection_key: "e".repeat(64), position: { resource_id: "a".repeat(64), source_id: "b".repeat(64), range_id: "c".repeat(64) } } : null,
    incidents: incident.report, incident_limitations: incident.analysis_limitations,
    published: [{ context: { capture: gold, description: { analysis_id: "d".repeat(64),
      analyzer: { component: "presentation-only", version: "1", revision: null }, rules_version: "presentation-only",
      language: "en", display: { connection_alias: "window archive", environment: null, database_name: null },
      recorded_at: request.as_of, observed_from: request.window.from, observed_until: request.window.until,
      inputs: [{ capture: original, role: "observation", catalog: null }], origin: "retained-input", input_read_complete: false,
      counts: { incidents: 0, findings: 1, changes: 1 }, output_coverage: { incidents: { state: "context-unavailable", reason: "no-diagnostic-revisions" }, findings: { state: "evaluated" }, changes: { state: "evaluated" } },
      finding_evaluations: { checks: [], populations: [], population_count: 0, omitted_populations: 0 }, change_evaluations: [], context_issues: [] } },
      limitations: ["Original schema context unavailable"], copies: [gold], native_clock: { kind: "unknown" }, native_window_qualified: false,
      rules: { kind: "other-rules", expected: "current rules" }, read_complete: false,
      findings: [{ finding: { analysis_id: "d".repeat(64), database: request.resource.database,
        display: { connection_alias: "window archive", environment: null, database_name: null }, finding,
        inputs: [], query: null, cause: null, guidance: "historical-requires-fresh-verification" }, placement }],
      changes: [{ change: { analysis_id: "d".repeat(64), database: request.resource.database,
        display: { connection_alias: "window archive", environment: null, database_name: null }, inputs: [],
        change: { id: "presentation:change", kind: "setting", source: "scans", action: "changed", at: request.window.until,
          after: request.window.from, until: null, title: { text: "Recorded <img> change" }, detail: { text: "Historical setting difference" },
          table: null, object: null, minor: false, by: null, job_execution: null } }, placement }],
    }], gaps: [], qualifications: ["Published page is partial; unknown is not a healthy empty window"],
    archive_work: { selected_rows: 2, queries: 3, pages: 1, returned_rows: 2, response_bytes: 2n, proof_bytes: 3n, recovery_objects: 0, recovery_bytes: 0n } };
}
