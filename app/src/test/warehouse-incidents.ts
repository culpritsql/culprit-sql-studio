// Presentation-only synthetic derived record over Rust's unverified local history artifact.
// These typed references have no manifest/body proof and are NEVER supplied to backend authentication.
import { incidentHistoryCase } from "./incident-history";
import type { WarehouseIncidentView } from "../gen/WarehouseIncidentView";
import type { DerivedIncidentQuery } from "../gen/DerivedIncidentQuery";
import type { DiagnosticEvidenceRef } from "../gen/DiagnosticEvidenceRef";

export function warehouseIncidentCase(marker = "", database = "b".repeat(64)): WarehouseIncidentView {
  const history = incidentHistoryCase("resolved");
  const entry = history.entries.at(-1)!;
  // Keep entry.original === null: the local JSONL intake intentionally has no authenticated revision receipt.
  const revisionReference: DiagnosticEvidenceRef = {
    capture: { observation_id: "presentation-diagnostic-revision", revision: { outbox_id: "presentation-only", owner_epoch: "1", sequence: "1" } },
    manifest_sha256: "1924dec05136f25cbca3a356e4960f93cc183d0ee7ddb9666eefe8e94b8ace08", source_scope_id: "9e74a471c56585412eebd8aa666e30e91336d0938f299c49eee8cd92df89bacb", stream: "operational-samples",
    native_key: { kind: "operational", data: { source: "culprit-diagnostics", key: entry.revision_id } },
    batch_id: "3591c5eeefe47647b0c01b2a0b6ebcd70eed3e278151f7b1d9f51592d8d1dd5b", record_id: "0494ce83f5715348d5b9bfd4c5a8c6da1fff6e1f505e0d072cb1493084e47d7e", payload_sha256: "efbaf1dcd26e234bfee53e70c291760df39205c517deaa61a9d0d8f01b460675", representation_version: 3, row_ordinal: 0,
  };
  // Activity evidence stays in its own role; it is never relabeled as the diagnostic revision reference.
  const activityReference: DiagnosticEvidenceRef = {
    capture: { observation_id: "presentation-activity", revision: { outbox_id: "presentation-only", owner_epoch: "1", sequence: "2" } },
    manifest_sha256: "81197ccd5a3eaf049b2802126a6ae10935d1089b8c24e4a3ccc763cbabddda24", source_scope_id: "99f43f17bcf93e629a8c62c197ee0ad80872dd8e6d7654ffad765c879e666d8f", stream: "activity-observations",
    native_key: { kind: "activity", data: { session: { id: "presentation-waiter", generation: "presentation-login" }, request: { id: "presentation-request", generation: "presentation-request-start" }, transaction: null } },
    batch_id: "fcf4d6de94c8385d4cef7b7cb213cc920c212f5000eefe0ca3b9a3655a8799ec", record_id: "68627f1be71fbfbd257c980a680cfb2a758ba8e4fb0c15175e6318f8e5fb2a69", payload_sha256: "7792978565a050495e0b8e6f76f2fe96e9fc67fac81de4aaab621c51322c5acd", representation_version: 3, row_ordinal: 0,
  };
  const query = (text: string, sha256: string): DerivedIncidentQuery => ({
    identity: null, sql: { state: "available", text, sha256, redaction_version: 1 }, evidence: [activityReference],
    caller: { client: { application: "<script>native-client</script>", user: "database-user", host: "client-host" }, session_context: null, evidence: activityReference }, causal_role: null,
  });
  const owner: DerivedIncidentQuery = {
    identity: { kind: "native-wait-owner", data: { kind: "session", id: "presentation-blocker", generation: "presentation-blocker-login" } },
    sql: { state: "unavailable", reason: "not-collected" }, evidence: [activityReference], caller: null,
    causal_role: { kind: "native-blocking-owner", explanation: { text: "Presentation-only blocking-owner role; acquiring SQL is unavailable" } },
  };
  return { analysis_limitations: [{ observed_from: "2026-11-22T10:00:00Z", observed_until: "2026-11-22T10:00:01Z", reasons: ["Original schema context unavailable"] }], report: {
    database, from: "2026-11-22T09:55:00Z", until: "2026-11-22T10:05:00Z", recorded_before: "2026-11-22T12:00:00Z", discovery_complete: false,
    incidents: [{ analysis_id: "c".repeat(64), database, display: { connection_alias: "historical", environment: null, database_name: "db" }, entry, original_interval: null, revision_reference: revisionReference,
      victim_summary: marker || "Waiting query is affected", affected_queries: [query("SELECT affected; SELECT '<img src=x onerror=alert(1)>'", "9b0691d6aa9ee7067399775db16a3edbf1f97988c4a6595063f37149df99964e")], culprit_queries: [owner], candidate_queries: [query("SELECT candidate; SELECT '<script>candidate</script>'", "ff91d12a58e74583b20a6dc5834063a22a3ef6b42afd16ac6acc22405f4c38eb")], attribution_status: "established", mechanism: "Original lock dependency", developer_action: "Close the retained transaction path", verification: "Compare a bounded later original", caller: null }],
    analyses: [], history, relationships: [], gaps: [], archive_work: { selected_rows: 0, queries: 1, pages: 1, returned_rows: 0, response_bytes: 2n, proof_bytes: 3n, recovery_objects: 0, recovery_bytes: 0n }, candidate_rows: 1,
  } };
}
