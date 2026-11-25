import type { NativeLogAnalysis } from "../gen/NativeLogAnalysis";

/** Presentation-only artifact case. It is never an authenticated native capture or a database owner. */
export function nativeEventArtifact(marker = "These imported rows do not count native incidents."): NativeLogAnalysis {
  return {
    input: { format: "mssql-extended-events-xml", sha256: "a".repeat(64), bytes: 200n, complete: true, limitation: null },
    evidence: { source: "supplied-mssql-event-xml", observed_from: "2026-11-23T10:00:00Z", observed_until: "2026-11-23T10:00:01Z", resource: null, observations: [
      { native: "mssql-extended-event", observation: { position: 0, kind: { event: "attention", native_duration: null }, timestamp: "2020-01-01T14:00:00+02:00", timestamp_utc: "2020-01-01T12:00:00Z", event_sequence: null, session_id: "59", request_id: null, transaction_id: null, database_id: "7", database: "<img src=x>", client: { application: "<script>client</script>", user: null, host: "reported-host" }, statement: { state: "available", text: "SELECT <script>sql</script>", sha256: "b".repeat(64), redaction_version: 3 } } },
      { native: "mssql-extended-event", observation: { position: 1, kind: { event: "error-reported", number: 1222, severity: 16, state: 1, intercepted: null, user_defined: null }, timestamp: "2026-10-25T03:30:00", timestamp_utc: null, event_sequence: null, session_id: null, request_id: null, transaction_id: null, database_id: null, database: null, client: null, statement: { state: "unavailable", reason: "not-collected" } } },
    ] },
    source: { source: "supplied-mssql-event-xml", state: "collected", reason: null },
    records_examined: 2, non_failure_records: 0, issues: [], stopped: null,
    report: { incident_input: { kind: "ineligible", reason: "shared-ownership-unavailable" }, incident_input_explanation: "The artifact does not prove native ownership.", source_state: "collected", source_reason: null, title: "Supplied database event evidence", summary: marker, limitations: ["No historical owner or complete history is established."], entries: [0, 1].map((position) => ({ position, title: `Native event ${position}`, explanation: "The captured event is contextual evidence.", investigation: "Compare independently qualified original evidence.", verify: "Verify using the same qualified native scope." })) },
    native_events: { timeline: [], unplaced_positions: [1], deadlocks: [], locations: [{ position: 0, buffer_offset: null, event_offset: 8n }, { position: 1, buffer_offset: null, event_offset: 120n }] },
  };
}
