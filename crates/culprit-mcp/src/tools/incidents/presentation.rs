//! Presentation selects bounded original fields. It performs no attribution, native classification or arithmetic.
use super::*;
use culprit_core::{
    DerivedCaller, DerivedCausalRoleKind, DerivedIncident, DerivedIncidentQuery, DerivedQueryIdentity,
    IncidentAnalysisContext, IncidentDatabaseSelector, IncidentEvidenceRelationship, IncidentHistoryEntry,
    IncidentHistoryEvidence, IncidentReadGap, IncidentReadReport, collected_analysis::CollectedEvidenceMetrics,
    telemetry::*,
};
use serde::Serialize;

const INCIDENT_LIMIT: usize = 5;
const HISTORY_LIMIT: usize = 8;
const QUERY_LIMIT: usize = 5;
const CONTEXT_LIMIT: usize = 16;
const TEXT_CHARS: usize = 1024;

pub(super) fn value(report: &IncidentReadReport) -> Result<Value> {
    let mut output = Presentation::new(report);
    while !super::super::response::admits(&output)? {
        if !output.trim() {
            return Err(DeliveryProblem::Capacity.error());
        }
    }
    serde_json::to_value(output).map_err(|_| DeliveryProblem::Integrity.error())
}
#[derive(Serialize)]
struct Presentation<'a> {
    database: &'a IncidentDatabaseSelector,
    from: &'a str,
    until: &'a str,
    recorded_before: &'a str,
    discovery_complete: bool,
    presentation_complete: bool,
    source_context: String,
    incidents_available: usize,
    incidents_omitted: usize,
    incidents: Vec<Incident<'a>>,
    history: History<'a>,
    relationships_available: usize,
    relationships_omitted: usize,
    relationships: Vec<&'a IncidentEvidenceRelationship>,
    analyses_available: usize,
    analyses_omitted: usize,
    analyses: Vec<&'a IncidentAnalysisContext>,
    gaps_available: usize,
    gaps_omitted: usize,
    gaps: Vec<&'a IncidentReadGap>,
    archive_work: &'a CollectedEvidenceMetrics,
    candidate_rows: u32,
}
impl<'a> Presentation<'a> {
    fn new(report: &'a IncidentReadReport) -> Self {
        let mut value = Self {
            database: &report.database,
            from: &report.from,
            until: &report.until,
            recorded_before: &report.recorded_before,
            discovery_complete: report.discovery_complete,
            presentation_complete: false,
            source_context: culprit_core::t!("mcp.incident.source_context"),
            incidents_available: report.incidents.len(),
            incidents_omitted: 0,
            incidents: report.incidents.iter().take(INCIDENT_LIMIT).map(Incident::new).collect(),
            history: History::new(&report.history),
            relationships_available: report.relationships.len(),
            relationships_omitted: 0,
            relationships: report.relationships.iter().take(CONTEXT_LIMIT).collect(),
            analyses_available: report.analyses.len(),
            analyses_omitted: 0,
            analyses: report.analyses.iter().take(CONTEXT_LIMIT).collect(),
            gaps_available: report.gaps.len(),
            gaps_omitted: 0,
            gaps: report.gaps.iter().take(CONTEXT_LIMIT).collect(),
            archive_work: &report.archive_work,
            candidate_rows: report.candidate_rows,
        };
        value.recount();
        value
    }
    fn recount(&mut self) {
        self.incidents_omitted = self.incidents_available - self.incidents.len();
        self.relationships_omitted = self.relationships_available - self.relationships.len();
        self.analyses_omitted = self.analyses_available - self.analyses.len();
        self.gaps_omitted = self.gaps_available - self.gaps.len();
        self.history.recount();
        self.presentation_complete = self.incidents_omitted == 0
            && self.relationships_omitted == 0
            && self.analyses_omitted == 0
            && self.gaps_omitted == 0
            && self.history.complete()
            && self.incidents.iter().all(Incident::complete);
    }
    fn trim(&mut self) -> bool {
        let removed = self.relationships.pop().is_some()
            || self.analyses.pop().is_some()
            || self.gaps.pop().is_some()
            || self.history.entries.pop().is_some()
            || self.incidents.iter_mut().rev().any(Incident::trim)
            || self.incidents.pop().is_some();
        self.recount();
        removed
    }
}
#[derive(Serialize)]
struct History<'a> {
    provenance: culprit_core::IncidentHistoryProvenance,
    selected_read_complete: bool,
    source_state: Option<culprit_core::SourceState>,
    source_reason: Option<String>,
    title: String,
    summary: String,
    limitations: Vec<String>,
    limitations_available: usize,
    limitations_omitted: usize,
    entries_available: usize,
    entries_omitted: usize,
    entries: Vec<HistoryEntry<'a>>,
    text_truncated: bool,
}
impl<'a> History<'a> {
    fn new(report: &'a culprit_core::IncidentHistoryReport) -> Self {
        Self {
            provenance: report.provenance,
            selected_read_complete: report.selected_read_complete,
            source_state: report.source_state,
            source_reason: report.source_reason.as_deref().map(preview),
            title: preview(&report.title),
            summary: preview(&report.summary),
            limitations: report.limitations.iter().take(CONTEXT_LIMIT).map(|text| preview(text)).collect(),
            limitations_available: report.limitations.len(),
            limitations_omitted: report.limitations.len().saturating_sub(CONTEXT_LIMIT),
            entries_available: report.entries.len(),
            entries_omitted: 0,
            entries: report.entries.iter().take(HISTORY_LIMIT).map(HistoryEntry::new).collect(),
            text_truncated: [&report.title, &report.summary].into_iter().any(|text| truncated(text))
                || report.source_reason.as_deref().is_some_and(truncated)
                || report.limitations.iter().take(CONTEXT_LIMIT).any(|text| truncated(text)),
        }
    }
    fn recount(&mut self) {
        self.entries_omitted = self.entries_available - self.entries.len();
    }
    fn complete(&self) -> bool {
        self.entries_omitted == 0
            && self.limitations_omitted == 0
            && !self.text_truncated
            && self.entries.iter().all(HistoryEntry::complete)
    }
}
#[derive(Serialize)]
struct HistoryEntry<'a> {
    controller_version: u32,
    episode_id: &'a str,
    revision_id: &'a str,
    revision: &'a str,
    previous_revision: &'a Option<String>,
    predecessor_selected: bool,
    lifecycle: DiagnosticLifecycle,
    analysis: DiagnosticAnalysisStatus,
    source: &'a str,
    target_id: &'a str,
    target_scope: culprit_core::MeasurementScope,
    scope_id: &'a str,
    scope: culprit_core::MeasurementScope,
    scope_label: String,
    title: String,
    phase: String,
    first_seen_at: &'a str,
    last_seen_at: &'a str,
    recorded_at: &'a str,
    original: &'a Option<DiagnosticEvidenceRef>,
    issue: &'a Option<DiagnosticAnalysisIssue>,
    reasons: Vec<String>,
    reasons_available: usize,
    reasons_omitted: usize,
    next_step: String,
    input_references: u32,
    followup_recipe: &'a Option<String>,
    followup_recipe_version: Option<u32>,
    evidence: Vec<HistoryEvidence<'a>>,
    evidence_available: usize,
    evidence_omitted: usize,
    text_truncated: bool,
}
impl<'a> HistoryEntry<'a> {
    fn new(entry: &'a IncidentHistoryEntry) -> Self {
        Self {
            controller_version: entry.controller_version,
            episode_id: &entry.episode_id,
            revision_id: &entry.revision_id,
            revision: &entry.revision,
            previous_revision: &entry.previous_revision,
            predecessor_selected: entry.predecessor_selected,
            lifecycle: entry.lifecycle,
            analysis: entry.analysis,
            source: &entry.source,
            target_id: &entry.target_id,
            target_scope: entry.target_scope,
            scope_id: &entry.scope_id,
            scope: entry.scope,
            scope_label: preview(&entry.scope_label),
            title: preview(&entry.title),
            phase: preview(&entry.phase),
            first_seen_at: &entry.first_seen_at,
            last_seen_at: &entry.last_seen_at,
            recorded_at: &entry.recorded_at,
            original: &entry.original,
            issue: &entry.issue,
            reasons: entry.reasons.iter().take(CONTEXT_LIMIT).map(|text| preview(text)).collect(),
            reasons_available: entry.reasons.len(),
            reasons_omitted: entry.reasons.len().saturating_sub(CONTEXT_LIMIT),
            next_step: preview(&entry.next_step),
            input_references: entry.input_references,
            followup_recipe: &entry.followup_recipe,
            followup_recipe_version: entry.followup_recipe_version,
            evidence: entry.evidence.iter().take(CONTEXT_LIMIT).map(HistoryEvidence::new).collect(),
            evidence_available: entry.evidence.len(),
            evidence_omitted: entry.evidence.len().saturating_sub(CONTEXT_LIMIT),
            text_truncated: [&entry.title, &entry.phase, &entry.scope_label, &entry.next_step]
                .into_iter()
                .any(|text| truncated(text))
                || entry.reasons.iter().take(CONTEXT_LIMIT).any(|text| truncated(text)),
        }
    }
    fn complete(&self) -> bool {
        !self.text_truncated
            && self.reasons_omitted == 0
            && self.evidence_omitted == 0
            && self.evidence.iter().all(|row| !row.text_truncated)
    }
}
#[derive(Serialize)]
struct HistoryEvidence<'a> {
    label: String,
    capture: &'a DiagnosticCaptureRef,
    reference: &'a Option<DiagnosticEvidenceRef>,
    availability: String,
    text_truncated: bool,
}
impl<'a> HistoryEvidence<'a> {
    fn new(row: &'a IncidentHistoryEvidence) -> Self {
        Self {
            label: preview(&row.label),
            capture: &row.capture,
            reference: &row.reference,
            availability: preview(&row.availability),
            text_truncated: truncated(&row.label) || truncated(&row.availability),
        }
    }
}
#[derive(Serialize)]
struct Incident<'a> {
    analysis_id: &'a str,
    entry: HistoryEntry<'a>,
    original_interval: &'a Option<TelemetryInterval>,
    revision_reference: &'a DiagnosticEvidenceRef,
    victim_summary: String,
    affected_queries: Vec<Query<'a>>,
    affected_queries_available: usize,
    affected_queries_omitted: usize,
    culprit_queries: Vec<Query<'a>>,
    culprit_queries_available: usize,
    culprit_queries_omitted: usize,
    candidate_queries: Vec<Query<'a>>,
    candidate_queries_available: usize,
    candidate_queries_omitted: usize,
    attribution_status: culprit_core::DerivedAttributionStatus,
    mechanism: Option<String>,
    developer_action: String,
    verification: String,
    caller: &'a Option<DerivedCaller>,
    text_truncated: bool,
}
impl<'a> Incident<'a> {
    fn new(row: &'a DerivedIncident) -> Self {
        let mut value = Self {
            analysis_id: &row.analysis_id,
            entry: HistoryEntry::new(&row.entry),
            original_interval: &row.original_interval,
            revision_reference: &row.revision_reference,
            victim_summary: preview(&row.victim_summary),
            affected_queries: row.affected_queries.iter().take(QUERY_LIMIT).map(Query::new).collect(),
            affected_queries_available: row.affected_queries.len(),
            affected_queries_omitted: 0,
            culprit_queries: row.culprit_queries.iter().take(QUERY_LIMIT).map(Query::new).collect(),
            culprit_queries_available: row.culprit_queries.len(),
            culprit_queries_omitted: 0,
            candidate_queries: row.candidate_queries.iter().take(QUERY_LIMIT).map(Query::new).collect(),
            candidate_queries_available: row.candidate_queries.len(),
            candidate_queries_omitted: 0,
            attribution_status: row.attribution_status,
            mechanism: row.mechanism.as_deref().map(preview),
            developer_action: preview(&row.developer_action),
            verification: preview(&row.verification),
            caller: &row.caller,
            text_truncated: [&row.victim_summary, &row.developer_action, &row.verification]
                .into_iter()
                .any(|text| truncated(text))
                || row.mechanism.as_deref().is_some_and(truncated),
        };
        value.recount();
        value
    }
    fn recount(&mut self) {
        self.affected_queries_omitted = self.affected_queries_available - self.affected_queries.len();
        self.culprit_queries_omitted = self.culprit_queries_available - self.culprit_queries.len();
        self.candidate_queries_omitted = self.candidate_queries_available - self.candidate_queries.len();
    }
    fn complete(&self) -> bool {
        !self.text_truncated
            && self.entry.complete()
            && self.affected_queries_omitted == 0
            && self.culprit_queries_omitted == 0
            && self.candidate_queries_omitted == 0
            && self
                .affected_queries
                .iter()
                .chain(&self.culprit_queries)
                .chain(&self.candidate_queries)
                .all(Query::complete)
    }
    fn trim(&mut self) -> bool {
        let removed = self.candidate_queries.pop().is_some()
            || self.culprit_queries.pop().is_some()
            || self.affected_queries.pop().is_some();
        self.recount();
        removed
    }
}
#[derive(Serialize)]
struct Query<'a> {
    identity: &'a Option<DerivedQueryIdentity>,
    causal_role: Option<CausalRole>,
    sql: Sql<'a>,
    caller: &'a Option<DerivedCaller>,
    evidence: &'a [DiagnosticEvidenceRef],
    evidence_available: usize,
    evidence_omitted: usize,
}
impl<'a> Query<'a> {
    fn new(row: &'a DerivedIncidentQuery) -> Self {
        Self {
            identity: &row.identity,
            causal_role: row.causal_role.as_ref().map(|role| CausalRole {
                kind: role.kind,
                explanation: preview(role.explanation.text()),
                text_truncated: truncated(role.explanation.text()),
            }),
            sql: Sql::new(&row.sql),
            caller: &row.caller,
            evidence: &row.evidence[..row.evidence.len().min(CONTEXT_LIMIT)],
            evidence_available: row.evidence.len(),
            evidence_omitted: row.evidence.len().saturating_sub(CONTEXT_LIMIT),
        }
    }
    fn complete(&self) -> bool {
        self.evidence_omitted == 0
            && self.sql.complete()
            && self.causal_role.as_ref().is_none_or(|role| !role.text_truncated)
    }
}
#[derive(Serialize)]
struct CausalRole {
    kind: DerivedCausalRoleKind,
    explanation: String,
    text_truncated: bool,
}
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
enum Sql<'a> {
    Available { text: String, sha256: &'a str, redaction_version: u32, text_truncated: bool },
    Unavailable { reason: &'a PayloadUnavailable },
}
impl<'a> Sql<'a> {
    fn new(row: &'a DefinitionPayload) -> Self {
        match row {
            DefinitionPayload::Available { text, sha256, redaction_version } => Self::Available {
                text: preview(text),
                sha256,
                redaction_version: *redaction_version,
                text_truncated: truncated(text),
            },
            DefinitionPayload::Unavailable { reason } => Self::Unavailable { reason },
        }
    }
    fn complete(&self) -> bool {
        !matches!(self, Self::Available { text_truncated: true, .. })
    }
}
fn preview(text: &str) -> String {
    culprit_core::format::escaped_preview(text, TEXT_CHARS).text
}
fn truncated(text: &str) -> bool {
    culprit_core::format::escaped_preview(text, TEXT_CHARS).truncated
}
