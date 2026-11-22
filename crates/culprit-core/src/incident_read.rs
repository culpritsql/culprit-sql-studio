//! Bounded incident discovery is an authenticated historical read, never current native action authority.
use crate::{
    DerivedIncident, IncidentDatabaseSelector, IncidentHistoryReport,
    collected_analysis::{CollectedEvidenceMetrics, EvidenceGap},
    telemetry::{DiagnosticCaptureRef, DiagnosticEvidenceRef},
};
use serde::Serialize;
use ts_rs::TS;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, TS)]
#[serde(tag = "state", rename_all = "kebab-case")]
#[ts(export)]
pub enum IncidentEvidenceAvailability {
    Authenticated,
    /// No row is invented from a public key or a neighboring capture. These are archive read gaps only.
    Unavailable {
        gaps: Vec<EvidenceGap>,
    },
    LimitReached,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct IncidentEvidenceRelationship {
    pub revision: DiagnosticEvidenceRef,
    pub role: crate::DiagnosticAncestryRole,
    pub capture: DiagnosticCaptureRef,
    pub reference: Option<DiagnosticEvidenceRef>,
    /// Historical input catalog clock, never an assignment of current ownership or time basis.
    pub catalog: Option<crate::DerivedCatalogProvenance>,
    pub availability: IncidentEvidenceAvailability,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum IncidentReadGap {
    CandidateLimit,
    CandidateClockBoundary { range_id: String, batch_id: String, row_ordinal: u32 },
    EpisodeLimit,
    CaptureLimit,
    CandidateUnavailable { range_id: String, batch_id: String, row_ordinal: u32, gaps: Vec<EvidenceGap> },
    CaptureUnavailable { capture: DiagnosticCaptureRef, gaps: Vec<EvidenceGap> },
    OriginalUnavailable { reference: Box<DiagnosticEvidenceRef>, gaps: Vec<EvidenceGap> },
    PredecessorNotSelected { episode_id: String, controller_version: u32, revision: String },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct IncidentAnalysisContext {
    pub capture: DiagnosticCaptureRef,
    pub description: crate::DerivedDiagnosisDescription,
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct IncidentReadReport {
    /// Route-only candidate selector. It is not a historical database incarnation/member identity.
    pub database: IncidentDatabaseSelector,
    pub from: String,
    pub until: String,
    /// Fixed discovery cutoff, not a native source watermark or complete-history claim.
    pub recorded_before: String,
    /// Completion of this bounded Gold/ancestry read; never complete native history or population coverage.
    pub discovery_complete: bool,
    /// The same canonical derived contract used by synthesis, CLI, MCP and desktop.
    pub incidents: Vec<DerivedIncident>,
    /// Original synthesis metadata preserves independent input completeness, clocks and rules per capture.
    pub analyses: Vec<IncidentAnalysisContext>,
    pub history: IncidentHistoryReport,
    /// These links exist only because immutable native/capture receipts name their endpoints. Time adds no edge.
    pub relationships: Vec<IncidentEvidenceRelationship>,
    pub gaps: Vec<IncidentReadGap>,
    /// Existing warehouse/selected-payload metrics. Per-object recovery metadata transport retains its own bounds;
    /// this is not a complete measurement of remote CPU/I/O or native source cost.
    pub archive_work: CollectedEvidenceMetrics,
    pub candidate_rows: u32,
}
