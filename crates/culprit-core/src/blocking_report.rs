//! A focused native closure explains its own current frame separately from its original incident binding.
use crate::{Blocking, MaintenanceReport, SourceState, telemetry::*};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[ts(export)]
pub struct BlockingReport {
    pub source: String,
    pub context: NativeReadContext,
    pub source_state: SourceState,
    pub source_reason: Option<String>,
    pub read_complete: bool,
    pub native_rows: u32,
    pub observed_rows: u32,
    pub original: DiagnosticEvidenceRef,
    pub binding: FocusedRequestBinding,
    pub membership: FocusedRequestMembership,
    pub request_end: BlockingRequestState,
    pub closure: BlockingClosureDescription,
    pub relationships: Vec<BlockingObservation>,
    pub blocking: Option<Box<Blocking>>,
    pub maintenance: Option<Box<MaintenanceReport>>,
    pub metadata_queue: MetadataQueueReport,
    pub title: String,
    pub summary: String,
    pub limitations: Vec<String>,
    pub investigation: String,
    pub verify: String,
}

/// The native proof and its wording remain separate from the general blocking graph and from current action authority.
#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[ts(export)]
pub struct MetadataQueueReport {
    pub evidence: crate::blocking::MetadataQueueEvidence,
    pub summary: String,
    pub explanations: Vec<MetadataQueueExplanation>,
    pub limitations: Vec<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[ts(export)]
pub struct MetadataQueueExplanation {
    pub key: String,
    pub victim_summary: String,
    pub mechanism: String,
    pub investigation: String,
    pub verify: String,
}
