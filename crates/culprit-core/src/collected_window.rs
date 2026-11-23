//! Selected native windows retain independent source/population/generation and each original measurement.
//! A joined section is a derived workload, never a manufactured native capture or current action authority.
use crate::{
    DerivedContextIssue, DerivedInputReference, PublishedResourceWindow,
    collected_analysis::{CollectedQueryAnalysisResult, CollectedQueryInput, CollectedQueryPopulation},
    telemetry::{DiagnosticCaptureRef, TelemetryInterval},
};
use serde::Serialize;
use ts_rs::TS;

pub const MAX_COLLECTED_WINDOW_INPUTS: usize = 512;
pub const MAX_COLLECTED_WINDOW_ROWS: usize = 100_000;
pub const MAX_COLLECTED_WINDOW_INPUT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct CollectedWindowInput {
    pub original: DiagnosticCaptureRef,
    pub interval: TelemetryInterval,
    pub runtime_rows: Option<u32>,
    pub wait_rows: Option<u32>,
    pub read_complete: bool,
    /// Original cohort, outcome, replica and fragment keys remain attached to their own capture.
    pub queries: Vec<CollectedQueryInput>,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct CollectedWindowSection {
    pub analysis_id: String,
    pub scope_id: String,
    /// Original namespace/member generation is known. This grants no current/native read authority.
    pub native_scope_qualified: bool,
    pub population: CollectedQueryPopulation,
    /// Whole native intervals; an edge is never prorated to the requested time.
    pub observed: PublishedResourceWindow,
    pub inputs: Vec<CollectedWindowInput>,
    pub catalog_inputs: Vec<DerivedInputReference>,
    pub context_issues: Vec<DerivedContextIssue>,
    pub analysis: CollectedQueryAnalysisResult,
    /// Whole, compatible native periods use the same original statistics and existing change rules.
    pub comparison: CollectedWindowComparison,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum CollectedWindowGap {
    NoNativeIntervals,
    UnqualifiedScope {
        original: DiagnosticCaptureRef,
    },
    UnknownPopulation {
        original: DiagnosticCaptureRef,
    },
    IncompleteOriginal {
        original: DiagnosticCaptureRef,
    },
    AmbiguousRevision {
        originals: Vec<DiagnosticCaptureRef>,
    },
    OverlappingIntervals {
        originals: Vec<DiagnosticCaptureRef>,
    },
    SectionDoesNotCoverWindow {
        analysis_id: String,
        scope_id: String,
        population: CollectedQueryPopulation,
        window: PublishedResourceWindow,
    },
    NativeBoundaryExpanded {
        original: DiagnosticCaptureRef,
    },
    ConflictingDefinition {
        originals: Vec<DiagnosticCaptureRef>,
    },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct CollectedWindowSupersession {
    pub previous: DiagnosticCaptureRef,
    pub selected: DiagnosticCaptureRef,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct CollectedQueryWindowAnalysis {
    pub rules_version: String,
    pub requested: PublishedResourceWindow,
    /// No joint total across sections. Each carries its own compatible source/native execution population.
    pub sections: Vec<CollectedWindowSection>,
    pub superseded: Vec<CollectedWindowSupersession>,
    pub gaps: Vec<CollectedWindowGap>,
}

#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum CollectedComparisonGap {
    TooFewIntervals,
    UnqualifiedScope,
    UnknownPopulation,
    IncompleteOriginal,
    IncompleteQueryProjection,
    MissingRuntimeDenominator,
    NativeBoundarySelected { requested: crate::changes::Period, selected: crate::changes::Period },
    QueryMissingFromPeriod { query_id: String },
    PlanMixUnestablished { query_id: String },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct CollectedWindowComparison {
    pub rules_version: String,
    pub baseline_inputs: Vec<DiagnosticCaptureRef>,
    pub recent_inputs: Vec<DiagnosticCaptureRef>,
    pub changes: Option<crate::Changes>,
    pub gaps: Vec<CollectedComparisonGap>,
}

impl CollectedComparisonGap {
    pub fn explanation(&self) -> String {
        match self {
            Self::TooFewIntervals => crate::t!("window.comparison.too_few"),
            Self::UnqualifiedScope => crate::t!("window.comparison.scope"),
            Self::UnknownPopulation => crate::t!("window.comparison.population"),
            Self::IncompleteOriginal => crate::t!("window.comparison.original"),
            Self::IncompleteQueryProjection => crate::t!("window.comparison.projection"),
            Self::MissingRuntimeDenominator => crate::t!("window.comparison.denominator"),
            Self::NativeBoundarySelected { .. } => crate::t!("window.comparison.boundary"),
            Self::QueryMissingFromPeriod { .. } => crate::t!("window.comparison.unobserved_query"),
            Self::PlanMixUnestablished { .. } => crate::t!("window.comparison.plan_mix"),
        }
    }
}
