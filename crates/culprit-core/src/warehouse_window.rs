//! One archive resource/window action. Native sections, published interpretations and missing history remain distinct.
use crate::{
    CollectedQueryWindowAnalysis, DerivedChange, DerivedFinding, IncidentAnalysisContext, IncidentReadReport,
    PublishedResourceCursor, PublishedResourceRequest, PublishedResourceSelector, PublishedResourceWindow, Result,
    collected_analysis::{CollectedEvidenceMetrics, EvidenceGap},
    telemetry::{DeliveryProblem, DiagnosticCaptureRef, TelemetryCollection, canonical_time},
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS: u32 = 1;
pub const MAX_WAREHOUSE_WINDOW_PAGE_ROWS: u32 = 8;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseWindowView {
    /// Already-published answers with exact authenticated lineage; raw workload is not implicitly reconstructed.
    PublishedAnswers,
    /// Full compatible native workload interpretation, within the SAME unchanged action limits.
    CompatibleWorkload,
}

#[derive(Clone, Debug, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseWindowQuery {
    pub resource: PublishedResourceSelector,
    pub window: PublishedResourceWindow,
    /// Independent collector/publication cutoff, never the native end of this workload.
    pub as_of: String,
    pub view: WarehouseWindowView,
    /// Published-analysis captures per page; native workload reconstruction is one bounded whole-window action.
    pub page_rows: u32,
    pub after: Option<PublishedResourceCursor>,
}
impl WarehouseWindowQuery {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        if self.page_rows == 0
            || self.page_rows > MAX_WAREHOUSE_WINDOW_PAGE_ROWS
            || self.view == WarehouseWindowView::CompatibleWorkload && self.after.is_some()
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        PublishedResourceRequest {
            as_of: self.as_of.clone(),
            window: Some(self.window.clone()),
            after: self.after.clone(),
            page_rows: self.page_rows,
        }
        .validate()
    }
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowOriginal {
    pub original: DiagnosticCaptureRef,
    pub collection: TelemetryCollection,
    pub native_clock: WindowNativeClock,
    pub read_complete: bool,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WindowNativeClock {
    Interval { from: String, until: String },
    Observation { from: String, until: String },
    Unknown,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowPublishedAnalysis {
    pub context: IncidentAnalysisContext,
    pub limitations: Vec<String>,
    /// Identical complete published copies remain explicit without multiplying findings or changes.
    pub copies: Vec<DiagnosticCaptureRef>,
    /// Each original analysis keeps its own rules, source/member/epoch, clock and evaluation coverage.
    /// Unknown native placement remains visible separately; it is not silently selected by acquisition time.
    pub native_clock: WindowNativeClock,
    pub native_window_qualified: bool,
    pub rules: WindowPublishedRules,
    pub findings: Vec<WindowPublishedFinding>,
    pub changes: Vec<WindowPublishedChange>,
    pub read_complete: bool,
}
/// One already-authenticated Observation header, retaining its independent native clock.
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowOriginalTime {
    pub original: DiagnosticCaptureRef,
    pub native_clock: WindowNativeClock,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WindowRowTime {
    /// Only the row's exact Observation inputs; catalog/as-of parents are not execution clocks.
    Originals {
        inputs: Vec<WindowOriginalTime>,
    },
    /// The typed event's own UTC clock. Source and uncertain after/until bounds remain on the change.
    /// This is not a native execution clock or a claim that the change caused a nearby symptom.
    RecordedChange {
        at: String,
        after: Option<String>,
        until: Option<String>,
    },
    Unknown,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowRowPlacement {
    pub time: WindowRowTime,
    /// None means an original clock is unknown. False is a qualified clock outside this window.
    /// Overlap selects candidates only and never establishes causality, current health or ownership.
    pub overlaps_window: Option<bool>,
    pub read_complete: bool,
    /// Rendered by the same Core qualification owner; never an evidence or admission discriminator.
    pub qualification: String,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowPublishedFinding {
    pub finding: DerivedFinding,
    pub placement: WindowRowPlacement,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WindowPublishedChange {
    pub change: DerivedChange,
    pub placement: WindowRowPlacement,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WindowPublishedRules {
    MatchesCurrentBuiltin,
    /// A custom/newer/older policy is distinct. Its lexical version is not a freshness order.
    OtherRules {
        expected: String,
    },
}

#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseWindowGap {
    NoPublishedOriginals,
    NoPublishedAnalyses,
    MorePublishedAnalyses,
    PublicationDiscovery { gaps: Vec<crate::PublishedResourceGap> },
    OriginalLimit,
    UnsupportedOriginal { original: DiagnosticCaptureRef },
    OriginalUnavailable { original: DiagnosticCaptureRef, gaps: Vec<EvidenceGap> },
    UnplacedNativeClock { original: DiagnosticCaptureRef },
    RawWithoutPublishedAnalysis { original: DiagnosticCaptureRef },
    UnavailableAnalysisInputs { analysis: DiagnosticCaptureRef },
    IncompletePublishedAnalysis { analysis: DiagnosticCaptureRef },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseWindowReport {
    pub request: WarehouseWindowQuery,
    /// Candidate traversal only. True does not establish configured fleet, full native population or health.
    pub discovery_complete: bool,
    pub originals: Vec<WindowOriginal>,
    pub workload: WindowWorkload,
    pub next: Option<PublishedResourceCursor>,
    pub incidents: IncidentReadReport,
    pub incident_limitations: Vec<crate::WarehouseAnalysisLimitations>,
    pub published: Vec<WindowPublishedAnalysis>,
    pub gaps: Vec<WarehouseWindowGap>,
    pub archive_work: CollectedEvidenceMetrics,
    /// Shared source-owned wording; typed gaps and original authority remain separately available.
    pub qualifications: Vec<String>,
}

impl WindowNativeClock {
    pub fn from_original(source: &crate::telemetry::CollectionDescription) -> Self {
        let (bounds, interval) = if let Some(history) = source.query_history() {
            (Some((&history.interval.from, &history.interval.until)), true)
        } else if let Some(snapshot) = source.observation.snapshot() {
            (Some((&snapshot.observed_from, &snapshot.observed_until)), false)
        } else if let Some(activity) = source.observation.activity() {
            (Some((&activity.observed_from, &activity.observed_until)), false)
        } else {
            (None, false)
        };
        let Some((from, until)) = bounds else { return WindowNativeClock::Unknown };
        match (canonical_time(from), canonical_time(until)) {
            (Ok(from), Ok(until)) if interval => WindowNativeClock::Interval { from, until },
            (Ok(from), Ok(until)) => WindowNativeClock::Observation { from, until },
            _ => WindowNativeClock::Unknown,
        }
    }

    pub fn overlaps(&self, window: &PublishedResourceWindow) -> Result<Option<bool>> {
        let (from, until) = match self {
            Self::Interval { from, until } | Self::Observation { from, until } => (from, until),
            Self::Unknown => return Ok(None),
        };
        let parse = |value: &str| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .filter(|value| value.offset().local_minus_utc() == 0)
                .ok_or_else(|| DeliveryProblem::Integrity.error())
        };
        let (from, until, start, end) = (parse(from)?, parse(until)?, parse(&window.from)?, parse(&window.until)?);
        if from > until || start >= end {
            return Err(DeliveryProblem::Integrity.error());
        }
        Ok(Some(if from == until { from >= start && from < end } else { from < end && until > start }))
    }
}

#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WindowWorkload {
    NotRequested,
    Evaluated { analysis: Box<CollectedQueryWindowAnalysis> },
}
impl WindowWorkload {
    pub fn analysis(&self) -> Option<&CollectedQueryWindowAnalysis> {
        match self {
            Self::NotRequested => None,
            Self::Evaluated { analysis } => Some(analysis),
        }
    }
}

mod presentation;
