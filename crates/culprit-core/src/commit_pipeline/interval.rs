//! Native counter intervals are independent context, never execution attribution or continuous role proof.
use super::*;
use crate::{MeasurementScope, ResetMarker};

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum CommitPipelineIntervalScope {
    /// Exact shared server/member/process counters; no database or primary/standby role is attributed.
    SharedServerIndependentOfDatabaseRole,
    /// Exact local database and equal known endpoint updateability, not continuous role history.
    LocalDatabase { database_writable: bool },
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineIntervalValue {
    pub kind: CommitPipelineCounterKind,
    pub value: MeasurementValue,
    /// The unchanged native reset for this quantity, never inferred from a process or another counter.
    pub reset: ResetMarker,
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CommitPipelineIntervalMechanism {
    NativeBufferFullWrites,
    NativeLogBufferWaits,
    LocalWriteWork,
    LocalSyncWork,
    NativeFlushWork,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineInterval {
    pub before: DiagnosticEvidenceRef,
    pub after: DiagnosticEvidenceRef,
    pub native_source: String,
    pub identity: NativeIdentity,
    pub measurement_scope: MeasurementScope,
    pub scope_basis: CommitPipelineIntervalScope,
    /// Original acquisition brackets; neither boundary is invented as an exact counter-read instant.
    pub before_observed_from: String,
    pub before_observed_until: String,
    pub after_observed_from: String,
    pub after_observed_until: String,
    pub values: Vec<CommitPipelineIntervalValue>,
    /// Positive native work only. No entry establishes the affected query's cause, cost or absence of delay.
    pub mechanisms: Vec<CommitPipelineIntervalMechanism>,
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CommitPipelineIntervalIssue {
    MissingEndpoint,
    AmbiguousEndpoint,
    IncompleteRead,
    IncompleteSource,
    NativeOwnerChanged,
    ScopeUnavailable,
    DatabaseRoleUnknown,
    DatabaseRoleChanged,
    ObservationOrder,
    UnknownReset,
    ChangedReset,
    ResetAfterObservation,
    CounterMissing,
    CounterMeaningChanged,
    CounterDecreased,
    ValueUnavailable,
    FileCounterIdentityUnavailable,
    PresentationLimited,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineIntervalGap {
    /// Capture authority survives genuine zero-row/failed endpoints; no dummy row is substituted.
    pub before_capture: Option<DiagnosticCaptureRef>,
    pub after_capture: Option<DiagnosticCaptureRef>,
    pub before: Option<DiagnosticEvidenceRef>,
    pub after: Option<DiagnosticEvidenceRef>,
    pub counter: Option<CommitPipelineCounterKind>,
    pub reason: CommitPipelineIntervalIssue,
}
