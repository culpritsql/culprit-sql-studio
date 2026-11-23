//! Native commit-stage points and independent original context; none is a current cause or a workload rate.
use crate::{ReplicationEntry, SourceState, telemetry::*};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const MAX_COMMIT_PIPELINE_FACTS: usize = 64;
mod interval;
pub use interval::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum CommitPipelineStage {
    LogBufferSpace,
    LocalLogFlush,
    LocalLogWrite,
    LocalLogSync,
    LogSpaceReuse,
    SegmentInitializationWrite,
    SegmentInitializationSync,
    /// Send/write/harden/ack cannot be separated by the wait alone. Replay is not inferred.
    SynchronousAcknowledgement,
    SynchronousHardeningAcknowledgement,
    /// The provider's native wait class establishes governance; a limit alone does not.
    RateGovernance {
        component: Option<LogRateGovernanceReason>,
    },
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CommitPipelineCounterKind {
    GeneratedBytes,
    LogRecords,
    FullPageImages,
    BufferFullWrites,
    WrittenBytes,
    WriteOperations,
    SyncOperations,
    WriteTime,
    SyncTime,
    FlushOperations,
    FlushWaitTime,
    LogBufferWaits,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineCounter {
    pub kind: CommitPipelineCounterKind,
    /// Exact native value/unit/nature/reset/object/instance/type; no cooked rate or ratio.
    pub sample: NativeCounterSample,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineOriginal {
    pub reference: DiagnosticEvidenceRef,
    /// This original's actual acquisition bounds/owner, not a historical query's executor.
    pub context: NativeReadContext,
    pub source_state: SourceState,
    pub complete_source_scope: bool,
    pub read_complete: bool,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum CommitPipelineFact {
    Counters {
        original: CommitPipelineOriginal,
        values: Vec<CommitPipelineCounter>,
    },
    FileIo {
        original: CommitPipelineOriginal,
        point: Box<FileIoCounters>,
    },
    Allocation {
        original: CommitPipelineOriginal,
        point: Box<OperationalSpace>,
    },
    CatalogLogFile {
        original: CommitPipelineOriginal,
        point: Box<MssqlDatabaseFile>,
    },
    /// One selected native transaction's lifetime totals, not a producer roster or rate.
    Transaction {
        original: CommitPipelineOriginal,
        point: Box<MssqlCleanupTransaction>,
    },
    Replica {
        original: CommitPipelineOriginal,
        entry: Box<ReplicationEntry>,
    },
    Maintenance {
        original: CommitPipelineOriginal,
        point: Box<MaintenanceProgressObservation>,
    },
    ConfiguredRates {
        original: CommitPipelineOriginal,
        point: Box<ConfiguredLogRateLimits>,
    },
    HyperscaleComponent {
        original: CommitPipelineOriginal,
        point: Box<HyperscaleLogComponent>,
    },
}
impl CommitPipelineFact {
    pub fn original(&self) -> &CommitPipelineOriginal {
        match self {
            Self::Counters { original, .. }
            | Self::FileIo { original, .. }
            | Self::Allocation { original, .. }
            | Self::CatalogLogFile { original, .. }
            | Self::Transaction { original, .. }
            | Self::Replica { original, .. }
            | Self::Maintenance { original, .. }
            | Self::ConfiguredRates { original, .. }
            | Self::HyperscaleComponent { original, .. } => original,
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum CommitPipelineIncrease {
    TransactionLogBytes {
        session: NativeActivityIdentity,
        transaction: NativeActivityIdentity,
        before: Box<DiagnosticEvidenceRef>,
        after: Box<DiagnosticEvidenceRef>,
        bytes: String,
        actor: Option<Box<CommitPipelineProducerActor>>,
    },
    /// A complete selected-DB catalog allocation sum increased. Not an autogrowth event or effective capacity.
    CatalogLogAllocationBytes { before: DiagnosticCaptureRef, after: DiagnosticCaptureRef, bytes: String },
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineProducerActor {
    pub actor: ActivityKey,
    pub reference: DiagnosticEvidenceRef,
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CommitPipelineContextBinding {
    SameNativeOwner,
    SharedNativeOwner,
    Unqualified,
    Foreign,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineCoverage {
    pub original: DiagnosticCaptureRef,
    pub context: Option<NativeReadContext>,
    pub binding: CommitPipelineContextBinding,
    pub sources: Vec<StateSourceCoverage>,
    pub read_complete: bool,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CommitPipelineReport {
    pub original: DiagnosticEvidenceRef,
    pub context: NativeReadContext,
    pub observed_at: String,
    pub source_state: SourceState,
    pub read_complete: bool,
    pub native_rows: u32,
    pub observed_rows: u32,
    pub actor: ActivityKey,
    pub statement: Option<NativeIdentity>,
    pub native_wait: Option<String>,
    pub wait_ms: Option<String>,
    /// None withholds a supported commit-stage conclusion; it is not a healthy verdict.
    pub stage: Option<CommitPipelineStage>,
    pub inputs: Vec<CommitPipelineCoverage>,
    pub facts: Vec<CommitPipelineFact>,
    pub increases: Vec<CommitPipelineIncrease>,
    pub intervals: Vec<CommitPipelineInterval>,
    pub interval_gaps: Vec<CommitPipelineIntervalGap>,
    pub context_summaries: Vec<String>,
    pub examined_rows: u32,
    pub facts_limited: bool,
    pub title: String,
    pub summary: String,
    pub mechanism: Option<String>,
    pub investigation: String,
    pub verify: String,
    pub limitations: Vec<String>,
}

#[cfg(test)]
mod tests;
