//! Host registration and bounded, source-free automatic work. None of these values admits native SQL.
use super::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const SYNTHESIS_CONTEXT_POLICY_VERSION: u32 = 1;
pub const MAX_SYNTHESIS_CONTEXT_CAPTURES: usize = 16;
pub const MAX_PENDING_SYNTHESIS_JOBS: u32 = 128;
pub const MAX_SYNTHESIS_CATALOG_HEADS: u32 = 64;
pub const MAX_SYNTHESIS_CATALOG_VERSIONS: u32 = 2;
pub const MAX_SYNTHESIS_PLAN_BYTES: usize = 64 * 1024;
pub const MAX_SYNTHESIS_METADATA_BYTES: u64 = 4 * 1024 * 1024;
pub const SYNTHESIS_BACKFILL_PAGE: u32 = 32;

/// The immutable identity of one installed pure analyzer and its local context-selection policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SynthesisRegistration {
    pub analyzer: ComponentVersion,
    pub rules_version: String,
    pub context_policy: u32,
}
impl SynthesisRegistration {
    pub fn validate(&self) -> Result<()> {
        if !label(&self.analyzer.component)
            || self.analyzer.version.as_deref().is_none_or(|value| !label(value))
            || self.analyzer.revision.as_deref().is_some_and(|value| !label(value))
            || !label(&self.rules_version)
            || self.context_policy != SYNTHESIS_CONTEXT_POLICY_VERSION
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }
    pub fn key(&self) -> Result<String> {
        self.validate()?;
        crate::synthesis::synthesis_metadata_key(&("automatic-synthesis-policy-v1", self))
    }
    pub(crate) fn matches(&self, synthesizer: &dyn Synthesizer) -> bool {
        self.analyzer == synthesizer.analyzer() && self.rules_version == synthesizer.rules_version()
    }
}
fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

/// The host retains this callback owner after installing it before the first native reservation.
/// A worker never discovers an analyzer, a database connection, or credentials.
pub struct AutomaticSynthesis {
    registration: SynthesisRegistration,
    synthesizer: Arc<dyn Synthesizer>,
    budget: CompletedCaptureBudget,
}
impl AutomaticSynthesis {
    pub fn new(synthesizer: Arc<dyn Synthesizer>, budget: CompletedCaptureBudget) -> Result<Self> {
        budget.validate()?;
        let registration = SynthesisRegistration {
            analyzer: synthesizer.analyzer(),
            rules_version: synthesizer.rules_version().into(),
            context_policy: SYNTHESIS_CONTEXT_POLICY_VERSION,
        };
        registration.validate()?;
        Ok(Self { registration, synthesizer, budget })
    }
    pub fn registration(&self) -> &SynthesisRegistration {
        &self.registration
    }
    pub(crate) fn budget(&self) -> CompletedCaptureBudget {
        self.budget
    }
    pub(crate) fn analyzer(&self) -> Result<Arc<dyn Synthesizer>> {
        if !self.registration.matches(self.synthesizer.as_ref()) {
            return Err(DeliveryProblem::Conflict.error());
        }
        Ok(self.synthesizer.clone())
    }
}

#[derive(Clone, Debug)]
pub enum SynthesisTurn {
    Idle,
    Deferred { reason: SynthesisDeferral },
    Committed { primary: DiagnosticCaptureRef, output: DiagnosticCaptureRef, queue_retired: bool },
    Failed { primary: DiagnosticCaptureRef, code: culprit_core::ErrorCode },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SynthesisDeferral {
    NativeCaptureOwned,
    QueueBackpressure,
    PolicyMismatch,
    LocalBudget,
}

/// Local queue health, independent of native source health or the clocks represented by retained rows.
#[derive(Clone, Debug)]
pub struct SynthesisBacklog {
    pub pending: u32,
    pub failed: u32,
    pub oldest_pending: Option<SynthesisQueuedCapture>,
    pub failure_watermark: SynthesisFailureWatermark,
}
#[derive(Clone, Debug)]
pub struct SynthesisQueuedCapture {
    pub primary: DiagnosticCaptureRef,
    /// UTC enqueue time. It is never an original database observation or event timestamp.
    pub queued_at: String,
}

/// Constant-size durable failure memory. Policy replacement and original eviction never clear a known loss.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SynthesisFailureWatermark {
    pub total: u64,
    pub latest: Option<SynthesisFailure>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SynthesisFailure {
    pub primary: DiagnosticCaptureRef,
    pub code: culprit_core::ErrorCode,
    /// UTC local failure settlement time, distinct from every original/native observation clock.
    pub failed_at: String,
}
impl SynthesisFailureWatermark {
    pub fn is_valid(&self) -> bool {
        (self.total == 0 && self.latest.is_none())
            || (self.total > 0
                && self.total <= i64::MAX as u64
                && self.latest.as_ref().is_some_and(|failure| {
                    failure.primary.is_valid()
                        && canonical_time(&failure.failed_at).is_ok_and(|time| time == failure.failed_at)
                }))
    }
}
