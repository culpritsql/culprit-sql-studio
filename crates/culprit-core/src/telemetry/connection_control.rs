//! Collector-owned outcomes of existing configured-route attempts. These are never native availability samples.
use super::*;
use crate::{Engine, ErrorCode};

pub const COLLECTOR_CONNECTION_SOURCE: &str = "collector-connection-control";
pub const MAX_CONNECTION_CONTROL_BYTES: usize = 64 * 1024;

/// Frozen saved metadata only. This target establishes no database, replica, session or native member identity.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ConfiguredConnectionIdentity {
    pub id: String,
    pub route_revision: String,
    pub name: String,
    pub engine: Engine,
}
impl ConfiguredConnectionIdentity {
    pub fn new(namespace: &str, route_revision: &str, name: &str, engine: Engine) -> Result<Self> {
        let id = digest(
            &serde_json::to_vec(&("configured-collector-connection-v1", namespace, name, engine))
                .map_err(|_| invalid())?,
        );
        let target = Self { id, route_revision: route_revision.into(), name: name.into(), engine };
        if namespace.is_empty() || namespace.len() > 128 || !target.is_valid() {
            return Err(invalid());
        }
        Ok(target)
    }
    pub fn is_valid(&self) -> bool {
        valid_digest(&self.id)
            && valid_digest(&self.route_revision)
            && !self.name.trim().is_empty()
            && self.name.len() <= 256
            && !self.name.chars().any(char::is_control)
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum CollectorCaptureOutcome {
    /// The API returned a manifest; its original status is retained independently.
    Returned,
    Failed {
        code: ErrorCode,
    },
    /// The process did not retain the original API result. Recovery must not reconstruct it from a stop label.
    Unknown,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CollectorConnectionOutcome {
    Success,
    Failure,
    Unknown,
}

/// An opaque link copied only from the same original authenticated native description, never a new admission proof.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CollectorConnectionNativeLink {
    pub resource_id: String,
    pub source_scope_id: String,
}
impl CollectorConnectionNativeLink {
    pub fn from_original(source: &CollectionDescription) -> Result<Option<Self>> {
        if !source.partition_is_consistent()
            || !source.is_partition_qualified()
            || matches!(source.target, CollectionTarget::ConfiguredConnection(_))
            || source
                .target
                .database()
                .is_some_and(|database| database.incarnation.as_deref().is_none_or(str::is_empty))
        {
            return Ok(None);
        }
        Ok(Some(Self {
            resource_id: source.target.telemetry_id()?,
            source_scope_id: source.captured_range()?.scope_id,
        }))
    }
    fn is_valid(&self) -> bool {
        valid_digest(&self.resource_id) && valid_digest(&self.source_scope_id)
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CollectorConnectionDescription {
    pub attempt: CaptureIdentity,
    /// Hash of the exact frozen collector control row, not a native source/body receipt.
    pub snapshot_sha256: String,
    pub observed_from: String,
    pub observed_until: String,
}
impl CollectorConnectionDescription {
    pub fn is_valid(&self) -> bool {
        self.attempt.is_valid()
            && self.attempt.revision.is_some()
            && valid_digest(&self.snapshot_sha256)
            && canonical_time(&self.observed_from).is_ok()
            && canonical_time(&self.observed_until).is_ok()
    }
    pub fn matches_source(&self, source: &CollectionDescription) -> bool {
        self.is_valid()
            && source.source == COLLECTOR_CONNECTION_SOURCE
            && matches!(&source.target, CollectionTarget::ConfiguredConnection(target) if target.is_valid())
            && source.partition == SourcePartition::Target
            && source.continuity == SourceContinuity::Unverified
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum CollectorConnectionOriginalEvidence {
    NoNativeEvidence,
    MayContainNativeEvidence,
    ManifestUnavailable,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CollectorConnectionObservation {
    /// Original allocated attempt. A missing digest explicitly means no terminal native manifest was sealed.
    pub original: DiagnosticAttemptReceipt,
    pub original_state: OutboxCaptureState,
    pub original_status: Option<CollectionStatus>,
    pub original_evidence: CollectorConnectionOriginalEvidence,
    pub source_work: SourceWorkState,
    pub collector_outcome: CollectorCaptureOutcome,
    pub collector_started_at: String,
    /// Original terminal/recovery observation clock; not a server timestamp or outage onset.
    pub collector_finished_at: String,
    /// Original monotonic API lifetime only. None after interruption; never subtract wall clocks to fill it.
    pub collector_elapsed_ms: Option<f64>,
    /// Existing bounded recorder. Missing instrumentation remains unknown, never a successful zero-work attempt.
    pub connections: Option<ReadConnectionWork>,
    pub original_source_queries: Option<u32>,
    pub native: Option<CollectorConnectionNativeLink>,
}
impl CollectorConnectionObservation {
    pub fn is_valid(&self) -> bool {
        self.original.is_valid()
            && self.original.capture.revision.is_some()
            && !matches!(self.original_state, OutboxCaptureState::Capturing | OutboxCaptureState::Evicting)
            && canonical_time(&self.collector_started_at).is_ok()
            && canonical_time(&self.collector_finished_at).is_ok()
            && self.collector_elapsed_ms.is_none_or(|ms| ms.is_finite() && ms >= 0.0)
            && (self.original.manifest_sha256.is_some() == self.original_status.is_some())
            && self.connections.as_ref().is_none_or(ReadConnectionWork::is_valid)
            && self.native.as_ref().is_none_or(CollectorConnectionNativeLink::is_valid)
            && match self.original_evidence {
                CollectorConnectionOriginalEvidence::ManifestUnavailable => self.original.manifest_sha256.is_none(),
                CollectorConnectionOriginalEvidence::NoNativeEvidence => {
                    self.original.manifest_sha256.is_some() && self.native.is_none()
                }
                CollectorConnectionOriginalEvidence::MayContainNativeEvidence => {
                    self.original.manifest_sha256.is_some()
                }
            }
            && (self.original.manifest_sha256.is_some()
                || (self.original_status.is_none()
                    && self.connections.is_none()
                    && self.original_source_queries.is_none()
                    && self.native.is_none()))
            && (self.collector_outcome != CollectorCaptureOutcome::Unknown || self.collector_elapsed_ms.is_none())
            && !(self.original_state == OutboxCaptureState::Ready && self.original.manifest_sha256.is_none())
    }
    pub fn snapshot_sha256(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(invalid());
        }
        Ok(digest(&serde_json::to_vec(self).map_err(|_| invalid())?))
    }
    pub fn fits_source(&self, source: &CollectionDescription) -> bool {
        matches!(&source.observation, SourceObservation::CollectorConnection(header)
            if header.matches_source(source) && header.attempt == self.original.capture
                && header.observed_from == self.collector_started_at && header.observed_until == self.collector_finished_at
                && self.snapshot_sha256().ok().as_ref() == Some(&header.snapshot_sha256))
    }
    /// This describes Culprit's recorded connection work, never native health or the configured fleet's availability.
    pub fn connection_outcome(&self) -> CollectorConnectionOutcome {
        let Some(work) = &self.connections else { return CollectorConnectionOutcome::Unknown };
        if !self.is_valid() || self.source_work != SourceWorkState::Quiescent || work.has_unconfirmed_work() {
            return CollectorConnectionOutcome::Unknown;
        }
        let phases = || work.attempts.iter().flat_map(|attempt| &attempt.phases);
        if phases().any(|phase| matches!(phase.outcome, ReadConnectionOutcome::Failed { .. })) {
            return CollectorConnectionOutcome::Failure;
        }
        if !work.has_complete_observation_roster()
            || work.attempts.is_empty()
            || phases().any(|phase| matches!(phase.outcome, ReadConnectionOutcome::Stopped { .. }))
        {
            return CollectorConnectionOutcome::Unknown;
        }
        if work.attempts.iter().all(|attempt| {
            attempt.reader_generation.is_some()
                && attempt.connection_use != ReadConnectionUse::Unknown
                && attempt.phases.first().is_some_and(|phase| phase.outcome == ReadConnectionOutcome::Completed)
        }) && (phases().any(|phase| {
            phase.stage == ReadConnectionStage::FirstQuery && phase.outcome == ReadConnectionOutcome::Completed
        }) || (self.original_status == Some(CollectionStatus::Complete)
            && self.collector_outcome == CollectorCaptureOutcome::Returned))
        {
            CollectorConnectionOutcome::Success
        } else {
            CollectorConnectionOutcome::Unknown
        }
    }
    /// Wall-clock regressions stay visible; monotonic durations remain their original independent evidence.
    pub fn ordered_collector_clocks(&self) -> bool {
        ObservationBounds::new(&self.collector_started_at, &self.collector_finished_at).is_some()
            && self.connections.as_ref().is_none_or(|work| {
                work.attempts
                    .iter()
                    .flat_map(|attempt| &attempt.phases)
                    .all(ReadConnectionPhase::has_ordered_client_clocks)
            })
    }
}
fn invalid() -> crate::Error {
    CollectionStop::InvalidPage.error()
}

#[cfg(test)]
mod tests;
