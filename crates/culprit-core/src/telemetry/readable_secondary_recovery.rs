//! One original readable-secondary request asks about native recovery; context never implies blame.
use super::*;
use std::collections::BTreeSet;
mod postgres;
pub use postgres::*;
mod mssql;
pub use mssql::*;
mod delta;
pub use delta::*;

pub const MAX_RECOVERY_PROCESSES: usize = 1;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum RecoveryReaderRole {
    GrantedRelationHolder,
    NativeRedoBlocker,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RecoveryReaderEvidence {
    pub session: NativeActivityIdentity,
    pub observed_from: String,
    pub observed_until: String,
    pub role: RecoveryReaderRole,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReadableSecondaryRecoveryReport {
    pub source_state: crate::SourceState,
    pub summary: String,
    pub investigation: String,
    pub verify: String,
    pub readers: Vec<RecoveryReaderEvidence>,
    pub conflict_changes: Vec<RecoveryConflictChange>,
    pub limitations: Vec<String>,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum ReadableSecondaryRecoveryNative {
    Postgres { before: Option<Box<PostgresRecoveryEndpoint>>, after: Option<Box<PostgresRecoveryEndpoint>> },
    Mssql { before: Option<Box<MssqlRecoveryEndpoint>>, after: Option<Box<MssqlRecoveryEndpoint>> },
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReadableSecondaryRecoveryPoint {
    pub context: NativeReadContext,
    pub focus: Box<ActivityRequestFocus>,
    pub observed_at: String,
    pub checked_until: String,
    pub binding: FocusedRequestBinding,
    pub membership: FocusedRequestMembership,
    pub request_end: BlockingRequestState,
    pub native: ReadableSecondaryRecoveryNative,
}
impl ReadableSecondaryRecoveryPoint {
    pub fn is_valid(&self) -> bool {
        let Some(bounds) = ObservationBounds::new(&self.context.observed_from, &self.context.observed_until) else {
            return false;
        };
        self.context.has_qualified_local_owner()
            && self.context.database_writable == Some(false)
            && self.focus.is_valid()
            && self.focus.database_writable == Some(false)
            && self.focus.original.key.request.is_some()
            && matches!(
                (self.binding, self.membership),
                (FocusedRequestBinding::Matched, FocusedRequestMembership::Observed)
                    | (
                        FocusedRequestBinding::ParticipantMissing | FocusedRequestBinding::ParticipantChanged,
                        FocusedRequestMembership::NotObserved
                    )
            )
            && focused_request_original_scope_matches(&self.focus, &self.context)
            && ObservationBounds::new(&self.focus.original_observed_at, &self.context.observed_from).is_some()
            && ObservationBounds::new(&self.observed_at, &self.checked_until).is_some()
            && bounds.contains(&self.observed_at)
            && bounds.contains(&self.checked_until)
            && if self.membership == FocusedRequestMembership::Observed {
                matches!(
                    self.request_end,
                    BlockingRequestState::SameStatement | BlockingRequestState::Missing | BlockingRequestState::Changed
                )
            } else {
                self.request_end == BlockingRequestState::NotRead
            }
            && self.native_valid(&bounds)
    }
    fn native_valid(&self, bounds: &ObservationBounds) -> bool {
        let observed =
            self.membership == FocusedRequestMembership::Observed && self.binding == FocusedRequestBinding::Matched;
        match &self.native {
            ReadableSecondaryRecoveryNative::Postgres { before, after } => {
                self.context.target.engine() == crate::Engine::Postgres
                    && (before.is_some() == after.is_some() && observed == before.is_some())
                    && before.iter().chain(after).all(|point| {
                        point.is_valid()
                            && point.native_epoch().as_deref() == self.context.partition.native_epoch()
                            && bounds.contains(&point.observed_at)
                            && ObservationBounds::new(&self.observed_at, &point.observed_at).is_some()
                            && ObservationBounds::new(&point.observed_at, &self.checked_until).is_some()
                    })
                    && before.as_ref().zip(after.as_ref()).is_none_or(|(a, b)| {
                        a.database_oid == b.database_oid
                            && ObservationBounds::new(&a.observed_at, &b.observed_at).is_some()
                    })
            }
            ReadableSecondaryRecoveryNative::Mssql { before, after } => {
                self.context.target.engine() == crate::Engine::Mssql
                    && (before.is_some() == after.is_some() && observed == before.is_some())
                    && before.iter().chain(after).all(|point| {
                        point.is_valid()
                            && self.focus.statement_identity.qualifiers.get("database_id")
                                == Some(&point.database_id.to_string())
                            && bounds.contains(&point.replica_observed_at)
                            && ObservationBounds::new(&self.observed_at, &point.replica_observed_at).is_some()
                            && bounds.contains(&point.observed_at)
                            && ObservationBounds::new(&self.observed_at, &point.observed_at).is_some()
                            && ObservationBounds::new(&point.observed_at, &self.checked_until).is_some()
                    })
                    && before.as_ref().zip(after.as_ref()).is_none_or(|(a, b)| {
                        a.database_id == b.database_id
                            && a.replica.group_id == b.replica.group_id
                            && a.replica.group_database_id == b.replica.group_database_id
                            && a.replica.replica_id == b.replica.replica_id
                            && ObservationBounds::new(&a.observed_at, &b.observed_at).is_some()
                    })
            }
        }
    }
    /// Native identity/edge witnesses at both endpoints; this does not claim uninterrupted ownership between them.
    pub fn native_blocking_readers(&self) -> Vec<RecoveryReaderEvidence> {
        if !self.is_valid() || self.request_end != BlockingRequestState::SameStatement {
            return vec![];
        }
        let mut readers: Vec<RecoveryReaderEvidence> = match &self.native {
            ReadableSecondaryRecoveryNative::Postgres { before: Some(before), after: Some(after) } => {
                if before.processes_limited || after.processes_limited {
                    return vec![];
                }
                before
                    .processes
                    .iter()
                    .filter_map(|first| {
                        let last = after.processes.iter().find(|last| {
                            last.pid == first.pid && first.started_at.is_some() && first.started_at == last.started_at
                        })?;
                        let edge = first
                            .relation_edges
                            .iter()
                            .find(|edge| first.proves_relation_holder(edge, &self.focus, &before.database_oid))?;
                        let last_edge = last.relation_edges.iter().find(|other| {
                            other.resource == edge.resource
                                && other.requested_mode == edge.requested_mode
                                && last.proves_relation_holder(other, &self.focus, &after.database_oid)
                        })?;
                        Some(RecoveryReaderEvidence {
                            session: last_edge.reader.clone(),
                            observed_from: before.observed_at.clone(),
                            observed_until: after.observed_at.clone(),
                            role: RecoveryReaderRole::GrantedRelationHolder,
                        })
                    })
                    .collect()
            }
            ReadableSecondaryRecoveryNative::Mssql { before: Some(before), after: Some(after) } => {
                if before.requests_limited || after.requests_limited {
                    return vec![];
                }
                before
                    .requests
                    .iter()
                    .filter_map(|first| {
                        let last = after.requests.iter().find(|last| {
                            last.session_id == first.session_id
                                && last.session_generation_without_offset == first.session_generation_without_offset
                                && last.request_id == first.request_id
                                && first.started_without_offset.is_some()
                                && first.started_without_offset == last.started_without_offset
                        })?;
                        (first.reader_is_native_blocker(&self.focus) && last.reader_is_native_blocker(&self.focus))
                            .then(|| RecoveryReaderEvidence {
                                session: last.reader_session.as_ref().unwrap().clone(),
                                observed_from: before.observed_at.clone(),
                                observed_until: after.observed_at.clone(),
                                role: RecoveryReaderRole::NativeRedoBlocker,
                            })
                    })
                    .collect()
            }
            _ => vec![],
        };
        let mut seen = BTreeSet::new();
        readers
            .retain(|reader| seen.insert((reader.session.id.clone(), reader.session.generation.clone(), reader.role)));
        readers
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        serde_json::to_vec(&(
            "readable-secondary-recovery-v1",
            &self.context,
            &self.focus.parent,
            &self.focus.original.key,
            &self.focus.statement_identity,
            &self.observed_at,
        ))
        .map(|bytes| digest(&bytes))
        .map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub fn fits_source(&self, source: &CollectionDescription) -> bool {
        let SourceObservation::SourceRead { snapshot, execution, .. } = &source.observation else { return false };
        self.is_valid()
            && execution.as_ref() == &self.context
            && snapshot.dataset == SnapshotDataset::ReadableSecondaryRecovery
            && snapshot.records == 1
            && source.target == self.context.target
            && source.source == crate::source_id::READABLE_SECONDARY_RECOVERY
    }
    pub fn into_source_read(self, focus: &SourceReadFocus) -> Result<(CollectionDescription, TelemetryRecord)> {
        if !self.is_valid() || focus.readable_secondary_request() != Some(self.focus.as_ref()) {
            return Err(CollectionStop::InvalidPage.error());
        }
        let source = crate::source_id::READABLE_SECONDARY_RECOVERY.to_owned();
        // Selected endpoints are not an exhaustive process population or an absence/recovery verdict.
        let coverage = StateSourceCoverage::observed(source.clone(), 1, false);
        let description = CollectionDescription {
            target: self.context.target.clone(),
            partition: SourcePartition::Target,
            source: source.clone(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::SourceRead {
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::ReadableSecondaryRecovery,
                    observed_from: self.context.observed_from.clone(),
                    observed_until: self.context.observed_until.clone(),
                    records: 1,
                    sources: vec![coverage],
                },
                focus_key: focus.key()?,
                execution: Box::new(self.context.clone()),
            },
        };
        if !self.fits_source(&description) {
            return Err(CollectionStop::InvalidPage.error());
        }
        let record = TelemetryRecord::Operational(OperationalObservation {
            source,
            key: self.key()?,
            fact: OperationalFact::ReadableSecondaryRecovery(Box::new(self)),
        });
        Ok((description, record))
    }
    pub(super) fn redact(&mut self) {
        self.focus.original.redact(self.context.target.engine());
    }
}
fn native_number(value: &str, positive: bool) -> bool {
    value.parse::<u32>().is_ok_and(|n| (!positive || n > 0) && n.to_string() == value)
}
fn bounded_name(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}
