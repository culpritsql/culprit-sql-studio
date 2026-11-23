//! One selected, still-current request's operator/thread point. Native bigint counters stay exact.
//! Task/worker associations are checked at their own endpoints; a partial profile is never a whole query.
use super::*;
use std::collections::BTreeSet;
mod operators;
pub use operators::*;
mod relational_inputs;
pub use relational_inputs::*;
mod input_rows;
pub use input_rows::*;
mod output_rows;
pub use output_rows::*;
mod evidence;
pub use evidence::*;
mod operator_context;
pub use operator_context::*;

/// Documented native exchange codes, separate from an adverse verdict or a physical-I/O/data-skew claim.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlParallelExchangeWait {
    Producer,
    Consumer,
    PortSynchronization,
    ConsumerSynchronization,
    RowsetSynchronization,
}
impl MssqlParallelExchangeWait {
    pub fn from_native(native: &str) -> Option<Self> {
        match native {
            "CXPACKET" => Some(Self::Producer),
            "CXCONSUMER" => Some(Self::Consumer),
            "CXSYNC_PORT" => Some(Self::PortSynchronization),
            "CXSYNC_CONSUMER" => Some(Self::ConsumerSynchronization),
            "CXROWSET_SYNC" => Some(Self::RowsetSynchronization),
            _ => None,
        }
    }
}

pub const MAX_EXECUTION_PROFILE_ROWS: usize = 512;
/// Presentation only; every native node/thread remains durable in the original point.
pub const MAX_PRESENTED_EXECUTION_NODES: usize = 16;
pub const MAX_EXECUTION_PROFILE_PREDICATE_BYTES: usize = 4096;

/// NULL and signed native bigint values remain distinct from an interpretable nonnegative count/duration.
#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlOperatorThreadCounters {
    #[serde(deserialize_with = "Option::deserialize")]
    pub rows_returned: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub rows_read: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub estimated_rows: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub elapsed_ms: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub cpu_ms: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub logical_reads: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub physical_reads: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub read_ahead_reads: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub spill_write_pages: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub end_of_scans: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub scans: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub rebinds: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub rewinds: Option<String>,
}
impl MssqlOperatorThreadCounters {
    pub fn is_valid(&self) -> bool {
        [
            &self.rows_returned,
            &self.rows_read,
            &self.estimated_rows,
            &self.elapsed_ms,
            &self.cpu_ms,
            &self.logical_reads,
            &self.physical_reads,
            &self.read_ahead_reads,
            &self.spill_write_pages,
            &self.end_of_scans,
            &self.scans,
            &self.rebinds,
            &self.rewinds,
        ]
        .into_iter()
        .flatten()
        .all(|value| native_integer(value))
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlOperatorThreadProfile {
    pub node_id: u32,
    pub thread_id: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    pub task_address: Option<String>,
    pub physical_operator: String,
    pub counters: MssqlOperatorThreadCounters,
    /// Native tick origin is not UTC. A close tick alone cannot establish completion of absent siblings.
    #[serde(deserialize_with = "Option::deserialize")]
    pub open_ticks: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub close_ticks: Option<String>,
    /// Source-reported operator context. It does not establish current catalog ownership or incarnation.
    #[serde(deserialize_with = "Option::deserialize")]
    pub database_id: Option<i16>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub object_id: Option<i32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub index_id: Option<i32>,
}
impl MssqlOperatorThreadProfile {
    pub fn is_valid(&self) -> bool {
        self.node_id <= i32::MAX as u32
            && self.thread_id <= i32::MAX as u32
            && self.task_address.as_deref().is_none_or(native_address)
            && !self.physical_operator.is_empty()
            && self.physical_operator.encode_utf16().count() <= 256
            && !self.physical_operator.chars().any(char::is_control)
            && self.counters.is_valid()
            && [&self.open_ticks, &self.close_ticks].into_iter().flatten().all(|value| native_integer(value))
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlSelectedWorkerPoint {
    pub observed_at: String,
    pub tasks: Vec<MssqlWorkerTask>,
    pub limited: bool,
}
impl MssqlSelectedWorkerPoint {
    fn is_valid(&self, actor: &ActivityKey) -> bool {
        let mut addresses = BTreeSet::new();
        let mut workers = BTreeSet::new();
        canonical_time(&self.observed_at).is_ok()
            && self.tasks.len() <= MAX_WORKER_TASKS
            && self.tasks.iter().all(|task| {
                task.is_valid()
                    && addresses.insert(&task.address)
                    && task.assigned_worker().is_none_or(|worker| workers.insert(&worker.address))
                    && task.session_id.map(|value| value.to_string()).as_deref() == Some(actor.session.id.as_str())
                    && task.request_id.map(|value| value.to_string()).as_deref()
                        == actor.request.as_ref().map(|request| request.id.as_str())
            })
    }
    /// Equal addresses alone never identify an assigned worker or its request generation.
    pub fn same_assigned_task<'a>(
        &'a self,
        other: &'a Self,
        actor: &ActivityKey,
        address: &str,
    ) -> Option<&'a MssqlWorkerTask> {
        let before = self.tasks.iter().find(|task| task.address == address)?;
        let after = other.tasks.iter().find(|task| task.address == address)?;
        let left = before.assigned_worker()?;
        let right = after.assigned_worker()?;
        (before.request.as_ref() == Some(actor)
            && after.request.as_ref() == Some(actor)
            && before.execution_context_id == after.execution_context_id
            && before.scheduler_id == after.scheduler_id
            && before.parent_node_id == after.parent_node_id
            && left.address == right.address
            && left.created_ms_ticks == right.created_ms_ticks
            && left.task_bound_ms_ticks == right.task_bound_ms_ticks)
            .then_some(after)
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlExecutionProfilesUnavailable {
    NotObserved,
    ParticipantChanged,
    UnverifiedParticipant,
    PlanUnavailable,
    PlanChanged,
    Unsupported,
    MetadataHidden,
    LayoutUnavailable,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MssqlExecutionProfiles {
    Observed {
        observed_from: String,
        observed_until: String,
        rows: Vec<MssqlOperatorThreadProfile>,
        limited: bool,
        operators: MssqlExecutionOperators,
    },
    Unavailable {
        reason: MssqlExecutionProfilesUnavailable,
    },
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum ExecutionProfileAvailability {
    Observed,
    NotObserved,
    Unavailable { reason: MssqlExecutionProfilesUnavailable },
}
impl MssqlExecutionProfiles {
    pub fn availability(&self) -> ExecutionProfileAvailability {
        match self {
            Self::Observed { rows, .. } if rows.is_empty() => ExecutionProfileAvailability::NotObserved,
            Self::Observed { .. } => ExecutionProfileAvailability::Observed,
            Self::Unavailable { reason } => ExecutionProfileAvailability::Unavailable { reason: *reason },
        }
    }
    fn fits_point(&self, point: &MssqlCurrentExecutionProfilePoint) -> bool {
        match self {
            Self::Observed { observed_from, observed_until, rows, limited, operators } => {
                let mut keys = BTreeSet::new();
                ObservationBounds::new(observed_from, observed_until).is_some()
                    && point.work.as_ref().is_some_and(|work| work.request_plan.is_some())
                    && point
                        .workers_before
                        .as_ref()
                        .is_some_and(|before| ObservationBounds::new(&before.observed_at, observed_from).is_some())
                    && point
                        .workers_after
                        .as_ref()
                        .is_some_and(|after| ObservationBounds::new(observed_until, &after.observed_at).is_some())
                    && point.binding == FocusedRequestBinding::Matched
                    && point.membership == FocusedRequestMembership::Observed
                    && rows.len() <= MAX_EXECUTION_PROFILE_ROWS
                    && (!*limited || !rows.is_empty())
                    && rows.iter().all(|row| row.is_valid() && keys.insert((row.node_id, row.thread_id)))
                    && operators.fits_point(point, rows)
            }
            Self::Unavailable { .. } => true,
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlCurrentExecutionProfilePoint {
    pub context: NativeReadContext,
    pub focus: Box<ActivityRequestFocus>,
    pub observed_at: String,
    pub checked_until: String,
    pub binding: FocusedRequestBinding,
    pub membership: FocusedRequestMembership,
    pub statement: DefinitionPayload,
    #[serde(deserialize_with = "Option::deserialize")]
    pub work: Option<Box<MssqlCurrentRequestWork>>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub workers_before: Option<MssqlSelectedWorkerPoint>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub workers_after: Option<MssqlSelectedWorkerPoint>,
    pub profiles: MssqlExecutionProfiles,
    #[serde(deserialize_with = "Option::deserialize")]
    pub relational_inputs: Option<Box<MssqlExecutionRelationalInputs>>,
}
impl MssqlCurrentExecutionProfilePoint {
    pub fn is_valid(&self) -> bool {
        let Some(bounds) = ObservationBounds::new(&self.context.observed_from, &self.context.observed_until) else {
            return false;
        };
        self.context.has_qualified_local_owner()
            && self.context.database_writable.is_some()
            && CurrentRequestWorkProfile::MssqlRequest.accepts_focus(&self.focus)
            && focused_request_original_scope_matches(&self.focus, &self.context)
            && ObservationBounds::new(&self.observed_at, &self.checked_until).is_some()
            && bounds.contains(&self.observed_at)
            && bounds.contains(&self.checked_until)
            && self.statement.is_valid()
            && payload_bounded(&self.statement, MAX_CURRENT_REQUEST_WORK_SQL_BYTES)
            && self.work.as_ref().is_none_or(|work| {
                work.is_valid()
                    && work.checked_until == self.checked_until
                    && work.current_plan.matches_statement(&self.focus.statement_identity)
                    && work.fits_context(&self.context)
            })
            && match self.membership {
                FocusedRequestMembership::Observed => {
                    self.work.is_some() && self.binding == FocusedRequestBinding::Matched
                }
                _ => self.work.is_none(),
            }
            && [&self.workers_before, &self.workers_after].into_iter().flatten().all(|point| {
                point.is_valid(&self.focus.original.key)
                    && bounds.contains(&point.observed_at)
                    && ObservationBounds::new(&self.observed_at, &point.observed_at).is_some()
                    && ObservationBounds::new(&point.observed_at, &self.checked_until).is_some()
            })
            && self.relational_inputs.as_ref().is_none_or(|inputs| inputs.matches_point(self))
            && self.profiles.fits_point(self)
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        serde_json::to_vec(&(
            "mssql-current-execution-profile-v1",
            &self.context,
            &self.focus.parent,
            &self.focus.original.key,
            &self.focus.statement_identity,
            &self.observed_at,
        ))
        .map(|bytes| digest(&bytes))
        .map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub(super) fn redact(&mut self) {
        self.focus.original.redact(crate::Engine::Mssql);
        redact_sql(&mut self.statement);
        if let Some(work) = &mut self.work
            && let MssqlCurrentRequestPlan::Observed { plan, .. } = &mut work.current_plan
            && let DefinitionPayload::Available { text, .. } = plan
        {
            *plan = DefinitionPayload::plan(text, crate::Engine::Mssql);
        }
        if let MssqlExecutionProfiles::Observed {
            operators: MssqlExecutionOperators::Observed { nodes, plan_sha256, .. },
            ..
        } = &mut self.profiles
        {
            for node in nodes {
                redact_sql(&mut node.predicate);
                redact_sql(&mut node.seek_predicate);
            }
            if let Some(work) = &self.work
                && let MssqlCurrentRequestPlan::Observed { plan: DefinitionPayload::Available { sha256, .. }, .. } =
                    &work.current_plan
            {
                *plan_sha256 = sha256.clone();
            }
        }
    }
    pub fn fits_source(&self, source: &CollectionDescription) -> bool {
        let SourceObservation::SourceRead { snapshot, execution, .. } = &source.observation else { return false };
        self.is_valid()
            && execution.as_ref() == &self.context
            && source.target == self.context.target
            && snapshot.dataset == SnapshotDataset::CurrentExecutionProfile
            && snapshot.records == 1
            && source.source == crate::source_id::CURRENT_EXECUTION_PROFILE
    }
}
fn native_integer(value: &str) -> bool {
    value.parse::<i64>().is_ok_and(|number| number.to_string() == value)
}
fn native_address(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn redact_sql(payload: &mut DefinitionPayload) {
    if let DefinitionPayload::Available { text, .. } = payload {
        *payload = DefinitionPayload::sql(text, crate::Engine::Mssql);
    }
}
fn payload_bounded(payload: &DefinitionPayload, limit: usize) -> bool {
    match payload {
        DefinitionPayload::Available { text, .. } => text.len() <= limit,
        _ => true,
    }
}

#[cfg(test)]
mod tests;
