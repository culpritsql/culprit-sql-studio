//! One bounded native worker/task point. Assigned workers, pending tasks and waiters keep different units.
//! Native addresses are capture-local locators; neither task counts nor parallel reservations establish live DOP.
use super::*;
use std::collections::BTreeSet;
mod evidence;
pub use evidence::*;

pub const MAX_WORKER_TASKS: usize = 128;
pub const MAX_WORKER_WAITERS: usize = 128;
pub const MAX_WORKER_REQUESTS: usize = MAX_WORKER_TASKS + MAX_WORKER_WAITERS;
/// Bounded Gold query detail; the entire native point remains retained and independently readable.
pub const MAX_PRESENTED_WORKER_ACTORS: usize = (crate::derived_diagnosis::MAX_DERIVED_QUERIES - 1) / 4;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlWorkerBinding {
    pub address: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub task_address: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub created_ms_ticks: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub task_bound_ms_ticks: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub state: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub is_preemptive: Option<bool>,
}
impl MssqlWorkerBinding {
    pub fn is_valid(&self) -> bool {
        address(&self.address)
            && self.task_address.as_deref().is_none_or(native_address)
            && self.created_ms_ticks.as_deref().is_none_or(native_ticks)
            && self.task_bound_ms_ticks.as_deref().is_none_or(native_ticks)
            && self.state.as_deref().is_none_or(|state| !state.is_empty() && state.len() <= 60)
    }
    pub fn is_assigned_to(&self, task: &MssqlWorkerTask) -> bool {
        self.is_valid()
            && self.task_address.as_deref() == Some(task.address.as_str())
            && self.created_ms_ticks.is_some()
            && self.task_bound_ms_ticks.is_some()
            && self.state.as_deref().is_some_and(|state| matches!(state, "RUNNING" | "RUNNABLE" | "SUSPENDED"))
            && matches!(task.state.as_str(), "RUNNING" | "RUNNABLE" | "SUSPENDED" | "SPINLOOP")
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlWorkerWait {
    pub task_address: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub session_id: Option<i16>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub execution_context_id: Option<u32>,
    pub native_type: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub elapsed_ms: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub request_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub scheduler_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub parent_node_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub request: Option<ActivityKey>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocking_task_address: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocking_session_id: Option<i16>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocking_execution_context_id: Option<i32>,
}
impl MssqlWorkerWait {
    pub fn is_valid(&self) -> bool {
        address(&self.task_address)
            && !self.native_type.is_empty()
            && self.native_type.len() <= 60
            && self.elapsed_ms.as_deref().is_none_or(counter)
            && self.blocking_task_address.as_deref().is_none_or(native_address)
            && self.request.as_ref().is_none_or(|request| {
                request.is_valid()
                    && self.session_id.and_then(|id| u32::try_from(id).ok()).map(|id| id.to_string()).as_deref()
                        == Some(request.session.id.as_str())
                    && self.request_id.map(|id| id.to_string()).as_deref()
                        == request.request.as_ref().map(|request| request.id.as_str())
            })
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlWorkerTask {
    pub address: String,
    pub state: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub scheduler_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub parent_node_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub scheduler_work_queue_count: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub session_id: Option<i16>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub request_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub execution_context_id: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub worker: Option<MssqlWorkerBinding>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait: Option<MssqlWorkerWait>,
    /// Joined native request key only. Missing generations/statement checks never acquire one by session ID.
    #[serde(deserialize_with = "Option::deserialize")]
    pub request: Option<ActivityKey>,
}
impl MssqlWorkerTask {
    pub fn is_valid(&self) -> bool {
        address(&self.address)
            && !self.state.is_empty()
            && self.state.len() <= 60
            && self.scheduler_work_queue_count.as_deref().is_none_or(counter)
            && self.worker.as_ref().is_none_or(MssqlWorkerBinding::is_valid)
            && self.wait.as_ref().is_none_or(|wait| {
                wait.is_valid()
                    && wait.task_address == self.address
                    && wait.session_id == self.session_id
                    && wait.execution_context_id == self.execution_context_id
                    && wait.request_id == self.request_id
                    && wait.request == self.request
                    && wait.scheduler_id == self.scheduler_id
                    && wait.parent_node_id == self.parent_node_id
            })
            && self.request.as_ref().is_none_or(|request| {
                request.is_valid()
                    && self.session_id.and_then(|id| u32::try_from(id).ok()).map(|id| id.to_string()).as_deref()
                        == Some(request.session.id.as_str())
                    && request.request.as_ref().map(|request| request.id.as_str())
                        == self.request_id.map(|id| id.to_string()).as_deref()
            })
    }
    pub fn worker_binding_unavailable(&self) -> bool {
        self.worker.as_ref().is_some_and(|worker| !worker.is_assigned_to(self))
            || self.worker.is_none() && !matches!(self.state.as_str(), "PENDING" | "DONE")
    }
    pub fn assigned_worker(&self) -> Option<&MssqlWorkerBinding> {
        self.worker.as_ref().filter(|worker| worker.is_assigned_to(self))
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlWorkerRequest {
    pub observation: ActivityObservation,
    #[serde(deserialize_with = "Option::deserialize")]
    pub before_check: Option<TaskRequestCheck>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub after_check: Option<TaskRequestCheck>,
}
impl MssqlWorkerRequest {
    pub fn context(&self) -> TaskRequestContext {
        TaskRequestContext::from_checks(true, &self.before_check, &self.after_check)
    }
    fn checks_are_valid(&self, point: &MssqlWorkerStarvationPoint) -> bool {
        [&self.before_check, &self.after_check].into_iter().flatten().all(|check| {
            ObservationBounds::new(&point.observed_at, &point.context.observed_until)
                .is_some_and(|bounds| bounds.contains(&check.observed_at))
        }) && match (&self.before_check, &self.after_check) {
            (Some(before), Some(after)) => ObservationBounds::new(&before.observed_at, &after.observed_at).is_some(),
            _ => true,
        }
    }
    pub fn is_qualified(&self) -> bool {
        self.context() == TaskRequestContext::CheckedAtBounds
            && self.observation.has_session_generation_authority()
            && self.observation.key.request.is_some()
            && self.observation.statement_identity.is_some()
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlWorkerStarvationPoint {
    /// Actual native acquisition owner. Its database route, when present, does not own this task population.
    pub context: NativeReadContext,
    /// Exact source-observed shared instance/node; all task/worker counts retain shared scope.
    pub worker_scope: SharedResourceIdentity,
    pub observed_at: String,
    pub tasks: Vec<MssqlWorkerTask>,
    /// A separate bounded THREADPOOL census includes waits with no session/task/request association.
    pub threadpool_waits: Vec<MssqlWorkerWait>,
    pub requests: Vec<MssqlWorkerRequest>,
    pub tasks_limited: bool,
    pub waits_limited: bool,
}
impl MssqlWorkerStarvationPoint {
    pub fn is_valid(&self) -> bool {
        let mut tasks = BTreeSet::new();
        let mut waits = BTreeSet::new();
        let mut workers = BTreeSet::new();
        let mut requests = BTreeSet::new();
        self.context.has_qualified_local_owner()
            && self.worker_scope.is_valid()
            && self.worker_scope.engine == crate::Engine::Mssql
            && matches!(&self.context.partition,SourcePartition::InstanceLocal{member:Some(member),epoch:Some(_)} if member==&self.worker_scope)
            && matches!(self.worker_scope.kind, SharedResourceKind::Instance | SharedResourceKind::Node)
            && match &self.context.target {
                CollectionTarget::Database(_) => self.context.database_writable.is_some(),
                CollectionTarget::ConfiguredConnection(_) => false,
                CollectionTarget::Shared(owner) => {
                    owner == &self.worker_scope && self.context.database_writable.is_none()
                }
            }
            && ObservationBounds::new(&self.context.observed_from, &self.context.observed_until)
                .is_some_and(|bounds| bounds.contains(&self.observed_at))
            && self.tasks.len() <= MAX_WORKER_TASKS
            && self.threadpool_waits.len() <= MAX_WORKER_WAITERS
            && self.requests.len() <= MAX_WORKER_REQUESTS
            && self.tasks.iter().all(|task| {
                task.is_valid()
                    && tasks.insert(&task.address)
                    && task.assigned_worker().is_none_or(|worker| workers.insert(&worker.address))
            })
            && self
                .threadpool_waits
                .iter()
                .all(|wait| wait.is_valid() && wait.native_type == "THREADPOOL" && waits.insert(&wait.task_address))
            && self.requests.iter().all(|request| {
                request.observation.is_valid()
                    && request.checks_are_valid(self)
                    && request.observation.key.request.is_some()
                    && requests.insert(&request.observation.key)
            })
            && ActivityEvidence::validate(&self.requests.iter().map(|row| row.observation.clone()).collect::<Vec<_>>())
                .is_ok()
            && self
                .tasks
                .iter()
                .filter_map(|task| task.request.as_ref())
                .chain(self.threadpool_waits.iter().filter_map(|wait| wait.request.as_ref()))
                .all(|request| requests.contains(request))
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        serde_json::to_vec(&("mssql-worker-starvation-point-v1", &self.context, &self.observed_at))
            .map(|bytes| digest(&bytes))
            .map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub fn qualified_request(&self, key: &ActivityKey) -> Option<&ActivityObservation> {
        self.requests.iter().find(|row| row.observation.key == *key && row.is_qualified()).map(|row| &row.observation)
    }
    pub fn fits_source(&self, source: &CollectionDescription) -> bool {
        let SourceObservation::SourceRead { snapshot, execution, .. } = &source.observation else { return false };
        self.is_valid()
            && execution.as_ref() == &self.context
            && snapshot.dataset == SnapshotDataset::WorkerStarvation
            && snapshot.records == 1
            && source.source == crate::source_id::WORKER_QUEUE
    }
}

pub fn shared_worker_context(context: &NativeReadContext) -> bool {
    context.has_qualified_local_owner()
        && context.database_writable.is_none()
        && matches!((&context.target, &context.partition),
            (CollectionTarget::Shared(owner),SourcePartition::InstanceLocal {member:Some(member),epoch:Some(_)} )
                if owner==member && owner.engine==crate::Engine::Mssql
                    && matches!(owner.kind,SharedResourceKind::Instance|SharedResourceKind::Node))
}
fn counter(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|number| number.to_string() == value)
}
fn native_ticks(value: &str) -> bool {
    value.parse::<i64>().is_ok_and(|number| number.to_string() == value)
}
fn native_address(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn address(value: &str) -> bool {
    native_address(value) && !value.bytes().all(|byte| byte == b'0')
}

/// Closed native descriptor validation precedes the positive condition. Counter names, totals on another
/// population, missing/NULL fields and a task count cannot become worker queue pressure.
pub(super) fn positive_worker_queue(row: &OperationalObservation) -> bool {
    if !row.is_valid() {
        return false;
    }
    match &row.fact {
        OperationalFact::Queue(queue) => {
            row.source == crate::source_id::WORKER_QUEUE
                && queue.kind == QueueKind::WorkerTasks
                && queue.count.parse::<u64>().is_ok_and(|count| count > 0)
        }
        OperationalFact::CounterSet(sample) if row.source == crate::source_id::SCHEDULERS => {
            let Ok(point) = MssqlSchedulerPoint::from_sample(sample) else { return false };
            let value = |name| {
                MSSQL_SCHEDULER_FIELDS.iter().find(|field| field.name == name).and_then(|field| point.field(field).ok())
            };
            matches!(value("status"), Some(MssqlNativeFieldValue::Observed("VISIBLE ONLINE")))
                && matches!(value("is_online"), Some(MssqlNativeFieldValue::Observed("1")))
                && matches!(value("work_queue_count"),Some(MssqlNativeFieldValue::Observed(count))
                    if count.parse::<u64>().is_ok_and(|count| count > 0))
        }
        _ => false,
    }
}
pub(super) fn original_worker_queue_matches(source: &CollectionDescription, row: &TelemetryRecord) -> bool {
    match row {
        TelemetryRecord::Activity(request) => {
            request.has_session_generation_authority()
                && request.statement_identity.is_some()
                && request.scope == ActivityScopeBasis::RequestDatabase
                && request.wait.as_ref().is_some_and(|wait| wait.class == Some(ActivityWaitClass::WorkerQueue))
        }
        TelemetryRecord::Operational(_) => matches!(
            DiagnosticOriginalCondition::from_record(source, row),
            Some(DiagnosticOriginalCondition::MssqlWorkerQueue)
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests;

/// Native SQL Server lock-wait grammar shared by Activity and task diagnosis.
pub fn mssql_lock_wait_type(wait_type: &str) -> bool {
    wait_type.get(..4).is_some_and(|prefix| prefix.eq_ignore_ascii_case("LCK_"))
}

/// The same source-free explanation is used by durable incident history and every Gold consumer.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct WorkerStarvationReport {
    pub context: NativeReadContext,
    pub worker_scope: SharedResourceIdentity,
    pub observed_at: String,
    pub source_state: crate::SourceState,
    pub evidence: WorkerStarvationEvidence,
    pub summary: String,
    pub mechanisms: Vec<String>,
    pub investigation: String,
    pub verify: String,
    pub limitations: Vec<String>,
}
