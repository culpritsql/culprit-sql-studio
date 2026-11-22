//! Interpret the retained point once. Task units never substitute for assigned-worker identities.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum WorkerContributionKind {
    AssignedParallelWorkers,
    PreemptiveAssignedWorkers,
    LockWaitingAssignedWorkers,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkerContribution {
    pub request: ActivityKey,
    pub kind: WorkerContributionKind,
    /// Some only for a native scheduler with independently positive queue evidence at this same point.
    pub queue_scheduler_id: Option<u32>,
    /// Distinct reciprocally associated worker addresses at the original point. Not CPU demand/live DOP.
    pub assigned_workers: u32,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkerBlockingContribution {
    pub affected: ActivityKey,
    pub owner: ActivityWaitOwner,
    pub assigned_waiting_workers: u32,
    pub queue_scheduler_id: u32,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkerStarvationEvidence {
    pub observed_tasks: u32,
    pub assigned_workers: u32,
    pub observed_threadpool_waits: u32,
    pub unassigned_threadpool_waits: u32,
    pub affected_requests: Vec<ActivityKey>,
    pub impacted_schedulers: Vec<u32>,
    pub contributions: Vec<WorkerContribution>,
    pub blocking: Vec<WorkerBlockingContribution>,
    pub tasks_limited: bool,
    pub waits_limited: bool,
    pub unqualified_requests: u32,
    pub unverified_worker_bindings: u32,
}
impl WorkerStarvationEvidence {
    pub fn from_point(point: &MssqlWorkerStarvationPoint) -> Result<Self> {
        if !point.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let index = super::super::activity::ActivityIndex::new(point.requests.iter().map(|row| &row.observation))?;
        let impacted = point
            .tasks
            .iter()
            .filter(|task| {
                task.state == "PENDING"
                    || task
                        .scheduler_work_queue_count
                        .as_deref()
                        .is_some_and(|value| value.parse::<u64>().is_ok_and(|value| value > 0))
            })
            .filter_map(|task| task.scheduler_id)
            .chain(point.threadpool_waits.iter().filter_map(|wait| wait.scheduler_id))
            .collect::<BTreeSet<_>>();
        let mut affected = BTreeSet::new();
        let mut contributions = Vec::new();
        let mut blocking = Vec::new();
        for request in point.requests.iter().filter(|row| row.is_qualified()) {
            let request = &request.observation;
            if !index.has_session_generation_authority(&request.key.session) {
                continue;
            }
            if request.wait.as_ref().is_some_and(|wait| wait.class == Some(ActivityWaitClass::WorkerQueue)) {
                affected.insert(request.key.clone());
            }
            let assigned = point
                .tasks
                .iter()
                .filter(|task| task.request.as_ref() == Some(&request.key) && task.assigned_worker().is_some())
                .collect::<Vec<_>>();
            let schedules = assigned
                .iter()
                .map(|task| task.scheduler_id.filter(|id| impacted.contains(id)))
                .collect::<BTreeSet<_>>();
            for scheduler in schedules {
                let local = assigned
                    .iter()
                    .filter(|task| task.scheduler_id.filter(|id| impacted.contains(id)) == scheduler)
                    .copied()
                    .collect::<Vec<_>>();
                for kind in [
                    WorkerContributionKind::AssignedParallelWorkers,
                    WorkerContributionKind::PreemptiveAssignedWorkers,
                    WorkerContributionKind::LockWaitingAssignedWorkers,
                ] {
                    let count = local
                        .iter()
                        .filter(|task| match kind {
                            WorkerContributionKind::AssignedParallelWorkers => {
                                task.execution_context_id.is_some_and(|id| id > 0)
                            }
                            WorkerContributionKind::PreemptiveAssignedWorkers => {
                                task.assigned_worker().is_some_and(|worker| worker.is_preemptive == Some(true))
                            }
                            WorkerContributionKind::LockWaitingAssignedWorkers => {
                                task.wait.as_ref().is_some_and(|wait| mssql_lock_wait_type(&wait.native_type))
                            }
                        })
                        .count() as u32;
                    if count > 0 {
                        contributions.push(WorkerContribution {
                            request: request.key.clone(),
                            kind,
                            assigned_workers: count,
                            queue_scheduler_id: scheduler,
                        });
                    }
                }
                // The same native generation index used by blocking analysis qualifies the reported owner.
                // A victim holding a worker is not itself the blocking owner. Unknown/MARS acquiring SQL stays unavailable.
                if let Some(scheduler) = scheduler
                    && request.waiting_on_lock == Some(true)
                {
                    for owner in request.wait.iter().flat_map(|wait| &wait.owners) {
                        let Some(session) = owner.session() else { continue };
                        if session == request.key.session || !index.has_session_generation_authority(&session) {
                            continue;
                        }
                        // Parallel tasks may wait on different owners than the request coordinator. Only
                        // a task's own native blocking-session ID may connect its held worker to this owner.
                        let held = local
                            .iter()
                            .filter(|task| {
                                task.wait.as_ref().is_some_and(|wait| {
                                    mssql_lock_wait_type(&wait.native_type)
                                        && wait.blocking_session_id.map(|id| id.to_string()).as_deref()
                                            == Some(session.id.as_str())
                                })
                            })
                            .count() as u32;
                        if held == 0 {
                            continue;
                        }
                        blocking.push(WorkerBlockingContribution {
                            affected: request.key.clone(),
                            owner: owner.clone(),
                            assigned_waiting_workers: held,
                            queue_scheduler_id: scheduler,
                        });
                    }
                }
            }
        }
        let mut unassigned = 0;
        for wait in &point.threadpool_waits {
            let request = wait.request.as_ref().and_then(|key| point.qualified_request(key));
            if let Some(request) = request {
                affected.insert(request.key.clone());
            } else {
                unassigned += 1;
            }
        }
        Ok(Self {
            observed_tasks: point.tasks.len() as u32,
            assigned_workers: point.tasks.iter().filter(|task| task.assigned_worker().is_some()).count() as u32,
            observed_threadpool_waits: point.threadpool_waits.len() as u32,
            unassigned_threadpool_waits: unassigned,
            affected_requests: affected.into_iter().collect(),
            impacted_schedulers: impacted.into_iter().collect(),
            contributions,
            blocking,
            tasks_limited: point.tasks_limited,
            waits_limited: point.waits_limited,
            unqualified_requests: point.requests.iter().filter(|row| !row.is_qualified()).count() as u32,
            unverified_worker_bindings: point.tasks.iter().filter(|task| task.worker_binding_unavailable()).count()
                as u32,
        })
    }
    pub fn is_valid(&self) -> bool {
        let mut affected = BTreeSet::new();
        let mut contributions = BTreeSet::new();
        self.observed_tasks as usize <= MAX_WORKER_TASKS
            && self.assigned_workers <= self.observed_tasks
            && self.observed_threadpool_waits as usize <= MAX_WORKER_WAITERS
            && self.unassigned_threadpool_waits <= self.observed_threadpool_waits
            && self.affected_requests.len() <= MAX_WORKER_REQUESTS
            && self.affected_requests.iter().all(|key| key.is_valid() && key.request.is_some() && affected.insert(key))
            && self.contributions.len() <= 3 * MAX_WORKER_REQUESTS
            && self.contributions.iter().all(|row| {
                row.request.is_valid()
                    && row.request.request.is_some()
                    && row.assigned_workers > 0
                    && row.assigned_workers <= self.assigned_workers
                    && row.queue_scheduler_id.is_none_or(|id| self.impacted_schedulers.contains(&id))
                    && contributions.insert((&row.request, row.kind as u8, row.queue_scheduler_id))
            })
            && self.impacted_schedulers.len() <= MAX_WORKER_TASKS + MAX_WORKER_WAITERS
            && self.impacted_schedulers.windows(2).all(|pair| pair[0] < pair[1])
            && self.blocking.len() <= MAX_WORKER_TASKS
            && self.blocking.iter().all(|row| {
                row.affected.is_valid()
                    && row.owner.is_valid()
                    && row.owner.session().is_some()
                    && row.assigned_waiting_workers > 0
                    && row.assigned_waiting_workers <= self.assigned_workers
                    && self.impacted_schedulers.contains(&row.queue_scheduler_id)
            })
            && self.unverified_worker_bindings <= self.observed_tasks
            && self.unqualified_requests as usize <= MAX_WORKER_REQUESTS
    }
}
