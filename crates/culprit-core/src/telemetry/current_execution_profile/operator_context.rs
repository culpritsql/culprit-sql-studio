//! Point-only association of native operator work and its checked task's independent final-endpoint wait.
//! A task can serve several operators; no endpoint state identifies an exclusive active RelOp or data skew.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ExecutionTaskWaitKind {
    BufferRead,
    NativeLock,
    ExchangeSynchronization,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ParallelOperatorContextGap {
    ParticipantChanged,
    ProfilesUnavailable,
    LimitedProfiles,
    LimitedTasks,
    PlanUnavailable,
    SerialOrUnknownTopology,
    InsufficientCheckedTasks,
    UnverifiedTask,
    UnsupportedWait,
    NoObservedPhysicalReads,
    UnknownWorkCounters,
    MultipleProfiledOperators,
    BlockingOwnerIdentityUnavailable,
    ExchangeCauseUnavailable,
    PresentationLimited,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ParallelOperatorObservation {
    pub node_id: u32,
    pub thread_id: u32,
    pub kind: ExecutionTaskWaitKind,
    pub native_wait: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_ms: Option<String>,
    pub task_observed_at: String,
    pub profile_observed_from: String,
    pub profile_observed_until: String,
    /// Count of observed nodes using this task, not an inventory of the entire execution.
    pub profiled_nodes_for_task: u32,
    /// Distinct checked task associations observed for this node, never live DOP or all threads.
    pub checked_tasks_at_node: u32,
    pub operator: MssqlExecutionOperator,
    #[serde(deserialize_with = "Option::deserialize")]
    pub physical_reads: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub rows_read: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub rows_returned: Option<String>,
}
impl ParallelOperatorObservation {
    /// This is a difference between native counters at one operator, never a workload selectivity/health verdict.
    pub fn read_more_than_returned(&self) -> bool {
        nonnegative(self.rows_read.as_deref())
            .zip(nonnegative(self.rows_returned.as_deref()))
            .is_some_and(|(read, returned)| read > returned)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ParallelOperatorContext {
    pub examined_profile_rows: u32,
    pub observations: Vec<ParallelOperatorObservation>,
    pub gaps: Vec<ParallelOperatorContextGap>,
}
impl ParallelOperatorContext {
    pub fn from_point(point: &MssqlCurrentExecutionProfilePoint) -> Result<Self> {
        if !point.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let mut result = Self { examined_profile_rows: 0, observations: vec![], gaps: vec![] };
        if point.binding != FocusedRequestBinding::Matched || point.membership != FocusedRequestMembership::Observed {
            result.add_gap(ParallelOperatorContextGap::ParticipantChanged);
            return Ok(result);
        }
        let MssqlExecutionProfiles::Observed { observed_from, observed_until, rows, limited, operators } =
            &point.profiles
        else {
            result.add_gap(ParallelOperatorContextGap::ProfilesUnavailable);
            return Ok(result);
        };
        result.examined_profile_rows = rows.len() as u32;
        if *limited {
            result.add_gap(ParallelOperatorContextGap::LimitedProfiles);
            return Ok(result);
        }
        let Some((before, after)) = point.workers_before.as_ref().zip(point.workers_after.as_ref()) else {
            result.add_gap(ParallelOperatorContextGap::UnverifiedTask);
            return Ok(result);
        };
        if before.limited || after.limited {
            result.add_gap(ParallelOperatorContextGap::LimitedTasks);
            return Ok(result);
        }
        if matches!(operators, MssqlExecutionOperators::Unavailable { .. }) {
            result.add_gap(ParallelOperatorContextGap::PlanUnavailable);
            return Ok(result);
        }
        let profile = ProfileContext::new(point, rows, operators, after, observed_from, observed_until);
        for row in rows {
            result.observe(row, &profile);
        }
        Ok(result)
    }
    fn observe(&mut self, row: &MssqlOperatorThreadProfile, profile: &ProfileContext<'_>) {
        let (point, operators, after, from, until) =
            (profile.point, profile.operators, profile.after, profile.from, profile.until);
        let Some(operator) =
            operators.node(row.node_id).filter(|operator| operator.native_operator == row.physical_operator)
        else {
            self.add_gap(ParallelOperatorContextGap::PlanUnavailable);
            return;
        };
        if operator.parallel == Some(false) {
            return;
        }
        if operator.parallel.is_none() {
            self.add_gap(ParallelOperatorContextGap::SerialOrUnknownTopology);
            return;
        }
        let checked_tasks = profile.checked_tasks_at_node(row.node_id);
        if checked_tasks < 2 {
            self.add_gap(ParallelOperatorContextGap::InsufficientCheckedTasks);
            return;
        }
        let task = row.task_address.as_deref().and_then(|address| profile.checked_tasks.get(address).copied());
        let Some(task) = task else {
            self.add_gap(ParallelOperatorContextGap::UnverifiedTask);
            return;
        };
        let Some(wait) = task.wait.as_ref() else {
            return;
        };
        let Some(kind) = point.context.target.engine().spec().execution_task_wait(&wait.native_type) else {
            self.add_gap(ParallelOperatorContextGap::UnsupportedWait);
            return;
        };
        if kind == ExecutionTaskWaitKind::ExchangeSynchronization {
            self.add_gap(ParallelOperatorContextGap::ExchangeCauseUnavailable);
            return;
        }
        if kind == ExecutionTaskWaitKind::BufferRead
            && !nonnegative(row.counters.physical_reads.as_deref()).is_some_and(|reads| reads > 0)
        {
            self.add_gap(if nonnegative(row.counters.physical_reads.as_deref()).is_none() {
                ParallelOperatorContextGap::UnknownWorkCounters
            } else {
                ParallelOperatorContextGap::NoObservedPhysicalReads
            });
            return;
        }
        if kind == ExecutionTaskWaitKind::NativeLock {
            self.add_gap(ParallelOperatorContextGap::BlockingOwnerIdentityUnavailable);
        }
        let node_count =
            row.task_address.as_deref().and_then(|address| profile.task_node_counts.get(address)).copied().unwrap_or(0);
        if node_count > 1 {
            self.add_gap(ParallelOperatorContextGap::MultipleProfiledOperators);
        }
        if self.observations.len() == MAX_PRESENTED_EXECUTION_NODES {
            self.add_gap(ParallelOperatorContextGap::PresentationLimited);
            return;
        }
        self.observations.push(ParallelOperatorObservation {
            node_id: row.node_id,
            thread_id: row.thread_id,
            kind,
            native_wait: wait.native_type.clone(),
            wait_ms: wait.elapsed_ms.clone(),
            task_observed_at: after.observed_at.clone(),
            profile_observed_from: from.into(),
            profile_observed_until: until.into(),
            profiled_nodes_for_task: node_count,
            checked_tasks_at_node: checked_tasks,
            operator: operator.clone(),
            physical_reads: available(&row.counters.physical_reads),
            rows_read: available(&row.counters.rows_read),
            rows_returned: available(&row.counters.rows_returned),
        });
    }
    fn add_gap(&mut self, gap: ParallelOperatorContextGap) {
        if !self.gaps.contains(&gap) {
            self.gaps.push(gap);
        }
    }
}
struct ProfileContext<'a> {
    point: &'a MssqlCurrentExecutionProfilePoint,
    operators: &'a MssqlExecutionOperators,
    after: &'a MssqlSelectedWorkerPoint,
    from: &'a str,
    until: &'a str,
    checked_tasks: BTreeMap<&'a str, &'a MssqlWorkerTask>,
    node_task_counts: BTreeMap<u32, u32>,
    task_node_counts: BTreeMap<&'a str, u32>,
}
impl<'a> ProfileContext<'a> {
    fn new(
        point: &'a MssqlCurrentExecutionProfilePoint,
        rows: &'a [MssqlOperatorThreadProfile],
        operators: &'a MssqlExecutionOperators,
        after: &'a MssqlSelectedWorkerPoint,
        from: &'a str,
        until: &'a str,
    ) -> Self {
        let mut checked_tasks = BTreeMap::new();
        let mut task_nodes = BTreeMap::<&str, BTreeSet<u32>>::new();
        let mut node_tasks = BTreeMap::<u32, BTreeSet<&str>>::new();
        for row in rows {
            let Some(address) = row.task_address.as_deref() else { continue };
            task_nodes.entry(address).or_default().insert(row.node_id);
            let task = point
                .workers_before
                .as_ref()
                .and_then(|before| before.same_assigned_task(after, &point.focus.original.key, address));
            if let Some(task) = task {
                checked_tasks.insert(address, task);
                node_tasks.entry(row.node_id).or_default().insert(address);
            }
        }
        Self {
            point,
            operators,
            after,
            from,
            until,
            checked_tasks,
            node_task_counts: node_tasks.into_iter().map(|(id, tasks)| (id, tasks.len() as u32)).collect(),
            task_node_counts: task_nodes.into_iter().map(|(address, nodes)| (address, nodes.len() as u32)).collect(),
        }
    }
    fn checked_tasks_at_node(&self, node_id: u32) -> u32 {
        self.node_task_counts.get(&node_id).copied().unwrap_or(0)
    }
}

fn nonnegative(value: Option<&str>) -> Option<i64> {
    value?.parse::<i64>().ok().filter(|value| *value >= 0)
}
fn available(value: &Option<String>) -> Option<String> {
    value.as_ref().filter(|value| nonnegative(Some(value)).is_some()).cloned()
}
