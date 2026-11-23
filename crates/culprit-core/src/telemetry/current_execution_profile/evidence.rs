//! Descriptive comparisons over one operator's observed thread rows. No row roster proves finished siblings.
use super::*;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionThreadRange {
    pub observed: u32,
    pub interpreted: u32,
    pub negative: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    pub minimum: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub maximum: Option<String>,
}
impl ExecutionThreadRange {
    fn from_values<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Self {
        let mut range = Self { observed: 0, interpreted: 0, negative: 0, minimum: None, maximum: None };
        let mut minimum = None;
        let mut maximum = None;
        for value in values {
            range.observed += 1;
            let Some(value) = value.and_then(|value| value.parse::<i64>().ok()) else { continue };
            if value < 0 {
                range.negative += 1;
                continue;
            }
            range.interpreted += 1;
            minimum = Some(minimum.map_or(value, |old: i64| old.min(value)));
            maximum = Some(maximum.map_or(value, |old: i64| old.max(value)));
        }
        range.minimum = minimum.map(|value| value.to_string());
        range.maximum = maximum.map(|value| value.to_string());
        range
    }
    pub fn is_valid(&self) -> bool {
        if self.interpreted > self.observed || self.negative > self.observed - self.interpreted {
            return false;
        }
        match (&self.minimum, &self.maximum) {
            (None, None) => self.interpreted == 0,
            (Some(minimum), Some(maximum)) => {
                self.interpreted > 0
                    && minimum.parse::<i64>().is_ok_and(|low| {
                        low >= 0
                            && low.to_string() == *minimum
                            && maximum.parse::<i64>().is_ok_and(|high| high >= low && high.to_string() == *maximum)
                    })
            }
            _ => false,
        }
    }
    pub fn uneven(&self) -> bool {
        self.observed >= 2 && self.observed == self.interpreted && self.minimum != self.maximum
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ExecutionTaskContext {
    CheckedAtBounds,
    Unavailable,
    Changed,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionProfileThreadEvidence {
    pub thread_id: u32,
    pub task_context: ExecutionTaskContext,
    /// The task's independent final-endpoint wait. It is not a measured fraction of operator elapsed time.
    #[serde(deserialize_with = "Option::deserialize")]
    pub endpoint_wait: Option<MssqlWorkerWait>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub preemptive: Option<bool>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionProfileNodeEvidence {
    pub node_id: u32,
    pub rows_returned: ExecutionThreadRange,
    pub estimated_rows: ExecutionThreadRange,
    pub rows_read: ExecutionThreadRange,
    pub elapsed_ms: ExecutionThreadRange,
    pub physical_reads: ExecutionThreadRange,
    pub spill_write_pages: ExecutionThreadRange,
    pub threads: Vec<ExecutionProfileThreadEvidence>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub input_rows: Option<ExecutionInputRows>,
    /// Cumulative native output only; independent of the strict completed serial-scan interpretation.
    #[serde(deserialize_with = "Option::deserialize")]
    pub output_so_far: Option<OutputRowsSoFar>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub operator: Option<MssqlExecutionOperator>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CurrentExecutionProfileEvidence {
    pub profile_rows: u32,
    pub profile_nodes: u32,
    pub profiles_limited: bool,
    pub nodes_limited: bool,
    pub tasks_limited: bool,
    pub nodes: Vec<ExecutionProfileNodeEvidence>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub relational_inputs: Option<Box<MssqlExecutionRelationalInputs>>,
}
impl CurrentExecutionProfileEvidence {
    pub fn has_relational_input_context(&self) -> bool {
        self.relational_inputs.as_ref().is_some_and(|inputs| inputs.is_valid())
    }
    pub fn is_valid(&self) -> bool {
        let mut nodes = BTreeSet::new();
        let mut count = 0usize;
        self.profile_rows as usize <= MAX_EXECUTION_PROFILE_ROWS
            && self.profile_nodes <= self.profile_rows
            && self.nodes.len() == (self.profile_nodes as usize).min(MAX_PRESENTED_EXECUTION_NODES)
            && self.nodes_limited == (self.profile_nodes as usize > MAX_PRESENTED_EXECUTION_NODES)
            && self.nodes.iter().all(|node| {
                count += node.threads.len();
                let mut threads = BTreeSet::new();
                nodes.insert(node.node_id)
                    && !node.threads.is_empty()
                    && node.threads.iter().all(|thread| {
                        threads.insert(thread.thread_id)
                            && if thread.task_context == ExecutionTaskContext::CheckedAtBounds {
                                thread.endpoint_wait.as_ref().is_none_or(MssqlWorkerWait::is_valid)
                            } else {
                                thread.endpoint_wait.is_none() && thread.preemptive.is_none()
                            }
                    })
                    && [
                        &node.rows_returned,
                        &node.estimated_rows,
                        &node.rows_read,
                        &node.elapsed_ms,
                        &node.physical_reads,
                        &node.spill_write_pages,
                    ]
                    .into_iter()
                    .all(|range| range.is_valid() && range.observed as usize == node.threads.len())
                    && node
                        .operator
                        .as_ref()
                        .is_none_or(|operator| operator.is_valid() && operator.node_id == node.node_id)
                    && node.input_rows.is_some()
                        == node.operator.as_ref().is_some_and(super::input_rows::is_variable_input)
                    && node
                        .input_rows
                        .as_ref()
                        .is_none_or(|comparison| comparison.fits_ranges(&node.rows_returned, &node.estimated_rows))
                    && node.output_so_far.as_ref().is_none_or(|output| !self.profiles_limited && output.fits_node(node))
            })
            && count <= self.profile_rows as usize
            && (self.nodes_limited || count == self.profile_rows as usize)
            && self.relational_inputs.as_ref().is_none_or(|inputs| inputs.is_valid())
    }
    pub fn from_point(point: &MssqlCurrentExecutionProfilePoint) -> Result<Self> {
        if !point.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let MssqlExecutionProfiles::Observed { rows, limited, operators, .. } = &point.profiles else {
            return Ok(Self {
                profile_rows: 0,
                profile_nodes: 0,
                profiles_limited: false,
                nodes_limited: false,
                tasks_limited: false,
                nodes: vec![],
                relational_inputs: point.relational_inputs.clone(),
            });
        };
        let mut grouped = BTreeMap::<u32, Vec<&MssqlOperatorThreadProfile>>::new();
        for row in rows {
            grouped.entry(row.node_id).or_default().push(row)
        }
        let mut nodes = grouped
            .into_iter()
            .map(|(node_id, rows)| {
                let range = |select: fn(&MssqlOperatorThreadCounters) -> Option<&str>| {
                    ExecutionThreadRange::from_values(rows.iter().map(|row| select(&row.counters)))
                };
                let input_rows = operators
                    .node(node_id)
                    .and_then(|node| ExecutionInputRows::from_node(node, &rows, operators, *limited));
                let output_so_far = OutputRowsSoFar::from_rows(input_rows.as_ref(), &rows);
                ExecutionProfileNodeEvidence {
                    node_id,
                    rows_returned: range(|row| row.rows_returned.as_deref()),
                    estimated_rows: range(|row| row.estimated_rows.as_deref()),
                    rows_read: range(|row| row.rows_read.as_deref()),
                    elapsed_ms: range(|row| row.elapsed_ms.as_deref()),
                    physical_reads: range(|row| row.physical_reads.as_deref()),
                    spill_write_pages: range(|row| row.spill_write_pages.as_deref()),
                    input_rows,
                    output_so_far,
                    threads: rows.into_iter().map(|row| thread(point, row)).collect(),
                    operator: operators.node(node_id).cloned(),
                }
            })
            .collect::<Vec<_>>();
        let profile_nodes = nodes.len() as u32;
        nodes.sort_by_key(|node| (!node.rows_returned.uneven(), node.node_id));
        let nodes_limited = nodes.len() > MAX_PRESENTED_EXECUTION_NODES;
        nodes.truncate(MAX_PRESENTED_EXECUTION_NODES);
        Ok(Self {
            profile_rows: rows.len() as u32,
            profile_nodes,
            nodes_limited,
            profiles_limited: *limited,
            tasks_limited: point.workers_before.as_ref().is_some_and(|point| point.limited)
                || point.workers_after.as_ref().is_some_and(|point| point.limited),
            nodes,
            relational_inputs: point.relational_inputs.clone(),
        })
    }
}
fn thread(
    point: &MssqlCurrentExecutionProfilePoint,
    row: &MssqlOperatorThreadProfile,
) -> ExecutionProfileThreadEvidence {
    let (task_context, task) = match (&point.workers_before, &point.workers_after, &row.task_address) {
        (Some(before), Some(after), Some(address)) => {
            let task = before.same_assigned_task(after, &point.focus.original.key, address);
            let observed_both = before.tasks.iter().any(|task| &task.address == address)
                && after.tasks.iter().any(|task| &task.address == address);
            (
                if task.is_some() {
                    ExecutionTaskContext::CheckedAtBounds
                } else if observed_both {
                    ExecutionTaskContext::Changed
                } else {
                    ExecutionTaskContext::Unavailable
                },
                task,
            )
        }
        _ => (ExecutionTaskContext::Unavailable, None),
    };
    ExecutionProfileThreadEvidence {
        thread_id: row.thread_id,
        task_context,
        endpoint_wait: task.and_then(|task| task.wait.clone()),
        preemptive: task.and_then(MssqlWorkerTask::assigned_worker).and_then(|worker| worker.is_preemptive),
    }
}

/// Shared source-free report used by the persisted answer, developer fields and ordinary local consumers.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[ts(export)]
pub struct CurrentExecutionProfileReport {
    pub context: NativeReadContext,
    pub source_state: crate::SourceState,
    pub evidence: CurrentExecutionProfileEvidence,
    /// Exact operator work and independently checked task state; never an exclusive active-operator verdict.
    pub parallel_context: ParallelOperatorContext,
    pub summary: String,
    pub mechanisms: Vec<String>,
    pub investigation: String,
    pub verify: String,
    pub limitations: Vec<String>,
}
