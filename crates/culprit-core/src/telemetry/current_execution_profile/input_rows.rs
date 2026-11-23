//! Native output comparisons require an observed serial scan denominator. They never diagnose harm or input size.
use super::*;
use crate::plan_facts::RelationalInputKind;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ExecutionInputRowsUnavailable {
    LimitedProfiles,
    PlanTopologyUnavailable,
    ParallelOrMultipleThreads,
    RepeatedOrUnknownScans,
    NativeTicksUnavailable,
    OutputCountsUnavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum ExecutionInputRows {
    /// One reported serial scan, not final input cardinality or whole-query completion.
    SerialScan {
        returned: String,
        estimated: String,
    },
    Unavailable {
        reason: ExecutionInputRowsUnavailable,
    },
}
impl ExecutionInputRows {
    pub fn is_valid(&self) -> bool {
        match self {
            Self::SerialScan { returned, estimated } => {
                nonnegative(returned).is_some() && nonnegative(estimated).is_some()
            }
            Self::Unavailable { .. } => true,
        }
    }
    pub fn exceeds_estimate(&self) -> bool {
        match self {
            Self::SerialScan { returned, estimated } => {
                nonnegative(returned).zip(nonnegative(estimated)).is_some_and(|(rows, estimate)| rows > estimate)
            }
            Self::Unavailable { .. } => false,
        }
    }
    pub(super) fn from_node(
        node: &MssqlExecutionOperator,
        rows: &[&MssqlOperatorThreadProfile],
        operators: &MssqlExecutionOperators,
        limited: bool,
    ) -> Option<Self> {
        if !is_variable_input(node) {
            return None;
        }
        let compare = || -> std::result::Result<Self, ExecutionInputRowsUnavailable> {
            if limited {
                return Err(ExecutionInputRowsUnavailable::LimitedProfiles);
            }
            if rows.len() != 1 {
                return Err(ExecutionInputRowsUnavailable::ParallelOrMultipleThreads);
            }
            serial_path(node, operators)?;
            let row = rows[0];
            if node.kind != crate::NodeKind::Scan || row.physical_operator != node.native_operator {
                return Err(ExecutionInputRowsUnavailable::PlanTopologyUnavailable);
            }
            let counts = &row.counters;
            if counts.scans.as_deref() != Some("1")
                || counts.end_of_scans.as_deref() != Some("1")
                || !matches!(counts.rebinds.as_deref(), Some("0" | "1"))
                || counts.rewinds.as_deref() != Some("0")
            {
                return Err(ExecutionInputRowsUnavailable::RepeatedOrUnknownScans);
            }
            let ticks =
                row.open_ticks.as_deref().and_then(nonnegative).zip(row.close_ticks.as_deref().and_then(nonnegative));
            if !ticks.is_some_and(|(open, close)| open > 0 && close >= open) {
                return Err(ExecutionInputRowsUnavailable::NativeTicksUnavailable);
            }
            let (Some(returned), Some(estimated)) = (
                counts.rows_returned.as_deref().filter(|v| nonnegative(v).is_some()),
                counts.estimated_rows.as_deref().filter(|v| nonnegative(v).is_some()),
            ) else {
                return Err(ExecutionInputRowsUnavailable::OutputCountsUnavailable);
            };
            Ok(Self::SerialScan { returned: returned.into(), estimated: estimated.into() })
        };
        Some(compare().unwrap_or_else(|reason| Self::Unavailable { reason }))
    }
    pub(super) fn fits_ranges(&self, returned: &ExecutionThreadRange, estimated: &ExecutionThreadRange) -> bool {
        self.is_valid()
            && match self {
                Self::SerialScan { returned: rows, estimated: estimate } => {
                    matches_output_counts(rows, estimate, returned, estimated)
                }
                Self::Unavailable { .. } => true,
            }
    }
}
pub(super) fn is_variable_input(node: &MssqlExecutionOperator) -> bool {
    node.relational_input.is_some_and(|input| {
        matches!(
            input.qualified_kind(),
            RelationalInputKind::TableValuedParameter
                | RelationalInputKind::LocalTableVariable
                | RelationalInputKind::Unknown
        )
    })
}
fn serial_path(
    node: &MssqlExecutionOperator,
    operators: &MssqlExecutionOperators,
) -> std::result::Result<(), ExecutionInputRowsUnavailable> {
    let mut selected = node;
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(selected.node_id) {
            return Err(ExecutionInputRowsUnavailable::PlanTopologyUnavailable);
        }
        match selected.parallel {
            Some(true) => return Err(ExecutionInputRowsUnavailable::ParallelOrMultipleThreads),
            None => return Err(ExecutionInputRowsUnavailable::PlanTopologyUnavailable),
            Some(false) => (),
        }
        let Some(parent) = selected.parent_node_id else {
            return Ok(());
        };
        selected = operators.node(parent).ok_or(ExecutionInputRowsUnavailable::PlanTopologyUnavailable)?;
    }
}
pub(super) fn nonnegative(value: &str) -> Option<i64> {
    value.parse::<i64>().ok().filter(|value| *value >= 0).filter(|number| number.to_string() == value)
}

/// Both projections bind the same native counter pair; neither can replace a value with another node/range.
pub(super) fn matches_output_counts(
    rows: &str,
    estimate: &str,
    returned: &ExecutionThreadRange,
    estimated: &ExecutionThreadRange,
) -> bool {
    returned.observed == 1
        && returned.interpreted == 1
        && returned.minimum.as_deref() == Some(rows)
        && returned.maximum.as_deref() == Some(rows)
        && estimated.observed == 1
        && estimated.interpreted == 1
        && estimated.minimum.as_deref() == Some(estimate)
        && estimated.maximum.as_deref() == Some(estimate)
}
