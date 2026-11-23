//! Preserve useful lightweight-profile output without manufacturing an execution/traversal denominator.
use super::input_rows::{matches_output_counts, nonnegative};
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OutputRowsSoFar {
    pub returned: String,
    pub estimated: String,
    /// The existing strict serial-scan refusal remains authoritative and separately visible.
    pub normalization_unavailable: ExecutionInputRowsUnavailable,
}
impl OutputRowsSoFar {
    pub fn is_valid(&self) -> bool {
        nonnegative(&self.returned).is_some()
            && nonnegative(&self.estimated).is_some()
            && matches!(
                self.normalization_unavailable,
                ExecutionInputRowsUnavailable::RepeatedOrUnknownScans
                    | ExecutionInputRowsUnavailable::NativeTicksUnavailable
            )
    }
    pub(super) fn from_rows(
        comparison: Option<&ExecutionInputRows>,
        rows: &[&MssqlOperatorThreadProfile],
    ) -> Option<Self> {
        // These two refusals occur only after the existing owner checked a nonlimited, single-thread,
        // explicitly serial path and the exact native scan/operator. Reuse that decision once.
        let Some(ExecutionInputRows::Unavailable { reason }) = comparison else { return None };
        let [row] = rows else { return None };
        let value = Self {
            returned: row.counters.rows_returned.clone()?,
            estimated: row.counters.estimated_rows.clone()?,
            normalization_unavailable: *reason,
        };
        value.is_valid().then_some(value)
    }
    pub(super) fn fits_node(&self, node: &ExecutionProfileNodeEvidence) -> bool {
        self.is_valid()
            && matches!(&node.input_rows, Some(ExecutionInputRows::Unavailable { reason }) if *reason == self.normalization_unavailable)
            && matches_output_counts(&self.returned, &self.estimated, &node.rows_returned, &node.estimated_rows)
    }
}

#[cfg(test)]
mod tests;
