//! Word the shared native evidence; no source access, runtime substitution or harmful-reuse verdict.
use super::*;
use culprit_core::plan_facts::RelationalInputKind;

pub(super) fn mechanisms(evidence: &CurrentExecutionProfileEvidence) -> Vec<String> {
    let mut lines = Vec::new();
    for node in &evidence.nodes {
        if let Some(comparison) = &node.input_rows {
            let input = node.operator.as_ref().and_then(|operator| operator.relational_input).unwrap();
            let kind = match input.qualified_kind() {
                RelationalInputKind::TableValuedParameter => "tvp",
                RelationalInputKind::LocalTableVariable => "local",
                _ => "unknown",
            };
            lines.push(t!(
                "view.execution_profile.input.kind",
                node = format::digits(node.node_id.to_string()),
                kind = kind
            ));
            if let ExecutionInputRows::SerialScan { returned, estimated } = comparison {
                lines.push(t!(
                    "view.execution_profile.input.output",
                    node = format::digits(node.node_id.to_string()),
                    rows = format::digits(returned.clone()),
                    estimate = format::digits(estimated.clone()),
                    comparison = if comparison.exceeds_estimate() { "above" } else { "within" }
                ));
            }
            if let Some(output) = &node.output_so_far {
                lines.push(t!(
                    "view.execution_profile.input.output_so_far",
                    node = format::digits(node.node_id.to_string()),
                    rows = format::digits(output.returned.clone()),
                    estimate = format::digits(output.estimated.clone())
                ));
            }
        }
        if node
            .spill_write_pages
            .maximum
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|value| value > 0)
        {
            lines.push(t!(
                "view.execution_profile.input.spill",
                node = format::digits(node.node_id.to_string()),
                threads = format::digits(node.spill_write_pages.interpreted.to_string()),
                minimum = format::digits(node.spill_write_pages.minimum.clone().unwrap()),
                maximum = format::digits(node.spill_write_pages.maximum.clone().unwrap())
            ));
        }
    }
    lines
}
pub(super) fn has_input(evidence: &CurrentExecutionProfileEvidence) -> bool {
    evidence.nodes.iter().any(|node| node.input_rows.is_some())
}
pub(super) fn limitations(evidence: &CurrentExecutionProfileEvidence) -> Vec<String> {
    if !has_input(evidence) {
        return vec![];
    }
    let mut lines =
        vec![t!("view.execution_profile.input.causality"), t!("view.execution_profile.input.comparison_missing")];
    for node in &evidence.nodes {
        if let Some(ExecutionInputRows::Unavailable { reason }) = &node.input_rows {
            let reason = match reason {
                ExecutionInputRowsUnavailable::LimitedProfiles => "limited",
                ExecutionInputRowsUnavailable::PlanTopologyUnavailable => "topology",
                ExecutionInputRowsUnavailable::ParallelOrMultipleThreads => "parallel",
                ExecutionInputRowsUnavailable::RepeatedOrUnknownScans => "repeated",
                ExecutionInputRowsUnavailable::NativeTicksUnavailable => "ticks",
                ExecutionInputRowsUnavailable::OutputCountsUnavailable => "counts",
            };
            lines.push(t!(
                "view.execution_profile.input.denominator",
                node = format::digits(node.node_id.to_string()),
                reason = reason
            ));
        }
    }
    lines
}
