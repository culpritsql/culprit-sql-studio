//! One pure explanation over the authenticated current-execution point. Uneven output is descriptive.
use culprit_core::{Result, format, t, telemetry::*};

pub fn explain(
    answer: &CurrentExecutionProfileAnswer,
    point: &MssqlCurrentExecutionProfilePoint,
) -> Result<CurrentExecutionProfileReport> {
    if !answer.is_valid()
        || !point.is_valid()
        || point.context != answer.result_context
        || point.observed_at != answer.observed_at
        || point.checked_until != answer.checked_until
        || point.binding != answer.binding
        || point.membership != answer.membership
        || CurrentExecutionProfileEvidence::from_point(point)? != answer.evidence
        || answer.profiles != point.profiles.availability()
    {
        return Err(CollectionStop::InvalidPage.error());
    }
    let mut mechanisms = Vec::new();
    for node in &answer.evidence.nodes {
        if node.rows_returned.uneven()
            && let (Some(minimum), Some(maximum)) = (&node.rows_returned.minimum, &node.rows_returned.maximum)
        {
            mechanisms.push(t!(
                "view.execution_profile.uneven",
                node = format::digits(node.node_id.to_string()),
                threads = format::digits(node.rows_returned.observed.to_string()),
                minimum = format::digits(minimum.clone()),
                maximum = format::digits(maximum.clone())
            ));
        }
        if let Some(operator) = &node.operator
            && let Some(object) = &operator.object
        {
            mechanisms.push(t!(
                "view.execution_profile.operator",
                node = format::digits(node.node_id.to_string()),
                operator = culprit_core::format::escaped_preview(&operator.native_operator, 256).text,
                object = culprit_core::format::escaped_preview(object, 512).text
            ));
        }
        let waits = node
            .threads
            .iter()
            .filter_map(|thread| thread.endpoint_wait.as_ref())
            .map(|wait| wait.native_type.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for wait in waits {
            mechanisms.push(t!(
                "view.execution_profile.endpoint_wait",
                node = format::digits(node.node_id.to_string()),
                wait = culprit_core::format::escaped_preview(wait, 60).text
            ));
        }
    }
    mechanisms.extend(relational_inputs::mechanisms(&answer.evidence));
    if mechanisms.is_empty() {
        mechanisms.push(t!("view.execution_profile.no_pattern"));
    }
    let mut limitations = limitations(answer);
    match &point.profiles {
        MssqlExecutionProfiles::Unavailable { .. } => limitations.push(t!("view.execution_profile.unavailable")),
        MssqlExecutionProfiles::Observed { rows, operators, .. } => {
            if rows.is_empty() {
                limitations.push(t!("view.execution_profile.not_observed"));
            }
            if matches!(operators, MssqlExecutionOperators::Unavailable { .. }) {
                limitations.push(t!("view.execution_profile.plan_unavailable"));
            }
        }
    }
    let mut report = CurrentExecutionProfileReport {
        context: point.context.clone(),
        source_state: answer.source_state,
        evidence: answer.evidence.clone(),
        parallel_context: ParallelOperatorContext::from_point(point)?,
        summary: summary(answer),
        mechanisms,
        investigation: if relational_inputs::has_input(&answer.evidence) {
            t!("view.execution_profile.input.action")
        } else {
            t!("view.execution_profile.action")
        },
        verify: if relational_inputs::has_input(&answer.evidence) {
            t!("view.execution_profile.input.verify")
        } else {
            t!("view.execution_profile.verify")
        },
        limitations,
    };
    operator_context::append(&mut report);
    Ok(report)
}
pub fn summary(answer: &CurrentExecutionProfileAnswer) -> String {
    match answer.profiles {
        ExecutionProfileAvailability::Unavailable { .. } => return t!("view.execution_profile.unavailable"),
        ExecutionProfileAvailability::NotObserved => return t!("view.execution_profile.not_observed"),
        _ => (),
    }

    t!(
        "view.execution_profile.summary",
        threads = format::digits(answer.evidence.profile_rows.to_string()),
        nodes = format::digits(answer.evidence.profile_nodes.to_string())
    )
}
pub fn limitations(answer: &CurrentExecutionProfileAnswer) -> Vec<String> {
    let evidence = &answer.evidence;
    let mut reasons = vec![
        t!("view.execution_profile.population"),
        t!("view.execution_profile.clocks"),
        t!("view.execution_profile.plan_context"),
        t!("view.execution_profile.caller"),
        t!("view.execution_profile.units"),
    ];
    reasons.extend(relational_inputs::limitations(evidence));
    if evidence.profiles_limited || evidence.tasks_limited {
        reasons.push(t!("view.execution_profile.limited"));
    }
    if evidence.nodes_limited {
        reasons.push(t!(
            "view.execution_profile.presentation_limited",
            count = format::digits(MAX_PRESENTED_EXECUTION_NODES.to_string())
        ));
    }
    if evidence
        .nodes
        .iter()
        .flat_map(|node| &node.threads)
        .any(|thread| thread.task_context != ExecutionTaskContext::CheckedAtBounds)
    {
        reasons.push(t!("view.execution_profile.task_unverified"));
    }
    if evidence.nodes.iter().any(|node| {
        node.rows_returned.interpreted != node.rows_returned.observed
            || node.rows_read.interpreted != node.rows_read.observed
    }) {
        reasons.push(t!("view.execution_profile.counters_unknown"));
    }
    if answer.source_state == culprit_core::SourceState::Partial {
        reasons.push(t!("view.execution_profile.partial"));
    }
    reasons
}
mod operator_context;
mod relational_inputs;
#[cfg(test)]
mod tests;
