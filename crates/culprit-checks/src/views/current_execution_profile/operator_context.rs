//! Render one typed operator/task context. Endpoints are independent; no active-operator or skew detector.
use super::*;
use std::collections::BTreeSet;
pub(super) fn append(report: &mut CurrentExecutionProfileReport) {
    let kinds = append_observations(report);
    if !report.parallel_context.observations.is_empty() {
        report.limitations.push(t!("view.execution_profile.operator_context.independent"));
        report.limitations.push(t!("view.execution_profile.operator_context.not_skew"));
    }
    append_actions(report, kinds);
    for gap in &report.parallel_context.gaps {
        report.limitations.push(t!("view.execution_profile.operator_context.gap", reason = gap_key(*gap)));
    }
}
fn append_observations(report: &mut CurrentExecutionProfileReport) -> BTreeSet<ExecutionTaskWaitKind> {
    let mut kinds = BTreeSet::new();
    let mut described_nodes = BTreeSet::new();
    let already_named = report
        .evidence
        .nodes
        .iter()
        .filter(|node| node.operator.as_ref().is_some_and(|operator| operator.object.is_some()))
        .map(|node| node.node_id)
        .collect::<BTreeSet<_>>();
    for observation in &report.parallel_context.observations {
        let key = match observation.kind {
            ExecutionTaskWaitKind::BufferRead => "read",
            ExecutionTaskWaitKind::NativeLock => "lock",
            ExecutionTaskWaitKind::ExchangeSynchronization => continue,
        };
        kinds.insert(observation.kind);
        append_point(observation, key, &mut report.mechanisms);
        if described_nodes.insert(observation.node_id) {
            if !already_named.contains(&observation.node_id)
                && let Some(object) = &observation.operator.object
            {
                report.mechanisms.push(t!(
                    "view.execution_profile.operator",
                    node = format::digits(observation.node_id.to_string()),
                    operator = format::escaped_preview(&observation.operator.native_operator, 256).text,
                    object = format::escaped_preview(object, 512).text
                ));
            }
            append_predicate(observation, &mut report.mechanisms);
        }
    }
    kinds
}
fn append_actions(report: &mut CurrentExecutionProfileReport, kinds: BTreeSet<ExecutionTaskWaitKind>) {
    for kind in kinds {
        let (action, verify) = match kind {
            ExecutionTaskWaitKind::BufferRead => (
                t!("view.execution_profile.operator_context.read_action"),
                t!("view.execution_profile.operator_context.read_verify"),
            ),
            ExecutionTaskWaitKind::NativeLock => (
                t!("view.execution_profile.operator_context.lock_action"),
                t!("view.execution_profile.operator_context.lock_verify"),
            ),
            ExecutionTaskWaitKind::ExchangeSynchronization => continue,
        };
        report.investigation.push_str("\n\n");
        report.investigation.push_str(&action);
        report.verify.push_str("\n\n");
        report.verify.push_str(&verify);
    }
}

fn append_point(observation: &ParallelOperatorObservation, key: &str, mechanisms: &mut Vec<String>) {
    mechanisms.push(t!(
        "view.execution_profile.operator_context.point",
        node = format::digits(observation.node_id.to_string()),
        thread = format::digits(observation.thread_id.to_string()),
        kind = key,
        profile_from = observation.profile_observed_from.as_str(),
        profile_until = observation.profile_observed_until.as_str(),
        task_at = observation.task_observed_at.as_str()
    ));
    if let Some(reads) = &observation.physical_reads {
        mechanisms.push(t!(
            "view.execution_profile.operator_context.reads",
            node = format::digits(observation.node_id.to_string()),
            thread = format::digits(observation.thread_id.to_string()),
            reads = format::digits(reads.clone())
        ));
    }
    if observation.read_more_than_returned() {
        mechanisms.push(t!(
            "view.execution_profile.operator_context.read_output",
            node = format::digits(observation.node_id.to_string()),
            thread = format::digits(observation.thread_id.to_string()),
            read = format::digits(observation.rows_read.clone().unwrap()),
            returned = format::digits(observation.rows_returned.clone().unwrap())
        ));
    }
}

fn append_predicate(observation: &ParallelOperatorObservation, mechanisms: &mut Vec<String>) {
    for payload in [&observation.operator.predicate, &observation.operator.seek_predicate] {
        if let DefinitionPayload::Available { text, .. } = payload {
            mechanisms.push(t!(
                "view.execution_profile.operator_context.predicate",
                node = format::digits(observation.node_id.to_string()),
                predicate = format::escaped_preview(text, 256).text
            ));
        }
    }
}
fn gap_key(gap: ParallelOperatorContextGap) -> &'static str {
    match gap {
        ParallelOperatorContextGap::ParticipantChanged => "participant",
        ParallelOperatorContextGap::ProfilesUnavailable => "profiles",
        ParallelOperatorContextGap::LimitedProfiles => "profile-limit",
        ParallelOperatorContextGap::LimitedTasks => "task-limit",
        ParallelOperatorContextGap::PlanUnavailable => "plan",
        ParallelOperatorContextGap::SerialOrUnknownTopology => "serial",
        ParallelOperatorContextGap::InsufficientCheckedTasks => "few-tasks",
        ParallelOperatorContextGap::UnverifiedTask => "task",
        ParallelOperatorContextGap::UnsupportedWait => "wait",
        ParallelOperatorContextGap::NoObservedPhysicalReads => "no-read-count",
        ParallelOperatorContextGap::UnknownWorkCounters => "counters",
        ParallelOperatorContextGap::MultipleProfiledOperators => "many-nodes",
        ParallelOperatorContextGap::BlockingOwnerIdentityUnavailable => "blocker",
        ParallelOperatorContextGap::ExchangeCauseUnavailable => "exchange",
        ParallelOperatorContextGap::PresentationLimited => "presentation",
    }
}
