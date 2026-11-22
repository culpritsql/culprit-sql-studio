//! Explain one authenticated native point. Assignment is a narrower role than harmful SQL or exclusive cause.
use culprit_core::{Result, format, t, telemetry::*};

pub fn explain(answer: &WorkerStarvationAnswer, point: &MssqlWorkerStarvationPoint) -> Result<WorkerStarvationReport> {
    if !answer.is_valid()
        || !point.is_valid()
        || point.context != answer.result_context
        || point.worker_scope != answer.worker_scope
        || point.observed_at != answer.observed_at
        || WorkerStarvationEvidence::from_point(point)? != answer.evidence
    {
        return Err(CollectionStop::InvalidPage.error());
    }
    let evidence = &answer.evidence;
    let mut mechanisms = Vec::new();
    for kind in [
        WorkerContributionKind::AssignedParallelWorkers,
        WorkerContributionKind::PreemptiveAssignedWorkers,
        WorkerContributionKind::LockWaitingAssignedWorkers,
    ] {
        let count = evidence
            .contributions
            .iter()
            .filter(|row| row.kind == kind && row.queue_scheduler_id.is_some())
            .map(|row| row.assigned_workers)
            .sum::<u32>();
        if count > 0 {
            mechanisms.push(t!(
                "view.worker_starvation.mechanism",
                kind = match kind {
                    WorkerContributionKind::AssignedParallelWorkers => "parallel",
                    WorkerContributionKind::PreemptiveAssignedWorkers => "preemptive",
                    WorkerContributionKind::LockWaitingAssignedWorkers => "blocked",
                },
                count = format::digits(count.to_string())
            ));
        }
    }
    if mechanisms.is_empty() {
        mechanisms.push(t!("view.worker_starvation.no_qualified_contributor"));
    }
    let mut limitations = limitations(evidence);
    if answer.source_state == culprit_core::SourceState::Partial {
        limitations.push(t!("view.worker_starvation.partial"));
    }
    Ok(WorkerStarvationReport {
        context: answer.result_context.clone(),
        worker_scope: answer.worker_scope.clone(),
        observed_at: answer.observed_at.clone(),
        source_state: answer.source_state,
        evidence: evidence.clone(),
        summary: summary(answer),
        mechanisms,
        investigation: t!("view.worker_starvation.action"),
        verify: t!("view.worker_starvation.verify"),
        limitations,
    })
}
pub fn summary(answer: &WorkerStarvationAnswer) -> String {
    t!(
        "view.worker_starvation.summary",
        tasks = format::digits(answer.evidence.observed_tasks.to_string()),
        workers = format::digits(answer.evidence.assigned_workers.to_string()),
        waits = format::digits(answer.evidence.observed_threadpool_waits.to_string())
    )
}
pub fn limitations(evidence: &WorkerStarvationEvidence) -> Vec<String> {
    let mut reasons = vec![
        t!("view.worker_starvation.scope"),
        t!("view.worker_starvation.clocks"),
        t!("view.worker_starvation.units"),
        t!("view.worker_starvation.caller"),
    ];
    if evidence.tasks_limited || evidence.waits_limited {
        reasons.push(t!("view.worker_starvation.limited"));
    }
    if evidence.unassigned_threadpool_waits > 0 {
        reasons.push(t!(
            "view.worker_starvation.unassigned",
            count = format::digits(evidence.unassigned_threadpool_waits.to_string())
        ));
    }
    if evidence.impacted_schedulers.is_empty() {
        reasons.push(t!("view.worker_starvation.scheduler_unknown"));
    }
    let actors = evidence.contributions.iter().map(|row| &row.request).collect::<std::collections::BTreeSet<_>>();
    let owners = evidence.blocking.iter().map(|row| &row.owner).collect::<Vec<_>>();
    if evidence.affected_requests.len() > MAX_PRESENTED_WORKER_ACTORS
        || actors.len() > MAX_PRESENTED_WORKER_ACTORS
        || owners.len() > MAX_PRESENTED_WORKER_ACTORS
    {
        reasons.push(t!(
            "view.worker_starvation.presentation_limited",
            count = format::digits(MAX_PRESENTED_WORKER_ACTORS.to_string())
        ));
    }
    if evidence.unverified_worker_bindings > 0 {
        reasons.push(t!("view.worker_starvation.worker_unverified"));
    }
    if evidence.unqualified_requests > 0 {
        reasons.push(t!("view.worker_starvation.request_unverified"));
    }
    reasons
}
#[cfg(test)]
mod tests;
