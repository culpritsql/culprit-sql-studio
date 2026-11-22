use super::*;
#[test]
fn unknown_scheduler_unassigned_wait_and_partial_actor_context_are_independent_qualifications() {
    let evidence = WorkerStarvationEvidence {
        observed_tasks: 0,
        assigned_workers: 0,
        observed_threadpool_waits: 1,
        unassigned_threadpool_waits: 1,
        affected_requests: vec![],
        impacted_schedulers: vec![],
        contributions: vec![],
        blocking: vec![],
        tasks_limited: true,
        waits_limited: false,
        unqualified_requests: 1,
        unverified_worker_bindings: 0,
    };
    assert!(evidence.is_valid());
    let reasons = limitations(&evidence);
    for reason in [
        t!("view.worker_starvation.scope"),
        t!("view.worker_starvation.units"),
        t!("view.worker_starvation.clocks"),
        t!("view.worker_starvation.scheduler_unknown"),
        t!("view.worker_starvation.request_unverified"),
        t!("view.worker_starvation.limited"),
    ] {
        assert!(reasons.contains(&reason));
    }
}
