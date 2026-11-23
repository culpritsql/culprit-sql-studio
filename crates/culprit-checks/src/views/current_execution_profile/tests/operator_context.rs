use super::*;
use culprit_evidence::analytical::test_support::{ParallelOperatorCase as Case, parallel_operator_fixture};
#[test]
fn checked_io_task_and_exact_operator_work_select_conditional_read_guidance_without_active_node_or_skew_claim() {
    let fixture = parallel_operator_fixture(Case::IoRead);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    let [context] = report.parallel_context.observations.as_slice() else { panic!() };
    assert_eq!(context.kind, ExecutionTaskWaitKind::BufferRead);
    assert_eq!((context.node_id, context.thread_id), (0, 12));
    assert_eq!(context.physical_reads.as_deref(), Some("64"));
    assert_eq!(context.checked_tasks_at_node, 2);
    assert_eq!(context.rows_read.as_deref(), Some("1000"));
    assert_eq!(context.rows_returned.as_deref(), Some("10"));
    assert!(context.read_more_than_returned());
    assert_eq!(context.operator.object.as_deref(), Some("dbo.items"));
    assert!(context.operator.predicate.is_available());
    assert_eq!(context.profile_observed_until, "2020-01-01T12:00:03.6Z");
    assert_eq!(context.task_observed_at, "2020-01-01T12:00:03.7Z");
    assert!(report.investigation.contains(&t!("view.execution_profile.operator_context.read_action")));
    assert!(report.verify.contains(&t!("view.execution_profile.operator_context.read_verify")));
    assert!(report.limitations.contains(&t!("view.execution_profile.operator_context.independent")));
    assert!(report.limitations.contains(&t!("view.execution_profile.operator_context.not_skew")));
    let decoded: CurrentExecutionProfileReport = serde_json::from_slice(&serde_json::to_vec(&report).unwrap()).unwrap();
    assert_eq!(decoded, report);
}
#[test]
fn lock_wait_can_locate_the_affected_task_without_manufacturing_blocker_generation_or_acquiring_sql() {
    let fixture = parallel_operator_fixture(Case::LockOwnerUnknown);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    assert_eq!(report.parallel_context.observations[0].kind, ExecutionTaskWaitKind::NativeLock);
    assert!(report.parallel_context.gaps.contains(&ParallelOperatorContextGap::BlockingOwnerIdentityUnavailable));
    assert!(report.investigation.contains(&t!("view.execution_profile.operator_context.lock_action")));
    assert!(!report.investigation.contains(&t!("view.execution_profile.operator_context.read_action")));
}
#[test]
fn shared_task_multiple_nodes_remain_ambiguous_and_exchange_or_uneven_rows_never_supply_skew() {
    let fixture = parallel_operator_fixture(Case::MultipleNodes);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    assert_eq!(report.parallel_context.observations.len(), 1);
    assert!(report.parallel_context.observations.iter().all(|context| context.profiled_nodes_for_task == 2));
    assert!(report.parallel_context.gaps.contains(&ParallelOperatorContextGap::MultipleProfiledOperators));
    let fixture = parallel_operator_fixture(Case::ExchangeOnly);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    assert!(report.evidence.nodes[0].rows_returned.uneven());
    assert!(report.parallel_context.observations.is_empty());
    assert!(report.parallel_context.gaps.contains(&ParallelOperatorContextGap::ExchangeCauseUnavailable));
    assert_eq!(report.investigation, t!("view.execution_profile.action"));
}
#[test]
fn changed_unqualified_limited_or_unknown_native_points_withhold_specific_remedies_and_preserve_serial_advice() {
    for case in [
        Case::ChangedWorker,
        Case::LimitedProfiles,
        Case::LimitedTasks,
        Case::NoPlan,
        Case::UnknownCounts,
        Case::NegativeCounts,
        Case::NoPhysicalReads,
        Case::Serial,
    ] {
        let fixture = parallel_operator_fixture(case);
        let report = explain(&fixture.answer, point(&fixture)).unwrap();
        assert!(report.parallel_context.observations.is_empty(), "{case:?}");
        if matches!(case, Case::Serial) {
            assert!(report.parallel_context.gaps.is_empty());
        } else {
            assert!(!report.parallel_context.gaps.is_empty(), "{case:?}");
        }
        assert_eq!(report.investigation, t!("view.execution_profile.action"));
        assert!(!report.limitations.is_empty());
    }
}
