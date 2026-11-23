use super::*;
use culprit_evidence::analytical::test_support::{ExecutionProfileCase, current_execution_profile_fixture};
fn point(
    fixture: &culprit_evidence::analytical::test_support::ExecutionProfileFixture,
) -> &MssqlCurrentExecutionProfilePoint {
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &fixture.row.record
    else {
        panic!()
    };
    point
}
#[test]
fn observed_thread_ranges_and_native_waits_share_one_pure_explanation_without_skew_or_io_cause_claim() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    assert!(report.evidence.nodes[0].rows_returned.uneven());
    assert!(report.mechanisms.iter().any(|line| line.contains("9007199254740993")));
    assert!(report.mechanisms.iter().any(|line| line.contains("LCK_M_S")));
    assert!(report.mechanisms.iter().any(|line| line.contains("PAGEIOLATCH_SH")));
    assert!(report.limitations.contains(&t!("view.execution_profile.population")));
    assert!(report.limitations.contains(&t!("view.execution_profile.units")));
    assert!(report.summary.contains("2"));
    assert!(!report.investigation.is_empty() && !report.verify.is_empty());
    let copy: CurrentExecutionProfileReport = serde_json::from_slice(&serde_json::to_vec(&report).unwrap()).unwrap();
    assert_eq!(copy, report);
}
#[test]
fn absent_instrumentation_changed_actor_unknown_counters_and_worker_reuse_remain_independent_gaps() {
    for case in [
        ExecutionProfileCase::Empty,
        ExecutionProfileCase::Unavailable,
        ExecutionProfileCase::ChangedRequest,
        ExecutionProfileCase::UnknownCounters,
        ExecutionProfileCase::UnknownWorker,
        ExecutionProfileCase::NoPlanContext,
    ] {
        let fixture = current_execution_profile_fixture(case);
        let report = explain(&fixture.answer, point(&fixture)).unwrap();
        assert!(!report.limitations.is_empty());
        if matches!(case, ExecutionProfileCase::UnknownCounters) {
            assert!(!report.evidence.nodes[0].rows_returned.uneven());
            assert!(report.limitations.contains(&t!("view.execution_profile.counters_unknown")));
        }
        if matches!(case, ExecutionProfileCase::UnknownWorker) {
            assert!(report.limitations.contains(&t!("view.execution_profile.task_unverified")));
        }
        if matches!(case, ExecutionProfileCase::NoPlanContext) {
            assert!(report.limitations.contains(&t!("view.execution_profile.plan_unavailable")));
        }
    }
}
#[test]
fn changed_answer_counts_or_original_native_bounds_cannot_supply_a_pure_success() {
    let mut fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    fixture.answer.evidence.nodes[0].rows_returned.maximum = Some("12".into());
    assert!(explain(&fixture.answer, point(&fixture)).is_err());
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let mut changed = point(&fixture).clone();
    changed.context.observed_until = "2020-01-01T12:00:05Z".into();
    assert!(explain(&fixture.answer, &changed).is_err());
}

mod relational_inputs;

mod operator_context;
