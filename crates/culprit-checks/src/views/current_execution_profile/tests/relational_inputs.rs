use super::*;
use culprit_evidence::analytical::test_support::{
    ExecutionRelationalCase as Case, current_execution_profile_relational_fixture,
};
#[test]
fn serial_tvp_and_table_variable_use_same_execution_counts_with_conditional_action_and_no_cause_verdict() {
    for case in [Case::TableValuedParameter, Case::LocalTableVariable, Case::Spill] {
        let fixture = current_execution_profile_relational_fixture(case);
        let report = explain(&fixture.answer, point(&fixture)).unwrap();
        assert!(report.mechanisms.iter().any(|line| line.contains("10000") && line.contains("1")));
        assert!(report.evidence.nodes[0].input_rows.as_ref().unwrap().exceeds_estimate());
        assert_eq!(report.investigation, t!("view.execution_profile.input.action"));
        assert_eq!(report.verify, t!("view.execution_profile.input.verify"));
        assert!(report.limitations.contains(&t!("view.execution_profile.input.causality")));
        assert!(report.limitations.contains(&t!("view.execution_profile.input.comparison_missing")));
        let spills = report.mechanisms.iter().any(|line| line.contains("spill page writes"));
        assert_eq!(spills, matches!(case, Case::Spill));
    }
}
#[test]
fn zero_unknown_kind_repeated_parallel_limited_and_missing_bodies_keep_distinct_truthful_outputs() {
    for case in [
        Case::Healthy,
        Case::UnknownKind,
        Case::RepeatedScan,
        Case::Parallel,
        Case::MissingCounts,
        Case::Limited,
        Case::MissingBody,
        Case::Ended,
        Case::ReusedPlan,
    ] {
        let fixture = current_execution_profile_relational_fixture(case);
        let report = explain(&fixture.answer, point(&fixture)).unwrap();
        match case {
            Case::Healthy => {
                assert!(!report.evidence.nodes[0].input_rows.as_ref().unwrap().exceeds_estimate());
                assert!(report.mechanisms.iter().any(|line| line.contains("has not exceeded")));
            }
            Case::UnknownKind => {
                assert!(report.mechanisms.iter().any(|line| line.contains("kind is unknown")));
                assert!(
                    !report
                        .mechanisms
                        .iter()
                        .any(|line| line.contains("binds node") && line.contains("table-valued parameter"))
                );
            }
            Case::MissingBody | Case::Ended | Case::ReusedPlan => {
                assert!(report.evidence.nodes.iter().all(|node| node.input_rows.is_none()));
                assert!(!report.mechanisms.iter().any(|line| line.contains("table-valued parameter")));
            }
            _ => {
                assert!(matches!(report.evidence.nodes[0].input_rows, Some(ExecutionInputRows::Unavailable { .. })));
                assert!(!report.mechanisms.iter().any(|line| line.contains("output exceeds")));
            }
        }
    }
}
#[test]
fn selected_native_estimate_or_serial_denominator_tamper_cannot_override_the_authenticated_point() {
    let mut fixture = current_execution_profile_relational_fixture(Case::TableValuedParameter);
    fixture.answer.evidence.nodes[0].input_rows =
        Some(ExecutionInputRows::SerialScan { returned: "1".into(), estimated: "10000".into() });
    assert!(explain(&fixture.answer, point(&fixture)).is_err());
    let fixture = current_execution_profile_relational_fixture(Case::TableValuedParameter);
    let mut changed = point(&fixture).clone();
    let MssqlExecutionProfiles::Observed { rows, .. } = &mut changed.profiles else { panic!() };
    rows[0].counters.rewinds = Some("1".into());
    assert!(explain(&fixture.answer, &changed).is_err());
}

#[test]
fn lightweight_output_stays_cumulative_and_keeps_native_type_unknown_kind_and_existing_conditional_guidance() {
    for case in [Case::Lightweight, Case::LightweightRepeated, Case::LightweightUnknownKind] {
        let fixture = current_execution_profile_relational_fixture(case);
        let report = explain(&fixture.answer, point(&fixture)).unwrap();
        assert_eq!(report.evidence.nodes[0].output_so_far.as_ref().unwrap().returned, "512");
        assert!(matches!(
            report.evidence.nodes[0].input_rows,
            Some(ExecutionInputRows::Unavailable { reason: ExecutionInputRowsUnavailable::RepeatedOrUnknownScans })
        ));
        assert!(report.mechanisms.contains(&t!(
            "view.execution_profile.input.output_so_far",
            node = "0",
            rows = "512",
            estimate = "1"
        )));
        assert!(!report.mechanisms.iter().any(|line| line.contains("one observed serial scan")));
        assert_eq!(report.investigation, t!("view.execution_profile.input.action"));
        assert!(report.limitations.contains(&t!("view.execution_profile.input.causality")));
        if matches!(case, Case::LightweightUnknownKind) {
            assert!(report.mechanisms.iter().any(|line| line.contains("kind is unknown")));
            assert!(!report.mechanisms.iter().any(|line| line.contains("to a table-valued parameter")));
        }
    }
}

#[test]
fn partial_lightweight_output_and_foreign_or_tampered_answers_cannot_supply_pure_success() {
    let fixture = current_execution_profile_relational_fixture(Case::LightweightLimited);
    let report = explain(&fixture.answer, point(&fixture)).unwrap();
    assert!(report.evidence.nodes[0].output_so_far.is_none());
    let mut fixture = current_execution_profile_relational_fixture(Case::Lightweight);
    fixture.answer.evidence.nodes[0].output_so_far.as_mut().unwrap().returned = "511".into();
    assert!(explain(&fixture.answer, point(&fixture)).is_err());
    let fixture = current_execution_profile_relational_fixture(Case::Lightweight);
    let mut foreign = point(&fixture).clone();
    foreign.context.partition = SourcePartition::Target;
    assert!(explain(&fixture.answer, &foreign).is_err());
}
