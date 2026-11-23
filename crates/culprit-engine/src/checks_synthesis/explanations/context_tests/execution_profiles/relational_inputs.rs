//! Real canonical EOF/receipt handles, the same synthesis entry and the same Gold projection; no source factory.
use super::*;
use test_support::{
    ExecutionRelationalCase as Case, current_execution_profile_relational_fixture,
    current_execution_profile_relational_result,
};
async fn originals(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    captures_from_fixture(current_execution_profile_relational_fixture(case), sequence, false, |focus, execution| {
        current_execution_profile_relational_result(focus, execution, case)
    })
    .await
}
#[tokio::test]
async fn canonical_serial_tvp_work_remains_an_affected_candidate_with_original_caller_and_conditional_action() {
    for (offset, case) in
        [Case::TableValuedParameter, Case::LocalTableVariable, Case::Spill, Case::Healthy].into_iter().enumerate()
    {
        let (basis, work, profile, history) = originals(case, 891001 + offset as u64 * 10).await;
        let result = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        result.validate().unwrap();
        let incident = &result.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.affected_queries.is_empty());
        assert!(!incident.candidate_queries.is_empty());
        assert!(
            incident
                .affected_queries
                .iter()
                .chain(&incident.candidate_queries)
                .all(|query| query.causal_role.is_none())
        );
        let selected = incident
            .candidate_queries
            .iter()
            .find(|query| matches!(query.identity, Some(DerivedQueryIdentity::Activity { .. })))
            .unwrap();
        assert!(selected.caller.as_ref().is_some_and(|caller| caller.evidence.capture == *basis.capture()));
        assert!(selected.evidence.iter().any(|reference| reference.capture == *profile.capture()));
        assert!(incident.mechanism.as_ref().unwrap().contains("one observed serial scan"));
        assert!(incident.entry.reasons.contains(&t!("view.execution_profile.input.causality")));
        // The original current-work report and the later profile answer each retain their own guidance.
        assert_eq!(
            incident.developer_action,
            format!(
                "{}\n\n{}",
                t!("view.current_request_work.mssql.investigate"),
                t!("view.execution_profile.input.action")
            )
        );
        assert_eq!(
            incident.verification,
            format!(
                "{}\n\n{}",
                t!("view.current_request_work.mssql.verify"),
                t!("view.execution_profile.input.verify")
            )
        );
        let roundtrip: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
        assert_eq!(roundtrip, result);
    }
}
#[tokio::test]
async fn unknown_kind_ended_reused_missing_and_incompatible_work_cannot_create_a_tvp_culprit_or_reconstruct_missing_originals()
 {
    for (offset, case) in [
        Case::UnknownKind,
        Case::RepeatedScan,
        Case::Parallel,
        Case::MissingCounts,
        Case::Limited,
        Case::Ended,
        Case::ReusedPlan,
        Case::MissingBody,
    ]
    .into_iter()
    .enumerate()
    {
        let (basis, work, profile, history) = originals(case, 891101 + offset as u64 * 10).await;
        let result = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        result.validate().unwrap();
        let incident = &result.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        if matches!(case, Case::UnknownKind) {
            assert!(incident.mechanism.as_deref().unwrap_or("").contains("kind is unknown"));
            assert!(!incident.mechanism.as_deref().unwrap_or("").contains("to a table-valued parameter"));
        } else {
            assert!(!incident.mechanism.as_deref().unwrap_or("").contains("output exceeds"));
        }
        let missing = project(&[&history, &basis, &work], vec![]).unwrap();
        assert!(missing.incidents[0].culprit_queries.is_empty());
        assert!(!missing.incidents[0].mechanism.as_deref().unwrap_or("").contains("one observed serial scan"));
    }
}

#[tokio::test]
async fn canonical_lightweight_output_reaches_same_gold_with_native_caller_and_repeatable_identity_without_cause() {
    let (basis, work, profile, history) = originals(Case::Lightweight, 998001).await;
    let captures = [&history, &basis, &work, &profile];
    let result = project(&captures, vec![]).unwrap();
    result.validate().unwrap();
    let incident = &result.incidents[0];
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(incident.mechanism.as_ref().unwrap().contains(&t!(
        "view.execution_profile.input.output_so_far",
        node = "0",
        rows = "512",
        estimate = "1"
    )));
    assert!(!incident.mechanism.as_ref().unwrap().contains("one observed serial scan"));
    let query = incident
        .candidate_queries
        .iter()
        .find(|query| query.evidence.iter().any(|reference| reference.capture == *profile.capture()))
        .unwrap();
    assert!(query.causal_role.is_none() && query.sql.is_available());
    assert!(query.caller.as_ref().is_some_and(|caller| caller.evidence.capture == *basis.capture()));
    assert!(incident.developer_action.contains(&t!("view.execution_profile.input.action")));
    assert_eq!(project(&captures, vec![]).unwrap(), result);
}

#[tokio::test]
async fn partial_unknown_kind_and_foreign_profile_do_not_fill_missing_gold_execution_evidence_or_causes() {
    for (index, case) in [Case::LightweightUnknownKind, Case::LightweightLimited].into_iter().enumerate() {
        let (basis, work, profile, history) = originals(case, 998101 + index as u64 * 10).await;
        let result = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        result.validate().unwrap();
        assert!(result.incidents[0].culprit_queries.is_empty());
        let words = t!("view.execution_profile.input.output_so_far", node = "0", rows = "512", estimate = "1");
        assert_eq!(
            result.incidents[0].mechanism.as_deref().unwrap_or("").contains(&words),
            matches!(case, Case::LightweightUnknownKind)
        );
        let (_, _, foreign, _) = originals(Case::Lightweight, 998301 + index as u64 * 10).await;
        let unavailable = project(&[&history, &basis, &work, &foreign], vec![]).unwrap();
        unavailable.validate().unwrap();
        assert!(unavailable.incidents[0].culprit_queries.is_empty());
        assert!(!unavailable.incidents[0].mechanism.as_deref().unwrap_or("").contains(&words));
    }
}
