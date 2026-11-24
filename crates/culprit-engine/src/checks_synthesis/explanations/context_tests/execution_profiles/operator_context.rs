//! Same canonical EOF/original/history owners as the existing profile path; no alternate diagnosis or source.
use super::*;
use test_support::{ParallelOperatorCase as Case, parallel_operator_fixture, parallel_operator_result};
async fn originals(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    captures_from_fixture(parallel_operator_fixture(case), sequence, false, |focus, execution| {
        parallel_operator_result(focus, execution, case)
    })
    .await
}
#[tokio::test]
async fn canonical_parallel_operator_read_context_reaches_gold_with_affected_query_predicate_caller_and_conditional_remedy()
 {
    let (basis, work, profile, history) = originals(Case::IoRead, 995001).await;
    let result = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
    result.validate().unwrap();
    let incident = &result.incidents[0];
    assert!(!incident.affected_queries.is_empty() && !incident.candidate_queries.is_empty());
    assert!(incident.culprit_queries.is_empty());
    assert!(
        incident.affected_queries.iter().chain(&incident.candidate_queries).all(|query| query.causal_role.is_none())
    );
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(incident.developer_action.contains(&t!("view.execution_profile.operator_context.read_action")));
    assert!(incident.verification.contains(&t!("view.execution_profile.operator_context.read_verify")));
    assert!(incident.entry.reasons.contains(&t!("view.execution_profile.operator_context.independent")));
    let query = incident
        .candidate_queries
        .iter()
        .find(|query| query.evidence.iter().any(|reference| reference.capture == *profile.capture()))
        .unwrap();
    assert!(query.causal_role.is_none() && query.sql.is_available());
    assert!(query.caller.as_ref().is_some_and(|caller| caller.evidence.capture == *basis.capture()));
    assert!(incident.mechanism.as_ref().unwrap().contains("dbo.items"));
    let copy: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
    assert_eq!(copy, result);
}
#[tokio::test]
async fn canonical_parallel_lock_and_ambiguous_or_recycled_points_never_invent_a_culprit_or_an_exclusive_active_operator()
 {
    for (index, case) in [
        Case::LockOwnerUnknown,
        Case::MultipleNodes,
        Case::ChangedWorker,
        Case::LimitedProfiles,
        Case::NoPlan,
        Case::ExchangeOnly,
    ]
    .into_iter()
    .enumerate()
    {
        let (basis, work, profile, history) = originals(case, 995101 + index as u64 * 10).await;
        let result = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        result.validate().unwrap();
        let incident = &result.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.candidate_queries.is_empty());
        assert!(
            incident
                .affected_queries
                .iter()
                .chain(&incident.candidate_queries)
                .all(|query| query.causal_role.is_none())
        );
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        if matches!(case, Case::LockOwnerUnknown) {
            assert!(incident.developer_action.contains(&t!("view.execution_profile.operator_context.lock_action")));
        }
        if matches!(case, Case::ChangedWorker | Case::LimitedProfiles | Case::NoPlan | Case::ExchangeOnly) {
            assert!(!incident.developer_action.contains(&t!("view.execution_profile.operator_context.read_action")));
        }
    }
}

#[tokio::test]
async fn parallel_operator_context_survives_resolution_and_never_claims_live_dop_completed_threads_or_skew() {
    let (basis, work, profile, history) = originals(Case::IoRead, 995_300).await;
    let active = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
    active.validate().unwrap();
    let active_inc = &active.incidents[0];
    assert!(active_inc.entry.reasons.contains(&t!("view.execution_profile.operator_context.not_skew")));
    assert!(active_inc.entry.reasons.contains(&t!("view.execution_profile.operator_context.independent")));

    let raw_hist = history.verified_original().unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::DiagnosticRevision(active_rev),
        ..
    }) = &raw_hist.records()[0].row().record
    else {
        panic!()
    };
    let mut empty_act = basis.verified_original().unwrap().source().clone();
    let SourceObservation::Activity(header) = &mut empty_act.observation else { panic!() };
    header.at = "2020-01-01T12:00:30Z".into();
    header.observed_from = header.at.clone();
    header.observed_until = header.at.clone();
    header.observations = 0;
    header.native_rows = 0;
    header.coverage = StateSourceCoverage::collected(empty_act.source.clone(), 0);
    let recovered = retained(empty_act, vec![], 995_310).await;
    let rec_orig = recovered.verified_original().unwrap();
    let prev = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: active_rev.episode.scope.target.clone(),
        input: active_rev.episode.latest.capture_ref(),
        episodes: vec![active_rev.episode.clone()],
    };
    let resolved_rev = observe_diagnostic_activity(
        rec_orig.source(),
        rec_orig.original(),
        &[],
        Some(&prev),
        &ActivityPolicy::default(),
        "2020-01-01T12:00:31Z",
    )
    .unwrap()
    .revisions
    .into_iter()
    .find(|r| r.episode.lifecycle == DiagnosticLifecycle::Resolved)
    .unwrap();
    drop(raw_hist);
    drop(rec_orig);

    let res_hist = history_revision(resolved_rev, 995_311).await;
    let resolved = project(&[&res_hist, &basis, &work, &profile, &recovered], vec![]).unwrap();
    resolved.validate().unwrap();
    let [res_inc] = resolved.incidents.as_slice() else { panic!() };
    assert_eq!(res_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(res_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(res_inc.culprit_queries.is_empty());
    assert_eq!(res_inc.mechanism, active_inc.mechanism);
    assert_eq!(res_inc.developer_action, active_inc.developer_action);
}
