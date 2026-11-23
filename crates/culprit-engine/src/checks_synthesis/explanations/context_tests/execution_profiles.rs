//! The existing neutral EOF/proof reader authenticates all inputs before shared source-free synthesis.
use super::*;
use test_support::{ExecutionProfileCase, current_execution_profile_fixture, current_execution_profile_result};
async fn captures(
    case: ExecutionProfileCase,
    sequence: u64,
) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    captures_with_context(case, sequence, false).await
}
pub(super) async fn captures_with_input_context(
    case: ExecutionProfileCase,
    sequence: u64,
) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    captures_with_context(case, sequence, true).await
}
async fn captures_with_context(
    case: ExecutionProfileCase,
    sequence: u64,
    include_context: bool,
) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    captures_from_fixture(
        current_execution_profile_fixture(ExecutionProfileCase::Observed),
        sequence,
        include_context,
        |focus, execution| {
            if include_context {
                test_support::current_execution_profile_result_with_input_context(focus, execution, case)
            } else {
                current_execution_profile_result(focus, execution, case)
            }
        },
    )
    .await
}
async fn captures_from_fixture(
    fixture: test_support::ExecutionProfileFixture,
    sequence: u64,
    include_context: bool,
    build_result: impl FnOnce(&SourceReadFocus, &SourceReadExecutionScope) -> (CollectionDescription, Vec<TelemetryRecord>),
) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    let basis = retained(fixture.basis_source, vec![fixture.basis.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let first = original.reference(0).unwrap();
    let mut work_source = fixture.work_source;
    let SourceObservation::CurrentRequestWork { result, .. } = &mut work_source.observation else { panic!() };
    result.focus.as_mut().unwrap().parent = first.clone();
    let work = retained(work_source, vec![fixture.work.record], sequence + 1).await;
    let detail = work.verified_original().unwrap();
    let work_ref = detail.reference(0).unwrap();
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(original.source()).unwrap(),
        fixture.episode.signal,
        fixture.episode.first_seen_at,
        first,
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: work_ref.clone() });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        detail.source(),
        &work_ref,
        detail.records()[0].row(),
    )
    .unwrap()
    .unwrap();
    if include_context {
        let retained = SourceReadFocus::from_followup_input(
            SourceReadQuestion::RelationalInputContext,
            &episode,
            detail.source(),
            &work_ref,
            detail.records()[0].row(),
        )
        .unwrap()
        .unwrap();
        episode = episode
            .enroll_source_read(retained.clone())
            .unwrap()
            .transition_source_read(
                &retained,
                SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::EvidenceUnavailable },
            )
            .unwrap();
    }
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(original.source()).unwrap())
            .unwrap();
    let (source, rows) = build_result(&focus, &execution);
    let captured = retained(source, rows, sequence + 2).await;
    let output = captured.verified_original().unwrap();
    let selected = output.reference(0).unwrap();
    let answer = CurrentExecutionProfileAnswer::from_originals(
        &focus,
        original.source(),
        original.records()[0].row(),
        output.source(),
        output.original(),
        &[(&selected, output.records()[0].row())],
    )
    .unwrap();
    let episode = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(answer))),
            },
        )
        .unwrap();
    drop(original);
    drop(detail);
    drop(output);
    let recorded = history(episode, sequence + 3).await;
    (basis, work, captured, recorded)
}
#[tokio::test]
async fn canonical_profile_uses_one_pure_gold_owner_and_keeps_affected_candidate_and_unknown_culprits_distinct() {
    let (basis, work, profile, history) = captures(ExecutionProfileCase::Observed, 890_001).await;
    let diagnosis = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
    diagnosis.validate().unwrap();
    let incident = &diagnosis.incidents[0];
    assert!(!incident.affected_queries.is_empty());
    assert!(incident.affected_queries.iter().all(|query| query.causal_role.is_none()));
    assert!(
        incident
            .candidate_queries
            .iter()
            .any(|query| matches!(query.identity, Some(DerivedQueryIdentity::Activity { .. }))
                && query.sql.is_available())
    );
    assert!(
        incident.culprit_queries.is_empty(),
        "a profiled victim/task wait or uneven row count does not identify harmful SQL or a blocker generation"
    );
    let candidate = incident
        .candidate_queries
        .iter()
        .find(|query| matches!(query.identity, Some(DerivedQueryIdentity::Activity { .. })))
        .unwrap();
    assert!(candidate.caller.as_ref().is_some_and(|caller| caller.evidence.capture == *basis.capture()));
    assert!(incident.mechanism.as_ref().unwrap().contains("9007199254740993"));
    assert!(incident.mechanism.as_ref().unwrap().contains("PAGEIOLATCH_SH"));
    assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
    assert!(incident.entry.reasons.contains(&t!("view.execution_profile.population")));
    let read: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&diagnosis).unwrap()).unwrap();
    assert_eq!(read, diagnosis);
}
#[tokio::test]
async fn missing_profile_body_changed_actor_and_unavailable_profiling_cannot_borrow_a_culprit_or_silently_become_complete()
 {
    for case in [
        ExecutionProfileCase::ChangedRequest,
        ExecutionProfileCase::Unavailable,
        ExecutionProfileCase::Empty,
        ExecutionProfileCase::UnknownWorker,
    ] {
        let (basis, work, profile, history) = captures(case, 890_011).await;
        let diagnosis = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        diagnosis.validate().unwrap();
        assert!(diagnosis.incidents[0].culprit_queries.is_empty());
        if matches!(
            case,
            ExecutionProfileCase::ChangedRequest | ExecutionProfileCase::Unavailable | ExecutionProfileCase::Empty
        ) {
            assert!(
                diagnosis.incidents[0]
                    .entry
                    .reasons
                    .iter()
                    .any(|reason| reason == &t!("view.execution_profile.unavailable")
                        || reason == &t!("view.execution_profile.not_observed"))
            );
        }
        let missing = project(&[&history, &basis, &work], vec![]).unwrap();
        assert!(missing.incidents[0].culprit_queries.is_empty());
        assert!(
            !missing.incidents[0].mechanism.as_ref().is_some_and(|text| text.contains("9007199254740993")),
            "serialized answer counts cannot replace a missing original profile"
        );
    }
}
#[tokio::test]
async fn tampered_answer_ranges_cannot_override_authenticated_operator_rows() {
    let (basis, work, profile, history) = captures(ExecutionProfileCase::Observed, 890_021).await;
    let recorded = history.verified_original().unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::DiagnosticRevision(revision),
        ..
    }) = &recorded.records()[0].row().record
    else {
        panic!()
    };
    let mut episode = revision.episode.clone();
    let mut stages = episode.source_reads.iter().cloned().collect::<Vec<_>>();
    let stage =
        stages.iter_mut().find(|stage| stage.focus.question == SourceReadQuestion::CurrentExecutionProfile).unwrap();
    let SourceReadStageState::Answered { answer } = &mut stage.state else { panic!() };
    let SourceReadAnswer::CurrentExecutionProfile(answer) = answer.as_mut() else { panic!() };
    answer.evidence.nodes[0].rows_returned.maximum = Some("9007199254740992".into());
    let originals = [
        basis.verified_original().unwrap(),
        work.verified_original().unwrap(),
        profile.verified_original().unwrap(),
        history.verified_original().unwrap(),
    ];
    let refs = originals.iter().map(|input| input as &dyn OriginalProjection).collect::<Vec<_>>();
    if let Ok(stages) = SourceReadStages::from_stages(stages) {
        episode.source_reads = stages;
        assert!(for_episode(&refs, &episode).is_err());
    }
}

mod relational_inputs;

mod operator_context;
