use super::*;
use culprit_evidence::analytical::test_support::{
    MetadataLockContextCase as Case, metadata_lock_context_result, metadata_lock_original,
};
async fn scenario(
    case: Case,
    sequence: u64,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, MetadataLockContextAnswer) {
    let input = metadata_lock_original();
    let basis = retained(input.source, vec![input.original.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let TelemetryRecord::Activity(actor) = &basis.records()[0].row().record else { panic!() };
    let update = observe_diagnostic_activity(
        basis.source(),
        original.original(),
        &[DiagnosticActivityRow { reference: &reference, observation: actor }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:01Z",
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::LockWait { .. }))
        .unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::MetadataLockContext,
        &episode,
        basis.source(),
        &reference,
        basis.records()[0].row(),
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
            .unwrap();
    let (source, rows) = metadata_lock_context_result(&focus, &execution, case);
    let result = retained(source, rows, sequence + 1).await;
    let retained = result.verified_original().unwrap();
    let refs = (0..result.records().len()).map(|i| retained.reference(i).unwrap()).collect::<Vec<_>>();
    let pairs = refs.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = MetadataLockContextAnswer::from_originals(
        &focus,
        basis.source(),
        basis.records()[0].row(),
        result.source(),
        retained.original(),
        &pairs,
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
                answer: Box::new(SourceReadAnswer::MetadataLockContext(Box::new(answer.clone()))),
            },
        )
        .unwrap();
    (basis, result, episode, answer)
}
#[tokio::test]
async fn canonical_both_flavor_tickets_publish_affected_and_candidate_native_callers_without_a_proved_sandwich() {
    for (index, case) in [Case::Mysql, Case::Mariadb, Case::MixedTickets, Case::MissingSql, Case::UnknownTransaction]
        .into_iter()
        .enumerate()
    {
        let (basis, result, episode, answer) = scenario(case, 981_000 + 10 * index as u64).await;
        let history = history(episode, 981_002 + 10 * index as u64).await;
        let output = project(&[&history, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(answer.is_valid());
        assert_eq!(answer.evidence.candidates.len(), 1);
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        assert_eq!(incident.candidate_queries.len(), 2);
        assert!(incident.candidate_queries.iter().all(|query| query.causal_role.is_none() && query.caller.is_some()));
        assert!(incident.affected_queries.iter().any(
            |query| matches!(&query.identity,Some(DerivedQueryIdentity::Activity {actor,..}) if actor.session.id=="701")
        ));
        let sleeping=incident.candidate_queries.iter().find(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity {actor,..}) if actor.session.id=="703")).unwrap();
        assert!(!sleeping.sql.is_available());
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(!serde_json::to_string(&output).unwrap().contains("mdl-private-canary"));
        assert!(output.findings.is_empty());
    }
}
#[tokio::test]
async fn canonical_partial_changed_ended_unknown_or_other_object_context_cannot_become_a_culprit_or_healthy_absence() {
    for (index, case) in [
        Case::UnknownEvent,
        Case::Prefix,
        Case::ChangedTicket,
        Case::EndedRequest,
        Case::OtherObject,
        Case::UnknownMode,
        Case::Convert,
    ]
    .into_iter()
    .enumerate()
    {
        let (basis, result, episode, answer) = scenario(case, 982_000 + 10 * index as u64).await;
        let history = history(episode, 982_002 + 10 * index as u64).await;
        let output = project(&[&history, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(answer.evidence.candidates.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.candidate_queries.is_empty());
        assert!(!incident.affected_queries.is_empty());
        assert!(!incident.entry.reasons.is_empty());
    }
}
#[tokio::test]
async fn a_replayed_row_receipt_changed_capture_or_foreign_native_owner_cannot_replace_the_authenticated_answer() {
    let (basis, result, episode, answer) = scenario(Case::Mysql, 983_000).await;
    let stage = episode.source_reads.get(SourceReadQuestion::MetadataLockContext).unwrap();
    assert!(episode.transition_source_read(&stage.focus, SourceReadStageState::Collecting).is_err());
    let original = result.verified_original().unwrap();
    let mut refs = (0..result.records().len()).map(|i| original.reference(i).unwrap()).collect::<Vec<_>>();
    refs[1] = refs[0].clone();
    let pairs = refs.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    assert!(
        MetadataLockContextAnswer::from_originals(
            &stage.focus,
            basis.source(),
            basis.records()[0].row(),
            result.source(),
            original.original(),
            &pairs
        )
        .is_err()
    );
    let mut changed = answer;
    changed.result_context.partition = SourcePartition::Target;
    assert!(!changed.is_valid());
}
#[cfg(feature = "collector")]
#[tokio::test]
async fn the_existing_local_capture_owner_seals_the_selected_cohort_and_reopens_it_without_native_queries() {
    let (source, rows) = test_support::metadata_lock_context_evidence(Case::Mysql);
    let fixture = culprit_collector::analytical::test_support::LocalCaptureFixture::activity(source, rows).await;
    assert_eq!(fixture.manifest.status, CollectionStatus::Complete);
    assert_eq!(fixture.manifest.source_queries, 0);
    // This canonical-file fixture has no database/provider and adds no synthesis reread.
    let source = fixture.manifest.description.as_ref().unwrap();
    assert!(matches!(source.observation, SourceObservation::MetadataLockContext { .. }));
}

#[tokio::test]
async fn a_recorded_answer_without_its_selected_native_cohort_keeps_history_and_exposes_missing_context() {
    let (basis, _result, episode, _answer) = scenario(Case::Mysql, 984_000).await;
    let history = history(episode, 984_002).await;
    let output = project(&[&history, &basis], vec![]).unwrap();
    let incident = &output.incidents[0];
    assert!(incident.culprit_queries.is_empty());
    assert!(incident.candidate_queries.is_empty());
    assert!(!incident.affected_queries.is_empty());
    assert!(
        incident
            .entry
            .reasons
            .iter()
            .any(|reason| reason == &culprit_core::t!("view.metadata_queue.candidate.missing"))
    );
}

#[cfg(all(feature = "collector", feature = "mysql-native-tests"))]
mod native;
