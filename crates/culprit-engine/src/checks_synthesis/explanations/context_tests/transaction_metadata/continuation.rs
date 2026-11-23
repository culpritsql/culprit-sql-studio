//! Authenticated warehouse handles feed the unchanged pure owner; second points never replace first actor context.
use super::*;
use test_support::{TransactionMetadataContinuationCase as Continued, TransactionMetadataObservation as Observation};
struct Captures {
    basis: EvidenceCapture,
    first: EvidenceCapture,
    fresh: EvidenceCapture,
    second: EvidenceCapture,
    revision: DiagnosticRevision,
}
impl Captures {
    fn originals<'a>(&'a self, history: &'a EvidenceCapture) -> Vec<&'a EvidenceCapture> {
        vec![history, &self.basis, &self.first, &self.fresh, &self.second]
    }
}
async fn captures(case: Continued, sequence: u64) -> Captures {
    let initial = if case == Continued::MultiXact { Case::MultiXact } else { Case::Overflow };
    let (basis, first, episode, _) = scenario_point(initial, sequence, InitialPoint::Continuation(case)).await;
    let mechanism = if case == Continued::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    };
    let next =
        test_support::transaction_metadata_original_at(mechanism, "2020-01-01T12:01:10Z", "2020-01-01T12:01:11Z");
    let fresh = retained(next.source, vec![next.original.record], sequence + 2).await;
    let native = fresh.verified_original().unwrap();
    let reference = native.reference(0).unwrap();
    let TelemetryRecord::Activity(actor) = &fresh.records()[0].row().record else { panic!() };
    let previous = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: episode.scope.target.clone(),
        input: episode.latest.capture_ref(),
        episodes: vec![episode.clone()],
    };
    let update = observe_diagnostic_activity(
        fresh.source(),
        native.original(),
        &[DiagnosticActivityRow { reference: &reference, observation: actor }],
        Some(&previous),
        &ActivityPolicy::default(),
        "2020-01-01T12:01:11Z",
    )
    .unwrap();
    let before = update.checkpoint.episodes.into_iter().find(|next| next.id == episode.id).unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::TransactionMetadata,
        &before,
        fresh.source(),
        &reference,
        fresh.records()[0].row(),
    )
    .unwrap()
    .unwrap();
    let execution = SourceReadExecutionScope::from_context(
        focus.question,
        &NativeReadContext::from_activity(fresh.source()).unwrap(),
    )
    .unwrap();
    let (source, records) =
        test_support::transaction_metadata_continuation_result(&focus, &execution, case, Observation::Continued);
    let second = retained(source, records, sequence + 3).await;
    let current = second.verified_original().unwrap();
    let references =
        (0..second.records().len()).map(|position| current.reference(position).unwrap()).collect::<Vec<_>>();
    let rows =
        references.iter().zip(second.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        &focus,
        fresh.source(),
        fresh.records()[0].row(),
        second.source(),
        current.original(),
        &rows,
    )
    .unwrap();
    let pending = before.enroll_source_read(focus.clone()).unwrap();
    let pending = DiagnosticRevision::between(Some(&before), pending, "2020-01-01T12:01:11Z").unwrap().unwrap().episode;
    let collecting = pending.transition_source_read(&focus, SourceReadStageState::Collecting).unwrap();
    let collecting =
        DiagnosticRevision::between(Some(&pending), collecting, "2020-01-01T12:01:12Z").unwrap().unwrap().episode;
    let answered = collecting
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    let revision = DiagnosticRevision::between(Some(&collecting), answered, "2020-01-01T12:02:00Z").unwrap().unwrap();
    assert_eq!(revision.previous_revision.as_ref(), Some(&collecting.revision));
    assert_eq!(revision.episode.first, before.first);
    assert_eq!(revision.episode.source_reads.iter().count(), 2);
    Captures { basis, first, fresh, second, revision }
}
fn assert_first_context(fixture: &Captures, incident: &DerivedIncident, expected_candidates: usize) {
    let first = fixture.first.verified_original().unwrap().reference(0).unwrap();
    assert_eq!(incident.affected_queries.len(), 1);
    assert_eq!(incident.candidate_queries.len(), expected_candidates);
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(incident.candidate_queries.iter().all(|candidate| candidate.causal_role.is_none()
        && candidate.evidence == vec![first.clone()]
        && candidate.caller.as_ref().is_none_or(|caller| caller.evidence == first)));
    assert!(incident.entry.evidence.iter().any(|evidence| evidence.reference.as_ref() == Some(&first)));
    assert!(incident.entry.reasons.iter().any(|reason| reason.contains("1234")));
    assert!(!incident.entry.reasons.iter().any(|reason| reason.contains("9999")));
    assert!(incident.affected_queries.len() + incident.candidate_queries.len() <= MAX_DERIVED_QUERIES);
}
#[tokio::test]
async fn same_incident_counter_continuation_preserves_initial_candidates_xmin_and_caller_without_a_second_roster() {
    for (index, case) in [Continued::Continued, Continued::MultiXact, Continued::OtherActors, Continued::WideRoster]
        .into_iter()
        .enumerate()
    {
        let sequence = 915_000 + index as u64 * 10;
        let fixture = captures(case, sequence).await;
        let revision = history_revision(fixture.revision.clone(), sequence + 4).await;
        let output = project_at(&fixture.originals(&revision), vec![], "2020-01-01T12:04:00Z").unwrap();
        let incident = &output.incidents[0];
        assert_first_context(&fixture, incident, if case == Continued::WideRoster { 31 } else { 1 });
        let second = fixture.second.verified_original().unwrap().reference(0).unwrap();
        assert!(incident.entry.evidence.iter().any(|evidence| evidence.reference.as_ref() == Some(&second)));
        assert!(
            incident
                .entry
                .reasons
                .iter()
                .any(|reason| reason.contains("2020-01-01T12:00:03Z") && reason.contains("2020-01-01T12:01:13Z"))
        );
        assert!(
            output.findings.is_empty(),
            "cluster-shared deltas and overflow candidates establish no exclusive cause"
        );
    }
}
#[tokio::test]
async fn empty_ended_partial_unknown_changed_reset_or_missing_counter_never_erases_the_first_observation() {
    for (index, case) in [
        Continued::Partial,
        Continued::ChangedRequest,
        Continued::Empty,
        Continued::Unavailable,
        Continued::UnknownReset,
        Continued::ChangedReset,
        Continued::MissingCounter,
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = 916_000 + index as u64 * 10;
        let fixture = captures(case, sequence).await;
        let revision = history_revision(fixture.revision.clone(), sequence + 4).await;
        let output = project_at(&fixture.originals(&revision), vec![], "2020-01-01T12:04:00Z").unwrap();
        let incident = &output.incidents[0];
        assert_first_context(&fixture, incident, 1);
        assert!(!incident.entry.reasons.is_empty());
        if matches!(case, Continued::Empty | Continued::Unavailable) {
            assert!(
                incident.entry.reasons.iter().any(|reason| reason.contains("no authenticated native counter body"))
            );
            assert!(fixture.second.records().is_empty());
        }
    }
}
#[tokio::test]
async fn independently_proved_recovery_keeps_the_first_point_and_unavailable_second_in_latest_gold_history() {
    let sequence = 917_000;
    let fixture = captures(Continued::Empty, sequence).await;
    let mut source = fixture.fresh.source().clone();
    let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
    frame.at = "2020-01-01T12:03:00Z".into();
    frame.observed_from = frame.at.clone();
    frame.observed_until = frame.at.clone();
    frame.native_rows = 0;
    frame.observations = 0;
    frame.coverage = StateSourceCoverage::collected(source.source.clone(), 0);
    let empty = retained(source, vec![], sequence + 5).await;
    let previous = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: fixture.revision.episode.scope.target.clone(),
        input: fixture.revision.episode.latest.capture_ref(),
        episodes: vec![fixture.revision.episode.clone()],
    };
    let update = observe_diagnostic_activity(
        empty.source(),
        empty.verified_original().unwrap().original(),
        &[],
        Some(&previous),
        &ActivityPolicy::default(),
        "2020-01-01T12:03:01Z",
    )
    .unwrap();
    let recovered =
        update.revisions.into_iter().find(|revision| revision.episode.id == fixture.revision.episode.id).unwrap();
    assert_eq!(recovered.episode.lifecycle, DiagnosticLifecycle::Resolved);
    assert!(recovered.episode.recovery.is_some());
    assert_eq!(recovered.episode.source_reads, fixture.revision.episode.source_reads);
    let revision = history_revision(recovered, sequence + 6).await;
    let mut originals = fixture.originals(&revision);
    originals.push(&empty);
    let output = project_at(&originals, vec![], "2020-01-01T12:04:00Z").unwrap();
    assert_eq!(output.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_first_context(&fixture, &output.incidents[0], 1);
}
