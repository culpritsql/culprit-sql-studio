use super::*;
use culprit_evidence::analytical::test_support::{OutputGrantCase as Case, output_grant_original, output_grant_result};
async fn scenario(
    case: Case,
    sequence: u64,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, OutputGrantRetentionAnswer) {
    scenario_with_peer_count(case, sequence, usize::from(case == Case::Peer)).await
}
async fn scenario_with_peer_count(
    case: Case,
    sequence: u64,
    peer_count: usize,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, OutputGrantRetentionAnswer) {
    let input = output_grant_original();
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
        .iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::ClientResultOutput { .. }))
        .unwrap()
        .clone();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::OutputGrantRetention,
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
    let (mut source, mut rows) = output_grant_result(&focus, &execution, case);
    assert!(peer_count == 0 || case == Case::Peer);
    if peer_count > 1 {
        let template = rows[1].clone();
        for index in 1..peer_count {
            let TelemetryRecord::Operational(mut row) = template.clone() else { panic!() };
            let OperationalFact::ExecutionGrant(grant) = &mut row.fact else { panic!() };
            grant.identity = NativeIdentity::new(format!("native-concurrent-reservation-{index}"));
            let actor = grant.request.as_mut().unwrap();
            actor.key.session.id = (77 + index).to_string();
            if let Some(session) = grant.native_properties.get_mut("session_id") {
                *session = actor.key.session.id.clone();
            }
            row.key = grant.key().unwrap();
            assert!(row.is_valid());
            rows.push(TelemetryRecord::Operational(row));
        }
        let SourceObservation::SourceRead { snapshot, .. } = &mut source.observation else { panic!() };
        snapshot.records = rows.len() as u32;
        snapshot.sources[0] =
            StateSourceCoverage::observed(snapshot.sources[0].source.clone(), rows.len() as u32, true);
    }
    let result = retained(source, rows, sequence + 1).await;
    let captured = result.verified_original().unwrap();
    let references =
        (0..result.records().len()).map(|position| captured.reference(position).unwrap()).collect::<Vec<_>>();
    let pairs =
        references.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = OutputGrantRetentionAnswer::from_originals(
        &focus,
        basis.source(),
        basis.records()[0].row(),
        result.source(),
        captured.original(),
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
                answer: Box::new(SourceReadAnswer::OutputGrantRetention(Box::new(answer.clone()))),
            },
        )
        .unwrap();
    (basis, result, episode, answer)
}
#[tokio::test]
async fn canonical_output_wait_plus_same_request_positive_reservation_reuses_the_memory_mechanism_without_client_or_network_blame()
 {
    for (index, case) in
        [Case::Held, Case::Peer, Case::Partial, Case::RestrictedSql, Case::UnknownDuration].into_iter().enumerate()
    {
        let (basis, result, episode, answer) = scenario(case, 920_000 + index as u64 * 10).await;
        let revisions = history(episode, 920_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        let read = result.verified_original().unwrap().operational_read().unwrap();
        let report = culprit_checks::output_grant_retention::explain(&answer, &read, None).unwrap();
        assert_eq!(report.status, OutputGrantRetentionStatus::Observed);
        assert!(report.memory.entries.iter().any(|entry| entry.mechanism
            == Some(culprit_core::MemoryParticipantMechanism::ReservationDuringClientOutputWait)));
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.caller.is_some());
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.candidate_queries.len(), usize::from(case == Case::Peer));
        assert!(
            incident
                .candidate_queries
                .iter()
                .all(|query| query.causal_role.is_none() && query.identity != incident.affected_queries[0].identity)
        );
        assert!(!serde_json::to_string(&output).unwrap().contains("output-private-canary"));
        assert!(incident.mechanism.is_some());
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(
            incident.entry.reasons.iter().any(|reason| reason.contains("locks") && reason.contains("not examined"))
        );
        assert_eq!(answer.original_wait_ms.as_deref(), Some("27"));
        assert_eq!(answer.observed_output_wait_ms.as_deref(), (case != Case::UnknownDuration).then_some("31"));
        assert!(output.findings.is_empty(), "a point observation cannot become a network-health finding");
    }
}
#[tokio::test]
async fn changed_wait_zero_null_or_missing_reservation_retains_the_original_symptom_without_healthy_or_culprit_inference()
 {
    for (index, case) in [Case::OtherWait, Case::Zero, Case::Null, Case::Missing].into_iter().enumerate() {
        let (basis, result, episode, answer) = scenario(case, 921_000 + index as u64 * 10).await;
        let read = result.verified_original().unwrap().operational_read().unwrap();
        let report = culprit_checks::output_grant_retention::explain(&answer, &read, None).unwrap();
        assert_ne!(report.status, OutputGrantRetentionStatus::Observed);
        let revisions = history(episode, 921_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.candidate_queries.is_empty());
        assert!(
            incident
                .entry
                .reasons
                .iter()
                .any(|reason| reason.contains("not a healthy") || reason.contains("not establish"))
        );
        if case == Case::Missing {
            assert!(result.records().is_empty());
            assert!(answer.subject.is_none());
        }
    }
}
#[tokio::test]
async fn scalar_long_running_changed_generation_foreign_scope_or_tampered_native_answer_cannot_authorize_the_output_question()
 {
    let input = output_grant_original();
    let reference = input.focus.basis.as_ref();
    let TelemetryRecord::Activity(actor) = &input.original.record else { panic!() };
    let scalar = DiagnosticEpisode::start(
        input.episode.scope.clone(),
        DiagnosticSignal::Activity {
            key: actor.key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        input.episode.first_seen_at.clone(),
        input.episode.first.clone(),
    )
    .unwrap();
    assert!(
        SourceReadFocus::from_followup_input(
            SourceReadQuestion::OutputGrantRetention,
            &scalar,
            &input.source,
            reference,
            &input.original
        )
        .unwrap()
        .is_none()
    );
    let (_, result, episode, answer) = scenario(Case::Held, 922_000).await;
    let mut read = result.verified_original().unwrap().operational_read().unwrap();
    let mut changed_original_role = answer.clone();
    changed_original_role.basis_context.database_writable = Some(false);
    let matched_focus = episode.source_read_focus(SourceReadQuestion::OutputGrantRetention).unwrap().unwrap();
    assert!(answer.matches_prerequisite(&matched_focus.prerequisite));
    assert!(!changed_original_role.matches_prerequisite(&matched_focus.prerequisite));
    let mut edited_duration = answer.clone();
    edited_duration.observed_output_wait_ms = Some("32".into());
    assert!(culprit_checks::output_grant_retention::explain(&edited_duration, &read, None).is_err());
    let mut edited = answer.clone();
    edited.result_context.partition = SourcePartition::Target;
    assert!(culprit_checks::output_grant_retention::explain(&edited, &read, None).is_err());
    read.description.target = CollectionTarget::Database(culprit_core::DatabaseIdentity::server(
        culprit_core::Engine::Mssql,
        "foreign",
        "elsewhere.invalid",
        1,
        None,
    ));
    assert!(culprit_checks::output_grant_retention::explain(&answer, &read, None).is_err());
    let mut bad = input.focus.clone();
    let SourceReadPrerequisite::ObservedClientOutputWait { request } = &mut bad.prerequisite else { panic!() };
    request.original.key.request.as_mut().unwrap().generation = "another-request".into();
    assert!(!bad.matches_original(&input.source, &input.original));
}
#[tokio::test]
async fn native_memory_acquisition_context_remains_distinct_from_logical_snapshot_namespace_and_unadmitted_questions_retire_only_with_proved_recovery()
 {
    let (basis, result, episode, answer) = scenario(Case::Held, 923_000).await;
    assert_eq!(result.source().partition, SourcePartition::Target);
    assert_eq!(RequestMemorySnapshot::acquisition_context(result.source()).unwrap(), answer.result_context);
    assert_eq!(
        NativeReadContext::from_snapshot(result.source()).unwrap().partition,
        SourcePartition::Target,
        "logical snapshot semantics remain unchanged"
    );
    let input = output_grant_original();
    let focus = input.focus.clone();
    let deferred = input
        .episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
        )
        .unwrap();
    assert!(deferred.has_owed_question());
    let mut empty = basis.source().clone();
    let SourceObservation::Activity(frame) = &mut empty.observation else { panic!() };
    frame.at = "2020-01-01T12:00:10Z".into();
    frame.observed_from = frame.at.clone();
    frame.observed_until = frame.at.clone();
    frame.native_rows = 0;
    frame.observations = 0;
    frame.coverage = StateSourceCoverage::observed(empty.source.clone(), 0, true);
    let capture = DiagnosticCaptureRef {
        capture: CaptureIdentity {
            observation_id: "proved-output-actor-gone".into(),
            revision: Some(CaptureRevision {
                outbox_id: digest(b"output-empty"),
                owner_epoch: "1".into(),
                sequence: "1".into(),
            }),
        },
        manifest_sha256: digest(b"proved-empty-manifest"),
        source_scope_id: empty.captured_range().unwrap().scope_id,
    };
    let previous = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: input.source.target.clone(),
        input: input.focus.basis.capture_ref(),
        episodes: vec![deferred],
    };
    let update = observe_diagnostic_activity(
        &empty,
        &capture,
        &[],
        Some(&previous),
        &ActivityPolicy::default(),
        "2020-01-01T12:00:11Z",
    )
    .unwrap();
    assert!(update.revisions.iter().any(|revision| revision.episode.lifecycle == DiagnosticLifecycle::Resolved
        && matches!(
            revision.episode.source_reads.get(SourceReadQuestion::OutputGrantRetention).unwrap().state,
            SourceReadStageState::Recovered
        )));
    assert!(
        episode.source_reads.get(SourceReadQuestion::OutputGrantRetention).unwrap().result().is_some(),
        "a terminal sibling answer remains original history"
    );
}

#[tokio::test]
async fn a_complete_large_native_reservation_roster_publishes_a_bounded_incident_without_changing_its_evidence_or_cause()
 {
    let peers = MAX_DERIVED_QUERIES + 8;
    let (basis, result, episode, answer) = scenario_with_peer_count(Case::Peer, 924_000, peers).await;
    assert!(result.is_read_complete());
    assert_eq!(result.records().len(), peers + 1);
    assert_eq!(answer.peers.len(), peers);
    let native_read = result.verified_original().unwrap().operational_read().unwrap();
    let full_report = culprit_checks::output_grant_retention::explain(&answer, &native_read, None).unwrap();
    assert_eq!(full_report.memory.entries.len(), peers + 1);
    let original_body = serde_json::to_vec(&result.source()).unwrap();
    let revisions = history(episode, 924_002).await;
    let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
    output.validate().unwrap();
    let [incident] = output.incidents.as_slice() else { panic!() };
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.candidate_queries.len(), MAX_DERIVED_QUERIES - 1);
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(
        incident.mechanism.as_deref(),
        Some(full_report.summary.as_str()),
        "full-roster interpretation is retained before presentation selection"
    );
    assert!(
        incident.entry.reasons.iter().any(|reason| reason
            == &t!("view.synthesis.limit.query_details", affected = "0", culprits = "0", candidates = "9"))
    );
    let original = result.verified_original().unwrap();
    assert!(output.description.contains_capture_for(&original.reference(peers).unwrap()));
    assert!(
        answer.peers.contains(&original.reference(peers).unwrap()),
        "the recorded answer retains the omitted peer's exact original row reference"
    );
    assert!(
        incident
            .entry
            .evidence
            .iter()
            .any(|evidence| evidence.reference.is_none() && evidence.capture == answer.result),
        "the shared history contract retains the whole follow-up capture without copying every peer link"
    );
    assert!(incident.entry.evidence.len() < peers, "history links stay independent of the peer roster size");
    assert!(incident.candidate_queries.iter().all(|query| query.causal_role.is_none()));
    assert_eq!(serde_json::to_vec(&result.source()).unwrap(), original_body);
    assert_eq!(
        serde_json::to_vec(&project(&[&revisions, &basis, &result], vec![]).unwrap()).unwrap(),
        serde_json::to_vec(&output).unwrap()
    );
}

#[tokio::test]
async fn a_reservation_roster_at_the_combined_gold_boundary_keeps_all_details_without_claiming_omission() {
    let peers = MAX_DERIVED_QUERIES - 1;
    let (basis, result, episode, _) = scenario_with_peer_count(Case::Peer, 924_010, peers).await;
    let revisions = history(episode, 924_012).await;
    let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
    output.validate().unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.affected_queries.len() + incident.candidate_queries.len(), MAX_DERIVED_QUERIES);
    assert_eq!(incident.candidate_queries.len(), peers);
    let omitted = t!("view.synthesis.limit.query_details", affected = "0", culprits = "0", candidates = "0");
    assert!(!incident.entry.reasons.contains(&omitted));
}
