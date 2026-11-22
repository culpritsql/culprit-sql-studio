use super::*;
use crate::collected_evidence::RetainedEvidenceFixture as Fixture;
use culprit_evidence::analytical::{
    evidence::{EvidenceCapture, EvidenceFamily, RecoveryPayloadBudget},
    test_support::{self, GrantOutputCase, PageTaskHolderCase, PageWriterCase},
};

fn native_caller(request: &mut ActivityObservation) {
    request.client = Some(ActivityClient {
        application: Some("db-reported-application".into()),
        user: Some("db-reported-login".into()),
        host: Some("db-reported-host".into()),
    });
    request.session_context = Some(Box::new(ActivitySessionContext {
        lifecycle: SessionLifecycle::Running,
        login_age_ms: None,
        last_request_end_age_ms: None,
        client_library: Some("db-reported-library".into()),
        client_process_id: None,
        native: None,
    }));
}
fn assert_native_caller(query: &DerivedIncidentQuery) {
    let caller = query.caller.as_ref().unwrap();
    assert!(caller.is_valid());
    assert_eq!(caller.client.as_ref().unwrap().application.as_deref(), Some("db-reported-application"));
    let session = caller.session_context.as_ref().unwrap();
    assert_eq!(session.client_library.as_deref(), Some("db-reported-library"));
    assert!(session.native.is_none(), "an unknown native session database stays unknown");
    assert!(query.evidence.contains(&caller.evidence));
}
async fn retained(source: CollectionDescription, records: Vec<TelemetryRecord>, sequence: u64) -> EvidenceCapture {
    let family = if source.observation.collection() == TelemetryCollection::Activity {
        EvidenceFamily::ActivityObservations
    } else {
        EvidenceFamily::OperationalSamples
    };
    Fixture::with_sequence(source, records, sequence)
        .await
        .read_families_with_recovery(&[family], RecoveryPayloadBudget::default())
        .await
}
async fn history(episode: DiagnosticEpisode, sequence: u64) -> EvidenceCapture {
    let parent = episode.latest.capture_ref();
    let revision = DiagnosticRevision::between(None, episode, "2020-01-01T12:00:20Z").unwrap().unwrap();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: revision.episode.scope.target.clone(),
        input: parent.clone(),
        part: 0,
        parts: 1,
        episodes: vec![revision.episode.clone()],
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let source = CollectionDescription {
        target: revision.episode.scope.target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent,
            step_id: digest(b"incident-context-original-step"),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(revision.episode.scope.key().unwrap(), revision.episode.scope.clone())].into(),
        }),
    };
    retained(
        source,
        vec![
            TelemetryRecord::Operational(OperationalObservation {
                source: DIAGNOSTICS_SOURCE.into(),
                key: revision.id.clone(),
                fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
            }),
            TelemetryRecord::DiagnosticCheckpoint(checkpoint),
        ],
        sequence,
    )
    .await
}
fn project(captures: &[&EvidenceCapture], issues: Vec<DerivedContextIssue>) -> Result<DerivedDiagnosis> {
    let originals = captures.iter().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
    let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "incident-context-policy-1")?
        .analyze_originals(
            &borrowed,
            "2020-01-01T12:01:00Z",
            &DerivedDisplayContext { connection_alias: "saved-route".into(), environment: None, database_name: None },
            issues,
        )
}

#[tokio::test]
async fn task_point_incident_uses_its_authenticated_activity_parent_for_the_victim_and_never_a_later_actor() {
    let mut input = test_support::page_task_holder_fixture(PageTaskHolderCase::QualifiedBoth);
    let TelemetryRecord::Activity(waiter) = &mut input.activity_rows[0] else { panic!() };
    native_caller(waiter);
    let changed_source = input.activity_source.clone();
    let mut changed_records = input.activity_rows.clone();
    let activity = retained(input.activity_source, input.activity_rows, 850_001).await;
    let pages = retained(input.page_source, input.page_rows, 850_002).await;
    let original = activity.verified_original().unwrap();
    let page = pages.verified_original().unwrap();
    let TelemetryRecord::Activity(waiter) = &original.records()[0].row().record else { panic!() };
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::PageWait(point), .. }) =
        &page.records()[0].row().record
    else {
        panic!()
    };
    let task = point.task_wait.as_ref().unwrap();
    let parent = original.reference(0).unwrap();
    let page_reference = page.reference(0).unwrap();
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(original.source()).unwrap(),
        DiagnosticSignal::PageTaskWait {
            key: waiter.key.clone(),
            statement_identity: waiter.statement_identity.clone().unwrap(),
            parent: Box::new(parent.clone()),
            page: Box::new(page_reference.clone()),
            task: Box::new(task.clone()),
            database_writable: true,
        },
        task.observed_at.clone(),
        page_reference,
    )
    .unwrap();
    let mut wrong_statement = episode.signal.clone();
    let DiagnosticSignal::PageTaskWait { statement_identity, .. } = &mut wrong_statement else { panic!() };
    *statement_identity = NativeIdentity::new("another-statement-in-the-same-native-request");
    let wrong_episode = DiagnosticEpisode::start(
        episode.scope.clone(),
        wrong_statement,
        episode.first_seen_at.clone(),
        episode.first.clone(),
    )
    .unwrap();
    let revisions = history(episode, 850_003).await;
    let output = project(&[&revisions, &activity, &pages], vec![]).unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.affected_queries.len(), 1);
    let affected = &incident.affected_queries[0];
    assert_eq!(affected.evidence, vec![parent]);
    assert!(matches!(&affected.identity, Some(DerivedQueryIdentity::Activity { actor, .. }) if actor==&waiter.key));
    assert_native_caller(affected);
    assert!(affected.causal_role.is_none());
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
    assert!(incident.caller.is_some());
    let wrong_revision = history(wrong_episode, 850_004).await;
    assert!(project(&[&wrong_revision, &activity, &pages], vec![]).is_err());
    let missing = project(&[&revisions, &pages], vec![DerivedContextIssue::MissingOriginal]).unwrap();
    assert!(missing.incidents[0].affected_queries.is_empty());
    assert!(missing.incidents[0].caller.is_none());
    let TelemetryRecord::Activity(changed) = &mut changed_records[0] else { panic!() };
    changed.key.session.generation = "a-different-native-login".into();
    let changed = retained(changed_source, changed_records, 850_001).await;
    assert!(project(&[&revisions, &changed, &pages], vec![]).is_err());
}

async fn temporary(
    state: SourceState,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, TemporaryResourceAnswer) {
    let (source, records) = test_support::temporary_resource_basis();
    let basis = retained(source, records, 860_001).await;
    let original = basis.verified_original().unwrap();
    let references =
        (0..original.records().len()).map(|position| original.reference(position).unwrap()).collect::<Vec<_>>();
    let rows = original
        .records()
        .iter()
        .zip(&references)
        .map(|(row, reference)| DiagnosticOriginalRow { reference, record: &row.row().record })
        .collect::<Vec<_>>();
    let observed =
        observe_diagnostic_original(original.source(), original.original(), &rows, None, &[], "2020-01-01T12:00:02Z")
            .unwrap();
    let episode =
        observed.checkpoint.episodes.into_iter().next().unwrap().finish_original_observation_planning().unwrap();
    let focus = episode.source_read_focus().unwrap().unwrap();
    let execution = SourceReadExecutionScope::from_context(
        focus.question,
        &NativeReadContext::from_snapshot(original.source()).unwrap(),
    )
    .unwrap();
    let (source, mut records) = test_support::temporary_resource_result(&focus, &execution, state);
    for record in &mut records {
        let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TaskSpace(task), .. }) =
            record
        else {
            panic!()
        };
        if let Some(request) = &mut task.request {
            native_caller(request);
        }
    }
    let result = retained(source, records, 860_002).await;
    let observed = result.verified_original().unwrap();
    let result_refs =
        (0..observed.records().len()).map(|position| observed.reference(position).unwrap()).collect::<Vec<_>>();
    let rows =
        result_refs.iter().zip(observed.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = TemporaryResourceAnswer::from_originals(
        &focus,
        original.source(),
        original.records()[0].row(),
        observed.source(),
        observed.original(),
        &rows,
    )
    .unwrap();
    let episode =
        episode.enroll_source_read(focus).unwrap().transition_source_read(SourceReadStageState::Collecting).unwrap();
    (basis, result, episode, answer)
}
fn answered(episode: DiagnosticEpisode, answer: TemporaryResourceAnswer) -> DiagnosticEpisode {
    episode
        .transition_source_read(SourceReadStageState::Answered {
            answer: Box::new(SourceReadAnswer::TemporaryResources(Box::new(answer))),
        })
        .unwrap()
}

#[tokio::test]
async fn completed_tempdb_answer_retains_only_its_exact_checked_native_requests_as_candidates() {
    for state in [SourceState::Collected, SourceState::Partial] {
        let (basis, result, episode, answer) = temporary(state).await;
        let checked = answer.checked_task_contexts.clone();
        assert!(!checked.is_empty());
        let data_context = NativeReadContext::from_snapshot(result.source()).unwrap();
        assert_eq!(data_context.partition, SourcePartition::Target);
        assert_eq!(data_context.database_writable, None);
        assert_ne!(data_context, answer.result_context);
        assert_eq!(result.source().observation.execution_context(), Some(&answer.result_context));
        let revisions = history(answered(episode, answer), 860_003).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert_eq!(incident.candidate_queries.len(), checked.len());
        assert!(incident.affected_queries.is_empty(), "an old cleanup boundary does not identify a victim query");
        assert!(incident.culprit_queries.is_empty(), "task counters never prove an allocating culprit");
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        for query in &incident.candidate_queries {
            assert!(query.causal_role.is_none());
            assert_eq!(query.evidence.len(), 1);
            assert!(checked.contains(&query.evidence[0]));
            assert_native_caller(query);
            assert!(matches!(query.identity, Some(DerivedQueryIdentity::Activity { statement: Some(_), .. })));
        }
        assert_eq!(incident.entry.analysis, DiagnosticAnalysisStatus::Unresolved);
        let missing = project(&[&revisions, &basis], vec![DerivedContextIssue::MissingOriginal]).unwrap();
        assert!(missing.incidents[0].candidate_queries.is_empty());
        assert_eq!(missing.incidents[0].attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(!missing.incidents[0].entry.reasons.is_empty());
    }
}

#[tokio::test]
async fn tempdb_answer_cannot_promote_an_unchecked_or_missing_request_by_asserting_a_checked_row_reference() {
    let (basis, result, episode, mut answer) = temporary(SourceState::Partial).await;
    let original = result.verified_original().unwrap();
    let position = original
        .records()
        .iter()
        .position(|row| {
            matches!(&row.row().record,
        TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TaskSpace(task), .. })
        if task.request_context()!=TaskRequestContext::CheckedAtBounds)
        })
        .unwrap();
    answer.checked_task_context_count = 1;
    answer.checked_task_contexts = vec![original.reference(position).unwrap()];
    assert!(answer.is_valid(), "structural reference membership alone is not native request qualification");
    let revisions = history(answered(episode, answer), 860_004).await;
    assert!(project(&[&revisions, &basis, &result], vec![]).is_err());
}

#[tokio::test]
async fn a_missing_first_task_row_does_not_hide_an_independently_authenticated_checked_peer() {
    let (basis, result, episode, answer) = temporary(SourceState::Collected).await;
    let checked = answer.checked_task_contexts.clone();
    assert_eq!(checked.len(), 2);
    let fixture = Fixture::with_sequence(
        result.source().clone(),
        result.records().iter().map(|row| row.row().record.clone()).collect(),
        860_002,
    )
    .await;
    fixture.remove_first_row(TelemetryStream::OperationalSamples);
    let partial = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
    assert!(!partial.is_read_complete());
    let revisions = history(answered(episode, answer), 860_005).await;
    let output = project(&[&revisions, &basis, &partial], vec![]).unwrap();
    assert!(!output.description.input_read_complete);
    let incident = &output.incidents[0];
    assert_eq!(incident.candidate_queries.len(), 1);
    assert_eq!(incident.candidate_queries[0].evidence, vec![checked[1].clone()]);
    assert_native_caller(&incident.candidate_queries[0]);
    assert!(incident.culprit_queries.is_empty());
}

#[tokio::test]
async fn request_memory_candidates_preserve_native_session_context_without_bypassing_mars_refusal() {
    for (case, expected) in [(GrantOutputCase::Qualified, 1), (GrantOutputCase::NegativeMarsPeer, 0)] {
        let (source, mut records) = test_support::grant_output_evidence(case);
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::ExecutionGrant(grant), ..
        }) = &mut records[0]
        else {
            panic!()
        };
        native_caller(grant.request.as_mut().unwrap());
        let captured = retained(source, records, 870_001).await;
        let original = captured.verified_original().unwrap();
        let position = original.records().iter().position(|row| matches!(&row.row().record,
            TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::ExecutionGrant(grant), .. }) if grant.state==ExecutionGrantState::Waiting)).unwrap();
        let explanation = request_memory(&original, position).unwrap();
        assert_eq!(explanation.candidates.len(), expected, "{case:?}");
        for query in &explanation.candidates {
            assert_native_caller(query);
            assert!(query.causal_role.is_none());
        }
        assert!(explanation.culprits.iter().all(|query| query.causal_role.is_some()));
    }
}

#[tokio::test]
async fn holder_candidates_retain_the_same_native_caller_but_task_only_points_do_not_invent_one() {
    for (case, expected) in [(PageTaskHolderCase::QualifiedBoth, 1), (PageTaskHolderCase::TaskOnly, 0)] {
        let (source, mut records) = test_support::page_task_holder_evidence(case);
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::PageTaskHolders(holder), ..
        }) = &mut records[0]
        else {
            panic!()
        };
        for point in [&mut holder.before, &mut holder.after] {
            if let Some(request) = &mut point.holder.request {
                native_caller(request);
            }
        }
        let captured = retained(source, records, 880_001).await;
        let original = captured.verified_original().unwrap();
        let explanation = holders(&original, 0).unwrap();
        assert_eq!(explanation.candidates.len(), expected);
        assert_eq!(explanation.culprits.len(), expected);
        for query in &explanation.candidates {
            assert_native_caller(query);
            assert!(query.causal_role.is_none());
        }
        for culprit in &explanation.culprits {
            assert_native_caller(culprit);
            assert!(!culprit.sql.is_available());
        }
    }
}

#[tokio::test]
async fn selected_writers_keep_native_caller_context_as_candidates_even_when_target_writes_are_proven() {
    for case in [PageWriterCase::TargetWriter, PageWriterCase::MissingRequest, PageWriterCase::ChangedRequest] {
        let (source, mut records) = test_support::page_writer_evidence(case);
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::PageWaitWriters(observation),
            ..
        }) = &mut records[0]
        else {
            panic!()
        };
        for candidate in &mut observation.candidates {
            if let Some(request) = &mut candidate.request {
                native_caller(request);
            }
        }
        let captured = retained(source, records, 890_001).await;
        let original = captured.verified_original().unwrap();
        let explanation = writers(&original, 0).unwrap();
        assert_eq!(explanation.candidates.len(), if case == PageWriterCase::TargetWriter { 2 } else { 0 });
        assert!(explanation.culprits.is_empty(), "selected object/index writes are not page-latch ownership");
        for query in &explanation.candidates {
            assert_native_caller(query);
            assert!(query.causal_role.is_none());
        }
    }
}

#[tokio::test]
async fn compiler_admission_keeps_canonical_ownership_limits_without_naming_gateway_rows_as_culprits() {
    let (source, records) = test_support::compiler_admission_evidence();
    let captured = retained(source, records, 900_001).await;
    let original = captured.verified_original().unwrap();
    let explanation = compilation(&original, 0).unwrap();
    assert!(explanation.candidates.is_empty());
    assert!(explanation.culprits.is_empty());
    assert!(explanation.limitations.contains(&t!("view.compiler_admission.limit.ownership")));
    assert!(explanation.limitations.contains(&t!("view.compiler_admission.limit.clock")));
    assert!(explanation.mechanism.is_some());
    assert!(explanation.action.is_some());
    assert!(explanation.verify.is_some());
}

#[tokio::test]
async fn current_request_work_preserves_native_coverage_limits_without_inventing_a_plan_reuse_cause() {
    let (source, records) = test_support::current_request_work_evidence_cases().remove(0);
    let captured = retained(source, records, 910_001).await;
    let original = captured.verified_original().unwrap();
    let read = native_rows(&original).unwrap();
    let LocalSnapshotReport::CurrentRequestWork(report) =
        crate::local_captures::explain_operational_report(Kind::CurrentRequestWork, &read).unwrap()
    else {
        panic!()
    };
    let explanation = current_work(&original, 0).unwrap();
    assert!(!report.limitations.is_empty());
    assert_eq!(explanation.limitations, report.limitations);
    assert!(explanation.candidates.is_empty());
    assert!(explanation.culprits.is_empty());
    assert_eq!(explanation.attribution_status(), DerivedAttributionStatus::Unestablished);
}
