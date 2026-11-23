//! The real automatic queue uses the normal pure synthesizer and canonical zero-native-work writer.
use super::*;
use culprit_collector::{
    analytical::test_support::LocalCaptureFixture,
    outbox::{CompletedCaptureBudget, DiagnosticInput, DiagnosticUpdate, Outbox},
    synthesis::{AutomaticSynthesis, SynthesisTurn},
};
use culprit_core::limits::OperationGuard;

fn automatic() -> AutomaticSynthesis {
    AutomaticSynthesis::new(
        Arc::new(
            ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "automatic-resource-context-1")
                .unwrap(),
        ),
        CompletedCaptureBudget::default(),
    )
    .unwrap()
}
fn activity_rows(input: &DiagnosticInput) -> Vec<DiagnosticActivityRow<'_>> {
    input
        .rows
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(observation) = &row.row.record else { panic!() };
            DiagnosticActivityRow { reference: &row.reference, observation }
        })
        .collect()
}
async fn commit(
    outbox: &mut Outbox,
    input: DiagnosticInput,
    revisions: Vec<DiagnosticRevision>,
    checkpoint: DiagnosticCheckpoint,
) -> CaptureIdentity {
    let step_id = digest(&serde_json::to_vec(&("automatic-context-control", &input.parent)).unwrap());
    outbox
        .commit_diagnostics(
            input,
            DiagnosticUpdate {
                step_id,
                analyzed_at: "2020-01-01T12:00:20Z".into(),
                revisions,
                checkpoint,
                historical_episodes: vec![],
            },
        )
        .await
        .unwrap()
}
async fn observe(outbox: &mut Outbox) -> (CaptureIdentity, Option<String>) {
    let input = outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    let update = observe_diagnostic_activity(
        &input.source,
        &input.parent,
        &activity_rows(&input),
        input.checkpoint.as_ref(),
        &ActivityPolicy::default(),
        "2020-01-01T12:00:20Z",
    )
    .unwrap();
    let episode = update
        .revisions
        .iter()
        .find(|revision| {
            matches!(
                revision.episode.signal,
                DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. }
            )
        })
        .map(|revision| revision.episode.id.clone());
    (Box::pin(commit(outbox, input, update.revisions, update.checkpoint)).await, episode)
}
async fn materialize(
    outbox: &mut Outbox,
    automatic: &AutomaticSynthesis,
    count: usize,
    expected: &CaptureIdentity,
) -> DiagnosticCaptureRef {
    let mut selected = None;
    for _ in 0..count {
        let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
        let SynthesisTurn::Committed { primary, output, queue_retired } =
            outbox.delivery().synthesize_pending(automatic, operation.handle(), ()).await.unwrap()
        else {
            panic!("the actual automatic turn must commit")
        };
        assert!(queue_retired);
        let manifest = outbox.verify(&output.capture.observation_id).await.unwrap();
        assert_eq!(manifest.source_queries, 0);
        assert_eq!(manifest.status, CollectionStatus::Complete);
        if primary.capture == *expected {
            selected = Some(output);
        }
    }
    selected.unwrap()
}
async fn gold(
    fixture: &mut LocalCaptureFixture,
    output: &DiagnosticCaptureRef,
) -> (DerivedDiagnosisDescription, Vec<DerivedIncident>) {
    // verify() authenticates the same output files before this test reads their ordinary typed NDJSON.
    let manifest = fixture.outbox.as_mut().unwrap().verify(&output.capture.observation_id).await.unwrap();
    let SourceObservation::DerivedDiagnosis(description) = manifest.description.unwrap().observation else { panic!() };
    let mut incidents = Vec::new();
    for file in manifest.files.iter().filter(|file| file.stream == TelemetryStream::DerivedIncidents) {
        let path = fixture.directory.join("captures").join(&output.capture.observation_id).join(&file.file);
        for line in std::fs::read_to_string(path).unwrap().lines() {
            let row: ExportedRecord = serde_json::from_str(line).unwrap();
            let TelemetryRecord::DerivedIncident(incident) = row.record else { panic!() };
            incidents.push(*incident);
        }
    }
    (*description, incidents)
}

#[tokio::test]
async fn automatic_log_context_reaches_real_gold_with_candidates_and_an_independent_unknown_role_file_increase() {
    for (case, local_only) in
        [(Case::Exact, false), (Case::UnknownRole, false), (Case::Exact, true), (Case::UnknownRole, true)]
    {
        let automatic = automatic();
        let mut originals = commit_pipeline_fixture(case).originals;
        if local_only {
            for original in &mut originals {
                let CollectionTarget::Database(database) = &mut original.source.target else { panic!() };
                database.incarnation = None;
            }
        }
        let primary = originals.remove(0);
        let mut fixture =
            Box::pin(LocalCaptureFixture::with_automatic_synthesis(primary.source, primary.records, &automatic)).await;
        let original = fixture.original_reference();
        let mut contexts = Vec::new();
        for point in originals {
            contexts.push(Box::pin(fixture.append_original(point.source, point.records)).await);
        }
        let (revision, episode) = Box::pin(observe(fixture.outbox.as_mut().unwrap())).await;
        let output = Box::pin(materialize(fixture.outbox.as_mut().unwrap(), &automatic, 6, &revision)).await;
        let (description, incidents) = gold(&mut fixture, &output).await;
        assert!(description.input_read_complete);
        assert!(
            !description.context_issues.contains(&DerivedContextIssue::UnqualifiedPointScope),
            "an exact source-owned local epoch needs no global incarnation: {case:?}/{local_only}"
        );
        assert!(description.contains_capture_for(&incidents[0].revision_reference));
        assert!(description.inputs.iter().any(|input| input.capture == original));
        for file in &contexts[2..] {
            assert!(description.inputs.iter().any(|input| &input.capture == file));
        }
        let incident = incidents.iter().find(|incident| Some(&incident.entry.episode_id) == episode.as_ref()).unwrap();
        assert_eq!(incident.affected_queries.len(), 1, "{case:?}");
        assert!(!incident.victim_summary.is_empty());
        assert!(incident.mechanism.as_deref().unwrap().contains("flush"));
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.candidate_queries.len(), if case == Case::Exact { 1 } else { 0 });
        assert!(description.context_issues.contains(&DerivedContextIssue::IndependentPointRole));
        assert!(incident.entry.reasons.contains(&t!("view.commit_pipeline.limit.growth")), "{case:?}");
        assert!(!serde_json::to_string(incident).unwrap().contains("commit-private-canary"));
    }
}

#[tokio::test]
async fn automatic_partial_log_context_keeps_original_gaps_without_a_producer_or_healthy_absence() {
    let automatic = automatic();
    let mut originals = commit_pipeline_fixture(Case::Partial).originals;
    let primary = originals.remove(0);
    let mut fixture =
        Box::pin(LocalCaptureFixture::with_automatic_synthesis(primary.source, primary.records, &automatic)).await;
    for point in originals {
        Box::pin(fixture.append_original(point.source, point.records)).await;
    }
    let (revision, episode) = Box::pin(observe(fixture.outbox.as_mut().unwrap())).await;
    let output = Box::pin(materialize(fixture.outbox.as_mut().unwrap(), &automatic, 6, &revision)).await;
    let (_, incidents) = gold(&mut fixture, &output).await;
    let incident = incidents.iter().find(|incident| Some(&incident.entry.episode_id) == episode.as_ref()).unwrap();
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.candidate_queries.is_empty());
    assert!(incident.culprit_queries.is_empty());
    assert!(!incident.entry.reasons.is_empty());
}

#[tokio::test]
async fn automatic_output_lock_gold_follows_exact_focused_ancestry_and_never_blames_a_mars_output_sibling() {
    use test_support::{OutputBlockingCaseFixture as BlockingCase, output_blocking_fixture};
    for case in [BlockingCase::Mssql, BlockingCase::PgRelation, BlockingCase::Mars] {
        let automatic = automatic();
        let scenario = output_blocking_fixture(case);
        let rows = scenario
            .records
            .iter()
            .map(|row| {
                let TelemetryRecord::Activity(observation) = row else { panic!() };
                ActivityFrameRow { observation, reference: None }
            })
            .collect::<Vec<_>>();
        let report = culprit_checks::focused_blocking::explain(&scenario.source, &rows, true).unwrap();
        let expected = report
            .output_retention
            .explanations
            .first()
            .map(|words| (words.mechanism.clone(), words.investigation.clone(), words.verify.clone()));
        let generic_mechanism = report.summary;
        assert_eq!(expected.is_some(), case != BlockingCase::Mars);
        let mut fixture =
            Box::pin(LocalCaptureFixture::with_automatic_synthesis(scenario.source, scenario.records, &automatic))
                .await;
        // The same fixture owner first seals the real original victim, then its rebound focused result.
        Box::pin(observe(fixture.outbox.as_mut().unwrap())).await;
        let outbox = fixture.outbox.as_mut().unwrap();
        let input = outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
        let update = analyze_diagnostic_blocking(
            &input.source,
            &input.parent,
            &activity_rows(&input),
            input.checkpoint.as_ref().unwrap(),
            "2020-01-01T12:00:20Z",
            None,
        )
        .unwrap();
        let episode = update
            .revisions
            .iter()
            .find(|revision| matches!(revision.episode.signal, DiagnosticSignal::LockWait { .. }))
            .unwrap()
            .episode
            .id
            .clone();
        let revision = Box::pin(commit(outbox, input, update.revisions, update.checkpoint)).await;
        let output = Box::pin(materialize(outbox, &automatic, 4, &revision)).await;
        let (description, incidents) = gold(&mut fixture, &output).await;
        let incident = incidents.iter().find(|incident| incident.entry.episode_id == episode).unwrap();
        assert_eq!(incident.affected_queries.len(), 1, "{case:?}");
        assert!(
            description.inputs.len() >= 3,
            "original victim, actual focused result and immutable revision are retained"
        );
        assert!(
            incident.culprit_queries.iter().any(|query| query
                .causal_role
                .as_ref()
                .is_some_and(|role| role.kind == DerivedCausalRoleKind::NativeBlockingOwner)),
            "{case:?}"
        );
        if let Some((mechanism, action, verify)) = expected {
            assert_eq!(incident.mechanism.as_deref(), Some(mechanism.as_str()), "{case:?}");
            assert_eq!(incident.developer_action, action);
            assert_eq!(incident.verification, verify);
        } else {
            assert_eq!(
                incident.mechanism.as_deref(),
                Some(generic_mechanism.as_str()),
                "native session ownership survives while a MARS output sibling cannot be blamed"
            );
        }
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
    }
}
