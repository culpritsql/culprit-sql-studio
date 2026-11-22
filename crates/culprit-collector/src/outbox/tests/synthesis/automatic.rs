use super::*;
use crate::synthesis::{AutomaticSynthesis, SynthesisContextIssue, SynthesisDeferral, SynthesisTurn};
use culprit_core::DerivedFindingEvaluations;

#[derive(Clone, Debug)]
struct Seen {
    primary: DiagnosticCaptureRef,
    context: Vec<DiagnosticCaptureRef>,
    issues: Vec<SynthesisContextIssue>,
    primary_rows: usize,
}
struct QueueAnalysis {
    seen: Arc<Mutex<Vec<Seen>>>,
    cancel: Option<Arc<Mutex<Option<OperationGuard>>>>,
}
impl Synthesizer for QueueAnalysis {
    fn analyzer(&self) -> ComponentVersion {
        ComponentVersion::package("queue-controlled-analysis", "1", None)
    }
    fn rules_version(&self) -> &str {
        "queue-controlled-1"
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        self.seen.lock().unwrap().push(Seen {
            primary: input.primary().original().clone(),
            context: input.context().iter().map(|capture| capture.original().clone()).collect(),
            issues: input.context_issues().to_vec(),
            primary_rows: input.primary().records().len(),
        });
        let mut description = DerivedDiagnosisDescription {
            analysis_id: String::new(),
            analyzer: self.analyzer(),
            rules_version: self.rules_version().into(),
            language: "en".into(),
            display: input.display().clone(),
            recorded_at: input.recorded_at().into(),
            observed_from: input.primary().collected_from().into(),
            observed_until: input.primary().collected_until().into(),
            inputs: input
                .captures()
                .map(|capture| DerivedInputReference {
                    capture: capture.original().clone(),
                    role: DerivedInputRole::Observation,
                    catalog: None,
                })
                .collect(),
            origin: AnalysisOrigin::Collected,
            input_read_complete: true,
            counts: DerivedDiagnosisCounts { incidents: 0, findings: 0, changes: 0 },
            context_issues: input.derived_context_issues(),
            finding_evaluations: DerivedFindingEvaluations::default(),
            change_evaluations: vec![],
            output_coverage: DerivedOutputCoverageSet {
                incidents: DerivedOutputCoverage::ContextUnavailable {
                    reason: DerivedOutputGap::NoDiagnosticRevisions,
                },
                findings: DerivedOutputCoverage::ContextUnavailable {
                    reason: DerivedOutputGap::AnalysisSourceUnavailable,
                },
                changes: DerivedOutputCoverage::ContextUnavailable {
                    reason: DerivedOutputGap::CatalogComparisonUnavailable,
                },
            },
        };
        description.analysis_id = description.expected_analysis_id()?;
        if let Some(owner) = &self.cancel {
            drop(owner.lock().unwrap().take());
        }
        Ok(DerivedDiagnosis { description, incidents: vec![], findings: vec![], changes: vec![] })
    }
}
fn automatic(seen: Arc<Mutex<Vec<Seen>>>, cancel: Option<Arc<Mutex<Option<OperationGuard>>>>) -> AutomaticSynthesis {
    AutomaticSynthesis::new(Arc::new(QueueAnalysis { seen, cancel }), CompletedCaptureBudget::default()).unwrap()
}
async fn install(outbox: &mut Outbox, automatic: &AutomaticSynthesis) {
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    outbox.register_automatic_synthesis(automatic, operation.handle(), ()).await.unwrap();
}
async fn turn(outbox: &Outbox, automatic: &AutomaticSynthesis) -> SynthesisTurn {
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    outbox.delivery().synthesize_pending(automatic, operation.handle(), ()).await.unwrap()
}
async fn backlog(outbox: &Outbox) -> crate::synthesis::SynthesisBacklog {
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    outbox.delivery().synthesis_backlog(operation.handle(), ()).await.unwrap()
}

async fn counts(outbox: &Outbox) -> (u32, u32, u32, u32) {
    outbox.files.run(|||files|{
        Ok(files.journal.conn.query_row("SELECT (SELECT count(*) FROM synthesis_jobs),(SELECT count(*) FROM synthesis_processed),(SELECT count(*) FROM synthesized_outputs),(SELECT count(*) FROM diagnostic_pins WHERE owner LIKE 'synthesis:job:%')",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap())
    }).await.unwrap()
}

#[tokio::test]
async fn automatic_jobs_pin_committed_originals_survive_restart_and_never_enroll_their_own_output() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let automatic = automatic(seen.clone(), None);
    install(&mut outbox, &automatic).await;
    let manifest = complete(&mut outbox).await;
    let primary = original(&root, &manifest);
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    let health = backlog(&outbox).await;
    assert_eq!((health.pending, health.failed), (1, 0));
    assert_eq!(health.oldest_pending.unwrap().primary, primary);
    drop(outbox);
    let mut outbox = Outbox::open(&root).unwrap();
    assert!(
        outbox.reserve("fixture", &request()).await.is_err(),
        "a persisted host policy cannot run with no installed callback"
    );
    install(&mut outbox, &automatic).await;
    let SynthesisTurn::Committed { primary: actual, output, queue_retired } = turn(&outbox, &automatic).await else {
        panic!()
    };
    assert_eq!(actual, primary);
    assert!(queue_retired);
    let sealed = outbox.verify(&output.capture.observation_id).await.unwrap();
    assert_eq!(sealed.source_queries, 0);
    assert_eq!(sealed.request.collection(), TelemetryCollection::DerivedDiagnosis);
    assert_eq!(sealed.status, CollectionStatus::Complete);
    assert_eq!(counts(&outbox).await, (0, 1, 1, 0));
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Idle));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].primary, primary);
    assert!(seen[0].context.is_empty());
    assert!(seen[0].primary_rows > 0);
    assert!(seen[0].issues.contains(&SynthesisContextIssue::CatalogNotRetainedAsOf));
}

#[tokio::test]
async fn automatic_work_defers_for_every_live_native_lease_phase_and_never_recovers_that_owner() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let automatic = automatic(seen.clone(), None);
    install(&mut outbox, &automatic).await;
    complete(&mut outbox).await;
    let delivery = outbox.delivery();
    let mut live = outbox.reserve("fixture", &request()).await.unwrap();
    let identity = live.identity().clone();
    for phase in 0..3 {
        if phase == 1 {
            live.source_started().await.unwrap();
        }
        if phase == 2 {
            live.source_closed(SourceShutdown::Quiescent).await.unwrap();
        }
        let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
        assert!(matches!(
            delivery.synthesize_pending(&automatic, operation.handle(), ()).await.unwrap(),
            SynthesisTurn::Deferred { reason: SynthesisDeferral::NativeCaptureOwned }
        ));
        let status = delivery.status(20, None).await.unwrap();
        let retained = status.entries.iter().find(|entry| entry.capture == identity).unwrap();
        assert_eq!(retained.state, OutboxCaptureState::Capturing);
        assert_eq!(
            retained.source_work,
            [SourceWorkState::NotStarted, SourceWorkState::Active, SourceWorkState::Quiescent][phase]
        );
    }
    assert!(seen.lock().unwrap().is_empty());
    // Existing native fixture owner seals and finishes; the automatic worker never takes its client.
    let mut files = ObservationFiles::create(live.directory(), request(), identity, None).unwrap();
    files.describe(source()).unwrap();
    for (index, stream) in TelemetryCollection::QueryHistory.streams().iter().enumerate() {
        files.write_stream(index, &records(*stream)).unwrap();
    }
    files.confirm_source(SourceCompletion::Verified).unwrap();
    files.finish(0).unwrap();
    live.finish().await.unwrap();
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Committed { .. }));
}

#[tokio::test]
async fn cancellation_before_work_and_during_analysis_keeps_the_pending_original_and_frozen_plan() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let cancellation =
        Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let automatic = automatic(seen.clone(), Some(cancellation.clone()));
    install(&mut outbox, &automatic).await;
    complete(&mut outbox).await;
    let before = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let handle = before.handle();
    drop(before);
    assert!(outbox.delivery().synthesize_pending(&automatic, handle, ()).await.is_err());
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    let handle = cancellation.lock().unwrap().as_ref().unwrap().handle();
    assert!(outbox.delivery().synthesize_pending(&automatic, handle, ()).await.is_err());
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    let plan: String = outbox
        .files
        .run(|| {
            |files| {
                Ok(files.journal.conn.query_row("SELECT plan_json FROM synthesis_jobs", [], |row| row.get(0)).unwrap())
            }
        })
        .await
        .unwrap();
    assert!(!plan.is_empty());
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Committed { .. }));
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn registration_is_before_sources_and_input_backpressure_preserves_a_real_output_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut late) = box_at(&temp);
    complete(&mut late).await;
    let observer = automatic(Arc::new(Mutex::new(vec![])), None);
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    assert!(late.register_automatic_synthesis(&observer, operation.handle(), ()).await.is_err());
    let temp = tempfile::tempdir().unwrap();
    let mut outbox =
        Outbox::create(&temp.path().join("bounded"), OutboxLimits { max_captures: 3, ..Default::default() }).unwrap();
    install(&mut outbox, &observer).await;
    complete(&mut outbox).await;
    complete(&mut outbox).await;
    assert_eq!(kind(&outbox.reserve("fixture", &request()).await.err().unwrap()), OutboxProblem::Capacity);
    assert_eq!(counts(&outbox).await, (2, 0, 0, 2));
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 3);
}

#[tokio::test]
async fn empty_native_and_diagnostic_captures_are_both_enrolled_without_fabricated_row_references() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let automatic = automatic(seen.clone(), None);
    install(&mut outbox, &automatic).await;
    super::super::eviction::activity(&mut outbox, "2020-01-01T06:00:00Z", None).await;
    let input = outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    let diagnostic = outbox
        .commit_diagnostics(
            input.clone(),
            super::super::restored_diagnostics::pending_update(&input, "automatic-empty"),
        )
        .await
        .unwrap();
    assert_eq!(counts(&outbox).await.0, 2);
    for _ in 0..2 {
        assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Committed { .. }));
    }
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().any(|seen| seen.primary.capture == diagnostic));
    assert!(seen.iter().all(|seen| seen.primary_rows == 0));
    assert!(seen.iter().all(|seen| seen.context.is_empty()));
}

#[tokio::test]
async fn archival_releases_derived_pins_and_retires_completed_queue_and_output_metadata_with_the_primary() {
    use crate::delivery::recovery::stage_and_commit;
    let temp = tempfile::tempdir().unwrap();
    let (store, owner, contract, mut outbox) = super::super::eviction::remote_box(temp.path()).await;
    let observer = automatic(Arc::new(Mutex::new(vec![])), None);
    install(&mut outbox, &observer).await;
    let primary = complete(&mut outbox).await;
    let SynthesisTurn::Committed { output, .. } = turn(&outbox, &observer).await else { panic!() };
    let delivery = outbox.delivery();
    let prepared = delivery.prepare(&primary.capture.observation_id, contract.clone(), vec![]).await.unwrap();
    let native = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    assert!(!delivery.evict(&native).await.unwrap());
    let prepared = delivery.prepare(&output.capture.observation_id, contract, vec![]).await.unwrap();
    let derived = stage_and_commit(&store, owner.fence(), Some(&native.manifest().range_id), &prepared).await.unwrap();
    delivery.mark_archived(&derived).await.unwrap();
    assert!(delivery.evict(&native).await.unwrap());
    assert_eq!(counts(&outbox).await, (0, 0, 0, 0));
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Idle));
    assert!(store.load(&derived.manifest().range_id).await.is_ok());
}

async fn captured_fixture(
    outbox: &mut Outbox,
    source: CollectionDescription,
    rows: Vec<TelemetryRecord>,
) -> CollectionManifest {
    let collection = source.observation.collection();
    let selection = match collection {
        TelemetryCollection::Snapshot(dataset) => CollectionSelection::Snapshot { dataset },
        TelemetryCollection::QueryHistory => CollectionSelection::QueryHistory {
            from: source.query_history().unwrap().interval.from.clone(),
            until: source.query_history().unwrap().interval.until.clone(),
        },
        _ => panic!("this fixture uses existing catalog/history readers only"),
    };
    let request =
        CollectionRequest { subject: CollectionSubject::Database, selection, budget: CollectionBudget::default() };
    let mut lease = outbox.reserve("fixture", &request).await.unwrap();
    let mut files = ObservationFiles::create(lease.directory(), request, lease.identity().clone(), None).unwrap();
    lease.source_started().await.unwrap();
    files.describe(source).unwrap();
    for (index, stream) in collection.streams().iter().enumerate() {
        files
            .write_stream(index, &rows.iter().filter(|row| row.stream() == *stream).cloned().collect::<Vec<_>>())
            .unwrap();
    }
    files.confirm_source(SourceCompletion::Verified).unwrap();
    lease.source_closed(SourceShutdown::Quiescent).await.unwrap();
    let manifest = files.finish(0).unwrap();
    lease.finish().await.unwrap();
    manifest
}
fn history_rows(source: &CollectionDescription) -> Vec<TelemetryRecord> {
    let identity = plan();
    let query_id = identity.query.definition_id(source).unwrap();
    let plan_id = identity.definition_id(source).unwrap();
    let mut rows =
        TelemetryCollection::QueryHistory.streams().iter().flat_map(|stream| records(*stream)).collect::<Vec<_>>();
    for row in &mut rows {
        match row {
            TelemetryRecord::QueryRuntime(point) => {
                point.key.references.query.as_mut().unwrap().definition_id = Some(query_id.clone());
                point.key.references.plan.as_mut().unwrap().definition_id = Some(plan_id.clone());
            }
            TelemetryRecord::QueryDefinition(definition) => definition.id = query_id.clone(),
            TelemetryRecord::PlanDefinition(definition) => {
                definition.id = plan_id.clone();
                definition.query_definition_id = Some(query_id.clone());
            }
            _ => (),
        }
    }
    rows
}

fn catalog_with_native_incarnation(
    case: culprit_evidence::analytical::test_support::TableTemporalCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, rows) = culprit_evidence::analytical::test_support::table_temporal_evidence(case);
    let CollectionTarget::Database(database) = &mut source.target else { panic!() };
    database.incarnation = Some("recorded-native-catalog-generation".into());
    (source, rows)
}

#[tokio::test]
async fn selected_catalog_context_keeps_exact_incarnation_partial_coverage_and_its_own_acquisition_clock() {
    use culprit_evidence::analytical::test_support::TableTemporalCase;
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let (catalog_source, rows) = catalog_with_native_incarnation(TableTemporalCase::Partial);
    let catalog = captured_fixture(&mut outbox, catalog_source.clone(), rows).await;
    let mut history = source();
    history.target = catalog_source.target;
    let native = captured_fixture(&mut outbox, history.clone(), history_rows(&history)).await;
    for _ in 0..2 {
        assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    }
    let seen = seen.lock().unwrap();
    let result = seen.iter().find(|seen| seen.primary.capture == native.capture).unwrap();
    assert_eq!(result.context, vec![original(&root, &catalog)]);
    assert!(!result.issues.contains(&SynthesisContextIssue::CatalogNotRetainedAsOf));
    assert_eq!(
        catalog.description.unwrap().observation.snapshot().unwrap().sources[0].state,
        culprit_core::SourceState::Partial
    );
    assert!(
        chrono::DateTime::parse_from_rfc3339(catalog.finished_at.as_ref().unwrap()).unwrap()
            <= chrono::DateTime::parse_from_rfc3339(&native.started_at).unwrap()
    );
}

#[tokio::test]
async fn newer_catalogs_and_changed_native_database_generations_are_not_substituted_for_missing_context() {
    use culprit_evidence::analytical::test_support::TableTemporalCase;
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let (catalog_source, rows) = catalog_with_native_incarnation(TableTemporalCase::SystemVersioned);
    let mut history = source();
    history.target = catalog_source.target.clone();
    let first = captured_fixture(&mut outbox, history.clone(), history_rows(&history)).await;
    let catalog = captured_fixture(&mut outbox, catalog_source.clone(), rows).await;
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    assert!(
        seen.lock().unwrap().iter().find(|seen| seen.primary.capture == first.capture).unwrap().context.is_empty(),
        "future acquisition is not historical context"
    );
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    let matching = captured_fixture(&mut outbox, history.clone(), history_rows(&history)).await;
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .find(|seen| seen.primary.capture == matching.capture)
            .unwrap()
            .context
            .iter()
            .any(|reference| reference.capture == catalog.capture),
        "the same source and generation must actually have usable earlier catalog context"
    );
    let CollectionTarget::Database(database) = &mut history.target else { panic!() };
    database.incarnation = Some("different-native-database-generation".into());
    let changed = captured_fixture(&mut outbox, history.clone(), history_rows(&history)).await;
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    assert!(
        seen.lock().unwrap().iter().find(|seen| seen.primary.capture == changed.capture).unwrap().context.is_empty(),
        "same route label cannot repair a native incarnation mismatch"
    );
}

#[tokio::test]
async fn late_canonical_corruption_never_reaches_analysis_or_installs_a_successful_queue_result() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let primary = complete(&mut outbox).await;
    let last = primary.files.last().unwrap();
    let path = root.join("captures").join(&primary.capture.observation_id).join(&last.file);
    let mut bytes = fs::read(&path).unwrap();
    bytes.push(b'x');
    fs::write(path, bytes).unwrap();
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Failed { .. }));
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(counts(&outbox).await, (0, 1, 0, 0));
    let health = backlog(&outbox).await;
    assert_eq!((health.pending, health.failed), (0, 1));
    assert!(health.oldest_pending.is_none());
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Idle));
    assert_eq!(backlog(&outbox).await.failed, 1, "no pending work is not a healthy failed attempt");
    let status = outbox.status(10, None).await.unwrap();
    assert_eq!(status.source_debts, 0);
    assert!(status.entries.iter().all(|entry| entry.state != OutboxCaptureState::Ready));
}

#[tokio::test]
async fn a_failed_queue_acknowledgement_returns_the_known_commit_and_restart_reuses_that_exact_output() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen, None);
    install(&mut outbox, &observer).await;
    complete(&mut outbox).await;
    journal_fault(
        &outbox,
        "CREATE TRIGGER fail_synthesis_ack BEFORE DELETE ON synthesis_jobs BEGIN SELECT RAISE(ABORT,'controlled'); END",
    )
    .await;
    let SynthesisTurn::Committed { output, queue_retired, .. } = turn(&outbox, &observer).await else { panic!() };
    assert!(!queue_retired);
    assert_eq!(counts(&outbox).await, (1, 0, 1, 1));
    journal_fault(&outbox, "DROP TRIGGER fail_synthesis_ack").await;
    drop(outbox);
    let mut outbox = Outbox::open(&root).unwrap();
    install(&mut outbox, &observer).await;
    let SynthesisTurn::Committed { output: replayed, queue_retired, .. } = turn(&outbox, &observer).await else {
        panic!()
    };
    assert!(queue_retired);
    assert_eq!(replayed, output);
    assert_eq!(counts(&outbox).await, (0, 1, 1, 0));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
}

#[tokio::test]
async fn a_real_pending_diagnostic_follows_exact_native_ancestry_and_missing_archived_context_remains_explicit() {
    use crate::delivery::recovery::stage_and_commit;
    let temp = tempfile::tempdir().unwrap();
    let (store, owner, contract, mut outbox) = super::super::eviction::remote_box(temp.path()).await;
    let root = temp.path().join("outbox");
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let actor:ActivityObservation=serde_json::from_value(serde_json::json!({"key":{"session":{"id":"61","generation":"login-1"},"request":{"id":"0","generation":"request-1"}},"blocking_status":"known","idle":false,"scope":"request-database","state":"running","open_transactions":0,"elapsed_ms":"120000","session_counters":{},"request_counters":{},"statement":{"state":"unavailable","reason":"not-collected"}})).unwrap();
    let primary = super::super::eviction::activity(&mut outbox, "2020-01-01T06:00:00Z", Some(actor)).await;
    let input = outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    let native_original = input.parent.clone();
    let diagnostic = outbox
        .commit_diagnostics(
            input.clone(),
            super::super::restored_diagnostics::pending_update(&input, "automatic-ancestry"),
        )
        .await
        .unwrap();
    // Canonical selection uses the same closed Core reference walker that automatic synthesis consumes.
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let diagnostic_manifest = outbox.verify(&diagnostic.observation_id).await.unwrap();
    let diagnostic_ref = original(&root, &diagnostic_manifest);
    let expected = native_original.clone();
    outbox
        .delivery()
        .with_synthesis_inputs(selected(diagnostic_ref.clone(), vec![]), operation.handle(), (), move |capture| {
            let revision = capture
                .primary()
                .records()
                .iter()
                .find_map(|row| match &row.record {
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::DiagnosticRevision(revision),
                        ..
                    }) => Some(revision),
                    _ => None,
                })
                .unwrap();
            assert!(revision.referenced_evidence()?.iter().any(|link| match link.target {
                culprit_core::DiagnosticAncestryTarget::Record(row) => row.capture_ref() == expected,
                culprit_core::DiagnosticAncestryTarget::Capture(capture) => *capture == expected,
            }));
            Ok(())
        })
        .await
        .unwrap();
    let SynthesisTurn::Committed { output, .. } = turn(&outbox, &observer).await else { panic!() };
    let delivery = outbox.delivery();
    let prepared = delivery.prepare(&primary.capture.observation_id, contract.clone(), vec![]).await.unwrap();
    let native = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    let prepared = delivery.prepare(&diagnostic.observation_id, contract.clone(), vec![]).await.unwrap();
    let diagnostic_commit =
        stage_and_commit(&store, owner.fence(), Some(&native.manifest().range_id), &prepared).await.unwrap();
    delivery.mark_archived(&diagnostic_commit).await.unwrap();
    let prepared = delivery.prepare(&output.capture.observation_id, contract, vec![]).await.unwrap();
    let output_commit =
        stage_and_commit(&store, owner.fence(), Some(&diagnostic_commit.manifest().range_id), &prepared).await.unwrap();
    delivery.mark_archived(&output_commit).await.unwrap();
    assert!(delivery.evict(&native).await.unwrap());
    let SynthesisTurn::Committed { primary, .. } = turn(&outbox, &observer).await else { panic!() };
    assert_eq!(primary, diagnostic_ref);
    let seen = seen.lock().unwrap();
    let answer = seen.iter().find(|entry| entry.primary == diagnostic_ref).unwrap();
    assert!(answer.context.is_empty());
    assert!(answer.issues.contains(&SynthesisContextIssue::MissingLocalOriginal { capture: native_original }));
    assert_eq!(answer.primary_rows, 1);
}

#[tokio::test]
async fn automatic_diagnostic_context_is_the_exact_authenticated_original_with_its_recorded_session_generation() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let actor:ActivityObservation=serde_json::from_value(serde_json::json!({"key":{"session":{"id":"61","generation":"login-1"},"request":{"id":"0","generation":"request-1"}},"blocking_status":"known","idle":false,"scope":"request-database","state":"running","open_transactions":0,"elapsed_ms":"120000","session_counters":{},"request_counters":{},"statement":{"state":"unavailable","reason":"not-collected"}})).unwrap();
    let native = super::super::eviction::activity(&mut outbox, "2020-01-01T06:00:00Z", Some(actor)).await;
    let input = outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    let diagnostic = outbox
        .commit_diagnostics(
            input.clone(),
            super::super::restored_diagnostics::pending_update(&input, "automatic-positive-ancestry"),
        )
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    }
    let seen = seen.lock().unwrap();
    let diagnostic = seen.iter().find(|entry| entry.primary.capture == diagnostic).unwrap();
    assert_eq!(diagnostic.context, vec![original(&root, &native)]);
    assert!(!diagnostic.issues.iter().any(|issue| matches!(
        issue,
        SynthesisContextIssue::MissingLocalOriginal { .. } | SynthesisContextIssue::UnavailableOriginal { .. }
    )));
}

struct HeldAnalysis {
    ordinary: QueueAnalysis,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl Synthesizer for HeldAnalysis {
    fn analyzer(&self) -> ComponentVersion {
        self.ordinary.analyzer()
    }
    fn rules_version(&self) -> &str {
        self.ordinary.rules_version()
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        self.entered.lock().unwrap().take().unwrap().send(()).unwrap();
        let (mutex, ready) = self.release.as_ref();
        let mut released = mutex.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        self.ordinary.synthesize(input)
    }
}
struct WorkerGuard(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
#[tokio::test]
async fn dropping_the_async_queue_caller_retains_files_and_the_external_guard_until_the_started_worker_stops() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let (entered, started) = tokio::sync::oneshot::channel();
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let observer = Arc::new(
        AutomaticSynthesis::new(
            Arc::new(HeldAnalysis {
                ordinary: QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None },
                entered: Mutex::new(Some(entered)),
                release: release.clone(),
            }),
            CompletedCaptureBudget::default(),
        )
        .unwrap(),
    );
    install(&mut outbox, &observer).await;
    complete(&mut outbox).await;
    let delivery = outbox.delivery();
    let automatic = observer.clone();
    let (closed, mut settled) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
        delivery.synthesize_pending(&automatic, operation.handle(), WorkerGuard(Some(closed))).await
    });
    started.await.unwrap();
    caller.abort();
    let _ = caller.await;
    assert!(
        matches!(settled.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
        "the actual blocking worker still owns the guard"
    );
    {
        let (mutex, ready) = release.as_ref();
        *mutex.lock().unwrap() = true;
        ready.notify_one();
    }
    settled.await.unwrap();
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
}

#[tokio::test]
async fn malformed_terminal_health_cannot_be_reported_as_an_empty_healthy_queue() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let observer = automatic(Arc::new(Mutex::new(vec![])), None);
    install(&mut outbox, &observer).await;
    complete(&mut outbox).await;
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    journal_fault(&outbox, "UPDATE synthesis_processed SET result_json='{}'").await;
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    assert!(outbox.delivery().synthesis_backlog(operation.handle(), ()).await.is_err());
}

mod failures;
mod informational;
