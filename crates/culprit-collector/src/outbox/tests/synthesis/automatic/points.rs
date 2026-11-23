//! Real canonical capture/Ready/queue paths; controlled native bodies are not source-cost qualification.
use super::*;
use culprit_evidence::analytical::test_support::{CommitPipelineCase as Case, commit_pipeline_fixture};

async fn originals(outbox: &mut Outbox, case: Case) -> Vec<CollectionManifest> {
    let mut manifests = Vec::new();
    for fixture in commit_pipeline_fixture(case).originals {
        manifests.push(Box::pin(captured_fixture(outbox, fixture.source, fixture.records)).await);
    }
    manifests
}
async fn settle(outbox: &Outbox, observer: &AutomaticSynthesis, count: usize) {
    for _ in 0..count {
        assert!(matches!(turn(outbox, observer).await, SynthesisTurn::Committed { .. }));
    }
}

#[tokio::test]
async fn log_context_uses_all_four_exact_original_endpoints_without_acquiring_a_native_source() {
    let temporary = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let manifests = originals(&mut outbox, Case::Exact).await;
    let expected = manifests[1..].iter().map(|manifest| original(&root, manifest)).collect::<Vec<_>>();
    settle(&outbox, &observer, manifests.len()).await;
    {
        let observed = seen.lock().unwrap();
        let primary = observed.iter().find(|seen| seen.primary.capture == manifests[0].capture).unwrap();
        assert_eq!(primary.context.len(), 4);
        assert!(expected.iter().all(|capture| primary.context.contains(capture)));
        assert!(
            primary.issues.contains(&SynthesisContextIssue::IndependentPointRole),
            "catalog file allocation retains its independently unknown native role"
        );
        assert!(!primary.issues.contains(&SynthesisContextIssue::PointNotRetainedNearObservation));
    }
    assert_eq!(outbox.status(20, None).await.unwrap().source_debts, 0);
}

#[tokio::test]
async fn partial_originals_remain_explicit_and_changed_transaction_role_does_not_discard_file_context() {
    for case in [Case::Partial, Case::UnknownRole, Case::ChangedRole] {
        let temporary = tempfile::tempdir().unwrap();
        let (root, mut outbox) = box_at(&temporary);
        let seen = Arc::new(Mutex::new(vec![]));
        let observer = automatic(seen.clone(), None);
        install(&mut outbox, &observer).await;
        let manifests = originals(&mut outbox, case).await;
        settle(&outbox, &observer, manifests.len()).await;
        let observed = seen.lock().unwrap();
        let selected = observed.iter().find(|seen| seen.primary.capture == manifests[0].capture).unwrap();
        assert!(selected.context.contains(&original(&root, &manifests[3])), "{case:?}");
        assert!(selected.context.contains(&original(&root, &manifests[4])), "{case:?}");
        if case == Case::Partial {
            assert!(selected.context.contains(&original(&root, &manifests[2])));
            assert!(
                manifests[2]
                    .description
                    .as_ref()
                    .unwrap()
                    .observation
                    .snapshot()
                    .unwrap()
                    .sources
                    .iter()
                    .any(|source| source.state == culprit_core::SourceState::Partial)
            );
        } else {
            assert!(!selected.context.contains(&original(&root, &manifests[2])), "{case:?}");
            assert!(selected.issues.contains(&SynthesisContextIssue::IndependentPointRole));
        }
    }
}

#[tokio::test]
async fn foreign_member_restart_and_stale_points_are_not_substituted_by_route_name() {
    for case in [Case::ForeignMember, Case::Restart] {
        let temporary = tempfile::tempdir().unwrap();
        let (root, mut outbox) = box_at(&temporary);
        let seen = Arc::new(Mutex::new(vec![]));
        let observer = automatic(seen.clone(), None);
        install(&mut outbox, &observer).await;
        let manifests = originals(&mut outbox, case).await;
        settle(&outbox, &observer, manifests.len()).await;
        let observed = seen.lock().unwrap();
        let selected = observed.iter().find(|seen| seen.primary.capture == manifests[0].capture).unwrap();
        for changed in [&manifests[2], &manifests[4]] {
            assert!(!selected.context.contains(&original(&root, changed)), "{case:?}");
        }
    }
    let temporary = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let mut fixtures = commit_pipeline_fixture(Case::Exact).originals;
    let original = fixtures.remove(0);
    let mut point = fixtures.remove(0);
    let SourceObservation::LocalResource { snapshot, .. } = &mut point.source.observation else { panic!() };
    snapshot.observed_from = "2020-01-01T11:50:00Z".into();
    snapshot.observed_until = snapshot.observed_from.clone();
    captured_fixture(&mut outbox, point.source, point.records).await;
    let primary = captured_fixture(&mut outbox, original.source, original.records).await;
    settle(&outbox, &observer, 2).await;
    let observed = seen.lock().unwrap();
    let selected = observed.iter().find(|seen| seen.primary.capture == primary.capture).unwrap();
    assert!(selected.context.is_empty());
    assert!(selected.issues.contains(&SynthesisContextIssue::PointOutsideObservationWindow));
}

#[tokio::test]
async fn equal_native_clock_alternatives_are_ambiguous_and_not_two_distinct_measurements() {
    let temporary = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let mut fixtures = commit_pipeline_fixture(Case::Exact).originals;
    let original = fixtures.remove(0);
    let point = fixtures.remove(0);
    for _ in 0..2 {
        captured_fixture(&mut outbox, point.source.clone(), point.records.clone()).await;
    }
    let primary = captured_fixture(&mut outbox, original.source, original.records).await;
    settle(&outbox, &observer, 3).await;
    let observed = seen.lock().unwrap();
    let selected = observed.iter().find(|seen| seen.primary.capture == primary.capture).unwrap();
    assert!(selected.context.is_empty());
    assert!(selected.issues.contains(&SynthesisContextIssue::AmbiguousPointContext));
}

#[tokio::test]
async fn cancelled_plan_replays_the_exact_point_refs_after_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let cancellation =
        Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let observer = automatic(seen.clone(), Some(cancellation.clone()));
    install(&mut outbox, &observer).await;
    let manifests = originals(&mut outbox, Case::Exact).await;
    let operation = cancellation.lock().unwrap().as_ref().unwrap().handle();
    assert!(outbox.delivery().synthesize_pending(&observer, operation, ()).await.is_err());
    let expected = seen.lock().unwrap()[0].context.clone();
    assert_eq!(expected.len(), 4);
    drop(outbox);
    let mut outbox = Outbox::open(&root).unwrap();
    install(&mut outbox, &observer).await;
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    assert_eq!(seen.lock().unwrap()[1].context, expected);
    assert_eq!(seen.lock().unwrap()[1].primary.capture, manifests[0].capture);
}

#[tokio::test]
async fn a_foreign_canonical_point_inserted_into_a_frozen_plan_is_refused_before_analysis() {
    let temporary = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let cancellation =
        Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let observer = automatic(seen.clone(), Some(cancellation.clone()));
    install(&mut outbox, &observer).await;
    let manifests = originals(&mut outbox, Case::Exact).await;
    let operation = cancellation.lock().unwrap().as_ref().unwrap().handle();
    assert!(outbox.delivery().synthesize_pending(&observer, operation, ()).await.is_err());
    let foreign = commit_pipeline_fixture(Case::ForeignMember).originals.remove(2);
    let foreign = captured_fixture(&mut outbox, foreign.source, foreign.records).await;
    let reference = original(&root, &foreign);
    let primary_id = manifests[0].capture.observation_id.clone();
    outbox
        .files
        .run(move || {
            move |files| {
                let encoded: String = files
                    .journal
                    .conn
                    .query_row("SELECT plan_json FROM synthesis_jobs WHERE primary_id=?1", [&primary_id], |row| {
                        row.get(0)
                    })
                    .unwrap();
                let mut plan: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                let before: DiagnosticCaptureRef = serde_json::from_value(plan["context"][0].clone()).unwrap();
                let previous = before.key().unwrap();
                let next = reference.key().unwrap();
                plan["context"][0] = serde_json::to_value(reference).unwrap();
                for key in plan["points"].as_array_mut().unwrap() {
                    if key.as_str() == Some(&previous) {
                        *key = serde_json::Value::String(next.clone());
                    }
                }
                files
                    .journal
                    .conn
                    .execute(
                        "UPDATE synthesis_jobs SET plan_json=?1 WHERE primary_id=?2",
                        rusqlite::params![serde_json::to_string(&plan).unwrap(), primary_id],
                    )
                    .unwrap();
                Ok(())
            }
        })
        .await
        .unwrap();
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Failed { .. }));
    assert_eq!(seen.lock().unwrap().len(), 1, "authentic foreign rows cannot repair the original native owner");
}

#[tokio::test]
async fn later_retained_points_do_not_rewrite_an_already_frozen_missing_context() {
    let temporary = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let cancellation =
        Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let observer = automatic(seen.clone(), Some(cancellation.clone()));
    install(&mut outbox, &observer).await;
    let mut fixtures = commit_pipeline_fixture(Case::Exact).originals;
    let primary = fixtures.remove(0);
    captured_fixture(&mut outbox, primary.source, primary.records).await;
    let operation = cancellation.lock().unwrap().as_ref().unwrap().handle();
    assert!(outbox.delivery().synthesize_pending(&observer, operation, ()).await.is_err());
    for point in fixtures {
        captured_fixture(&mut outbox, point.source, point.records).await;
    }
    assert!(matches!(turn(&outbox, &observer).await, SynthesisTurn::Committed { .. }));
    let observed = seen.lock().unwrap();
    assert!(observed[0].context.is_empty());
    assert!(observed[1].context.is_empty());
    assert_eq!(observed[0].issues, observed[1].issues);
    assert!(observed[1].issues.contains(&SynthesisContextIssue::PointNotRetainedNearObservation));
}

#[tokio::test]
async fn retained_native_points_and_catalogs_share_one_unchanged_head_and_pin_allowance() {
    let temporary = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temporary);
    let observer = automatic(Arc::new(Mutex::new(vec![])), None);
    install(&mut outbox, &observer).await;
    let point = commit_pipeline_fixture(Case::Exact).originals.remove(1);
    let cap = crate::synthesis::MAX_SYNTHESIS_RETAINED_HEADS;
    assert_eq!(cap, 64);
    for index in 0..=cap {
        let mut source = point.source.clone();
        source.source = format!("controlled-commit-context-{index}");
        captured_fixture(&mut outbox, source, point.records.clone()).await;
    }
    let (heads, pins, jobs): (u32, u32, u32) = outbox.files.run(|| |files| {
        Ok(files.journal.conn.query_row("SELECT (SELECT count(*) FROM synthesis_context_heads),(SELECT count(*) FROM diagnostic_pins WHERE owner LIKE 'synthesis:context-head:%'),(SELECT count(*) FROM synthesis_jobs)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap())
    }).await.unwrap();
    assert_eq!((heads, pins), (cap, cap));
    assert_eq!(jobs, cap + 1, "head retirement never discards pending original work");
}

#[tokio::test]
async fn a_route_without_a_qualified_native_database_epoch_retains_no_cross_capture_point_context() {
    let temporary = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let mut manifests = Vec::new();
    for mut fixture in commit_pipeline_fixture(Case::Exact).originals {
        let CollectionTarget::Database(database) = &mut fixture.source.target else { panic!() };
        database.incarnation = None;
        if let SourceObservation::Activity(header) = &mut fixture.source.observation {
            // A genuine unresolved original can retain its rows with a partial scope; it cannot lend
            // independent points a missing native recovery/process generation through the route name.
            header.epoch = None;
            header.coverage.state = culprit_core::SourceState::Partial;
            header.coverage.complete_scope = false;
            let SourcePartition::InstanceLocal { epoch, .. } = &mut fixture.source.partition else { panic!() };
            *epoch = None;
        }
        manifests.push(captured_fixture(&mut outbox, fixture.source, fixture.records).await);
    }
    settle(&outbox, &observer, manifests.len()).await;
    let observed = seen.lock().unwrap();
    let primary = observed.iter().find(|seen| seen.primary.capture == manifests[0].capture).unwrap();
    assert!(primary.context.is_empty());
    assert!(primary.issues.contains(&SynthesisContextIssue::UnqualifiedPointScope));
}

async fn metadata_point(
    outbox: &mut Outbox,
    root: &Path,
    original_case: culprit_evidence::analytical::test_support::TransactionMetadataOriginal,
    mut source: CollectionDescription,
    rows: Vec<TelemetryRecord>,
) -> CollectionManifest {
    let basis =
        captured_fixture(outbox, original_case.source.clone(), vec![original_case.original.record.clone()]).await;
    let captured = original(root, &basis);
    let actor = match &original_case.original.record {
        TelemetryRecord::Activity(actor) => actor.as_ref(),
        _ => panic!(),
    };
    let exported = ExportedRecord::new(&original_case.source, &basis.capture, actor.clone().into()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&original_case.source, &captured.manifest_sha256, &exported, 0).unwrap();
    let mechanism =
        actor.wait.as_ref().and_then(|wait| wait.class).and_then(TransactionMetadataWait::from_class).unwrap();
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&original_case.source).unwrap(),
        DiagnosticSignal::TransactionMetadataWait {
            key: actor.key.clone(),
            statement_identity: actor.statement_identity.clone(),
            mechanism,
        },
        original_case.source.observation.activity().unwrap().at.clone(),
        reference,
    )
    .unwrap();
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let SourceObservation::SourceRead { focus_key, execution, .. } = &mut source.observation else { panic!() };
    *focus_key = focus.key().unwrap();
    let execution = SourceReadExecutionScope::from_context(focus.question, execution).unwrap();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: CollectionBudget::default(),
    };
    let mut lease = outbox.reserve("fixture", &request).await.unwrap();
    let mut files = ObservationFiles::create(lease.directory(), request, lease.identity().clone(), None).unwrap();
    lease.source_started().await.unwrap();
    files.describe(source).unwrap();
    files.write_stream(0, &rows).unwrap();
    files.confirm_source(SourceCompletion::Verified).unwrap();
    lease.source_closed(SourceShutdown::Quiescent).await.unwrap();
    let manifest = files.finish(0).unwrap();
    lease.finish().await.unwrap();
    manifest
}
#[tokio::test]
async fn transaction_metadata_reuses_retained_point_heads_and_frozen_eof_context_without_new_source_work() {
    use culprit_evidence::analytical::test_support::{
        TransactionMetadataSlruCase, transaction_metadata_original, transaction_metadata_slru_original,
        transaction_metadata_slru_pair,
    };
    let temporary = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let current = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
    let [before, after] =
        transaction_metadata_slru_pair(&current.focus, &current.execution, TransactionMetadataSlruCase::Continued);
    let earlier = metadata_point(&mut outbox, &root, transaction_metadata_slru_original(), before.0, before.1).await;
    let current = metadata_point(&mut outbox, &root, current, after.0, after.1).await;
    settle(&outbox, &observer, 4).await;
    {
        let recorded = seen.lock().unwrap();
        let current_plan = recorded.iter().find(|entry| entry.primary.capture == current.capture).unwrap();
        assert!(current_plan.context.contains(&original(&root, &earlier)));
        assert!(
            !current_plan.context.contains(&original(&root, &current)),
            "current point cannot select itself or hide the adjacent endpoint"
        );
        assert!(current_plan.primary_rows > 0);
    }
    assert_eq!(outbox.status(20, None).await.unwrap().source_debts, 0);
}
#[tokio::test]
async fn unrelated_commit_wait_does_not_import_transaction_metadata_actor_or_xmin_context() {
    use culprit_evidence::analytical::test_support::{
        TransactionMetadataSlruCase, transaction_metadata_original, transaction_metadata_slru_pair,
    };
    let temporary = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temporary);
    let seen = Arc::new(Mutex::new(vec![]));
    let observer = automatic(seen.clone(), None);
    install(&mut outbox, &observer).await;
    let metadata = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
    let [_, point] =
        transaction_metadata_slru_pair(&metadata.focus, &metadata.execution, TransactionMetadataSlruCase::Continued);
    let point = metadata_point(&mut outbox, &root, metadata, point.0, point.1).await;
    let commit = commit_pipeline_fixture(Case::Exact).originals.remove(0);
    let commit = captured_fixture(&mut outbox, commit.source, commit.records).await;
    settle(&outbox, &observer, 3).await;
    let observed = seen.lock().unwrap();
    let selected = observed.iter().find(|entry| entry.primary.capture == commit.capture).unwrap();
    assert!(
        !selected.context.contains(&original(&root, &point)),
        "shared counters are not an unrelated commit diagnosis's actor roster"
    );
}
