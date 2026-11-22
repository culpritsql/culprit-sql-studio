//! Real planner/scheduler/Outbox answer ownership over controlled native-shaped inputs; no native approval.
use super::*;
use culprit_evidence::analytical::test_support::{worker_starvation_basis, worker_starvation_result};

pub(in crate::fleet::investigations) fn frame(focus: &SourceReadFocus, execution: &SourceReadExecutionScope) -> Frame {
    let (source, records) = worker_starvation_result(focus, execution);
    Frame { source, records }
}
async fn fixture(shared: bool, qualified: bool) -> Harness {
    let (source, records) = worker_starvation_basis(shared);
    let mut fixture = if shared {
        Harness::with_shared_original(Frame { source, records }).await
    } else {
        Harness::with_activity(Frame { source, records }).await
    };
    fixture.backend.recipe.id = "fixture.worker-starvation".into();
    fixture.backend.recipe.selector = SourceReadQuestion::WorkerStarvation.selector();
    let context = SourceReadExecutionScope::context_from_source(&fixture.backend.activity.source).unwrap();
    let RecipeAdmission::Measured(receipt) = &mut fixture.backend.recipe.admission else { panic!() };
    receipt.recipe_id = fixture.backend.recipe.id.clone();
    receipt.selector = fixture.backend.recipe.selector;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &context).unwrap(),
    );
    if !qualified {
        fixture.backend.recipe.admission = RecipeAdmission::Unqualified;
    }
    fixture
}
async fn owed(runtime: &Runtime) -> Vec<DiagnosticEpisode> {
    runtime
        .fixture
        .outbox
        .diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 8)
        .await
        .unwrap()
        .checkpoints
        .into_iter()
        .flat_map(|checkpoint| checkpoint.episodes)
        .filter(|episode| {
            episode
                .source_reads
                .get(SourceReadQuestion::WorkerStarvation)
                .is_some_and(|stage| stage.focus.question == SourceReadQuestion::WorkerStarvation)
        })
        .collect()
}
async fn answered(runtime: &Runtime) -> DiagnosticEpisode {
    let status = runtime.fixture.outbox.status(100, None).await.unwrap();
    assert!(status.next_cursor.is_none());
    let latest = status
        .entries
        .into_iter()
        .filter(|entry| entry.request.collection() == TelemetryCollection::Diagnostics)
        .max_by_key(|entry| entry.capture.revision.as_ref().unwrap().sequence.parse::<u64>().unwrap())
        .unwrap();
    let operation = culprit_core::limits::OperationGuard::new(std::time::Duration::from_secs(5)).unwrap();
    runtime
        .fixture
        .outbox
        .delivery()
        .with_completed_diagnostics(
            crate::outbox::CompletedCaptureLookup {
                connection: &latest.connection,
                observation_id: &latest.capture.observation_id,
            },
            crate::outbox::CompletedCaptureBudget::default(),
            operation.handle(),
            (),
            |capture| {
                Ok(capture.records().iter().find_map(|row| match &row.record {
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::DiagnosticRevision(revision),
                        ..
                    }) if revision
                        .episode
                        .source_reads
                        .get(SourceReadQuestion::WorkerStarvation)
                        .is_some_and(|stage| stage.focus.question == SourceReadQuestion::WorkerStarvation) =>
                    {
                        Some(revision.episode.clone())
                    }
                    _ => None,
                }))
            },
        )
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn request_and_sessionless_shared_originals_plan_one_bounded_question_and_persist_the_exact_answer_without_reopen()
 {
    for shared in [false, true] {
        let mut runtime = Runtime::from_fixture(fixture(shared, true).await).await;
        let original = owed(&runtime).await.into_iter().next().unwrap();
        assert_eq!(original.signal.activity_key().is_none(), shared);
        assert!(runtime.enroll().await);
        let job = runtime.take_focus().await;
        let result = runtime.execute(&job).await;
        assert_eq!(result.event.status, CollectionStatus::Complete);
        let Outcome::FocusedComplete { result: capture } = result.outcome else { panic!() };
        let input = runtime
            .fixture
            .outbox
            .diagnostic_input(&capture.capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION)
            .await
            .unwrap();
        assert_eq!(input.rows.len(), 1, "the exact canonical worker point is retained, not a pointer-only answer");
        runtime.finish(Outcome::FocusedComplete { result: capture.clone() }).await;
        let policies = runtime.policies();
        assert_eq!(
            analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
                .await
                .unwrap(),
            1
        );
        let completed = answered(&runtime).await;
        assert_eq!(completed.first, original.first);
        assert_eq!(completed.report, original.report);
        let SourceReadStageState::Answered { answer } =
            &completed.source_reads.get(SourceReadQuestion::WorkerStarvation).unwrap().state
        else {
            panic!()
        };
        let SourceReadAnswer::WorkerStarvation(answer) = answer.as_ref() else { panic!() };
        assert_eq!(answer.result, capture);
        assert_eq!(answer.evidence.unassigned_threadpool_waits, 1);
        assert_eq!(answer.evidence.assigned_workers, 4);
        runtime.reconcile().await;
        assert!(!runtime.enroll().await);
        assert_eq!(
            runtime.fixture.outbox.diagnostic_input_state(&capture, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
            DiagnosticInputState::Consumed
        );
    }
}
#[tokio::test]
async fn unqualified_worker_recipes_retain_both_original_trigger_shapes_and_open_no_native_followup() {
    for shared in [false, true] {
        let mut runtime = Runtime::from_fixture(fixture(shared, false).await).await;
        let before = runtime.native_calls();
        let episodes = owed(&runtime).await;
        assert_eq!(episodes.len(), 1);
        assert!(matches!(
            episodes[0].source_reads.get(SourceReadQuestion::WorkerStarvation).unwrap().state,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }
        ));
        assert!(!runtime.enroll().await);
        assert_eq!(runtime.native_calls(), before);
    }
}
#[tokio::test]
async fn qualified_complete_actor_disappearance_stops_new_worker_escalation_and_preserves_the_original_question() {
    let mut runtime = Runtime::from_fixture(fixture(false, false).await).await;
    let original = owed(&runtime).await.into_iter().next().unwrap();
    let before = runtime.native_calls();
    runtime.complete_empty_baseline().await;
    assert!(!runtime.enroll().await);
    let recovered = answered(&runtime).await;
    assert_eq!(recovered.first, original.first);
    assert_eq!(recovered.lifecycle, DiagnosticLifecycle::Resolved);
    assert!(matches!(
        recovered.source_reads.get(SourceReadQuestion::WorkerStarvation).unwrap().state,
        SourceReadStageState::Recovered
    ));
    assert!(recovered.recovery.is_some());
    assert!(runtime.native_calls().iter().zip(before).all(|(after, before)| *after >= before));
}
#[tokio::test]
async fn source_failure_cancel_and_context_change_are_persisted_without_successful_worker_answers() {
    for stopped in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::SourceChanged] {
        let mut fixture = fixture(false, true).await;
        fixture.backend.focus_failure = Some(stopped);
        let mut runtime = Runtime::from_fixture(fixture).await;
        assert!(runtime.enroll().await);
        let first = owed(&runtime).await.into_iter().next().unwrap().first;
        let job = runtime.take_focus().await;
        let result = runtime.execute(&job).await;
        assert_ne!(result.event.status, CollectionStatus::Complete);
        runtime.finish(result.outcome).await;
        runtime.reconcile().await;
        let status = runtime.fixture.outbox.status(100, None).await.unwrap();
        assert!(!status.entries.iter().any(|entry| entry.request.collection()
            == TelemetryCollection::Snapshot(SnapshotDataset::WorkerStarvation)
            && entry.state == OutboxCaptureState::Ready));
        let retained = answered(&runtime).await;
        assert_eq!(retained.first, first);
        assert!(!matches!(
            retained.source_reads.get(SourceReadQuestion::WorkerStarvation).unwrap().clone().state,
            SourceReadStageState::Answered { .. }
        ));
    }
}
