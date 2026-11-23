//! The actual controller, journal and bounded source pipeline over controlled native-shaped originals.
use super::*;
use culprit_evidence::analytical::test_support::{
    ExecutionProfileCase, current_execution_profile_basis, current_execution_profile_result,
    current_execution_profile_work,
};
async fn prepared(qualified: bool) -> Runtime {
    let (source, records) = current_execution_profile_basis();
    let mut harness = fixture_for(Frame { source, records }).await;
    let mut profile = harness.backend.recipe.clone();
    profile.id = "controlled.current-execution-profile".into();
    profile.selector = SourceReadQuestion::CurrentExecutionProfile.selector();
    let RecipeAdmission::Measured(receipt) = &mut profile.admission else { panic!() };
    receipt.recipe_id = profile.id.clone();
    receipt.selector = profile.selector;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(
            SourceReadQuestion::CurrentExecutionProfile,
            &NativeReadContext::from_activity(&harness.backend.activity.source).unwrap(),
        )
        .unwrap(),
    );
    if !qualified {
        profile.admission = RecipeAdmission::Unqualified;
    }
    harness.backend.additional_recipes.push(profile);
    let mut runtime = Runtime::from_fixture(harness).await;
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    let participant = intent.focus.activity_participant().unwrap();
    let (source, records) = current_execution_profile_work(participant);
    runtime.fixture.backend.current_work = Some(Frame { source, records });
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    runtime.finish(result.outcome).await;
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    // The real runner settles the completed CurrentWork scheduler owner before admitting another question.
    let calls = runtime.native_calls();
    runtime.reconcile().await;
    assert_eq!(runtime.native_calls(), calls);
    runtime
}
async fn episode(runtime: &Runtime) -> DiagnosticEpisode {
    runtime
        .fixture
        .outbox
        .diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 8)
        .await
        .unwrap()
        .checkpoints
        .into_iter()
        .flat_map(|checkpoint| checkpoint.episodes)
        .find(|episode| {
            matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
        })
        .unwrap()
}
#[tokio::test]
async fn adverse_completed_current_work_runs_one_bounded_shared_profile_and_preserves_terminal_answer_and_sibling() {
    let mut runtime = prepared(true).await;
    let before = episode(&runtime).await;
    let q = SourceReadQuestion::CurrentExecutionProfile;
    assert!(before.source_reads.get(q).unwrap().needs_work());
    assert!(runtime.enroll().await);
    let job = runtime.take_focus().await;
    let intent = runtime
        .fixture
        .journal
        .focused_intents()
        .await
        .unwrap()
        .into_iter()
        .find(|intent| intent.key().ok().as_deref() == Some(job.key.as_str()))
        .unwrap();
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, q);
    let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
    let (source, records) = current_execution_profile_result(focus, execution, ExecutionProfileCase::Observed);
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let calls = runtime.native_calls();
    let policies = runtime.policies();
    assert_eq!(
        analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
            .await
            .unwrap(),
        1
    );
    let answered = episode(&runtime).await;
    assert_eq!(answered.first, before.first);
    assert_eq!(answered.report, before.report);
    let SourceReadStageState::Answered { answer } = &answered.source_reads.get(q).unwrap().state else { panic!() };
    let SourceReadAnswer::CurrentExecutionProfile(answer) = answer.as_ref() else { panic!() };
    assert_eq!(answer.result, capture);
    assert_eq!(answer.evidence.profile_rows, 2);
    assert_eq!(runtime.native_calls(), calls);
    assert!(
        answered.source_reads.get(SourceReadQuestion::RelationalInputContext).is_some(),
        "terminal/unsupported sibling questions do not suppress this source"
    );
    runtime.reconcile().await;
    assert!(!runtime.enroll().await);
    let SourceObservation::Activity(frame) = &mut runtime.fixture.backend.activity.source.observation else { panic!() };
    frame.at = "2020-01-01T12:00:05Z".into();
    runtime.complete_empty_baseline().await;
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    let recovered =
        crate::fleet::investigations::tests::source_reads::temporary::latest_history_episode(&runtime, &answered.id)
            .await;
    assert_eq!(recovered.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(recovered.first, answered.first);
    assert_eq!(recovered.source_reads.get(q), answered.source_reads.get(q));
    assert!(!runtime.enroll().await);
}
#[tokio::test]
async fn unqualified_profile_remains_visible_without_native_dispatch() {
    let mut runtime = prepared(false).await;
    let q = SourceReadQuestion::CurrentExecutionProfile;
    assert!(matches!(
        episode(&runtime).await.source_reads.get(q).unwrap().state,
        SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }
    ));
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}
#[tokio::test]
async fn source_failure_or_cancellation_retains_original_without_successful_profile_answer() {
    for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::SourceChanged] {
        let mut runtime = prepared(true).await;
        let before = episode(&runtime).await;
        runtime.fixture.backend.focus_failure = Some(stop);
        assert!(runtime.enroll().await);
        let job = runtime.take_focus().await;
        let result = runtime.execute(&job).await;
        assert_ne!(result.event.status, CollectionStatus::Complete);
        runtime.finish(result.outcome).await;
        runtime.reconcile().await;
        let retained =
            crate::fleet::investigations::tests::source_reads::temporary::latest_history_episode(&runtime, &before.id)
                .await;
        assert_eq!(retained.first, before.first);
        assert!(!matches!(
            retained.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap().state,
            SourceReadStageState::Answered { .. }
        ));
        let status = runtime.fixture.outbox.status(100, None).await.unwrap();
        assert!(!status.entries.iter().any(|entry| entry.request.collection()
            == TelemetryCollection::Snapshot(SnapshotDataset::CurrentExecutionProfile)
            && entry.state == OutboxCaptureState::Ready));
    }
}

#[tokio::test]
async fn a_recent_fast_original_with_parallel_wait_context_creates_no_question_or_native_followup() {
    let (source, mut records) = current_execution_profile_basis();
    let TelemetryRecord::Activity(actor) = &mut records[0] else { panic!() };
    actor.elapsed_ms = Some("100".into());
    actor.state = "running".into();
    actor.idle = false;
    actor.waiting_on_lock = Some(false);
    actor.open_transactions = Some(0);
    actor.transaction_age_ms = None;
    actor.wait.as_mut().unwrap().elapsed_ms = Some("1".into());
    let mut runtime = Runtime::from_fixture(fixture_for(Frame { source, records }).await).await;
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}
