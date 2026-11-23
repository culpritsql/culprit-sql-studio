//! The actual controller, scheduler, canonical Outbox and verifier operate over controlled originals.
//! This proves lifecycle ownership, not a native metadata wait or production cost qualification.
use super::*;
use culprit_evidence::analytical::test_support::{
    TransactionMetadataContinuationCase as Case, TransactionMetadataObservation as Observation,
    transaction_metadata_continuation_result, transaction_metadata_original, transaction_metadata_original_at,
};
use std::sync::atomic::Ordering;

async fn fixture() -> Harness {
    let original = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
    let mut fixture =
        Harness::with_activity(Frame { source: original.source, records: vec![original.original.record] }).await;
    let context = NativeReadContext::from_activity(&fixture.backend.activity.source).unwrap();
    let recipe = &mut fixture.backend.recipe;
    recipe.id = "controlled.transaction-metadata".into();
    recipe.version = 2;
    recipe.selector = SourceReadQuestion::TransactionMetadata.selector();
    recipe.budget.max_queries = 12;
    recipe.budget.max_snapshot_rows = 32;
    recipe.budget.page_rows = 32;
    recipe.budget.total_bytes = 1024 * 1024;
    recipe.budget.page_bytes = recipe.budget.page_bytes.min(recipe.budget.total_bytes);
    recipe.budget.statement_timeout_ms = 1000;
    recipe.budget.total_timeout_ms = 5000;
    let RecipeAdmission::Measured(receipt) = &mut recipe.admission else { panic!() };
    receipt.recipe_id = recipe.id.clone();
    receipt.recipe_version = recipe.version;
    receipt.selector = recipe.selector;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(SourceReadQuestion::TransactionMetadata, &context).unwrap(),
    );
    receipt.tested_budget = recipe.budget.clone();
    fixture
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
        .find(|episode| matches!(episode.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
        .unwrap()
}
async fn consume(runtime: &mut Runtime) {
    let calls = runtime.native_calls();
    let policies = runtime.policies();
    assert_eq!(
        analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
            .await
            .unwrap(),
        1
    );
    runtime.reconcile().await;
    runtime.reconcile().await;
    assert_eq!(runtime.native_calls(), calls, "offline completion and settlement cannot open a native source");
}
async fn admitted(
    runtime: &mut Runtime,
    observation: Observation,
    case: Case,
) -> (crate::fleet::scheduler::FocusedJob, crate::fleet::work::WorkResult) {
    assert!(runtime.enroll().await);
    let intents = runtime.fixture.journal.focused_intents().await.unwrap();
    let [intent] = intents.as_slice() else { panic!() };
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::TransactionMetadata);
    let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
    assert_eq!(intent.budget.max_queries, 12);
    let (source, records) = transaction_metadata_continuation_result(focus, execution, case, observation);
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    assert_eq!(result.event.status, CollectionStatus::Complete);
    assert!(matches!(result.outcome, Outcome::FocusedComplete { .. }));
    (job, result)
}
async fn initial(runtime: &mut Runtime) -> DiagnosticEpisode {
    let (_, result) = admitted(runtime, Observation::Initial, Case::Continued).await;
    runtime.finish(result.outcome).await;
    consume(runtime).await;
    episode(runtime).await
}
async fn fresh_baseline(runtime: &mut Runtime, at: &str) {
    let original = transaction_metadata_original_at(TransactionMetadataWait::Subtransaction, at, at);
    runtime.fixture.backend.activity = Frame { source: original.source, records: vec![original.original.record] };
    runtime.started -= runtime.scheduler.wait_allowed(runtime.started.elapsed(), |_| false);
    runtime.baseline().await;
}
#[tokio::test]
async fn one_post_cooldown_observation_survives_ready_restart_and_never_replays_or_replaces_first_answer() {
    let mut runtime = Runtime::from_fixture(fixture().await).await;
    let first = initial(&mut runtime).await;
    let first_stage = first.source_reads.get(SourceReadQuestion::TransactionMetadata).unwrap().clone();
    fresh_baseline(&mut runtime, "2020-01-01T12:00:30Z").await;
    assert_eq!(
        episode(&runtime).await.source_reads.len(),
        1,
        "a genuinely new adverse original before cooldown is insufficient"
    );
    assert!(!runtime.enroll().await);
    fresh_baseline(&mut runtime, "2020-01-01T12:01:10Z").await;
    let nominated = episode(&runtime).await;
    assert_eq!(nominated.id, first.id);
    assert_eq!(nominated.first, first.first);
    assert_eq!(nominated.source_reads.get(SourceReadQuestion::TransactionMetadata), Some(&first_stage));
    assert_eq!(nominated.source_reads.len(), MAX_TRANSACTION_METADATA_OBSERVATIONS);
    let second_focus = nominated.source_reads.latest(SourceReadQuestion::TransactionMetadata).unwrap().focus.clone();
    assert_ne!(*second_focus.basis, first.first);
    let (job, result) = admitted(&mut runtime, Observation::Continued, Case::Continued).await;
    assert_ne!(job.episode_stage_key, first_stage.key(&first.id).unwrap());
    let Outcome::FocusedComplete { result: capture } = result.outcome else { panic!() };
    let calls = runtime.native_calls();
    let mut runtime = super::recovery::restart(runtime, 0).await;
    runtime.reconcile().await;
    assert!(runtime.scheduler.focused_state(&job.key).unwrap().source_work == SourceWorkState::Unconfirmed);
    consume(&mut runtime).await;
    let answered = episode(&runtime).await;
    assert_eq!(answered.source_reads.get(SourceReadQuestion::TransactionMetadata), Some(&first_stage));
    assert_eq!(answered.source_reads.get_focus(&second_focus).unwrap().result(), Some(&capture));
    assert_eq!(runtime.native_calls(), calls);
    assert!(matches!(
        runtime.fixture.journal.focused_stage(&job.episode_stage_key).await.unwrap(),
        Some(FocusedStage::Completed)
    ));
    let revisions = runtime.revisions(&capture.capture).await;
    assert!(revisions.iter().all(
        |revision| revision.episode.source_reads.get(SourceReadQuestion::TransactionMetadata) == Some(&first_stage)
    ));
    fresh_baseline(&mut runtime, "2020-01-01T12:02:30Z").await;
    let before = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), before);
    assert_eq!(episode(&runtime).await.source_reads.len(), MAX_TRANSACTION_METADATA_OBSERVATIONS);
}
#[tokio::test]
async fn recovery_retires_only_the_unstarted_second_question_and_retains_initial_immutable_history() {
    let mut runtime = Runtime::from_fixture(fixture().await).await;
    let first = initial(&mut runtime).await;
    fresh_baseline(&mut runtime, "2020-01-01T12:01:10Z").await;
    let nominated = episode(&runtime).await;
    let second = nominated.source_reads.latest(SourceReadQuestion::TransactionMetadata).unwrap().focus.clone();
    assert!(nominated.source_reads.get_focus(&second).unwrap().needs_admission());
    let SourceObservation::Activity(frame) = &mut runtime.fixture.backend.activity.source.observation else { panic!() };
    frame.at = "2020-01-01T12:02:00Z".into();
    frame.observed_from = frame.at.clone();
    frame.observed_until = frame.at.clone();
    runtime.complete_empty_baseline().await;
    let recovered = super::temporary::latest_history_episode(&runtime, &first.id).await;
    assert_eq!(recovered.lifecycle, DiagnosticLifecycle::Resolved);
    assert!(recovered.recovery.is_some());
    assert_eq!(recovered.first, first.first);
    assert_eq!(
        recovered.source_reads.get(SourceReadQuestion::TransactionMetadata),
        first.source_reads.get(SourceReadQuestion::TransactionMetadata)
    );
    assert_eq!(recovered.source_reads.get_focus(&second).unwrap().state, SourceReadStageState::Recovered);
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
    assert_eq!(runtime.fixture.outbox.status(20, None).await.unwrap().source_debts, 0);
}
#[tokio::test]
async fn unqualified_or_refused_selected_profile_cannot_spend_continuation_budget_or_erase_first_evidence() {
    let mut runtime = Runtime::from_fixture(fixture().await).await;
    let first = initial(&mut runtime).await;
    let qualified = runtime.fixture.backend.recipe.admission.clone();
    runtime.fixture.backend.recipe.admission = RecipeAdmission::Unqualified;
    fresh_baseline(&mut runtime, "2020-01-01T12:01:10Z").await;
    let continued = episode(&runtime).await;
    assert_eq!(
        continued.source_reads.latest(SourceReadQuestion::TransactionMetadata).unwrap().state,
        SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }
    );
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
    runtime.fixture.backend.recipe.admission = qualified;
    runtime.fixture.backend.accept = false;
    fresh_baseline(&mut runtime, "2020-01-01T12:02:20Z").await;
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
    assert!(runtime.fixture.backend.verified.load(Ordering::SeqCst) > 0);
    assert_eq!(
        episode(&runtime).await.source_reads.get(SourceReadQuestion::TransactionMetadata),
        first.source_reads.get(SourceReadQuestion::TransactionMetadata)
    );
}
