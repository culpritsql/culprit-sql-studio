//! Controlled native facts exercise the real queue and admission owner, never source-cost qualification.
use super::*;
use culprit_evidence::analytical::test_support::{OutputGrantCase, output_grant_original, output_grant_result};

async fn fixture(qualified: bool) -> Harness {
    let original = output_grant_original();
    let mut fixture =
        Harness::with_activity(Frame { source: original.source, records: vec![original.original.record] }).await;
    fixture.backend.recipe.id = "fixture.output-grant-retention".into();
    fixture.backend.recipe.selector = SourceReadQuestion::OutputGrantRetention.selector();
    let RecipeAdmission::Measured(receipt) = &mut fixture.backend.recipe.admission else { panic!() };
    receipt.recipe_id = fixture.backend.recipe.id.clone();
    receipt.selector = fixture.backend.recipe.selector;
    receipt.tested_budget.max_snapshot_rows = 64;
    receipt.tested_budget.page_rows = 64;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(
            SourceReadQuestion::OutputGrantRetention,
            &NativeReadContext::from_activity(&fixture.backend.activity.source).unwrap(),
        )
        .unwrap(),
    );
    if !qualified {
        fixture.backend.recipe.admission = RecipeAdmission::Unqualified;
    }
    fixture
}
async fn episode(runtime: &mut Runtime) -> DiagnosticEpisode {
    runtime
        .fixture
        .outbox
        .diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 8)
        .await
        .unwrap()
        .checkpoints
        .into_iter()
        .flat_map(|checkpoint| checkpoint.episodes)
        .find(|episode| matches!(episode.signal, DiagnosticSignal::ClientResultOutput { .. }))
        .unwrap()
}
#[tokio::test]
async fn an_unqualified_output_question_keeps_the_original_without_opening_another_native_source() {
    let mut runtime = Runtime::from_fixture(fixture(false).await).await;
    let before = episode(&mut runtime).await;
    assert!(matches!(
        before.source_reads.get(SourceReadQuestion::OutputGrantRetention).unwrap().state,
        SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }
    ));
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
    assert!(runtime.fixture.journal.focused_intents().await.unwrap().is_empty());
    assert_eq!(episode(&mut runtime).await.first, before.first);
}
#[tokio::test]
async fn one_output_question_uses_the_original_request_and_keeps_its_terminal_answer_without_reopening() {
    let mut runtime = Runtime::from_fixture(fixture(true).await).await;
    let before = episode(&mut runtime).await;
    let focus = before.source_reads.get(SourceReadQuestion::OutputGrantRetention).unwrap().focus.clone();
    let execution = SourceReadExecutionScope::from_context(
        focus.question,
        &focus.original_context(&runtime.fixture.backend.activity.source).unwrap(),
    )
    .unwrap();
    let (source, records) = output_grant_result(&focus, &execution, OutputGrantCase::Held);
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    assert!(runtime.enroll().await);
    let intent = runtime.fixture.journal.focused_intents().await.unwrap().remove(0);
    let DiagnosticFocus::SourceRead { focus: admitted } = &intent.focus else { panic!() };
    assert_eq!(admitted.as_ref(), &focus);
    assert_eq!(intent.focus.parent(), &before.first);
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    assert_eq!(result.event.status, CollectionStatus::Complete);
    let Outcome::FocusedComplete { result: capture } = result.outcome else { panic!() };
    runtime.finish(Outcome::FocusedComplete { result: capture.clone() }).await;
    let policies = runtime.policies();
    assert_eq!(
        analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
            .await
            .unwrap(),
        1
    );
    let answered = episode(&mut runtime).await;
    assert_eq!(answered.first, before.first);
    let SourceReadStageState::Answered { answer } = &answered.source_reads.get(focus.question).unwrap().state else {
        panic!()
    };
    let SourceReadAnswer::OutputGrantRetention(answer) = answer.as_ref() else { panic!() };
    assert_eq!(answer.result, capture);
    assert!(answer.subject.is_some());
    runtime.reconcile().await;
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
    assert_eq!(calls, [2; 5]);
}
