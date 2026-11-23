//! The real planner/scheduler/Outbox operate on canonically sealed controlled originals; no production cost is qualified.
use super::*;
use culprit_evidence::analytical::test_support::{
    ReadableSecondaryCase as Case, readable_secondary_original, readable_secondary_result,
};

async fn runtime(child_qualified: bool) -> Runtime {
    let original = readable_secondary_original(Engine::Postgres);
    let mut harness = fixture_for(Frame { source: original.source, records: vec![original.row.record] }).await;
    let mut child = harness.backend.recipe.clone();
    child.id = "controlled.readable-secondary-recovery".into();
    child.selector = SourceReadQuestion::ReadableSecondaryRecovery.selector();
    child.budget.max_queries = 9;
    if child_qualified {
        child.admission = RecipeAdmission::BuiltinSafety(Box::new(BuiltinRecipeSafety {
            id: "controlled.recovery-source-policy".into(),
            version: 1,
            recipe_id: child.id.clone(),
            recipe_version: child.version,
            purpose: child.purpose,
            selector: child.selector,
            component: harness.backend.component.clone(),
            templates_sha256: digest(b"controlled-recovery-reader"),
            budget: child.budget.clone(),
        }));
        let mut primary = std::mem::replace(&mut harness.backend.recipe, child);
        primary.admission = RecipeAdmission::Unqualified;
        harness.backend.additional_recipes.push(primary);
    } else {
        child.admission = RecipeAdmission::Unqualified;
        harness.backend.additional_recipes.push(child);
    }
    Runtime::from_fixture(harness).await
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
async fn unqualified_readonly_context_keeps_its_own_stage_without_suppressing_an_eligible_primary_or_dispatching_it() {
    let mut runtime = runtime(false).await;
    let before = episode(&runtime).await;
    let stage = before.source_reads.get(SourceReadQuestion::ReadableSecondaryRecovery).unwrap();
    assert_eq!(stage.state, SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified });
    assert!(before.report.is_none());
    let calls = runtime.native_calls();
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    assert_eq!(intent.focus.selector(), RecipeSelector::CurrentRequestWork);
    assert_eq!(runtime.native_calls(), calls, "preparation/admission never performs source work");
    assert_eq!(episode(&runtime).await.source_reads, before.source_reads);
}
#[tokio::test]
async fn builtin_source_question_runs_through_the_existing_owner_and_retains_answer_after_recovery_without_measured_cost()
 {
    let mut runtime = runtime(true).await;
    let before = episode(&runtime).await;
    assert!(before.report.is_none());
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::ReadableSecondaryRecovery);
    assert!(intent.qualification.measured().is_none());
    let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
    assert!(matches!(execution.as_ref(), SourceReadExecutionScope::Local(_)));
    assert!(execution.target().database().unwrap().incarnation.is_none());
    let (source, records) = readable_secondary_result(focus, execution, Case::PostgresRelation);
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let calls = runtime.native_calls();
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    assert_eq!(runtime.native_calls(), calls);
    let answered = episode(&runtime).await;
    let question = SourceReadQuestion::ReadableSecondaryRecovery;
    let SourceReadStageState::Answered { answer } = &answered.source_reads.get(question).unwrap().state else {
        panic!()
    };
    assert_eq!(answer.result(), &capture);
    assert_eq!(answered.first, before.first);
    assert_eq!(answered.report, before.report);
    assert_eq!(answered.analysis, before.analysis, "the child cannot settle the separate unperformed primary");
    runtime.reconcile().await;
    assert!(!runtime.enroll().await);
    let SourceObservation::Activity(frame) = &mut runtime.fixture.backend.activity.source.observation else { panic!() };
    frame.at = "2020-01-01T12:00:07Z".into();
    runtime.complete_empty_baseline().await;
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    let recovered =
        crate::fleet::investigations::tests::source_reads::temporary::latest_history_episode(&runtime, &answered.id)
            .await;
    assert_eq!(recovered.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(recovered.first, before.first);
    assert_eq!(recovered.source_reads.get(question), answered.source_reads.get(question));
    assert!(!runtime.enroll().await);
}
