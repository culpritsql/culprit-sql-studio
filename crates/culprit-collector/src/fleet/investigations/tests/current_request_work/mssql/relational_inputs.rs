//! Real enrollment/Fleet/Outbox/CAS over controlled originals; these fixtures qualify no production cost.
use super::*;
use crate::fleet::scheduler::FocusStatus;
use culprit_evidence::analytical::test_support::{TvpLoopCase, relational_input_question_fixture};

async fn prepared(qualified: bool) -> Runtime {
    prepared_with_fence(qualified, None).await
}
async fn prepared_with_fence(qualified: bool, fence: Option<&culprit_evidence::delivery::mutations::Fence>) -> Runtime {
    let original = relational_input_question_fixture(TvpLoopCase::RepeatedAccess);
    let activity = Frame { source: original.activity_source, records: vec![original.activity_row.record] };
    let mut harness = match fence {
        None => fixture_for(activity).await,
        Some(fence) => {
            let mut fixture = Harness::with_archived_activity(activity, fence).await;
            fixture.backend.recipe.id = "synthetic.current-request-work".into();
            fixture.backend.recipe.selector = RecipeSelector::CurrentRequestWork;
            let RecipeAdmission::Measured(receipt) = &mut fixture.backend.recipe.admission else { panic!() };
            receipt.recipe_id = fixture.backend.recipe.id.clone();
            receipt.selector = RecipeSelector::CurrentRequestWork;
            fixture
        }
    };
    let mut retained = harness.backend.recipe.clone();
    retained.id = "controlled.selected-retained-input".into();
    retained.selector = SourceReadQuestion::RelationalInputContext.selector();
    let RecipeAdmission::Measured(receipt) = &mut retained.admission else { panic!() };
    receipt.recipe_id = retained.id.clone();
    receipt.selector = retained.selector;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(
            SourceReadQuestion::RelationalInputContext,
            &NativeReadContext::from_activity(&harness.backend.activity.source).unwrap(),
        )
        .unwrap(),
    );
    if !qualified {
        retained.admission = RecipeAdmission::Unqualified;
    }
    harness.backend.additional_recipes.push(retained);
    let mut runtime = Runtime::from_fixture(harness).await;
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    let participant = intent.focus.activity_participant().unwrap();
    let mut source = original.work_source;
    let SourceObservation::CurrentRequestWork { result, .. } = &mut source.observation else { panic!() };
    result.focus = Some(Box::new(participant.clone()));
    runtime.fixture.backend.current_work = Some(Frame { source, records: vec![original.work_row.record] });
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    assert_eq!(
        runtime.fixture.outbox.diagnostic_input_state(&capture, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
        DiagnosticInputState::Consumed
    );
    assert!(matches!(
        runtime.scheduler.focused_state(&job.key).unwrap().status,
        FocusStatus::Completed { result } if result == capture
    ));
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await, "the prior completed owner still occupies this episode until reconciliation");
    // Match FleetRunner: consumed offline analysis is followed by awaited owner retirement before new planning.
    runtime.reconcile().await;
    assert!(runtime.scheduler.focused_state(&job.key).is_none());
    assert!(runtime.fixture.journal.focused_intent(&job.key).await.unwrap().is_none());
    assert!(!runtime.fixture.outbox.investigation_parent_keys().await.unwrap().contains(&job.key));
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
async fn capture_retained_answer(runtime: &mut Runtime) -> (DiagnosticCaptureRef, [usize; 5]) {
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
    let original = relational_input_question_fixture(TvpLoopCase::RepeatedAccess);
    let mut source = original.source;
    let SourceObservation::SourceRead { focus_key, .. } = &mut source.observation else { panic!() };
    *focus_key = focus.key().unwrap();
    let mut records = original.records;
    let TelemetryRecord::ObjectDefinition(definition) = &mut records[0] else { panic!() };
    let ObjectDefinitionFact::MssqlRetainedPlan(retained) = &mut definition.fact else { panic!() };
    if let Some(proof) = retained.relational_input_proof.take() {
        retained.relational_input_proof =
            Some(MssqlRelationalInputProof::for_retained(&source, retained, proof.bindings).unwrap());
    }
    *definition = ObjectDefinitionRecord::new(definition.source.clone(), definition.fact.clone()).unwrap();
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let calls = runtime.native_calls();
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    assert_eq!(runtime.native_calls(), calls, "offline completion adds no source work");
    (capture, calls)
}

#[tokio::test]
async fn adverse_current_work_automatically_owes_one_selected_retained_question_and_pins_answer_across_recovery() {
    let mut runtime = prepared(true).await;
    let before = episode(&runtime).await;
    let stage = before.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap();
    assert_eq!(stage.focus.question, SourceReadQuestion::RelationalInputContext);
    let selected_basis = stage.focus.basis.as_ref().clone();
    let SourceReadPrerequisite::ObservedCurrentRequestPlan { observation, .. } = &stage.focus.prerequisite else {
        panic!()
    };
    {
        let input = runtime
            .fixture
            .outbox
            .diagnostic_input(&observation.capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION)
            .await
            .unwrap();
        let SourceObservation::CurrentRequestWork { result, .. } = &input.source.observation else { panic!() };
        assert_eq!(result.focus.as_ref().unwrap().parent, selected_basis);
    }
    assert_eq!(selected_basis, before.latest);
    assert_ne!(selected_basis, before.first);
    let (capture, calls) = capture_retained_answer(&mut runtime).await;
    let answered = episode(&runtime).await;
    assert_eq!(answered.first, before.first);
    assert_eq!(answered.report, before.report);
    let stage = answered.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap();
    let SourceReadStageState::Answered { answer } = &stage.state else { panic!() };
    let SourceReadAnswer::RelationalInputs(answer) = answer.as_ref() else { panic!() };
    assert_eq!(answer.result, capture);
    assert_eq!(answer.basis, selected_basis);
    assert_ne!(answer.basis, answered.first);
    assert_eq!(answer.unresolved, RelationalInputGap::ALL);
    assert!(matches!(answer.selection, RelationalInputSelection::Observed { input_proof: Some(_), .. }));
    assert_eq!(runtime.native_calls(), calls);
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
    assert_eq!(recovered.first, before.first);
    assert_eq!(
        recovered.source_reads.get(SourceReadQuestion::RelationalInputContext),
        answered.source_reads.get(SourceReadQuestion::RelationalInputContext),
        "the historical retained answer remains immutable",
    );
    let profile = answered.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap();
    assert_eq!(profile.state, SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::Unsupported });
    let recovered_profile = recovered.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap();
    assert_eq!(recovered_profile.focus, profile.focus);
    assert_eq!(recovered_profile.state, SourceReadStageState::Recovered);
    assert!(recovered.recovery.is_some(), "only the genuine complete successor retires unadmitted live work");
    assert!(!runtime.enroll().await);
}
#[tokio::test]
async fn unqualified_retained_recipe_keeps_question_visible_without_native_dispatch_or_type_only_enrollment() {
    let mut runtime = prepared(false).await;
    let episode = episode(&runtime).await;
    assert!(matches!(
        episode.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().state,
        SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }
    ));
    let calls = runtime.native_calls();
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}

mod reference_inventory;
