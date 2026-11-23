//! Real planner resume over authenticated two-question outcomes. Cursor data never creates admission.
use super::*;
use crate::fleet::{
    investigations::routes::{PlanningPosition, PlanningQuestionCursor},
    storage::CaptureStorage,
};

async fn remember_after(runtime: &Runtime, before: &DiagnosticEpisode, selector: RecipeSelector) -> PlanningPosition {
    let page = runtime.fixture.outbox.diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 1).await.unwrap();
    let checkpoint = page.checkpoints.first().unwrap();
    let index = checkpoint.episodes.iter().position(|episode| episode.id == before.id).unwrap();
    let position = PlanningPosition {
        checkpoint: None,
        episode: index,
        target: 0,
        question: Some(PlanningQuestionCursor::after(before, selector)),
    };
    runtime.investigations.bindings.set_planning_position(position.clone());
    position
}
async fn prepare_with(runtime: &mut Runtime, cancellation: &CollectionCancellation) -> bool {
    runtime
        .investigations
        .prepare(
            &runtime.fixture.backend,
            &runtime.fixture.journal,
            &mut CaptureStorage::local(&mut runtime.fixture.outbox),
            &mut runtime.scheduler,
            runtime.started,
            cancellation,
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn a_completed_profile_shrinks_the_roster_and_cancelled_resume_cannot_skip_the_remaining_retained_question() {
    let mut runtime = prepared(ProfileRoute::Admitted, None).await;
    let before = episode(&runtime).await;
    assert_eq!(before.source_read_admission_order().len(), 2);
    let (_, outcome, _) = capture_profile(&mut runtime, ExecutionProfileCase::NoPlanContext, false).await;
    // This is the exact fairness position an interrupted pass can retain after inspecting the live question;
    // the genuine admitted source above, not the cursor, supplies the captured result and original scope.
    let position = remember_after(&runtime, &before, SourceReadQuestion::CurrentExecutionProfile.selector()).await;
    runtime.finish(outcome).await;
    analyze(&mut runtime).await;
    runtime.reconcile().await;
    let answered = episode(&runtime).await;
    assert_ne!(answered.revision, before.revision);
    let profile = answered.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap().clone();
    assert!(matches!(profile.state, SourceReadStageState::Answered { .. }));
    assert_eq!(answered.source_read_admission_order().len(), 1);
    let calls = runtime.native_calls();
    let cancelled = CollectionCancellation::default();
    cancelled.cancel();
    assert!(!prepare_with(&mut runtime, &cancelled).await);
    assert_eq!(runtime.investigations.bindings.planning_position(), position);
    assert_eq!(runtime.native_calls(), calls);
    assert!(
        runtime.enroll().await,
        "the remaining applicable question resumes in this pass, not another checkpoint traversal"
    );
    let intents = runtime.fixture.journal.focused_intents().await.unwrap();
    let [intent] = intents.as_slice() else { panic!() };
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::RelationalInputContext);
    assert_eq!(intent.episode_id, answered.id);
    assert_eq!(episode(&runtime).await.source_reads.get(SourceReadQuestion::CurrentExecutionProfile), Some(&profile));
    assert_eq!(runtime.native_calls(), calls, "planning and cancellation open no native source");
}
#[tokio::test]
async fn a_terminal_retained_sibling_cannot_hide_a_newly_admissible_live_question_at_its_old_numeric_slot() {
    let mut runtime = prepared(ProfileRoute::Unqualified, None).await;
    let before = episode(&runtime).await;
    let answered = finish_retained(&mut runtime).await;
    let terminal = answered.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().clone();
    remember_after(&runtime, &before, SourceReadQuestion::RelationalInputContext.selector()).await;
    let measured =
        followup_recipe(&runtime.fixture, SourceReadQuestion::CurrentExecutionProfile, "controlled.live-profile");
    let existing =
        runtime.fixture.backend.additional_recipes.iter_mut().find(|recipe| recipe.id == measured.id).unwrap();
    *existing = measured;
    let calls = runtime.native_calls();
    assert!(
        runtime.enroll().await,
        "the live question at current position zero must not be skipped after a sibling terminates"
    );
    let intents = runtime.fixture.journal.focused_intents().await.unwrap();
    let [intent] = intents.as_slice() else { panic!() };
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::CurrentExecutionProfile);
    assert_eq!(episode(&runtime).await.source_reads.get(SourceReadQuestion::RelationalInputContext), Some(&terminal));
    assert_eq!(runtime.native_calls(), calls);
}
#[tokio::test]
async fn an_unchanged_revision_resumes_after_the_exact_refused_selector_without_repeating_it_or_opening_a_source() {
    let mut runtime = prepared(ProfileRoute::Unqualified, None).await;
    let before = episode(&runtime).await;
    remember_after(&runtime, &before, SourceReadQuestion::CurrentExecutionProfile.selector()).await;
    let calls = runtime.native_calls();
    assert!(runtime.enroll().await);
    let intents = runtime.fixture.journal.focused_intents().await.unwrap();
    let [intent] = intents.as_slice() else { panic!() };
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::RelationalInputContext);
    assert_eq!(intent.episode_id, before.id);
    assert_eq!(runtime.native_calls(), calls);
}
