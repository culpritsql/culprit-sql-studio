//! The real Fleet/Outbox/CAS owners over controlled canonical originals; never production qualification.
use super::*;
use crate::fleet::scheduler::FocusedJob;
use culprit_evidence::analytical::test_support::{
    ExecutionProfileCase, current_execution_profile_basis, current_execution_profile_result,
    current_execution_profile_result_with_input_context, current_execution_profile_work,
};
use culprit_evidence::delivery::mutations::Fence;

#[derive(Clone, Copy)]
enum ProfileRoute {
    Admitted,
    Unqualified,
    Absent,
    WrongComponent,
}
fn followup_recipe(harness: &Harness, question: SourceReadQuestion, id: &str) -> RecipeDescriptor {
    let mut recipe = harness.backend.recipe.clone();
    recipe.id = id.into();
    recipe.selector = question.selector();
    let RecipeAdmission::Measured(receipt) = &mut recipe.admission else { panic!() };
    receipt.recipe_id = recipe.id.clone();
    receipt.selector = recipe.selector;
    receipt.scope = RecipeCostScope::NativeRead(
        SourceReadExecutionScope::from_context(
            question,
            &NativeReadContext::from_activity(&harness.backend.activity.source).unwrap(),
        )
        .unwrap(),
    );
    assert!(recipe.is_valid());
    recipe
}
async fn prepared(route: ProfileRoute, fence: Option<&Fence>) -> Runtime {
    let (source, records) = current_execution_profile_basis();
    let mut harness = match fence {
        Some(fence) => {
            let mut harness = Harness::with_archived_activity(Frame { source, records }, fence).await;
            harness.backend.recipe.id = "synthetic.current-request-work".into();
            harness.backend.recipe.selector = RecipeSelector::CurrentRequestWork;
            let RecipeAdmission::Measured(receipt) = &mut harness.backend.recipe.admission else { panic!() };
            receipt.recipe_id = harness.backend.recipe.id.clone();
            receipt.selector = RecipeSelector::CurrentRequestWork;
            harness
        }
        None => fixture_for(Frame { source, records }).await,
    };
    // Adverse original/current-work is unchanged. Both questions share its exact authenticated native basis.
    let retained = followup_recipe(&harness, SourceReadQuestion::RelationalInputContext, "controlled.retained-inputs");
    harness.backend.additional_recipes.push(retained);
    if !matches!(route, ProfileRoute::Absent) {
        let mut profile =
            followup_recipe(&harness, SourceReadQuestion::CurrentExecutionProfile, "controlled.live-profile");
        match route {
            ProfileRoute::Unqualified => profile.admission = RecipeAdmission::Unqualified,
            ProfileRoute::WrongComponent => {
                let RecipeAdmission::Measured(receipt) = &mut profile.admission else { panic!() };
                receipt.component.version = Some("controlled-other-component-version".into());
            }
            _ => (),
        }
        harness.backend.additional_recipes.push(profile);
    }
    let mut runtime = Runtime::from_fixture(harness).await;
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    let (source, records) = current_execution_profile_work(intent.focus.activity_participant().unwrap());
    runtime.fixture.backend.current_work = Some(Frame { source, records });
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    runtime.finish(result.outcome).await;
    analyze(&mut runtime).await;
    let calls = runtime.native_calls();
    runtime.reconcile().await;
    assert!(runtime.scheduler.focused_state(&job.key).is_none());
    assert!(runtime.fixture.journal.focused_intent(&job.key).await.unwrap().is_none());
    assert!(!runtime.fixture.outbox.investigation_parent_keys().await.unwrap().contains(&job.key));
    assert_eq!(runtime.native_calls(), calls);
    runtime
}
async fn analyze(runtime: &mut Runtime) {
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
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
        .flat_map(|part| part.episodes)
        .find(|episode| {
            matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
        })
        .unwrap()
}
async fn next_intent(runtime: &Runtime, job: &FocusedJob) -> DiagnosticInvestigationIntent {
    runtime.fixture.journal.focused_intent(&job.key).await.unwrap().unwrap()
}
async fn capture_profile(
    runtime: &mut Runtime,
    case: ExecutionProfileCase,
    include_context: bool,
) -> (FocusedJob, Outcome, DiagnosticCaptureRef) {
    assert!(runtime.enroll().await);
    let job = runtime.take_focus().await;
    let intent = next_intent(runtime, &job).await;
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::CurrentExecutionProfile);
    let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
    let (source, records) = if include_context {
        current_execution_profile_result_with_input_context(focus, execution, case)
    } else {
        current_execution_profile_result(focus, execution, case)
    };
    runtime.fixture.backend.source_read_result = Some(Frame { source, records });
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    assert_eq!(result.event.status, CollectionStatus::Complete);
    let manifest = runtime.fixture.outbox.verify(&capture.capture.observation_id).await.unwrap();
    assert_eq!(manifest.investigation_intent().unwrap(), Some(intent));
    (job, result.outcome, capture)
}
fn retained_empty(focus: &SourceReadFocus, execution: &SourceReadExecutionScope) -> Frame {
    let source = culprit_core::source_id::RETAINED_PLANS;
    Frame {
        source: CollectionDescription {
            source: "retained-plans-state".into(),
            target: focus.target.clone(),
            partition: SourcePartition::Target,
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::SourceRead {
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::RetainedPlans,
                    observed_from: "2020-01-01T12:00:05Z".into(),
                    observed_until: "2020-01-01T12:00:06Z".into(),
                    records: 0,
                    sources: vec![StateSourceCoverage::observed(source.into(), 0, false)],
                },
                focus_key: focus.key().unwrap(),
                execution: Box::new(NativeReadContext {
                    target: focus.target.clone(),
                    partition: execution.partition().clone(),
                    observed_from: "2020-01-01T12:00:05Z".into(),
                    observed_until: "2020-01-01T12:00:06Z".into(),
                    database_writable: execution.database_writable(),
                }),
            },
        },
        records: vec![],
    }
}
async fn finish_retained(runtime: &mut Runtime) -> DiagnosticEpisode {
    assert!(runtime.enroll().await);
    let job = runtime.take_focus().await;
    let intent = next_intent(runtime, &job).await;
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, SourceReadQuestion::RelationalInputContext);
    let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
    runtime.fixture.backend.source_read_result = Some(retained_empty(focus, execution));
    let result = runtime.execute(&job).await;
    runtime.finish(result.outcome).await;
    analyze(runtime).await;
    runtime.reconcile().await;
    let episode = episode(runtime).await;
    let SourceReadStageState::Answered { answer } =
        &episode.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().state
    else {
        panic!()
    };
    let SourceReadAnswer::RelationalInputs(answer) = answer.as_ref() else { panic!() };
    assert_eq!(answer.selection, RelationalInputSelection::NotObserved);
    assert_eq!(answer.source_state, culprit_core::SourceState::Partial);
    episode
}
fn assert_one_capture(before: [usize; 5], after: [usize; 5]) {
    assert_eq!(after, before.map(|value| value + 1), "one open/observe/verify/close with no second plan acquisition");
}
#[tokio::test]
async fn admitted_live_profile_runs_first_and_unknown_type_reuses_input_context_without_retained_native_work() {
    for case in [ExecutionProfileCase::Observed, ExecutionProfileCase::Unavailable] {
        let mut runtime = prepared(ProfileRoute::Admitted, None).await;
        let original = episode(&runtime).await;
        let retained = original.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().clone();
        let before = runtime.native_calls();
        let (_, outcome, capture) = capture_profile(&mut runtime, case, true).await;
        runtime.finish(outcome).await;
        analyze(&mut runtime).await;
        runtime.reconcile().await;
        let answered = episode(&runtime).await;
        assert_eq!(answered.first, original.first);
        assert_eq!(answered.report, original.report);
        assert_eq!(answered.source_reads.get(SourceReadQuestion::RelationalInputContext), Some(&retained));
        let reused = answered.source_read_context_reuse(&retained.focus).unwrap();
        assert_eq!(reused.capture_ref(), capture);
        let (source, row) = runtime.fixture.outbox.diagnostic_evidence(reused).await.unwrap();
        assert!(reused.validate_canonical_record(&source, &row.record).is_ok());
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::MssqlCurrentExecutionProfile(point),
            ..
        }) = &row.record
        else {
            panic!()
        };
        let inputs = point.relational_inputs.as_deref().unwrap();
        assert!(inputs.proof.is_none() && inputs.module_parameters.is_none());
        assert!(!answered.has_owed_question());
        assert!(!runtime.enroll().await);
        assert_one_capture(before, runtime.native_calls());
        let entries = runtime.fixture.outbox.status(100, None).await.unwrap().entries;
        assert!(
            !entries.iter().any(
                |entry| entry.request.collection() == TelemetryCollection::Snapshot(SnapshotDataset::RetainedPlans)
            )
        );
    }
}
#[tokio::test]
async fn retained_only_unqualified_or_foreign_component_profile_catalogs_preserve_existing_retained_route() {
    for route in [ProfileRoute::Absent, ProfileRoute::Unqualified, ProfileRoute::WrongComponent] {
        let mut runtime = prepared(route, None).await;
        let original = episode(&runtime).await;
        let before = runtime.native_calls();
        let answer = finish_retained(&mut runtime).await;
        assert_eq!(answer.first, original.first);
        assert_one_capture(before, runtime.native_calls());
        assert!(!runtime.enroll().await);
        let stage = answer.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap();
        assert!(!matches!(stage.state, SourceReadStageState::Collecting | SourceReadStageState::Answered { .. }));
    }
}
#[tokio::test]
async fn gone_or_missing_selected_input_payload_leaves_independent_retained_history_owed_after_one_profile() {
    for case in [ExecutionProfileCase::ChangedRequest, ExecutionProfileCase::NoPlanContext] {
        let mut runtime = prepared(ProfileRoute::Admitted, None).await;
        let before = runtime.native_calls();
        let (_, outcome, _) = capture_profile(&mut runtime, case, false).await;
        runtime.finish(outcome).await;
        analyze(&mut runtime).await;
        runtime.reconcile().await;
        assert_one_capture(before, runtime.native_calls());
        let profiled = episode(&runtime).await;
        let retained = profiled.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap();
        assert!(profiled.source_read_context_reuse(&retained.focus).is_none());
        assert!(profiled.has_owed_question());
        let terminal_profile = profiled.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).unwrap().clone();
        let before = runtime.native_calls();
        let answered = finish_retained(&mut runtime).await;
        assert_one_capture(before, runtime.native_calls());
        assert_eq!(answered.source_reads.get(SourceReadQuestion::CurrentExecutionProfile), Some(&terminal_profile));
        assert!(!runtime.enroll().await);
    }
}
#[tokio::test]
async fn cancelled_profile_keeps_owner_until_settlement_and_unknown_source_debt_never_authorizes_a_sibling() {
    for stop in [CollectionStop::Cancelled, CollectionStop::CancellationUnconfirmed] {
        let mut runtime = prepared(ProfileRoute::Admitted, None).await;
        assert!(runtime.enroll().await);
        let job = runtime.take_focus().await;
        let intent = next_intent(&runtime, &job).await;
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
        let DiagnosticExecutionBinding::NativeRead { execution } = &intent.execution else { panic!() };
        let (source, records) =
            current_execution_profile_result_with_input_context(focus, execution, ExecutionProfileCase::Observed);
        runtime.fixture.backend.source_read_result = Some(Frame { source, records });
        runtime.fixture.backend.focus_failure = Some(stop);
        let result = runtime.try_execute(&job).await;
        if stop == CollectionStop::CancellationUnconfirmed {
            assert_eq!(CollectionStop::from_error(&result.err().unwrap()), stop);
            assert_eq!(runtime.fixture.outbox.status(100, None).await.unwrap().unconfirmed_sources, 1);
            assert!(runtime.fixture.outbox.investigation_parent_keys().await.unwrap().contains(&job.key));
        } else {
            let result = result.unwrap();
            assert_ne!(result.event.status, CollectionStatus::Complete);
            runtime.finish(result.outcome).await;
        }
        let calls = runtime.native_calls();
        let retained_before =
            episode(&runtime).await.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().clone();
        if stop == CollectionStop::CancellationUnconfirmed {
            assert!(runtime.scheduler.focused_state(&job.key).is_some());
            // An unconfirmed native shutdown aborts the running owner. Resume through the real durable
            // recovery path rather than scheduling more work on an operation that never completed.
            runtime = crate::fleet::investigations::tests::source_reads::recovery::restart(runtime, 1).await;
            assert_eq!(runtime.scheduler.focused_state(&job.key).unwrap().source_work, SourceWorkState::Unconfirmed);
            runtime.reconcile().await;
            assert!(!runtime.enroll().await);
            assert!(runtime.scheduler.focused_state(&job.key).is_some());
            assert_eq!(
                episode(&runtime).await.source_reads.get(SourceReadQuestion::RelationalInputContext),
                Some(&retained_before)
            );
            assert_eq!(runtime.native_calls(), calls);
        } else {
            assert!(!runtime.enroll().await, "the current admitted owner blocks a sibling before reconciliation");
            assert_eq!(runtime.native_calls(), calls);
            runtime.reconcile().await;
            runtime.fixture.backend.focus_failure = None;
            let _ = finish_retained(&mut runtime).await;
        }
    }
}
#[tokio::test]
async fn restart_of_a_ready_unacknowledged_profile_keeps_exact_owner_and_reuses_context_only_after_original_consumption()
 {
    use crate::fleet::investigations::tests::source_reads::recovery::restart;
    let mut runtime = prepared(ProfileRoute::Admitted, None).await;
    let first = episode(&runtime).await.first;
    let (job, _unacknowledged, capture) = capture_profile(&mut runtime, ExecutionProfileCase::Observed, true).await;
    let calls = runtime.native_calls();
    let mut runtime = restart(runtime, 0).await;
    assert_eq!(runtime.scheduler.focused_state(&job.key).unwrap().source_work, SourceWorkState::Unconfirmed);
    runtime.reconcile().await;
    assert!(!runtime.enroll().await);
    analyze(&mut runtime).await;
    let answered = episode(&runtime).await;
    assert_eq!(answered.first, first);
    let retained = answered.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap();
    assert_eq!(answered.source_read_context_reuse(&retained.focus).unwrap().capture_ref(), capture);
    runtime.reconcile().await;
    runtime.reconcile().await;
    assert!(runtime.scheduler.focused_state(&job.key).is_none());
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}
#[tokio::test]
async fn cold_archived_pending_profile_authenticates_originals_to_eof_and_retains_reuse_across_resolution() {
    use crate::delivery::{
        diagnostic_recovery::{load_diagnostic_checkpoint, load_diagnostic_input},
        file_store::FileRecoveryStore,
        recovery::RecoveryStore,
    };
    use crate::fleet::investigations::tests::source_reads::recovery::archive_ready;
    use culprit_evidence::delivery::{contract::Contract, recovery::RecoveryRead as _};
    let archive = tempfile::tempdir().unwrap();
    let contract = Contract::new(
        "profile-policy".into(),
        "one".into(),
        "file:profile-policy".into(),
        digest(b"profile-policy-schema"),
    )
    .unwrap();
    let store = FileRecoveryStore::create(&archive.path().join("recovery"), contract.clone()).unwrap();
    let owner = store.acquire().await.unwrap();
    let mut runtime = prepared(ProfileRoute::Admitted, Some(owner.fence())).await;
    let original = episode(&runtime).await;
    let (_, _unacknowledged, capture) = capture_profile(&mut runtime, ExecutionProfileCase::Observed, true).await;
    let calls = runtime.native_calls();
    let archived = archive_ready(&mut runtime, &store, owner.fence(), &contract).await;
    let range = &archived[&capture.capture.observation_id].manifest().range_id;
    let heads = store.diagnostic_heads(None, 10).await.unwrap();
    assert_eq!(heads.heads.len(), 1);
    let directory = tempfile::tempdir().unwrap();
    let mut cold =
        Outbox::create_remote(&directory.path().join("outbox"), OutboxLimits::default(), store.id()).unwrap();
    cold.recover().await.unwrap();
    cold.restore_diagnostics(load_diagnostic_checkpoint(&store, &heads.heads[0]).await.unwrap()).await.unwrap();
    cold.restore_diagnostic_input("saved-source", load_diagnostic_input(&store, range).await.unwrap(), &store)
        .await
        .unwrap();
    let sequence = store.recover(None, 1).await.unwrap().high_sequence;
    cold.begin_diagnostic_epoch(owner.fence(), sequence).await.unwrap();
    let policies = runtime.policies();
    let cancelled = CollectionCancellation::default();
    cancelled.cancel();
    assert_eq!(analyze_pending(&mut cold, &policies, &cancelled, 10, Some(&store)).await.unwrap(), 0);
    assert_eq!(
        cold.diagnostic_input_state(&capture, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
        DiagnosticInputState::Pending
    );
    assert_eq!(runtime.native_calls(), calls);
    assert_eq!(
        analyze_pending(&mut cold, &policies, &CollectionCancellation::default(), 10, Some(&store)).await.unwrap(),
        1
    );
    let input = cold.diagnostic_input(&capture.capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap();
    let answered = input.checkpoint.unwrap().episodes.into_iter().find(|episode| episode.id == original.id).unwrap();
    assert_eq!(answered.first, original.first);
    let retained = answered.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap();
    let reused = answered.source_read_context_reuse(&retained.focus).unwrap();
    assert_eq!(reused.capture_ref(), capture);
    assert!(!answered.has_owed_question());
    assert_eq!(runtime.native_calls(), calls);
    // Prove ordinary native recovery retains the same answers instead of re-enrolling either question.
    drop(cold);
    runtime.finish(Outcome::FocusedComplete { result: capture }).await;
    analyze(&mut runtime).await;
    runtime.reconcile().await;
    let SourceObservation::Activity(header) = &mut runtime.fixture.backend.activity.source.observation else {
        panic!()
    };
    header.at = "2020-01-01T12:00:07Z".into();
    runtime.complete_empty_baseline().await;
    analyze(&mut runtime).await;
    let resolved =
        crate::fleet::investigations::tests::source_reads::temporary::latest_history_episode(&runtime, &original.id)
            .await;
    assert_eq!(resolved.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(resolved.first, original.first);
    assert_eq!(resolved.source_reads, answered.source_reads);
    assert!(!resolved.has_owed_question());
    assert!(!runtime.enroll().await);
}

mod cursor;
