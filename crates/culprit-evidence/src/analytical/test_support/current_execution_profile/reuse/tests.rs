use super::*;

fn retained(episode: &DiagnosticEpisode) -> &SourceReadStage {
    episode.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap()
}
#[test]
fn complete_current_context_with_unknown_type_reuses_exact_evidence_without_fabricating_a_retained_result() {
    let fixture = current_execution_profile_context_reuse_fixture(ExecutionProfileCase::Observed);
    let stage = retained(&fixture.episode);
    assert!(stage.needs_admission(), "the recorded historical state is unchanged");
    let inputs = fixture.answer.evidence.relational_inputs.as_deref().unwrap();
    assert!(inputs.module_parameters.is_none() && inputs.proof.is_none());
    assert!(fixture.answer.evidence.has_relational_input_context());
    assert_eq!(fixture.episode.source_read_context_reuse(&stage.focus), Some(&fixture.reference));
    assert!(!fixture.episode.has_owed_question());
    assert!(fixture.episode.source_read_admission_order().is_empty());
    assert!(fixture.episode.source_read_capture(SourceReadQuestion::RelationalInputContext).is_none());
    assert_eq!(
        fixture.episode.source_read_capture(SourceReadQuestion::CurrentExecutionProfile),
        Some(&fixture.capture)
    );
    assert!(!fixture.episode.needs_question(&DiagnosticFocus::SourceRead { focus: Box::new(stage.focus.clone()) }));
    assert!(matches!(
        fixture.episode.plan_source_read_focus(&stage.focus, &fixture.basis_source, &fixture.basis,).unwrap(),
        DiagnosticFocusPlan::NotRequired
    ));
    let restored: DiagnosticEpisode = serde_json::from_slice(&serde_json::to_vec(&fixture.episode).unwrap()).unwrap();
    assert_eq!(restored, fixture.episode);
    assert_eq!(restored.source_read_context_reuse(&retained(&restored).focus), Some(&fixture.reference));
}
#[test]
fn unavailable_profiling_does_not_discard_independently_acquired_input_context_or_invent_full_thread_coverage() {
    let fixture = current_execution_profile_context_reuse_fixture(ExecutionProfileCase::Unavailable);
    assert!(fixture.answer.is_valid());
    assert!(matches!(fixture.answer.profiles, ExecutionProfileAvailability::Unavailable { .. }));
    assert_eq!(fixture.answer.evidence.profile_rows, 0);
    assert_eq!(fixture.episode.source_read_context_reuse(&retained(&fixture.episode).focus), Some(&fixture.reference));
    assert_eq!(fixture.answer.source_state, culprit_core::SourceState::Partial);
}
#[test]
fn missing_changed_or_truncated_selected_context_cannot_suppress_the_retained_question() {
    for case in [ExecutionProfileCase::NoPlanContext, ExecutionProfileCase::ChangedRequest] {
        let fixture = current_execution_profile_context_reuse_fixture(case);
        assert!(fixture.episode.is_valid() && fixture.answer.is_valid());
        assert!(!fixture.answer.evidence.has_relational_input_context());
        assert!(fixture.episode.source_read_context_reuse(&retained(&fixture.episode).focus).is_none());
        assert!(fixture.episode.has_owed_question());
        assert_eq!(fixture.episode.source_read_admission_order().len(), 1);
    }
    let fixture = current_execution_profile_context_reuse_fixture(ExecutionProfileCase::Observed);
    let mut row = fixture.row.record.clone();
    let TelemetryRecord::Operational(observation) = &mut row else { panic!() };
    let OperationalFact::MssqlCurrentExecutionProfile(point) = &mut observation.fact else { panic!() };
    point.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized };
    assert!(!point.is_valid(), "truncated required payload cannot retain a complete-context capsule");
    assert!(ExportedRecord::new(&fixture.source, &fixture.capture.capture, row).is_err());
}
#[test]
fn a_genuinely_different_current_cache_incarnation_is_complementary_instead_of_reusing_the_old_work_context() {
    let fixture = current_execution_profile_fixture(ExecutionProfileCase::Observed);
    let execution = SourceReadExecutionScope::from_context(
        fixture.focus.question,
        &fixture.focus.original_context(&fixture.basis_source).unwrap(),
    )
    .unwrap();
    let (source, mut rows) = super::super::current_execution_profile_result_with_input_context(
        &fixture.focus,
        &execution,
        ExecutionProfileCase::Observed,
    );
    let TelemetryRecord::Operational(observation) = &mut rows[0] else { panic!() };
    let OperationalFact::MssqlCurrentExecutionProfile(point) = &mut observation.fact else { panic!() };
    let inputs = point.relational_inputs.as_mut().unwrap();
    inputs.context.identity.plan_generation = "2".into();
    let MssqlCurrentRequestPlan::Observed { context, .. } = &mut point.work.as_mut().unwrap().current_plan else {
        panic!()
    };
    context.identity.plan_generation = "2".into();
    let MssqlExecutionProfiles::Observed { operators: MssqlExecutionOperators::Observed { context, .. }, .. } =
        &mut point.profiles
    else {
        panic!()
    };
    context.identity.plan_generation = "2".into();
    assert!(point.is_valid());
    observation.key = point.key().unwrap();
    let fixture = finish(fixture, source, rows);
    assert!(fixture.answer.evidence.has_relational_input_context());
    assert!(fixture.episode.source_read_context_reuse(&retained(&fixture.episode).focus).is_none());
    assert!(fixture.episode.has_owed_question());
}
#[test]
fn admitted_or_terminal_retained_siblings_are_never_hidden_rewound_or_cleared_by_context_reuse() {
    let fixture = current_execution_profile_context_reuse_fixture(ExecutionProfileCase::Observed);
    let focus = retained(&fixture.episode).focus.clone();
    for state in [
        SourceReadStageState::Collecting,
        SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::Interrupted },
    ] {
        let mut changed = fixture.episode.clone();
        if matches!(state, SourceReadStageState::Collecting) {
            assert!(changed.transition_source_read(&focus, state.clone()).is_err(), "reuse grants no new admission");
            // A separately recorded admitted owner must remain owed; this constructs model data, not admission.
            let mut stages = changed.source_reads.iter().cloned().collect::<Vec<_>>();
            stages.iter_mut().find(|stage| stage.focus == focus).unwrap().state = state.clone();
            changed.source_reads = SourceReadStages::from_stages(stages).unwrap();
            assert!(changed.is_valid());
        } else {
            changed = changed.transition_source_read(&focus, state.clone()).unwrap();
        }
        assert!(changed.source_read_context_reuse(&focus).is_none());
        assert_eq!(retained(&changed).state, state);
        assert_eq!(
            changed.source_reads.get(SourceReadQuestion::CurrentExecutionProfile),
            fixture.episode.source_reads.get(SourceReadQuestion::CurrentExecutionProfile)
        );
        assert_eq!(changed.first, fixture.episode.first);
        assert!(changed.source_read_admission_order().is_empty());
        if matches!(state, SourceReadStageState::Collecting) {
            assert!(changed.has_owed_question());
        } else {
            assert!(changed.transition_source_read(&focus, SourceReadStageState::Collecting).is_err());
        }
    }
}
#[test]
fn safe_public_stage_retains_recorded_state_and_exact_reuse_reference_without_native_authority() {
    let fixture = current_execution_profile_context_reuse_fixture(ExecutionProfileCase::Observed);
    let stage = retained(&fixture.episode);
    let public = crate::analytical::investigations::source_stage(&fixture.episode, stage).unwrap();
    assert_eq!(public["state"], "deferred");
    assert!(public["result"].is_null(), "no retained source was executed");
    assert_eq!(public["context_reuse"]["question"], "current-execution-profile");
    assert_eq!(public["context_reuse"]["reference"], serde_json::to_value(&fixture.reference).unwrap());
    for field in ["basis", "prerequisite", "target", "partition", "original", "role", "proof"] {
        assert!(public.get(field).is_none(), "{field}");
    }
    assert!(serde_json::from_value::<SourceReadStage>(public).is_err());
    assert_eq!(stage, retained(&fixture.episode));
}
