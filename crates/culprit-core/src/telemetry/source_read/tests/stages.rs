//! The collection tests use genuine independent question fixtures as structural model data. An episode still
//! refuses a foreign question's original basis; these tests mint no joint incident or native admission.
use super::*;

fn pending(focus: &SourceReadFocus) -> SourceReadStage {
    SourceReadStage { focus: focus.clone(), state: SourceReadStageState::Pending }
}
fn independent_stages() -> (Original, Original, SourceReadStages) {
    let compiler = original();
    let log = super::log_rates::original_log_wait();
    let stages = SourceReadStages::from_stages(vec![pending(&log.focus), pending(&compiler.focus)]).unwrap();
    (compiler, log, stages)
}
#[test]
fn recorded_question_collection_is_bounded_unique_and_canonical_without_becoming_episode_authority() {
    let (compiler, log, stages) = independent_stages();
    assert!(stages.is_valid());
    assert_eq!(stages.len(), 2);
    let reversed = SourceReadStages::from_stages(vec![pending(&compiler.focus), pending(&log.focus)]).unwrap();
    assert_eq!(serde_json::to_vec(&stages).unwrap(), serde_json::to_vec(&reversed).unwrap());
    let stored: SourceReadStages = serde_json::from_slice(&serde_json::to_vec(&stages).unwrap()).unwrap();
    assert_eq!(stored, stages);
    assert!(SourceReadStages::from_stages(vec![pending(&compiler.focus), pending(&compiler.focus)]).is_err());
    let duplicate = serde_json::to_vec(&vec![pending(&compiler.focus); SourceReadQuestion::ALL.len() + 1]).unwrap();
    assert!(serde_json::from_slice::<SourceReadStages>(&duplicate).is_err());
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    assert!(enrolled.enroll_source_read(log.focus).is_err(), "structural data is not another incident's authority");
}
#[test]
fn exact_focus_transition_preserves_siblings_and_rejects_same_question_rebinding() {
    let (compiler, log, mut stages) = independent_stages();
    let sibling = stages.get(log.focus.question).unwrap().clone();
    assert!(stages.transition(&compiler.focus, SourceReadStageState::Recovered).is_err());
    stages.transition(&compiler.focus, SourceReadStageState::Collecting).unwrap();
    assert_eq!(stages.get(log.focus.question), Some(&sibling));
    let mut foreign = compiler.focus.clone();
    foreign.basis.manifest_sha256 = digest(b"foreign-native-original");
    assert!(foreign.is_valid());
    let before = stages.clone();
    assert!(
        stages
            .transition(&foreign, SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::SourceChanged })
            .is_err()
    );
    assert!(stages.enroll(pending(&foreign)).is_err());
    assert_eq!(stages, before);
    assert!(stages.accepts_transition(&before));
    assert!(!stages.accepts_transition(&SourceReadStages::new()));
}
#[test]
fn terminal_sibling_cannot_hide_or_rewind_another_admitted_question_after_roundtrip() {
    let (compiler, log, mut stages) = independent_stages();
    stages.transition(&compiler.focus, SourceReadStageState::Collecting).unwrap();
    let capture = result_capture(&compiler);
    stages.transition(&compiler.focus, SourceReadStageState::Captured { capture: capture.clone() }).unwrap();
    let terminal = stages.get(compiler.focus.question).unwrap().clone();
    stages.transition(&log.focus, SourceReadStageState::Collecting).unwrap();
    let mut stored: SourceReadStages = serde_json::from_slice(&serde_json::to_vec(&stages).unwrap()).unwrap();
    assert_eq!(stored.get(compiler.focus.question), Some(&terminal));
    assert!(stored.get(log.focus.question).unwrap().needs_work());
    assert!(!stored.get(log.focus.question).unwrap().needs_admission());
    assert!(stored.transition(&compiler.focus, SourceReadStageState::Collecting).is_err());
    stored.enroll(pending(&compiler.focus)).unwrap();
    assert_eq!(stored.get(compiler.focus.question), Some(&terminal));
    let results = stored.iter().filter_map(SourceReadStage::result).collect::<Vec<_>>();
    assert_eq!(results, vec![&capture]);
}
#[test]
fn exact_question_key_matches_actual_intent_and_keeps_changed_focus_from_replacing_the_stage() {
    let compiler = original();
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    let (recipe, component) = recipe(&compiler);
    let intent = DiagnosticInvestigationIntent::new_source_read(
        &enrolled,
        &recipe,
        compiler.focus.clone(),
        compiler.execution.clone(),
        &component,
        &CollectionBudget::default(),
    )
    .unwrap();
    let stage = enrolled.source_reads.get(compiler.focus.question).unwrap();
    assert_eq!(stage.key(&enrolled.id).unwrap(), intent.episode_stage_key().unwrap());
    assert_eq!(enrolled.source_read_foci().unwrap(), vec![compiler.focus.clone()]);
    assert!(enrolled.source_read_focus(SourceReadQuestion::HyperscaleLogComponents).unwrap().is_none());
    let collecting = enrolled.transition_source_read(&compiler.focus, SourceReadStageState::Collecting).unwrap();
    assert!(collecting.has_owed_question());
    assert!(matches!(
        collecting
            .plan_source_read_for_execution(
                &compiler.focus,
                &compiler.source,
                &compiler.row,
                &compiler.execution.clone().into()
            )
            .unwrap(),
        DiagnosticFocusPlan::NotRequired
    ));
}
#[test]
fn episode_wire_requires_the_new_collection_and_never_accepts_the_removed_scalar_field() {
    let compiler = original();
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    let mut value = serde_json::to_value(&enrolled).unwrap();
    assert!(value.get("source_read").is_none());
    assert_eq!(value.get("source_reads").unwrap().as_array().unwrap().len(), 1);
    let stored: DiagnosticEpisode = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(stored, enrolled);
    value.as_object_mut().unwrap().remove("source_reads");
    assert!(serde_json::from_value::<DiagnosticEpisode>(value.clone()).is_err());
    value["source_read"] = serde_json::to_value(pending(&compiler.focus)).unwrap();
    assert!(serde_json::from_value::<DiagnosticEpisode>(value).is_err());
}
#[test]
fn one_exhaustive_reference_owner_retains_prerequisites_and_still_obeys_revision_caps() {
    let compiler = original();
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    let revision =
        DiagnosticRevision::between(Some(&compiler.episode), enrolled, "2020-01-01T12:00:03Z").unwrap().unwrap();
    for reference in compiler.focus.references() {
        assert!(revision.inputs.contains(reference));
        assert!(revision.referenced_evidence().unwrap().iter().any(|link| {
            matches!(link.target, crate::diagnostic_ancestry::DiagnosticAncestryTarget::Record(value) if value == reference)
        }));
    }
    let mut missing = revision.clone();
    let detail = compiler.focus.prerequisite.references().next().unwrap();
    missing.inputs.retain(|reference| reference != detail);
    assert!(!missing.is_valid());
    let mut overflowing = revision;
    for ordinal in 0..MAX_DIAGNOSTIC_INPUTS {
        let mut reference = compiler.focus.basis.as_ref().clone();
        reference.row_ordinal = ordinal as u32;
        reference.payload_sha256 = digest(format!("controlled-distinct-input-{ordinal}").as_bytes());
        if !overflowing.inputs.contains(&reference) {
            overflowing.inputs.push(reference);
        }
    }
    assert!(overflowing.inputs.len() > MAX_DIAGNOSTIC_INPUTS);
    assert!(!overflowing.is_valid());
}
#[test]
fn live_episode_requirement_is_distinct_from_historical_work_and_never_manufactures_recovery() {
    assert!(SourceReadQuestion::TemporarySpaceContributors.requires_live_episode());
    assert!(SourceReadQuestion::WorkerStarvation.requires_live_episode());
    assert!(SourceReadQuestion::TransactionMetadata.requires_live_episode());
    assert!(SourceReadQuestion::MetadataLockContext.requires_live_episode());
    for question in [
        SourceReadQuestion::CompilationHistory,
        SourceReadQuestion::HyperscaleLogComponents,
        SourceReadQuestion::RelationalInputContext,
    ] {
        assert!(!question.requires_live_episode());
    }
    let compiler = original();
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    assert!(enrolled.recover_source_read_stages().is_err());
    let stopped = enrolled
        .transition_source_read(
            &compiler.focus,
            SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::SourceChanged },
        )
        .unwrap();
    assert_eq!(stopped.lifecycle, DiagnosticLifecycle::Active);
    assert!(stopped.recovery.is_none());
}

#[test]
fn primary_analysis_and_child_debt_remain_independent_and_cannot_advance_in_one_revision() {
    let compiler = original();
    let enrolled = compiler.episode.enroll_source_read(compiler.focus.clone()).unwrap();
    let before = enrolled
        .transition_source_read(
            &compiler.focus,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
        )
        .unwrap();
    let TelemetryRecord::Activity(actor) = &compiler.row.record else { panic!() };
    let DiagnosticFocusPlan::Ready(primary) =
        before.plan_focus(&compiler.source, actor, &compiler.focus.basis).unwrap()
    else {
        panic!("a child refusal does not settle partial primary analysis")
    };
    assert!(before.needs_question(&primary));
    let mut primary_complete = before.clone();
    primary_complete.analysis = DiagnosticAnalysisStatus::Analyzed;
    primary_complete.issue = None;
    let revision =
        DiagnosticRevision::between(Some(&before), primary_complete.clone(), "2020-01-01T12:00:04Z").unwrap().unwrap();
    assert_eq!(revision.episode.source_reads, before.source_reads);
    assert!(!primary_complete.needs_question(&primary));
    assert!(primary_complete.has_owed_question(), "the independently deferred child is still owed");
    let terminal = before
        .transition_source_read(
            &compiler.focus,
            SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::Unsupported },
        )
        .unwrap();
    assert!(terminal.has_owed_question(), "terminal child state cannot hide partial primary work");
    let mut both_changed = terminal.clone();
    both_changed.analysis = DiagnosticAnalysisStatus::Analyzed;
    assert!(
        DiagnosticRevision::between(Some(&before), both_changed, "2020-01-01T12:00:04Z").is_err(),
        "a primary-only transition requires identical child states"
    );
    let mut complete = terminal.clone();
    complete.analysis = DiagnosticAnalysisStatus::Analyzed;
    assert!(DiagnosticRevision::between(Some(&terminal), complete.clone(), "2020-01-01T12:00:04Z").is_ok());
    assert!(!complete.has_owed_question());
}

#[test]
fn a_child_only_signal_does_not_acquire_phantom_primary_debt_after_its_stage_stops() {
    let value = super::log_rates::original_log_wait();
    let enrolled = value.episode.enroll_source_read(value.focus.clone()).unwrap();
    let stopped = enrolled
        .transition_source_read(
            &value.focus,
            SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::Unsupported },
        )
        .unwrap();
    assert_eq!(stopped.analysis, DiagnosticAnalysisStatus::Pending);
    assert!(!stopped.has_owed_question());
    let mut invented_primary = stopped.clone();
    invented_primary.analysis = DiagnosticAnalysisStatus::Analyzed;
    assert!(DiagnosticRevision::between(Some(&stopped), invented_primary, "2020-01-01T12:00:04Z").is_err());
}

#[test]
fn recovery_retires_completed_partial_primary_without_faking_complete_analysis_or_erasing_unknown_debt() {
    let original = original();
    assert_eq!(original.episode.analysis, DiagnosticAnalysisStatus::Partial);
    assert!(original.episode.report.is_some());
    let active = original
        .episode
        .enroll_source_read(original.focus.clone())
        .unwrap()
        .transition_source_read(
            &original.focus,
            SourceReadStageState::Stopped { issue: DiagnosticAnalysisIssue::Unsupported },
        )
        .unwrap();
    assert!(active.has_owed_question(), "the still-adverse primary is independent of its terminal child");
    let mut recovered = active.clone();
    recovered.lifecycle = DiagnosticLifecycle::Resolved;
    recovered.recovery = Some(DiagnosticRecoveryProof::CompleteScope { capture: original.focus.basis.capture_ref() });
    let TelemetryRecord::Activity(actor) = &original.row.record else { panic!() };
    assert!(recovered.is_valid());
    assert_eq!(recovered.analysis, DiagnosticAnalysisStatus::Partial);
    assert_eq!(recovered.report, active.report);
    assert_eq!(recovered.source_reads, active.source_reads);
    assert!(!recovered.has_owed_question());
    assert!(!recovered.retained_in_checkpoint());
    assert_eq!(
        recovered.plan_focus(&original.source, actor, &original.focus.basis).unwrap(),
        DiagnosticFocusPlan::NotRequired
    );
    let revision =
        DiagnosticRevision::between(Some(&active), recovered.clone(), "2020-01-01T12:00:04Z").unwrap().unwrap();
    assert_eq!(revision.episode.analysis, DiagnosticAnalysisStatus::Partial);
    assert_eq!(revision.episode.source_reads, active.source_reads);
    let mut missing = original.episode.clone();
    missing.lifecycle = recovered.lifecycle;
    missing.recovery = recovered.recovery;
    missing.report = None;
    assert!(missing.is_valid());
    assert!(missing.has_owed_question(), "no retained primary answer cannot be called settled partial work");
    assert!(missing.retained_in_checkpoint());
    assert!(matches!(
        missing.plan_focus(&original.source, actor, &original.focus.basis).unwrap(),
        DiagnosticFocusPlan::Ready(_)
    ));
}

#[test]
fn qualified_recovery_preserves_unperformed_primary_and_independently_admitted_historical_child_work() {
    let original = original();
    let TelemetryRecord::Activity(actor) = &original.row.record else { panic!() };
    for state in
        [DiagnosticAnalysisStatus::Pending, DiagnosticAnalysisStatus::Deferred, DiagnosticAnalysisStatus::Collecting]
    {
        let mut recovered = original.episode.clone();
        recovered.lifecycle = DiagnosticLifecycle::Resolved;
        recovered.recovery =
            Some(DiagnosticRecoveryProof::CompleteScope { capture: original.focus.basis.capture_ref() });
        recovered.analysis = state;
        recovered.issue =
            (state == DiagnosticAnalysisStatus::Deferred).then_some(DiagnosticAnalysisIssue::CostUnqualified);
        assert!(recovered.is_valid());
        assert!(recovered.has_owed_question(), "{state:?}");
        let DiagnosticFocusPlan::Ready(focus) =
            recovered.plan_focus(&original.source, actor, &original.focus.basis).unwrap()
        else {
            panic!("the original primary was not yet performed: {state:?}")
        };
        assert!(recovered.needs_question(&focus));
    }
    let collecting = original
        .episode
        .enroll_source_read(original.focus.clone())
        .unwrap()
        .transition_source_read(&original.focus, SourceReadStageState::Collecting)
        .unwrap();
    let mut recovered = collecting.clone();
    recovered.lifecycle = DiagnosticLifecycle::Resolved;
    recovered.recovery = Some(DiagnosticRecoveryProof::CompleteScope { capture: original.focus.basis.capture_ref() });
    assert!(recovered.has_owed_question(), "an admitted native child still owes actual settlement");
    assert!(recovered.retained_in_checkpoint());
    assert!(recovered.needs_question(&DiagnosticFocus::SourceRead { focus: Box::new(original.focus.clone()) }));
    let capture = result_capture(&original);
    let captured =
        recovered.transition_source_read(&original.focus, SourceReadStageState::Captured { capture }).unwrap();
    assert_eq!(captured.analysis, DiagnosticAnalysisStatus::Partial);
    assert!(!captured.retained_in_checkpoint());
}
