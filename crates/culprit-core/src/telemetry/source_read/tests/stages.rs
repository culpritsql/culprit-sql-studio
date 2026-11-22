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
    for question in [
        SourceReadQuestion::CompilationHistory,
        SourceReadQuestion::HyperscaleLogComponents,
        SourceReadQuestion::RelationalInputContext,
        SourceReadQuestion::TransactionMetadata,
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
