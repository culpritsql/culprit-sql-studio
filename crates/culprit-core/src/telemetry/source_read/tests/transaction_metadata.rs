//! Canonical model originals exercise two observations; none qualifies native source cost.
use super::*;
use crate::telemetry::transaction_metadata::tests::{cache, slru};
mod fixture;
use fixture::*;

#[test]
fn a_second_exact_observation_preserves_initial_answer_and_stage_identity_across_roundtrip() {
    let trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let first = episode.source_reads.get(focus.question).unwrap().clone();
    let enrolled = episode.enroll_source_read(focus.clone()).unwrap();
    assert_eq!(enrolled.source_reads.len(), MAX_TRANSACTION_METADATA_OBSERVATIONS);
    assert_eq!(enrolled.source_reads.get(focus.question), Some(&first));
    let second = enrolled.source_reads.get_focus(&focus).unwrap();
    assert_ne!(first.key(&episode.id).unwrap(), second.key(&episode.id).unwrap());
    let restored: DiagnosticEpisode = serde_json::from_slice(&serde_json::to_vec(&enrolled).unwrap()).unwrap();
    assert!(restored.is_valid());
    assert_eq!(
        restored.source_reads.get_focus(&focus).unwrap().key(&episode.id).unwrap(),
        second.key(&episode.id).unwrap()
    );
    assert_eq!(restored.source_read_capture(focus.question), Some(&trial.answer.result));
    let revision =
        DiagnosticRevision::between(Some(&episode), enrolled.clone(), "2020-01-01T12:01:11Z").unwrap().unwrap();
    let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &focus.prerequisite else {
        panic!()
    };
    for reference in [episode.first.clone(), focus.basis.as_ref().clone(), continuation.previous.clone()] {
        assert!(revision.inputs.contains(&reference));
        assert!(revision.referenced_evidence().unwrap().iter().any(|link| matches!(link.target, crate::diagnostic_ancestry::DiagnosticAncestryTarget::Record(value) if value == &reference)));
    }
    assert!(matches!(restored.plan_source_read_focus(&focus, &source, &row).unwrap(), DiagnosticFocusPlan::Ready(_)));
    assert!(restored.enroll_source_read(focus.clone()).unwrap().source_reads == restored.source_reads);
    assert!(restored.transition_source_read(&first.focus, SourceReadStageState::Collecting).is_err());
}
#[test]
fn a_third_observation_or_changed_same_ordinal_focus_cannot_replace_either_stage() {
    let trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let enrolled = episode.enroll_source_read(focus.clone()).unwrap();
    let mut changed = focus.clone();
    changed.basis.payload_sha256 = digest(b"changed same ordinal");
    assert!(enrolled.enroll_source_read(changed).is_err());
    assert!(
        SourceReadFocus::from_followup_input(focus.question, &enrolled, &source, &reference, &row).unwrap().is_none()
    );
    let mut invalid = focus.clone();
    invalid.question = SourceReadQuestion::CompilationHistory;
    assert!(!invalid.is_valid());
    let stages = vec![
        enrolled.source_reads.get(focus.question).unwrap().clone(),
        enrolled.source_reads.get_focus(&focus).unwrap().clone(),
        enrolled.source_reads.get_focus(&focus).unwrap().clone(),
    ];
    assert!(SourceReadStages::from_stages(stages.clone()).is_err());
    assert!(serde_json::from_value::<SourceReadStages>(serde_json::to_value(stages).unwrap()).is_err());
    assert!(SourceReadStages::from_stages(vec![enrolled.source_reads.get_focus(&focus).unwrap().clone()]).is_err());
    let mut missing_owner = focus.clone();
    let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } =
        &mut missing_owner.prerequisite
    else {
        panic!()
    };
    let SourcePartition::InstanceLocal { member, .. } = &mut continuation.request.partition else { panic!() };
    *member = None;
    assert!(!missing_owner.is_valid());
}
#[test]
fn fresh_participant_and_native_owner_are_required_and_old_or_ended_answers_do_not_nominate() {
    let trial = trial();
    let (episode, source, row, _) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    assert!(
        SourceReadFocus::from_followup_input(
            SourceReadQuestion::TransactionMetadata,
            &episode,
            &trial.source,
            &episode.first,
            &trial.original
        )
        .unwrap()
        .is_none()
    );
    for change in ["role", "member", "process", "statement", "wait"] {
        let mut source = source.clone();
        let mut record = row.record.clone();
        match change {
            "role" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.database_writable = None;
            }
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut source.partition else {
                    panic!()
                };
                member.native = NativeIdentity::new("foreign-member");
            }
            "process" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut source.partition else { panic!() };
                *epoch = Some("foreign-process".into());
                if let SourceObservation::Activity(frame) = &mut source.observation {
                    frame.epoch = Some("foreign-process".into());
                }
            }
            "statement" => {
                let TelemetryRecord::Activity(actor) = &mut record else { panic!() };
                actor.statement_identity = Some(NativeIdentity::new("foreign-statement"));
            }
            _ => {
                let TelemetryRecord::Activity(actor) = &mut record else { panic!() };
                actor.wait = None;
            }
        }
        let (observed, source, changed, actual) = recorded(&trial, source, record, "2020-01-01T12:01:10Z", 4);
        assert!(
            !matches!(
                SourceReadFocus::from_followup_input(
                    SourceReadQuestion::TransactionMetadata,
                    &observed,
                    &source,
                    &actual,
                    &changed
                ),
                Ok(Some(_))
            ),
            "{change}"
        );
    }
    for end in [BlockingRequestState::Changed, BlockingRequestState::Missing] {
        let ended = trial_with_end(end);
        let (episode, source, row, reference) = fresh(&ended, "2020-01-01T12:01:10Z", 3);
        assert!(
            SourceReadFocus::from_followup_input(
                SourceReadQuestion::TransactionMetadata,
                &episode,
                &source,
                &reference,
                &row
            )
            .unwrap()
            .is_none()
        );
    }
}
#[test]
fn prior_point_authentication_cannot_be_replaced_by_another_canonical_source_or_body() {
    let trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &focus.prerequisite else {
        panic!()
    };
    let TelemetryRecord::Operational(point) = &trial.point.record else { panic!() };
    assert!(focus.matches_prerequisite_input(&source, &row, &trial.point_source, [(&continuation.previous, point)]));
    let mut changed = point.clone();
    changed.key = digest(b"changed prior point");
    assert!(!focus.matches_prerequisite_input(
        &source,
        &row,
        &trial.point_source,
        [(&continuation.previous, &changed)]
    ));
    assert!(!focus.matches_prerequisite_input(&source, &row, &trial.point_source, []));
    assert!(!focus.matches_prerequisite_input(
        &source,
        &row,
        &trial.point_source,
        [(&continuation.previous, point), (&continuation.previous, point)]
    ));
    let mut wrong = trial.point_source.clone();
    wrong.target = DatabaseIdentity::server(Engine::Postgres, "other", "fixture.invalid", 5432, None).into();
    assert!(!focus.matches_prerequisite_input(&source, &row, &wrong, [(&continuation.previous, point)]));
}

#[test]
fn completed_second_observation_keeps_both_receipts_and_cannot_nominate_a_third() {
    let mut trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let TelemetryRecord::Activity(actor) = &row.record else { panic!() };
    let (result_source, result, receipt) =
        point(&focus, &source, actor, "2020-01-01T12:01:12Z", "2020-01-01T12:01:13Z", "2020-01-01T12:01:14Z", 4);
    let answer = TransactionMetadataAnswer::from_originals(
        &focus,
        &source,
        &row,
        &result_source,
        &receipt.capture_ref(),
        &[(&receipt, &result)],
    )
    .unwrap();
    let completed = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer.clone()))),
            },
        )
        .unwrap();
    assert_eq!(completed.source_read_captures().collect::<Vec<_>>(), vec![&trial.answer.result, &answer.result]);
    assert!(!completed.has_owed_question());
    let first = completed.source_reads.get(focus.question).unwrap().clone();
    let mut forged = answer;
    forged.mechanism = TransactionMetadataWait::MultiXact;
    let mut stages = completed.source_reads.iter().cloned().collect::<Vec<_>>();
    stages[1].state =
        SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(forged))) };
    assert!(SourceReadStages::from_stages(stages).is_err());
    trial.checkpoint.episodes = vec![completed];
    let (later, source, row, reference) = fresh(&trial, "2020-01-01T12:02:20Z", 5);
    assert_eq!(later.source_reads.get(focus.question), Some(&first));
    assert!(SourceReadFocus::from_followup_input(focus.question, &later, &source, &reference, &row).unwrap().is_none());
}
#[test]
fn recovery_stops_unstarted_continuation_without_erasing_first_or_clearing_inflight_work() {
    let trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let first = episode.source_reads.get(focus.question).unwrap().clone();
    let enrolled = episode.enroll_source_read(focus.clone()).unwrap();
    for original in [
        enrolled.clone(),
        enrolled
            .transition_source_read(
                &focus,
                SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
            )
            .unwrap(),
    ] {
        let recovered = recover(original, "2020-01-01T12:02:00Z");
        assert_eq!(recovered.lifecycle, DiagnosticLifecycle::Resolved);
        assert!(recovered.recovery.is_some());
        assert_eq!(recovered.source_reads.get(focus.question), Some(&first));
        assert_eq!(recovered.source_reads.get_focus(&focus).unwrap().state, SourceReadStageState::Recovered);
        assert!(!recovered.retained_in_checkpoint());
        assert!(matches!(
            recovered.plan_source_read_focus(&focus, &source, &row).unwrap(),
            DiagnosticFocusPlan::NotRequired
        ));
    }
    let collecting = enrolled.transition_source_read(&focus, SourceReadStageState::Collecting).unwrap();
    let recovered = recover(collecting, "2020-01-01T12:02:00Z");
    assert_eq!(recovered.source_reads.get(focus.question), Some(&first));
    assert_eq!(recovered.source_reads.get_focus(&focus).unwrap().state, SourceReadStageState::Collecting);
    assert!(recovered.has_owed_question(), "recovery is not an acknowledgement of native cancellation");
}
#[test]
fn actual_intent_uses_the_same_exact_observation_key_and_unqualified_or_foreign_receipt_refuses() {
    let trial = trial();
    let (episode, source, row, reference) = fresh(&trial, "2020-01-01T12:01:10Z", 3);
    let focus = continuation(&episode, &source, &row, &reference);
    let episode = episode.enroll_source_read(focus.clone()).unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &NativeReadContext::from_activity(&source).unwrap())
            .unwrap();
    let (mut recipe, component) = super::recipe(&super::original());
    recipe.id = "fixture.transaction-metadata".into();
    recipe.selector = focus.question.selector();
    let RecipeAdmission::Measured(receipt) = &mut recipe.admission else { panic!() };
    receipt.recipe_id = recipe.id.clone();
    receipt.selector = recipe.selector;
    receipt.scope = RecipeCostScope::NativeRead(execution.clone());
    let intent = DiagnosticInvestigationIntent::new_source_read(
        &episode,
        &recipe,
        focus.clone(),
        execution.clone(),
        &component,
        &CollectionBudget::default(),
    )
    .unwrap();
    assert_eq!(
        intent.episode_stage_key().unwrap(),
        episode.source_reads.get_focus(&focus).unwrap().key(&episode.id).unwrap()
    );
    let stored: DiagnosticInvestigationIntent = serde_json::from_value(serde_json::to_value(&intent).unwrap()).unwrap();
    assert_eq!(stored.episode_stage_key().unwrap(), intent.episode_stage_key().unwrap());
    let mut foreign = recipe.clone();
    let RecipeAdmission::Measured(receipt) = &mut foreign.admission else { panic!() };
    let RecipeCostScope::NativeRead(SourceReadExecutionScope::Local(scope)) = &mut receipt.scope else { panic!() };
    let SourcePartition::InstanceLocal { epoch, .. } = &mut scope.partition else { panic!() };
    *epoch = Some("foreign-process".into());
    assert!(
        DiagnosticInvestigationIntent::new_source_read(
            &episode,
            &foreign,
            focus.clone(),
            execution.clone(),
            &component,
            &CollectionBudget::default()
        )
        .is_err()
    );
    recipe.admission = RecipeAdmission::Unqualified;
    assert!(
        DiagnosticInvestigationIntent::new_source_read(
            &episode,
            &recipe,
            focus,
            execution,
            &component,
            &CollectionBudget::default()
        )
        .is_err()
    );
}
