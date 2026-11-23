//! Original incident evidence can owe independent finite questions, even after its wait is quiet.
use super::*;

pub(super) fn qualify(
    input: &DiagnosticInput,
    assignment: &DiagnosticAssignment,
    update: &mut DiagnosticUpdate,
) -> Result<()> {
    for episode in &mut update.checkpoint.episodes {
        for &question in SourceReadQuestion::ALL {
            qualify_question(input, assignment, episode, question)?;
        }
    }
    update.step_id = digest(
        &serde_json::to_vec(&("diagnostic-source-read-policy-v2", &update.step_id, &assignment.recipes))
            .map_err(|_| CollectionStop::InvalidPage.error())?,
    );
    rebuild_revisions(input, update)
}

fn qualify_question(
    input: &DiagnosticInput,
    assignment: &DiagnosticAssignment,
    episode: &mut DiagnosticEpisode,
    question: SourceReadQuestion,
) -> Result<()> {
    if episode.lifecycle == DiagnosticLifecycle::Resolved && question.requires_live_episode() {
        return Ok(());
    }
    let Some(focus) = nominated_focus(input, episode, question)? else { return Ok(()) };
    let existing = episode.source_reads.get_focus(&focus);
    if existing.is_some_and(|stage| !stage.needs_admission()) || !continuation_due(episode, &focus) {
        return Ok(());
    }
    if episode.source_read_context_reuse(&focus).is_some() {
        return Ok(());
    }
    if existing.is_none() && !input_justifies_enrollment(input, episode, &focus) {
        return Ok(());
    }
    let recipe = assignment.recipes.as_deref().and_then(|recipes| {
        recipes.iter().find(|recipe| {
            recipe.purpose == RecipePurpose::Investigation
                && recipe.selector == question.selector()
                && recipe.subjects.iter().any(|subject| subject.accepts(&assignment.subject))
        })
    });
    let issue = if assignment.recipes.is_some() {
        declared_issue(recipe)
    } else {
        DiagnosticAnalysisIssue::SourceUnavailable { code: None, stopped: None }
    };
    *episode = episode
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Deferred { issue })?;
    Ok(())
}

fn nominated_focus(
    input: &DiagnosticInput,
    episode: &DiagnosticEpisode,
    question: SourceReadQuestion,
) -> Result<Option<SourceReadFocus>> {
    if episode.source_reads.latest(question).is_none_or(SourceReadStage::needs_admission)
        && let Some(focus) = episode.source_read_focus(question)?
    {
        return Ok(Some(focus));
    }
    let mut selected = None;
    for row in &input.rows {
        if let Some(focus) =
            SourceReadFocus::from_followup_input(question, episode, &input.source, &row.reference, &row.row)?
        {
            if selected.is_some() {
                return Err(CollectionStop::InvalidPage.error());
            }
            selected = Some(focus);
        }
    }
    Ok(selected)
}

fn input_justifies_enrollment(input: &DiagnosticInput, episode: &DiagnosticEpisode, focus: &SourceReadFocus) -> bool {
    let original_input = match &focus.prerequisite {
        SourceReadPrerequisite::ObservedCleanupBoundary { .. }
        | SourceReadPrerequisite::ObservedTransactionMetadataContinuation { .. } => focus.basis.capture_ref(),
        prerequisite => prerequisite
            .observation()
            .map(DiagnosticEvidenceRef::capture_ref)
            .unwrap_or_else(|| episode.first.capture_ref()),
    };
    if original_input != input.parent {
        return false;
    }
    if focus.question != SourceReadQuestion::TemporarySpaceContributors
        && !matches!(focus.prerequisite, SourceReadPrerequisite::ObservedTransactionMetadataContinuation { .. })
    {
        return true;
    }
    // The native body, not boundary presence or cost qualification, satisfies the shared attention policy.
    input.rows.iter().any(|row| row.reference == *focus.basis && focus.matches_original(&input.source, &row.row))
}

/// Native observation clocks must witness continued adversity after the SAME scheduler cooldown.
/// This does not reserve work; durable admission independently enforces resource/family cooldown and debt.
pub(crate) fn continuation_due(episode: &DiagnosticEpisode, focus: &SourceReadFocus) -> bool {
    let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &focus.prerequisite else {
        return true;
    };
    let Some(stage) = episode.source_reads.get(SourceReadQuestion::TransactionMetadata) else {
        return false;
    };
    let SourceReadStageState::Answered { answer } = &stage.state else {
        return false;
    };
    let SourceReadAnswer::TransactionMetadata(first) = answer.as_ref() else {
        return false;
    };
    let (Ok(first_until), Ok(fresh_at)) = (
        chrono::DateTime::parse_from_rfc3339(&first.result_context.observed_until),
        chrono::DateTime::parse_from_rfc3339(&continuation.request.original_observed_at),
    ) else {
        return false;
    };
    first_until
        .checked_add_signed(chrono::Duration::seconds(i64::from(crate::fleet::FOCUSED_COOLDOWN_SECONDS)))
        .is_some_and(|after_cooldown| fresh_at >= after_cooldown)
}
