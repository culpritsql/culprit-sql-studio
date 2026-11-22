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
    let existing = episode.source_reads.get(question);
    if existing.is_some_and(|stage| !stage.needs_admission()) {
        return Ok(());
    }
    let Some(focus) = nominated_focus(input, episode, question)? else { return Ok(()) };
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
    if let Some(focus) = episode.source_read_focus(question)? {
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
        SourceReadPrerequisite::ObservedCleanupBoundary { .. } => focus.basis.capture_ref(),
        prerequisite => prerequisite
            .observation()
            .map(DiagnosticEvidenceRef::capture_ref)
            .unwrap_or_else(|| episode.first.capture_ref()),
    };
    if original_input != input.parent {
        return false;
    }
    if focus.question != SourceReadQuestion::TemporarySpaceContributors {
        return true;
    }
    // The native body, not boundary presence or cost qualification, satisfies the shared attention policy.
    input.rows.iter().any(|row| row.reference == *focus.basis && focus.matches_original(&input.source, &row.row))
}
