//! A retained answer selects already checked native context, never an allocating statement or a cause.
use super::*;
mod relational_inputs;
mod transaction_metadata;
mod workers;

pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    explanation: &mut Explanation,
) -> Result<()> {
    for stage in episode.source_reads.iter() {
        let SourceReadStageState::Answered { answer } = &stage.state else { continue };
        let mut followup = Explanation::default();
        match answer.as_ref() {
            SourceReadAnswer::TemporaryResources(answer) => temporary(originals, answer, &mut followup),
            SourceReadAnswer::TransactionMetadata(answer) => {
                transaction_metadata::append(originals, answer, &mut followup)
            }
            SourceReadAnswer::WorkerStarvation(answer) => workers::append(originals, answer, &mut followup),
            SourceReadAnswer::RelationalInputs(answer) => relational_inputs::append(originals, answer, &mut followup),
            // Compilation totals do not identify a historical execution's caller, runtime inputs or harmful reuse.
            SourceReadAnswer::QueryCompilation(_) => Ok(()),
        }?;
        explanation.merge_followup(followup);
    }
    Ok(())
}

fn temporary(
    originals: &[&dyn OriginalProjection],
    answer: &TemporaryResourceAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    if !answer.is_valid() {
        return Err(invalid());
    }
    if answer.checked_task_contexts.is_empty() {
        return Ok(());
    }
    let Some(original) = originals.iter().copied().find(|original| original.capture() == &answer.result.capture) else {
        return Ok(());
    };
    if original.original() != &answer.result
        // SourceRead keeps its logical data namespace separate from this source-owned native execution.
        || original.source().observation.execution_context() != Some(&answer.result_context)
    {
        return Err(invalid());
    }
    let report = task_space_report(original)?;
    for reference in &answer.checked_task_contexts {
        let Some((selected, position)) = resolve(originals, reference)? else { continue };
        if selected.original() != original.original() {
            return Err(invalid());
        }
        let TelemetryRecord::Operational(point) = &selected.records()[position].row().record else {
            return Err(invalid());
        };
        let entry = report.entries.iter().find(|entry| entry.observation_key == point.key).ok_or_else(invalid)?;
        // The answer's checked-context claim must still match the shared interpreter over the full raw roster.
        let candidate = task_space_request(selected, position, entry)?.ok_or_else(invalid)?;
        explanation.candidates.push(candidate);
    }
    for limitation in report.limitations {
        if !explanation.limitations.contains(&limitation) {
            explanation.limitations.push(limitation);
        }
    }
    Ok(())
}
