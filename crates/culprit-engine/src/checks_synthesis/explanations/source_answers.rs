//! A retained answer selects already checked native context, never an allocating statement or a cause.
use super::*;
mod execution_profiles;
mod metadata_locks;
mod output_grant;
mod query_compilation;
mod readable_secondary_recovery;
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
        if episode.source_reads.get(stage.focus.question) != Some(stage) {
            continue;
        }
        let mut followup = Explanation::default();
        match answer.as_ref() {
            SourceReadAnswer::TemporaryResources(answer) => temporary(originals, answer, &mut followup),
            SourceReadAnswer::OutputGrantRetention(answer) => output_grant::append(originals, answer, &mut followup),
            SourceReadAnswer::MetadataLockContext(answer) => metadata_locks::append(originals, answer, &mut followup),
            SourceReadAnswer::ReadableSecondaryRecovery(answer) => {
                readable_secondary_recovery::append(originals, answer, &mut followup)
            }
            SourceReadAnswer::TransactionMetadata(answer) => {
                transaction_metadata::append(originals, episode, answer, &mut followup)
            }
            SourceReadAnswer::WorkerStarvation(answer) => workers::append(originals, answer, &mut followup),
            SourceReadAnswer::RelationalInputs(answer) => relational_inputs::append(originals, answer, &mut followup),
            SourceReadAnswer::CurrentExecutionProfile(answer) => {
                execution_profiles::append(originals, answer, &mut followup)
            }
            SourceReadAnswer::QueryCompilation(answer) => query_compilation::append(originals, answer, &mut followup),
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
    let mut mechanisms = Vec::new();
    let mut actions = Vec::new();
    for entry in report
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.activity,
                culprit_core::TaskAllocationActivity::UserObjects
                    | culprit_core::TaskAllocationActivity::InternalObjects
                    | culprit_core::TaskAllocationActivity::Mixed
            ) || entry.current_request.is_some()
        })
        .take(8)
    {
        if !mechanisms.contains(&entry.explanation) {
            mechanisms.push(entry.explanation.clone());
        }
        if !actions.contains(&entry.investigation) {
            actions.push(entry.investigation.clone());
        }
    }
    if mechanisms.is_empty() && report.entries.is_empty() && answer.source_state != SourceState::Collected {
        mechanisms.push(report.summary.clone());
    }
    if !mechanisms.is_empty() {
        explanation.mechanism = Some(mechanisms.join("\n\n"));
    }
    if !actions.is_empty() {
        explanation.action = Some(actions.join("\n\n"));
    }
    for limitation in report.limitations {
        if !explanation.limitations.contains(&limitation) {
            explanation.limitations.push(limitation);
        }
    }
    Ok(())
}
