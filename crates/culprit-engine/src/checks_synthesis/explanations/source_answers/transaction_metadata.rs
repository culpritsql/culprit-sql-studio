//! A native overflow state is a transaction candidate, never proof that its current SQL caused the wait.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    answer: &TransactionMetadataAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let point = resolved_point(originals, answer)?;
    retain_recorded_points(originals, episode, explanation)?;
    let inputs = comparison_inputs(originals, episode, answer)?;
    append_continuation_context(originals, episode, explanation)?;
    if continuation_stage(episode).is_some()
        && let Some(point) = point
    {
        explanation
            .limitations
            .push(t!("view.transaction_metadata.continuation.original", at = point.observed_at.as_str()));
    }
    let report = culprit_checks::transaction_metadata::explain(answer, point, &inputs)?;
    for change in &report.slru_changes {
        let PostgresSlruChange::Observed { before, after, .. } = change else { continue };
        for reference in [before.as_ref(), after.as_ref()] {
            if !explanation.context_evidence.iter().any(|evidence| evidence.reference.as_ref() == Some(reference)) {
                explanation.context_evidence.push(IncidentHistoryEvidence {
                    label: t!("view.transaction_metadata.slru.evidence"),
                    capture: reference.capture_ref(),
                    reference: Some(reference.clone()),
                    availability: t!("view.transaction_metadata.slru.authenticated"),
                });
            }
        }
    }
    explanation.mechanism = Some(report.summary);
    explanation.action = Some(report.investigation);
    explanation.verify = Some(report.verify);
    for limitation in report.limitations {
        if !explanation.limitations.contains(&limitation) {
            explanation.limitations.push(limitation);
        }
    }
    if let (Some(point), Some(reference)) = (point, &answer.observation) {
        for cache in &point.caches {
            if cache.overflowed != Some(true)
                || !report.observed_overflows.contains(&cache.actor.key.session)
                || cache.actor.key.request.is_none()
            {
                continue;
            }
            explanation.candidates.push(queries::from_qualified_request(&cache.actor, reference.clone()));
        }
    }
    // No native dependency/row-lock edge in this question identifies an exclusive or contributing culprit.
    Ok(())
}

fn slru_inputs<'a>(
    originals: &[&'a dyn OriginalProjection],
) -> Result<Vec<culprit_checks::transaction_metadata::SlruOriginal<'a>>> {
    let mut inputs = Vec::new();
    for original in originals.iter().copied().filter(|original|matches!(&original.source().observation,SourceObservation::SourceRead{snapshot,..} if snapshot.dataset==SnapshotDataset::TransactionMetadata)) {
        for (position,row) in original.records().iter().enumerate() {
            if !matches!(row.row().record,TelemetryRecord::Operational(OperationalObservation{fact:OperationalFact::PostgresTransactionMetadata(_),..})) {return Err(invalid());}
            inputs.push(culprit_checks::transaction_metadata::SlruOriginal{
                source:original.source(),reference:original.reference(position)?,record:&row.row().record,read_complete:original.is_read_complete(),
            });
        }
    }
    Ok(inputs)
}

fn resolved_point<'a>(
    originals: &[&'a dyn OriginalProjection],
    answer: &TransactionMetadataAnswer,
) -> Result<Option<&'a PostgresTransactionMetadata>> {
    let selected = answer.observation.as_ref().map(|reference| resolve(originals, reference)).transpose()?.flatten();
    let point = selected
        .map(|(original, position)| {
            if original.original() != &answer.result
                || original.source().observation.execution_context() != Some(&answer.result_context)
            {
                return Err(invalid());
            }
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::PostgresTransactionMetadata(point),
                ..
            }) = &original.records()[position].row().record
            else {
                return Err(invalid());
            };
            if !point.fits_source_scope(original.source()) {
                return Err(invalid());
            }
            Ok(point.as_ref())
        })
        .transpose()?;
    Ok(point)
}
fn retain_recorded_points(
    originals: &[&dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    explanation: &mut Explanation,
) -> Result<()> {
    for stage in episode.source_reads.iter() {
        let SourceReadStageState::Answered { answer } = &stage.state else {
            continue;
        };
        let SourceReadAnswer::TransactionMetadata(answer) = answer.as_ref() else {
            continue;
        };
        if resolved_point(originals, answer)?.is_none() {
            continue;
        }
        let Some(reference) = &answer.observation else {
            continue;
        };
        if !explanation.context_evidence.iter().any(|row| row.reference.as_ref() == Some(reference)) {
            explanation.context_evidence.push(IncidentHistoryEvidence {
                label: t!("view.transaction_metadata.slru.evidence"),
                capture: reference.capture_ref(),
                reference: Some(reference.clone()),
                availability: t!("view.transaction_metadata.slru.authenticated"),
            });
        }
    }
    Ok(())
}

fn comparison_inputs<'a>(
    originals: &[&'a dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    initial: &TransactionMetadataAnswer,
) -> Result<Vec<culprit_checks::transaction_metadata::SlruOriginal<'a>>> {
    let inputs = slru_inputs(originals)?;
    let Some(later) = continuation_answer(episode) else {
        return Ok(inputs);
    };
    let Some(second) = &later.observation else {
        return Ok(Vec::new());
    };
    // The explicit bounded continuation pairs only these exact originals, never a more convenient ambient point.
    Ok(inputs
        .into_iter()
        .filter(|input| initial.observation.as_ref() == Some(&input.reference) || second == &input.reference)
        .collect())
}
fn continuation_stage(episode: &DiagnosticEpisode) -> Option<&SourceReadStage> {
    let stage = episode.source_reads.latest(SourceReadQuestion::TransactionMetadata)?;
    matches!(stage.focus.prerequisite, SourceReadPrerequisite::ObservedTransactionMetadataContinuation { .. })
        .then_some(stage)
}
fn continuation_answer(episode: &DiagnosticEpisode) -> Option<&TransactionMetadataAnswer> {
    let SourceReadStageState::Answered { answer } = &continuation_stage(episode)?.state else {
        return None;
    };
    let SourceReadAnswer::TransactionMetadata(answer) = answer.as_ref() else {
        return None;
    };
    Some(answer)
}
fn append_continuation_context(
    originals: &[&dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    explanation: &mut Explanation,
) -> Result<()> {
    let Some(later) = continuation_answer(episode) else {
        return Ok(());
    };
    explanation.limitations.push(t!(
        "view.transaction_metadata.continuation.recorded",
        from = later.result_context.observed_from.as_str(),
        until = later.result_context.observed_until.as_str(),
        state = match later.source_state {
            culprit_core::SourceState::Collected => "collected",
            culprit_core::SourceState::Partial => "partial",
            culprit_core::SourceState::Empty => "empty",
            culprit_core::SourceState::Unavailable => "unavailable",
            culprit_core::SourceState::NotApplicable => "not-applicable",
        }
    ));
    explanation.limitations.push(t!(
        "view.transaction_metadata.continuation.request",
        end = match later.request_end {
            Some(BlockingRequestState::SameStatement) => "same",
            Some(BlockingRequestState::Changed) => "changed",
            Some(BlockingRequestState::Missing) => "missing",
            _ => "unavailable",
        }
    ));
    if resolved_point(originals, later)?.is_none() {
        explanation.limitations.push(t!("view.transaction_metadata.continuation.unavailable"));
    }
    Ok(())
}
