//! The same queue interpreter projects ticket candidates; it cannot manufacture a native pending edge.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &MetadataLockContextAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    if !answer.is_valid() {
        return Err(invalid());
    }
    let mut matching = originals.iter().copied().filter(|original| original.original() == &answer.result);
    let Some(original) = matching.next() else {
        explanation.limitations.push(culprit_core::t!("view.metadata_queue.candidate.missing"));
        return Ok(());
    };
    if matching.next().is_some() || original.source().observation.execution_context() != Some(&answer.result_context) {
        return Err(invalid());
    }
    if !original.is_read_complete() {
        explanation.limitations.push(culprit_core::t!("view.metadata_queue.candidate.missing"));
        return Ok(());
    }
    let rows = original
        .records()
        .iter()
        .map(|row| match &row.row().record {
            TelemetryRecord::Activity(actor) => Ok(ActivityFrameRow { observation: actor, reference: None }),
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let report = culprit_checks::focused_blocking::explain(original.source(), &rows, original.is_read_complete())?;
    if report.metadata_queue.evidence != answer.evidence {
        return Err(invalid());
    }
    explanation.limitations.extend(report.metadata_queue.limitations);
    for case in &report.metadata_queue.evidence.candidates {
        let words =
            report.metadata_queue.explanations.iter().find(|words| words.key == case.key).ok_or_else(invalid)?;
        append_paragraph(&mut explanation.victim_summary, Some(words.victim_summary.clone()));
        append_paragraph(&mut explanation.mechanism, Some(words.mechanism.clone()));
        append_paragraph(&mut explanation.action, Some(words.investigation.clone()));
        append_paragraph(&mut explanation.verify, Some(words.verify.clone()));
        for (position, row) in original.records().iter().enumerate() {
            let TelemetryRecord::Activity(actor) = &row.row().record else {
                return Err(invalid());
            };
            if actor.key == case.affected {
                if let Some(query) = queries::from_record(original, position)? {
                    explanation.affected.push(query);
                }
            } else if case
                .pending_exclusive
                .iter()
                .chain(&case.granted_holders)
                .any(|candidate| candidate.actor == actor.key)
                && let Some(query) = queries::from_record(original, position)?
            {
                explanation.candidates.push(query);
            }
        }
    }
    // Original one-hop interpretations can retain their independent established roles. This context adds none.
    Ok(())
}
