//! Native stage words and selected producer candidates reuse the one pure view; no source acquisition/detector.
use super::*;
use culprit_checks::commit_pipeline::{Original, OriginalRow};

pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    episode: &DiagnosticEpisode,
    explanation: &mut Explanation,
) -> Result<()> {
    let Some((basis, position)) = resolve(originals, &episode.first)? else { return Ok(()) };
    let TelemetryRecord::Activity(actor) = &basis.records()[position].row().record else { return Ok(()) };
    let supported = actor.wait.as_ref().is_some_and(|wait| {
        wait.class == Some(ActivityWaitClass::LogRateGovernance)
            || wait
                .native_type
                .as_deref()
                .is_some_and(|native| basis.source().target.engine().spec().commit_wait_stage(native).is_some())
    });
    if !supported {
        return Ok(());
    };
    let inputs = inputs(originals, basis.original())?;
    let report = culprit_checks::commit_pipeline::explain(&inputs, &episode.first)?;
    let Some(mechanism) = &report.mechanism else {
        explanation.limitations.push(report.summary);
        explanation.limitations.extend(report.limitations);
        return Ok(());
    };
    let mut projected = Explanation::words(mechanism, &report.investigation, &report.verify);
    projected.victim_summary = Some(report.summary.clone());
    if let Some(query) = queries::from_record(basis, position)? {
        projected.affected.push(query);
    }
    for increase in &report.increases {
        let culprit_core::CommitPipelineIncrease::TransactionLogBytes { before, after, actor: Some(producer), .. } =
            increase
        else {
            continue;
        };
        let Some((original, position)) = resolve(originals, &producer.reference)? else { continue };
        let TelemetryRecord::Activity(actor) = &original.records()[position].row().record else {
            return Err(invalid());
        };
        if actor.key != producer.actor || actor.key.request.is_some() {
            return Err(invalid());
        }
        projected.candidates.push(DerivedIncidentQuery {
            causal_role: None,
            identity: Some(DerivedQueryIdentity::Activity { actor: actor.key.clone(), statement: None }),
            // An idle session's previous batch is not the statement that produced this transaction's log.
            sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            caller: DerivedCaller::from_activity(actor, &producer.reference)?,
            evidence: vec![before.as_ref().clone(), after.as_ref().clone(), producer.reference.clone()],
        });
    }
    projected.limitations = report.context_summaries;
    projected.limitations.extend(report.limitations);
    explanation.merge_followup(projected);
    Ok(())
}

// Keep the production and canonical-control inventory identical; unrelated catalogs are not commit context.
pub(super) fn inputs<'a>(
    originals: &[&'a dyn OriginalProjection],
    primary: &DiagnosticCaptureRef,
) -> Result<Vec<Original<'a>>> {
    originals
        .iter()
        .copied()
        .filter(|original| {
            original.original() == primary
                || original.source().observation.snapshot().is_some_and(|header| {
                    matches!(
                        header.dataset.stream(),
                        TelemetryStream::OperationalSamples | TelemetryStream::ReplicationSamples
                    )
                })
        })
        .map(|original| {
            Ok(Original {
                source: original.source(),
                original: original.original(),
                read_complete: original.is_read_complete(),
                rows: original
                    .records()
                    .iter()
                    .enumerate()
                    .map(|(position, row)| {
                        Ok(OriginalRow { record: &row.row().record, reference: original.reference(position)? })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect()
}
