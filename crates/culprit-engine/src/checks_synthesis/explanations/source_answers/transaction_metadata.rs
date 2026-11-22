//! A native overflow state is a transaction candidate, never proof that its current SQL caused the wait.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &TransactionMetadataAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
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
    let report = culprit_checks::transaction_metadata::explain(answer, point)?;
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
