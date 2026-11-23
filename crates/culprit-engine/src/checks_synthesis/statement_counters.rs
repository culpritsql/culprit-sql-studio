//! Counter-only originals retain an explicit application-analysis limitation instead of a healthy empty result.
//! The existing point interpreter and checked metric projector remain the only owners of native counter meaning.
use super::*;
use culprit_core::collected_analysis::CollectedAnalysisLimit;

pub(super) fn supports(original: &dyn OriginalProjection) -> bool {
    original.source().observation.snapshot().is_some_and(|header| header.dataset == SnapshotDataset::StatementCounters)
}

pub(super) fn evaluations(original: &dyn OriginalProjection) -> Result<DerivedFindingEvaluations> {
    let snapshot = snapshot(original)?;
    culprit_checks::statement_work::explain(&snapshot, original.is_read_complete())?;
    // These points contain no source-bound definition reference or native application classification. An adjacent
    // SQL capture, identical queryid or acquisition role cannot supply that authority or a database denominator.
    let mut limitations = vec![
        CollectedAnalysisLimit::MissingQueryDefinition,
        CollectedAnalysisLimit::UnknownStatementOrigin,
        CollectedAnalysisLimit::RuntimeDenominatorUnavailable,
        CollectedAnalysisLimit::ExecutionPopulationUnknown,
    ];
    if !original.is_read_complete() || !snapshot.coverage.complete_scope {
        limitations.push(CollectedAnalysisLimit::RuntimeBounds);
    }
    for row in &snapshot.observations {
        let OperationalFact::StatementCounters(sample) = &row.fact else { return Err(invalid()) };
        if StatementQueryMetrics::from_sample(sample)?.is_none() {
            let reason = if sample.totals.iter().any(|counter| {
                matches!(counter.key, RuntimeMetricKey::Executions | RuntimeMetricKey::Duration)
                    && counter.total.exact_real().is_none()
            }) {
                CollectedAnalysisLimit::CounterPrecision
            } else {
                CollectedAnalysisLimit::DurationUnavailable
            };
            if !limitations.contains(&reason) {
                limitations.push(reason);
            }
        }
    }
    let mut evaluations = evaluation::Evaluations::new();
    evaluations.add_statement_counters(limitations, original.source().target.engine())?;
    Ok(evaluations.finish())
}

fn snapshot(original: &dyn OriginalProjection) -> Result<StatementCounterSnapshot> {
    if !supports(original) || !original.original().is_valid() {
        return Err(invalid());
    }
    let observations = original
        .records()
        .iter()
        .map(|row| match &row.row().record {
            TelemetryRecord::Operational(observation)
                if matches!(observation.fact, OperationalFact::StatementCounters(_)) =>
            {
                Ok(observation.clone())
            }
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let snapshot = StatementCounterSnapshot::from_source(original.source(), observations)?;
    if !snapshot.is_valid_read(original.is_read_complete()) {
        return Err(invalid());
    }
    Ok(snapshot)
}
