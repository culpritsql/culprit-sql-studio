//! Per-quantity native reset/type/unit admission. No rate, ratio, per-commit estimate or missing-as-zero value.
use super::*;
use CommitPipelineIntervalIssue as Issue;
pub(super) fn compare(
    first: &Point<'_>,
    last: &Point<'_>,
    before_context: &NativeReadContext,
    report: &mut CommitPipelineReport,
) -> Vec<CommitPipelineIntervalValue> {
    let mut result = Vec::new();
    for after in &last.values {
        let matches = first.values.iter().filter(|before| before.kind == after.kind).collect::<Vec<_>>();
        let [before] = matches.as_slice() else {
            gap(report, Some(first.reference), Some(last.reference), Some(after.kind), Issue::CounterMissing);
            continue;
        };
        if last.values.iter().filter(|point| point.kind == after.kind).count() != 1 {
            gap(report, Some(first.reference), Some(last.reference), Some(after.kind), Issue::CounterMeaningChanged);
            continue;
        }
        let (a, b) = (&before.sample, &after.sample);
        let reason = if a.key != b.key
            || a.native_object != b.native_object
            || a.native_name != b.native_name
            || a.native_instance != b.native_instance
            || a.native_type != b.native_type
            || a.nature != CounterNature::Cumulative
            || b.nature != CounterNature::Cumulative
            || a.unit != b.unit
            || a.scope != b.scope
        {
            Some(Issue::CounterMeaningChanged)
        } else if a.reset == ResetMarker::Unknown || b.reset == ResetMarker::Unknown {
            Some(Issue::UnknownReset)
        } else if !same_reset(&a.reset, &b.reset) {
            Some(Issue::ChangedReset)
        } else if matches!(&a.reset, ResetMarker::At(at) if instant(at).zip(instant(&before_context.observed_until)).is_none_or(|(reset,observed)|reset>observed))
        {
            Some(Issue::ResetAfterObservation)
        } else {
            None
        };
        if let Some(reason) = reason {
            gap(report, Some(first.reference), Some(last.reference), Some(after.kind), reason);
            continue;
        }
        let Some(value) = difference(&a.value, &b.value, a.unit) else {
            let decreased = a.value.exact_real().zip(b.value.exact_real()).is_some_and(|(a, b)| b < a)
                || a.value.integer().zip(b.value.integer()).is_some_and(|(a, b)| b < a);
            gap(
                report,
                Some(first.reference),
                Some(last.reference),
                Some(after.kind),
                if decreased { Issue::CounterDecreased } else { Issue::ValueUnavailable },
            );
            continue;
        };
        result.push(CommitPipelineIntervalValue { kind: after.kind, value, reset: a.reset.clone() });
    }
    for before in &first.values {
        if !last.values.iter().any(|after| after.kind == before.kind) {
            gap(report, Some(first.reference), Some(last.reference), Some(before.kind), Issue::CounterMissing);
        }
    }
    result
}
fn same_reset(a: &ResetMarker, b: &ResetMarker) -> bool {
    match (a, b) {
        (ResetMarker::At(a), ResetMarker::At(b)) => instant(a).zip(instant(b)).is_some_and(|(a, b)| a == b),
        (ResetMarker::Generation(a), ResetMarker::Generation(b)) => a == b,
        _ => false,
    }
}
fn difference(before: &MeasurementValue, after: &MeasurementValue, unit: CounterUnit) -> Option<MeasurementValue> {
    if let (MeasurementValue::Integer(before), MeasurementValue::Integer(after)) = (before, after) {
        return Some(MeasurementValue::Integer(
            after.parse::<u64>().ok()?.checked_sub(before.parse::<u64>().ok()?)?.to_string(),
        ));
    }
    if unit != CounterUnit::Milliseconds {
        return None;
    }
    let (before, after) = (before.exact_real()?, after.exact_real()?);
    let delta = after - before;
    (delta.is_finite() && delta >= 0.0).then_some(MeasurementValue::Real(delta))
}
pub(super) fn mechanisms(values: &[CommitPipelineIntervalValue]) -> Vec<CommitPipelineIntervalMechanism> {
    use CommitPipelineCounterKind as Kind;
    use CommitPipelineIntervalMechanism as Mechanism;
    let positive = |kind| {
        values.iter().any(|value| {
            value.kind == kind
                && match &value.value {
                    MeasurementValue::Integer(value) => value.parse::<u64>().is_ok_and(|value| value > 0),
                    MeasurementValue::Real(value) => *value > 0.0,
                }
        })
    };
    let mut result = Vec::new();
    for (kind, mechanism) in [
        (Kind::BufferFullWrites, Mechanism::NativeBufferFullWrites),
        (Kind::LogBufferWaits, Mechanism::NativeLogBufferWaits),
        (Kind::WriteOperations, Mechanism::LocalWriteWork),
        (Kind::SyncOperations, Mechanism::LocalSyncWork),
        (Kind::FlushOperations, Mechanism::NativeFlushWork),
    ] {
        if positive(kind) {
            result.push(mechanism);
        }
    }
    result
}
