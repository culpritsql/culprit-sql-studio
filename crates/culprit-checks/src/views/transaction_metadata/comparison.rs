//! Retained points supply cluster-shared counters only. Actor and xmin context stays in the exact answer.
use super::*;
use culprit_core::{
    CounterTransition, MAX_DERIVED_CAPTURES, MeasurementContext, MeasurementPeriod, MeasurementScope,
    MeasurementSource, ResetMarker,
};
use std::collections::BTreeSet;

/// Pure borrowed input; the caller authenticates capture/receipt/EOF through its existing read owner.
/// This is not a capability token and cannot admit native work.
pub struct SlruOriginal<'a> {
    pub source: &'a CollectionDescription,
    pub reference: DiagnosticEvidenceRef,
    pub record: &'a TelemetryRecord,
    pub read_complete: bool,
}
struct Point<'a> {
    original: &'a SlruOriginal<'a>,
    context: NativeReadContext,
    point: &'a PostgresTransactionMetadata,
    state: SourceState,
    from: String,
    until: String,
}
impl SlruOriginal<'_> {
    fn point(&self) -> Result<Point<'_>> {
        self.reference.validate_canonical_record(self.source, self.record)?;
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::PostgresTransactionMetadata(point),
            ..
        }) = self.record
        else {
            return Err(invalid());
        };
        if !point.fits_source_scope(self.source) {
            return Err(invalid());
        }
        let context = SourceReadExecutionScope::context_from_source(self.source)?;
        let state = self.source.observation.snapshot().ok_or_else(invalid)?.coverage_state()?;
        let from = canonical_time(&context.observed_from)?;
        let until = canonical_time(&context.observed_until)?;
        Ok(Point { original: self, context, point, state, from, until })
    }
}
pub(super) fn compare(
    answer: &TransactionMetadataAnswer,
    originals: &[SlruOriginal<'_>],
) -> Result<Vec<PostgresSlruChange>> {
    if originals.len() > MAX_DERIVED_CAPTURES {
        return Err(invalid());
    }
    let mut captures = BTreeSet::new();
    let mut points = Vec::new();
    for original in originals {
        if !captures.insert(original.reference.capture_ref().key()?) {
            return Err(invalid());
        }
        points.push(original.point()?);
    }
    let current = answer
        .observation
        .as_ref()
        .and_then(|reference| points.iter().find(|point| point.original.reference == *reference));
    if current.is_some_and(|point| point.context != answer.result_context || point.state != answer.source_state) {
        return Err(invalid());
    }
    Ok([PostgresSlruKind::Subtransaction, PostgresSlruKind::MultiXactMember, PostgresSlruKind::MultiXactOffset]
        .into_iter()
        .map(|kind| {
            comparison(&points, current, kind).unwrap_or_else(|reason| PostgresSlruChange::Unavailable { kind, reason })
        })
        .collect())
}
fn comparison(
    points: &[Point<'_>],
    current: Option<&Point<'_>>,
    kind: PostgresSlruKind,
) -> std::result::Result<PostgresSlruChange, PostgresSlruChangeGap> {
    use PostgresSlruChangeGap as Gap;
    let current = current.ok_or(Gap::MissingPoint)?;
    if !current.original.read_complete {
        return Err(Gap::UnreadBody);
    }
    let mut candidates = points
        .iter()
        .filter(|point| point.original.reference.capture != current.original.reference.capture)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(Gap::MissingPoint);
    }
    candidates.retain(|point| point.context.same_local_database_owner(&current.context));
    if candidates.is_empty() {
        return Err(Gap::IncompatibleOwner);
    }
    candidates.retain(|point| {
        point.context.database_writable.is_some()
            && point.context.database_writable == current.context.database_writable
    });
    if candidates.is_empty() {
        return Err(Gap::RoleChanged);
    }
    let (before_point, after_point) = endpoints(&candidates, current)?;
    if !before_point.original.read_complete || !after_point.original.read_complete {
        return Err(Gap::UnreadBody);
    }
    let before = before_point.point.slrus.iter().find(|row| row.kind == kind).ok_or(Gap::MissingPoint)?;
    let after = after_point.point.slrus.iter().find(|row| row.kind == kind).ok_or(Gap::MissingPoint)?;
    if before.native_name != after.native_name {
        return Err(Gap::IncompatibleOwner);
    }
    let reset = continued_reset(before, after, before_point, after_point)?;
    Ok(PostgresSlruChange::Observed {
        kind,
        native_name: after.native_name.clone(),
        scope: MeasurementScope::Server,
        before: Box::new(before_point.original.reference.clone()),
        after: Box::new(after_point.original.reference.clone()),
        before_context: Box::new(before_point.context.clone()),
        after_context: Box::new(after_point.context.clone()),
        observed_from: before.observed_at.clone(),
        observed_until: after.observed_at.clone(),
        stats_reset: reset,
        before_state: before_point.state,
        after_state: after_point.state,
        counters: counter_changes(before, after),
    })
}

fn continued_reset(
    before: &PostgresSlruCounters,
    after: &PostgresSlruCounters,
    before_point: &Point<'_>,
    after_point: &Point<'_>,
) -> std::result::Result<String, PostgresSlruChangeGap> {
    use PostgresSlruChangeGap as Gap;
    let (Some(first_reset), Some(last_reset)) = (&before.stats_reset, &after.stats_reset) else {
        return Err(Gap::UnknownReset);
    };
    let first_reset = canonical_time(first_reset).map_err(|_| Gap::ResetClock)?;
    let last_reset = canonical_time(last_reset).map_err(|_| Gap::ResetClock)?;
    if !ObservationBounds::new(&before.observed_at, &after.observed_at)
        .is_some_and(|bounds| bounds.has_positive_duration())
    {
        return Err(Gap::UnorderedClocks);
    }
    if ObservationBounds::new(&first_reset, &before.observed_at).is_none()
        || ObservationBounds::new(&last_reset, &after.observed_at).is_none()
    {
        return Err(Gap::ResetClock);
    }
    if first_reset != last_reset {
        return Err(Gap::ResetChanged);
    }
    // Native reset equality is stricter than the shared display-timestamp tolerance.
    // Counter chronology and ownership still use the existing measurement transition owner.
    match measurement(before, &before_point.context)
        .ok_or(Gap::IncompatibleOwner)?
        .counter_transition(&measurement(after, &after_point.context).ok_or(Gap::IncompatibleOwner)?)
    {
        CounterTransition::Continued => Ok(first_reset),
        CounterTransition::Unmarked => Err(Gap::UnknownReset),
        _ => Err(Gap::UnorderedClocks),
    }
}
fn measurement(row: &PostgresSlruCounters, context: &NativeReadContext) -> Option<MeasurementContext> {
    Some(MeasurementContext {
        database: context.target.database()?.clone(),
        source: MeasurementSource::Named("pg_stat_slru".into()),
        population: Some(NativeIdentity::new(&row.native_name)),
        scope: MeasurementScope::Server,
        period: MeasurementPeriod::Cumulative { observed_at: row.observed_at.clone(), hours: None },
        reset: row.stats_reset.clone().map(ResetMarker::At).unwrap_or(ResetMarker::Unknown),
    })
}
fn counter_values(row: &PostgresSlruCounters) -> [(&'static str, Option<String>); 7] {
    [
        ("blks_zeroed", row.blocks_zeroed.clone()),
        ("blks_hit", row.blocks_hit.clone()),
        ("blks_read", row.blocks_read.clone()),
        ("blks_written", row.blocks_written.clone()),
        ("blks_exists", row.blocks_exists.clone()),
        ("flushes", row.flushes.clone()),
        ("truncates", row.truncates.clone()),
    ]
}
fn counter_changes(before: &PostgresSlruCounters, after: &PostgresSlruCounters) -> Vec<PostgresSlruCounterChange> {
    counter_values(before)
        .into_iter()
        .zip(counter_values(after))
        .map(|((name, before), (_, after))| {
            let change = before
                .as_ref()
                .zip(after.as_ref())
                .and_then(|(before, after)| after.parse::<u64>().ok()?.checked_sub(before.parse::<u64>().ok()?))
                .map(|value| value.to_string());
            PostgresSlruCounterChange { native_name: name.into(), before, after, change }
        })
        .collect()
}

fn endpoints<'a, 'p>(
    points: &[&'a Point<'p>],
    current: &'a Point<'p>,
) -> std::result::Result<(&'a Point<'p>, &'a Point<'p>), PostgresSlruChangeGap> {
    use PostgresSlruChangeGap as Gap;
    let earlier = points.iter().copied().filter(|point| point.until <= current.from).collect::<Vec<_>>();
    if let Some(latest) = earlier.iter().map(|point| &point.until).max() {
        let matching = earlier.iter().copied().filter(|point| point.until == *latest).collect::<Vec<_>>();
        return match matching.as_slice() {
            [before] => Ok((*before, current)),
            _ => Err(Gap::AmbiguousPoint),
        };
    }
    let later = points.iter().copied().filter(|point| point.from >= current.until).collect::<Vec<_>>();
    let Some(first) = later.iter().map(|point| &point.from).min() else { return Err(Gap::UnorderedClocks) };
    let matching = later.iter().copied().filter(|point| point.from == *first).collect::<Vec<_>>();
    match matching.as_slice() {
        [after] => Ok((current, *after)),
        _ => Err(Gap::AmbiguousPoint),
    }
}

#[cfg(test)]
mod tests;
