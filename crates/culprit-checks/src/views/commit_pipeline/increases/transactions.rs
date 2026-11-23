//! An increase belongs to one selected native transaction lifetime, never to its current or previous SQL.
use super::*;
struct Point<'a> {
    original: &'a Original<'a>,
    context: NativeReadContext,
    reference: DiagnosticEvidenceRef,
    transaction: &'a MssqlCleanupTransaction,
}
pub(super) fn append(originals: &[Original<'_>], report: &mut CommitPipelineReport) -> Result<()> {
    let points = points(originals, &report.context)?;
    for after in &points {
        if !is_first_after(&points, after, &report.observed_at) {
            continue;
        }
        let Some(before) = last_before(&points, after, &report.observed_at) else {
            continue;
        };
        let (Some(after_bytes), Some(before_bytes)) =
            (after.transaction.log_bytes_used.as_deref(), before.transaction.log_bytes_used.as_deref())
        else {
            continue;
        };
        let Some(bytes) = increase(after_bytes, before_bytes) else {
            continue;
        };
        report.increases.push(CommitPipelineIncrease::TransactionLogBytes {
            session: after.transaction.session.clone(),
            transaction: after.transaction.transaction.clone(),
            before: Box::new(before.reference.clone()),
            after: Box::new(after.reference.clone()),
            bytes,
            actor: actor(originals, before, after)?.map(Box::new),
        });
    }
    Ok(())
}
fn points<'a>(originals: &'a [Original<'a>], primary: &NativeReadContext) -> Result<Vec<Point<'a>>> {
    let mut points = Vec::new();
    for original in originals {
        let Some(context) = context(original, primary).filter(|context| {
            context.database_writable.is_some() && context.database_writable == primary.database_writable
        }) else {
            continue;
        };
        let Some(header) =
            original.source.observation.snapshot().filter(|header| header.dataset == SnapshotDataset::Cleanup)
        else {
            continue;
        };
        let read = original.operational()?;
        for row in &original.rows {
            let TelemetryRecord::Operational(point) = row.record else {
                continue;
            };
            if read.source_coverage(&point.source).is_none_or(|source| source.state != SourceState::Collected) {
                continue;
            }
            let OperationalFact::MssqlCleanupHorizon(horizon) = &point.fact else {
                continue;
            };
            let Some(transaction) = horizon.oldest_confirmed.as_deref().filter(|point| point.log_bytes_used.is_some())
            else {
                continue;
            };
            if header.records == 0 {
                continue;
            }
            points.push(Point { original, context: context.clone(), reference: row.reference.clone(), transaction });
        }
    }
    Ok(points)
}
fn same_owner(left: &Point<'_>, right: &Point<'_>) -> bool {
    left.transaction.session == right.transaction.session
        && left.transaction.transaction == right.transaction.transaction
        && left.context.same_local_database_owner(&right.context)
        && left.context.database_writable == right.context.database_writable
}
fn is_first_after(points: &[Point<'_>], wanted: &Point<'_>, at: &str) -> bool {
    let candidates = points
        .iter()
        .filter(|point| same_owner(point, wanted) && ObservationBounds::new(at, &point.context.observed_from).is_some())
        .collect::<Vec<_>>();
    let Some(first) = candidates
        .iter()
        .min_by(|left, right| instant(&left.context.observed_from).cmp(&instant(&right.context.observed_from)))
    else {
        return false;
    };
    first.reference == wanted.reference
        && candidates
            .iter()
            .filter(|point| instant(&point.context.observed_from) == instant(&first.context.observed_from))
            .count()
            == 1
}
fn last_before<'p, 'o>(points: &'p [Point<'o>], after: &Point<'_>, at: &str) -> Option<&'p Point<'o>> {
    let candidates = points
        .iter()
        .filter(|before| {
            before.original.original != after.original.original
                && same_owner(before, after)
                && after.context.matches_earlier_scope(&before.context)
                && ObservationBounds::new(&before.context.observed_until, at).is_some()
                && local_before(
                    &before.transaction.observed_time_without_offset,
                    &after.transaction.observed_time_without_offset,
                )
        })
        .collect::<Vec<_>>();
    let last = candidates
        .iter()
        .max_by(|left, right| instant(&left.context.observed_until).cmp(&instant(&right.context.observed_until)))?;
    (candidates
        .iter()
        .filter(|point| instant(&point.context.observed_until) == instant(&last.context.observed_until))
        .count()
        == 1)
        .then_some(*last)
}
fn actor(
    originals: &[Original<'_>],
    before: &Point<'_>,
    after: &Point<'_>,
) -> Result<Option<CommitPipelineProducerActor>> {
    let Some(window) = ObservationBounds::new(&before.context.observed_from, &after.context.observed_until) else {
        return Ok(None);
    };
    let mut candidates = Vec::new();
    for original in originals {
        if !original.read_complete {
            continue;
        }
        let Some(snapshot) = original.source.observation.activity().filter(|snapshot| {
            snapshot.coverage.complete_scope
                && snapshot.coverage.state == SourceState::Collected
                && window.contains(&snapshot.at)
        }) else {
            continue;
        };
        let context = NativeReadContext::from_activity(original.source)?;
        if context.target != after.context.target
            || context.partition != after.context.partition
            || context.database_writable != after.context.database_writable
        {
            continue;
        }
        let rows = original
            .rows
            .iter()
            .map(|row| match row.record {
                TelemetryRecord::Activity(observation) => {
                    Ok(ActivityFrameRow { observation, reference: Some(&row.reference) })
                }
                _ => Err(invalid()),
            })
            .collect::<Result<Vec<_>>>()?;
        let frame = ActivityFrameEvidence::new(original.source, &rows, true)?;
        for row in &original.rows {
            let TelemetryRecord::Activity(actor) = row.record else {
                continue;
            };
            if actor.idle
                && actor.key.request.is_none()
                && actor.key.session == after.transaction.session
                && actor.key.transaction.as_deref() == Some(after.transaction.transaction.id.as_str())
                && frame.has_session_generation_authority(&actor.key.session)
                && snapshot.database_writable == context.database_writable
            {
                candidates
                    .push(CommitPipelineProducerActor { actor: actor.key.clone(), reference: row.reference.clone() });
            }
        }
    }
    Ok(if candidates.len() == 1 { candidates.pop() } else { None })
}
