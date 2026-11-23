//! Unique source-capture brackets precede exact native population matching; no older agreeable fallback.
use super::*;
use CommitPipelineIntervalIssue as Issue;

pub(super) fn append(frames: &[Frame<'_>], report: &mut CommitPipelineReport) -> Result<()> {
    let at = instant(&report.observed_at).ok_or_else(invalid)?;
    if frames.iter().any(|frame| {
        instant(frame.observed_from).is_some_and(|from| from < at)
            && instant(frame.observed_until).is_some_and(|until| until > at)
    }) {
        gap(report, None, None, None, Issue::ObservationOrder);
        return Ok(());
    }
    let before = frames
        .iter()
        .filter(|frame| instant(frame.observed_until).is_some_and(|until| until <= at))
        .collect::<Vec<_>>();
    let after =
        frames.iter().filter(|frame| instant(frame.observed_from).is_some_and(|from| from >= at)).collect::<Vec<_>>();
    let first = before.iter().max_by_key(|frame| instant(frame.observed_until)).copied();
    let last = after.iter().min_by_key(|frame| instant(frame.observed_from)).copied();
    let (Some(first), Some(last)) = (first, last) else {
        let reason = if first
            .into_iter()
            .chain(last)
            .any(|frame| frame.context.as_ref().is_none_or(|context| !compatible_owner(context, &report.context)))
        {
            Issue::NativeOwnerChanged
        } else {
            Issue::MissingEndpoint
        };
        frame_gap(report, first, last, reason);
        return Ok(());
    };
    if before.iter().filter(|frame| instant(frame.observed_until) == instant(first.observed_until)).count() != 1
        || after.iter().filter(|frame| instant(frame.observed_from) == instant(last.observed_from)).count() != 1
    {
        frame_gap(report, Some(first), Some(last), Issue::AmbiguousEndpoint);
        return Ok(());
    }
    if first.original.original == last.original.original
        || ObservationBounds::new(first.observed_until, last.observed_from).is_none()
        || instant(first.observed_until) >= instant(last.observed_until)
    {
        gap(report, None, None, None, Issue::ObservationOrder);
        return Ok(());
    }
    if !first.original.read_complete || !last.original.read_complete {
        frame_gap(report, Some(first), Some(last), Issue::IncompleteRead);
        return Ok(());
    }
    if [first, last]
        .iter()
        .any(|frame| frame.coverage.state != SourceState::Collected || !frame.coverage.complete_scope)
    {
        frame_gap(report, Some(first), Some(last), Issue::IncompleteSource);
        return Ok(());
    }
    let (Some(first_context), Some(last_context)) = (&first.context, &last.context) else {
        frame_gap(report, Some(first), Some(last), Issue::NativeOwnerChanged);
        return Ok(());
    };
    if !compatible_owner(first_context, &report.context) || !compatible_owner(last_context, &report.context) {
        frame_gap(report, Some(first), Some(last), Issue::NativeOwnerChanged);
        return Ok(());
    }
    let mut identities = BTreeSet::new();
    for point in first.points.iter().chain(&last.points).filter(|point| !point.values.is_empty()) {
        identities.insert(point.set.identity.key()?);
    }
    for identity in identities {
        if report.intervals.len() == MAX_COMMIT_PIPELINE_FACTS {
            gap(report, None, None, None, Issue::PresentationLimited);
            report.facts_limited = true;
            break;
        }
        let a = first
            .points
            .iter()
            .filter(|point| point.set.identity.key().is_ok_and(|key| key == identity))
            .collect::<Vec<_>>();
        let b = last
            .points
            .iter()
            .filter(|point| point.set.identity.key().is_ok_and(|key| key == identity))
            .collect::<Vec<_>>();
        let ([a], [b]) = (a.as_slice(), b.as_slice()) else {
            gap(
                report,
                a.first().map(|point| point.reference),
                b.first().map(|point| point.reference),
                None,
                Issue::CounterMissing,
            );
            continue;
        };
        if a.set.identity != b.set.identity {
            gap(report, Some(a.reference), Some(b.reference), None, Issue::CounterMeaningChanged);
            continue;
        }
        let Some(scope_basis) = scope(first, last, a.set.scope, b.set.scope, report) else { continue };
        let values = values::compare(a, b, first_context, report);
        if values.is_empty() {
            continue;
        }
        let mechanisms = values::mechanisms(&values);
        report.intervals.push(CommitPipelineInterval {
            before: a.reference.clone(),
            after: b.reference.clone(),
            native_source: first.native_source.into(),
            identity: a.set.identity.clone(),
            measurement_scope: a.set.scope,
            scope_basis,
            before_observed_from: first.observed_from.to_owned(),
            before_observed_until: first.observed_until.to_owned(),
            after_observed_from: last.observed_from.to_owned(),
            after_observed_until: last.observed_until.to_owned(),
            values,
            mechanisms,
        });
    }
    Ok(())
}
fn scope(
    first: &Frame<'_>,
    last: &Frame<'_>,
    a: MeasurementScope,
    b: MeasurementScope,
    report: &mut CommitPipelineReport,
) -> Option<CommitPipelineIntervalScope> {
    // The selected pair's contexts were admitted before any counter identity was matched.
    let first_context = first.context.as_ref()?;
    let last_context = last.context.as_ref()?;
    let reason = if !first_context.has_qualified_local_owner()
        || !last_context.has_qualified_local_owner()
        || first_context.target != last_context.target
        || first_context.partition != last_context.partition
    {
        Some(Issue::NativeOwnerChanged)
    } else if a != b {
        Some(Issue::ScopeUnavailable)
    } else {
        None
    };
    if let Some(reason) = reason {
        frame_gap(report, Some(first), Some(last), reason);
        return None;
    }
    match (&first_context.target, a) {
        (CollectionTarget::Shared(owner), MeasurementScope::Server) if matches!(&first_context.partition,SourcePartition::InstanceLocal{member:Some(member),..}if member==owner) => {
            Some(CommitPipelineIntervalScope::SharedServerIndependentOfDatabaseRole)
        }
        (CollectionTarget::Database(_), MeasurementScope::Database) => {
            if first_context.database_writable.is_none()
                || last_context.database_writable.is_none()
                || report.context.database_writable.is_none()
            {
                frame_gap(report, Some(first), Some(last), Issue::DatabaseRoleUnknown);
                None
            } else if first_context.database_writable != last_context.database_writable
                || first_context.database_writable != report.context.database_writable
            {
                frame_gap(report, Some(first), Some(last), Issue::DatabaseRoleChanged);
                None
            } else if !first_context.same_local_database_owner(&report.context)
                || !last_context.same_local_database_owner(&report.context)
            {
                frame_gap(report, Some(first), Some(last), Issue::NativeOwnerChanged);
                None
            } else {
                Some(CommitPipelineIntervalScope::LocalDatabase { database_writable: first_context.database_writable? })
            }
        }
        _ => {
            frame_gap(report, Some(first), Some(last), Issue::ScopeUnavailable);
            None
        }
    }
}
