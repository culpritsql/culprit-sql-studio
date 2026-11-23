//! Whole-census endpoint sums grant no file-incarnation, role-continuity or native-growth-event claim.
use super::*;
struct Point<'a> {
    original: &'a Original<'a>,
    context: NativeReadContext,
    summary: Box<StorageFileSummary>,
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
        let (Some(a), Some(b)) = (
            after.summary.log_catalog_allocated_bytes.as_deref(),
            before.summary.log_catalog_allocated_bytes.as_deref(),
        ) else {
            continue;
        };
        if let Some(bytes) = increase(a, b) {
            report.increases.push(CommitPipelineIncrease::CatalogLogAllocationBytes {
                before: before.original.original.clone(),
                after: after.original.original.clone(),
                bytes,
            });
        }
    }
    Ok(())
}
fn points<'a>(originals: &'a [Original<'a>], primary: &NativeReadContext) -> Result<Vec<Point<'a>>> {
    let mut points = Vec::new();
    for original in originals {
        let Some(context) = context(original, primary) else {
            continue;
        };
        let Some(header) = original.source.observation.snapshot().filter(|header| {
            header.dataset == SnapshotDataset::Operational
                && header.sources.iter().any(|source| source.source == source_id::FILE_CAPACITY)
        }) else {
            continue;
        };
        if header.records == 0 {
            continue;
        }
        let read = original.operational()?;
        let storage = StorageRead::from_source(original.source, read.observations, true)?;
        let Some(summary) = super::super::super::storage::explain(&storage)?.file_summary else {
            continue;
        };
        if summary.source_state == SourceState::Collected
            && summary.log_file_count.is_some()
            && summary.log_catalog_allocated_bytes.is_some()
            && summary.native_database_id.is_some()
            && summary.observed_at.as_deref().is_some_and(|at| instant(at).is_some())
        {
            points.push(Point { original, context, summary });
        }
    }
    Ok(points)
}
fn owner_matches(before: &NativeReadContext, after: &NativeReadContext) -> bool {
    before.same_local_database_owner(after)
        && ObservationBounds::new(&before.observed_until, &after.observed_from).is_some()
}
fn is_first_after(points: &[Point<'_>], wanted: &Point<'_>, at: &str) -> bool {
    let candidates = points
        .iter()
        .filter(|point| {
            point.context.target == wanted.context.target
                && point.context.partition == wanted.context.partition
                && ObservationBounds::new(at, &point.context.observed_from).is_some()
        })
        .collect::<Vec<_>>();
    let Some(first) = candidates
        .iter()
        .min_by(|left, right| instant(&left.context.observed_from).cmp(&instant(&right.context.observed_from)))
    else {
        return false;
    };
    first.original.original == wanted.original.original
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
                && owner_matches(&before.context, &after.context)
                && ObservationBounds::new(&before.context.observed_until, at).is_some()
                && before.summary.native_database_id == after.summary.native_database_id
                && before.summary.observed_at.as_deref().zip(after.summary.observed_at.as_deref()).is_some_and(
                    |(before, after)| instant(before).zip(instant(after)).is_some_and(|(before, after)| before < after),
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_owner_policy_preserves_unknown_or_changed_role_without_granting_native_admission() {
        let mut identity = DatabaseIdentity::server(
            Engine::Mssql,
            "sample",
            "fixture.invalid",
            1433,
            Some("original-database".into()),
        );
        identity.incarnation = Some("controlled-original-database-generation".into());
        let member = SharedResourceIdentity {
            engine: Engine::Mssql,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new("original-machine"),
            native: NativeIdentity::new("original-instance"),
        };
        let before = NativeReadContext {
            target: identity.into(),
            partition: SourcePartition::InstanceLocal { member: Some(member), epoch: Some("original-process".into()) },
            observed_from: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:00.5Z".into(),
            database_writable: None,
        };
        let mut after = before.clone();
        after.observed_from = "2020-01-01T12:00:01Z".into();
        after.observed_until = after.observed_from.clone();
        assert!(before.has_qualified_local_owner());
        assert!(owner_matches(&before, &after));
        assert!(!after.matches_earlier_scope(&before), "quantity policy cannot become native role/admission proof");
        let mut known_before = before.clone();
        known_before.database_writable = Some(true);
        after.database_writable = Some(false);
        assert!(owner_matches(&known_before, &after));
        assert!(!after.matches_earlier_scope(&known_before));
        let mut unidentified = after.clone();
        let CollectionTarget::Database(database) = &mut unidentified.target else { panic!() };
        database.incarnation = None;
        assert!(!owner_matches(&known_before, &unidentified), "route labels cannot replace an original incarnation");
        after.partition = SourcePartition::Target;
        assert!(!owner_matches(&known_before, &after));
    }
}
