use super::*;
use culprit_core::DerivedContextIssue as Issue;

pub(crate) fn dataset(original: &dyn OriginalProjection) -> Option<SnapshotDataset> {
    let dataset = original.source().observation.snapshot()?.dataset;
    matches!(
        dataset,
        SnapshotDataset::Tables
            | SnapshotDataset::Indexes
            | SnapshotDataset::Columns
            | SnapshotDataset::Statistics
            | SnapshotDataset::Settings
            | SnapshotDataset::ForeignKeys
            | SnapshotDataset::CheckConstraints
            | SnapshotDataset::Objects
            | SnapshotDataset::Triggers
            | SnapshotDataset::Partitions
    )
    .then_some(dataset)
}

pub(crate) fn same_scope(primary: &dyn OriginalProjection, original: &dyn OriginalProjection) -> bool {
    let Some(database) = primary.source().target.database() else { return false };
    if database.engine != culprit_core::Engine::Mssql
        || primary.source().target != original.source().target
        || !primary.source().partition_is_consistent()
        || !original.source().partition_is_consistent()
        || !primary.source().partition.is_qualified()
        || !original.source().partition.is_qualified()
    {
        return false;
    }
    if database.incarnation.is_none() {
        return same_local_catalog_owner(primary.source(), original.source());
    }
    if primary.source().partition == original.source().partition {
        return true;
    }
    // This is the recorded acquisition owner only, never a historical query executor/replica assignment.
    primary.source().query_history().and_then(|history| history.acquisition_owner.as_deref()).is_some_and(|owner| {
        owner.has_qualified_local_owner()
            && owner.target == original.source().target
            && owner.partition == original.source().partition
    })
}

fn same_local_catalog_owner(primary: &CollectionDescription, catalog: &CollectionDescription) -> bool {
    if !matches!(
        catalog.observation,
        SourceObservation::LocalResource { .. } | SourceObservation::SnapshotAcquisition { .. }
    ) {
        return false;
    }
    let Some(catalog_owner) = catalog
        .observation
        .snapshot_acquisition_context()
        .cloned()
        .or_else(|| NativeReadContext::from_snapshot(catalog).ok())
    else {
        return false;
    };
    let primary_owner = if let Some(history) = primary.query_history() {
        history.acquisition_owner.as_deref().cloned()
    } else if primary.observation.activity().is_some() {
        NativeReadContext::from_activity(primary).ok()
    } else {
        primary.observation.execution_context().cloned().or_else(|| NativeReadContext::from_snapshot(primary).ok())
    };
    // Local partitions encode the source-observed DB recovery/file identity as well as its member/process.
    // Known equal endpoint roles do not establish continuous role or historical execution ownership.
    primary_owner.is_some_and(|owner| {
        owner.same_local_database_owner(&catalog_owner)
            && owner.database_writable.is_some()
            && owner.database_writable == catalog_owner.database_writable
    })
}

pub(super) struct CatalogSelection<'a> {
    pub originals: Vec<(u16, &'a dyn OriginalProjection)>,
    pub issues: Vec<Issue>,
}

pub(super) fn heads<'a>(
    primary: &dyn OriginalProjection,
    originals: &[&'a dyn OriginalProjection],
) -> Result<CatalogSelection<'a>> {
    let as_of = validation::time(primary.collected_from())?;
    let mut heads: Vec<(SnapshotDataset, u16, &'a dyn OriginalProjection, bool)> = Vec::new();
    let mut issues = Vec::new();
    for (index, &original) in originals.iter().enumerate() {
        if original.capture() == primary.capture() {
            continue;
        }
        let Some(dataset) = dataset(original) else {
            continue;
        };
        if !same_scope(primary, original) {
            issue(&mut issues, Issue::UnqualifiedCatalogScope);
            continue;
        }
        let Ok(until) = validation::time(original.collected_until()) else {
            issue(&mut issues, Issue::UnknownCatalogClock);
            continue;
        };
        if until > as_of {
            issue(&mut issues, Issue::CatalogNotRetainedAsOf);
            continue;
        }
        let index = index.try_into().map_err(|_| invalid())?;
        if let Some(head) = heads.iter_mut().find(|head| head.0 == dataset) {
            let current = validation::time(head.2.collected_until())?;
            if until > current {
                *head = (dataset, index, original, false);
            } else if until == current {
                head.3 = true;
            }
        } else {
            heads.push((dataset, index, original, false));
        }
    }
    let mut selected = Vec::new();
    for (_, index, original, ambiguous) in heads {
        if ambiguous {
            issue(&mut issues, Issue::AmbiguousCatalog);
        } else {
            selected.push((index, original));
        }
    }
    Ok(CatalogSelection { originals: selected, issues })
}
fn issue(issues: &mut Vec<Issue>, issue: Issue) {
    if !issues.contains(&issue) {
        issues.push(issue);
    }
}
