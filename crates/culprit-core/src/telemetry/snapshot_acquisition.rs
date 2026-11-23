//! An ordinary snapshot's actual read owner is separate from its native data population and reset evidence.
use super::*;

impl SourceObservation {
    /// Completed original acquisition facts only. The caller still authenticates this source and its rows;
    /// public projections, pending headers and matching keys do not restore this private native context.
    pub fn snapshot_acquisition_context(&self) -> Option<&NativeReadContext> {
        match self {
            Self::SnapshotAcquisition { execution, .. } => Some(execution),
            _ => None,
        }
    }
    /// Original completed native decoder metadata only, independent of read ownership and public labels.
    pub fn snapshot_settings_context(&self) -> Option<&NativeSettingsContext> {
        match self {
            Self::SnapshotAcquisition { settings_context, .. } => settings_context.as_ref(),
            _ => None,
        }
    }
}

fn execution_is_valid(dataset: SnapshotDataset, from: &str, execution: &NativeReadContext) -> bool {
    matches!(
        dataset,
        SnapshotDataset::Settings | SnapshotDataset::StatementCounters | SnapshotDataset::StatementDefinitions
    ) && execution.has_qualified_local_owner()
        && execution.target.database().is_some()
        && execution.database_writable.is_some()
        && matches!(&execution.partition, SourcePartition::InstanceLocal { member: Some(member), .. }
            if member.kind == SharedResourceKind::Instance)
        && same_time(from, &execution.observed_from)
}
fn settings_context_is_valid(
    dataset: SnapshotDataset,
    execution: &NativeReadContext,
    settings_context: Option<&NativeSettingsContext>,
) -> bool {
    match (dataset, settings_context) {
        (SnapshotDataset::Settings, Some(context)) => context.is_valid_for(execution.target.engine()),
        (SnapshotDataset::Settings, None) => false,
        (_, None) => true,
        (_, Some(_)) => false,
    }
}
pub(super) fn completed_header_is_valid(
    snapshot: &StateSnapshotDescription,
    execution: &NativeReadContext,
    settings_context: Option<&NativeSettingsContext>,
) -> bool {
    snapshot.is_valid()
        && settings_context_is_valid(snapshot.dataset, execution, settings_context)
        && execution_is_valid(snapshot.dataset, &snapshot.observed_from, execution)
        && same_time(&snapshot.observed_until, &execution.observed_until)
}

pub(super) fn source_is_consistent(source: &CollectionDescription) -> Option<bool> {
    let (dataset, execution, valid) = match &source.observation {
        SourceObservation::PagedSnapshotAcquisition { snapshot, execution, settings_context } => (
            snapshot.dataset,
            execution,
            snapshot.is_valid()
                && execution_is_valid(snapshot.dataset, &snapshot.observed_from, execution)
                && settings_context_is_valid(snapshot.dataset, execution, settings_context.as_ref()),
        ),
        SourceObservation::SnapshotAcquisition { snapshot, execution, settings_context } => {
            (snapshot.dataset, execution, completed_header_is_valid(snapshot, execution, settings_context.as_ref()))
        }
        _ => return None,
    };
    let namespace_matches = match dataset {
        SnapshotDataset::Settings => source.partition == SourcePartition::Target,
        SnapshotDataset::StatementCounters | SnapshotDataset::StatementDefinitions => {
            source.partition == execution.partition
        }
        _ => false,
    };
    Some(valid && namespace_matches && source.target == execution.target)
}

pub(super) fn complete(
    source: &CollectionDescription,
    snapshot: StateSnapshotDescription,
    execution: Box<NativeReadContext>,
    settings_context: Option<NativeSettingsContext>,
) -> Result<CollectionDescription> {
    let SourceObservation::PagedSnapshotAcquisition {
        snapshot: declared,
        execution: initial,
        settings_context: initial_settings,
    } = &source.observation
    else {
        return Err(CollectionStop::InvalidPage.error());
    };
    if source_is_consistent(source) != Some(true)
        || !declared.accepts_completion(&snapshot)
        || initial_settings != &settings_context
        || !execution.completes_read(initial)
        || !completed_header_is_valid(&snapshot, &execution, settings_context.as_ref())
    {
        return Err(CollectionStop::InvalidPage.error());
    }
    let mut completed = source.clone();
    completed.observation = SourceObservation::SnapshotAcquisition { snapshot, execution, settings_context };
    if source_is_consistent(&completed) != Some(true) {
        return Err(CollectionStop::InvalidPage.error());
    }
    Ok(completed)
}

#[cfg(test)]
mod tests;
