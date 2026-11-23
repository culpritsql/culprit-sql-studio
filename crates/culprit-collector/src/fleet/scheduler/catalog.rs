//! Stable catalog deadlines may yield briefly to admitted focused work; live observations remain hard.
use super::*;

const MAX_CATALOG_GRACE: Duration = Duration::from_secs(300);

pub(super) fn grace(schedule: &FleetSchedule) -> Duration {
    match schedule.collection {
        ScheduledCollection::Snapshot {
            dataset:
                SnapshotDataset::Settings
                | SnapshotDataset::Tables
                | SnapshotDataset::Columns
                | SnapshotDataset::Indexes
                | SnapshotDataset::ForeignKeys
                | SnapshotDataset::CheckConstraints
                | SnapshotDataset::Objects
                | SnapshotDataset::Triggers
                | SnapshotDataset::Partitions,
        } => (Duration::from_secs(u64::from(schedule.every_seconds)) / 2).min(MAX_CATALOG_GRACE),
        _ => Duration::ZERO,
    }
}

pub(super) fn deadline(schedule: &FleetSchedule, due: Duration) -> Result<Duration> {
    due.checked_add(grace(schedule)).ok_or_else(state_failed)
}
