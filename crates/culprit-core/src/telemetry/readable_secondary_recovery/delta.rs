//! Compare only native cancellation counters under one qualified readonly process and known reset.
use super::*;
use crate::ResetMarker;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum RecoveryConflictDeltaGap {
    MissingPoints,
    UnorderedClocks,
    IncompatibleNativeIdentity,
    SnapshotModeUnknown,
    MissingCounter,
    UnknownReset,
    ResetChanged,
    CounterDecreased,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum RecoveryConflictChange {
    Observed {
        kind: PostgresRecoveryConflictKind,
        from: String,
        until: String,
        reset_at: String,
        before: String,
        after: String,
        change: String,
    },
    Unavailable {
        kind: PostgresRecoveryConflictKind,
        reason: RecoveryConflictDeltaGap,
    },
}
impl ReadableSecondaryRecoveryPoint {
    pub fn conflict_changes(&self) -> Result<Vec<RecoveryConflictChange>> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let ReadableSecondaryRecoveryNative::Postgres { before, after } = &self.native else { return Ok(vec![]) };
        match before.as_ref().zip(after.as_ref()) {
            Some((before, after)) => after.changes_since(before),
            None => Ok(PostgresRecoveryConflictKind::ALL
                .iter()
                .map(|kind| RecoveryConflictChange::Unavailable {
                    kind: *kind,
                    reason: RecoveryConflictDeltaGap::MissingPoints,
                })
                .collect()),
        }
    }
}
impl PostgresRecoveryEndpoint {
    /// Independent native points retain their own clocks; callers keep their original capture provenance.
    pub fn changes_since(&self, before: &Self) -> Result<Vec<RecoveryConflictChange>> {
        if !self.is_valid() || !before.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let (a, b) = (before, self);
        Ok(PostgresRecoveryConflictKind::ALL
            .iter()
            .map(|kind| {
                let observed = || -> std::result::Result<RecoveryConflictChange, RecoveryConflictDeltaGap> {
                    if !ObservationBounds::new(&a.observed_at, &b.observed_at)
                        .is_some_and(|bounds| bounds.has_positive_duration())
                    {
                        return Err(RecoveryConflictDeltaGap::UnorderedClocks);
                    }
                    if a.conflicts.identity != b.conflicts.identity {
                        return Err(RecoveryConflictDeltaGap::IncompatibleNativeIdentity);
                    }
                    if a.stats_fetch_consistency != b.stats_fetch_consistency
                        || !matches!(a.stats_fetch_consistency.as_str(), "cache" | "snapshot")
                    {
                        return Err(RecoveryConflictDeltaGap::SnapshotModeUnknown);
                    }
                    let first = counter(a, *kind).ok_or(RecoveryConflictDeltaGap::MissingCounter)?;
                    let last = counter(b, *kind).ok_or(RecoveryConflictDeltaGap::MissingCounter)?;
                    let (ResetMarker::At(left), ResetMarker::At(right)) = (&first.reset, &last.reset) else {
                        return Err(RecoveryConflictDeltaGap::UnknownReset);
                    };
                    if left != right {
                        return Err(RecoveryConflictDeltaGap::ResetChanged);
                    }
                    let (MeasurementValue::Integer(earlier), MeasurementValue::Integer(later)) =
                        (&first.value, &last.value)
                    else {
                        return Err(RecoveryConflictDeltaGap::MissingCounter);
                    };
                    let a_count = earlier.parse::<u64>().map_err(|_| RecoveryConflictDeltaGap::MissingCounter)?;
                    let b_count = later.parse::<u64>().map_err(|_| RecoveryConflictDeltaGap::MissingCounter)?;
                    let change = b_count.checked_sub(a_count).ok_or(RecoveryConflictDeltaGap::CounterDecreased)?;
                    Ok(RecoveryConflictChange::Observed {
                        kind: *kind,
                        from: a.observed_at.clone(),
                        until: b.observed_at.clone(),
                        reset_at: left.clone(),
                        before: earlier.clone(),
                        after: later.clone(),
                        change: change.to_string(),
                    })
                };
                observed().unwrap_or_else(|reason| RecoveryConflictChange::Unavailable { kind: *kind, reason })
            })
            .collect())
    }
}
fn counter(endpoint: &PostgresRecoveryEndpoint, kind: PostgresRecoveryConflictKind) -> Option<&NativeCounterSample> {
    endpoint.conflicts.counters.iter().find(|counter| {
        counter.native_name == kind.native_name()
            && counter.key == kind.native_name()
            && counter.native_object == "pg_stat_database_conflicts"
    })
}
