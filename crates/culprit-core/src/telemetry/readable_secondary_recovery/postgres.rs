//! Native PostgreSQL standby cancellations and startup-process points; no ended SQL is reconstructed.
use super::*;
use crate::blocking::{
    MAX_FOCUSED_LOCK_SNAPSHOT_ROWS, MAX_NATIVE_RELATION_LOCK_MODES, PostgresRelationLockMode, PostgresRelationLockTag,
};
pub const MAX_RECOVERY_RELATION_EDGES: usize = 1;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresRecoveryRelationEdge {
    pub resource: PostgresRelationLockTag,
    pub requested_mode: PostgresRelationLockMode,
    pub reader: NativeActivityIdentity,
    pub reader_single_backend: bool,
    pub granted_modes: Vec<PostgresRelationLockMode>,
}
impl PostgresRecoveryRelationEdge {
    fn is_valid(&self) -> bool {
        self.resource.is_valid()
            && self.reader.is_valid()
            && self.reader.id.parse::<i32>().is_ok_and(|id| id > 0 && id.to_string() == self.reader.id)
            && canonical_time(&self.reader.generation).is_ok()
            && bounded_name(self.requested_mode.native(), 128)
            && self.granted_modes.len() <= MAX_NATIVE_RELATION_LOCK_MODES
            && self.granted_modes.iter().all(|mode| bounded_name(mode.native(), 128))
            && self.granted_modes.iter().collect::<BTreeSet<_>>().len() == self.granted_modes.len()
    }
    pub fn blocks(&self, original: &ActivityRequestFocus, database_oid: &str) -> bool {
        self.is_valid()
            && self.reader_single_backend
            && self.reader == original.original.key.session
            && self.resource.database_oid == database_oid
            && !self.granted_modes.is_empty()
            && self
                .granted_modes
                .iter()
                .map(|mode| self.requested_mode.conflicts_with(mode))
                .collect::<Option<Vec<_>>>()
                .is_some_and(|conflicts| conflicts.into_iter().any(|conflict| conflict))
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PostgresRecoveryConflictKind {
    Tablespace,
    Lock,
    Snapshot,
    BufferPin,
    Deadlock,
    LogicalSlot,
}
impl PostgresRecoveryConflictKind {
    pub const ALL: &'static [Self] =
        &[Self::Tablespace, Self::Lock, Self::Snapshot, Self::BufferPin, Self::Deadlock, Self::LogicalSlot];
    pub fn native_name(self) -> &'static str {
        match self {
            Self::Tablespace => "confl_tablespace",
            Self::Lock => "confl_lock",
            Self::Snapshot => "confl_snapshot",
            Self::BufferPin => "confl_bufferpin",
            Self::Deadlock => "confl_deadlock",
            Self::LogicalSlot => "confl_active_logicalslot",
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresRecoveryProcess {
    pub pid: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub started_at: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub database_oid: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_type: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_event: Option<String>,
    pub lock_snapshot_rows: u32,
    pub edges_limited: bool,
    pub relation_edges: Vec<PostgresRecoveryRelationEdge>,
}
impl PostgresRecoveryProcess {
    pub fn proves_relation_holder(
        &self,
        edge: &PostgresRecoveryRelationEdge,
        original: &ActivityRequestFocus,
        database_oid: &str,
    ) -> bool {
        self.is_valid()
            && self.started_at.is_some()
            && self.wait_type.as_deref() == Some("Lock")
            && self.lock_snapshot_rows <= MAX_FOCUSED_LOCK_SNAPSHOT_ROWS
            && !self.edges_limited
            && self.relation_edges.contains(edge)
            && edge.blocks(original, database_oid)
    }
    pub fn is_valid(&self) -> bool {
        self.pid.parse::<i32>().is_ok_and(|id| id > 0 && id.to_string() == self.pid)
            && self.started_at.as_deref().is_none_or(|at| canonical_time(at).is_ok())
            && self.database_oid.as_deref().is_none_or(|id| native_number(id, true))
            && self.lock_snapshot_rows <= MAX_FOCUSED_LOCK_SNAPSHOT_ROWS + 1
            && self.relation_edges.len() <= MAX_RECOVERY_RELATION_EDGES
            && self.relation_edges.iter().all(PostgresRecoveryRelationEdge::is_valid)
            && self
                .relation_edges
                .iter()
                .map(|edge| (&edge.resource, &edge.requested_mode))
                .collect::<BTreeSet<_>>()
                .len()
                == self.relation_edges.len()
            && [&self.wait_type, &self.wait_event].into_iter().flatten().all(|v| bounded_name(v, 128))
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresRecoveryEndpoint {
    pub observed_at: String,
    pub database_oid: String,
    pub postmaster_started_at: String,
    pub stats_fetch_consistency: String,
    pub conflicts: NativeCounterSet,
    pub replay: Box<NativeReplicationSample>,
    pub processes: Vec<PostgresRecoveryProcess>,
    pub processes_limited: bool,
}
impl PostgresRecoveryEndpoint {
    pub fn native_epoch(&self) -> Option<String> {
        let started = self.conflicts.identity.qualifiers.get("postmaster_started_at")?;
        if canonical_time(&self.postmaster_started_at).ok().as_ref() != Some(started) {
            return None;
        }
        let version = self.replay.measurements.properties.get("server_version_num")?.parse::<u32>().ok()?;
        (version / 10000 == 18)
            .then(|| postgres_activity_epoch(&self.database_oid, &self.postmaster_started_at, version))
    }
    pub fn is_valid(&self) -> bool {
        self.native_epoch().is_some()
            && canonical_time(&self.observed_at).is_ok()
            && native_number(&self.database_oid, true)
            && bounded_name(&self.stats_fetch_consistency, 128)
            && self.conflicts.is_valid()
            && self.conflicts.scope == crate::MeasurementScope::Database
            && self.conflicts.identity.id == "pg_stat_database_conflicts"
            && self.conflicts.identity.qualifiers.get("dbid") == Some(&self.database_oid)
            && self.conflicts.counters.iter().all(|counter| {
                PostgresRecoveryConflictKind::ALL.iter().any(|kind| counter.native_name == kind.native_name())
                    && counter.nature == CounterNature::Cumulative
                    && counter.unit == CounterUnit::Count
                    && counter.scope == crate::MeasurementScope::Database
                    && matches!(counter.value, MeasurementValue::Integer(_))
            })
            && self.replay.is_valid()
            && self.replay.measurements.scope == crate::MeasurementScope::Server
            && self.replay.measurements.identity.id == "pg_recovery"
            && self.replay.measurements.properties.get("kind").map(String::as_str) == Some("standby")
            && self.replay.measurements.properties.get("read_standby").map(String::as_str) == Some("true")
            && self.replay.measurements.properties.get("read_database_oid") == Some(&self.database_oid)
            && self.processes.len() <= MAX_RECOVERY_PROCESSES
            && self.processes.iter().all(PostgresRecoveryProcess::is_valid)
            && self.processes.iter().map(|process| &process.pid).collect::<BTreeSet<_>>().len() == self.processes.len()
    }
}
