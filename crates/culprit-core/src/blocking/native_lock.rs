//! Native lock-resource points. Mode interpretation stays with the native type, not engine-name branches in views.
use crate::{Engine, telemetry::*};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ts_rs::TS;

mod mysql_metadata;
pub use mysql_metadata::*;

mod mssql;
pub use mssql::MssqlBlockingSessionPoint;
mod mssql_object;
pub use mssql_object::*;

pub const MAX_FOCUSED_LOCK_SNAPSHOT_ROWS: u32 = 512;
pub const MAX_NATIVE_RELATION_LOCK_MODES: usize = 16;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", content = "data", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum NativeLockEvidence {
    Postgres(Box<PostgresLockPoint>),
    MssqlBlockingSession(Box<MssqlBlockingSessionPoint>),
    MssqlObject(Box<MssqlObjectLockPoint>),
    MysqlMetadata(Box<MysqlMetadataLockPoint>),
}
impl NativeLockEvidence {
    pub fn is_valid_for(&self, key: &ActivityKey) -> bool {
        match self {
            Self::Postgres(point) => point.is_valid_for(key),
            Self::MssqlBlockingSession(point) => point.is_valid_for(key),
            Self::MssqlObject(point) => point.is_valid_for(key),
            Self::MysqlMetadata(point) => point.is_valid_for(key),
        }
    }
    pub fn is_valid_for_lock(&self, key: &ActivityKey, lock: &super::LockWait) -> bool {
        self.is_valid_for(key)
            && match self {
                Self::Postgres(point) => point.requested_mode.as_ref().is_none_or(|mode| {
                    lock.resource.as_deref() == Some("relation") && lock.mode.as_deref() == Some(mode.native())
                }),
                Self::MssqlObject(point) => {
                    point.resource.as_ref().is_none_or(|_| lock.resource.as_deref() == Some("OBJECT"))
                        && point.requested_mode.as_ref().is_none_or(|mode| lock.mode.as_deref() == Some(mode.native()))
                }
                Self::MysqlMetadata(_) => lock.resource.as_deref() == Some("METADATA"),
                Self::MssqlBlockingSession(_) => {
                    lock.resource.is_none() && lock.mode.is_none() && lock.object.is_none() && lock.index.is_none()
                }
            }
    }
    pub fn accepts_source(&self, source: &CollectionDescription) -> bool {
        match self {
            Self::MysqlMetadata(point) => {
                source.target.engine() == Engine::Mysql
                    && matches!(&source.observation,SourceObservation::MetadataLockContext {result,..} if result.request.focus.original.key==point.subject)
                    && source.observation.activity().is_some_and(|snapshot| {
                        ObservationBounds::new(&snapshot.observed_from, &snapshot.observed_until).is_some_and(
                            |bounds| bounds.contains(&point.observed_at) && bounds.contains(&point.settled_at),
                        )
                    })
                    && source.target.database().is_some_and(|target| {
                        point.tickets.iter().all(|ticket| ticket.resource.schema == target.database)
                    })
            }
            Self::MssqlObject(point) => {
                source.target.engine() == Engine::Mssql
                    && matches!(&source.observation,SourceObservation::FocusedBlocking{result,..}
                        if point.resource.as_ref().is_none_or(|resource|result.focus.statement_identity.qualifiers.get("database_id")==Some(&resource.database_id.to_string())))
                    && source.observation.activity().is_some_and(|snapshot| {
                        ObservationBounds::new(&snapshot.observed_from, &snapshot.observed_until).is_some_and(
                            |bounds| bounds.contains(&point.observed_at) && bounds.contains(&point.settled_at),
                        )
                    })
            }
            Self::MssqlBlockingSession(point) => {
                source.target.engine() == Engine::Mssql
                    && matches!(source.observation, SourceObservation::FocusedBlocking { .. })
                    && source.observation.activity().is_some_and(|snapshot| {
                        ObservationBounds::new(&snapshot.observed_from, &snapshot.observed_until)
                            .is_some_and(|bounds| bounds.contains(&point.observed_at))
                    })
            }
            Self::Postgres(point) => {
                source.target.engine() == Engine::Postgres
                    && matches!(source.observation, SourceObservation::FocusedBlocking { .. })
                    && source.observation.activity().is_some_and(|snapshot| {
                        ObservationBounds::new(&snapshot.observed_from, &snapshot.observed_until)
                            .is_some_and(|bounds| bounds.contains(&point.observed_at))
                    })
            }
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresRelationLockTag {
    pub database_oid: String,
    pub relation_oid: String,
}
impl PostgresRelationLockTag {
    pub fn is_valid(&self) -> bool {
        oid(&self.database_oid, false) && oid(&self.relation_oid, true)
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", content = "native", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum PostgresRelationLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
    Unknown(String),
}
impl PostgresRelationLockMode {
    pub fn from_native(native: &str) -> Self {
        match native {
            "AccessShareLock" => Self::AccessShare,
            "RowShareLock" => Self::RowShare,
            "RowExclusiveLock" => Self::RowExclusive,
            "ShareUpdateExclusiveLock" => Self::ShareUpdateExclusive,
            "ShareLock" => Self::Share,
            "ShareRowExclusiveLock" => Self::ShareRowExclusive,
            "ExclusiveLock" => Self::Exclusive,
            "AccessExclusiveLock" => Self::AccessExclusive,
            _ => Self::Unknown(native.into()),
        }
    }
    pub fn native(&self) -> &str {
        match self {
            Self::AccessShare => "AccessShareLock",
            Self::RowShare => "RowShareLock",
            Self::RowExclusive => "RowExclusiveLock",
            Self::ShareUpdateExclusive => "ShareUpdateExclusiveLock",
            Self::Share => "ShareLock",
            Self::ShareRowExclusive => "ShareRowExclusiveLock",
            Self::Exclusive => "ExclusiveLock",
            Self::AccessExclusive => "AccessExclusiveLock",
            Self::Unknown(native) => native,
        }
    }
    fn position(&self) -> Option<usize> {
        Some(match self {
            Self::AccessShare => 0,
            Self::RowShare => 1,
            Self::RowExclusive => 2,
            Self::ShareUpdateExclusive => 3,
            Self::Share => 4,
            Self::ShareRowExclusive => 5,
            Self::Exclusive => 6,
            Self::AccessExclusive => 7,
            Self::Unknown(_) => return None,
        })
    }
    /// PostgreSQL's documented relation-mode conflict table. Unknown future modes never establish compatibility.
    pub fn conflicts_with(&self, other: &Self) -> Option<bool> {
        const CONFLICTS: [u8; 8] = [128, 192, 240, 248, 236, 252, 254, 255];
        Some(CONFLICTS[self.position()?] & (1 << other.position()?) != 0)
    }
    fn is_valid(&self) -> bool {
        let text = self.native();
        !text.is_empty() && text.len() <= 128 && !text.chars().any(char::is_control)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresLockBlocker {
    /// The original pg_blocking_pids result. Zero is an unresolved prepared-transaction owner.
    pub native_pid: String,
    /// Same-resource actor metadata guarded against the already reached original participant generation.
    pub actor: Option<ActivityKey>,
    /// Native activity/worker roster established a single backend, not a normalized parallel group leader.
    pub single_backend: bool,
    pub granted_modes: Vec<PostgresRelationLockMode>,
    pub waiting_mode: Option<PostgresRelationLockMode>,
    /// The same matched-actor query's wait, independently of the earlier Activity wait.
    #[serde(deserialize_with = "Option::deserialize")]
    pub output_context: Option<super::NativeBlockingOwnerContext>,
}
impl PostgresLockBlocker {
    fn is_valid(&self) -> bool {
        oid(&self.native_pid, false)
            && self.actor.as_ref().is_none_or(|actor| {
                postgres_actor(actor) && actor.session.id == self.native_pid && self.native_pid != "0"
            })
            && (!self.single_backend || self.actor.is_some())
            && self.granted_modes.len() <= MAX_NATIVE_RELATION_LOCK_MODES
            && self.granted_modes.iter().all(PostgresRelationLockMode::is_valid)
            && self.granted_modes.iter().collect::<BTreeSet<_>>().len() == self.granted_modes.len()
            && self.waiting_mode.as_ref().is_none_or(PostgresRelationLockMode::is_valid)
            && self.output_context.as_ref().is_none_or(|context| {
                context.is_valid() && context.owner == self.owner() && context.actor == self.actor
            })
    }
    pub fn owner(&self) -> ActivityWaitOwner {
        if self.native_pid == "0" {
            ActivityWaitOwner::Transaction { native_id: "0".into(), owner: TransactionWaitOwner::Prepared }
        } else {
            ActivityWaitOwner::Session {
                id: self.native_pid.clone(),
                generation: self.actor.as_ref().map(|actor| actor.session.generation.clone()),
            }
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresLockPoint {
    pub observed_at: String,
    /// Expected original selector; waiter_matches separately records the native generation guard.
    pub waiter: ActivityKey,
    pub waiter_matches: bool,
    pub single_backend: bool,
    /// Retained relevant backend/parallel-worker rows, bounded separately from the lock snapshot.
    pub actor_rows: u32,
    pub resource: Option<PostgresRelationLockTag>,
    pub requested_mode: Option<PostgresRelationLockMode>,
    /// A bounded prefix is never a complete resource/mode census, even when the owning Activity frame reached EOF.
    pub snapshot_rows: u32,
    pub snapshot_limit: u32,
    pub snapshot_complete: bool,
    pub blockers_complete: bool,
    pub blockers: Vec<PostgresLockBlocker>,
}
impl PostgresLockPoint {
    pub fn is_valid_for(&self, key: &ActivityKey) -> bool {
        &self.waiter == key
            && postgres_actor(key)
            && key.request.is_some()
            && (!self.single_backend || self.waiter_matches && self.actor_rows <= MAX_BLOCKING_PARTICIPANTS)
            && self.actor_rows <= MAX_BLOCKING_PARTICIPANTS + 1
            && (!self.waiter_matches || self.actor_rows > 0)
            && self.blockers.iter().all(|owner| !owner.single_backend || self.actor_rows <= MAX_BLOCKING_PARTICIPANTS)
            && canonical_time(&self.observed_at).is_ok()
            && self.resource.as_ref().is_none_or(PostgresRelationLockTag::is_valid)
            && self.requested_mode.as_ref().is_none_or(PostgresRelationLockMode::is_valid)
            && (self.resource.is_some() == self.requested_mode.is_some())
            && self.snapshot_limit == MAX_FOCUSED_LOCK_SNAPSHOT_ROWS
            && self.snapshot_rows <= self.snapshot_limit + 1
            && (self.snapshot_complete == (self.snapshot_rows <= self.snapshot_limit))
            && self.blockers.len() <= MAX_BLOCKING_LOOKUPS as usize
            && self.blockers.iter().all(PostgresLockBlocker::is_valid)
            && self.blockers.iter().map(|owner| &owner.native_pid).collect::<BTreeSet<_>>().len() == self.blockers.len()
    }
    pub fn edge(&self, owner: &PostgresLockBlocker) -> NativeRelationLockEdge {
        if !self.waiter_matches
            || !self.single_backend
            || self.actor_rows > MAX_BLOCKING_PARTICIPANTS
            || !owner.single_backend
            || !self.snapshot_complete
            || !self.blockers_complete
            || self.resource.is_none()
            || owner.actor.is_none()
        {
            return NativeRelationLockEdge::Unknown;
        }
        let Some(requested) = &self.requested_mode else { return NativeRelationLockEdge::Unknown };
        let Some(granted) =
            owner.granted_modes.iter().map(|mode| requested.conflicts_with(mode)).collect::<Option<Vec<_>>>()
        else {
            return NativeRelationLockEdge::Unknown;
        };
        let Some(pending) =
            owner.waiting_mode.as_ref().map(|mode| requested.conflicts_with(mode)).unwrap_or(Some(false))
        else {
            return NativeRelationLockEdge::Unknown;
        };
        match (granted.into_iter().any(|conflict| conflict), pending) {
            (true, false) => NativeRelationLockEdge::GrantedConflict,
            (false, true) => NativeRelationLockEdge::PendingOnlyConflict,
            (true, true) => NativeRelationLockEdge::GrantedAndPendingConflict,
            (false, false) => NativeRelationLockEdge::Unknown,
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum NativeRelationLockEdge {
    GrantedConflict,
    PendingOnlyConflict,
    GrantedAndPendingConflict,
    Unknown,
}
fn oid(value: &str, positive: bool) -> bool {
    value.parse::<u32>().is_ok_and(|parsed| (!positive || parsed > 0) && parsed.to_string() == value)
}

fn postgres_actor(key: &ActivityKey) -> bool {
    let pid = key.session.id.parse::<i32>().ok();
    key.is_valid()
        && pid.is_some_and(|pid| pid > 0 && pid.to_string() == key.session.id)
        && canonical_time(&key.session.generation).is_ok()
        && key
            .request
            .as_ref()
            .is_none_or(|request| request.id == "statement" && canonical_time(&request.generation).is_ok())
        && key.transaction.as_deref().is_none_or(|at| canonical_time(at).is_ok())
}

#[cfg(test)]
mod tests;
