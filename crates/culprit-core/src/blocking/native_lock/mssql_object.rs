//! Native OBJECT lock identity and transaction-owned queue evidence. SQL labels establish no lock or cause.
use super::*;
use crate::blocking::NativeBlockingOwnerContext;
mod modes;
pub use modes::MssqlObjectLockMode;

/// Every documented resource-group column participates in equality, including partition/subtype/description.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlObjectLockResource {
    pub resource_type: String,
    pub subtype: Option<String>,
    pub database_id: u32,
    pub description: Option<String>,
    pub associated_entity_id: String,
    pub lock_partition: u32,
    /// Original catalog incarnation captured with this resource, not current/historical schema equivalence.
    pub created_without_offset: Option<String>,
}
impl MssqlObjectLockResource {
    pub fn is_valid(&self) -> bool {
        self.resource_type == "OBJECT"
            && self.database_id > 0
            && self.database_id <= i16::MAX as u32
            && self.subtype.as_deref().is_none_or(|value| text(value, 240, true))
            && self.description.as_deref().is_none_or(|value| text(value, 1024, true))
            && self.created_without_offset.as_deref().is_none_or(native_datetime)
            && self
                .associated_entity_id
                .parse::<i32>()
                .is_ok_and(|id| id != 0 && id.to_string() == self.associated_entity_id)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlLockTransaction {
    pub id: String,
    /// Native datetime with no offset; it is not a UTC instant.
    pub begun_without_offset: String,
    /// Effective local user-transaction witness from the same native session/active-transaction bridge.
    pub active_local_user_transaction: Option<bool>,
}
impl MssqlLockTransaction {
    pub fn is_valid(&self) -> bool {
        count(&self.id, true) && native_datetime(&self.begun_without_offset)
    }
}
/// Original native waiting-task pointer joined to this requested lock owner address.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlObjectLockWaitEdge {
    pub task_address: String,
    pub owner_address: String,
    pub waiter_execution_context_id: String,
    pub blocking_session_id: String,
    pub blocker_execution_context_id: Option<String>,
}
impl MssqlObjectLockWaitEdge {
    fn is_valid(&self) -> bool {
        address(&self.task_address)
            && address(&self.owner_address)
            && count(&self.waiter_execution_context_id, false)
            && count(&self.blocking_session_id, true)
            && self.blocker_execution_context_id.as_deref().is_none_or(|id| count(id, false))
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlObjectLockBlocker {
    pub context: NativeBlockingOwnerContext,
    pub transaction: Option<MssqlLockTransaction>,
    pub owner_type: Option<String>,
    pub owner_id: Option<String>,
    pub owner_guid: Option<String>,
    pub granted_modes: Vec<MssqlObjectLockMode>,
    pub waiting_mode: Option<MssqlObjectLockMode>,
    pub waiting_status: Option<String>,
    pub native_wait_edges: Vec<MssqlObjectLockWaitEdge>,
    /// Exact native participant and transaction at the original snapshot and the paid settlement witness.
    pub matched: bool,
    pub settled: bool,
}
impl MssqlObjectLockBlocker {
    pub fn is_valid(&self) -> bool {
        self.context.is_valid()
            && self.owner_type.as_deref().is_none_or(|owner| text(owner, 60, false))
            && self.owner_id.as_deref().is_none_or(signed_id)
            && self.owner_guid.as_deref().is_none_or(|guid| text(guid, 36, false))
            && self.transaction.as_ref().is_none_or(MssqlLockTransaction::is_valid)
            && self.granted_modes.len() <= MAX_NATIVE_RELATION_LOCK_MODES
            && self.granted_modes.iter().all(MssqlObjectLockMode::is_valid)
            && self.waiting_mode.as_ref().is_none_or(MssqlObjectLockMode::is_valid)
            && self.waiting_status.as_deref().is_none_or(|status| text(status, 60, false))
            && (self.waiting_mode.is_none() || self.waiting_status.is_some())
            && self.native_wait_edges.len() <= MAX_FOCUSED_LOCK_SNAPSHOT_ROWS as usize
            && self.native_wait_edges.iter().all(MssqlObjectLockWaitEdge::is_valid)
    }
    pub(super) fn qualified(&self) -> bool {
        self.is_valid()
            && self.matched
            && self.settled
            && !self.native_wait_edges.is_empty()
            && self.owner_type.as_deref() == Some("TRANSACTION")
            && self.owner_guid.as_deref().is_none_or(|guid| guid == "00000000-0000-0000-0000-000000000000")
            && self.transaction.as_ref().is_some_and(|transaction| {
                Some(transaction.id.as_str()) == self.owner_id.as_deref()
                    && transaction.active_local_user_transaction == Some(true)
            })
            && self.waiting_status.as_deref().is_none_or(|status| status == "WAIT")
            && (self.waiting_status.is_none()
                || self.waiting_mode.is_some() && self.context.has_single_current_request())
            && self.context.complete_request_population
            && matches!(self.context.current_requests.as_deref(), Some("0" | "1"))
            && self.context.actor.as_ref().is_some_and(|actor| {
                if actor.request.is_none() {
                    actor.transaction.as_deref() == self.owner_id.as_deref() && actor.transaction.is_some()
                } else {
                    actor.transaction.as_ref().is_none_or(|id| Some(id.as_str()) == self.owner_id.as_deref())
                }
            })
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlObjectLockAvailability {
    Observed,
    PermissionDenied,
    Limited,
    Unsupported,
    Unavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlObjectLockPoint {
    pub observed_at: String,
    pub settled_at: String,
    pub waiter: ActivityKey,
    pub waiter_matches: bool,
    pub waiter_settled: bool,
    pub waiter_current_requests: Option<String>,
    pub waiter_complete_request_population: bool,
    pub waiter_transaction: Option<MssqlLockTransaction>,
    pub resource: Option<MssqlObjectLockResource>,
    pub requested_mode: Option<MssqlObjectLockMode>,
    pub owner_type: Option<String>,
    pub owner_id: Option<String>,
    pub owner_guid: Option<String>,
    pub requested_status: Option<String>,
    pub lock_owner_address: Option<String>,
    /// The DMV changes this locator when active MARS ownership changes; it is never acquiring-SQL proof.
    pub lock_request_id: Option<String>,
    pub lock_execution_context_id: Option<String>,
    pub availability: MssqlObjectLockAvailability,
    pub snapshot_rows: u32,
    pub snapshot_limit: u32,
    pub snapshot_complete: bool,
    pub blockers_complete: bool,
    pub blockers: Vec<MssqlObjectLockBlocker>,
}
impl MssqlObjectLockPoint {
    pub fn is_valid_for(&self, key: &ActivityKey) -> bool {
        &self.waiter == key
            && key.is_valid()
            && key.request.is_some()
            && ObservationBounds::new(&self.observed_at, &self.settled_at).is_some()
            && self.waiter_current_requests.as_deref().is_none_or(|requests| count(requests, false))
            && (!self.waiter_complete_request_population || self.waiter_current_requests.is_some())
            && self.waiter_transaction.as_ref().is_none_or(MssqlLockTransaction::is_valid)
            && self.resource.as_ref().is_none_or(MssqlObjectLockResource::is_valid)
            && self.requested_mode.as_ref().is_none_or(MssqlObjectLockMode::is_valid)
            && (self.requested_mode.is_none() || self.resource.is_some())
            && self.owner_type.as_deref().is_none_or(|owner| text(owner, 60, false))
            && self.owner_id.as_deref().is_none_or(signed_id)
            && self.owner_guid.as_deref().is_none_or(|guid| text(guid, 36, false))
            && self.requested_status.as_deref().is_none_or(|status| text(status, 60, false))
            && self.lock_owner_address.as_deref().is_none_or(address)
            && self.lock_request_id.as_deref().is_none_or(|id| count(id, false))
            && self.lock_execution_context_id.as_deref().is_none_or(|id| count(id, false))
            && self.snapshot_limit > 0
            && self.snapshot_limit <= MAX_FOCUSED_LOCK_SNAPSHOT_ROWS
            && self.snapshot_rows <= self.snapshot_limit + 1
            && (self.availability != MssqlObjectLockAvailability::Observed
                || self.snapshot_complete == (self.snapshot_rows <= self.snapshot_limit))
            && self.blockers.len() <= MAX_BLOCKING_LOOKUPS as usize
            && self.blockers.iter().all(MssqlObjectLockBlocker::is_valid)
            && self.blockers.iter().map(|owner| &owner.context.owner).collect::<BTreeSet<_>>().len()
                == self.blockers.len()
    }
    pub fn edge(&self, owner: &MssqlObjectLockBlocker) -> NativeRelationLockEdge {
        if !self.waiter_matches
            || !self.waiter_settled
            || !self.snapshot_complete
            || !self.blockers_complete
            || self.availability != MssqlObjectLockAvailability::Observed
            || self.owner_type.as_deref() != Some("TRANSACTION")
            || self.resource.as_ref().is_none_or(|resource| resource.created_without_offset.is_none())
            || self.requested_status.as_deref() != Some("WAIT")
            || !self.waiter_complete_request_population
            || self.waiter_current_requests.as_deref() != Some("1")
            || self
                .waiter
                .request
                .as_ref()
                .is_none_or(|request| Some(request.id.as_str()) != self.lock_request_id.as_deref())
            || self.owner_guid.as_deref().is_some_and(|guid| guid != "00000000-0000-0000-0000-000000000000")
            || !self.waiter_transaction.as_ref().is_some_and(|transaction| {
                transaction.active_local_user_transaction == Some(true)
                    && Some(transaction.id.as_str()) == self.owner_id.as_deref()
            })
            || !owner.qualified()
            || owner.native_wait_edges.iter().any(|edge| {
                edge.owner_address.bytes().all(|byte| byte == b'0')
                    || edge.task_address.bytes().all(|byte| byte == b'0')
                    || Some(edge.owner_address.as_str()) != self.lock_owner_address.as_deref()
                    || Some(edge.waiter_execution_context_id.as_str()) != self.lock_execution_context_id.as_deref()
                    || owner.context.owner.session().is_none_or(|session| session.id != edge.blocking_session_id)
            })
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
fn count(value: &str, positive: bool) -> bool {
    value.parse::<i64>().is_ok_and(|n| (!positive || n > 0) && n >= 0 && n.to_string() == value)
}
fn text(value: &str, max: usize, empty: bool) -> bool {
    (empty || !value.is_empty()) && value.len() <= max && !value.chars().any(char::is_control)
}

fn native_datetime(value: &str) -> bool {
    value.len() <= 64 && chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
}

fn address(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|ch| ch.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests;

fn signed_id(value: &str) -> bool {
    value.parse::<i64>().is_ok_and(|id| id.to_string() == value)
}
