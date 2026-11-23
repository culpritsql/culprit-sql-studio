//! The one bounded endpoint decoder. Actor identity still uses the ordinary Activity participant decoder.
use super::*;
use crate::{
    activity::participants::Check,
    read::{boolean, optional_counter, optional_text, signed_integer, text},
    value::Rec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "lowercase", deny_unknown_fields)]
pub(in crate::activity::blocking) enum NativeEntry {
    Lock(Box<NativeLock>),
    Task(NativeTask),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::activity::blocking) struct NativeLock {
    pub session_id: i32,
    pub resource: MssqlObjectLockResource,
    pub object_label: Option<String>,
    pub mode: Option<String>,
    pub status: Option<String>,
    pub owner_type: Option<String>,
    pub owner_id: Option<String>,
    pub owner_guid: Option<String>,
    pub owner_address: Option<String>,
    pub request_id: Option<i32>,
    pub execution_context_id: Option<i32>,
    pub transaction: Option<MssqlLockTransaction>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::activity::blocking) struct NativeTask {
    pub task_address: String,
    pub session_id: i32,
    pub execution_context_id: i32,
    pub owner_address: Option<String>,
    pub blocking_session_id: Option<i32>,
    pub blocker_execution_context_id: Option<i32>,
}
pub(in crate::activity::blocking) struct Actor {
    pub check: Check,
    pub session: Option<NativeActivityIdentity>,
    pub requests: Option<String>,
    pub transaction: Option<String>,
}
impl Actor {
    fn decode(row: &Rec) -> Result<Self> {
        let id = optional_counter(row, "native_session_id")?;
        let login = optional_text(row, "native_login")?.map(|time| crate::sql_time::native_time(&time)).transpose()?;
        let session = match (id, login) {
            (Some(id), Some(generation)) => Some(NativeActivityIdentity { id, generation }),
            (None, None) => None,
            _ => return Err(invalid()),
        };
        Ok(Self {
            check: Check::decode(row)?,
            session,
            requests: optional_counter(row, "current_requests")?,
            transaction: optional_counter(row, "request_transaction_id")?,
        })
    }
    pub fn matches(&self, row: &ActivityRecord) -> bool {
        self.session.as_ref() == Some(&row.facts.key.session)
            && match (row.facts.key.request.as_ref(), self.requests.as_deref()) {
                (None, Some("0")) => true,
                (Some(_), Some("1")) => self.check.observation_state(&row.facts) == RequestState::SameStatement,
                _ => false,
            }
    }
    pub fn matches_transaction(&self, row: &ActivityRecord, lock: &NativeLock) -> bool {
        self.matches(row)
            && lock.owner_type.as_deref() == Some("TRANSACTION")
            && lock.transaction.as_ref().is_some_and(|tx| {
                tx.active_local_user_transaction == Some(true) && Some(tx.id.as_str()) == lock.owner_id.as_deref()
            })
            && match &row.facts.key.request {
                Some(_) => {
                    self.transaction == lock.owner_id
                        && lock.request_id.as_ref().is_some_and(|id| {
                            row.facts.key.request.as_ref().is_some_and(|request| request.id == id.to_string())
                        })
                }
                None => row.facts.key.transaction.is_some() && row.facts.key.transaction == lock.owner_id,
            }
    }
    pub fn context(&self, row: &ActivityRecord, owner: ActivityWaitOwner) -> NativeBlockingOwnerContext {
        let matched = self.matches(row);
        NativeBlockingOwnerContext {
            owner,
            actor: matched.then(|| row.facts.key.clone()),
            statement: matched
                .then(|| row.facts.statement_identity.clone())
                .flatten()
                .filter(|_| row.facts.key.request.is_some()),
            native_wait: matched
                .then(|| self.check.wait_type.clone())
                .flatten()
                .filter(|_| row.facts.key.request.is_some()),
            wait_ms: None,
            current_requests: self.requests.clone(),
            complete_request_population: self.session.as_ref() == Some(&row.facts.key.session)
                && self.requests.is_some(),
        }
    }
}
pub(in crate::activity::blocking) struct Snapshot {
    pub at: String,
    pub visible: bool,
    pub limit: u32,
    pub entries: Vec<NativeEntry>,
    pub actors: BTreeMap<usize, Actor>,
}
impl Snapshot {
    pub fn decode(rows: Vec<Rec>, frame: &ActivityFrame, limit: u32) -> Result<Self> {
        let mut guard = None;
        let mut actors = BTreeMap::new();
        for row in &rows {
            if signed_integer(row, "ordinal")? == -1 {
                if guard.replace(row).is_some() {
                    return Err(invalid());
                }
            } else {
                let actor = Actor::decode(row)?;
                if actor.check.ordinal >= frame.records.len() || actors.insert(actor.check.ordinal, actor).is_some() {
                    return Err(invalid());
                }
            }
        }
        let guard = guard.ok_or_else(invalid)?;
        let at = canonical_time(&text(guard, "observed_at")?)?;
        let visible = boolean(guard, "population_visible")?;
        let entries: Vec<NativeEntry> = serde_json::from_str(&text(guard, "lock_payload")?).map_err(|_| invalid())?;
        if entries.len() > limit as usize + 1
            || (!visible && (!entries.is_empty() || !actors.is_empty()))
            || visible && actors.len() != frame.records.len()
            || rows.iter().any(|row| boolean(row, "population_visible").ok() != Some(visible))
            || actors.values().any(|actor| actor.check.at != at)
            || ObservationBounds::new(&frame.context.at, &at).is_none()
            || entries.iter().any(|entry| !entry.is_valid())
            || entries.iter().enumerate().any(|(index, entry)| entries[..index].contains(entry))
        {
            return Err(invalid());
        }
        Ok(Self { at, visible, limit, entries, actors })
    }
    pub fn complete(&self) -> bool {
        self.visible && self.entries.len() <= self.limit as usize
    }
    pub fn locks(&self) -> impl Iterator<Item = &NativeLock> {
        self.entries
            .iter()
            .filter_map(|entry| if let NativeEntry::Lock(lock) = entry { Some(lock.as_ref()) } else { None })
    }
    pub fn tasks(&self, lock: &NativeLock) -> Vec<&NativeTask> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                NativeEntry::Task(task)
                    if task.session_id == lock.session_id
                        && Some(task.execution_context_id) == lock.execution_context_id
                        && task.owner_address.is_some()
                        && task.owner_address == lock.owner_address =>
                {
                    Some(task)
                }
                _ => None,
            })
            .collect()
    }
    pub fn actor<'a>(&'a self, frame: &'a ActivityFrame, session: i32) -> Option<(&'a ActivityRecord, &'a Actor)> {
        frame.records.iter().enumerate().find_map(|(ordinal, row)| {
            (row.facts.key.session.id == session.to_string())
                .then(|| self.actors.get(&ordinal).map(|actor| (row, actor)))
                .flatten()
        })
    }
}
impl NativeEntry {
    fn is_valid(&self) -> bool {
        match self {
            Self::Lock(lock) => {
                lock.session_id > 0
                    && lock.session_id <= i16::MAX as i32
                    && lock.resource.is_valid()
                    && lock.object_label.as_deref().is_none_or(|label| label.len() <= 2048)
                    && [&lock.mode, &lock.status, &lock.owner_type]
                        .iter()
                        .all(|value| value.as_deref().is_none_or(|value| bounded_text(value, 60)))
                    && lock.owner_id.as_deref().is_none_or(native_signed_id)
                    && lock.owner_guid.as_deref().is_none_or(|guid| bounded_text(guid, 36))
                    && lock.owner_address.as_deref().is_none_or(native_address)
                    && lock.request_id.is_none_or(|id| id >= 0)
                    && lock.execution_context_id.is_none_or(|id| id >= 0)
                    && lock.transaction.as_ref().is_none_or(MssqlLockTransaction::is_valid)
            }
            Self::Task(task) => {
                task.session_id > 0
                    && task.session_id <= i16::MAX as i32
                    && task.execution_context_id >= 0
                    && native_address(&task.task_address)
                    && task.owner_address.as_deref().is_none_or(native_address)
                    && task.blocker_execution_context_id.is_none_or(|id| id >= 0)
            }
        }
    }
}
fn bounded_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn native_signed_id(value: &str) -> bool {
    value.parse::<i64>().is_ok_and(|id| id.to_string() == value)
}
fn native_address(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|ch| ch.is_ascii_hexdigit())
}

impl NativeLock {
    /// Display labels establish no owner. Compare only native resource/incarnation, request and transaction facts.
    pub fn same_native_lock(&self, other: &Self) -> bool {
        (
            self.session_id,
            &self.resource,
            &self.mode,
            &self.status,
            &self.owner_type,
            &self.owner_id,
            &self.owner_guid,
            &self.owner_address,
            self.request_id,
            self.execution_context_id,
            &self.transaction,
        ) == (
            other.session_id,
            &other.resource,
            &other.mode,
            &other.status,
            &other.owner_type,
            &other.owner_id,
            &other.owner_guid,
            &other.owner_address,
            other.request_id,
            other.execution_context_id,
            &other.transaction,
        )
    }
}
