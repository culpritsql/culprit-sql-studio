//! Performance Schema metadata tickets are observations, not an exposed native wait-for queue.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MysqlMetadataLockResource {
    pub object_type: String,
    pub schema: String,
    pub name: String,
}
impl MysqlMetadataLockResource {
    pub fn is_valid(&self) -> bool {
        self.object_type == "TABLE" && label(&self.schema) && label(&self.name)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MysqlMetadataTransactionEvent {
    /// Performance Schema event ID and TIMER_START; TRX_ID is documented unused in both flavors.
    pub event_id: Option<String>,
    pub timer_start: Option<String>,
    pub state: Option<String>,
    pub end_event_id: Option<String>,
}
impl MysqlMetadataTransactionEvent {
    pub fn is_valid(&self) -> bool {
        self.event_id.as_deref().is_none_or(|id| unsigned(id, true))
            && self.timer_start.as_deref().is_none_or(|start| unsigned(start, false))
            && self.state.as_deref().is_none_or(|state| matches!(state, "ACTIVE" | "COMMITTED" | "ROLLED BACK"))
            && self.end_event_id.as_deref().is_none_or(|id| unsigned(id, false))
    }
    pub fn is_active(&self) -> bool {
        self.is_valid()
            && self.state.as_deref() == Some("ACTIVE")
            && self.end_event_id.is_none()
            && self.event_id.is_some()
            && self.timer_start.is_some()
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MysqlMetadataLockTicket {
    /// Address of the native MDL ticket, not the shared object or a durable incarnation.
    pub address: String,
    pub resource: MysqlMetadataLockResource,
    pub mode: String,
    pub duration: String,
    pub status: String,
    pub owner_thread_id: Option<String>,
    pub owner_process_id: Option<String>,
    /// Requesting native event counter; equality with a statement EVENT_ID is not assumed.
    pub owner_event_id: Option<String>,
    pub actor: Option<ActivityKey>,
    pub transaction: Option<MysqlMetadataTransactionEvent>,
    pub matched: bool,
    pub settled: bool,
}
impl MysqlMetadataLockTicket {
    pub fn is_valid(&self) -> bool {
        unsigned(&self.address, true)
            && self.resource.is_valid()
            && native_word(&self.mode)
            && native_word(&self.duration)
            && native_word(&self.status)
            && self.owner_thread_id.as_deref().is_none_or(|id| unsigned(id, false))
            && self.owner_process_id.as_deref().is_none_or(|id| unsigned(id, false))
            && self.owner_event_id.as_deref().is_none_or(|id| unsigned(id, false))
            && self.actor.as_ref().is_none_or(|actor| {
                actor.is_valid()
                    && Some(actor.session.id.as_str()) == self.owner_process_id.as_deref()
                    && actor.session.id.parse::<u32>().is_ok_and(|id| id > 0 && id.to_string() == actor.session.id)
                    && Some(actor.session.generation.as_str()) == self.owner_thread_id.as_deref()
                    && actor.transaction.is_none()
                    && actor
                        .request
                        .as_ref()
                        .is_none_or(|request| unsigned(&request.id, true) && unsigned(&request.generation, false))
            })
            && (!self.matched || self.actor.is_some())
            && (!self.settled || self.matched)
            && self.transaction.as_ref().is_none_or(MysqlMetadataTransactionEvent::is_valid)
    }
    pub fn is_qualified_actor(&self) -> bool {
        self.is_valid()
            && self.matched
            && self.settled
            && self.owner_event_id.as_deref().is_some_and(|id| unsigned(id, true))
    }
    pub fn known_mode(&self) -> bool {
        matches!(
            self.mode.as_str(),
            "SHARED"
                | "SHARED_HIGH_PRIO"
                | "SHARED_READ"
                | "SHARED_WRITE"
                | "SHARED_WRITE_LOW_PRIO"
                | "SHARED_UPGRADABLE"
                | "SHARED_READ_ONLY"
                | "SHARED_NO_WRITE"
                | "SHARED_NO_READ_WRITE"
                | "EXCLUSIVE"
        )
    }
    pub fn known_duration(&self) -> bool {
        matches!(self.duration.as_str(), "STATEMENT" | "TRANSACTION" | "EXPLICIT")
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MysqlMetadataLockAvailability {
    Observed,
    Limited,
    Unavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MysqlMetadataLockOrigin {
    MysqlPerformanceSchema,
    MariadbPerformanceSchema,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MysqlMetadataLockPoint {
    pub origin: MysqlMetadataLockOrigin,
    pub observed_at: String,
    pub settled_at: String,
    pub subject: ActivityKey,
    pub subject_matches: bool,
    pub subject_settled: bool,
    pub availability: MysqlMetadataLockAvailability,
    pub snapshot_rows: u32,
    pub snapshot_limit: u32,
    /// The selected retained prefix reached EOF; it does not prove historical/global instrumentation completeness.
    pub snapshot_complete: bool,
    pub tickets: Vec<MysqlMetadataLockTicket>,
}
impl MysqlMetadataLockPoint {
    pub fn is_valid_for(&self, key: &ActivityKey) -> bool {
        &self.subject == key
            && key.is_valid()
            && key.request.is_some()
            && ObservationBounds::new(&self.observed_at, &self.settled_at).is_some()
            && self.snapshot_limit > 0
            && self.snapshot_limit < MAX_BLOCKING_PARTICIPANTS
            && self.snapshot_rows <= self.snapshot_limit + 1
            && self.tickets.len() <= self.snapshot_limit as usize
            && self.tickets.len() <= self.snapshot_rows as usize
            && (!self.snapshot_complete || self.snapshot_rows <= self.snapshot_limit)
            && self.tickets.iter().all(MysqlMetadataLockTicket::is_valid)
            && self.tickets.iter().map(|ticket| &ticket.address).collect::<BTreeSet<_>>().len() == self.tickets.len()
            && (!self.subject_settled || self.subject_matches)
    }
    pub fn accepts_actor(&self, key: &ActivityKey) -> bool {
        &self.subject == key || self.tickets.iter().any(|ticket| ticket.actor.as_ref() == Some(key))
    }
}
fn unsigned(value: &str, positive: bool) -> bool {
    value.parse::<u64>().is_ok_and(|id| (!positive || id > 0) && id.to_string() == value)
}
fn label(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= 64 && !value.chars().any(char::is_control)
}
fn native_word(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && !value.chars().any(char::is_control)
}
