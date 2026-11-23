//! Native boxed secondary redo request points, never an Azure/Hyperscale role inference.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlRedoRequest {
    pub session_id: u32,
    pub request_id: u32,
    pub session_generation_without_offset: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub started_without_offset: Option<String>,
    pub command: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_type: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_ms: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub wait_resource: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocking_session_id: Option<i32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub reader_session: Option<NativeActivityIdentity>,
}
impl MssqlRedoRequest {
    fn is_valid(&self) -> bool {
        (1..=i16::MAX as u32).contains(&self.session_id)
            && self.request_id <= i32::MAX as u32
            && super::super::replication::local_time(&self.session_generation_without_offset)
            && self.started_without_offset.as_deref().is_none_or(super::super::replication::local_time)
            && matches!(self.command.as_str(), "DB STARTUP" | "PARALLEL REDO TASK" | "PARALLEL REDO HELP TASK")
            && self.wait_type.as_deref().is_none_or(|value| bounded_name(value, 128))
            && self
                .wait_resource
                .as_deref()
                .is_none_or(|value| value.len() <= 1024 && !value.chars().any(char::is_control))
            && self.wait_ms.as_deref().is_none_or(|value| native_number(value, false))
            && self.reader_session.as_ref().is_none_or(|reader| {
                reader.is_valid()
                    && self.blocking_session_id.map(|id| id.to_string()).as_deref() == Some(reader.id.as_str())
            })
    }
    pub fn reader_is_native_blocker(&self, original: &ActivityRequestFocus) -> bool {
        self.is_valid()
            && self.started_without_offset.is_some()
            && self.wait_type.as_deref() == Some("LCK_M_SCH_M")
            && self.reader_session.as_ref() == Some(&original.original.key.session)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlRecoveryEndpoint {
    pub observed_at: String,
    pub replica_observed_at: String,
    pub database_id: u32,
    pub replica: Box<AvailabilityReplicaSample>,
    pub requests: Vec<MssqlRedoRequest>,
    pub requests_limited: bool,
}
impl MssqlRecoveryEndpoint {
    pub fn is_valid(&self) -> bool {
        canonical_time(&self.observed_at).is_ok()
            && ObservationBounds::new(&self.replica_observed_at, &self.observed_at).is_some()
            && self.database_id > 0
            && ReplicationFact::AvailabilityGroup(self.replica.clone()).is_valid()
            && self.replica.is_local == Some(true)
            && self.replica.database_id == Some(self.database_id)
            && self.replica.progress.is_primary == Some(false)
            && self.replica.replica.as_ref().is_some_and(|replica| replica.state_observed && replica.role == Some(2))
            && self.requests.len() <= MAX_BLOCKING_PARTICIPANTS as usize
            && self.requests.iter().all(MssqlRedoRequest::is_valid)
            && self
                .requests
                .iter()
                .map(|row| (row.session_id, row.request_id, &row.started_without_offset))
                .collect::<BTreeSet<_>>()
                .len()
                == self.requests.len()
    }
}
