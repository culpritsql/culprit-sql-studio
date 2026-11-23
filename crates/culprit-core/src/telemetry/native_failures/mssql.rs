//! Extended Events fields describe the supplied native event, never current request or database authority.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlExtendedEventKind {
    /// An attention can be a client cancel or timeout. The event alone cannot distinguish them.
    Attention {
        native_duration: Option<String>,
    },
    /// Severity includes informational errors; interception is independent of eventual request outcome.
    ErrorReported {
        number: u32,
        severity: u8,
        state: u8,
        intercepted: Option<bool>,
        user_defined: Option<bool>,
    },
    DeadlockReport {
        graphs: u32,
    },
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlExtendedEventObservation {
    pub position: u32,
    pub kind: MssqlExtendedEventKind,
    /// The original timestamp attribute, including an offset-free value or absence.
    pub timestamp: Option<String>,
    pub timestamp_utc: Option<String>,
    /// Native actions are optional. These IDs lack member/process/session generations and cannot authorize a read.
    pub event_sequence: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub transaction_id: Option<String>,
    pub database_id: Option<String>,
    pub database: Option<String>,
    /// Native client labels are contextual, not an application/service owner mapping.
    pub client: Option<ActivityClient>,
    /// The event's captured sql_text action, not the statement that acquired a resource or caused this event.
    pub statement: DefinitionPayload,
}
impl MssqlExtendedEventObservation {
    pub fn qualified_timestamp(value: &str) -> Option<String> {
        canonical_time(value)
            .ok()
            .filter(|at| DateTime::parse_from_rfc3339(at).is_ok_and(|at| at.timestamp_subsec_nanos() < 1_000_000_000))
    }
    pub fn key(&self) -> Result<String> {
        super::position_key("mssql-extended-event-position-v1", self.position)
    }
    pub fn is_valid(&self) -> bool {
        (self.position as usize) < MAX_NATIVE_FAILURE_OBSERVATIONS
            && self.timestamp.as_deref().is_none_or(valid_timestamp)
            && self.timestamp_utc == self.timestamp.as_deref().and_then(Self::qualified_timestamp)
            && self.event_sequence.as_deref().is_none_or(unsigned_decimal)
            && self.session_id.as_deref().is_none_or(native_int)
            && self.request_id.as_deref().is_none_or(native_int)
            && self.transaction_id.as_deref().is_none_or(unsigned_decimal)
            && self.database_id.as_deref().is_none_or(native_int)
            && self
                .database
                .as_deref()
                .is_none_or(|value| !value.is_empty() && value.len() <= 512 && !value.contains('\0'))
            && self.client.as_ref().is_none_or(ActivityClient::is_valid)
            && self.statement.is_valid()
            && match &self.statement {
                DefinitionPayload::Available { text, .. } => text.len() <= 64 * 1024,
                DefinitionPayload::Unavailable { .. } => true,
            }
            && match &self.kind {
                MssqlExtendedEventKind::Attention { native_duration } => {
                    native_duration.as_deref().is_none_or(unsigned_decimal)
                }
                MssqlExtendedEventKind::ErrorReported { number, severity, .. } => {
                    (1..=i32::MAX as u32).contains(number) && *severity <= 25
                }
                MssqlExtendedEventKind::DeadlockReport { graphs } => (1..=4096).contains(graphs),
            }
    }
    pub(crate) fn redact(&mut self) {
        if let DefinitionPayload::Available { text, .. } = &self.statement {
            self.statement = DefinitionPayload::sql(text, crate::Engine::Mssql);
        }
    }
}
fn valid_timestamp(value: &str) -> bool {
    value.len() <= 64
        && (MssqlExtendedEventObservation::qualified_timestamp(value).is_some()
            || (value.split_once('.').is_none_or(|(_, fraction)| {
                !fraction.is_empty() && fraction.len() <= 9 && fraction.bytes().all(|byte| byte.is_ascii_digit())
            }) && chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .is_ok_and(|at| at.and_utc().timestamp_subsec_nanos() < 1_000_000_000)))
}
fn native_int(value: &str) -> bool {
    value.len() <= 10 && value.parse::<i32>().is_ok_and(|number| number >= 0 && number.to_string() == value)
}

#[cfg(test)]
mod tests;
