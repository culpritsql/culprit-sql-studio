//! Session facts must agree across MARS rows; current-request connection tokens remain separate observations.
use super::*;

#[derive(PartialEq, Eq)]
pub(in super::super) struct SessionContext<'a> {
    pub last_end_age_ms: Option<&'a str>,
    login: Option<&'a str>,
    protocol: Option<&'a str>,
    physical_reads: Option<&'a str>,
    idle_connection: Option<&'a MssqlIdleSessionConnection>,
}
impl<'a> SessionContext<'a> {
    pub(in super::super) fn from_row(row: &'a ActivityObservation) -> Option<Self> {
        let context = row.session_context.as_deref()?;
        let SessionNativeContext::Mssql(native) = context.native.as_deref()? else { return None };
        Some(Self {
            last_end_age_ms: context.last_request_end_age_ms.as_deref(),
            login: row.client.as_ref().and_then(|client| client.user.as_deref()),
            protocol: native.tds_protocol_version.as_deref(),
            physical_reads: row.session_counters.physical_reads.as_deref(),
            idle_connection: native.idle_connection.as_ref(),
        })
    }
    fn primary_connection(&self) -> Option<&str> {
        match self.idle_connection? {
            MssqlIdleSessionConnection::Observed { connection_id, .. } => Some(connection_id),
            _ => None,
        }
    }
}

pub(in super::super) fn fields(sessions: &[&ClientSession<'_>]) -> Vec<culprit_core::SessionCensusField> {
    let physical_reads = sum(sessions, |session| session.recent.as_ref()?.physical_reads);
    let clients = sessions.iter().filter_map(|session| session.recent.as_ref()).collect::<Vec<_>>();
    let logins = clients.iter().filter_map(|context| context.login).collect::<BTreeSet<_>>();
    let protocols = clients.iter().filter_map(|context| context.protocol).collect::<BTreeSet<_>>();
    let primary = clients.iter().filter_map(|context| context.primary_connection()).collect::<BTreeSet<_>>();
    let request_connections = sessions.iter().flat_map(|session| &session.request_connections).collect::<BTreeSet<_>>();
    let unknown_login = clients.iter().filter(|context| context.login.is_none()).count();
    let unknown_protocol = clients.iter().filter(|context| context.protocol.is_none()).count();
    let unknown_primary =
        sessions.len() - clients.iter().filter(|context| context.primary_connection().is_some()).count();
    vec![
        field("reported_login", t!("view.session_census.field.login"), unanimous(&logins, unknown_login)),
        number("reported_login_labels", t!("view.session_census.recent.field.logins"), Some(&logins.len().to_string())),
        number(
            "unknown_login_sessions",
            t!("view.session_census.recent.field.unknown_logins"),
            Some(&unknown_login.to_string()),
        ),
        field(
            "native_tds_protocol",
            t!("view.session_census.field.tds_protocol"),
            unanimous(&protocols, unknown_protocol),
        ),
        number(
            "native_tds_codes",
            t!("view.session_census.recent.field.protocols"),
            Some(&protocols.len().to_string()),
        ),
        number(
            "unknown_tds_sessions",
            t!("view.session_census.recent.field.unknown_protocols"),
            Some(&unknown_protocol.to_string()),
        ),
        number(
            "session_physical_reads",
            t!("view.session_census.field.session_physical_reads"),
            physical_reads.as_deref(),
        ),
        number(
            "known_idle_primary_connections",
            t!("view.session_census.recent.field.primary_connections"),
            Some(&primary.len().to_string()),
        ),
        number(
            "unconfirmed_primary_sessions",
            t!("view.session_census.recent.field.unknown_primary"),
            Some(&unknown_primary.to_string()),
        ),
        number(
            "request_connection_tokens",
            t!("view.session_census.recent.field.request_connections"),
            Some(&request_connections.len().to_string()),
        ),
    ]
}
fn unanimous(values: &BTreeSet<&str>, missing: usize) -> Option<String> {
    values.first().filter(|_| missing == 0 && values.len() == 1).map(|value| (*value).to_owned())
}
