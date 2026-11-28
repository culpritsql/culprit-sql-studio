use crate::{
    client::{Client, invalid_response},
    queries,
};
use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use culprit_core::{DatabaseIdentity, Engine, Result, ServerInfo};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Identity {
    pub node: String,
    pub database: String,
    database_uuid: String,
    version: String,
    observed_at: String,
}

impl Identity {
    pub async fn read(client: &Client) -> Result<Self> {
        let mut rows = client.rows(queries::IDENTITY, vec![]).await?;
        if rows.len() != 1 {
            return Err(invalid_response());
        }
        rows.pop().ok_or_else(invalid_response)
    }
    pub fn supported_profile(&self) -> bool {
        self.version.split('.').next().is_some_and(|v| v.parse::<u32>().is_ok())
            && culprit_core::version_at_least(&self.version, [23, 8, 0])
    }
    pub fn clock(&self) -> Result<DateTime<Utc>> {
        parse_utc(&self.observed_at)
    }
    pub fn target(&self, host: &str, port: u16) -> DatabaseIdentity {
        let mut id = DatabaseIdentity::server(Engine::Clickhouse, &self.database, host, port, Some(self.node.clone()));
        id.incarnation =
            (self.database_uuid != "00000000-0000-0000-0000-000000000000").then(|| self.database_uuid.clone());
        id
    }
    pub fn server(&self) -> ServerInfo {
        ServerInfo {
            engine: Engine::Clickhouse,
            product: "ClickHouse".into(),
            version: self.version.clone(),
            edition: None,
            cloud: None,
            platform: None,
            database: self.database.clone(),
            database_size_bytes: None,
            started_at: None,
            read_only_session: true,
            capabilities: vec![],
            missing: vec![],
        }
    }
}
pub fn parse_utc(value: &str) -> Result<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").map(|at| at.and_utc()).map_err(|_| invalid_response())
}
pub fn instant(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}
pub fn parameter(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}
