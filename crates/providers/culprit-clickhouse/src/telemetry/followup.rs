use super::*;
use crate::{client::invalid_response, identity::parameter, queries};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Execution {
    node: String,
    query_id: String,
    started_us: i64,
    ended_us: i64,
    family: String,
    outcome: String,
    exception_code: u32,
    query_duration_ms: u64,
    read_rows: u64,
    read_bytes: u64,
    written_rows: u64,
    written_bytes: u64,
    memory_usage: u64,
    profile: BTreeMap<String, u64>,
    tables: Vec<String>,
}
fn timestamp(us: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us).ok_or_else(invalid_response)
}
impl Reader {
    pub(super) async fn followup(
        &mut self,
        focus: &ClickHouseInvestigation,
        target: &DatabaseIdentity,
        limit: u32,
    ) -> Result<Captured> {
        if target != &focus.target {
            return Err(CollectionStop::SourceChanged.error());
        }
        let request = &focus.request;
        self.queries += 1;
        let rows: Vec<Execution> = self
            .client
            .rows(
                queries::EXECUTION,
                vec![
                    queries::profile_events(),
                    ("node", request.node.clone()),
                    ("query_id", request.query_id.clone()),
                    ("started", parameter(timestamp(request.started_us)?)),
                    ("ended", parameter(timestamp(request.ended_us)?)),
                ],
            )
            .await?;
        if rows.len() > 1 {
            return Err(CollectionStop::InvalidPage.error());
        }
        let Some(row) = rows.into_iter().next() else {
            return Err(CollectionStop::RetentionGap.error());
        };
        if row.node != request.node
            || row.query_id != request.query_id
            || row.started_us != request.started_us
            || row.ended_us != request.ended_us
            || row.exception_code == 0
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        let mut out = Captured::default();
        let point = ClickHouseExecution {
            focus: request.clone(),
            family: row.family,
            outcome: row.outcome,
            exception_code: row.exception_code,
            query_duration_ms: row.query_duration_ms,
            read_rows: row.read_rows,
            read_bytes: row.read_bytes,
            written_rows: row.written_rows,
            written_bytes: row.written_bytes,
            memory_usage: row.memory_usage,
            profile: row.profile,
        };
        out.add("system.query_log.execution", vec![ClickHouseObservation::Execution(point)], true)?;
        if let Some(table) = &focus.table {
            if !row.tables.contains(&format!("{}.{}", target.database, table)) {
                return Err(CollectionStop::SourceChanged.error());
            }
            let end = timestamp(request.ended_us)?;
            let window = Window {
                start: end - TimeDelta::microseconds(CLICKHOUSE_PART_CONTEXT_US),
                end: end + TimeDelta::microseconds(1),
            };
            let available = limit.saturating_sub(1).min(MAX_ROWS_PER_SOURCE);
            let mut params = window.parameters(available + 1);
            params.extend([("node", request.node.clone()), ("table", table.clone())]);
            self.queries += 1;
            match self.client.rows::<ClickHousePartEvent>(queries::PART_HISTORY, params).await {
                Ok(mut rows) => {
                    let complete = rows.len() <= available as usize;
                    rows.truncate(available as usize);
                    out.add(
                        "system.part_log",
                        rows.into_iter().map(ClickHouseObservation::PartEvent).collect(),
                        complete,
                    )?;
                }
                Err(error) => out.unavailable(&self.client, "system.part_log", error)?,
            }
        }
        Ok(out)
    }
}
