use crate::{
    client::{Client, Parameters, invalid_response},
    identity::{Identity, instant, parameter, parse_utc},
    queries,
};
use chrono::{DateTime, TimeDelta, Utc};
use culprit_core::telemetry::{
    ClickHouseObservation, ClickHousePopulation, ClickHouseQueryFamily, DefinitionPayload, PayloadUnavailable,
};
use culprit_core::{Result, SnapshotOptions, Workload};
use serde::Deserialize;

const SAMPLE_BYTES: usize = 4096;

pub(crate) struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
impl Window {
    pub fn ending(end: DateTime<Utc>, minutes: u32) -> Result<Self> {
        let start =
            end.checked_sub_signed(TimeDelta::minutes(i64::from(minutes.max(1)))).ok_or_else(invalid_response)?;
        Ok(Self { start, end })
    }
    pub fn parameters(&self, rows: u32) -> Parameters {
        vec![("since", parameter(self.start)), ("until", parameter(self.end)), ("row_limit", rows.to_string())]
    }
}
#[derive(Deserialize)]
struct Family {
    node: String,
    family: String,
    executions: u64,
    failures: u64,
    elapsed_ms: u64,
    max_ms: u64,
    rows_read: u64,
    bytes_read: u64,
    rows_returned: u64,
    first_started: String,
    last_started: String,
    sample_sql: String,
    sample_complete: u8,
    population_executions: u64,
    population_elapsed_ms: u64,
    population_families: u64,
}
impl Family {
    fn population(&self) -> ClickHousePopulation {
        ClickHousePopulation {
            executions: self.population_executions,
            elapsed_ms: self.population_elapsed_ms,
            families: self.population_families,
        }
    }
    fn fact(&self, owner: &Identity, window: &Window, sample: DefinitionPayload) -> Result<ClickHouseQueryFamily> {
        if self.executions == 0 {
            return Err(invalid_response());
        }
        Ok(ClickHouseQueryFamily {
            node: self.node.clone(),
            database: owner.database.clone(),
            family: self.family.clone(),
            from: instant(window.start),
            until: instant(window.end),
            first_seen: instant(parse_utc(&self.first_started)?),
            last_seen: instant(parse_utc(&self.last_started)?),
            population: self.population(),
            sample,
            executions: self.executions,
            failures: self.failures,
            elapsed_ms: self.elapsed_ms,
            max_ms: self.max_ms,
            rows_read: self.rows_read,
            bytes_read: self.bytes_read,
            rows_returned: self.rows_returned,
        })
    }
}
async fn families(
    client: &Client,
    identity: &Identity,
    window: &Window,
    limit: u32,
    text_bytes: usize,
) -> Result<Vec<Family>> {
    let mut params = window.parameters(limit);
    params.push(("text_bytes", text_bytes.to_string()));
    let rows: Vec<Family> = client.rows(queries::WORKLOAD, params).await?;
    if rows.iter().any(|row| row.node != identity.node) {
        return Err(invalid_response());
    }
    Ok(rows)
}

pub(crate) async fn read(
    client: &Client,
    identity: &Identity,
    options: &SnapshotOptions,
    window: &Window,
) -> Result<Workload> {
    let limit = options.top_queries.clamp(1, 500);
    let rows = families(client, identity, window, limit, SAMPLE_BYTES).await?;
    let population = rows.first().map(Family::population).unwrap_or_default();
    let queries = rows
        .into_iter()
        .take(options.top_queries as usize)
        .map(|row| {
            let metrics = row.fact(
                identity,
                window,
                DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            )?;
            Ok(metrics.statement(row.sample_sql))
        })
        .collect::<Result<_>>()?;
    Ok(population.workload(
        instant(window.start),
        instant(window.end),
        f64::from(options.window_in_minutes()) / 60.0,
        queries,
    ))
}

pub(crate) async fn archive(
    client: &Client,
    owner: &Identity,
    window: &Window,
    limit: u32,
) -> Result<crate::health::Capture> {
    let mut rows = families(client, owner, window, limit + 1, SAMPLE_BYTES).await?;
    let complete = rows.len() <= limit as usize;
    rows.truncate(limit as usize);
    let rows = rows
        .into_iter()
        .map(|row| {
            // A truncated SQL token is unsuitable for redaction. Retain the numeric family in that case.
            let sample = if row.sample_sql.is_empty() {
                DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing }
            } else if row.sample_complete != 1 {
                DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized }
            } else {
                DefinitionPayload::sql(&row.sample_sql, culprit_core::Engine::Clickhouse)
            };
            Ok(ClickHouseObservation::QueryFamily(row.fact(owner, window, sample)?))
        })
        .collect::<Result<_>>()?;
    Ok(crate::health::Capture { rows, complete })
}

/// Compare the top retained families in each requested period. Missing families have unknown coverage:
/// the query log can be sampled, asynchronously flushed or expired independently of the read cap.
pub(crate) async fn periods(
    client: &Client,
    baseline: &culprit_core::changes::Period,
    recent: &culprit_core::changes::Period,
) -> Result<culprit_core::changes::PeriodData> {
    use culprit_core::{
        TimelineSource,
        changes::{PeriodData, PeriodStats, QueryPeriods},
        t,
    };
    let session = client.session()?;
    let owner = Identity::read(&session).await?;
    let mut data = PeriodData {
        source: TimelineSource::ClickHouseLog,
        baseline: baseline.clone(),
        recent: recent.clone(),
        notes: vec![t!("provider.clickhouse.timeline")],
        ..Default::default()
    };
    let mut queries = std::collections::BTreeMap::<String, QueryPeriods>::new();
    for (period, older) in [(baseline, true), (recent, false)] {
        let timestamp = |s: &str| DateTime::parse_from_rfc3339(s).map(|t| t.to_utc()).map_err(|_| invalid_response());
        let window = Window { start: timestamp(&period.start)?, end: timestamp(&period.end)? };
        if window.end <= window.start {
            return Err(invalid_response());
        }
        let rows = families(&session, &owner, &window, 500, SAMPLE_BYTES).await?;
        let total = rows
            .first()
            .map(|r| PeriodStats::new(r.population_executions as f64, r.population_elapsed_ms as f64))
            .unwrap_or_default();
        if older {
            data.baseline_total = total;
        } else {
            data.recent_total = total;
        }
        for row in rows {
            let query = queries.entry(row.family.clone()).or_insert_with(|| QueryPeriods {
                query_id: row.family,
                text: row.sample_sql,
                untracked: Some(t!("provider.clickhouse.timeline")),
                ..Default::default()
            });
            let mut stats = PeriodStats::new(row.executions as f64, row.elapsed_ms as f64);
            stats.max_ms = Some(row.max_ms as f64);
            if older {
                query.baseline = Some(stats);
            } else {
                query.recent = Some(stats);
            }
        }
    }
    // The selected log rows do not establish the fraction of the database workload that was logged.
    data.queries = queries.into_values().collect();
    Ok(data)
}
