//! Compare two reads of the same closed log window before attaching family trends to a scan.
use crate::{
    client::{Client, invalid_response},
    identity::{Identity, instant},
    queries,
    workload::Window,
};
use chrono::TimeDelta;
use culprit_core::{QueryTrend, Result, Timeline, TimelineSource, Workload};
use serde::Deserialize;
use std::collections::BTreeMap;

const MAX_BUCKETS: i64 = 72;
const MAX_FAMILIES: usize = 32;
const MIN_BUCKET_SECONDS: i64 = 60;

#[derive(Deserialize)]
struct Bucket {
    node: String,
    bucket: usize,
    family: String,
    executions: u64,
    elapsed_ms: u64,
}

pub(crate) async fn attach(client: &Client, owner: &Identity, window: &Window, workload: &mut Workload) -> Result<()> {
    let duration = (window.end - window.start).num_seconds();
    let width = ((duration + MAX_BUCKETS - 1) / MAX_BUCKETS).max(MIN_BUCKET_SECONDS);
    let count = ((duration + width - 1) / width) as usize;
    let mut trends = BTreeMap::new();
    let empty = QueryTrend { time_ms: vec![Some(0.0); count], executions: vec![Some(0.0); count] };
    for query in workload.queries.iter().take(MAX_FAMILIES) {
        let family = query.id.parse::<u64>().map_err(|_| invalid_response())?;
        trends.insert(family.to_string(), empty.clone());
    }
    let ids = trends.keys().cloned().collect::<Vec<_>>().join(",");
    let mut params = window.parameters(0);
    params.extend([("width_us", (width * 1_000_000).to_string()), ("families", format!("[{ids}]"))]);
    let rows: Vec<Bucket> = client.rows(queries::TIMELINE, params).await?;
    let mut timeline = Timeline {
        source: TimelineSource::ClickHouseLog,
        starts: (0..count).map(|i| instant(window.start + TimeDelta::seconds(width * i as i64))).collect(),
        end: instant(window.end),
        time_ms: vec![0.0; count],
        executions: vec![0.0; count],
        unknown: None,
    };
    for row in rows {
        if row.node != owner.node || row.bucket >= count || row.executions == 0 {
            return Err(invalid_response());
        }
        timeline.time_ms[row.bucket] += row.elapsed_ms as f64;
        timeline.executions[row.bucket] += row.executions as f64;
        if !row.family.is_empty() {
            let trend = trends.get_mut(&row.family).ok_or_else(invalid_response)?;
            trend.time_ms[row.bucket] = Some(row.elapsed_ms as f64);
            trend.executions[row.bucket] = Some(row.executions as f64);
        }
    }
    if timeline.time_ms.iter().sum::<f64>() != workload.total_time_ms
        || timeline.executions.iter().sum::<f64>() != workload.total_executions
        || workload.queries.iter().take(MAX_FAMILIES).any(|query| {
            !trends.get(&query.id).is_some_and(|trend| {
                trend.time_ms.iter().flatten().sum::<f64>() == query.total_time_ms
                    && trend.executions.iter().flatten().sum::<f64>() == query.executions
            })
        })
    {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::QueryFailed,
            culprit_core::message!("provider.clickhouse.window_changed"),
        ));
    }
    timeline.unknown = Some(timeline.executions.iter().map(|n| *n == 0.0).collect());
    for query in &mut workload.queries {
        query.trend = trends.remove(&query.id);
    }
    workload.timeline = Some(timeline);
    Ok(())
}
