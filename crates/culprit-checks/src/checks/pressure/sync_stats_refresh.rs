//! The `sync-stats-refresh` check: Queries stalling on `WAIT_ON_SYNC_STATISTICS_REFRESH` while SQL Server updates
//! outdated statistics synchronously during compilation (especially with `AUTO_UPDATE_STATISTICS_ASYNC = OFF`).

use super::*;
use crate::evidence::{Observation, Requirement as Evidence};
use culprit_core::{QueryRef, source_id};

const SYNC_STATS_WAIT: &str = "WAIT_ON_SYNC_STATISTICS_REFRESH";
const MIN_TOTAL_WAIT_MS: f64 = 5_000.0;
const MIN_HOURLY_WAIT_MS: f64 = 1_000.0;
const HIGH_HOURLY_WAIT_MS: f64 = 60_000.0;

pub fn sync_stats_refresh_info() -> CheckInfo {
    CheckInfo {
        id: "sync-stats-refresh".into(),
        title: t!("check.sync_stats_refresh.info.title"),
        category: Category::Configuration,
        engines: vec![Engine::Mssql],
        requires: vec![],
        description: t!("check.sync_stats_refresh.info.description"),
    }
}

check!(
    SyncStatsRefreshCheck: sync_stats_refresh_info,
    decide = sync_stats_cases,
    render = sync_stats_finding,
    case<'s> = SyncStatsCase<'s>,
    evidence = &[Evidence::Observed(
        Observation::SyncStatsWaits,
        &[source_id::WAITS, source_id::QUERY_WAITS],
    )],
);

pub struct SyncStatsCase<'s> {
    wait_ms: f64,
    tasks: Option<f64>,
    async_off: bool,
    severity: Severity,
    queries: Vec<(&'s QueryStat, f64)>,
}

fn sync_stats_cases(snapshot: &Snapshot) -> Vec<SyncStatsCase<'_>> {
    if snapshot.server.engine != Engine::Mssql {
        return Vec::new();
    }
    let db_wait: (f64, Option<f64>) = snapshot
        .waits
        .iter()
        .filter(|w| w.wait_type.as_deref() == Some(SYNC_STATS_WAIT) && w.wait_ms.is_finite() && w.wait_ms > 0.0)
        .fold((0.0, None), |(ms, tasks), w| {
            let next_tasks = match (tasks, w.tasks.filter(|t| t.is_finite() && *t > 0.0)) {
                (Some(a), Some(b)) => Some(a + b),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            (ms + w.wait_ms, next_tasks)
        });

    let mut queries: Vec<(&QueryStat, f64)> = app_queries(snapshot)
        .filter_map(|q| {
            let ms: f64 = q
                .waits
                .iter()
                .filter(|w| w.wait_type.as_deref() == Some(SYNC_STATS_WAIT) && w.wait_ms.is_finite() && w.wait_ms > 0.0)
                .map(|w| w.wait_ms)
                .sum();
            (ms > 0.0).then_some((q, ms))
        })
        .collect();
    queries.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.id.cmp(&b.0.id)));
    let query_wait_ms: f64 = queries.iter().map(|(_, ms)| *ms).sum();
    queries.truncate(5);

    let wait_ms = db_wait.0.max(query_wait_ms);
    if wait_ms < MIN_TOTAL_WAIT_MS {
        return Vec::new();
    }
    let (hours, _) = waits_window(snapshot);
    if let Some(hours) = hours
        && wait_ms / hours < MIN_HOURLY_WAIT_MS
    {
        return Vec::new();
    }
    let async_off = snapshot.setting("is_auto_update_stats_async_on").is_some_and(|s| !crate::config::is_on(&s.value));
    let hourly = hours.map(|h| wait_ms / h);
    let severity = if async_off && hourly.is_some_and(|rate| rate >= HIGH_HOURLY_WAIT_MS) {
        Severity::High
    } else if async_off || hourly.is_some_and(|rate| rate >= MIN_HOURLY_WAIT_MS * 10.0) {
        Severity::Medium
    } else {
        Severity::Low
    };
    vec![SyncStatsCase { wait_ms, tasks: db_wait.1, async_off, severity, queries }]
}

fn sync_stats_finding(snapshot: &Snapshot, case: SyncStatsCase<'_>) -> Finding {
    let category = if case.async_off { Category::Configuration } else { Category::Queries };
    let mut f = finding(
        "sync-stats-refresh",
        &server_key(snapshot),
        category,
        subject(SubjectKind::Database, snapshot.server.database.clone()),
    );
    let waited = format::duration(case.wait_ms);
    f.title = if case.async_off {
        t!("check.sync_stats_refresh.title.async_off", waited = &waited)
    } else {
        t!("check.sync_stats_refresh.title.async_on", waited = &waited)
    };
    f.summary = if case.async_off {
        t!("check.sync_stats_refresh.summary.async_off", waited = &waited)
    } else {
        t!("check.sync_stats_refresh.summary.async_on", waited = &waited)
    };
    f.severity = case.severity;
    f.confidence = Confidence::High;
    f.impact.score = base_score(f.severity);
    f.impact.set_label(t!("check.sync_stats_refresh.impact", waited = &waited));
    f.metrics = vec![Metric::said(key!("check.sync_stats_refresh.metric.wait_ms"), case.wait_ms, Unit::Ms)];
    if let Some(tasks) = case.tasks {
        f.metrics.push(Metric::said(key!("check.sync_stats_refresh.metric.tasks"), tasks, Unit::Count));
    }
    f.queries = case
        .queries
        .into_iter()
        .map(|(q, ms)| QueryRef { id: q.id.clone(), plan_node: None, note: Some(format::duration(ms)) })
        .collect();
    let db = quote(Engine::Mssql, &snapshot.server.database);
    f.fix = Some(if case.async_off {
        Fix {
            summary: t!("check.sync_stats_refresh.fix.async_off"),
            sql: Some(format!("ALTER DATABASE {db} SET AUTO_UPDATE_STATISTICS_ASYNC ON;")),
            cost: Some(t!("check.sync_stats_refresh.fix_cost")),
        }
    } else {
        Fix { summary: t!("check.sync_stats_refresh.fix.async_on"), sql: None, cost: None }
    });
    f.verify = vec![t!("check.sync_stats_refresh.verify")];
    f
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_core::{Setting, WaitStat};

    fn mssql_snapshot(wait_ms: f64, async_value: Option<&str>, uptime_hours: Option<f64>) -> Snapshot {
        let mut s: Snapshot =
            serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/mssql-demo.json")))
                .unwrap();
        s.waits = vec![WaitStat {
            category: "Compilation".into(),
            wait_type: Some(SYNC_STATS_WAIT.into()),
            wait_ms,
            tasks: Some(18.0),
            mechanism: None,
        }];
        s.settings.retain(|setting| setting.name != "is_auto_update_stats_async_on");
        if let Some(val) = async_value {
            s.settings.push(Setting {
                name: "is_auto_update_stats_async_on".into(),
                value: val.into(),
                default_value: None,
                scope: "database".into(),
            });
        }
        s.pressure.get_or_insert_with(Pressure::default).uptime_hours = uptime_hours;
        if let Some(w) = &mut s.workload {
            for q in &mut w.queries {
                q.waits.clear();
            }
        }
        s
    }

    #[test]
    fn sync_stats_refresh_stalls_recommend_async_auto_stats_when_off() {
        let snap = mssql_snapshot(120_000.0, Some("0"), Some(1.5));
        let findings = culprit_core::Check::run(&SyncStatsRefreshCheck, &snap);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::High);
        assert!(findings[0].summary.contains("AUTO_UPDATE_STATISTICS_ASYNC"));
        assert!(
            findings[0].fix.as_ref().unwrap().sql.as_deref().unwrap().contains("SET AUTO_UPDATE_STATISTICS_ASYNC ON;")
        );
    }

    #[test]
    fn minor_historical_blips_over_long_uptime_do_not_false_flag() {
        let snap = mssql_snapshot(10_000.0, Some("0"), Some(720.0));
        assert!(sync_stats_cases(&snap).is_empty());
    }
}
