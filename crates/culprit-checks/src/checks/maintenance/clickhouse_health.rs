use super::*;
use crate::evidence::{Observation, Requirement as Evidence};
use culprit_core::telemetry::{ClickHouseIncident, ClickHouseObservation};

pub fn clickhouse_health_info() -> CheckInfo {
    CheckInfo {
        id: "clickhouse-health".into(),
        title: t!("provider.clickhouse.check"),
        category: Category::Maintenance,
        engines: vec![Engine::Clickhouse],
        requires: vec![],
        description: t!("provider.clickhouse.check_description"),
    }
}
check!(ClickHouseHealthCheck: clickhouse_health_info, decide = cases, render = render, case<'s> = Case<'s>,
    evidence = &[Evidence::Observed(Observation::ClickHouse,culprit_core::telemetry::CLICKHOUSE_INCIDENT_SOURCES)],);

pub struct Case<'s> {
    point: &'s ClickHouseObservation,
    incident: ClickHouseIncident,
}
fn cases(snapshot: &Snapshot) -> Vec<Case<'_>> {
    snapshot
        .operations
        .clickhouse
        .iter()
        .filter_map(|point| Some(Case { point, incident: point.incident()? }))
        .collect()
}
fn render(snapshot: &Snapshot, case: Case<'_>) -> Finding {
    let identity = case.point.identity();
    let label = |key: &str| identity.qualifiers.get(key).map(String::as_str).unwrap_or_default();
    let node = label("node");
    let database = label("database");
    let (kind, subject_name) = match case.point {
        ClickHouseObservation::Failure(row) => (SubjectKind::Query, row.family.clone()),
        _ => (SubjectKind::Table, format!("{database}.{}", label("table"))),
    };
    let mut result = finding(
        "clickhouse-health",
        &identity.key().expect("a native identity serializes"),
        Category::Maintenance,
        subject(kind, &subject_name),
    );
    if let ClickHouseObservation::Failure(row) = case.point
        && let Some(query) = snapshot
            .workload
            .as_ref()
            .and_then(|workload| workload.queries.iter().find(|query| query.query_hash.as_ref() == Some(&row.family)))
    {
        result.queries.push(culprit_core::QueryRef { id: query.id.clone(), plan_node: None, note: None });
    }
    result.title = case.incident.title();
    result.summary = case.point.summary();
    result.confidence = Confidence::High;
    result.severity = Severity::Medium;
    result.impact.score = base_score(result.severity);
    result.fix = Some(Fix { summary: case.incident.followup(), sql: None, cost: None });
    result.verify = vec![t!("provider.clickhouse.verify", node = node)];
    result
}
