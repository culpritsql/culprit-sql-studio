//! The `public-endpoint` check: database server exposes a public network endpoint instead of private-only connectivity.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

pub fn public_endpoint_info() -> CheckInfo {
    CheckInfo {
        id: "public-endpoint".into(),
        title: t!("check.public_endpoint.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Yugabyte, Engine::Mysql],
        requires: vec![],
        description: t!("check.public_endpoint.info.description"),
    }
}

check!(
    PublicEndpointCheck: public_endpoint_info,
    decide = decide_public_endpoint,
    render = render_public_endpoint,
    case<'s> = PublicEndpointCase,
    evidence = &[Evidence::Posture(PostureMeasure::PublicEndpoint)],
);

pub struct PublicEndpointCase {
    severity: Severity,
}

fn decide_public_endpoint(snapshot: &Snapshot) -> Vec<PublicEndpointCase> {
    if !snapshot.server.engine.has_server() {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    if posture.public_network_access != Some(true) {
        return Vec::new();
    }
    let severity = if snapshot.environment.is_production() { Severity::High } else { Severity::Medium };
    vec![PublicEndpointCase { severity }]
}

fn render_public_endpoint(snapshot: &Snapshot, case: PublicEndpointCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("public-endpoint", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = if snapshot.environment.is_production() {
        Actionability::NeedsAttention
    } else {
        Actionability::OptionalImprovement
    };
    f.confidence = Confidence::High;
    f.title = t!("check.public_endpoint.title", database = db);
    f.summary = t!("check.public_endpoint.summary", database = db, product = snapshot.server.name());
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.public_endpoint.impact"));
    f.fix = Some(Fix { summary: t!("check.public_endpoint.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.public_endpoint.verify")];
    f
}
