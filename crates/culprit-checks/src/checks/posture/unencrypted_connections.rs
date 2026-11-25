//! The `unencrypted-connections` check: TLS transport encryption is not enforced or unencrypted client sessions are active.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

pub fn unencrypted_connections_info() -> CheckInfo {
    CheckInfo {
        id: "unencrypted-connections".into(),
        title: t!("check.unencrypted_connections.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Yugabyte, Engine::Mysql],
        requires: vec![],
        description: t!("check.unencrypted_connections.info.description"),
    }
}

check!(
    UnencryptedConnectionsCheck: unencrypted_connections_info,
    decide = decide_unencrypted,
    render = render_unencrypted,
    case<'s> = UnencryptedCase,
    evidence = &[Evidence::Posture(PostureMeasure::TransportEncryption)],
);

pub struct UnencryptedCase {
    severity: Severity,
    unencrypted_sessions: Option<u64>,
    min_tls_version: Option<String>,
}

fn decide_unencrypted(snapshot: &Snapshot) -> Vec<UnencryptedCase> {
    if !snapshot.server.engine.has_server() {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    let sessions = posture.unencrypted_sessions.filter(|n| *n > 0);
    let weak_tls = posture.tls_enforced == Some(false);
    if !weak_tls && sessions.is_none() {
        return Vec::new();
    }
    let severity =
        if snapshot.environment.is_production() || sessions.is_some() { Severity::High } else { Severity::Medium };
    vec![UnencryptedCase { severity, unencrypted_sessions: sessions, min_tls_version: posture.min_tls_version.clone() }]
}

fn render_unencrypted(snapshot: &Snapshot, case: UnencryptedCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("unencrypted-connections", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = Actionability::NeedsAttention;
    f.confidence = Confidence::High;
    f.title = match case.unencrypted_sessions {
        Some(count) => {
            t!("check.unencrypted_connections.title.sessions", database = db, count = format::count(count as f64))
        }
        None => t!("check.unencrypted_connections.title.policy", database = db),
    };
    f.summary = match (&case.unencrypted_sessions, &case.min_tls_version) {
        (Some(count), _) => {
            t!("check.unencrypted_connections.summary.sessions", database = db, count = format::count(*count as f64))
        }
        (None, Some(ver)) => t!("check.unencrypted_connections.summary.tls_version", database = db, version = ver),
        (None, None) => t!("check.unencrypted_connections.summary.not_enforced", database = db),
    };
    if let Some(count) = case.unencrypted_sessions {
        f.metrics.push(Metric::said(
            culprit_core::key!("check.unencrypted_connections.metric.sessions"),
            count as f64,
            Unit::Count,
        ));
    }
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.unencrypted_connections.impact"));
    f.fix = Some(Fix { summary: t!("check.unencrypted_connections.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.unencrypted_connections.verify")];
    f
}
