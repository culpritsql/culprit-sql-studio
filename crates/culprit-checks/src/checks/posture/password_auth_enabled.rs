//! The `password-auth-enabled` check: local/SQL password authentication remains enabled where managed identity (Entra / IAM) is available.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

pub fn password_auth_enabled_info() -> CheckInfo {
    CheckInfo {
        id: "password-auth-enabled".into(),
        title: t!("check.password_auth_enabled.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Mysql],
        requires: vec![],
        description: t!("check.password_auth_enabled.info.description"),
    }
}

check!(
    PasswordAuthEnabledCheck: password_auth_enabled_info,
    decide = decide_password_auth,
    render = render_password_auth,
    case<'s> = PasswordAuthCase,
    evidence = &[Evidence::Posture(PostureMeasure::PasswordAuth)],
);

pub struct PasswordAuthCase {
    severity: Severity,
}

fn decide_password_auth(snapshot: &Snapshot) -> Vec<PasswordAuthCase> {
    if !matches!(snapshot.server.engine, Engine::Mssql | Engine::Postgres | Engine::Mysql) {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    if posture.password_auth_enabled != Some(true) {
        return Vec::new();
    }
    let severity = if snapshot.environment.is_production() { Severity::Medium } else { Severity::Low };
    vec![PasswordAuthCase { severity }]
}

fn render_password_auth(snapshot: &Snapshot, case: PasswordAuthCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("password-auth-enabled", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = Actionability::OptionalImprovement;
    f.confidence = Confidence::High;
    f.title = t!("check.password_auth_enabled.title", database = db);
    f.summary = t!("check.password_auth_enabled.summary", database = db, product = snapshot.server.name());
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.password_auth_enabled.impact"));
    f.fix = Some(Fix { summary: t!("check.password_auth_enabled.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.password_auth_enabled.verify")];
    f
}
