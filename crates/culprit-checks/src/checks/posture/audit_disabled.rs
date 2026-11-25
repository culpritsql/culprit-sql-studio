//! The `audit-disabled` check: server or database audit logging is disabled.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

pub fn audit_disabled_info() -> CheckInfo {
    CheckInfo {
        id: "audit-disabled".into(),
        title: t!("check.audit_disabled.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Mysql],
        requires: vec![],
        description: t!("check.audit_disabled.info.description"),
    }
}

check!(
    AuditDisabledCheck: audit_disabled_info,
    decide = decide_audit_disabled,
    render = render_audit_disabled,
    case<'s> = AuditDisabledCase,
    evidence = &[Evidence::Posture(PostureMeasure::Auditing)],
);

pub struct AuditDisabledCase {
    severity: Severity,
}

fn decide_audit_disabled(snapshot: &Snapshot) -> Vec<AuditDisabledCase> {
    if !matches!(snapshot.server.engine, Engine::Mssql | Engine::Postgres | Engine::Mysql) {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    if posture.auditing_enabled != Some(false) {
        return Vec::new();
    }
    let severity = if snapshot.environment.is_production() { Severity::Medium } else { Severity::Low };
    vec![AuditDisabledCase { severity }]
}

fn render_audit_disabled(snapshot: &Snapshot, case: AuditDisabledCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("audit-disabled", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = if snapshot.environment.is_production() {
        Actionability::NeedsAttention
    } else {
        Actionability::OptionalImprovement
    };
    f.confidence = Confidence::High;
    f.title = t!("check.audit_disabled.title", database = db);
    f.summary = t!("check.audit_disabled.summary", database = db, product = snapshot.server.name());
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.audit_disabled.impact"));
    f.fix = Some(Fix { summary: t!("check.audit_disabled.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.audit_disabled.verify")];
    f
}
