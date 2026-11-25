//! The `integrity-check-overdue` check: physical database consistency check (`DBCC CHECKDB` / checksum verification) is overdue.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

const MAX_INTEGRITY_AGE_DAYS: f64 = 14.0;

pub fn integrity_check_overdue_info() -> CheckInfo {
    CheckInfo {
        id: "integrity-check-overdue".into(),
        title: t!("check.integrity_check_overdue.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Mysql],
        requires: vec![],
        description: t!("check.integrity_check_overdue.info.description"),
    }
}

check!(
    IntegrityCheckOverdueCheck: integrity_check_overdue_info,
    decide = decide_integrity_overdue,
    render = render_integrity_overdue,
    case<'s> = IntegrityOverdueCase,
    evidence = &[Evidence::Posture(PostureMeasure::IntegrityCheck)],
);

pub struct IntegrityOverdueCase {
    severity: Severity,
    days_since_check: f64,
}

fn decide_integrity_overdue(snapshot: &Snapshot) -> Vec<IntegrityOverdueCase> {
    if !matches!(snapshot.server.engine, Engine::Mssql | Engine::Postgres | Engine::Mysql) {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    let Some(days) = posture.last_integrity_check_days.filter(|d| d.is_finite() && *d > MAX_INTEGRITY_AGE_DAYS) else {
        return Vec::new();
    };
    let severity = if snapshot.environment.is_production() && days > 30.0 { Severity::High } else { Severity::Medium };
    vec![IntegrityOverdueCase { severity, days_since_check: days }]
}

fn render_integrity_overdue(snapshot: &Snapshot, case: IntegrityOverdueCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("integrity-check-overdue", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = Actionability::NeedsAttention;
    f.confidence = Confidence::High;
    let days_str = format::count(case.days_since_check.round());
    f.title = t!("check.integrity_check_overdue.title", database = db, days = &days_str);
    f.summary = t!("check.integrity_check_overdue.summary", database = db, days = &days_str);
    f.metrics.push(Metric::said(
        culprit_core::key!("check.integrity_check_overdue.metric.days"),
        case.days_since_check,
        Unit::Days,
    ));
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.integrity_check_overdue.impact", days = &days_str));
    f.fix = Some(Fix { summary: t!("check.integrity_check_overdue.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.integrity_check_overdue.verify")];
    f
}
