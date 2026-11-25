//! The `backup-retention-short` check: automated backup retention is shorter than policy or full backups are stale.

use super::*;
use crate::evidence::{PostureMeasure, Requirement as Evidence};
use culprit_core::t;

const MIN_PROD_RETENTION_DAYS: u32 = 7;
const MAX_FULL_BACKUP_AGE_DAYS: f64 = 7.0;

pub fn backup_retention_short_info() -> CheckInfo {
    CheckInfo {
        id: "backup-retention-short".into(),
        title: t!("check.backup_retention_short.info.title"),
        category: Category::Posture,
        engines: vec![Engine::Mssql, Engine::Postgres, Engine::Yugabyte, Engine::Mysql],
        requires: vec![],
        description: t!("check.backup_retention_short.info.description"),
    }
}

check!(
    BackupRetentionShortCheck: backup_retention_short_info,
    decide = decide_backup_retention,
    render = render_backup_retention,
    case<'s> = BackupRetentionCase,
    evidence = &[Evidence::Posture(PostureMeasure::BackupRetention)],
);

pub struct BackupRetentionCase {
    severity: Severity,
    retention_days: Option<u32>,
    last_full_days: Option<f64>,
}

fn decide_backup_retention(snapshot: &Snapshot) -> Vec<BackupRetentionCase> {
    if !snapshot.server.engine.has_server() {
        return Vec::new();
    }
    let Some(posture) = snapshot.posture() else {
        return Vec::new();
    };
    let min_days = if snapshot.environment.is_production() { MIN_PROD_RETENTION_DAYS } else { 1 };
    let short_retention = posture.backup_retention_days.filter(|d| *d < min_days);
    let stale_full = posture.last_full_backup_days.filter(|age| age.is_finite() && *age > MAX_FULL_BACKUP_AGE_DAYS);
    if short_retention.is_none() && stale_full.is_none() {
        return Vec::new();
    }
    let severity = if snapshot.environment.is_production()
        && (short_retention == Some(0) || stale_full.is_some_and(|age| age > 14.0))
    {
        Severity::High
    } else {
        Severity::Medium
    };
    vec![BackupRetentionCase { severity, retention_days: short_retention, last_full_days: stale_full }]
}

fn render_backup_retention(snapshot: &Snapshot, case: BackupRetentionCase) -> Finding {
    let db = &snapshot.server.database;
    let mut f = finding("backup-retention-short", db, Category::Posture, subject(SubjectKind::Database, db));
    f.severity = case.severity;
    f.actionability = Actionability::NeedsAttention;
    f.confidence = Confidence::High;
    match (case.retention_days, case.last_full_days) {
        (Some(days), _) => {
            let days_str = format::count(f64::from(days));
            f.title = t!("check.backup_retention_short.title.retention", database = db, days = &days_str);
            f.summary = t!("check.backup_retention_short.summary.retention", database = db, days = &days_str);
            f.metrics.push(Metric::said(
                culprit_core::key!("check.backup_retention_short.metric.retention_days"),
                f64::from(days),
                Unit::Days,
            ));
        }
        (None, Some(age)) => {
            let age_str = format::count(age.round());
            f.title = t!("check.backup_retention_short.title.stale_backup", database = db, days = &age_str);
            f.summary = t!("check.backup_retention_short.summary.stale_backup", database = db, days = &age_str);
            f.metrics.push(Metric::said(
                culprit_core::key!("check.backup_retention_short.metric.last_backup_days"),
                age,
                Unit::Days,
            ));
        }
        (None, None) => unreachable!(),
    }
    f.impact.score = base_score(f.severity);
    f.impact.db_time_share = None;
    f.impact.time_ms_per_day = None;
    f.impact.set_label(t!("check.backup_retention_short.impact"));
    f.fix = Some(Fix { summary: t!("check.backup_retention_short.fix"), sql: None, cost: None });
    f.verify = vec![t!("check.backup_retention_short.verify")];
    f
}
