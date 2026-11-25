//! The `log-reuse-blocked` check: SQL Server transaction logs filling while `log_reuse_wait_desc` names an active
//! truncation blocker (`ACTIVE_TRANSACTION`, `REPLICATION`/CDC, `AVAILABILITY_REPLICA`, `LOG_BACKUP`, etc.) before
//! writes fail with Error 9002.

use super::*;
use crate::evidence::{Observation, Requirement as Evidence};
use culprit_core::operations::SpaceUsage;
use culprit_core::{Actionability, FileScope, key, t};

const LOG_REUSE_WARNING_PERCENT: u32 = 75;
const LOG_REUSE_HIGH_PERCENT: u32 = 85;

pub fn log_reuse_blocked_info() -> CheckInfo {
    CheckInfo {
        id: "log-reuse-blocked".into(),
        title: t!("check.log_reuse_blocked.info.title"),
        category: Category::Configuration,
        engines: vec![Engine::Mssql],
        requires: vec![],
        description: t!("check.log_reuse_blocked.info.description"),
    }
}

check!(
    LogReuseBlockedCheck: log_reuse_blocked_info,
    decide = blocked_logs,
    render = log_reuse_blocked_finding,
    case<'s> = BlockedLog<'s>,
    evidence = &[Evidence::Observed(Observation::Spaces, &[culprit_core::source_id::LOG_CAPACITY])],
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogReuseBlocker {
    ActiveTransaction,
    Replication,
    Replica,
    LogBackup,
    Other,
}

impl LogReuseBlocker {
    fn from_native(reason: &str) -> Option<Self> {
        let normalized = reason.trim().to_ascii_uppercase();
        if normalized.is_empty() || matches!(normalized.as_str(), "NOTHING" | "CHECKPOINT") {
            return None;
        }
        Some(match normalized.as_str() {
            "ACTIVE_TRANSACTION" | "XTP_CHECKPOINT" => Self::ActiveTransaction,
            "REPLICATION" => Self::Replication,
            "AVAILABILITY_REPLICA" | "DATABASE_MIRRORING" | "SLOG_SCAN" => Self::Replica,
            "LOG_BACKUP" | "ACTIVE_BACKUP_OR_RESTORE" => Self::LogBackup,
            _ => Self::Other,
        })
    }
}

pub struct BlockedLog<'s> {
    space: &'s SpaceUsage,
    used: f64,
    capacity: f64,
    free: f64,
    share: f64,
    constrained: bool,
    high: bool,
    reason: &'s str,
    blocker: LogReuseBlocker,
}

struct LogOccupancy {
    used: f64,
    capacity: f64,
    free: f64,
    share: f64,
    constrained: bool,
    high: bool,
}

fn exact_log_occupancy(space: &culprit_core::telemetry::OperationalSpace) -> Option<LogOccupancy> {
    let total = space.total_bytes.as_deref()?.parse::<u64>().ok()?;
    let used = space.used_bytes.as_deref()?.parse::<u64>().ok()?;
    if total == 0 || used > total {
        return None;
    }
    let limit = space.limit_bytes.as_deref().map(str::parse::<u64>).transpose().ok()?;
    let hard = space.growth_enabled == Some(false);
    let capacity = if hard { total } else { limit.filter(|limit| *limit >= total).unwrap_or(total) };
    let at_least = |percent: u32| u128::from(used) * 100 >= u128::from(capacity) * u128::from(percent);
    if !at_least(LOG_REUSE_WARNING_PERCENT) {
        return None;
    }
    let constrained = hard || limit.is_some_and(|limit| limit <= total);
    Some(LogOccupancy {
        used: used as f64,
        capacity: capacity as f64,
        free: (capacity - used) as f64,
        share: used as f64 / capacity as f64,
        constrained,
        high: constrained && at_least(LOG_REUSE_HIGH_PERCENT),
    })
}

fn chart_log_occupancy(space: &SpaceUsage) -> Option<LogOccupancy> {
    if !space.total_bytes.is_finite()
        || !space.used_bytes.is_finite()
        || space.total_bytes <= 0.0
        || space.used_bytes < 0.0
        || space.used_bytes > space.total_bytes
    {
        return None;
    }
    let hard = space.growth_enabled == Some(false);
    let capacity = if hard {
        space.total_bytes
    } else {
        space.limit_bytes.filter(|limit| *limit >= space.total_bytes).unwrap_or(space.total_bytes)
    };
    let share = space.used_bytes / capacity;
    if share < f64::from(LOG_REUSE_WARNING_PERCENT) / 100.0 {
        return None;
    }
    let constrained = hard || space.limit_bytes.is_some_and(|limit| limit <= space.total_bytes);
    Some(LogOccupancy {
        used: space.used_bytes,
        capacity,
        free: (capacity - space.used_bytes).max(0.0),
        share,
        constrained,
        high: constrained && share >= f64::from(LOG_REUSE_HIGH_PERCENT) / 100.0,
    })
}

fn blocked_logs(snapshot: &Snapshot) -> Vec<BlockedLog<'_>> {
    let Ok(evidence) = crate::storage::AllocationEvidence::from_snapshot(snapshot) else { return Vec::new() };
    snapshot
        .operations
        .spaces
        .iter()
        .filter(|space| space.kind == FileKind::Log && space.scope != FileScope::Server)
        .filter_map(|space| {
            let (occupancy, reuse_wait) = if let Some(native) = evidence.space(space) {
                (exact_log_occupancy(native)?, native.reuse_wait.as_deref())
            } else {
                (chart_log_occupancy(space)?, space.reuse_wait.as_deref())
            };
            let reason = reuse_wait?;
            let blocker = LogReuseBlocker::from_native(reason)?;
            // Routine LOG_BACKUP with autogrowth headroom below 85% is normal FULL-recovery cycling, not an incident.
            if blocker == LogReuseBlocker::LogBackup && !occupancy.constrained && occupancy.share < 0.85 {
                return None;
            }
            Some(BlockedLog {
                space,
                used: occupancy.used,
                capacity: occupancy.capacity,
                free: occupancy.free,
                share: occupancy.share,
                constrained: occupancy.constrained,
                high: occupancy.high,
                reason,
                blocker,
            })
        })
        .collect()
}

fn log_reuse_blocked_finding(_: &Snapshot, case: BlockedLog<'_>) -> Finding {
    let space = case.space;
    let mut f = finding(
        "log-reuse-blocked",
        &culprit_core::redact::shareable_name(&space.name),
        Category::Configuration,
        subject(SubjectKind::Database, &space.name),
    );
    let share = format::percent(case.share);
    f.title = t!("check.log_reuse_blocked.title", name = &space.name, share = &share, reason = case.reason);
    let summary_tail = if case.constrained {
        t!("check.log_reuse_blocked.summary.constrained")
    } else {
        t!("check.log_reuse_blocked.summary.can_grow", reason = case.reason)
    };
    f.summary = [
        t!(
            "check.log_reuse_blocked.summary.base",
            name = &space.name,
            used = format::bytes(case.used),
            capacity = format::bytes(case.capacity),
            free = format::bytes(case.free),
            reason = case.reason
        ),
        summary_tail,
    ]
    .join(" ");
    f.severity = if case.high { Severity::High } else { Severity::Medium };
    f.actionability = Actionability::NeedsAttention;
    f.confidence = Confidence::High;
    f.impact.score = base_score(f.severity);
    f.impact.set_label(t!("check.log_reuse_blocked.impact", share = &share, reason = case.reason));
    f.metrics = vec![
        Metric::said(key!("check.log_reuse_blocked.metric.used"), case.used, Unit::Bytes),
        Metric::said(key!("check.log_reuse_blocked.metric.capacity"), case.capacity, Unit::Bytes),
        Metric::said(key!("check.log_reuse_blocked.metric.free"), case.free, Unit::Bytes),
    ];
    let fix_summary = match case.blocker {
        LogReuseBlocker::ActiveTransaction => t!("check.log_reuse_blocked.fix.active_transaction"),
        LogReuseBlocker::Replication => t!("check.log_reuse_blocked.fix.replication"),
        LogReuseBlocker::Replica => t!("check.log_reuse_blocked.fix.replica"),
        LogReuseBlocker::LogBackup => t!("check.log_reuse_blocked.fix.log_backup"),
        LogReuseBlocker::Other => t!("check.log_reuse_blocked.fix.other", reason = case.reason),
    };
    f.fix = Some(Fix { summary: fix_summary, sql: None, cost: None });
    f.verify = vec![t!("check.log_reuse_blocked.verify")];
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_snapshot(used: f64, total: f64, growth_enabled: bool, reuse_wait: Option<&str>) -> Snapshot {
        let mut s: Snapshot =
            serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/mssql-demo.json")))
                .unwrap();
        s.operations = Default::default();
        s.operations.spaces.push(SpaceUsage {
            origin: None,
            name: "Transaction log".into(),
            kind: FileKind::Log,
            scope: FileScope::Database,
            total_bytes: total,
            used_bytes: used,
            parts: Vec::new(),
            growth_enabled: Some(growth_enabled),
            limit_bytes: Some(total),
            reuse_wait: reuse_wait.map(str::to_owned),
        });
        s
    }

    #[test]
    fn constrained_log_held_by_active_transaction_or_cdc_fires_high_before_error_9002() {
        for (reason, expected_phrase) in [
            ("ACTIVE_TRANSACTION", "DBCC OPENTRAN"),
            ("REPLICATION", "CDC"),
            ("AVAILABILITY_REPLICA", "Availability Group"),
        ] {
            let snap = log_snapshot(900.0, 1000.0, false, Some(reason));
            let findings = culprit_core::Check::run(&LogReuseBlockedCheck, &snap);
            assert_eq!(findings.len(), 1, "{reason}");
            assert_eq!(findings[0].severity, Severity::High, "{reason}");
            assert!(findings[0].title.contains(reason), "{reason}");
            assert!(findings[0].summary.contains("9002"), "{reason}");
            assert!(findings[0].fix.as_ref().unwrap().summary.contains(expected_phrase), "{reason}");
        }
    }

    #[test]
    fn benign_nothing_checkpoint_or_headroom_log_backup_never_false_flags() {
        assert!(blocked_logs(&log_snapshot(950.0, 1000.0, false, Some("NOTHING"))).is_empty());
        assert!(blocked_logs(&log_snapshot(950.0, 1000.0, false, Some("CHECKPOINT"))).is_empty());
        assert!(blocked_logs(&log_snapshot(500.0, 1000.0, false, Some("ACTIVE_TRANSACTION"))).is_empty());
        let mut growable = log_snapshot(800.0, 1000.0, true, Some("LOG_BACKUP"));
        growable.operations.spaces[0].limit_bytes = Some(10_000.0);
        assert!(blocked_logs(&growable).is_empty());
    }
}
