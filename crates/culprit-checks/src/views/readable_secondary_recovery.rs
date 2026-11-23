//! Pure interpretation of authenticated native recovery context. Long-running reads and aggregate deltas are not causes.
use culprit_core::{Result, telemetry::*};
pub fn explain(
    answer: &ReadableSecondaryRecoveryAnswer,
    point: Option<&ReadableSecondaryRecoveryPoint>,
) -> Result<ReadableSecondaryRecoveryReport> {
    if !answer.is_valid() {
        return Err(CollectionStop::InvalidPage.error());
    }
    let mut report = ReadableSecondaryRecoveryReport {
        source_state: answer.source_state,
        summary: culprit_core::t!("view.secondary_recovery.unavailable"),
        investigation: culprit_core::t!("view.secondary_recovery.action_unknown"),
        verify: culprit_core::t!("view.secondary_recovery.verify"),
        readers: vec![],
        conflict_changes: vec![],
        limitations: vec![
            culprit_core::t!("view.secondary_recovery.limit.cause"),
            culprit_core::t!("view.secondary_recovery.limit.sql"),
            culprit_core::t!("view.secondary_recovery.limit.endpoints"),
        ],
    };
    if answer.source_state == culprit_core::SourceState::Partial {
        report.limitations.push(culprit_core::t!("view.secondary_recovery.limit.partial"));
    }
    let Some(point) = point else { return Ok(report) };
    if !answer.matches_point(point) {
        return Err(CollectionStop::InvalidPage.error());
    }
    report.readers = point.native_blocking_readers();
    report.conflict_changes = point.conflict_changes()?;
    match &point.native {
        ReadableSecondaryRecoveryNative::Postgres { before: Some(_), after: Some(_) } => {
            report.summary = culprit_core::t!("view.secondary_recovery.postgres.summary");
            report.investigation = culprit_core::t!("view.secondary_recovery.postgres.action");
            report.limitations.push(culprit_core::t!("view.secondary_recovery.postgres.limit"));
            for change in &report.conflict_changes {
                report.limitations.push(match change {
                    RecoveryConflictChange::Observed { kind, from, until, reset_at, change, .. } => culprit_core::t!(
                        "view.secondary_recovery.delta",
                        counter = kind.native_name(),
                        count = culprit_core::format::digits(change.clone()),
                        from = from.as_str(),
                        until = until.as_str(),
                        reset = reset_at.as_str()
                    ),
                    RecoveryConflictChange::Unavailable { kind, .. } => {
                        culprit_core::t!("view.secondary_recovery.delta_unavailable", counter = kind.native_name())
                    }
                });
            }
        }
        ReadableSecondaryRecoveryNative::Mssql { before: Some(_), after: Some(_) } => {
            report.summary = culprit_core::t!("view.secondary_recovery.mssql.summary");
            report.investigation = culprit_core::t!("view.secondary_recovery.mssql.action");
        }
        _ => {}
    }
    if point.request_end != BlockingRequestState::SameStatement {
        report.limitations.push(culprit_core::t!("view.secondary_recovery.limit.request"));
    }
    if report.readers.is_empty() {
        report.limitations.push(culprit_core::t!("view.secondary_recovery.limit.owner"));
    }
    Ok(report)
}
pub(super) fn answer_reasons(answer: &ReadableSecondaryRecoveryAnswer) -> Vec<String> {
    vec![
        culprit_core::t!("view.secondary_recovery.question"),
        if answer.observation.is_some() {
            culprit_core::t!("view.secondary_recovery.retained")
        } else {
            culprit_core::t!("view.secondary_recovery.unavailable")
        },
        culprit_core::t!("view.secondary_recovery.limit.cause"),
    ]
}
