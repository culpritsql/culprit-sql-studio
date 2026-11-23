//! Render positive native mechanisms and conditional diagnostics once; shared work never becomes query blame.
use super::*;

pub(in crate::views::commit_pipeline) fn render(report: &mut CommitPipelineReport) {
    let mut mechanisms = BTreeSet::new();
    for interval in &report.intervals {
        for mechanism in &interval.mechanisms {
            let kind = counter(*mechanism);
            let Some(value) = interval.values.iter().find(|value| value.kind == kind) else { continue };
            let MeasurementValue::Integer(value) = &value.value else { continue };
            let text =
                culprit_core::i18n::t(summary_key(*mechanism), &[("count", format::digits(value.clone()).into())]);
            super::super::words::append(&mut report.context_summaries, &mut report.facts_limited, text);
            mechanisms.insert(*mechanism);
        }
        for value in &interval.values {
            let key = match value.kind {
                CommitPipelineCounterKind::WriteTime => key!("view.commit_pipeline.interval.time.write"),
                CommitPipelineCounterKind::SyncTime => key!("view.commit_pipeline.interval.time.sync"),
                _ => continue,
            };
            if let Some(milliseconds) = value.value.exact_real().filter(|value| *value > 0.0) {
                super::super::words::append(
                    &mut report.context_summaries,
                    &mut report.facts_limited,
                    culprit_core::i18n::t(key, &[("time", format::duration(milliseconds).into())]),
                );
            }
        }
    }
    if !report.intervals.is_empty() {
        report.limitations.push(t!("view.commit_pipeline.interval.limit.bracket"));
        report.limitations.push(t!("view.commit_pipeline.interval.limit.timing"));
    }
    if report.intervals.iter().any(|interval| {
        matches!(interval.scope_basis, CommitPipelineIntervalScope::SharedServerIndependentOfDatabaseRole)
    }) {
        report.limitations.push(t!("view.commit_pipeline.interval.limit.shared"));
    }
    let mut gap_keys = BTreeSet::new();
    for gap in &report.interval_gaps {
        gap_keys.insert(gap_key(gap.reason));
    }
    for key in gap_keys {
        report.limitations.push(culprit_core::i18n::t(key, &[]));
    }
    let action = match report.stage {
        Some(CommitPipelineStage::LocalLogSync)
            if mechanisms.contains(&CommitPipelineIntervalMechanism::LocalSyncWork) =>
        {
            Some(key!("view.commit_pipeline.interval.action.sync"))
        }
        Some(CommitPipelineStage::LocalLogWrite)
            if mechanisms.contains(&CommitPipelineIntervalMechanism::LocalWriteWork) =>
        {
            Some(key!("view.commit_pipeline.interval.action.write"))
        }
        Some(CommitPipelineStage::LocalLogFlush)
            if mechanisms.contains(&CommitPipelineIntervalMechanism::NativeFlushWork) =>
        {
            Some(key!("view.commit_pipeline.interval.action.flush"))
        }
        Some(CommitPipelineStage::LogBufferSpace)
            if mechanisms.iter().any(|mechanism| {
                matches!(
                    mechanism,
                    CommitPipelineIntervalMechanism::NativeBufferFullWrites
                        | CommitPipelineIntervalMechanism::NativeLogBufferWaits
                )
            }) =>
        {
            Some(key!("view.commit_pipeline.interval.action.buffer"))
        }
        _ => None,
    };
    let mut actions = BTreeSet::new();
    if let Some(key) = action {
        actions.insert(key);
    }
    if matches!(
        report.stage,
        Some(
            CommitPipelineStage::LogBufferSpace
                | CommitPipelineStage::LocalLogFlush
                | CommitPipelineStage::LocalLogWrite
                | CommitPipelineStage::LocalLogSync
        )
    ) && mechanisms.contains(&CommitPipelineIntervalMechanism::NativeBufferFullWrites)
    {
        actions.insert(key!("view.commit_pipeline.interval.action.buffer"));
    }
    if !actions.is_empty() {
        for key in actions {
            report.investigation.push_str("\n\n");
            report.investigation.push_str(&culprit_core::i18n::t(key, &[]));
        }
        if let Some(mechanism) = &mut report.mechanism {
            mechanism.push_str("\n\n");
            mechanism.push_str(&t!("view.commit_pipeline.interval.mechanism"));
        }
        report.verify.push_str("\n\n");
        report.verify.push_str(&t!("view.commit_pipeline.interval.verify"));
    }
}
fn counter(mechanism: CommitPipelineIntervalMechanism) -> CommitPipelineCounterKind {
    use CommitPipelineCounterKind as Kind;
    match mechanism {
        CommitPipelineIntervalMechanism::NativeBufferFullWrites => Kind::BufferFullWrites,
        CommitPipelineIntervalMechanism::NativeLogBufferWaits => Kind::LogBufferWaits,
        CommitPipelineIntervalMechanism::LocalWriteWork => Kind::WriteOperations,
        CommitPipelineIntervalMechanism::LocalSyncWork => Kind::SyncOperations,
        CommitPipelineIntervalMechanism::NativeFlushWork => Kind::FlushOperations,
    }
}
fn summary_key(mechanism: CommitPipelineIntervalMechanism) -> &'static str {
    match mechanism {
        CommitPipelineIntervalMechanism::NativeBufferFullWrites => key!("view.commit_pipeline.interval.work.buffer"),
        CommitPipelineIntervalMechanism::NativeLogBufferWaits => key!("view.commit_pipeline.interval.work.waits"),
        CommitPipelineIntervalMechanism::LocalWriteWork => key!("view.commit_pipeline.interval.work.write"),
        CommitPipelineIntervalMechanism::LocalSyncWork => key!("view.commit_pipeline.interval.work.sync"),
        CommitPipelineIntervalMechanism::NativeFlushWork => key!("view.commit_pipeline.interval.work.flush"),
    }
}
fn gap_key(issue: CommitPipelineIntervalIssue) -> &'static str {
    use CommitPipelineIntervalIssue as Issue;
    match issue {
        Issue::MissingEndpoint => key!("view.commit_pipeline.interval.gap.missing"),
        Issue::AmbiguousEndpoint => key!("view.commit_pipeline.interval.gap.ambiguous"),
        Issue::IncompleteRead | Issue::IncompleteSource => key!("view.commit_pipeline.interval.gap.partial"),
        Issue::NativeOwnerChanged | Issue::ScopeUnavailable => key!("view.commit_pipeline.interval.gap.owner"),
        Issue::DatabaseRoleUnknown | Issue::DatabaseRoleChanged => key!("view.commit_pipeline.interval.gap.role"),
        Issue::ObservationOrder => key!("view.commit_pipeline.interval.gap.time"),
        Issue::UnknownReset | Issue::ChangedReset | Issue::ResetAfterObservation => {
            key!("view.commit_pipeline.interval.gap.reset")
        }
        Issue::CounterMissing | Issue::CounterMeaningChanged | Issue::CounterDecreased | Issue::ValueUnavailable => {
            key!("view.commit_pipeline.interval.gap.values")
        }
        Issue::FileCounterIdentityUnavailable => key!("view.commit_pipeline.interval.gap.file"),
        Issue::PresentationLimited => key!("view.commit_pipeline.interval.gap.limit"),
    }
}
