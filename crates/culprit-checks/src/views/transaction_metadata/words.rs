use super::*;
pub(super) fn append(report: &mut TransactionMetadataReport) {
    if !report.slru_changes.iter().any(|change| matches!(change, PostgresSlruChange::Observed { .. })) {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.counters"));
    }
    for change in &report.slru_changes {
        match change {
            PostgresSlruChange::Unavailable { kind, reason } => report.limitations.push(culprit_core::t!(
                "view.transaction_metadata.slru.unavailable",
                family = family(*kind),
                reason = gap(*reason)
            )),
            PostgresSlruChange::Observed {
                native_name, observed_from, observed_until, stats_reset, counters, ..
            } => {
                for counter in counters {
                    if let Some(change) = counter.change.as_ref().filter(|change| change.as_str() != "0") {
                        report.limitations.push(culprit_core::t!(
                            "view.transaction_metadata.slru.change",
                            name = native_name.as_str(),
                            counter = counter.native_name.as_str(),
                            change = culprit_core::format::digits(change.clone()),
                            from = observed_from.as_str(),
                            until = observed_until.as_str(),
                            reset = stats_reset.as_str()
                        ));
                    }
                }
                if counters.iter().all(|counter| counter.change.as_deref() == Some("0")) {
                    report.limitations.push(culprit_core::t!(
                        "view.transaction_metadata.slru.zero",
                        name = native_name.as_str(),
                        from = observed_from.as_str(),
                        until = observed_until.as_str()
                    ));
                }
                if counters.iter().any(|counter| counter.change.is_none()) {
                    report
                        .limitations
                        .push(culprit_core::t!("view.transaction_metadata.slru.fields", name = native_name.as_str()));
                }
            }
        }
    }
    if report.slru_changes.iter().any(|change| matches!(change, PostgresSlruChange::Observed { .. })) {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.slru.scope"));
    }
    if report.slru_changes.iter().any(|change| {
        matches!(
            change,
            PostgresSlruChange::Observed { before_state: SourceState::Partial, .. }
                | PostgresSlruChange::Observed { after_state: SourceState::Partial, .. }
        )
    }) {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.slru.partial"));
    }
}
fn family(kind: PostgresSlruKind) -> &'static str {
    match kind {
        PostgresSlruKind::Subtransaction => "Subtrans",
        PostgresSlruKind::MultiXactMember => "MultiXactMember",
        PostgresSlruKind::MultiXactOffset => "MultiXactOffset",
    }
}
fn gap(gap: PostgresSlruChangeGap) -> &'static str {
    use PostgresSlruChangeGap::*;
    match gap {
        MissingPoint => "missing",
        AmbiguousPoint => "ambiguous",
        UnreadBody => "unread",
        IncompatibleOwner => "owner",
        RoleChanged => "role",
        UnorderedClocks => "order",
        UnknownReset => "unknown-reset",
        ResetChanged => "reset-changed",
        ResetClock => "reset-clock",
    }
}
