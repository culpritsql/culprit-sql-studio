//! Pure interpretation of the closed original answer. Neither shared counters nor overflow proves a culprit.
use culprit_core::{Error, Result, SourceState, telemetry::*};
mod comparison;
mod words;
pub use comparison::SlruOriginal;

pub fn explain(
    answer: &TransactionMetadataAnswer,
    point: Option<&PostgresTransactionMetadata>,
    originals: &[SlruOriginal<'_>],
) -> Result<TransactionMetadataReport> {
    if !answer.is_valid() {
        return Err(invalid());
    }
    let (summary, investigation) = match answer.mechanism {
        TransactionMetadataWait::Subtransaction => (
            culprit_core::t!("view.transaction_metadata.subtransaction.mechanism"),
            culprit_core::t!("view.transaction_metadata.subtransaction.action"),
        ),
        TransactionMetadataWait::MultiXact => (
            culprit_core::t!("view.transaction_metadata.multixact.mechanism"),
            culprit_core::t!("view.transaction_metadata.multixact.action"),
        ),
    };
    let mut report = TransactionMetadataReport {
        mechanism: answer.mechanism,
        source_state: answer.source_state,
        summary,
        investigation,
        verify: culprit_core::t!("view.transaction_metadata.verify"),
        observed_overflows: vec![],
        slru_changes: Vec::new(),
        observed_xmin_sources: vec![],
        limitations: vec![
            culprit_core::t!("view.transaction_metadata.limit.scope"),
            culprit_core::t!("view.transaction_metadata.limit.cause"),
            culprit_core::t!("view.transaction_metadata.limit.savepoints"),
            culprit_core::t!("view.transaction_metadata.limit.xmin"),
            culprit_core::t!("view.transaction_metadata.limit.multixact"),
        ],
    };
    if answer.source_state == SourceState::Partial {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.partial"));
    }
    let Some(point) = point else {
        report.slru_changes = comparison::compare(answer, &[])?;
        words::append(&mut report);
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.unavailable"));
        return Ok(report);
    };
    let Some(reference) = &answer.observation else {
        return Err(invalid());
    };
    let NativeRecordIdentity::Activity(key) = &answer.basis.native_key else {
        return Err(invalid());
    };
    let expected =
        NativeRecordIdentity::Operational { source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(), key: point.key()? };
    let Some(bounds) =
        ObservationBounds::new(&answer.result_context.observed_from, &answer.result_context.observed_until)
    else {
        return Err(invalid());
    };
    if !point.is_valid()
        || reference.native_key != expected
        || point.focus != *key
        || answer.request_end != Some(point.request_end)
        || answer.result_context.target.database().is_none_or(|database| database.database != point.database)
        || !bounds.contains(&point.observed_at)
        || point.retention.holders.iter().any(|holder| !bounds.contains(&holder.observed_at))
        || point.slrus.iter().any(|row| !bounds.contains(&row.observed_at))
    {
        return Err(invalid());
    }
    let overflows = point
        .caches
        .iter()
        .filter(|cache| cache.overflowed == Some(true))
        .map(|cache| cache.actor.key.session.clone())
        .collect::<Vec<_>>();
    if overflows != answer.overflowed_backends {
        return Err(invalid());
    }
    report.observed_overflows = overflows;
    report.observed_xmin_sources = point.retention.oldest_observed(CleanupHorizon::Data).into_iter().cloned().collect();
    if point.request_end != BlockingRequestState::SameStatement {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.focus"));
    }
    if point.caches.iter().any(|cache| cache.overflowed.is_none())
        || point.slrus.iter().any(|row| {
            row.stats_reset.is_none()
                || [
                    &row.blocks_zeroed,
                    &row.blocks_hit,
                    &row.blocks_read,
                    &row.blocks_written,
                    &row.blocks_exists,
                    &row.flushes,
                    &row.truncates,
                ]
                .into_iter()
                .any(Option::is_none)
        })
    {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.null"));
    }
    if point.slrus.iter().any(|row| {
        row.stats_reset.as_deref().is_some_and(|reset| ObservationBounds::new(reset, &row.observed_at).is_none())
    }) {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.reset_clock"));
    }
    if !point.retention.is_horizon_complete(CleanupHorizon::Data) {
        report.limitations.push(culprit_core::t!("view.transaction_metadata.limit.horizon"));
    }
    for holder in &report.observed_xmin_sources {
        for horizon in holder.horizons.iter().filter(|horizon| horizon.protects.contains(&CleanupHorizon::Data)) {
            report.limitations.push(culprit_core::t!(
                "view.transaction_metadata.retainer",
                scope = if holder.scope == culprit_core::MeasurementScope::Server { "server" } else { "database" },
                source = holder.source.as_str(),
                name = holder.name.as_str(),
                field = horizon.native_name.as_str(),
                value = horizon.native_value.as_str(),
                at = holder.observed_at.as_str()
            ));
        }
    }
    if report.observed_xmin_sources.iter().any(|holder| point.xmin_retaining_session(holder).is_some()) {
        report.investigation.push_str("\n\n");
        report.investigation.push_str(&culprit_core::t!("view.transaction_metadata.xmin_session.action"));
        report.verify.push_str("\n\n");
        report.verify.push_str(&culprit_core::t!("view.transaction_metadata.xmin_session.verify"));
    }
    report.slru_changes = comparison::compare(answer, originals)?;
    words::append(&mut report);
    Ok(report)
}
pub(super) fn answer_reasons(answer: &TransactionMetadataAnswer) -> Vec<String> {
    if !answer.is_valid() || answer.observation.is_none() {
        return vec![culprit_core::t!("view.transaction_metadata.limit.unavailable")];
    }
    vec![
        culprit_core::t!("view.transaction_metadata.answer", count = answer.overflowed_backends.len() as u32),
        culprit_core::t!("view.transaction_metadata.limit.cause"),
    ]
}
fn invalid() -> Error {
    CollectionStop::InvalidPage.error()
}
