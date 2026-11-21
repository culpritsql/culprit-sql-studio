//! One saved-original replay policy, preserving distinct CDC and change-feed pure reports.
use super::*;
pub(super) fn explain(snapshot: &Snapshot) -> culprit_core::ChangeCaptureReports {
    culprit_core::ChangeCaptureReports { cdc: cdc(snapshot), change_feed: change_feed(snapshot) }
}
fn cdc(snapshot: &Snapshot) -> Option<Vec<culprit_core::CdcReport>> {
    super::recorded_operational::explain(
        snapshot.operations.change_capture.originals(culprit_core::telemetry::MssqlChangeCaptureFamily::Cdc),
        crate::cdc::explain_recorded,
        crate::cdc::unavailable,
        culprit_core::message!("view.change_capture.history.invalid"),
        overflow,
    )
}
fn change_feed(snapshot: &Snapshot) -> Option<Vec<culprit_core::ChangeFeedReport>> {
    super::recorded_operational::explain(
        snapshot.operations.change_capture.originals(culprit_core::telemetry::MssqlChangeCaptureFamily::ChangeFeed),
        crate::change_feed::explain_recorded,
        crate::change_feed::unavailable,
        culprit_core::message!("view.change_capture.history.invalid"),
        overflow,
    )
}
fn overflow(shown: usize, total: usize) -> culprit_core::i18n::Message {
    culprit_core::message!(
        "view.change_capture.history.limited",
        shown = culprit_core::format::digits(shown.to_string()),
        total = culprit_core::format::digits(total.to_string())
    )
}
#[cfg(test)]
mod tests;
