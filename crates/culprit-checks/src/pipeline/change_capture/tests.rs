use super::*;
use culprit_core::{SourceState, telemetry::RecordedOperationalRead};
fn snapshot() -> Snapshot {
    serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/mssql-demo.json"))).unwrap()
}
#[test]
fn absent_originals_do_not_create_history_or_healthy_zero() {
    let snapshot = snapshot();
    let reports = explain(&snapshot);
    assert!(reports.cdc.is_none());
    assert!(reports.change_feed.is_none());
}
#[test]
fn explicit_withdrawals_preserve_each_raw_family_without_fabricated_native_context() {
    let mut snapshot = snapshot();
    let original = RecordedOperationalRead::Unavailable {
        reason: culprit_core::message!("view.change_capture.history.sharing_withheld"),
    };
    snapshot.operations.change_capture =
        serde_json::from_value(serde_json::json!({"cdc":[original.clone()],"change_feed":[original.clone()]})).unwrap();
    let reports = explain(&snapshot);
    let cdc = reports.cdc.unwrap();
    let feed = reports.change_feed.unwrap();
    for (state, complete, context, entries) in [
        (cdc[0].source_state, cdc[0].read_complete, cdc[0].context.as_ref(), cdc[0].entries.len()),
        (feed[0].source_state, feed[0].read_complete, feed[0].context.as_ref(), feed[0].entries.len()),
    ] {
        assert_eq!(state, SourceState::Unavailable);
        assert!(!complete);
        assert!(context.is_none());
        assert_eq!(entries, 0);
    }
    assert_eq!(
        snapshot.operations.change_capture.originals(culprit_core::telemetry::MssqlChangeCaptureFamily::Cdc),
        &[original.clone()]
    );
    assert_eq!(
        snapshot.operations.change_capture.originals(culprit_core::telemetry::MssqlChangeCaptureFamily::ChangeFeed),
        &[original]
    );
}
