use super::*;

#[test]
fn required_family_rosters_survive_roundtrip_without_legacy_keys_or_synthetic_availability() {
    let empty = RecordedChangeCaptureHistories::default();
    assert_eq!(serde_json::to_value(&empty).unwrap(), serde_json::json!({"cdc":[],"change_feed":[]}));
    for invalid in [
        serde_json::json!({}),
        serde_json::json!({"cdc":[]}),
        serde_json::json!({"cdc":null,"change_feed":[]}),
        serde_json::json!({"cdc":[],"change_feed":[],"healthy":true}),
    ] {
        assert!(serde_json::from_value::<RecordedChangeCaptureHistories>(invalid).is_err());
    }
    let original = RecordedOperationalRead::Unavailable {
        reason: crate::message!("view.change_capture.history.sharing_withheld"),
    };
    let histories: RecordedChangeCaptureHistories =
        serde_json::from_value(serde_json::json!({"cdc":[original],"change_feed":[]})).unwrap();
    let bytes = serde_json::to_vec(&histories).unwrap();
    let restored: RecordedChangeCaptureHistories = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored.originals(MssqlChangeCaptureFamily::Cdc), histories.originals(MssqlChangeCaptureFamily::Cdc));
    assert!(restored.originals(MssqlChangeCaptureFamily::ChangeFeed).is_empty());
}

#[test]
fn sharing_preserves_each_original_position_and_is_idempotent_without_fabricated_native_context() {
    let marker =
        RecordedOperationalRead::Unavailable { reason: crate::message!("view.change_capture.history.invalid") };
    let mut histories: RecordedChangeCaptureHistories =
        serde_json::from_value(serde_json::json!({"cdc":[marker.clone(),marker.clone()],"change_feed":[marker]}))
            .unwrap();
    histories.withdraw_for_sharing();
    let once = serde_json::to_value(&histories).unwrap();
    histories.withdraw_for_sharing();
    assert_eq!(serde_json::to_value(&histories).unwrap(), once);
    assert_eq!(histories.originals(MssqlChangeCaptureFamily::Cdc).len(), 2);
    assert_eq!(histories.originals(MssqlChangeCaptureFamily::ChangeFeed).len(), 1);
    for original in histories.cdc.iter().chain(&histories.change_feed) {
        assert!(matches!(original, RecordedOperationalRead::Unavailable { .. }));
    }
    assert!(!once.to_string().contains("context"));
}
