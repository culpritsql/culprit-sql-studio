use super::*;
fn request() -> PublishedResourceRequest {
    PublishedResourceRequest { as_of: "2026-11-22T12:00:00Z".into(), window: None, after: None, page_rows: 2 }
}
#[test]
fn published_resource_selection_has_explicit_utc_cutoff_and_bounded_pages() {
    let mut selected = request();
    assert!(selected.validate().is_ok());
    assert!(selected.accepts_acquisition("2026-11-22T11:00:00Z", "2026-11-22T12:00:00Z"));
    assert!(!selected.accepts_acquisition("2026-11-22T12:00:00Z", "2026-11-22T12:00:00.000000001Z"));
    assert!(!selected.accepts_acquisition("2026-11-22T11:00:00", "2026-11-22T11:00:01"));
    selected.window =
        Some(PublishedResourceWindow { from: "2026-11-22T10:00:00Z".into(), until: "2026-11-22T11:00:00Z".into() });
    assert!(selected.accepts_acquisition("2026-11-22T09:59:59Z", "2026-11-22T10:00:00Z"));
    assert!(!selected.accepts_acquisition("2026-11-22T11:00:00Z", "2026-11-22T11:00:01Z"));
    selected.page_rows = MAX_PUBLISHED_RESOURCE_PAGE_ROWS + 1;
    assert!(selected.validate().is_err());
    selected.page_rows = 1;
    selected.as_of = "2026-11-22T14:00:00+02:00".into();
    assert!(selected.validate().is_err());
}
#[test]
fn published_resource_cursor_is_strictly_typed_without_native_authority() {
    let mut selected = request();
    selected.after = Some(PublishedResourceCursor {
        selection_key: "a".repeat(64),
        position: PublishedResourcePosition {
            resource_id: "b".repeat(64),
            source_id: "c".repeat(64),
            range_id: "d".repeat(64),
        },
    });
    let bytes = serde_json::to_vec(&selected).unwrap();
    let restored: PublishedResourceRequest = serde_json::from_slice(&bytes).unwrap();
    assert!(restored.validate().is_ok());
    let mut value = serde_json::to_value(&selected).unwrap();
    value["after"]["native_owner"] = serde_json::json!("invented");
    assert!(serde_json::from_value::<PublishedResourceRequest>(value).is_err());
    selected.after.as_mut().unwrap().position.range_id = "not-a-key".into();
    assert!(selected.validate().is_err());
}
