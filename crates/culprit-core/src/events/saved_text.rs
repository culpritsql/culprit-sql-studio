use super::*;

fn catalog_event() -> ChangeEvent {
    let mut event = ChangeEvent::new(
        EventKind::Restart,
        EventSource::Database,
        "DbO.Items",
        "2026-11-21T10:00:00Z",
        crate::message!("engine.history.restarted"),
    )
    .detail(crate::message!("engine.history.restart_detail"))
    .action(EventAction::Changed)
    .table("DbO.Items")
    .object("native-instance")
    .between("2026-11-21T09:59:00Z")
    .until("2026-11-21T10:00:01Z")
    .minor();
    event.by = Some("recorded-owner".into());
    event
}

#[test]
fn saved_event_serde_preserves_cached_words_until_explicit_reading_without_changing_native_evidence() {
    let original = crate::i18n::scope("en", catalog_event);
    assert_eq!(original.id, "restart:dbo.items:2026-11-21T10:00:00Z");
    assert_eq!(original.title.text(), "The server restarted");
    let bytes = serde_json::to_vec(&original).unwrap();
    let mut french: ChangeEvent = crate::i18n::scope("fr", || serde_json::from_slice(&bytes).unwrap());
    assert_eq!(french, original);
    assert_eq!(serde_json::to_vec(&french).unwrap(), bytes);
    crate::i18n::scope("fr", || french.refresh_language());
    assert_ne!(french.title.text(), original.title.text());
    assert_eq!(french.title.source(), original.title.source());
    assert_eq!(french.detail.as_ref().unwrap().source(), original.detail.as_ref().unwrap().source());
    french.title = original.title.clone();
    french.detail = original.detail.clone();
    assert_eq!(french, original, "rendering cannot change event identity or native fields");
}

#[test]
fn key_shaped_raw_event_words_are_neither_translated_nor_discarded_by_storage() {
    let event = ChangeEvent::new(
        EventKind::Mark,
        EventSource::Mark,
        "native-mark",
        "2026-11-21T10:00:00Z",
        "engine.history.restarted",
    )
    .detail("native <value> \"quoted\" unchanged");
    assert!(event.title.source().is_none());
    assert!(event.detail.as_ref().unwrap().source().is_none());
    let mut replayed: ChangeEvent = serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap();
    crate::i18n::scope("de", || replayed.refresh_language());
    replayed.clear_rendered_text();
    assert_eq!(replayed, event);
}

#[test]
fn clearing_cached_event_words_retains_each_source_and_an_atomic_raw_replacement_removes_it() {
    let original = crate::i18n::scope("en", catalog_event);
    let mut cleared = original.clone();
    cleared.clear_rendered_text();
    assert_eq!(cleared.title.text(), "");
    assert_eq!(cleared.detail.as_ref().unwrap().text(), "");
    assert_eq!(cleared.title.source(), original.title.source());
    assert_eq!(cleared.detail.as_ref().unwrap().source(), original.detail.as_ref().unwrap().source());
    let bytes = serde_json::to_vec(&cleared).unwrap();
    let mut restored: ChangeEvent = crate::i18n::scope("fr", || serde_json::from_slice(&bytes).unwrap());
    assert_eq!(restored, cleared);
    crate::i18n::scope("en", || restored.refresh_language());
    assert_eq!(restored, original);
    restored.title.set("raw title");
    restored = restored.detail("raw detail");
    crate::i18n::scope("es", || restored.refresh_language());
    assert_eq!(restored.title.text(), "raw title");
    assert!(restored.title.source().is_none());
    assert_eq!(restored.detail.as_ref().unwrap().text(), "raw detail");
    assert!(restored.detail.as_ref().unwrap().source().is_none());
}

#[test]
fn event_deserialization_rejects_stale_independent_message_fields_and_flat_saved_words() {
    let original = serde_json::to_value(catalog_event()).unwrap();
    for field in ["title_message", "detail_message"] {
        let mut stale = original.clone();
        stale[field] = serde_json::Value::Null;
        assert!(serde_json::from_value::<ChangeEvent>(stale).is_err(), "{field}");
    }
    let mut stale = original;
    stale["title"] = "The server restarted".into();
    assert!(serde_json::from_value::<ChangeEvent>(stale).is_err());
}
