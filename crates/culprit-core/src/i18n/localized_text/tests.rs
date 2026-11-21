use super::*;
use crate::i18n::{MessageValue, scope};

fn catalog() -> Message {
    crate::message!("engine.query_log.statements", n = 1234, count = MessageValue::Count(1234.0))
}
#[test]
fn one_saved_observation_renders_for_another_reader_without_changing_its_message() {
    let mut stored = scope("en", || LocalizedText::new(catalog()));
    let first = stored.text().to_owned();
    let bytes = serde_json::to_vec(&stored).unwrap();
    stored = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(stored.text(), first);
    scope("fr", || stored.render());
    assert_ne!(stored.text(), first);
    assert!(stored.text().contains("1,2K"));
    assert_eq!(stored.source(), Some(&catalog()));
}
#[test]
fn mixed_raw_and_catalog_notes_survive_storage_clearing_and_another_locale_in_order() {
    let mut notes = scope("en", || {
        vec![
            LocalizedText::raw("raw-note <tag> canary"),
            LocalizedText::new(catalog()),
            LocalizedText::raw("engine.query_log.statements"),
        ]
    });
    notes.iter_mut().for_each(LocalizedText::clear_rendered);
    assert_eq!(notes[0].text(), "raw-note <tag> canary");
    assert!(notes[1].text().is_empty());
    assert_eq!(notes[2].text(), "engine.query_log.statements");
    let bytes = serde_json::to_vec(&notes).unwrap();
    let mut restored: Vec<LocalizedText> = serde_json::from_slice(&bytes).unwrap();
    scope("fr", || restored.iter_mut().for_each(LocalizedText::render));
    assert_eq!(restored.len(), 3);
    assert_eq!(restored[0].text(), notes[0].text());
    assert!(restored[0].source().is_none());
    assert_eq!(restored[1].text(), scope("fr", || catalog().render()));
    assert_eq!(restored[2].text(), notes[2].text());
    assert!(restored[2].source().is_none());
}
#[test]
fn replacing_or_clearing_text_cannot_leave_a_stale_authoritative_message() {
    let mut text = LocalizedText::new(catalog());
    text.set("user text");
    scope("fr", || text.render());
    assert_eq!(text.text(), "user text");
    assert!(text.source().is_none());
    text.set(catalog());
    assert_eq!(text.source(), Some(&catalog()));
    text.clear();
    assert!(text.text().is_empty());
    assert!(text.source().is_none());
    scope("fr", || text.render());
    assert!(text.text().is_empty());
}
#[test]
fn redaction_refreshes_catalog_arguments_and_keeps_raw_input_raw() {
    let mut catalog = LocalizedText::new(crate::message!(
        "model.observation.source.ysql",
        relation = "yb_servers()",
        scope = "node",
        node = "private-node-canary",
        limited = "no",
        role = ""
    ));
    assert!(catalog.text().contains("private-node-canary"));
    catalog.redact(|message| {
        let Message::Catalog { arguments, .. } = message else { panic!() };
        arguments.insert("node".into(), MessageValue::Text("[node]".into()));
    });
    assert!(!serde_json::to_string(&catalog).unwrap().contains("private-node-canary"));
    scope("fr", || catalog.render());
    assert!(catalog.text().contains("[node]"));
    let mut raw = LocalizedText::raw("private raw canary");
    raw.redact(|message| {
        let Message::Text { text } = message else { panic!() };
        *text = "[raw]".into();
    });
    assert!(raw.source().is_none());
    scope("fr", || raw.render());
    assert_eq!(raw.text(), "[raw]");
}
#[test]
fn existing_message_arguments_retain_nested_authority_without_interpreting_raw_keys() {
    let text = scope("en", || LocalizedText::new(catalog()));
    assert_eq!(Message::from(text.clone()), catalog());
    assert_eq!(MessageValue::from(text.clone()), MessageValue::Message(Box::new(catalog())));
    assert_eq!(Message::from(LocalizedText::raw("raw-key")), Message::from("raw-key"));
    let nested = Message::paragraph(vec![Message::from(&text), Message::from(LocalizedText::raw("raw-key"))]);
    assert!(scope("fr", || nested.render()).contains("1,2K"));
    assert!(scope("fr", || nested.render()).ends_with("raw-key"));
    assert_ne!(
        scope("en", || crate::t!("model.source.failed", error = &text)),
        scope("fr", || crate::t!("model.source.failed", error = &text))
    );
}
