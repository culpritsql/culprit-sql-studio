use super::*;
use crate::mssql::test_support::deadlocks::{KEY_LOOKUP_XDL, event};
use culprit_core::DeadlockPattern;

#[test]
fn a_saved_xdl_is_parsed_and_explained() {
    let found = parse_deadlock_xml(KEY_LOOKUP_XDL, "pasted").unwrap();
    assert_eq!(found.len(), 1);
    let d = &found[0];
    assert_eq!(d.pattern, DeadlockPattern::KeyLookup);
    assert!(d.at.is_none(), "an imported graph without an event clock stays undated");
    assert_eq!(d.objects, vec!["shop.customers".to_string()]);
    let victim = d.victim().unwrap();
    assert_eq!((victim.session.as_deref(), victim.application.as_deref()), (Some("61"), Some("orders-api")));
    assert_eq!(victim.isolation.as_deref(), Some("read committed"));
    assert!(victim.statement.as_deref().unwrap().starts_with("SELECT id, name"));
    assert_eq!(d.resources[0].holders[0].mode.as_deref(), Some("X"));
}

#[test]
fn system_health_events_carry_their_time_and_dtds_are_refused() {
    let found = parse_deadlock_xml(&event("2026-10-11T12:05:01.123Z"), "system_health").unwrap();
    assert_eq!(found[0].at.as_deref(), Some("2026-10-11T12:05:01.123000000Z"));
    let evil = r#"<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]><deadlock>&e;</deadlock>"#;
    assert!(parse_deadlock_xml(evil, "pasted").is_err());
    assert!(parse_deadlock_xml("<nothing/>", "pasted").is_err());
}

#[test]
fn full_event_precision_distinguishes_repeated_participants_and_canonicalizes_replays() {
    let first = parse_deadlock_xml(&event("2026-10-11T12:05:01.1234567Z"), "system_health").unwrap();
    let second = parse_deadlock_xml(&event("2026-10-11T12:05:01.1234568Z"), "system_health").unwrap();
    let replay = parse_deadlock_xml(&event("2026-10-11T14:05:01.123456700+02:00"), "system_health").unwrap();
    assert_eq!(first[0].at.as_deref(), Some("2026-10-11T12:05:01.123456700Z"));
    assert_eq!(second[0].at.as_deref(), Some("2026-10-11T12:05:01.123456800Z"));
    assert_ne!(first[0].id, second[0].id, "two native events with the same SPIDs remain distinct");
    assert_eq!(first, replay, "equivalent explicit offsets and trailing zeros describe the same event");
    assert_eq!(first[0].processes, second[0].processes);
    assert_eq!(first[0].headline, second[0].headline);
    assert_eq!(first[0].explanation, second[0].explanation);
    assert_eq!(first[0].fixes, second[0].fixes);
}

#[test]
fn invalid_event_clocks_are_rejected_without_inventing_an_offset_or_losing_precision() {
    let too_long = format!("2026-10-11T12:05:01.{}Z", "0".repeat(50));
    for at in [
        "",
        "2026-10-11T12:05:01.1234567",
        "2026-10-11T12:05:01.1234567Zgarbage",
        "2026-02-30T12:05:01.1234567Z",
        "2026-10-11T12:05:01.1234567001Z",
        too_long.as_str(),
    ] {
        let xml = event(at);
        assert!(parse_deadlock_xml(&xml, "pasted").is_err(), "invalid event clock: {at}");
        assert!(
            parse_with_time(&xml, "system_health", Some("2026-10-11T12:05:01.1234567Z")).is_err(),
            "a native row clock cannot repair an invalid event clock: {at}"
        );
    }
}

#[test]
fn a_claimed_report_cannot_be_empty_or_borrow_a_nested_events_graph() {
    for name in ["xml_deadlock_report", "database_xml_deadlock_report"] {
        for at in ["bad-clock", "2026-10-11T12:05:01.1234567Z"] {
            let missing =
                format!(r#"<event name="{name}" timestamp="{at}"><data name="xml_report"><value/></data></event>"#);
            assert!(parse_deadlock_xml(&missing, "pasted").is_err());
            assert!(parse_with_time(&missing, "system_health", Some("2026-10-11T12:05:01.1234567Z")).is_err());
            let nested =
                format!(r#"<event name="{name}" timestamp="{at}">{}</event>"#, event("2026-10-11T12:05:01.1234567Z"));
            assert!(parse_deadlock_xml(&nested, "pasted").is_err());
        }
    }
    assert!(parse_deadlock_xml(r#"<event name="unrelated"/>"#, "system_health").unwrap().is_empty());
}

#[test]
fn a_native_row_clock_qualifies_identity_before_construction_and_must_agree_with_event_time() {
    let at = "2026-10-11T12:05:01.1234567Z";
    let supplied = parse_with_time(KEY_LOOKUP_XDL, "system_health", Some(at)).unwrap();
    let wrapped = parse_deadlock_xml(&event(at), "system_health").unwrap();
    assert_eq!(supplied, wrapped, "a timestamp-less native graph and its ring replay share one identity");
    let later = parse_with_time(KEY_LOOKUP_XDL, "system_health", Some("2026-10-11T12:05:01.1234568Z")).unwrap();
    assert_ne!(supplied[0].id, later[0].id, "the native fallback clock is not assigned after ID construction");
    assert!(parse_with_time(&event(at), "system_health", Some("2026-10-11T12:05:01.1234568Z")).is_err());
    assert!(parse_with_time(&event(at), "system_health", Some("2026-10-11T12:05:01.1234567")).is_err());
}

#[test]
fn a_claimed_deadlock_requires_a_graph_while_unrelated_events_can_be_empty() {
    for xml in ["<RingBufferTarget/>", r#"<RingBufferTarget><event name="wait_info"/></RingBufferTarget>"#] {
        assert!(parse_deadlock_xml(xml, "system_health").unwrap().is_empty());
    }
    let malformed = r#"<event name="xml_deadlock_report" timestamp="2026-10-11T12:05:01.1234568Z"><data name="xml_report"><value><deadlock/></value></data></event>"#;
    assert!(parse_deadlock_xml(malformed, "system_health").is_err());
    let partial = format!("<RingBufferTarget>{}{malformed}</RingBufferTarget>", event("2026-10-11T12:05:01.1234567Z"));
    assert!(
        parse_deadlock_xml(&partial, "system_health").is_err(),
        "a malformed claimed graph cannot yield a successful partial set"
    );
}
