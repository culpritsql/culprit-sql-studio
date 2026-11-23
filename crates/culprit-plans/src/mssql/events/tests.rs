use super::*;
use crate::mssql::test_support::deadlocks::KEY_LOOKUP_XDL;
fn event(name: &str, at: Option<&str>, fields: &str) -> String {
    let at = at.map_or(String::new(), |at| format!(" timestamp=\"{at}\""));
    format!("<event name=\"{name}\" package=\"sqlserver\"{at}>{fields}</event>")
}
fn parse(xml: &str) -> ParsedEventXml {
    parse_xml(xml.as_bytes(), NativeLogLimits::default()).unwrap()
}
#[test]
fn native_attention_and_error_fields_are_exact_and_free_form_messages_never_become_rules_or_output() {
    let attention = event(
        "attention",
        Some("2020-01-01T14:00:00.1234567+02:00"),
        "<data name=\"duration\"><value>9007199254740993</value></data><action name=\"session_id\" package=\"sqlserver\"><value>59</value></action><action name=\"sql_text\" package=\"sqlserver\"><value>SELECT 'private-sql' WHERE id=42</value></action>",
    );
    let error = event(
        "error_reported",
        None,
        "<data name=\"error_number\"><value>50001</value></data><data name=\"severity\"><value>10</value></data><data name=\"state\"><value>1</value></data><data name=\"is_intercepted\"><value>true</value></data><data name=\"message\"><value>private-message says timeout OOM and names a cause</value></data>",
    );
    let batch = parse(&format!("<events>{attention}{error}</events>"));
    assert_eq!(batch.records_examined, 2);
    assert!(batch.issues.is_empty());
    assert_eq!(batch.records[0].timestamp_utc.as_deref(), Some("2020-01-01T12:00:00.123456700Z"));
    assert!(
        matches!(&batch.records[0].kind, MssqlExtendedEventKind::Attention { native_duration: Some(value) } if value == "9007199254740993")
    );
    assert!(matches!(
        batch.records[1].kind,
        MssqlExtendedEventKind::ErrorReported { number: 50001, severity: 10, intercepted: Some(true), .. }
    ));
    assert!(batch.records[1].timestamp_utc.is_none());
    let text = serde_json::to_string(&batch.records).unwrap();
    assert!(!text.contains("private-sql"));
    assert!(!text.contains("private-message"));
}
#[test]
fn deadlocks_reuse_the_native_graph_classifier_and_redaction_without_invented_utc_or_current_actors() {
    let xml = event(
        "xml_deadlock_report",
        Some("2026-10-25T03:30:00"),
        &format!("<data name=\"xml_report\"><value>{KEY_LOOKUP_XDL}</value></data>"),
    );
    let batch = parse(&xml);
    assert!(batch.issues.is_empty(), "{:?}", batch.issues);
    let mut expected =
        super::super::deadlocks::parse_deadlock_xml(KEY_LOOKUP_XDL, "supplied-extended-events-xml").unwrap().remove(0);
    expected.redact(Engine::Mssql);
    assert_eq!(batch.deadlocks[0].1, expected);
    assert!(expected.at.is_none());
    assert!(
        expected
            .processes
            .iter()
            .all(|actor| actor.login.is_none() && actor.host.is_none() && actor.application.is_none())
    );
    assert_eq!(batch.records[0].kind, MssqlExtendedEventKind::DeadlockReport { graphs: 1 });
}
#[test]
fn invalid_duplicate_foreign_package_and_unknown_records_keep_later_positive_records_as_partial() {
    let duplicate = event(
        "attention",
        None,
        "<action name=\"session_id\" package=\"sqlserver\"><value>59</value></action><action name=\"session_id\" package=\"sqlserver\"><value>60</value></action>",
    );
    let foreign = event("attention", None, "").replace("package=\"sqlserver\"", "package=\"foreign\"");
    let unknown = event("rpc_completed", None, "");
    let valid = event("attention", None, "");
    let batch = parse(&format!("<events>{duplicate}{foreign}{unknown}{valid}</events>"));
    assert_eq!(batch.records_examined, 4);
    assert_eq!(batch.records[0].position, 3);
    assert_eq!(
        batch.issues.iter().map(|issue| issue.problem.clone()).collect::<Vec<_>>(),
        [NativeLogProblem::DuplicateField, NativeLogProblem::NativeShape, NativeLogProblem::UnsupportedEvent]
    );
    for fields in [
        "<data name=\"duration\"><value>-1</value></data>",
        "<action name=\"sql_text\" package=\"foreign\"><value>SELECT 1</value></action>",
    ] {
        assert_eq!(parse(&event("attention", None, fields)).issues.len(), 1);
    }
}
#[test]
fn input_record_field_limits_and_native_ring_buffer_drops_never_claim_complete_empty_absence() {
    let row = event("attention", None, "");
    let batch = parse_xml(
        format!("<events>{row}{row}</events>").as_bytes(),
        NativeLogLimits { records: 1, ..Default::default() },
    )
    .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.stopped, Some(NativeLogProblem::RecordLimit));
    let batch = parse_xml(
        event("attention", None, "<data name=\"message\"><value>abcdefghijklmnopqrstuvwxyz0123456789</value></data>")
            .as_bytes(),
        NativeLogLimits { field_bytes: 32, ..Default::default() },
    )
    .unwrap();
    assert_eq!(batch.issues[0].problem, NativeLogProblem::FieldBytes);
    let batch = parse_xml(
        row.as_bytes(),
        NativeLogLimits { record_bytes: row.len() - 1, field_bytes: 1, ..Default::default() },
    )
    .unwrap();
    assert_eq!(batch.issues[0].problem, NativeLogProblem::RecordBytes);
    let batch = parse(&format!("<RingBufferTarget truncated=\"1\" droppedCount=\"4\">{row}</RingBufferTarget>"));
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.stopped, Some(NativeLogProblem::NativeShape));
}
#[test]
fn ssms_utf16_boms_share_the_existing_decoder_and_truncation_dtd_binary_and_malformed_inputs_refuse() {
    let xml = event("attention", None, "");
    for little in [true, false] {
        let mut bytes = if little { vec![0xff, 0xfe] } else { vec![0xfe, 0xff] };
        for unit in xml.encode_utf16() {
            bytes.extend(if little { unit.to_le_bytes() } else { unit.to_be_bytes() });
        }
        assert_eq!(parse_xml(&bytes, Default::default()).unwrap().records.len(), 1);
        bytes.push(0);
        assert!(matches!(parse_xml(&bytes, Default::default()), Err(NativeLogProblem::InvalidEncoding)));
    }
    for bad in [
        b"<event".as_slice(),
        b"<!DOCTYPE event [<!ENTITY x 'private'>]><event name='attention' package='sqlserver'>&x;</event>",
        &[0x1f, 0x8b, 0x08],
        b"XEL binary is not XML",
    ] {
        assert!(parse_xml(bad, Default::default()).is_err());
    }
    let too_deep = format!("{}{}", "<events>".repeat(1025), "</events>".repeat(1025));
    assert!(parse_xml(too_deep.as_bytes(), Default::default()).is_err());
}
