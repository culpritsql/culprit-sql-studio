use super::*;
use crate::mssql::{events::parse_xml, test_support::SAMPLE_EVENTS_XEL};
use culprit_core::telemetry::MssqlExtendedEventKind;

#[test]
fn genuine_sql_server_2022_xel_decodes_repeated_errors_attention_deadlock_and_buffer_offsets_with_redaction() {
    let batch = parse_xel(SAMPLE_EVENTS_XEL, NativeLogLimits::default()).unwrap();
    assert!(batch.stopped.is_none());
    assert_eq!(batch.records_examined, 6);
    assert_eq!(
        batch.issues,
        vec![
            NativeLogIssue { position: 0, problem: NativeLogProblem::UnsupportedEvent },
            NativeLogIssue { position: 3, problem: NativeLogProblem::UnsupportedEvent },
        ]
    );
    assert_eq!(batch.records.len(), 4);
    assert_eq!(batch.locations.len(), 4);
    assert_eq!(batch.deadlocks.len(), 1);

    // Positions 1 and 2 are repeated identical RAISERROR events in buffer 0x5000 with distinct sequences and offsets.
    assert_eq!(batch.records[0].position, 1);
    assert_eq!(batch.records[1].position, 2);
    assert_eq!(batch.records[0].event_sequence.as_deref(), Some("1"));
    assert_eq!(batch.records[1].event_sequence.as_deref(), Some("2"));
    assert_eq!(batch.locations[0].buffer_offset, Some(0x5000));
    assert_eq!(batch.locations[1].buffer_offset, Some(0x5000));
    assert!(batch.locations[0].event_offset < batch.locations[1].event_offset);
    for row in &batch.records[0..2] {
        assert_eq!(
            row.kind,
            MssqlExtendedEventKind::ErrorReported {
                number: 50000,
                severity: 16,
                state: 1,
                intercepted: Some(false),
                user_defined: Some(true),
            }
        );
        assert_eq!(row.database.as_deref(), Some("culprit_xel_lab"));
        assert!(row.timestamp_utc.as_deref().is_some_and(|ts| ts.starts_with("2026-11-25T04:24:44.")));
    }

    // Position 4 is client attention in buffer 0x6000.
    assert_eq!(batch.records[2].position, 4);
    assert_eq!(batch.locations[2].buffer_offset, Some(0x6000));
    assert!(matches!(batch.records[2].kind, MssqlExtendedEventKind::Attention { native_duration: Some(_) }));
    assert_eq!(batch.records[2].event_sequence.as_deref(), Some("3"));

    // Position 5 is xml_deadlock_report in buffer 0x7000.
    assert_eq!(batch.records[3].position, 5);
    assert_eq!(batch.locations[3].buffer_offset, Some(0x7000));
    assert_eq!(batch.records[3].kind, MssqlExtendedEventKind::DeadlockReport { graphs: 1 });
    assert_eq!(batch.deadlocks[0].0, 5);
    assert_eq!(batch.deadlocks[0].1.processes.len(), 2);
    assert!(batch.deadlocks[0].1.processes.iter().any(|proc| proc.victim));

    let serialized = serde_json::to_string(&(&batch.records, &batch.deadlocks)).unwrap();
    for secret in [
        "private-xel-error-canary",
        "private-xel-msg-canary",
        "private-xel-cancel-canary",
        "private-xel-deadlock-a",
        "private-xel-deadlock-b",
    ] {
        assert!(!serialized.contains(secret), "leaked {secret}");
    }
}

#[test]
fn binary_xel_and_exported_xml_produce_identical_normalized_observations_and_deadlock_semantics() {
    let xel_batch = parse_xel(SAMPLE_EVENTS_XEL, NativeLogLimits::default()).unwrap();

    // Extract the genuine UTF-16LE <deadlock> XML payload directly from Block 4 (0x7000) of SAMPLE_EVENTS_XEL.
    let block4_hdr = 0x7000 + EVENT_HEADER_BYTES;
    let payload_len =
        u32::from_le_bytes(SAMPLE_EVENTS_XEL[block4_hdr + 20..block4_hdr + 24].try_into().unwrap()) as usize;
    let edata = &SAMPLE_EVENTS_XEL[block4_hdr + 24..block4_hdr + 24 + payload_len];
    let voff = u32::from_le_bytes(edata[0..4].try_into().unwrap()) as usize;
    let vlen = u32::from_le_bytes(edata[4..8].try_into().unwrap()) as usize;
    let (pairs, _) = edata[voff..voff + vlen].as_chunks::<2>();
    let units: Vec<u16> = pairs.iter().copied().map(u16::from_le_bytes).collect();
    let deadlock_xml = String::from_utf16(&units).unwrap();

    let attention_duration = match &xel_batch.records[2].kind {
        MssqlExtendedEventKind::Attention { native_duration: Some(d) } => d.clone(),
        other => panic!("unexpected attention kind: {other:?}"),
    };
    let host = xel_batch.records[0].client.as_ref().unwrap().host.as_deref().unwrap();

    let exported_xml = format!(
        "<events>\
        <event name=\"error_reported\" package=\"sqlserver\" timestamp=\"{ts0}\">\
          <data name=\"error_number\"><value>50000</value></data>\
          <data name=\"severity\"><value>16</value></data>\
          <data name=\"state\"><value>1</value></data>\
          <data name=\"user_defined\"><value>true</value></data>\
          <data name=\"is_intercepted\"><value>false</value></data>\
          <data name=\"message\"><value>private-xel-msg-canary</value></data>\
          <action name=\"event_sequence\" package=\"package0\"><value>1</value></action>\
          <action name=\"session_id\" package=\"sqlserver\"><value>65</value></action>\
          <action name=\"request_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"transaction_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"database_id\" package=\"sqlserver\"><value>5</value></action>\
          <action name=\"database_name\" package=\"sqlserver\"><value>culprit_xel_lab</value></action>\
          <action name=\"client_app_name\" package=\"sqlserver\"><value>SQLCMD</value></action>\
          <action name=\"username\" package=\"sqlserver\"><value>sa</value></action>\
          <action name=\"client_hostname\" package=\"sqlserver\"><value>{host}</value></action>\
          <action name=\"sql_text\" package=\"sqlserver\"><value>SELECT N'private-xel-error-canary'; RAISERROR(N'private-xel-msg-canary', 16, 1);\n</value></action>\
        </event>\
        <event name=\"error_reported\" package=\"sqlserver\" timestamp=\"{ts1}\">\
          <data name=\"error_number\"><value>50000</value></data>\
          <data name=\"severity\"><value>16</value></data>\
          <data name=\"state\"><value>1</value></data>\
          <data name=\"user_defined\"><value>true</value></data>\
          <data name=\"is_intercepted\"><value>false</value></data>\
          <data name=\"message\"><value>private-xel-msg-canary</value></data>\
          <action name=\"event_sequence\" package=\"package0\"><value>2</value></action>\
          <action name=\"session_id\" package=\"sqlserver\"><value>65</value></action>\
          <action name=\"request_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"transaction_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"database_id\" package=\"sqlserver\"><value>5</value></action>\
          <action name=\"database_name\" package=\"sqlserver\"><value>culprit_xel_lab</value></action>\
          <action name=\"client_app_name\" package=\"sqlserver\"><value>SQLCMD</value></action>\
          <action name=\"username\" package=\"sqlserver\"><value>sa</value></action>\
          <action name=\"client_hostname\" package=\"sqlserver\"><value>{host}</value></action>\
          <action name=\"sql_text\" package=\"sqlserver\"><value>SELECT N'private-xel-error-canary'; RAISERROR(N'private-xel-msg-canary', 16, 1);\n</value></action>\
        </event>\
        <event name=\"attention\" package=\"sqlserver\" timestamp=\"{ts2}\">\
          <data name=\"duration\"><value>{attention_duration}</value></data>\
          <action name=\"event_sequence\" package=\"package0\"><value>3</value></action>\
          <action name=\"session_id\" package=\"sqlserver\"><value>65</value></action>\
          <action name=\"request_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"transaction_id\" package=\"sqlserver\"><value>0</value></action>\
          <action name=\"database_id\" package=\"sqlserver\"><value>5</value></action>\
          <action name=\"database_name\" package=\"sqlserver\"><value>culprit_xel_lab</value></action>\
          <action name=\"client_app_name\" package=\"sqlserver\"><value>SQLCMD</value></action>\
          <action name=\"username\" package=\"sqlserver\"><value>sa</value></action>\
          <action name=\"client_hostname\" package=\"sqlserver\"><value>{host}</value></action>\
          <action name=\"sql_text\" package=\"sqlserver\"><value>SELECT N'private-xel-cancel-canary'; WAITFOR DELAY '00:00:05';</value></action>\
        </event>\
        <event name=\"xml_deadlock_report\" package=\"sqlserver\" timestamp=\"{ts3}\">\
          <data name=\"xml_report\"><value>{deadlock_xml}</value></data>\
          <action name=\"event_sequence\" package=\"package0\"><value>4</value></action>\
          <action name=\"session_id\" package=\"sqlserver\"><value>54</value></action>\
          <action name=\"database_id\" package=\"sqlserver\"><value>1</value></action>\
          <action name=\"database_name\" package=\"sqlserver\"><value>master</value></action>\
        </event>\
        </events>",
        ts0 = xel_batch.records[0].timestamp.as_deref().unwrap(),
        ts1 = xel_batch.records[1].timestamp.as_deref().unwrap(),
        ts2 = xel_batch.records[2].timestamp.as_deref().unwrap(),
        ts3 = xel_batch.records[3].timestamp.as_deref().unwrap(),
    );

    let xml_batch = parse_xml(exported_xml.as_bytes(), NativeLogLimits::default()).unwrap();
    assert!(xml_batch.issues.is_empty());
    assert_eq!(xml_batch.records.len(), xel_batch.records.len());

    for (mut from_xel, from_xml) in xel_batch.records.into_iter().zip(xml_batch.records) {
        from_xel.position = from_xml.position;
        assert_eq!(from_xel, from_xml);
    }

    let mut xel_graph = xel_batch.deadlocks[0].1.clone();
    let xml_graph = xml_batch.deadlocks[0].1.clone();
    assert_eq!(xel_graph.source, "supplied-extended-events-xel");
    assert_eq!(xml_graph.source, "supplied-extended-events-xml");
    xel_graph.source = xml_graph.source.clone();
    xel_graph.id = xml_graph.id.clone();
    assert_eq!(xel_graph, xml_graph);
}

#[test]
fn malformed_truncated_version_unknown_and_budget_limited_xel_inputs_preserve_explicit_partial_semantics() {
    assert_eq!(parse_xel(b"<events/>", Default::default()).err(), Some(NativeLogProblem::NativeShape));
    assert_eq!(parse_xel(&SAMPLE_EVENTS_XEL[..100], Default::default()).err(), Some(NativeLogProblem::MalformedRecord));

    for unknown_version in [2u16, 99u16] {
        let mut mutated = SAMPLE_EVENTS_XEL.to_vec();
        mutated[4..6].copy_from_slice(&unknown_version.to_le_bytes());
        assert_eq!(parse_xel(&mutated, Default::default()).err(), Some(NativeLogProblem::UnsupportedVersion));
    }

    // Truncating inside Block 3 (0x6000..0x7000) retains the two verified errors from Block 2 (0x5000) as partial.
    let truncated = parse_xel(&SAMPLE_EVENTS_XEL[..0x6500], Default::default()).unwrap();
    assert_eq!(truncated.records.len(), 2);
    assert_eq!(truncated.stopped, Some(NativeLogProblem::MalformedRecord));

    // Corrupting the payload length inside Block 3 stops mid-buffer without claiming complete absence.
    let mut mid_corrupt = SAMPLE_EVENTS_XEL.to_vec();
    mid_corrupt[0x6000 + EVENT_HEADER_BYTES + 20..0x6000 + EVENT_HEADER_BYTES + 24]
        .copy_from_slice(&0x00FF_FFFFu32.to_le_bytes());
    let mid_batch = parse_xel(&mid_corrupt, Default::default()).unwrap();
    assert_eq!(mid_batch.records.len(), 2);
    assert_eq!(mid_batch.stopped, Some(NativeLogProblem::MalformedRecord));

    // Record limit stops deterministically.
    let capped = parse_xel(SAMPLE_EVENTS_XEL, NativeLogLimits { records: 2, ..Default::default() }).unwrap();
    assert_eq!(capped.records_examined, 2);
    assert_eq!(capped.records.len(), 1);
    assert_eq!(capped.stopped, Some(NativeLogProblem::RecordLimit));

    // Input byte limit refuses oversized buffer.
    assert_eq!(
        parse_xel(
            SAMPLE_EVENTS_XEL,
            NativeLogLimits {
                input_bytes: SAMPLE_EVENTS_XEL.len() - 1,
                record_bytes: 4096,
                field_bytes: 1024,
                records: 64
            }
        )
        .err(),
        Some(NativeLogProblem::InputLimit)
    );
}
