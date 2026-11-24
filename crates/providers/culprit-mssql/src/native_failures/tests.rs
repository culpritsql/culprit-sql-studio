use super::*;
use crate::{
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};
use culprit_core::telemetry::MssqlExtendedEventKind;

fn sys_health_config() -> Rec {
    Rec::of(&[
        ("observed_at", Val::Text("2026-10-11T12:00:01Z".into())),
        ("event_session_id", Val::Int(65536)),
        ("target_id", Val::Int(1)),
        ("target_package", Val::Text("package0".into())),
        ("file_path_type", Val::Text("nvarchar".into())),
        ("file_path_bytes", Val::Int(72)),
        ("file_path", Val::Text("/var/opt/mssql/log/system_health.xel".into())),
        ("max_file_size_mb", Val::Int(5)),
        ("max_rollover_files", Val::Int(4)),
        ("has_error_reported", Val::Int(1)),
        ("has_attention", Val::Int(1)),
        ("has_deadlock_report", Val::Int(1)),
        ("has_event_predicate", Val::Int(0)),
    ])
}

fn xel_row(file_name: &str, file_offset: i64, timestamp_utc: &str, xml: &str) -> Rec {
    Rec::of(&[
        ("file_name", Val::Text(file_name.into())),
        ("file_offset", Val::Int(file_offset)),
        ("timestamp_utc", Val::Text(timestamp_utc.into())),
        ("event_bytes", Val::Int((xml.len() * 2) as i64)),
        ("event_xml", Val::Text(xml.into())),
    ])
}

fn attention_xml(at: &str, sql: &str) -> String {
    format!(
        "<event name=\"attention\" package=\"sqlserver\" timestamp=\"{at}\"><data name=\"duration\"><value>2500000</value></data><action name=\"session_id\" package=\"sqlserver\"><value>59</value></action><action name=\"client_app_name\" package=\"sqlserver\"><value>checkout-api</value></action><action name=\"sql_text\" package=\"sqlserver\"><value>{sql}</value></action></event>"
    )
}

fn error_xml(at: &str, number: u32, severity: u8, intercepted: bool) -> String {
    format!(
        "<event name=\"error_reported\" package=\"sqlserver\" timestamp=\"{at}\"><data name=\"error_number\"><value>{number}</value></data><data name=\"severity\"><value>{severity}</value></data><data name=\"state\"><value>1</value></data><data name=\"is_intercepted\"><value>{intercepted}</value></data><data name=\"message\"><value>private error text</value></data></event>"
    )
}

#[tokio::test]
async fn same_buffer_file_offset_and_identical_events_stay_distinct_and_mid_buffer_caps_hold_back_watermarks() {
    let config = sys_health_config();
    let at = "2026-10-11T12:00:00.1234567Z";
    let identical = attention_xml(at, "SELECT 'secret-literal' WHERE order_id = 42");
    let fatal = error_xml("2026-10-11T12:00:00.2234567Z", 701, 17, false);
    let file_a = "/var/opt/mssql/log/system_health_0_100.xel";
    let rows = vec![
        xel_row(file_a, 512, at, &identical),
        xel_row(file_a, 512, at, &identical),
        xel_row(file_a, 1024, "2026-10-11T12:00:00.2234567Z", &fatal),
        xel_row(
            file_a,
            1024,
            "2026-10-11T12:00:00.3234567Z",
            &error_xml("2026-10-11T12:00:00.3234567Z", 1222, 16, false),
        ),
    ];

    // Limit = 3 cuts right inside buffer offset 1024 (row 2 and row 3 share offset 1024).
    let mut io = FakeRead::new(vec![Ok(vec![config.clone()]), Ok(rows.clone())]);
    let target = SystemHealthEventTarget::read(&mut io).await.unwrap();
    let mid_buffer = target.read_incremental(&mut io, None, 3).await.unwrap();
    assert_eq!(mid_buffer.observations.len(), 3);
    assert_ne!(mid_buffer.observations[0].position, mid_buffer.observations[1].position);
    assert_eq!(
        mid_buffer.observations[0].kind,
        MssqlExtendedEventKind::Attention { native_duration: Some("2500000".into()) }
    );
    assert_eq!(mid_buffer.observations[1].kind, mid_buffer.observations[0].kind);
    assert!(!serde_json::to_string(&mid_buffer.observations).unwrap().contains("secret-literal"));
    assert!(mid_buffer.truncated);
    assert!(mid_buffer.partial_buffer, "cap occurred mid-buffer at offset 1024");
    assert!(!mid_buffer.complete_scope, "mid-buffer cap must retain partial coverage");
    assert_eq!(
        mid_buffer.completed_watermark,
        Some(XelBufferWatermark { file_name: file_a.into(), file_offset: 512 }),
        "never advance initial_offset to buffer 1024 before all of buffer 1024 has been read"
    );
    assert!(!io.calls[1].sql.contains("ORDER BY"), "live file reads must not sort across rollover files");
    assert!(io.calls.iter().all(|call| !call.sql.contains("dm_xe_session_targets")));

    // Complete read of all 4 rows advances watermark to 1024 and marks unfiltered full event set complete.
    let mut io_full = FakeRead::new(vec![Ok(rows)]);
    let complete = target.read_incremental(&mut io_full, None, 10).await.unwrap();
    assert_eq!(complete.observations.len(), 4);
    assert!(!complete.truncated && !complete.partial_buffer && !complete.has_row_gap);
    assert!(complete.complete_scope);
    assert_eq!(complete.completed_watermark, Some(XelBufferWatermark { file_name: file_a.into(), file_offset: 1024 }));

    // Predicated session (default system_health behavior) stays partial even when every returned row is read.
    let predicated_config = replace(&config, &[("has_event_predicate", Val::Int(1))]);
    let mut io_pred = FakeRead::new(vec![Ok(vec![predicated_config]), Ok(vec![])]);
    let predicated_target = SystemHealthEventTarget::read(&mut io_pred).await.unwrap();
    let empty_predicated = predicated_target.read_incremental(&mut io_pred, None, 10).await.unwrap();
    assert!(!empty_predicated.complete_scope, "filtered system_health must never claim complete absence");
}

#[tokio::test]
async fn missing_config_oversized_targets_permissions_and_database_session_guards_never_touch_azure_dl() {
    // Missing configuration -> Unsupported.
    let mut missing = FakeRead::new(vec![Ok(vec![])]);
    assert_eq!(SystemHealthEventTarget::read(&mut missing).await.unwrap_err().code, ErrorCode::Unsupported);

    // Oversized file target -> ByteLimit.
    let oversized = replace(&sys_health_config(), &[("max_file_size_mb", Val::Int(2048))]);
    let mut io_big = FakeRead::new(vec![Ok(vec![oversized])]);
    assert_eq!(
        CollectionStop::from_error(&SystemHealthEventTarget::read(&mut io_big).await.unwrap_err()),
        CollectionStop::ByteLimit
    );

    // Permission denied attaches explicit least-privilege hint without session changes.
    let denied = Error::new(ErrorCode::PermissionDenied, "VIEW SERVER STATE denied");
    let mut io_denied = FakeRead::new(vec![Err(ReadError::Source(denied))]);
    let err = SystemHealthEventTarget::read(&mut io_denied).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    assert!(err.hint.is_some());

    // Azure SQL Database missing `culprit_events` returns explicit setup guidance and never queries `dl`.
    let azure_gov = Rec::of(&[
        ("database_name", Val::Text("azure-db".into())),
        ("incarnation", Val::Text("00000000-0000-0000-0000-000000000001".into())),
        ("physical_incarnation", Val::Text("00000000-0000-0000-0000-000000000002".into())),
        ("updateability", Val::Text("READ_WRITE".into())),
    ]);
    let mut io_azure =
        FakeRead::new(vec![Ok(vec![Rec::of(&[("edition", Val::Int(5))])]), Ok(vec![azure_gov]), Ok(vec![])]);
    let (_, coverage) = collect_from(&mut io_azure, "azure.fixture.invalid", 1433).await;
    assert_eq!(
        coverage.outcomes().into_iter().find(|o| o.source == sources::NATIVE_FAILURES.name).unwrap().state,
        culprit_core::SourceState::Unavailable
    );
    assert!(io_azure.calls.iter().all(|call| !call.sql.contains("'dl'") && !call.sql.contains("N'dl'")));
    assert!(io_azure.calls.iter().any(|call| {
        call.parameters.iter().any(|param| matches!(param, Parameter::Text(value) if value == DATABASE_SESSION))
    }));

    // Approved database `culprit_events` ring-buffer exports cleanly with database incarnation + session epoch.
    let db_config = Rec::of(&[
        ("observed_at", Val::Text("2026-10-11T12:00:01Z".into())),
        ("event_session_id", Val::Int(65537)),
        ("generation", Val::Text("2026-10-11T11:55:00.0000000".into())),
        ("memory_kib", Val::Int(1024)),
        ("maximum_events", Val::Int(256)),
        ("has_error_reported", Val::Int(1)),
        ("has_attention", Val::Int(1)),
        ("has_deadlock_report", Val::Int(1)),
        ("has_event_predicate", Val::Int(0)),
    ]);
    let ring_xml = format!(
        "<RingBufferTarget truncated=\"0\" eventCount=\"1\" droppedCount=\"0\">{}</RingBufferTarget>",
        error_xml("2026-10-11T12:00:01.1000000Z", 1222, 16, false)
    );
    let db_target = Rec::of(&[
        ("observed_until", Val::Text("2026-10-11T12:00:02Z".into())),
        ("target_bytes", Val::Int((ring_xml.len() * 2) as i64)),
        ("target_xml", Val::Text(ring_xml)),
    ]);
    let mut io_db_ok = FakeRead::new(vec![
        Ok(vec![Rec::of(&[
            ("database_name", Val::Text("azure-db".into())),
            ("incarnation", Val::Text("00000000-0000-0000-0000-000000000001".into())),
            ("physical_incarnation", Val::Text("00000000-0000-0000-0000-000000000002".into())),
            ("updateability", Val::Text("READ_WRITE".into())),
        ])]),
        Ok(vec![db_config]),
        Ok(vec![db_target]),
    ]);
    let db_context = database_identity::read_azure(&mut io_db_ok).await.unwrap();
    let captured = read_database_session(&mut io_db_ok, &db_context, "azure.fixture.invalid", 1433, 16).await.unwrap();
    assert!(captured.complete_scope);
    assert!(captured.description.partition_is_consistent());
    let export_id = culprit_core::telemetry::CaptureIdentity {
        observation_id: "db-events-test".into(),
        revision: Some(culprit_core::telemetry::CaptureRevision {
            outbox_id: "outbox-1".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    assert_eq!(captured.records.len(), 1);
    for record in captured.records {
        culprit_core::telemetry::ExportedRecord::new(&captured.description, &export_id, record).unwrap();
    }
}
