use super::*;
fn row() -> MssqlExtendedEventObservation {
    MssqlExtendedEventObservation {
        position: 0,
        kind: MssqlExtendedEventKind::Attention { native_duration: Some(u64::MAX.to_string()) },
        timestamp: Some("2026-10-25T03:30:00".into()),
        timestamp_utc: None,
        event_sequence: None,
        session_id: Some("59".into()),
        request_id: Some("0".into()),
        transaction_id: None,
        database_id: Some("8".into()),
        database: Some("context-only".into()),
        client: Some(ActivityClient {
            application: Some("driver-label".into()),
            user: Some("private-user".into()),
            host: Some("private-host".into()),
        }),
        statement: DefinitionPayload::sql("SELECT 'private-literal' WHERE id=42", crate::Engine::Mssql),
    }
}
#[test]
fn missing_local_dst_and_explicit_offset_clocks_are_distinct_and_not_repaired_with_intake_time() {
    let mut row = row();
    for (native, expected) in [
        (None, None),
        (Some("2026-10-25T03:30:00"), None),
        (Some("2026-10-25T03:30:00+02:00"), Some("2026-10-25T01:30:00.000000000Z")),
    ] {
        row.timestamp = native.map(str::to_owned);
        row.timestamp_utc = native.and_then(MssqlExtendedEventObservation::qualified_timestamp);
        assert_eq!(row.timestamp_utc.as_deref(), expected);
        assert!(row.is_valid());
        row.timestamp_utc = Some("2026-11-22T12:00:00Z".into());
        assert!(!row.is_valid());
    }
    for native in ["not-a-clock", "2026-10-25T03:30:00.1234567890", "2016-12-31T23:59:60Z"] {
        row.timestamp = Some(native.into());
        row.timestamp_utc = None;
        assert!(!row.is_valid(), "{native}");
    }
}
#[test]
fn native_numbers_nulls_and_unclassified_outcomes_are_retained_without_generations_or_timeout_claims() {
    let mut row = row();
    assert!(row.is_valid());
    for bad in ["-1", "+59", "059", "2147483648"] {
        row.session_id = Some(bad.into());
        assert!(!row.is_valid());
    }
    row.session_id = None;
    row.request_id = None;
    for severity in [0, 10, 16, 25] {
        row.kind = MssqlExtendedEventKind::ErrorReported {
            number: 50001,
            severity,
            state: 255,
            intercepted: Some(true),
            user_defined: Some(true),
        };
        assert!(row.is_valid());
    }
    row.kind = MssqlExtendedEventKind::ErrorReported {
        number: 701,
        severity: 26,
        state: 1,
        intercepted: None,
        user_defined: None,
    };
    assert!(!row.is_valid());
    row.kind = MssqlExtendedEventKind::DeadlockReport { graphs: 0 };
    assert!(!row.is_valid());
}
#[test]
fn ordinary_canonical_redaction_retains_native_client_context_but_explicit_sharing_withdraws_it() {
    let row = row();
    let mut snapshot = NativeFailureSnapshot {
        source: "supplied-mssql-event-xml".into(),
        observed_from: "2026-11-22T12:00:00Z".into(),
        observed_until: "2026-11-22T12:00:01Z".into(),
        resource: None,
        observations: vec![NativeFailureRecord::MssqlExtendedEvent(Box::new(row))],
    };
    assert!(snapshot.is_valid());
    snapshot.redact();
    let NativeFailureRecord::MssqlExtendedEvent(row) = &snapshot.observations[0] else { panic!() };
    assert_eq!(row.client.as_ref().unwrap().user.as_deref(), Some("private-user"));
    assert!(!serde_json::to_string(&snapshot).unwrap().contains("private-literal"));
    snapshot.redact_for_sharing();
    assert!(snapshot.is_valid());
    let text = serde_json::to_string(&snapshot).unwrap();
    assert!(!text.contains("private-user"));
    assert!(!text.contains("private-host"));
}

#[test]
fn imported_context_cannot_borrow_a_foreign_engine_or_a_pool_resource_owner() {
    let mut snapshot = NativeFailureSnapshot {
        source: "supplied-mssql-event-xml".into(),
        observed_from: "2026-11-22T12:00:00Z".into(),
        observed_until: "2026-11-22T12:00:01Z".into(),
        resource: Some(SharedResourceIdentity {
            engine: crate::Engine::Postgres,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new("native-namespace"),
            native: NativeIdentity::new("native-instance"),
        }),
        observations: vec![NativeFailureRecord::MssqlExtendedEvent(Box::new(row()))],
    };
    assert!(snapshot.resource.as_ref().unwrap().is_valid());
    assert!(!snapshot.is_valid());
    let resource = snapshot.resource.as_mut().unwrap();
    resource.engine = crate::Engine::Mssql;
    resource.kind = SharedResourceKind::Pool;
    assert!(!snapshot.is_valid());
    snapshot.resource = None;
    assert!(snapshot.is_valid(), "unowned supplied artifact remains useful without current authority");
}
