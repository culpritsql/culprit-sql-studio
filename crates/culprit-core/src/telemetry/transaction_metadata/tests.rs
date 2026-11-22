use super::*;
fn cache(count: Option<u32>, overflow: Option<bool>) -> PostgresSubtransactionCache {
    let mut actor = super::super::activity::tests::request("42");
    actor.key.session.generation = "2020-01-01T10:00:00Z".into();
    actor.key.request =
        Some(NativeActivityIdentity { id: "statement".into(), generation: "2020-01-01T11:00:00Z".into() });
    actor.scope = ActivityScopeBasis::SessionDatabase;
    actor.state = "active".into();
    actor.database = Some("db".into());
    actor.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    actor.session_context = Some(Box::new(ActivitySessionContext {
        lifecycle: PostgresSessionContext::lifecycle(false),
        login_age_ms: None,
        last_request_end_age_ms: None,
        client_library: None,
        client_process_id: None,
        native: Some(Box::new(SessionNativeContext::Postgres(PostgresSessionContext {
            database_oid: Some("42".into()),
            database_name: Some("db".into()),
            backend_type: Some("client backend".into()),
        }))),
    }));
    PostgresSubtransactionCache {
        backend_id: 0,
        actor,
        transaction_started_at: Some("2020-01-01T10:30:00Z".into()),
        cached_subxids: count,
        overflowed: overflow,
    }
}
fn slru() -> PostgresSlruCounters {
    PostgresSlruCounters {
        kind: PostgresSlruKind::Subtransaction,
        native_name: "subtransaction".into(),
        observed_at: "2020-01-01T12:00:00Z".into(),
        stats_reset: Some("2020-01-01T10:00:00Z".into()),
        blocks_zeroed: Some("0".into()),
        blocks_hit: Some("9223372036854775807".into()),
        blocks_read: Some("1".into()),
        blocks_written: None,
        blocks_exists: Some("0".into()),
        flushes: Some("0".into()),
        truncates: Some("0".into()),
    }
}
#[test]
fn native_flag_not_a_lock_or_savepoint_threshold_owns_overflow_and_null_stays_distinct() {
    for (count, flag) in
        [(Some(64), Some(false)), (Some(64), Some(true)), (Some(3), Some(true)), (Some(0), Some(true)), (None, None)]
    {
        let row = cache(count, flag);
        assert!(row.is_valid(), "count={count:?},flag={flag:?}");
        assert_ne!(row.backend_id.to_string(), row.actor.key.session.id);
        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["overflowed"], serde_json::json!(flag));
    }
    assert!(!cache(Some(64), None).is_valid());
    assert!(!cache(None, Some(false)).is_valid());
}
#[test]
fn foreign_lifecycle_database_scope_or_unestablished_native_actor_cannot_be_an_overflow_candidate() {
    let original = cache(Some(3), Some(true));
    let mut row = original.clone();
    row.actor.scope = ActivityScopeBasis::OtherDatabase;
    assert!(!row.is_valid());
    let mut row = original.clone();
    row.actor.session_context = None;
    assert!(!row.is_valid());
    let mut row = original;
    row.actor.key.session.id = "0".into();
    assert!(!row.is_valid());
}
#[test]
fn cumulative_native_counter_text_keeps_maximum_null_and_independent_reset_without_a_derived_rate() {
    let row = slru();
    assert!(row.is_valid());
    let json = serde_json::to_value(&row).unwrap();
    assert_eq!(json["blocks_hit"], "9223372036854775807");
    assert!(json["blocks_written"].is_null());
    let mut future = row.clone();
    future.stats_reset = Some("2020-01-01T12:00:01Z".into());
    assert!(
        future.is_valid(),
        "a raw reset after an opening stamp is retained; interpretation cannot compare it as an interval"
    );
    for value in ["-1", "1.0", "1e3", "00", "9223372036854775808"] {
        let mut row = row.clone();
        row.blocks_read = Some(value.into());
        assert!(!row.is_valid(), "{value}");
    }
    let mut wrong = row;
    wrong.native_name = "MultiXactMember".into();
    assert!(!wrong.is_valid());
}
#[test]
fn new_nullable_fields_require_presence_without_discarding_a_native_null() {
    let mut json = serde_json::to_value(cache(None, None)).unwrap();
    assert!(serde_json::from_value::<PostgresSubtransactionCache>(json.clone()).unwrap().is_valid());
    json.as_object_mut().unwrap().remove("overflowed");
    assert!(serde_json::from_value::<PostgresSubtransactionCache>(json).is_err());
    let mut json = serde_json::to_value(slru()).unwrap();
    json["stats_reset"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<PostgresSlruCounters>(json.clone()).unwrap().is_valid());
    json.as_object_mut().unwrap().remove("stats_reset");
    assert!(serde_json::from_value::<PostgresSlruCounters>(json).is_err());
}
