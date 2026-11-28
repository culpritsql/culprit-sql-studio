use super::*;

fn failed_insert() -> ClickHouseFailure {
    let snapshot: crate::Snapshot =
        serde_json::from_str(include_str!("../../../../../fixtures/clickhouse-demo.json")).unwrap();
    snapshot
        .operations
        .clickhouse
        .into_iter()
        .find_map(|point| match point {
            ClickHouseObservation::Failure(failure) if failure.exception_name == "TOO_MANY_PARTS" => Some(failure),
            _ => None,
        })
        .unwrap()
}

#[test]
fn typed_native_error_families_and_user_cancellation_have_distinct_outcomes() {
    use ClickHouseIncident::*;
    let original = failed_insert();
    for (error, expected) in [
        ("MEMORY_LIMIT_EXCEEDED", Some(MemoryLimit)),
        ("TOO_MANY_PARTS", Some(PartsLimit)),
        ("TOO_MANY_SIMULTANEOUS_QUERIES", Some(ConcurrencyLimit)),
        ("SOCKET_TIMEOUT", Some(Timeout)),
        ("NOT_ENOUGH_SPACE", Some(DiskSpace)),
        ("KEEPER_EXCEPTION", Some(Keeper)),
        ("QUOTA_EXCEEDED", Some(QueryLimit)),
        ("NO_ACTIVE_REPLICAS", Some(ReplicaUnavailable)),
        ("NETWORK_ERROR", Some(NetworkFailure)),
        ("TABLE_IS_READ_ONLY", Some(ReadOnlyWrite)),
        ("UNKNOWN_TABLE", Some(RejectedQuery)),
        ("QUERY_WAS_CANCELLED", None),
    ] {
        let point =
            ClickHouseObservation::Failure(ClickHouseFailure { exception_name: error.into(), ..original.clone() });
        assert_eq!(point.incident(), expected, "{error}");
    }
}

#[test]
fn a_replica_queue_needs_native_delay_or_a_positive_fault_to_raise_an_incident() {
    let mut replica = ClickHouseReplica {
        node: "n".into(),
        database: "d".into(),
        table: "events".into(),
        replica_name: "r".into(),
        is_readonly: 0,
        is_session_expired: 0,
        queue_size: 10,
        oldest_seconds: 0,
        absolute_delay: 0,
    };
    let classify = |row: &ClickHouseReplica| ClickHouseObservation::Replica(row.clone()).incident();
    assert_eq!(classify(&replica), None);
    replica.absolute_delay = 300;
    assert_eq!(classify(&replica), Some(ClickHouseIncident::ReplicaDelay));
    replica.queue_size = 0;
    assert_eq!(classify(&replica), None);
    replica.is_readonly = 1;
    assert_eq!(classify(&replica), Some(ClickHouseIncident::ReplicaReadOnly));
    replica.is_session_expired = 1;
    assert_eq!(classify(&replica), Some(ClickHouseIncident::ReplicaSessionExpired));
}

#[test]
fn a_focused_receipt_accepts_only_its_request_and_its_named_table_and_time() {
    let failure = failed_insert();
    let mut target = crate::DatabaseIdentity::server(
        crate::Engine::Clickhouse,
        &failure.database,
        "test.example",
        8443,
        Some(failure.node.clone()),
    );
    target.incarnation = Some("dc394046-b438-420d-bf2d-dece80b81270".into());
    let scope =
        ClickHouseInvestigation { target: target.clone(), request: failure.request(), table: Some("events".into()) };
    let window = StateSnapshotDescription {
        dataset: SnapshotDataset::Operational,
        observed_from: "2025-02-01T00:00:00Z".into(),
        observed_until: "2025-02-01T00:00:01Z".into(),
        records: 1,
        sources: vec![StateSourceCoverage::collected("system.part_log".into(), 1)],
    };
    let mut source = CollectionDescription {
        target: CollectionTarget::Database(target),
        partition: SourcePartition::Target,
        source: CLICKHOUSE_OPERATIONAL_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::ClickHouseInvestigation { snapshot: window.clone(), focus: Box::new(scope) },
    };
    let event = ClickHousePartEvent {
        node: failure.node.clone(),
        database: failure.database.clone(),
        table: "events".into(),
        partition_id: "p".into(),
        part_name: "p_1_1".into(),
        query_id: "another-request".into(),
        event: "NewPart".into(),
        occurred_us: failure.ended_us,
        duration_ms: 0,
        rows: 4,
        size_in_bytes: 100,
        error: 0,
    };
    let fits = |row, source: &CollectionDescription| ClickHouseObservation::PartEvent(row).fits_source(source);
    assert!(fits(event.clone(), &source));
    assert!(!fits(ClickHousePartEvent { table: "other".into(), ..event.clone() }, &source));
    assert!(!fits(ClickHousePartEvent { occurred_us: event.occurred_us + 1, ..event.clone() }, &source));
    assert!(!ClickHouseObservation::Failure(failure).fits_source(&source));
    source.observation = SourceObservation::Snapshot(window);
    assert!(!fits(event, &source));
}
