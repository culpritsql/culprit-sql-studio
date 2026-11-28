use culprit_core::{DatabaseIdentity, Engine, telemetry::*};

pub fn clickhouse_failure() -> (CollectionDescription, TelemetryRecord) {
    let failure = ClickHouseObservation::Failure(ClickHouseFailure {
        node: "node-a".into(),
        database: "sales".into(),
        query_id: "failed-insert".into(),
        initial_query_id: "failed-insert".into(),
        family: "941122".into(),
        started_us: 1735732800000000,
        ended_us: 1735732802000000,
        outcome: "ExceptionWhileProcessing".into(),
        exception_code: 252,
        exception_name: "TOO_MANY_PARTS".into(),
        query_duration_ms: 2000,
        read_rows: 0,
        read_bytes: 0,
        written_rows: 0,
        written_bytes: 0,
        memory_usage: 65536,
        tables: vec!["sales.orders".into()],
        total_tables: 1,
        profile: [("SelectedParts".into(), 3)].into(),
    });
    let mut db = DatabaseIdentity::server(Engine::Clickhouse, "sales", "ch.example.test", 8443, Some("node-a".into()));
    db.incarnation = Some("637839ab-45c7-4091-b729-94717b33d2e4".into());
    let source_name = failure.source_name();
    let source = CollectionDescription {
        target: CollectionTarget::Database(db),
        partition: SourcePartition::Target,
        source: CLICKHOUSE_OPERATIONAL_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Snapshot(StateSnapshotDescription {
            dataset: SnapshotDataset::Operational,
            observed_from: "2025-01-01T12:01:00Z".into(),
            observed_until: "2025-01-01T12:01:01Z".into(),
            records: 1,
            sources: vec![StateSourceCoverage::observed(source_name.into(), 1, true)],
        }),
    };
    let record = TelemetryRecord::Operational(OperationalObservation {
        source: source_name.into(),
        key: failure.identity().key().unwrap(),
        fact: OperationalFact::ClickHouse(Box::new(failure)),
    });
    (source, record)
}

pub fn clickhouse_failure_with_family() -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, failure) = clickhouse_failure();
    let SourceObservation::Snapshot(header) = &mut source.observation else { unreachable!() };
    let family = ClickHouseObservation::QueryFamily(ClickHouseQueryFamily {
        node: "node-a".into(),
        database: "sales".into(),
        family: "941122".into(),
        from: "2025-01-01T11:46:00Z".into(),
        until: header.observed_from.clone(),
        population: ClickHousePopulation { executions: 20, elapsed_ms: 14000, families: 5 },
        first_seen: "2025-01-01T11:59:00Z".into(),
        last_seen: "2025-01-01T12:00:00Z".into(),
        sample: DefinitionPayload::sql("INSERT INTO sales.orders VALUES (9264)", Engine::Clickhouse),
        executions: 3,
        failures: 1,
        elapsed_ms: 2100,
        max_ms: 2000,
        rows_read: 0,
        bytes_read: 0,
        rows_returned: 0,
    });
    let source_name = family.source_name();
    header.records += 1;
    header.sources.push(StateSourceCoverage::observed(source_name.into(), 1, true));
    let record = TelemetryRecord::Operational(OperationalObservation {
        source: source_name.into(),
        key: family.identity().key().unwrap(),
        fact: OperationalFact::ClickHouse(Box::new(family)),
    });
    (source, vec![failure, record])
}
