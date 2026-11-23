use super::*;
fn source() -> CollectionDescription {
    let mut database = crate::DatabaseIdentity::server(crate::Engine::Mssql, "shop", "fixture.invalid", 1433, None);
    database.incarnation = Some("11111111-1111-1111-1111-111111111111".into());
    let target = CollectionTarget::Database(database);
    let history = MssqlReadableResourceHistory {
        history: SampleHistoryDescription {
            from: "2020-01-01T06:00:00Z".into(),
            until: "2020-01-01T06:01:00Z".into(),
            observed_at: "2020-01-01T06:02:00Z".into(),
            available_from: Some("2020-01-01T05:00:00Z".into()),
            available_until: Some("2020-01-01T06:02:00Z".into()),
            expected_samples: 1,
            nominal_period_ms: Some(15000),
        },
        current_reader: Box::new(NativeReadContext {
            target: target.clone(),
            partition: SourcePartition::InstanceLocal { member: None, epoch: Some("a".repeat(64)) },
            observed_from: "2020-01-01T06:02:00Z".into(),
            observed_until: "2020-01-01T06:02:00Z".into(),
            database_writable: Some(false),
        }),
        observed_role: Some(MssqlResourceReplicaRole::HighAvailabilitySecondary),
    };
    CollectionDescription {
        target,
        partition: SourcePartition::Target,
        source: MSSQL_READABLE_RESOURCE_HISTORY_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::MssqlReadableResources(Box::new(history)),
    }
}
fn sample() -> ResourceMeasurement {
    ResourceMeasurement {
        mssql_managed_instance: None,
        data_io_accounting: DataIoAccounting::Unknown,
        at: "2020-01-01T06:00:15Z".into(),
        replica_id: None,
        replica_role: Some(NativeResourceReplicaRole::MssqlDbResourceStats(
            MssqlResourceReplicaRole::HighAvailabilitySecondary,
        )),
        metrics: vec![ResourceMetricSample {
            key: "cpu".into(),
            unit: ResourceUnit::Percent,
            scope: crate::MeasurementScope::Database,
            statistic: ResourceStatistic::Mean,
            period_ms: Some(15000),
            limit_basis: ResourceLimitBasis::DatabaseLimit,
            value: Some(0.0),
        }],
    }
}
#[test]
fn readable_history_roundtrip_preserves_separate_current_reader_and_unknown_historical_ownership() {
    let source = source();
    assert!(source.partition_is_consistent());
    let capture = CaptureIdentity { observation_id: "controlled-readable-history".into(), revision: None };
    let original = ExportedRecord::new(&source, &capture, TelemetryRecord::ResourceSample(sample())).unwrap();
    let encoded = serde_json::to_string(&source).unwrap();
    let decoded: CollectionDescription = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, source);
    assert_eq!(original.record_id, ExportedRecord::new(&decoded, &capture, original.record.clone()).unwrap().record_id);
    assert_eq!(source.observation.native_coverage_state().unwrap(), Some(crate::SourceState::Partial));
    let SourceObservation::MssqlReadableResources(history) = &source.observation else { panic!() };
    assert!(history.limitations().contains(&MssqlReadableResourceLimitation::CurrentMemberUnknown));
    assert_eq!(history.history.nominal_period_ms, Some(15000));
}
#[test]
fn current_member_primary_role_scope_alias_and_sample_identity_cannot_repair_historical_unknowns() {
    let source = source();
    let capture = CaptureIdentity { observation_id: "controlled-readable-history".into(), revision: None };
    for role in [
        None,
        Some(NativeResourceReplicaRole::MssqlDbResourceStats(MssqlResourceReplicaRole::Primary)),
        Some(NativeResourceReplicaRole::MssqlDbResourceStats(MssqlResourceReplicaRole::NamedReplica)),
    ] {
        let mut row = sample();
        row.replica_role = role;
        assert!(ExportedRecord::new(&source, &capture, TelemetryRecord::ResourceSample(row)).is_err());
    }
    let mut row = sample();
    row.replica_id = Some("91487244-730d-4b83-8e49-d482241c7288".into());
    assert!(ExportedRecord::new(&source, &capture, TelemetryRecord::ResourceSample(row)).is_err());
    let mut alias = source.clone();
    alias.source = crate::source_id::RESOURCES.into();
    assert!(!alias.partition_is_consistent());
    let mut generic = source.clone();
    generic.observation = SourceObservation::RetainedSamples(source.observation.sample_history().unwrap().clone());
    assert!(!generic.partition_is_consistent());
    let mut changed = source.clone();
    let SourceObservation::MssqlReadableResources(history) = &mut changed.observation else { panic!() };
    history.current_reader.database_writable = Some(true);
    assert!(!changed.partition_is_consistent());
}
#[test]
fn native_window_bounds_do_not_invent_sample_continuity_or_erase_retention_and_freshness_gaps() {
    let source = source();
    let SourceObservation::MssqlReadableResources(mut history) = source.observation else { panic!() };
    assert!(history.history.covers_request());
    history.history.available_from = Some("2020-01-01T06:00:15Z".into());
    assert_eq!(history.history.coverage_stop(), Some(CollectionStop::RetentionGap));
    let observation = SourceObservation::MssqlReadableResources(history.clone());
    assert!(!observation.can_advance_history_window());
    history.history.available_from = None;
    history.history.available_until = None;
    history.history.expected_samples = 0;
    history.observed_role = None;
    assert!(history.is_valid());
    assert_eq!(history.history.coverage_stop(), Some(CollectionStop::SourceNotReady));
    assert!(!history.accepts(&sample()));
}

#[test]
fn capture_observation_identity_never_aliases_same_clock_members_or_claims_distinct_native_executions() {
    let source = source();
    let first = CaptureIdentity { observation_id: "first-read".into(), revision: None };
    let next = CaptureIdentity { observation_id: "next-read".into(), revision: None };
    let row = TelemetryRecord::ResourceSample(sample());
    let a = ExportedRecord::new(&source, &first, row.clone()).unwrap();
    let repeated = ExportedRecord::new(&source, &first, row.clone()).unwrap();
    let b = ExportedRecord::new(&source, &next, row.clone()).unwrap();
    assert_eq!(a.record_id, repeated.record_id);
    assert_ne!(a.record_id, b.record_id);
    assert_eq!(a.record.native_identity(), b.record.native_identity());
    assert_eq!(a.record.stream(), TelemetryStream::ResourceSamples);
    assert_eq!(a.record, b.record);
    let mut other = source.clone();
    let SourceObservation::MssqlReadableResources(history) = &mut other.observation else { panic!() };
    history.current_reader.partition = SourcePartition::InstanceLocal {
        member: Some(SharedResourceIdentity {
            engine: crate::Engine::Mssql,
            kind: SharedResourceKind::Node,
            namespace: NativeIdentity::new("11111111-1111-1111-1111-111111111111")
                .qualified("kind", "azure-sql-logical-database"),
            native: NativeIdentity::new("55555555-5555-5555-5555-555555555555")
                .qualified("kind", "azure-sql-hyperscale-replica"),
        }),
        epoch: Some("b".repeat(64)),
    };
    let c = ExportedRecord::new(&other, &next, row).unwrap();
    assert_ne!(a.record_id, c.record_id);
}
#[test]
fn original_primary_resource_sample_identity_remains_independent_of_capture_replay() {
    let mut source = source();
    let history = source.observation.sample_history().unwrap().clone();
    source.source = crate::source_id::RESOURCES.into();
    source.observation = SourceObservation::RetainedSamples(history);
    let mut sample = sample();
    sample.replica_role = Some(NativeResourceReplicaRole::MssqlDbResourceStats(MssqlResourceReplicaRole::Primary));
    let a = ExportedRecord::new(
        &source,
        &CaptureIdentity { observation_id: "primary-first".into(), revision: None },
        TelemetryRecord::ResourceSample(sample.clone()),
    )
    .unwrap();
    let b = ExportedRecord::new(
        &source,
        &CaptureIdentity { observation_id: "primary-repeat".into(), revision: None },
        TelemetryRecord::ResourceSample(sample),
    )
    .unwrap();
    assert_eq!(a.record_id, b.record_id);
}
