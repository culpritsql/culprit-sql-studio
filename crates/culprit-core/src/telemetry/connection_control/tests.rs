use super::*;

fn observation(connections: Option<ReadConnectionWork>) -> CollectorConnectionObservation {
    CollectorConnectionObservation {
        original: DiagnosticAttemptReceipt {
            capture: CaptureIdentity {
                observation_id: "a".repeat(32),
                revision: Some(CaptureRevision {
                    outbox_id: "b".repeat(32),
                    owner_epoch: "1".into(),
                    sequence: "2".into(),
                }),
            },
            manifest_sha256: Some(digest(b"original terminal manifest")),
        },
        original_state: OutboxCaptureState::Incomplete,
        original_status: Some(CollectionStatus::Incomplete(CollectionStop::SourceUnavailable)),
        original_evidence: CollectorConnectionOriginalEvidence::NoNativeEvidence,
        source_work: SourceWorkState::Quiescent,
        collector_outcome: CollectorCaptureOutcome::Returned,
        collector_started_at: "2026-11-23T01:00:00Z".into(),
        collector_finished_at: "2026-11-23T01:00:01Z".into(),
        collector_elapsed_ms: Some(1.0),
        connections,
        original_source_queries: Some(1),
        native: None,
    }
}
fn connected() -> ReadConnectionWork {
    let recorder = ReadConnectionRecorder::default();
    let attempt = recorder.begin();
    attempt.connection_use(ReadConnectionUse::AuthenticatedReuse);
    attempt.acquired(17);
    attempt.phase(ReadConnectionStage::Acquire, None).finish(ReadConnectionOutcome::Completed);
    attempt
        .phase(ReadConnectionStage::FirstQuery, Some(digest(b"existing source SQL")))
        .finish(ReadConnectionOutcome::Completed);
    recorder.snapshot().unwrap()
}
fn source(row: &CollectorConnectionObservation) -> CollectionDescription {
    CollectionDescription {
        target: CollectionTarget::ConfiguredConnection(
            ConfiguredConnectionIdentity::new("collector", &digest(b"route"), "configured-alias", Engine::Mssql)
                .unwrap(),
        ),
        partition: SourcePartition::Target,
        source: COLLECTOR_CONNECTION_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::CollectorConnection(CollectorConnectionDescription {
            attempt: row.original.capture.clone(),
            snapshot_sha256: row.snapshot_sha256().unwrap(),
            observed_from: row.collector_started_at.clone(),
            observed_until: row.collector_finished_at.clone(),
        }),
    }
}
#[test]
fn success_requires_real_completed_connection_and_first_source_query_without_claiming_native_health() {
    let row = observation(Some(connected()));
    assert!(row.is_valid());
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Success);
    assert_eq!(row.original_state, OutboxCaptureState::Incomplete);
    let source = source(&row);
    assert!(source.partition_is_consistent());
    assert!(!source.is_partition_qualified());
    assert!(source.target.database().is_none());
    assert_eq!(source.target.measurement_scope(), crate::MeasurementScope::Unknown);
    assert_eq!(TelemetryStream::CollectorConnections.semantics(), TelemetrySemantics::Control);
    assert!(
        NativeQueryIdentity { query_id: "1".into(), qualifiers: Default::default() }.definition_id(&source).is_err()
    );
    let context = NativeReadContext {
        target: source.target.clone(),
        partition: source.partition.clone(),
        observed_from: row.collector_started_at.clone(),
        observed_until: row.collector_finished_at.clone(),
        database_writable: Some(true),
    };
    assert!(!context.is_valid());
    assert!(!context.has_qualified_local_owner());
}
#[test]
fn auth_and_query_failures_preserve_typed_stage_codes_and_never_become_empty_success() {
    for (stage, code, native) in [
        (ReadConnectionStage::Acquire, ErrorCode::AuthFailed, 18456),
        (ReadConnectionStage::FirstQuery, ErrorCode::PermissionDenied, 229),
    ] {
        let recorder = ReadConnectionRecorder::default();
        let attempt = recorder.begin();
        attempt.connection_use(ReadConnectionUse::FreshConnection);
        if stage == ReadConnectionStage::FirstQuery {
            attempt.acquired(1);
            attempt.phase(ReadConnectionStage::Acquire, None).finish(ReadConnectionOutcome::Completed);
        }
        attempt
            .phase(stage, (stage == ReadConnectionStage::FirstQuery).then(|| digest(b"private-query-canary")))
            .finish(ReadConnectionOutcome::Failed { code, native_code: Some(native) });
        let mut row = observation(recorder.snapshot());
        row.collector_outcome = CollectorCaptureOutcome::Failed { code };
        assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Failure);
        let phase = row.connections.as_ref().unwrap().attempts[0].phases.last().unwrap();
        assert_eq!(phase.outcome, ReadConnectionOutcome::Failed { code, native_code: Some(native) });
        assert!(!serde_json::to_string(&row).unwrap().contains("private-query-canary"));
    }
}
#[test]
fn missing_omitted_and_unconfirmed_observations_retain_unknown_instead_of_measured_zero() {
    assert_eq!(observation(None).connection_outcome(), CollectorConnectionOutcome::Unknown);
    let mut work = connected();
    work.omitted_attempts = 1;
    work.attempted += 1;
    assert_eq!(observation(Some(work)).connection_outcome(), CollectorConnectionOutcome::Unknown);
    let mut row = observation(Some(connected()));
    row.source_work = SourceWorkState::Unconfirmed;
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
    let mut row = observation(None);
    row.original.manifest_sha256 = None;
    row.original_source_queries = None;
    row.original_status = None;
    row.original_evidence = CollectorConnectionOriginalEvidence::ManifestUnavailable;
    row.original_state = OutboxCaptureState::Interrupted;
    row.collector_outcome = CollectorCaptureOutcome::Unknown;
    row.collector_elapsed_ms = None;
    assert!(row.is_valid());
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
    row.connections = Some(connected());
    assert!(!row.is_valid());
}
#[test]
fn configured_control_cannot_be_rebound_to_database_native_rows_or_other_subjects() {
    let row = observation(Some(connected()));
    let mut source = source(&row);
    let CollectionTarget::ConfiguredConnection(connection) = &source.target else { panic!() };
    let mut request = CollectionRequest {
        subject: CollectionSubject::ConfiguredConnection { connection: connection.clone() },
        selection: CollectionSelection::CollectorConnection { attempt: row.original.capture.clone() },
        budget: Default::default(),
    };
    request.validate().unwrap();
    assert!(source.matches_request(&request));
    request.selection = CollectionSelection::Activity;
    assert!(request.validate().is_err());
    request.selection = CollectionSelection::CollectorConnection { attempt: row.original.capture.clone() };
    request.subject = CollectionSubject::Database;
    assert!(request.validate().is_err());
    source.target = CollectionTarget::Database(crate::DatabaseIdentity::server(
        Engine::Mssql,
        "invented",
        "host.invalid",
        1433,
        None,
    ));
    assert!(!source.partition_is_consistent());
    assert!(!row.fits_source(&source));
}
#[test]
fn client_clock_regression_keeps_original_monotonic_duration_and_changes_the_snapshot_identity() {
    let mut row = observation(Some(connected()));
    let before = row.snapshot_sha256().unwrap();
    row.collector_finished_at = "2026-11-22T23:59:59Z".into();
    assert!(row.is_valid());
    assert!(!row.ordered_collector_clocks());
    assert_eq!(row.collector_elapsed_ms, Some(1.0));
    assert_ne!(row.snapshot_sha256().unwrap(), before);
    let encoded = serde_json::to_vec(&row).unwrap();
    let restored: CollectorConnectionObservation = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored, row);
}

#[test]
fn acquired_control_connection_does_not_invent_first_query_measurement_or_hide_incomplete_source_status() {
    let recorder = ReadConnectionRecorder::default();
    let attempt = recorder.begin();
    attempt.connection_use(ReadConnectionUse::FreshConnection);
    attempt.acquired(1);
    attempt.phase(ReadConnectionStage::Acquire, None).finish(ReadConnectionOutcome::Completed);
    let mut row = observation(recorder.snapshot());
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
    row.original_status = Some(CollectionStatus::Complete);
    row.original_state = OutboxCaptureState::Ready;
    row.original_evidence = CollectorConnectionOriginalEvidence::MayContainNativeEvidence;
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Success);
    assert_eq!(row.connections.as_ref().unwrap().attempts[0].phases.len(), 1);
    row.source_work = SourceWorkState::NotStarted;
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
}
