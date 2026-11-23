use super::*;
pub(super) struct Trial {
    pub source: CollectionDescription,
    pub original: ExportedRecord,
    pub point_source: CollectionDescription,
    pub point: ExportedRecord,
    pub answer: TransactionMetadataAnswer,
    pub checkpoint: DiagnosticCheckpoint,
}
fn capture(sequence: u64) -> CaptureIdentity {
    CaptureIdentity {
        observation_id: format!("metadata-observation-{sequence}"),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"metadata-observation-outbox"),
            owner_epoch: "1".into(),
            sequence: sequence.to_string(),
        }),
    }
}
pub(super) fn exported(
    source: &CollectionDescription,
    row: TelemetryRecord,
    sequence: u64,
) -> (ExportedRecord, DiagnosticEvidenceRef) {
    let row = ExportedRecord::new(source, &capture(sequence), row).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(source, &digest(sequence.to_string().as_bytes()), &row, 0).unwrap();
    (row, reference)
}
fn parent(at: &str) -> (CollectionDescription, ActivityObservation) {
    let mut actor = cache(Some(0), Some(false)).actor;
    let request = actor.key.request.as_ref().unwrap();
    actor.statement_identity = Some(crate::engines::postgres_activity_statement_identity(
        &actor.key.session.id,
        &actor.key.session.generation,
        &request.generation,
    ));
    actor.wait = Some(ActivityWait {
        class: Some(ActivityWaitClass::SubtransactionMetadata),
        native_type: Some("LWLock: SubtransSLRU".into()),
        identified_resources: vec![],
        resources: None,
        elapsed_ms: None,
        owners: vec![],
    });
    let source = CollectionDescription {
        target: DatabaseIdentity::server(Engine::Postgres, "db", "fixture.invalid", 5432, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("metadata-cluster"),
                native: NativeIdentity::new("metadata-member"),
            }),
            epoch: Some("metadata-native-process".into()),
        },
        source: "postgres-activity".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some("metadata-native-process".into()),
            database_writable: Some(true),
            observed_from: at.into(),
            observed_until: at.into(),
            at: at.into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ClientBackends,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected("postgres-activity".into(), 1),
        }),
    };
    (source, actor)
}
pub(super) fn point(
    focus: &SourceReadFocus,
    source: &CollectionDescription,
    actor: &ActivityObservation,
    from: &str,
    at: &str,
    until: &str,
    sequence: u64,
) -> (CollectionDescription, ExportedRecord, DiagnosticEvidenceRef) {
    let mut context = NativeReadContext::from_activity(source).unwrap();
    context.observed_from = from.into();
    context.observed_until = until.into();
    let source = CollectionDescription {
        target: source.target.clone(),
        partition: SourcePartition::Target,
        source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::TransactionMetadata,
                records: 1,
                observed_from: from.into(),
                observed_until: until.into(),
                sources: vec![StateSourceCoverage::observed(POSTGRES_TRANSACTION_METADATA_SOURCE.into(), 1, false)],
            },
            focus_key: focus.key().unwrap(),
            execution: Box::new(context),
        },
    };
    let mut cache = cache(Some(0), Some(false));
    cache.actor = actor.clone();
    let slrus = [
        (PostgresSlruKind::Subtransaction, "subtransaction"),
        (PostgresSlruKind::MultiXactMember, "multixact_member"),
        (PostgresSlruKind::MultiXactOffset, "multixact_offset"),
    ]
    .into_iter()
    .map(|(kind, name)| {
        let mut row = slru();
        row.kind = kind;
        row.native_name = name.into();
        row.observed_at = at.into();
        row
    })
    .collect();
    let point = PostgresTransactionMetadata {
        observed_at: at.into(),
        database_oid: "42".into(),
        database: "db".into(),
        focus: actor.key.clone(),
        request_end: BlockingRequestState::SameStatement,
        caches: vec![cache],
        cache_rows: 1,
        cache_limited: false,
        slrus,
        retention: RetentionSnapshot {
            holders: vec![],
            sources: vec![RetentionSourceCoverage {
                source: "pg_stat_activity".into(),
                state: crate::SourceState::Empty,
            }],
            coverage: RetentionCoverage { native_sources: vec!["pg_stat_activity".into()], qualified_horizons: vec![] },
        },
        retainer_rows: 0,
        retainer_limited: false,
    };
    assert!(point.fits_source_scope(&source));
    let row = TelemetryRecord::Operational(OperationalObservation {
        source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
        key: point.key().unwrap(),
        fact: OperationalFact::PostgresTransactionMetadata(Box::new(point)),
    });
    let (row, reference) = exported(&source, row, sequence);
    (source, row, reference)
}
pub(super) fn trial() -> Trial {
    trial_with_end(BlockingRequestState::SameStatement)
}
pub(super) fn trial_with_end(end: BlockingRequestState) -> Trial {
    let (source, actor) = parent("2020-01-01T12:00:00Z");
    let (original, reference) = exported(&source, actor.clone().into(), 1);
    let mut update = observe_diagnostic_activity(
        &source,
        &reference.capture_ref(),
        &[DiagnosticActivityRow { reference: &reference, observation: &actor }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:01Z",
    )
    .unwrap();
    let index = update
        .checkpoint
        .episodes
        .iter()
        .position(|episode| matches!(episode.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
        .unwrap();
    let episode = &update.checkpoint.episodes[index];
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let (point_source, mut point, _) =
        point(&focus, &source, &actor, "2020-01-01T12:00:02Z", "2020-01-01T12:00:03Z", "2020-01-01T12:00:04Z", 2);
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PostgresTransactionMetadata(native),
        ..
    }) = &mut point.record
    else {
        panic!()
    };
    native.request_end = end;
    let (point, result) = exported(&point_source, point.record, 2);
    let answer = TransactionMetadataAnswer::from_originals(
        &focus,
        &source,
        &original,
        &point_source,
        &result.capture_ref(),
        &[(&result, &point)],
    )
    .unwrap();
    update.checkpoint.episodes[index] = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer.clone()))),
            },
        )
        .unwrap();
    Trial { source, original, point_source, point, answer, checkpoint: update.checkpoint }
}
pub(super) fn recorded(
    trial: &Trial,
    source: CollectionDescription,
    record: TelemetryRecord,
    at: &str,
    sequence: u64,
) -> (DiagnosticEpisode, CollectionDescription, ExportedRecord, DiagnosticEvidenceRef) {
    let (row, reference) = exported(&source, record, sequence);
    let TelemetryRecord::Activity(actor) = &row.record else { panic!() };
    let update = observe_diagnostic_activity(
        &source,
        &reference.capture_ref(),
        &[DiagnosticActivityRow { reference: &reference, observation: actor }],
        Some(&trial.checkpoint),
        &ActivityPolicy::default(),
        at,
    )
    .unwrap();
    let original_id = trial
        .checkpoint
        .episodes
        .iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
        .unwrap()
        .id
        .clone();
    let episode = update
        .checkpoint
        .episodes
        .iter()
        .find(|episode| episode.id == original_id)
        .cloned()
        .or_else(|| {
            update
                .revisions
                .iter()
                .find(|revision| revision.episode.id == original_id)
                .map(|revision| revision.episode.clone())
        })
        .unwrap();
    (episode, source, row, reference)
}
pub(super) fn fresh(
    trial: &Trial,
    at: &str,
    sequence: u64,
) -> (DiagnosticEpisode, CollectionDescription, ExportedRecord, DiagnosticEvidenceRef) {
    let (source, actor) = parent(at);
    recorded(trial, source, actor.into(), at, sequence)
}
pub(super) fn continuation(
    episode: &DiagnosticEpisode,
    source: &CollectionDescription,
    row: &ExportedRecord,
    reference: &DiagnosticEvidenceRef,
) -> SourceReadFocus {
    SourceReadFocus::from_followup_input(SourceReadQuestion::TransactionMetadata, episode, source, reference, row)
        .unwrap()
        .unwrap()
}
pub(super) fn recover(episode: DiagnosticEpisode, at: &str) -> DiagnosticEpisode {
    let (mut source, _) = parent(at);
    let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
    frame.native_rows = 0;
    frame.observations = 0;
    frame.coverage = StateSourceCoverage::collected(source.source.clone(), 0);
    let previous = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: episode.scope.target.clone(),
        input: episode.latest.capture_ref(),
        episodes: vec![episode.clone()],
    };
    let input = DiagnosticCaptureRef {
        capture: capture(8),
        manifest_sha256: digest(b"complete native empty activity manifest"),
        source_scope_id: source.captured_range().unwrap().scope_id,
    };
    let update =
        observe_diagnostic_activity(&source, &input, &[], Some(&previous), &ActivityPolicy::default(), at).unwrap();
    update.revisions.into_iter().find(|revision| revision.episode.id == episode.id).unwrap().episode
}
