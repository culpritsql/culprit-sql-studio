//! One source-free corpus for native cache/SLRU/xmin evidence. These controlled rows are not native qualification.
use super::*;
use culprit_core::{Engine, MeasurementScope, SourceState};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMetadataCase {
    Overflow,
    ReducedCacheOverflow,
    Clear,
    UnknownCache,
    Partial,
    ChangedRequest,
    Standby,
    Empty,
    Unavailable,
    MultiXact,
}
impl TransactionMetadataCase {
    pub const ALL: &'static [Self] = &[
        Self::Overflow,
        Self::ReducedCacheOverflow,
        Self::Clear,
        Self::UnknownCache,
        Self::Partial,
        Self::ChangedRequest,
        Self::Standby,
        Self::Empty,
        Self::Unavailable,
        Self::MultiXact,
    ];
}
pub struct TransactionMetadataOriginal {
    pub source: CollectionDescription,
    pub original: ExportedRecord,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub execution: SourceReadExecutionScope,
}
fn actor(pid: &str, mechanism: Option<TransactionMetadataWait>) -> ActivityObservation {
    let backend = format!("2020-01-01T11:00:{pid}Z");
    let query = "2020-01-01T11:59:00Z";
    let mut actor = activity_observation(DefinitionPayload::sql(
        "SELECT payload FROM orders WHERE tenant=9541 AND token='transaction-private-canary'",
        Engine::Postgres,
    ));
    actor.key = ActivityKey {
        session: NativeActivityIdentity { id: pid.into(), generation: backend.clone() },
        request: Some(NativeActivityIdentity { id: "statement".into(), generation: query.into() }),
        transaction: None,
    };
    actor.statement_identity = Some(culprit_core::engines::postgres_activity_statement_identity(pid, &backend, query));
    actor.state = "active".into();
    actor.scope = ActivityScopeBasis::SessionDatabase;
    actor.command = None;
    actor.wait = mechanism.map(|mechanism| ActivityWait {
        class: Some(mechanism.class()),
        native_type: Some(
            match mechanism {
                TransactionMetadataWait::Subtransaction => "LWLock: SubtransSLRU",
                TransactionMetadataWait::MultiXact => "LWLock: MultiXactMemberSLRU",
            }
            .into(),
        ),
        identified_resources: vec![],
        resources: None,
        elapsed_ms: None,
        owners: vec![],
    });
    actor.blocking_status = ActivityBlockingStatus::Unknown;
    actor.waiting_on_lock = None;
    actor.elapsed_ms = Some("1000".into());
    actor.session_counters = ActivityCounters::default();
    actor.request_counters = Some(ActivityCounters::default());
    actor.session_context = Some(Box::new(ActivitySessionContext {
        lifecycle: PostgresSessionContext::lifecycle(false),
        login_age_ms: None,
        last_request_end_age_ms: None,
        client_library: None,
        client_process_id: None,
        native: Some(Box::new(SessionNativeContext::Postgres(PostgresSessionContext {
            database_oid: Some("16384".into()),
            database_name: Some("db".into()),
            backend_type: Some("client backend".into()),
        }))),
    }));
    assert!(actor.is_valid());
    actor
}
pub fn transaction_metadata_original(mechanism: TransactionMetadataWait) -> TransactionMetadataOriginal {
    let actor = actor("42", Some(mechanism));
    let source = CollectionDescription {
        target: culprit_core::DatabaseIdentity::server(Engine::Postgres, "db", "fixture.invalid", 5432, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("transaction-metadata-cluster"),
                native: NativeIdentity::new("transaction-metadata-member"),
            }),
            epoch: Some("transaction-metadata-process".into()),
        },
        source: "postgres-activity".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some("transaction-metadata-process".into()),
            database_writable: Some(true),
            observed_from: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:00Z".into(),
            at: "2020-01-01T12:00:00Z".into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ClientBackends,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected("postgres-activity".into(), 1),
        }),
    };
    let capture = CaptureIdentity {
        observation_id: "transaction-metadata-original".into(),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"transaction-metadata-outbox"),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let original = ExportedRecord::new(&source, &capture, actor.clone().into()).unwrap();
    let basis =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"transaction-metadata-original-manifest"), &original, 0)
            .unwrap();
    let update = observe_diagnostic_activity(
        &source,
        &basis.capture_ref(),
        &[DiagnosticActivityRow { reference: &basis, observation: &actor }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:01Z",
    )
    .unwrap();
    let [episode] = update.checkpoint.episodes.as_slice() else {
        panic!("only the direct native metadata wait is observed");
    };
    let episode = episode.clone();
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&source).unwrap()).unwrap();
    TransactionMetadataOriginal { source, original, episode, focus, execution }
}
fn slrus() -> Vec<PostgresSlruCounters> {
    [
        (PostgresSlruKind::Subtransaction, "subtransaction"),
        (PostgresSlruKind::MultiXactMember, "multixact_member"),
        (PostgresSlruKind::MultiXactOffset, "multixact_offset"),
    ]
    .into_iter()
    .map(|(kind, name)| PostgresSlruCounters {
        kind,
        native_name: name.into(),
        observed_at: "2020-01-01T12:00:03Z".into(),
        stats_reset: Some("2020-01-01T10:00:00Z".into()),
        blocks_zeroed: Some("0".into()),
        blocks_hit: Some("9223372036854775807".into()),
        blocks_read: Some("42".into()),
        blocks_written: Some("2".into()),
        blocks_exists: Some("0".into()),
        flushes: Some("1".into()),
        truncates: Some("0".into()),
    })
    .collect()
}
pub fn transaction_metadata_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: TransactionMetadataCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    use TransactionMetadataCase::*;
    let SourceReadPrerequisite::ObservedTransactionMetadataWait { key, mechanism, .. } = &focus.prerequisite else {
        unreachable!()
    };
    let mut caches = vec![
        PostgresSubtransactionCache {
            backend_id: 7,
            actor: actor("42", Some(*mechanism)),
            transaction_started_at: Some("2020-01-01T11:30:00Z".into()),
            cached_subxids: Some(0),
            overflowed: Some(false),
        },
        PostgresSubtransactionCache {
            backend_id: 12,
            actor: actor("43", None),
            transaction_started_at: Some("2020-01-01T11:30:00Z".into()),
            cached_subxids: Some(if case == ReducedCacheOverflow { 3 } else { 64 }),
            overflowed: Some(true),
        },
    ];
    if case == Clear {
        caches.truncate(1);
        caches[0].cached_subxids = Some(64);
    }
    if case == Standby {
        caches.truncate(1);
    }
    if case == UnknownCache {
        caches.truncate(1);
        caches[0].overflowed = None;
        caches[0].cached_subxids = None;
    }
    let coverage = RetentionCoverage {
        native_sources: vec!["pg_stat_activity".into(), "pg_replication_slots".into(), "pg_prepared_xacts".into()],
        qualified_horizons: if case == Standby { vec![] } else { vec![CleanupHorizon::Data, CleanupHorizon::Catalog] },
    };
    let (_, retained) = retention_evidence();
    let mut holders = retained
        .into_iter()
        .filter_map(|record| match record {
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::Retention(holder), ..
            }) => Some(*holder),
            _ => None,
        })
        .collect::<Vec<_>>();
    for holder in &mut holders {
        holder.observed_at = "2020-01-01T12:00:03Z".into();
    }
    let retention = RetentionSnapshot {
        holders,
        sources: coverage
            .native_sources
            .iter()
            .map(|name| RetentionSourceCoverage {
                source: name.clone(),
                state: if case == Partial {
                    SourceState::Partial
                } else if name == "pg_replication_slots" {
                    SourceState::Collected
                } else {
                    SourceState::Empty
                },
            })
            .collect(),
        coverage,
    };
    let mut point = PostgresTransactionMetadata {
        observed_at: "2020-01-01T12:00:03Z".into(),
        database_oid: "16384".into(),
        database: "db".into(),
        focus: key.clone(),
        request_end: if case == ChangedRequest {
            BlockingRequestState::Changed
        } else {
            BlockingRequestState::SameStatement
        },
        cache_rows: if case == Partial { 3 } else { caches.len() as u32 },
        cache_limited: case == Partial,
        caches,
        slrus: slrus(),
        retainer_rows: retention.holders.len() as u32,
        retention,
        retainer_limited: false,
    };
    if case == Partial {
        point.retention.sources.iter_mut().for_each(|source| source.state = SourceState::Partial);
    }
    let mut records = vec![TelemetryRecord::Operational(OperationalObservation {
        source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
        key: point.key().unwrap(),
        fact: OperationalFact::PostgresTransactionMetadata(Box::new(point)),
    })];
    if matches!(case, Empty | Unavailable) {
        records.clear();
    }
    let mut coverage = if case == Unavailable {
        StateSourceCoverage::unavailable(
            POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
            culprit_core::ErrorCode::PermissionDenied,
            None,
        )
    } else {
        StateSourceCoverage::observed(
            POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
            records.len() as u32,
            !matches!(case, Partial | UnknownCache | ChangedRequest),
        )
    };
    if case == Partial {
        coverage.stopped = Some(CollectionStop::RowLimit);
    }
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        database_writable: execution.database_writable(),
        observed_from: "2020-01-01T12:00:02Z".into(),
        observed_until: "2020-01-01T12:00:04Z".into(),
    };
    let source = CollectionDescription {
        target: focus.target.clone(),
        partition: SourcePartition::Target,
        source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::TransactionMetadata,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: records.len() as u32,
                sources: vec![coverage],
            },
            focus_key: focus.key().unwrap(),
            execution: Box::new(context),
        },
    };
    assert!(source.partition_is_consistent());
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}
pub fn transaction_metadata_evidence(case: TransactionMetadataCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mechanism = if case == TransactionMetadataCase::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    };
    let mut original = transaction_metadata_original(mechanism);
    if case == TransactionMetadataCase::Standby {
        let SourceReadExecutionScope::Local(scope) = &mut original.execution else { unreachable!() };
        scope.database_writable = false;
    }
    transaction_metadata_result(&original.focus, &original.execution, case)
}
pub fn transaction_metadata_answer(
    case: TransactionMetadataCase,
) -> (TransactionMetadataAnswer, Option<PostgresTransactionMetadata>) {
    let mut original = transaction_metadata_original(if case == TransactionMetadataCase::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    });
    if case == TransactionMetadataCase::Standby {
        let SourceReadExecutionScope::Local(scope) = &mut original.execution else { unreachable!() };
        scope.database_writable = false;
    }
    let (source, rows) = transaction_metadata_result(&original.focus, &original.execution, case);
    let capture = CaptureIdentity {
        observation_id: "transaction-metadata-result".into(),
        revision: original.original.capture.revision.clone(),
    };
    let result = DiagnosticCaptureRef {
        capture: capture.clone(),
        manifest_sha256: digest(b"transaction-metadata-result-manifest"),
        source_scope_id: source.captured_range().unwrap().scope_id,
    };
    let exports =
        rows.into_iter().map(|record| ExportedRecord::new(&source, &capture, record).unwrap()).collect::<Vec<_>>();
    let references = exports
        .iter()
        .enumerate()
        .map(|(ordinal, row)| {
            DiagnosticEvidenceRef::from_export(&source, &result.manifest_sha256, row, ordinal as u32).unwrap()
        })
        .collect::<Vec<_>>();
    let pairs = references.iter().zip(&exports).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        &original.focus,
        &original.source,
        &original.original,
        &source,
        &result,
        &pairs,
    )
    .unwrap();
    let point = exports.first().map(|row| {
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::PostgresTransactionMetadata(point),
            ..
        }) = &row.record
        else {
            unreachable!()
        };
        point.as_ref().clone()
    });
    (answer, point)
}
