//! Controlled native facts for the one canonical EOF/receipt reader; these are not live native qualification.
use super::*;
use culprit_core::{Engine, MeasurementScope, ResetMarker};
use std::collections::BTreeMap;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadableSecondaryCase {
    PostgresRelation,
    PostgresSnapshotWait,
    PostgresNoStartup,
    PostgresZero,
    PostgresUnknownReset,
    PostgresResetChanged,
    PostgresCounterDecreased,
    PostgresUnknownStatsMode,
    PostgresLimited,
    PostgresEnded,
    PostgresReusedReader,
    PostgresPendingOnly,
    MssqlRedo,
    MssqlOtherBlocker,
    MssqlOtherWait,
    MssqlEnded,
    MssqlLimited,
    MssqlNoRedo,
    Unavailable,
}
impl ReadableSecondaryCase {
    pub const ALL: &'static [Self] = &[
        Self::PostgresRelation,
        Self::PostgresSnapshotWait,
        Self::PostgresNoStartup,
        Self::PostgresZero,
        Self::PostgresUnknownReset,
        Self::PostgresResetChanged,
        Self::PostgresCounterDecreased,
        Self::PostgresUnknownStatsMode,
        Self::PostgresLimited,
        Self::PostgresEnded,
        Self::PostgresReusedReader,
        Self::PostgresPendingOnly,
        Self::MssqlRedo,
        Self::MssqlOtherBlocker,
        Self::MssqlOtherWait,
        Self::MssqlEnded,
        Self::MssqlLimited,
        Self::MssqlNoRedo,
        Self::Unavailable,
    ];
    pub fn engine(self) -> Engine {
        if matches!(
            self,
            Self::MssqlRedo
                | Self::MssqlOtherBlocker
                | Self::MssqlOtherWait
                | Self::MssqlEnded
                | Self::MssqlLimited
                | Self::MssqlNoRedo
        ) {
            Engine::Mssql
        } else {
            Engine::Postgres
        }
    }
}
pub struct ReadableSecondaryOriginal {
    pub source: CollectionDescription,
    pub row: ExportedRecord,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub execution: SourceReadExecutionScope,
}
const STARTED: &str = "2020-01-01T00:00:00.000000Z";
pub fn readable_secondary_original(engine: Engine) -> ReadableSecondaryOriginal {
    assert!(
        matches!(engine, Engine::Postgres | Engine::Mssql),
        "this controlled corpus declares exactly two native source profiles"
    );
    let (mut source, mut actor) = if engine == Engine::Postgres {
        let old = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
        let TelemetryRecord::Activity(actor) = old.original.record else { panic!() };
        (old.source, *actor)
    } else {
        let (source, _) = mssql_current_work_evidence(MssqlWorkCase::Observed);
        let SourceObservation::CurrentRequestWork { result, .. } = source.observation else { panic!() };
        let focus = result.focus.unwrap();
        let source = CollectionDescription {
            target: focus.target.clone(),
            partition: focus.partition.clone(),
            source: focus.source.clone(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::Activity(ActivitySnapshotDescription {
                epoch: Some(focus.epoch),
                database_writable: Some(false),
                observed_from: "2020-01-01T12:00:00Z".into(),
                observed_until: "2020-01-01T12:00:00Z".into(),
                at: "2020-01-01T12:00:00Z".into(),
                scope: MeasurementScope::Database,
                population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
                native_rows: 1,
                observations: 1,
                coverage: StateSourceCoverage::collected(focus.source, 1),
            }),
        };
        (source, focus.original)
    };
    let SourceObservation::Activity(native) = &mut source.observation else { panic!() };
    native.database_writable = Some(false);
    if engine == Engine::Postgres {
        let epoch = postgres_activity_epoch("16384", STARTED, 180000);
        native.epoch = Some(epoch.clone());
        let SourcePartition::InstanceLocal { epoch: actual, .. } = &mut source.partition else { panic!() };
        *actual = Some(epoch);
    }
    actor.elapsed_ms = Some("120000".into());
    actor.wait = None;
    actor.waiting_on_lock = Some(false);
    actor.idle = false;
    actor.blocking_status = ActivityBlockingStatus::Unknown;
    let capture = CaptureIdentity {
        observation_id: format!("recovery-original-{engine:?}"),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"recovery-controlled-original"),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let row = ExportedRecord::new(&source, &capture, actor.clone().into()).unwrap();
    let basis = DiagnosticEvidenceRef::from_export(&source, &digest(b"recovery-controlled-manifest"), &row, 0).unwrap();
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&source).unwrap(),
        DiagnosticSignal::Activity {
            key: actor.key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        source.observation.activity().unwrap().at.clone(),
        basis.clone(),
    )
    .unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::ReadableSecondaryRecovery,
        &episode,
        &source,
        &basis,
        &row,
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&source).unwrap()).unwrap();
    ReadableSecondaryOriginal { source, row, episode, focus, execution }
}
fn counter_set(after: bool, case: ReadableSecondaryCase) -> NativeCounterSet {
    let mut set = NativeCounterSet {
        identity: NativeIdentity::new("pg_stat_database_conflicts")
            .qualified("dbid", "16384")
            .qualified("postmaster_started_at", canonical_time(STARTED).unwrap()),
        scope: MeasurementScope::Database,
        counters: vec![],
        ratios: vec![],
        ratio_gaps: vec![],
        properties: BTreeMap::new(),
    };
    for kind in PostgresRecoveryConflictKind::ALL {
        let count = if case == ReadableSecondaryCase::PostgresZero {
            0
        } else if case == ReadableSecondaryCase::PostgresCounterDecreased {
            if after { 1 } else { 2 }
        } else {
            if after { 9007199254740994u64 } else { 9007199254740993u64 }
        };
        let reset = if case == ReadableSecondaryCase::PostgresUnknownReset {
            ResetMarker::Unknown
        } else {
            ResetMarker::At(
                if after && case == ReadableSecondaryCase::PostgresResetChanged {
                    "2020-01-01T11:59:00Z"
                } else {
                    "2020-01-01T10:00:00Z"
                }
                .into(),
            )
        };
        set.counters.push(NativeCounterSample {
            key: kind.native_name().into(),
            native_name: kind.native_name().into(),
            native_object: "pg_stat_database_conflicts".into(),
            native_type: None,
            native_instance: None,
            value: MeasurementValue::Integer(count.to_string()),
            unit: CounterUnit::Count,
            nature: CounterNature::Cumulative,
            scope: MeasurementScope::Database,
            reset,
        });
    }
    set
}
fn postgres_endpoint(
    original: &ActivityRequestFocus,
    after: bool,
    case: ReadableSecondaryCase,
) -> PostgresRecoveryEndpoint {
    let measurements = NativeCounterSet {
        identity: NativeIdentity::new("pg_recovery"),
        scope: MeasurementScope::Server,
        counters: vec![],
        ratios: vec![],
        ratio_gaps: vec![],
        properties: BTreeMap::from([
            ("server_version_num".into(), "180000".into()),
            ("kind".into(), "standby".into()),
            ("read_standby".into(), "true".into()),
            ("read_database_oid".into(), "16384".into()),
        ]),
    };
    let mut process = PostgresRecoveryProcess {
        pid: "73".into(),
        started_at: Some("2020-01-01T00:00:01Z".into()),
        database_oid: None,
        wait_type: Some("Lock".into()),
        wait_event: Some("relation".into()),
        lock_snapshot_rows: 2,
        edges_limited: false,
        relation_edges: vec![PostgresRecoveryRelationEdge {
            resource: culprit_core::blocking::PostgresRelationLockTag {
                database_oid: "16384".into(),
                relation_oid: "42000".into(),
            },
            requested_mode: culprit_core::blocking::PostgresRelationLockMode::AccessExclusive,
            reader: original.original.key.session.clone(),
            reader_single_backend: true,
            granted_modes: vec![culprit_core::blocking::PostgresRelationLockMode::AccessShare],
        }],
    };
    if case == ReadableSecondaryCase::PostgresSnapshotWait {
        process.wait_type = Some("IPC".into());
        process.wait_event = Some("RecoveryConflictSnapshot".into());
        process.relation_edges.clear();
    }
    if case == ReadableSecondaryCase::PostgresReusedReader && after {
        process.relation_edges[0].reader.generation = "2020-01-01T11:59:59Z".into();
    }
    if case == ReadableSecondaryCase::PostgresPendingOnly {
        process.relation_edges[0].granted_modes.clear();
    }
    PostgresRecoveryEndpoint {
        observed_at: if after { "2020-01-01T12:00:04Z" } else { "2020-01-01T12:00:03Z" }.into(),
        database_oid: "16384".into(),
        postmaster_started_at: STARTED.into(),
        stats_fetch_consistency: if case == ReadableSecondaryCase::PostgresUnknownStatsMode { "none" } else { "cache" }
            .into(),
        conflicts: counter_set(after, case),
        replay: Box::new(NativeReplicationSample::new(measurements).unwrap()),
        processes: if case == ReadableSecondaryCase::PostgresNoStartup { vec![] } else { vec![process] },
        processes_limited: case == ReadableSecondaryCase::PostgresLimited,
    }
}
fn mssql_endpoint(original: &ActivityRequestFocus, after: bool, case: ReadableSecondaryCase) -> MssqlRecoveryEndpoint {
    let (_, rows) = replication_evidence(ReplicationCase::MssqlAvailabilityGroup);
    let mut replica = rows
        .into_iter()
        .find_map(|row| match row {
            TelemetryRecord::Replication(ReplicationObservation {
                fact: ReplicationFact::AvailabilityGroup(point),
                ..
            }) => Some(point),
            _ => None,
        })
        .unwrap();
    replica.is_local = Some(true);
    replica.database_id = Some(7);
    replica.database_name = Some(original.target.database().unwrap().database.clone());
    let request = MssqlRedoRequest {
        session_id: 19,
        request_id: 0,
        session_generation_without_offset: "2020-01-01T01:00:00.0030000".into(),
        started_without_offset: Some("2020-01-01T11:00:00.0000000".into()),
        command: "DB STARTUP".into(),
        wait_type: Some(
            if case == ReadableSecondaryCase::MssqlOtherWait { "PAGEIOLATCH_SH" } else { "LCK_M_SCH_M" }.into(),
        ),
        wait_ms: Some("100".into()),
        wait_resource: Some("OBJECT: 7:42000:0".into()),
        blocking_session_id: Some(if case == ReadableSecondaryCase::MssqlOtherBlocker {
            99
        } else {
            original.original.key.session.id.parse().unwrap()
        }),
        reader_session: if case == ReadableSecondaryCase::MssqlOtherBlocker {
            None
        } else {
            Some(original.original.key.session.clone())
        },
    };
    MssqlRecoveryEndpoint {
        observed_at: if after { "2020-01-01T12:00:04Z" } else { "2020-01-01T12:00:03Z" }.into(),
        replica_observed_at: if after { "2020-01-01T12:00:03.9Z" } else { "2020-01-01T12:00:02.9Z" }.into(),
        database_id: 7,
        replica,
        requests: if case == ReadableSecondaryCase::MssqlNoRedo { vec![] } else { vec![request] },
        requests_limited: case == ReadableSecondaryCase::MssqlLimited,
    }
}
pub fn readable_secondary_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: ReadableSecondaryCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let original = focus.readable_secondary_request().unwrap();
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        database_writable: execution.database_writable(),
        observed_from: "2020-01-01T12:00:02Z".into(),
        observed_until: "2020-01-01T12:00:06Z".into(),
    };
    let point = ReadableSecondaryRecoveryPoint {
        context,
        focus: Box::new(original.clone()),
        observed_at: "2020-01-01T12:00:02.5Z".into(),
        checked_until: "2020-01-01T12:00:05Z".into(),
        binding: FocusedRequestBinding::Matched,
        membership: FocusedRequestMembership::Observed,
        request_end: if matches!(case, ReadableSecondaryCase::PostgresEnded | ReadableSecondaryCase::MssqlEnded) {
            BlockingRequestState::Missing
        } else {
            BlockingRequestState::SameStatement
        },
        native: if case.engine() == Engine::Postgres {
            ReadableSecondaryRecoveryNative::Postgres {
                before: Some(Box::new(postgres_endpoint(original, false, case))),
                after: Some(Box::new(postgres_endpoint(original, true, case))),
            }
        } else {
            ReadableSecondaryRecoveryNative::Mssql {
                before: Some(Box::new(mssql_endpoint(original, false, case))),
                after: Some(Box::new(mssql_endpoint(original, true, case))),
            }
        },
    };
    let (mut source, record) = point.into_source_read(focus).unwrap();
    if case == ReadableSecondaryCase::Unavailable {
        let SourceObservation::SourceRead { snapshot, .. } = &mut source.observation else { panic!() };
        snapshot.records = 0;
        snapshot.sources = vec![StateSourceCoverage::unavailable(
            culprit_core::source_id::READABLE_SECONDARY_RECOVERY.into(),
            culprit_core::ErrorCode::PermissionDenied,
            None,
        )];
        return (source, vec![]);
    }
    (source, vec![record])
}
pub fn readable_secondary_evidence(case: ReadableSecondaryCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let original = readable_secondary_original(case.engine());
    readable_secondary_result(&original.focus, &original.execution, case)
}
pub fn readable_secondary_answer(
    case: ReadableSecondaryCase,
) -> (ReadableSecondaryRecoveryAnswer, Option<ReadableSecondaryRecoveryPoint>) {
    let original = readable_secondary_original(case.engine());
    let (source, rows) = readable_secondary_result(&original.focus, &original.execution, case);
    let capture = CaptureIdentity {
        observation_id: "recovery-controlled-result".into(),
        revision: original.row.capture.revision.clone(),
    };
    let result = DiagnosticCaptureRef {
        capture: capture.clone(),
        source_scope_id: source.captured_range().unwrap().scope_id,
        manifest_sha256: digest(b"recovery-result-manifest"),
    };
    let exports = rows.into_iter().map(|row| ExportedRecord::new(&source, &capture, row).unwrap()).collect::<Vec<_>>();
    let refs = exports
        .iter()
        .enumerate()
        .map(|(i, row)| DiagnosticEvidenceRef::from_export(&source, &result.manifest_sha256, row, i as u32).unwrap())
        .collect::<Vec<_>>();
    let answer = ReadableSecondaryRecoveryAnswer::from_originals(
        &original.focus,
        &original.source,
        &original.row,
        &source,
        &result,
        &refs.iter().zip(&exports).collect::<Vec<_>>(),
    )
    .unwrap();
    let point = exports.first().map(|row| {
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::ReadableSecondaryRecovery(point),
            ..
        }) = &row.record
        else {
            panic!()
        };
        point.as_ref().clone()
    });
    (answer, point)
}
