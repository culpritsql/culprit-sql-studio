//! Canonical controlled originals shared across consumers; no native or cost qualification.
use super::*;
use culprit_core::{LockWait, blocking::*};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputBlockingCaseFixture {
    Mssql,
    PgRelation,
    PgSessionOnly,
    OtherWait,
    UnrelatedOutput,
    Mars,
    UnknownRoster,
    MissingStatement,
    ChangedOwner,
    Ended,
    Partial,
    Parallel,
    LockPrefix,
    MissingDetails,
    MissingSql,
}
pub struct OutputBlockingFixture {
    pub original_source: CollectionDescription,
    pub original: ExportedRecord,
    pub episode: DiagnosticEpisode,
    pub source: CollectionDescription,
    pub records: Vec<TelemetryRecord>,
}
pub fn output_blocking_fixture(case: OutputBlockingCaseFixture) -> OutputBlockingFixture {
    let pg = matches!(
        case,
        OutputBlockingCaseFixture::PgRelation
            | OutputBlockingCaseFixture::PgSessionOnly
            | OutputBlockingCaseFixture::Parallel
            | OutputBlockingCaseFixture::LockPrefix
    );
    let (original_source, mut victim, mut owner) = if pg {
        let (source, records) = blocking_evidence(BlockingCase::Matched);
        let SourceObservation::FocusedBlocking { result, .. } = source.observation else { unreachable!() };
        let TelemetryRecord::Activity(owner) = &records[1] else { unreachable!() };
        let original_source = CollectionDescription {
            target: result.focus.target.clone(),
            partition: result.focus.partition.clone(),
            source: result.focus.source.clone(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::Activity(ActivitySnapshotDescription {
                epoch: Some(result.focus.epoch.clone()),
                database_writable: result.focus.database_writable,
                at: result.focus.original_observed_at.clone(),
                observed_from: result.focus.original_observed_at.clone(),
                observed_until: result.focus.original_observed_at.clone(),
                population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
                scope: culprit_core::MeasurementScope::Database,
                native_rows: 1,
                observations: 1,
                coverage: StateSourceCoverage::collected(result.focus.source.clone(), 1),
            }),
        };
        (original_source, result.focus.original.clone(), owner.as_ref().clone())
    } else {
        let fixture = request_memory_investigation_fixture();
        let TelemetryRecord::Activity(victim) = fixture.baseline_record.record else { unreachable!() };
        let mut owner = victim.as_ref().clone();
        owner.key.session.id = "77".into();
        owner.key.session.generation = "native-output-holder-login".into();
        owner.key.request.as_mut().unwrap().id = "1".into();
        owner.statement_identity = Some(NativeIdentity::new("observed-output-holder-statement"));
        (fixture.baseline_source, *victim, owner)
    };
    let output_type = if pg { "Client: ClientWrite" } else { "ASYNC_NETWORK_IO" };
    owner.blocking_status = ActivityBlockingStatus::Known;
    owner.waiting_on_lock = Some(false);
    owner.idle = false;
    owner.state = "active".into();
    owner.open_transactions = Some(1);
    owner.wait = Some(ActivityWait {
        class: None,
        identified_resources: vec![],
        native_type: Some(output_type.into()),
        elapsed_ms: if pg { None } else { Some("37".into()) },
        owners: vec![],
        resources: None,
    });
    owner.client = Some(ActivityClient {
        application: Some("db-reported-output-owner".into()),
        user: Some("native-owner-login".into()),
        host: None,
    });
    owner.statement = if case == OutputBlockingCaseFixture::MissingSql {
        DefinitionPayload::Unavailable { reason: PayloadUnavailable::Restricted }
    } else {
        DefinitionPayload::sql(
            "SELECT value FROM current_context WHERE token='output-lock-private-canary'",
            original_source.target.engine(),
        )
    };
    let identity = ActivityWaitOwner::Session {
        id: owner.key.session.id.clone(),
        generation: Some(owner.key.session.generation.clone()),
    };
    victim.blocking_status = ActivityBlockingStatus::Known;
    victim.waiting_on_lock = Some(true);
    victim.wait = Some(ActivityWait {
        class: None,
        identified_resources: vec![],
        native_type: Some(if pg { "Lock: relation" } else { "LCK_M_X" }.into()),
        elapsed_ms: Some("40000".into()),
        owners: vec![identity.clone()],
        resources: None,
    });
    // Establish the actual original native victim, before any later answer/header is constructed.
    let recovery = Recovery::capture(
        "fixture:output-blocking-parent",
        &digest(b"output-blocking-native-schema"),
        original_source.clone(),
        vec![victim.clone().into()],
        940_000,
    );
    let original = ExportedRecord::new(&original_source, &recovery.manifest.capture, victim.clone().into()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&original_source, &recovery.manifest.capture_manifest_sha256, &original, 0)
            .unwrap();
    let observed = observe_diagnostic_activity(
        &original_source,
        &reference.capture_ref(),
        &[DiagnosticActivityRow { reference: &reference, observation: &victim }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:00.5Z",
    )
    .unwrap();
    let episode = observed
        .checkpoint
        .episodes
        .iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::LockWait { .. }))
        .unwrap()
        .clone();
    let DiagnosticFocusPlan::Ready(focus) = episode.plan_focus(&original_source, &victim, &reference).unwrap() else {
        unreachable!()
    };
    let DiagnosticFocus::Blocking { focus } = *focus else { unreachable!() };
    let mut context = NativeBlockingOwnerContext {
        owner: identity.clone(),
        actor: Some(owner.key.clone()),
        statement: owner.statement_identity.clone(),
        native_wait: Some(output_type.into()),
        wait_ms: owner.wait.as_ref().and_then(|wait| wait.elapsed_ms.clone()),
        current_requests: Some("1".into()),
        complete_request_population: true,
    };
    if matches!(case, OutputBlockingCaseFixture::OtherWait | OutputBlockingCaseFixture::UnrelatedOutput) {
        context.native_wait = Some("SOS_SCHEDULER_YIELD".into());
        owner.wait.as_mut().unwrap().native_type = context.native_wait.clone();
    }
    if case == OutputBlockingCaseFixture::Mars {
        context.current_requests = Some("2".into());
        context.actor = None;
        context.statement = None;
        context.native_wait = None;
        context.wait_ms = None;
    }
    if case == OutputBlockingCaseFixture::UnknownRoster {
        context.current_requests = None;
        context.complete_request_population = false;
    }
    if case == OutputBlockingCaseFixture::MissingStatement {
        context.statement = None;
    }
    if case == OutputBlockingCaseFixture::ChangedOwner {
        owner.key.session.generation = "another-output-holder-login".into();
    }
    let native = if pg {
        let single = case != OutputBlockingCaseFixture::Parallel;
        context.complete_request_population = single;
        let mut point = PostgresLockPoint {
            observed_at: "2020-01-01T12:00:01.5Z".into(),
            waiter: victim.key.clone(),
            waiter_matches: true,
            single_backend: true,
            actor_rows: 2,
            resource: Some(PostgresRelationLockTag { database_oid: "42".into(), relation_oid: "12345".into() }),
            requested_mode: Some(PostgresRelationLockMode::AccessShare),
            snapshot_rows: 2,
            snapshot_limit: MAX_FOCUSED_LOCK_SNAPSHOT_ROWS,
            snapshot_complete: true,
            blockers_complete: true,
            blockers: vec![PostgresLockBlocker {
                native_pid: owner.key.session.id.clone(),
                actor: Some(owner.key.clone()),
                single_backend: single,
                granted_modes: vec![PostgresRelationLockMode::AccessExclusive],
                waiting_mode: None,
                output_context: Some(context),
            }],
        };
        if case == OutputBlockingCaseFixture::PgSessionOnly {
            point.resource = None;
            point.requested_mode = None;
            point.blockers[0].granted_modes.clear();
        }
        if case == OutputBlockingCaseFixture::LockPrefix {
            point.snapshot_rows = MAX_FOCUSED_LOCK_SNAPSHOT_ROWS + 1;
            point.snapshot_complete = false;
        }
        NativeLockEvidence::Postgres(Box::new(point))
    } else {
        NativeLockEvidence::MssqlBlockingSession(Box::new(MssqlBlockingSessionPoint {
            observed_at: "2020-01-01T12:00:01.1Z".into(),
            waiter: victim.key.clone(),
            owners: vec![context],
        }))
    };
    victim.wait.as_mut().unwrap().resources = if case == OutputBlockingCaseFixture::MissingDetails {
        None
    } else {
        Some(vec![LockWait {
            session: victim.key.session.id.clone(),
            mode: pg.then(|| "AccessShareLock".into()).filter(|_| case != OutputBlockingCaseFixture::PgSessionOnly),
            resource: pg.then(|| "relation".into()).filter(|_| case != OutputBlockingCaseFixture::PgSessionOnly),
            object: pg.then(|| "public.orders".into()),
            index: None,
            native: Some(Box::new(native)),
        }])
    };
    let mut rows = vec![victim, owner];
    if case == OutputBlockingCaseFixture::Mars {
        let mut sibling = rows[1].clone();
        sibling.key.request.as_mut().unwrap().id = "2".into();
        sibling.statement_identity = Some(NativeIdentity::new("different-mars-statement"));
        sibling.wait = None;
        rows.push(sibling);
    }
    if case == OutputBlockingCaseFixture::UnrelatedOutput {
        let mut unrelated = rows[1].clone();
        unrelated.key.session.id = "78".into();
        unrelated.key.session.generation = "unrelated-output-login".into();
        unrelated.wait.as_mut().unwrap().native_type = Some("ASYNC_NETWORK_IO".into());
        rows.push(unrelated);
    }
    let source = CollectionDescription {
        target: original_source.target.clone(),
        partition: original_source.partition.clone(),
        source: if pg { "postgres-blocking" } else { "mssql-blocking" }.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::FocusedBlocking {
            snapshot: ActivitySnapshotDescription {
                epoch: Some(focus.epoch.clone()),
                database_writable: focus.database_writable,
                at: "2020-01-01T12:00:01.1Z".into(),
                observed_from: "2020-01-01T12:00:01Z".into(),
                observed_until: "2020-01-01T12:00:02Z".into(),
                population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
                scope: culprit_core::MeasurementScope::Database,
                native_rows: rows.len() as u32,
                observations: rows.len() as u32,
                coverage: StateSourceCoverage::observed(
                    if pg { "postgres-blocking" } else { "mssql-blocking" }.into(),
                    rows.len() as u32,
                    false,
                ),
            },
            result: Box::new(ActivityBlockingFocusResult {
                focus: *focus,
                binding: FocusedRequestBinding::Matched,
                membership: FocusedRequestMembership::Observed,
                request_end: if case == OutputBlockingCaseFixture::Ended {
                    BlockingRequestState::Missing
                } else {
                    BlockingRequestState::SameStatement
                },
                closure: BlockingClosureDescription {
                    status: if case == OutputBlockingCaseFixture::Partial {
                        BlockingClosureStatus::LookupLimit
                    } else {
                        BlockingClosureStatus::Observed
                    },
                    max_lookups: MAX_BLOCKING_LOOKUPS,
                    max_depth: MAX_BLOCKING_DEPTH,
                    max_participants: MAX_BLOCKING_PARTICIPANTS,
                },
            }),
        },
    };
    let records = rows.into_iter().map(TelemetryRecord::from).collect::<Vec<_>>();
    assert!(source.partition_is_consistent());
    assert!(records.iter().all(TelemetryRecord::is_valid));
    // Keep the original scope/value: no database, role or member is copied from a later row.
    OutputBlockingFixture { original_source, original, episode, source, records }
}
