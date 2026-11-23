//! Controlled SQL Server native originals for transport/pure/Gold conformance. No runtime or cost qualification.
use super::*;
use culprit_core::{Engine, LockWait, MeasurementScope, blocking::*};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MssqlMetadataQueueCase {
    Observed,
    RunningHolder,
    MissingSql,
    NoLabel,
    CoexistingOwnerPoint,
    DifferentObject,
    DifferentSubtype,
    DifferentDescription,
    DifferentPartition,
    DifferentSettlement,
    UnknownMode,
    MixedMode,
    Convert,
    Mars,
    UnknownRoster,
    LockPrefix,
    OwnerPrefix,
    ReusedTransaction,
    EndedRequest,
    MissingObjectGeneration,
    MissingNativeEdge,
    DuplicateRequested,
    PermissionDenied,
}
pub fn mssql_metadata_queue_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    use MssqlMetadataQueueCase::*;
    [
        Observed,
        RunningHolder,
        MissingSql,
        NoLabel,
        CoexistingOwnerPoint,
        DifferentObject,
        DifferentSubtype,
        DifferentDescription,
        DifferentPartition,
        DifferentSettlement,
        UnknownMode,
        MixedMode,
        Convert,
        Mars,
        UnknownRoster,
        LockPrefix,
        OwnerPrefix,
        ReusedTransaction,
        EndedRequest,
        MissingObjectGeneration,
        MissingNativeEdge,
        DuplicateRequested,
        PermissionDenied,
    ]
    .into_iter()
    .map(mssql_metadata_queue_evidence)
    .collect()
}
pub fn mssql_metadata_queue_evidence(case: MssqlMetadataQueueCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mut victim = actor(42, "SELECT id FROM dbo.orders WHERE tenant_id=9264");
    let mut gate = actor(43, "ALTER TABLE dbo.orders ADD pending_column int NULL");
    let mut holder = actor(44, "SELECT id FROM unrelated_current_work WHERE token='queue-holder-private'");
    holder.waiting_on_lock = Some(false);
    holder.wait = None;
    holder.open_transactions = Some(1);
    if case != MssqlMetadataQueueCase::RunningHolder {
        holder.key.request = None;
        holder.key.transaction = Some("1044".into());
        holder.idle = true;
        holder.state = "sleeping".into();
        holder.scope = ActivityScopeBasis::TransactionDatabase;
        holder.command = None;
        holder.request_counters = None;
        holder.statement_identity = None;
        holder.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    }
    victim.wait.as_mut().unwrap().owners = vec![owner(&gate.key)];
    gate.wait.as_mut().unwrap().owners = vec![owner(&holder.key)];
    let mut source = parent_source();
    let recovery = Recovery::capture(
        "fixture:mssql-schema-lock-parent",
        &digest(b"mssql-schema-lock-parent-schema"),
        source.clone(),
        vec![victim.clone().into()],
        970_000,
    );
    let original = ExportedRecord::new(&source, &recovery.manifest.capture, victim.clone().into()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &recovery.manifest.capture_manifest_sha256, &original, 0).unwrap();
    let focus = ActivityRequestFocus {
        parent: reference,
        target: source.target.clone(),
        partition: source.partition.clone(),
        database_writable: Some(true),
        source: source.source.clone(),
        epoch: source.partition.native_epoch().unwrap().into(),
        original_observed_at: "2020-01-01T12:00:00Z".into(),
        statement_identity: victim.statement_identity.clone().unwrap(),
        original: victim.clone(),
    };
    assert!(focus.matches_parent(&source, &victim, &focus.parent));
    let mut affected =
        point(&victim, &gate, MssqlObjectLockMode::SchemaStability, Some(MssqlObjectLockMode::SchemaModification));
    let mut pending = point(&gate, &holder, MssqlObjectLockMode::SchemaModification, None);
    use MssqlMetadataQueueCase::*;
    match case {
        DifferentObject => pending.resource.as_mut().unwrap().associated_entity_id = "54321".into(),
        DifferentSubtype => pending.resource.as_mut().unwrap().subtype = Some("native-subtype".into()),
        DifferentDescription => {
            pending.resource.as_mut().unwrap().description = Some("different native description".into())
        }
        DifferentPartition => pending.resource.as_mut().unwrap().lock_partition = 1,
        DifferentSettlement => pending.settled_at = "2020-01-01T12:00:01.9Z".into(),
        UnknownMode => affected.blockers[0].granted_modes.push(MssqlObjectLockMode::from_native("FutureMode")),
        MixedMode => affected.blockers[0].granted_modes.push(MssqlObjectLockMode::SchemaModification),
        Convert => {
            affected.blockers[0].waiting_status = Some("CONVERT".into());
            pending.requested_status = Some("CONVERT".into());
        }
        Mars => {
            let context = &mut affected.blockers[0].context;
            context.current_requests = Some("2".into());
            context.actor = None;
            context.statement = None;
            context.native_wait = None;
            context.wait_ms = None;
        }
        UnknownRoster => {
            affected.blockers[0].context.current_requests = None;
            affected.blockers[0].context.complete_request_population = false;
        }
        LockPrefix => {
            affected.snapshot_rows = 513;
            affected.snapshot_complete = false;
        }
        OwnerPrefix => affected.blockers_complete = false,
        ReusedTransaction => pending.blockers[0].settled = false,
        MissingObjectGeneration => {
            affected.resource.as_mut().unwrap().created_without_offset = None;
            pending.resource.as_mut().unwrap().created_without_offset = None;
        }
        MissingNativeEdge => affected.blockers[0].native_wait_edges.clear(),
        PermissionDenied => {
            affected.availability = MssqlObjectLockAvailability::PermissionDenied;
            affected.waiter_matches = false;
            affected.snapshot_complete = false;
        }
        _ => {}
    }
    set_point(&mut victim, affected, case != NoLabel);
    set_point(&mut gate, pending, case != NoLabel);
    if case == CoexistingOwnerPoint {
        for row in [&mut victim, &mut gate] {
            let owners = row
                .wait
                .as_ref()
                .unwrap()
                .owners
                .iter()
                .map(|owner| NativeBlockingOwnerContext {
                    owner: owner.clone(),
                    actor: None,
                    statement: None,
                    native_wait: None,
                    wait_ms: None,
                    current_requests: None,
                    complete_request_population: false,
                })
                .collect();
            let point = MssqlBlockingSessionPoint {
                observed_at: "2020-01-01T12:00:01.1Z".into(),
                waiter: row.key.clone(),
                owners,
            };
            row.wait.as_mut().unwrap().resources.as_mut().unwrap().insert(
                0,
                LockWait {
                    session: row.key.session.id.clone(),
                    mode: None,
                    resource: None,
                    object: None,
                    index: None,
                    native: Some(Box::new(NativeLockEvidence::MssqlBlockingSession(Box::new(point)))),
                },
            );
        }
    }

    if case == DuplicateRequested {
        let duplicate = victim.wait.as_ref().unwrap().resources.as_ref().unwrap()[0].clone();
        victim.wait.as_mut().unwrap().resources.as_mut().unwrap().push(duplicate);
    }
    if case == MissingSql {
        gate.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    }
    let rows = vec![victim, gate, holder];
    source.source = "mssql-blocking".into();
    source.observation = SourceObservation::FocusedBlocking {
        snapshot: ActivitySnapshotDescription {
            epoch: Some(focus.epoch.clone()),
            database_writable: Some(true),
            observed_from: "2020-01-01T12:00:01Z".into(),
            at: "2020-01-01T12:00:01.1Z".into(),
            observed_until: "2020-01-01T12:00:02Z".into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 3,
            observations: 3,
            coverage: StateSourceCoverage::observed("mssql-blocking".into(), 3, false),
        },
        result: Box::new(ActivityBlockingFocusResult {
            focus,
            binding: FocusedRequestBinding::Matched,
            membership: FocusedRequestMembership::Observed,
            request_end: if case == EndedRequest {
                BlockingRequestState::Missing
            } else {
                BlockingRequestState::SameStatement
            },
            closure: BlockingClosureDescription {
                status: BlockingClosureStatus::Observed,
                max_lookups: MAX_BLOCKING_LOOKUPS,
                max_depth: MAX_BLOCKING_DEPTH,
                max_participants: MAX_BLOCKING_PARTICIPANTS,
            },
        }),
    };
    let borrowed = rows.iter().map(|observation| ActivityFrameRow { observation, reference: None }).collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&source, &borrowed, true).unwrap();
    let SourceObservation::FocusedBlocking { result, .. } = &source.observation else { unreachable!() };
    assert!(result.accepts_frame(&frame), "{case:?}");
    (source, rows.into_iter().map(TelemetryRecord::from).collect())
}
fn parent_source() -> CollectionDescription {
    CollectionDescription {
        target: culprit_core::DatabaseIdentity::server(Engine::Mssql, "db", "fixture.invalid", 1433, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Mssql,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("controlled-native-instance-lineage"),
                native: NativeIdentity::new("controlled-native-instance-member"),
            }),
            epoch: Some("controlled-native-db-recovery-and-process".into()),
        },
        source: culprit_core::source_id::SESSIONS.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some("controlled-native-db-recovery-and-process".into()),
            database_writable: Some(true),
            observed_from: "2020-01-01T12:00:00Z".into(),
            at: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:00Z".into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(culprit_core::source_id::SESSIONS.into(), 1),
        }),
    }
}
fn actor(id: u32, sql: &str) -> ActivityObservation {
    let mut actor = activity_observation(DefinitionPayload::sql(sql, Engine::Mssql));
    actor.key = ActivityKey {
        session: NativeActivityIdentity { id: id.to_string(), generation: format!("2020-01-01T11:00:{id}.0030000") },
        request: Some(NativeActivityIdentity { id: "0".into(), generation: "2020-01-01T11:59:00.0030000".into() }),
        transaction: None,
    };
    actor.statement_identity.as_mut().unwrap().id = format!("{id:02X}").repeat(64);
    actor.waiting_on_lock = Some(true);
    actor.blocking_status = ActivityBlockingStatus::Known;
    actor.client = Some(ActivityClient {
        application: Some(format!("db-reported-lock-actor-{id}")),
        user: Some("native-login".into()),
        host: None,
    });
    actor.wait.as_mut().unwrap().native_type = Some(if id == 43 { "LCK_M_SCH_M" } else { "LCK_M_SCH_S" }.into());
    actor
}
fn owner(key: &ActivityKey) -> ActivityWaitOwner {
    ActivityWaitOwner::Session { id: key.session.id.clone(), generation: Some(key.session.generation.clone()) }
}
fn transaction(id: &str) -> MssqlLockTransaction {
    MssqlLockTransaction {
        id: id.into(),
        begun_without_offset: "2020-01-01T11:58:00.0030000".into(),
        active_local_user_transaction: Some(true),
    }
}
fn point(
    waiter: &ActivityObservation,
    blocker: &ActivityObservation,
    mode: MssqlObjectLockMode,
    waiting: Option<MssqlObjectLockMode>,
) -> MssqlObjectLockPoint {
    let id = format!("10{}", waiter.key.session.id);
    let blocker_id = format!("10{}", blocker.key.session.id);
    let address = format!("{:016X}", waiter.key.session.id.parse::<u32>().unwrap());
    MssqlObjectLockPoint {
        observed_at: "2020-01-01T12:00:01.5Z".into(),
        settled_at: "2020-01-01T12:00:01.8Z".into(),
        waiter: waiter.key.clone(),
        waiter_matches: true,
        waiter_settled: true,
        waiter_current_requests: Some("1".into()),
        waiter_complete_request_population: true,
        waiter_transaction: Some(transaction(&id)),
        resource: Some(MssqlObjectLockResource {
            resource_type: "OBJECT".into(),
            subtype: Some(String::new()),
            database_id: 7,
            description: Some(String::new()),
            associated_entity_id: "12345".into(),
            lock_partition: 0,
            created_without_offset: Some("2019-01-01T00:00:00".into()),
        }),
        requested_mode: Some(mode),
        owner_type: Some("TRANSACTION".into()),
        owner_id: Some(id),
        owner_guid: None,
        requested_status: Some("WAIT".into()),
        lock_owner_address: Some(address.clone()),
        lock_request_id: Some("0".into()),
        lock_execution_context_id: Some("0".into()),
        availability: MssqlObjectLockAvailability::Observed,
        snapshot_rows: 8,
        snapshot_limit: 512,
        snapshot_complete: true,
        blockers_complete: true,
        blockers: vec![MssqlObjectLockBlocker {
            context: NativeBlockingOwnerContext {
                owner: owner(&blocker.key),
                actor: Some(blocker.key.clone()),
                statement: blocker.statement_identity.clone(),
                native_wait: blocker.wait.as_ref().and_then(|wait| wait.native_type.clone()),
                wait_ms: None,
                current_requests: Some(if blocker.key.request.is_some() { "1" } else { "0" }.into()),
                complete_request_population: true,
            },
            transaction: Some(transaction(&blocker_id)),
            owner_type: Some("TRANSACTION".into()),
            owner_id: Some(blocker_id),
            owner_guid: None,
            granted_modes: vec![MssqlObjectLockMode::Other("IX".into())],
            waiting_status: waiting.as_ref().map(|_| "WAIT".into()),
            waiting_mode: waiting,
            native_wait_edges: vec![MssqlObjectLockWaitEdge {
                task_address: format!("000000000000FF{}", waiter.key.session.id),
                owner_address: address,
                waiter_execution_context_id: "0".into(),
                blocking_session_id: blocker.key.session.id.clone(),
                blocker_execution_context_id: Some("0".into()),
            }],
            matched: true,
            settled: true,
        }],
    }
}
fn set_point(row: &mut ActivityObservation, point: MssqlObjectLockPoint, label: bool) {
    row.wait.as_mut().unwrap().resources = Some(vec![LockWait {
        session: row.key.session.id.clone(),
        mode: point.requested_mode.as_ref().map(|mode| mode.native().into()),
        resource: Some("OBJECT".into()),
        object: label.then(|| "dbo.orders".into()),
        index: None,
        native: Some(Box::new(NativeLockEvidence::MssqlObject(Box::new(point)))),
    }]);
}
