//! Manufactured original native points for canonical replay; these are not native runtime or cost qualification.
use super::*;
use culprit_core::{LockWait, blocking::*};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataQueueCase {
    Observed,
    MissingSql,
    IdleHolder,
    DifferentRelation,
    DifferentPoint,
    UnknownMode,
    MixedMode,
    ParallelGroup,
    LockPrefix,
    OwnerPrefix,
    ReusedOwner,
    RequestChanged,
    PermissionLimited,
    UnverifiedOriginal,
    PreparedOwner,
    NoNativeDetails,
}
pub fn metadata_queue_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    use MetadataQueueCase::*;
    [
        Observed,
        MissingSql,
        IdleHolder,
        DifferentRelation,
        DifferentPoint,
        UnknownMode,
        MixedMode,
        ParallelGroup,
        LockPrefix,
        OwnerPrefix,
        ReusedOwner,
        RequestChanged,
        PermissionLimited,
        UnverifiedOriginal,
        PreparedOwner,
        NoNativeDetails,
    ]
    .into_iter()
    .map(metadata_queue_evidence)
    .collect()
}
pub fn metadata_queue_evidence(case: MetadataQueueCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let base = if case == MetadataQueueCase::UnverifiedOriginal {
        BlockingCase::UnverifiedOriginal
    } else {
        BlockingCase::Matched
    };
    let (mut source, records) = blocking_evidence(base);
    let mut rows = records
        .into_iter()
        .map(|row| match row {
            TelemetryRecord::Activity(point) => *point,
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    let mut holder = rows[1].clone();
    holder.key.session.id = "44".into();
    holder.key.session.generation = "2020-01-01T11:00:44Z".into();
    holder.statement = DefinitionPayload::sql(
        "SELECT id FROM unrelated_current_work WHERE tenant_id=9264",
        culprit_core::Engine::Postgres,
    );
    holder.statement_identity = Some(NativeIdentity::new("unrelated-current-holder-statement"));
    holder.client = Some(ActivityClient { application: Some("native-holder-client".into()), user: None, host: None });
    if case == MetadataQueueCase::IdleHolder {
        holder.key.request = None;
        holder.key.transaction = Some("2020-01-01T11:57:00Z".into());
        holder.idle = true;
        holder.state = "idle in transaction".into();
        holder.request_counters = None;
        holder.statement_identity = None;
        holder.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    }
    rows[1].statement =
        DefinitionPayload::sql("LOCK TABLE public.orders IN ACCESS EXCLUSIVE MODE", culprit_core::Engine::Postgres);
    rows[1].statement_identity = Some(NativeIdentity::new("pending-lock-request"));
    rows[1].client = Some(ActivityClient { application: Some("native-gate-client".into()), user: None, host: None });
    rows[1].waiting_on_lock = Some(true);
    rows[1].wait = Some(ActivityWait {
        class: None,
        identified_resources: vec![],
        native_type: Some("Lock: relation".into()),
        elapsed_ms: Some("5000".into()),
        owners: vec![owner(&holder.key)],
        resources: None,
    });
    let gate = rows[1].key.clone();
    let waiter = rows[0].key.clone();
    let holder_key = holder.key.clone();
    rows.push(holder);
    let mut affected = point(
        waiter,
        gate.clone(),
        PostgresRelationLockMode::AccessShare,
        Some(PostgresRelationLockMode::AccessExclusive),
    );
    let mut pending = point(gate, holder_key, PostgresRelationLockMode::AccessExclusive, None);
    match case {
        MetadataQueueCase::DifferentRelation => pending.resource.as_mut().unwrap().relation_oid = "54321".into(),
        MetadataQueueCase::DifferentPoint => pending.observed_at = "2020-01-01T12:00:01.6Z".into(),
        MetadataQueueCase::UnknownMode => {
            affected.blockers[0].granted_modes.push(PostgresRelationLockMode::Unknown("FutureMode".into()))
        }
        MetadataQueueCase::MixedMode => {
            affected.blockers[0].granted_modes.push(PostgresRelationLockMode::AccessExclusive)
        }
        MetadataQueueCase::ParallelGroup => affected.blockers[0].single_backend = false,
        MetadataQueueCase::LockPrefix => {
            affected.snapshot_rows = 513;
            affected.snapshot_complete = false;
        }
        MetadataQueueCase::OwnerPrefix => affected.blockers_complete = false,
        MetadataQueueCase::ReusedOwner => {
            pending.blockers[0].actor.as_mut().unwrap().session.generation = "2020-01-01T11:01:44Z".into()
        }
        MetadataQueueCase::PreparedOwner => {
            pending.blockers[0].native_pid = "0".into();
            pending.blockers[0].actor = None;
            pending.blockers[0].single_backend = false;
        }
        _ => {}
    }
    set_point(&mut rows[0], affected);
    set_point(&mut rows[1], pending);
    if case == MetadataQueueCase::MissingSql {
        rows[1].statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    }
    if case == MetadataQueueCase::NoNativeDetails {
        for row in &mut rows {
            if let Some(wait) = &mut row.wait {
                wait.resources = None;
            }
        }
    }
    let SourceObservation::FocusedBlocking { snapshot, result } = &mut source.observation else { unreachable!() };
    snapshot.native_rows = rows.len() as u32;
    snapshot.observations = rows.len() as u32;
    snapshot.coverage.records = rows.len() as u32;
    if case == MetadataQueueCase::RequestChanged {
        result.request_end = BlockingRequestState::Changed;
    }
    if case == MetadataQueueCase::PermissionLimited {
        snapshot.coverage.error_code = Some(culprit_core::ErrorCode::PermissionDenied);
    }
    let borrowed = rows.iter().map(|observation| ActivityFrameRow { observation, reference: None }).collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&source, &borrowed, true).unwrap();
    let SourceObservation::FocusedBlocking { result, .. } = &source.observation else { unreachable!() };
    assert!(result.accepts_frame(&frame), "{case:?}");
    (source, rows.into_iter().map(TelemetryRecord::from).collect())
}
fn owner(key: &ActivityKey) -> ActivityWaitOwner {
    ActivityWaitOwner::Session { id: key.session.id.clone(), generation: Some(key.session.generation.clone()) }
}
fn point(
    waiter: ActivityKey,
    blocker: ActivityKey,
    mode: PostgresRelationLockMode,
    waiting: Option<PostgresRelationLockMode>,
) -> PostgresLockPoint {
    PostgresLockPoint {
        observed_at: "2020-01-01T12:00:01.5Z".into(),
        waiter,
        waiter_matches: true,
        single_backend: true,
        actor_rows: 3,
        resource: Some(PostgresRelationLockTag { database_oid: "42".into(), relation_oid: "12345".into() }),
        requested_mode: Some(mode),
        snapshot_rows: 6,
        snapshot_limit: 512,
        snapshot_complete: true,
        blockers_complete: true,
        blockers: vec![PostgresLockBlocker {
            native_pid: blocker.session.id.clone(),
            actor: Some(blocker),
            single_backend: true,
            granted_modes: vec![PostgresRelationLockMode::AccessShare],
            waiting_mode: waiting,
        }],
    }
}
fn set_point(row: &mut ActivityObservation, point: PostgresLockPoint) {
    row.wait.as_mut().unwrap().resources = Some(vec![LockWait {
        session: row.key.session.id.clone(),
        mode: point.requested_mode.as_ref().map(|mode| mode.native().into()),
        resource: Some("relation".into()),
        object: Some("public.orders".into()),
        index: None,
        native: Some(Box::new(NativeLockEvidence::Postgres(Box::new(point)))),
    }]);
}
