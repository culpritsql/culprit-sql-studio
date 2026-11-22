//! Controlled native-shaped corpus shared by controller and canonical EOF consumer controls. No cost proof.
use super::*;
use crate::original_projection::OriginalProjection;
use culprit_core::{Engine, MeasurementScope};
use std::collections::BTreeMap;

pub fn worker_starvation_basis(shared: bool) -> (CollectionDescription, Vec<TelemetryRecord>) {
    // These are separate controlled native owners before any row/receipt is created. The generic Activity
    // corpus deliberately has unknown role/Target scope and cannot authorize a native source question.
    let mut source = if shared {
        shared_investigation_fixture().owner_source
    } else {
        request_memory_investigation_fixture().baseline_source
    };
    let mut actor = activity_observation(DefinitionPayload::sql(
        "SELECT value FROM dbo.worker_victim WHERE id=4728",
        Engine::Mssql,
    ));
    actor.waiting_on_lock = None;
    actor.wait = Some(ActivityWait {
        class: Some(ActivityWaitClass::WorkerQueue),
        identified_resources: vec![],
        native_type: Some("THREADPOOL".into()),
        elapsed_ms: Some("1000".into()),
        owners: vec![],
        resources: None,
    });
    if !shared {
        let SourceObservation::Activity(header) = &mut source.observation else { panic!() };
        header.observations = 1;
        header.native_rows = 1;
        header.coverage = StateSourceCoverage::collected(source.source.clone(), 1);
        actor.database = source.target.database().map(|database| database.database.clone());
        return (source, vec![TelemetryRecord::from(actor)]);
    }
    source.source = "controlled-shared-scheduler".into();
    let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
    header.records = 1;
    header.sources = vec![StateSourceCoverage::collected(culprit_core::source_id::SCHEDULERS.into(), 1)];
    let mut sample = NativeCounterSet {
        identity: NativeIdentity::new("0").qualified("native_object", MSSQL_SCHEDULER_OBJECT),
        scope: MeasurementScope::Server,
        counters: vec![],
        properties: BTreeMap::new(),
        ratios: vec![],
        ratio_gaps: vec![],
    };
    for field in MSSQL_SCHEDULER_FIELDS {
        match field.kind {
            MssqlNativeFieldKind::Identity => (),
            MssqlNativeFieldKind::TextProperty => {
                sample.properties.insert(field.name.into(), "VISIBLE ONLINE".into());
            }
            MssqlNativeFieldKind::IntegerProperty | MssqlNativeFieldKind::BooleanProperty => {
                sample.properties.insert(field.name.into(), if field.name == "is_online" { "1" } else { "0" }.into());
            }
            MssqlNativeFieldKind::UnqualifiedCounter { .. } => {
                sample.properties.insert(field.name.into(), "0".into());
            }
            MssqlNativeFieldKind::Counter { unit, nature } => sample.counters.push(NativeCounterSample {
                key: field.name.into(),
                native_name: field.name.into(),
                native_object: MSSQL_SCHEDULER_OBJECT.into(),
                value: MeasurementValue::Integer(if field.name == "work_queue_count" { "2" } else { "0" }.into()),
                unit,
                nature,
                scope: MeasurementScope::Server,
                reset: culprit_core::ResetMarker::Unknown,
                native_instance: None,
                native_type: None,
            }),
        }
    }
    let row = OperationalObservation::from_counters(culprit_core::source_id::SCHEDULERS, sample).unwrap();
    (source, vec![TelemetryRecord::Operational(row)])
}
fn actor(id: &str) -> MssqlWorkerRequest {
    let mut row = activity_observation(DefinitionPayload::sql(
        &format!("SELECT value FROM dbo.worker_context WHERE id={id}"),
        Engine::Mssql,
    ));
    row.key.session.id = id.into();
    row.key.session.generation = format!("native-login-{id}");
    row.wait = None;
    row.waiting_on_lock = None;
    row.client.as_mut().unwrap().application = Some(format!("db-client-{id}"));
    MssqlWorkerRequest {
        observation: row,
        before_check: Some(TaskRequestCheck {
            observed_at: "2020-01-01T12:00:10.3Z".into(),
            state: TaskRequestState::SameStatement,
        }),
        after_check: Some(TaskRequestCheck {
            observed_at: "2020-01-01T12:00:10.7Z".into(),
            state: TaskRequestState::SameStatement,
        }),
    }
}
fn task(id: u32, request: &MssqlWorkerRequest, context: u32) -> MssqlWorkerTask {
    let address = format!("{id:016X}");
    MssqlWorkerTask {
        address: address.clone(),
        state: "RUNNING".into(),
        scheduler_id: Some(0),
        parent_node_id: Some(0),
        scheduler_work_queue_count: Some("2".into()),
        session_id: Some(request.observation.key.session.id.parse().unwrap()),
        request_id: Some(0),
        execution_context_id: Some(context),
        request: Some(request.observation.key.clone()),
        wait: None,
        worker: Some(MssqlWorkerBinding {
            address: format!("{:016X}", id + 1000),
            task_address: Some(address),
            created_ms_ticks: Some("100".into()),
            task_bound_ms_ticks: Some("150".into()),
            state: Some("RUNNING".into()),
            is_preemptive: Some(false),
        }),
    }
}
fn wait(task: &MssqlWorkerTask, kind: &str) -> MssqlWorkerWait {
    MssqlWorkerWait {
        task_address: task.address.clone(),
        session_id: task.session_id,
        execution_context_id: task.execution_context_id,
        native_type: kind.into(),
        elapsed_ms: Some("9007199254740993".into()),
        request_id: task.request_id,
        scheduler_id: task.scheduler_id,
        parent_node_id: task.parent_node_id,
        request: task.request.clone(),
        blocking_task_address: None,
        blocking_session_id: None,
        blocking_execution_context_id: None,
    }
}
pub fn worker_starvation_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        observed_from: "2020-01-01T12:00:10Z".into(),
        observed_until: "2020-01-01T12:00:11Z".into(),
        database_writable: execution.database_writable(),
    };
    let worker_scope = match &context.partition {
        SourcePartition::InstanceLocal { member: Some(member), .. } => member.clone(),
        _ => panic!(),
    };
    let mut victim = actor("61");
    // Preserve the actual original generation for the selected victim; no current name becomes an identity.
    victim.observation.key.session.generation = "native-login-generation".into();
    victim.observation.wait = Some(ActivityWait {
        class: Some(ActivityWaitClass::WorkerQueue),
        identified_resources: vec![],
        native_type: Some("THREADPOOL".into()),
        elapsed_ms: Some("1000".into()),
        owners: vec![],
        resources: None,
    });
    let mut blocked = actor("62");
    let mut parallel = actor("63");
    let external = actor("64");
    parallel.observation.statement_identity.as_mut().unwrap().qualifiers.insert("database_id".into(), "9".into());
    parallel.observation.scope = ActivityScopeBasis::OtherDatabase;
    parallel.observation.database = Some("foreign-observed-db".into());
    blocked.observation.waiting_on_lock = Some(true);
    blocked.observation.wait = Some(ActivityWait {
        class: None,
        identified_resources: vec![],
        native_type: Some("LCK_M_X".into()),
        elapsed_ms: Some("5000".into()),
        owners: vec![ActivityWaitOwner::Session { id: "63".into(), generation: Some("native-login-63".into()) }],
        resources: None,
    });
    let mut pending = task(1, &victim, 0);
    pending.state = "PENDING".into();
    pending.worker = None;
    pending.wait = Some(wait(&pending, "THREADPOOL"));
    let mut held = task(2, &blocked, 0);
    held.state = "SUSPENDED".into();
    held.worker.as_mut().unwrap().state = Some("SUSPENDED".into());
    held.wait = Some(wait(&held, "LCK_M_X"));
    let held_wait = held.wait.as_mut().unwrap();
    held_wait.blocking_session_id = Some(63);
    held_wait.blocking_task_address = Some("0000000000000003".into());
    held_wait.blocking_execution_context_id = Some(1);
    let first_parallel = task(3, &parallel, 1);
    let mut second_parallel = task(4, &parallel, 2);
    second_parallel.worker.as_mut().unwrap().is_preemptive = Some(true);
    let mut other = task(5, &external, 1);
    other.scheduler_id = Some(99);
    other.scheduler_work_queue_count = Some("0".into());
    other.worker.as_mut().unwrap().is_preemptive = Some(true);
    let waits = vec![
        wait(&pending, "THREADPOOL"),
        MssqlWorkerWait {
            task_address: "00000000000000FF".into(),
            session_id: None,
            request_id: None,
            execution_context_id: None,
            scheduler_id: None,
            parent_node_id: None,
            request: None,
            native_type: "THREADPOOL".into(),
            elapsed_ms: Some("0".into()),
            blocking_task_address: None,
            blocking_session_id: None,
            blocking_execution_context_id: None,
        },
    ];
    let point = MssqlWorkerStarvationPoint {
        context: context.clone(),
        worker_scope,
        observed_at: "2020-01-01T12:00:10.25Z".into(),
        tasks: vec![pending, held, first_parallel, second_parallel, other],
        threadpool_waits: waits,
        requests: vec![victim, blocked, parallel, external],
        tasks_limited: false,
        waits_limited: false,
    };
    assert!(point.is_valid());
    let row = OperationalObservation {
        source: culprit_core::source_id::WORKER_QUEUE.into(),
        key: point.key().unwrap(),
        fact: OperationalFact::MssqlWorkerStarvation(Box::new(point)),
    };
    let source = CollectionDescription {
        target: context.target.clone(),
        partition: SourcePartition::Target,
        source: row.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::WorkerStarvation,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: 1,
                sources: vec![StateSourceCoverage::collected(row.source.clone(), 1)],
            },
            focus_key: focus.key().unwrap(),
            execution: Box::new(context),
        },
    };
    assert!(source.partition_is_consistent());
    (source, vec![TelemetryRecord::Operational(row)])
}

pub fn worker_starvation_evidence(shared: bool) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (source, rows) = worker_starvation_basis(shared);
    let capture = CaptureIdentity {
        observation_id: "controlled-worker-native-basis".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-worker-owner".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let row = ExportedRecord::new(&source, &capture, rows[0].clone()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-worker-native-basis"), &row, 0).unwrap();
    let focus = SourceReadFocus {
        basis: Box::new(reference),
        target: source.target.clone(),
        question: SourceReadQuestion::WorkerStarvation,
        prerequisite: SourceReadPrerequisite::ObservedWorkerQueue,
    };
    let context = focus.original_context(&source).unwrap();
    let execution = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    worker_starvation_result(&focus, &execution)
}
pub fn assert_worker_starvation_capture(
    capture: &crate::analytical::evidence::EvidenceCapture,
    source: &CollectionDescription,
    records: &[TelemetryRecord],
) {
    assert!(capture.is_read_complete());
    let original = capture.verified_original().unwrap();
    assert_eq!(original.source(), source);
    assert_eq!(original.records().len(), 1);
    assert_eq!(&original.records()[0].row().record, &records[0]);
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlWorkerStarvation(point),
        ..
    }) = &records[0]
    else {
        panic!()
    };
    assert!(point.fits_source(source));
    let facts = WorkerStarvationEvidence::from_point(point).unwrap();
    assert_eq!((facts.assigned_workers, facts.unassigned_threadpool_waits), (4, 1));
    assert_eq!(point.threadpool_waits[0].elapsed_ms.as_deref(), Some("9007199254740993"));
    assert_eq!(point.requests[2].observation.database.as_deref(), Some("foreign-observed-db"));
    assert_eq!(point.requests[2].observation.scope, ActivityScopeBasis::OtherDatabase);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn worker_basis_declares_its_actual_controlled_native_owner_before_canonical_receipts_and_refuses_unknowns() {
        for shared in [false, true] {
            let (source, rows) = worker_starvation_basis(shared);
            let context = SourceReadExecutionScope::context_from_source(&source).unwrap();
            let execution =
                SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &context).unwrap();
            assert_eq!(context.database_writable, if shared { None } else { Some(true) });
            assert!(context.partition.is_qualified());
            assert_eq!(matches!(context.target, CollectionTarget::Shared(_)), shared);
            let capture = CaptureIdentity {
                observation_id: "controlled-worker-original".into(),
                revision: Some(CaptureRevision {
                    outbox_id: "controlled-worker-owner".into(),
                    owner_epoch: "1".into(),
                    sequence: "1".into(),
                }),
            };
            let original = ExportedRecord::new(&source, &capture, rows[0].clone()).unwrap();
            let reference =
                DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-worker-original"), &original, 0)
                    .unwrap();
            let focus = SourceReadFocus {
                basis: Box::new(reference),
                target: source.target.clone(),
                question: SourceReadQuestion::WorkerStarvation,
                prerequisite: SourceReadPrerequisite::ObservedWorkerQueue,
            };
            assert!(focus.matches_basis(&source, &original, &execution));
            let mut unknown = context.clone();
            unknown.partition = SourcePartition::Target;
            assert!(SourceReadExecutionScope::from_context(focus.question, &unknown).is_err());
            if !shared {
                let mut unknown = context;
                unknown.database_writable = None;
                assert!(SourceReadExecutionScope::from_context(focus.question, &unknown).is_err());
            }
        }
    }
}
