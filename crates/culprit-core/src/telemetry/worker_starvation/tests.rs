use super::*;
fn point() -> MssqlWorkerStarvationPoint {
    let context = super::super::native_read_context::tests::read_context();
    let worker_scope = match &context.partition {
        SourcePartition::InstanceLocal { member: Some(member), .. } => member.clone(),
        _ => panic!(),
    };
    let mut request = super::super::activity::tests::request("42");
    request.statement_identity = Some(NativeIdentity::new("native-statement"));
    let task = MssqlWorkerTask {
        address: "0000000000000001".into(),
        state: "RUNNING".into(),
        scheduler_id: Some(0),
        parent_node_id: Some(0),
        scheduler_work_queue_count: Some("1".into()),
        session_id: Some(42),
        request_id: Some(0),
        execution_context_id: Some(1),
        request: Some(request.key.clone()),
        wait: None,
        worker: Some(MssqlWorkerBinding {
            address: "0000000000000002".into(),
            task_address: Some("0000000000000001".into()),
            created_ms_ticks: Some("1".into()),
            task_bound_ms_ticks: Some("2".into()),
            state: Some("RUNNING".into()),
            is_preemptive: Some(false),
        }),
    };
    MssqlWorkerStarvationPoint {
        context,
        worker_scope,
        observed_at: "2020-01-01T12:00:00.5Z".into(),
        tasks: vec![task],
        threadpool_waits: vec![],
        requests: vec![MssqlWorkerRequest {
            observation: request,
            before_check: Some(TaskRequestCheck {
                observed_at: "2020-01-01T12:00:00.6Z".into(),
                state: TaskRequestState::SameStatement,
            }),
            after_check: Some(TaskRequestCheck {
                observed_at: "2020-01-01T12:00:00.8Z".into(),
                state: TaskRequestState::SameStatement,
            }),
        }],
        tasks_limited: false,
        waits_limited: false,
    }
}
#[test]
fn native_task_rows_do_not_become_occupied_workers_without_reciprocal_generation_and_state_facts() {
    let original = point();
    assert!(original.is_valid());
    let evidence = WorkerStarvationEvidence::from_point(&original).unwrap();
    assert_eq!((evidence.observed_tasks, evidence.assigned_workers), (1, 1));
    assert_eq!(evidence.contributions[0].kind, WorkerContributionKind::AssignedParallelWorkers);
    for defect in ["no-worker", "one-way", "creation", "bound", "unknown-state", "pending"] {
        let mut point = original.clone();
        match defect {
            "no-worker" => point.tasks[0].worker = None,
            "one-way" => point.tasks[0].worker.as_mut().unwrap().task_address = Some("0000000000000009".into()),
            "creation" => point.tasks[0].worker.as_mut().unwrap().created_ms_ticks = None,
            "bound" => point.tasks[0].worker.as_mut().unwrap().task_bound_ms_ticks = None,
            "unknown-state" => point.tasks[0].worker.as_mut().unwrap().state = Some("FUTURE_NATIVE_STATE".into()),
            _ => point.tasks[0].state = "PENDING".into(),
        }
        assert!(point.is_valid());
        let evidence = WorkerStarvationEvidence::from_point(&point).unwrap();
        assert_eq!((evidence.observed_tasks, evidence.assigned_workers), (1, 0), "{defect}");
        assert!(evidence.contributions.is_empty(), "{defect}");
    }
}
#[test]
fn sessionless_threadpool_has_no_request_database_or_guessed_scheduler() {
    let mut point = point();
    point.tasks.clear();
    point.requests.clear();
    point.threadpool_waits.push(MssqlWorkerWait {
        task_address: "0000000000000003".into(),
        session_id: None,
        request_id: None,
        request: None,
        scheduler_id: None,
        parent_node_id: None,
        execution_context_id: None,
        native_type: "THREADPOOL".into(),
        elapsed_ms: Some("0".into()),
        blocking_task_address: None,
        blocking_session_id: None,
        blocking_execution_context_id: None,
    });
    assert!(point.is_valid());
    let evidence = WorkerStarvationEvidence::from_point(&point).unwrap();
    assert_eq!(evidence.unassigned_threadpool_waits, 1);
    assert!(evidence.affected_requests.is_empty());
    assert!(evidence.impacted_schedulers.is_empty());
    point.threadpool_waits[0].request = Some(super::super::activity::tests::request("42").key);
    assert!(!point.is_valid(), "the route cannot supply a missing native actor");
}
#[test]
fn changed_or_missing_native_requests_withhold_query_contributions_without_erasing_raw_worker_assignment() {
    for state in [TaskRequestState::Changed, TaskRequestState::Missing, TaskRequestState::Unverified] {
        let mut point = point();
        point.requests[0].after_check.as_mut().unwrap().state = state;
        assert!(point.is_valid());
        let evidence = WorkerStarvationEvidence::from_point(&point).unwrap();
        assert_eq!(evidence.assigned_workers, 1);
        assert_eq!(evidence.unqualified_requests, 1);
        assert!(evidence.contributions.is_empty());
        assert!(evidence.blocking.is_empty());
    }
}
#[test]
fn native_member_epoch_role_clocks_and_contradictory_task_worker_rows_refuse_without_name_repair() {
    for defect in ["member", "epoch", "clock", "duplicate-task", "duplicate-worker", "request", "scope"] {
        let mut point = point();
        match defect {
            "member" => point.worker_scope.native.id = "different-member".into(),
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut point.context.partition else { panic!() };
                *epoch = None;
            }
            "clock" => point.requests[0].after_check.as_mut().unwrap().observed_at = "2020-01-01T12:00:02Z".into(),
            "duplicate-task" => point.tasks.push(point.tasks[0].clone()),
            "duplicate-worker" => {
                let mut row = point.tasks[0].clone();
                row.address = "000000000000000A".into();
                row.worker.as_mut().unwrap().task_address = Some(row.address.clone());
                point.tasks.push(row);
            }
            "request" => point.tasks[0].request.as_mut().unwrap().session.generation = "borrowed-login".into(),
            _ => point.context.database_writable = None,
        }
        assert!(!point.is_valid(), "{defect}");
    }
}
#[test]
fn local_readonly_and_shared_resource_scopes_are_distinct_and_shared_role_is_not_fabricated() {
    let mut context = point().context;
    context.database_writable = Some(false);
    let local = SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &context).unwrap();
    assert_eq!(local.database_writable(), Some(false));
    let SourcePartition::InstanceLocal { member: Some(member), .. } = &context.partition else { panic!() };
    context.target = CollectionTarget::Shared(member.clone());
    context.database_writable = None;
    let shared = SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &context).unwrap();
    assert_eq!(shared.database_writable(), None);
    assert!(matches!(shared.subject(), CollectionSubject::Shared { .. }));
    assert!(!shared.accepts_question(SourceReadQuestion::TemporarySpaceContributors));
    assert!(SourceReadExecutionScope::from_context(SourceReadQuestion::CompilationHistory, &context).is_err());
    context.database_writable = Some(false);
    assert!(SharedResourceReadScope::from_context(&context).is_err());
}
#[test]
fn native_signed_ticks_unknown_states_and_complete_original_point_roundtrip_without_counter_math() {
    let mut point = point();
    point.tasks[0].worker.as_mut().unwrap().created_ms_ticks = Some("-1".into());
    let bytes = serde_json::to_vec(&point).unwrap();
    let copy: MssqlWorkerStarvationPoint = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(copy, point);
    assert!(copy.is_valid());
    let mut point = copy;
    point.tasks[0].scheduler_work_queue_count = Some("0".into());
    let evidence = WorkerStarvationEvidence::from_point(&point).unwrap();
    assert!(evidence.impacted_schedulers.is_empty());
    assert!(evidence.contributions.iter().all(|row| row.queue_scheduler_id.is_none()));
}
#[test]
fn positive_worker_queue_is_not_a_task_count_foreign_source_or_absence_claim() {
    let row = OperationalObservation {
        source: crate::source_id::WORKER_QUEUE.into(),
        key: "worker-tasks".into(),
        fact: OperationalFact::Queue(QueueSample {
            kind: QueueKind::WorkerTasks,
            count: "1".into(),
            scope: crate::MeasurementScope::Database,
            unattributed_count: "0".into(),
            longest_wait_ms: Some("100".into()),
            minimum_wait_ms: 100,
        }),
    };
    assert!(positive_worker_queue(&row));
    let mut wrong = row.clone();
    wrong.source = crate::source_id::PENDING_GRANTS.into();
    assert!(!positive_worker_queue(&wrong));
    let OperationalFact::Queue(queue) = &mut wrong.fact else { panic!() };
    queue.kind = QueueKind::MemoryGrants;
    assert!(!positive_worker_queue(&wrong));
    let mut zero = row;
    let OperationalFact::Queue(queue) = &mut zero.fact else { panic!() };
    queue.count = "0".into();
    queue.longest_wait_ms = None;
    assert!(!positive_worker_queue(&zero));
}

#[test]
fn source_binding_requires_this_question_dataset_clock_and_native_member_without_snapshot_fallback() {
    let point = point();
    let source = CollectionDescription {
        target: point.context.target.clone(),
        partition: SourcePartition::Target,
        source: crate::source_id::WORKER_QUEUE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::WorkerStarvation,
                observed_from: point.context.observed_from.clone(),
                observed_until: point.context.observed_until.clone(),
                records: 1,
                sources: vec![StateSourceCoverage::collected(crate::source_id::WORKER_QUEUE.into(), 1)],
            },
            focus_key: digest(b"controlled-worker-focus"),
            execution: Box::new(point.context.clone()),
        },
    };
    let record = TelemetryRecord::Operational(OperationalObservation {
        source: source.source.clone(),
        key: point.key().unwrap(),
        fact: OperationalFact::MssqlWorkerStarvation(Box::new(point.clone())),
    });
    let capture = CaptureIdentity { observation_id: "controlled-worker-point".into(), revision: None };
    assert!(ExportedRecord::new(&source, &capture, record.clone()).is_ok());
    let mut wrong = source.clone();
    let SourceObservation::SourceRead { snapshot, .. } = &mut wrong.observation else { panic!() };
    snapshot.dataset = SnapshotDataset::QueryCompilation;
    assert!(ExportedRecord::new(&wrong, &capture, record.clone()).is_err());
    let mut wrong = source.clone();
    let SourceObservation::SourceRead { execution, .. } = &mut wrong.observation else { panic!() };
    execution.observed_until = "2020-01-01T12:00:02Z".into();
    assert!(ExportedRecord::new(&wrong, &capture, record.clone()).is_err());
    let mut wrong = source;
    let snapshot = wrong.observation.snapshot().unwrap().clone();
    wrong.observation = SourceObservation::Snapshot(snapshot);
    assert!(!point.fits_source(&wrong));
}

#[test]
fn native_nullable_worker_fields_remain_required_and_unknown_bindings_are_explicit() {
    let point = point();
    let mut value = serde_json::to_value(&point).unwrap();
    value["tasks"][0]["worker"]["task_bound_ms_ticks"] = serde_json::Value::Null;
    let unknown: MssqlWorkerStarvationPoint = serde_json::from_value(value.clone()).unwrap();
    assert!(unknown.is_valid());
    let evidence = WorkerStarvationEvidence::from_point(&unknown).unwrap();
    assert_eq!(evidence.unverified_worker_bindings, 1);
    assert_eq!(evidence.assigned_workers, 0);
    value["tasks"][0]["worker"].as_object_mut().unwrap().remove("task_bound_ms_ticks");
    assert!(serde_json::from_value::<MssqlWorkerStarvationPoint>(value).is_err());
}
