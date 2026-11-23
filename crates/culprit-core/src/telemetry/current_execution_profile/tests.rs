use super::*;
use crate::telemetry::current_request_work::tests as work;

pub(in crate::telemetry) fn point() -> MssqlCurrentExecutionProfilePoint {
    let input = work::mssql::input();
    let focus = work::focus(&input, input.context.partition.clone());
    let CurrentRequestWorkNative::Mssql { work } = &input.observations[0].native else { panic!() };
    let mut work = work.clone();
    work.request_plan =
        Some(NativeIdentity::new("CCDD").qualified("native_object", "sys.dm_exec_requests.plan_handle"));
    let task = |address: u32| MssqlWorkerTask {
        address: format!("{address:016X}"),
        state: "RUNNING".into(),
        scheduler_id: Some(0),
        parent_node_id: Some(0),
        scheduler_work_queue_count: Some("0".into()),
        session_id: Some(42),
        request_id: Some(0),
        execution_context_id: Some(address),
        worker: Some(MssqlWorkerBinding {
            address: format!("{:016X}", address + 100),
            task_address: Some(format!("{address:016X}")),
            created_ms_ticks: Some("10".into()),
            task_bound_ms_ticks: Some("20".into()),
            state: Some("RUNNING".into()),
            is_preemptive: Some(false),
        }),
        wait: None,
        request: Some(focus.original.key.clone()),
    };
    let thread = |id: u32, rows: &str| MssqlOperatorThreadProfile {
        node_id: 0,
        thread_id: id,
        task_address: Some(format!("{id:016X}")),
        physical_operator: "Table Scan".into(),
        counters: MssqlOperatorThreadCounters {
            rows_returned: Some(rows.into()),
            rows_read: Some(rows.into()),
            ..Default::default()
        },
        open_ticks: Some("0".into()),
        close_ticks: Some("0".into()),
        database_id: Some(7),
        object_id: Some(101),
        index_id: Some(0),
    };
    let tasks = vec![task(1), task(2)];
    let point = MssqlCurrentExecutionProfilePoint {
        context: input.context,
        focus: Box::new(focus),
        observed_at: "2020-01-01T12:00:00.5Z".into(),
        checked_until: "2020-01-01T12:00:00.9Z".into(),
        binding: FocusedRequestBinding::Matched,
        membership: FocusedRequestMembership::Observed,
        statement: input.observations[0].statement.clone(),
        work: Some(work),
        workers_before: Some(MssqlSelectedWorkerPoint {
            observed_at: "2020-01-01T12:00:00.55Z".into(),
            tasks: tasks.clone(),
            limited: false,
        }),
        workers_after: Some(MssqlSelectedWorkerPoint {
            observed_at: "2020-01-01T12:00:00.8Z".into(),
            tasks,
            limited: false,
        }),
        profiles: MssqlExecutionProfiles::Observed {
            observed_from: "2020-01-01T12:00:00.6Z".into(),
            observed_until: "2020-01-01T12:00:00.7Z".into(),
            rows: vec![thread(1, "1"), thread(2, "9007199254740993")],
            limited: false,
            operators: MssqlExecutionOperators::Unavailable {
                reason: MssqlExecutionOperatorsUnavailable::PlanUnavailable,
            },
        },
        relational_inputs: None,
    };
    assert!(point.is_valid());
    point
}
#[test]
fn exact_profile_counts_nullable_facts_and_observed_ranges_roundtrip_without_workload_or_completion_inference() {
    let point = point();
    let encoded = serde_json::to_vec(&point).unwrap();
    let read: MssqlCurrentExecutionProfilePoint = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(read, point);
    let evidence = CurrentExecutionProfileEvidence::from_point(&read).unwrap();
    assert!(evidence.nodes[0].rows_returned.uneven());
    assert_eq!(evidence.nodes[0].rows_returned.maximum.as_deref(), Some("9007199254740993"));
    assert_eq!(evidence.nodes[0].physical_reads.interpreted, 0);
    assert_eq!(evidence.nodes[0].threads[0].task_context, ExecutionTaskContext::CheckedAtBounds);
    let MssqlExecutionProfiles::Observed { rows, .. } = &point.profiles else { panic!() };
    assert_eq!(rows[0].close_ticks.as_deref(), Some("0"));
}
#[test]
fn native_negative_and_unknown_thread_counts_withhold_only_the_observed_comparison() {
    for value in [None, Some("-1".to_owned())] {
        let mut point = point();
        let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
        rows[0].counters.rows_returned = value;
        assert!(point.is_valid());
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert_eq!(evidence.profile_rows, 2);
        assert!(!evidence.nodes[0].rows_returned.uneven());
        assert_eq!(evidence.nodes[0].rows_returned.interpreted, 1);
        assert_eq!(evidence.nodes[0].rows_returned.maximum.as_deref(), Some("9007199254740993"));
    }
    for malformed in ["01", "1.0", "9223372036854775808"] {
        let mut point = point();
        let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
        rows[0].counters.rows_returned = Some(malformed.into());
        assert!(!point.is_valid());
    }
}
#[test]
fn worker_address_without_reciprocal_generation_and_known_actor_cannot_supply_task_wait_attribution() {
    for defect in ["worker-generation", "task-generation", "actor-generation", "missing-before"] {
        let mut point = point();
        match defect {
            "missing-before" => {
                point.workers_before.as_mut().unwrap().tasks.remove(0);
            }
            "actor-generation" => {
                point.workers_after.as_mut().unwrap().tasks[0].request.as_mut().unwrap().session.generation =
                    "2020-01-01T10:00:01.0030000".into();
            }
            _ => {
                let worker = point.workers_after.as_mut().unwrap().tasks[0].worker.as_mut().unwrap();
                if defect == "worker-generation" {
                    worker.created_ms_ticks = Some("11".into())
                } else {
                    worker.task_bound_ms_ticks = Some("21".into())
                };
            }
        };
        assert!(point.is_valid());
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert_ne!(evidence.nodes[0].threads[0].task_context, ExecutionTaskContext::CheckedAtBounds);
        assert!(evidence.nodes[0].threads[0].endpoint_wait.is_none());
    }
}
#[test]
fn role_native_owner_profile_bounds_duplicate_threads_and_row_caps_remain_authoritative() {
    for defect in ["role", "member", "epoch", "clock", "duplicate", "cap"] {
        let mut point = point();
        match defect {
            "role" => point.context.database_writable = None,
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut point.context.partition else {
                    panic!()
                };
                member.native.id = "foreign-member".into();
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut point.context.partition else { panic!() };
                *epoch = Some("foreign-generation".into());
            }
            _ => {
                let MssqlExecutionProfiles::Observed { rows, observed_until, .. } = &mut point.profiles else {
                    panic!()
                };
                match defect {
                    "clock" => *observed_until = "2020-01-01T12:00:00.85Z".into(),
                    "duplicate" => rows.push(rows[0].clone()),
                    _ => {
                        rows.clear();
                        for id in 0..=MAX_EXECUTION_PROFILE_ROWS {
                            let mut row = point_profile();
                            row.thread_id = id as u32;
                            rows.push(row);
                        }
                    }
                }
            }
        }
        assert!(!point.is_valid(), "{defect}");
    }
}
fn point_profile() -> MssqlOperatorThreadProfile {
    let MssqlExecutionProfiles::Observed { mut rows, .. } = point().profiles else { panic!() };
    rows.remove(0)
}
