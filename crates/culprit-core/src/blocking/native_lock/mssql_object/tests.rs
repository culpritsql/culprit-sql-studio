use super::*;
fn key(id: &str, idle: bool) -> ActivityKey {
    ActivityKey {
        session: NativeActivityIdentity { id: id.into(), generation: "2020-01-01T11:00:00".into() },
        request: (!idle).then(|| NativeActivityIdentity { id: "0".into(), generation: "2020-01-01T11:59:00".into() }),
        transaction: idle.then(|| "101".into()),
    }
}
fn point() -> MssqlObjectLockPoint {
    let owner = key("43", false);
    MssqlObjectLockPoint {
        observed_at: "2020-01-01T12:00:01Z".into(),
        settled_at: "2020-01-01T12:00:01.5Z".into(),
        waiter: key("42", false),
        waiter_matches: true,
        waiter_settled: true,
        waiter_current_requests: Some("1".into()),
        waiter_complete_request_population: true,
        waiter_transaction: Some(MssqlLockTransaction {
            id: "100".into(),
            begun_without_offset: "2020-01-01T11:58:00".into(),
            active_local_user_transaction: Some(true),
        }),
        resource: Some(MssqlObjectLockResource {
            resource_type: "OBJECT".into(),
            subtype: None,
            database_id: 7,
            description: None,
            associated_entity_id: "123".into(),
            lock_partition: 0,
            created_without_offset: Some("2019-01-01T00:00:00".into()),
        }),
        requested_mode: Some(MssqlObjectLockMode::SchemaStability),
        owner_type: Some("TRANSACTION".into()),
        owner_id: Some("100".into()),
        owner_guid: None,
        requested_status: Some("WAIT".into()),
        lock_owner_address: Some("0000000000000042".into()),
        lock_request_id: Some("0".into()),
        lock_execution_context_id: Some("0".into()),
        availability: MssqlObjectLockAvailability::Observed,
        snapshot_rows: 6,
        snapshot_limit: 32,
        snapshot_complete: true,
        blockers_complete: true,
        blockers: vec![MssqlObjectLockBlocker {
            context: NativeBlockingOwnerContext {
                owner: ActivityWaitOwner::Session {
                    id: "43".into(),
                    generation: Some(owner.session.generation.clone()),
                },
                actor: Some(owner),
                statement: Some(NativeIdentity::new("original-statement")),
                native_wait: None,
                wait_ms: None,
                current_requests: Some("1".into()),
                complete_request_population: true,
            },
            transaction: Some(MssqlLockTransaction {
                id: "101".into(),
                begun_without_offset: "2020-01-01T11:58:01".into(),
                active_local_user_transaction: Some(true),
            }),
            owner_type: Some("TRANSACTION".into()),
            owner_id: Some("101".into()),
            owner_guid: None,
            granted_modes: vec![MssqlObjectLockMode::Other("IX".into())],
            waiting_mode: Some(MssqlObjectLockMode::SchemaModification),
            waiting_status: Some("WAIT".into()),
            native_wait_edges: vec![MssqlObjectLockWaitEdge {
                task_address: "000000000000FF42".into(),
                owner_address: "0000000000000042".into(),
                waiter_execution_context_id: "0".into(),
                blocking_session_id: "43".into(),
                blocker_execution_context_id: Some("0".into()),
            }],
            matched: true,
            settled: true,
        }],
    }
}
#[test]
fn native_schema_compatibility_distinguishes_queued_gate_from_granted_owner_without_escalation_or_sql_inference() {
    let point = point();
    assert!(point.is_valid_for(&point.waiter));
    assert_eq!(point.edge(&point.blockers[0]), NativeRelationLockEdge::PendingOnlyConflict);
    let mut both = point.blockers[0].clone();
    both.granted_modes.push(MssqlObjectLockMode::SchemaModification);
    assert_eq!(point.edge(&both), NativeRelationLockEdge::GrantedAndPendingConflict);
    let mut granted = point.clone();
    granted.requested_mode = Some(MssqlObjectLockMode::SchemaModification);
    granted.blockers[0].waiting_mode = None;
    granted.blockers[0].waiting_status = None;
    assert_eq!(granted.edge(&granted.blockers[0]), NativeRelationLockEdge::GrantedConflict);
    let encoded = serde_json::to_string(&point).unwrap();
    assert!(encoded.contains("\"subtype\":null") && encoded.contains("\"description\":null"));
}
#[test]
fn changed_epoch_actor_transaction_pointer_visibility_mars_conversion_or_unknown_mode_never_establishes_an_edge() {
    for fault in 0..14 {
        let mut point = point();
        match fault {
            0 => point.waiter_matches = false,
            1 => point.waiter_settled = false,
            2 => point.blockers[0].settled = false,
            3 => point.blockers[0].transaction.as_mut().unwrap().active_local_user_transaction = None,
            4 => point.blockers[0].owner_id = Some("999".into()),
            5 => point.blockers[0].context.complete_request_population = false,
            6 => point.blockers[0].waiting_status = Some("CONVERT".into()),
            7 => point.requested_status = Some("CONVERT".into()),
            8 => point.blockers[0].native_wait_edges[0].owner_address = "0000000000000099".into(),
            9 => point.blockers[0].native_wait_edges[0].blocking_session_id = "44".into(),
            10 => point.blockers[0].owner_guid = Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into()),
            11 => point.blockers[0].granted_modes.push(MssqlObjectLockMode::from_native("UnknownNativeMode")),
            12 => point.resource.as_mut().unwrap().created_without_offset = None,
            _ => point.waiter_current_requests = Some("2".into()),
        }
        assert_eq!(point.edge(&point.blockers[0]), NativeRelationLockEdge::Unknown, "{fault}");
    }
    let mut idle = point();
    idle.blockers[0].context.actor = Some(key("43", true));
    idle.blockers[0].context.current_requests = Some("0".into());
    idle.blockers[0].context.statement = None;
    idle.blockers[0].waiting_mode = None;
    idle.blockers[0].waiting_status = None;
    idle.blockers[0].granted_modes = vec![MssqlObjectLockMode::SchemaModification];
    assert!(idle.blockers[0].is_valid());
    assert_eq!(idle.edge(&idle.blockers[0]), NativeRelationLockEdge::GrantedConflict);
    idle.blockers[0].context.actor.as_mut().unwrap().transaction = None;
    assert_eq!(idle.edge(&idle.blockers[0]), NativeRelationLockEdge::Unknown);
}
