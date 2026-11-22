use super::*;
fn key(pid: &str) -> ActivityKey {
    ActivityKey {
        session: NativeActivityIdentity { id: pid.into(), generation: "2020-01-01T11:00:00Z".into() },
        request: Some(NativeActivityIdentity { id: "statement".into(), generation: "2020-01-01T11:59:00Z".into() }),
        transaction: None,
    }
}
fn point() -> PostgresLockPoint {
    PostgresLockPoint {
        observed_at: "2020-01-01T12:00:00Z".into(),
        waiter: key("42"),
        waiter_matches: true,
        single_backend: true,
        actor_rows: 3,
        resource: Some(PostgresRelationLockTag { database_oid: "42".into(), relation_oid: "12345".into() }),
        requested_mode: Some(PostgresRelationLockMode::AccessShare),
        snapshot_rows: 6,
        snapshot_limit: 512,
        snapshot_complete: true,
        blockers_complete: true,
        blockers: vec![PostgresLockBlocker {
            native_pid: "43".into(),
            actor: Some(key("43")),
            single_backend: true,
            granted_modes: vec![PostgresRelationLockMode::AccessShare],
            waiting_mode: Some(PostgresRelationLockMode::AccessExclusive),
        }],
    }
}
#[test]
fn all_native_relation_modes_follow_the_documented_symmetric_conflict_matrix_and_unknown_never_guesses() {
    use PostgresRelationLockMode::*;
    let modes = [
        AccessShare,
        RowShare,
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ];
    let expected = [128u8, 192, 240, 248, 236, 252, 254, 255];
    for (a, mode) in modes.iter().enumerate() {
        assert_eq!(PostgresRelationLockMode::from_native(mode.native()), *mode);
        for (b, other) in modes.iter().enumerate() {
            assert_eq!(mode.conflicts_with(other), Some(expected[a] & (1 << b) != 0));
            assert_eq!(mode.conflicts_with(other), other.conflicts_with(mode));
        }
        assert_eq!(mode.conflicts_with(&Unknown("future-native-mode".into())), None);
    }
    assert_eq!(AccessShare.conflicts_with(&AccessShare), Some(false));
    assert_eq!(AccessExclusive.conflicts_with(&AccessShare), Some(true));
}
#[test]
fn pending_only_granted_only_and_mixed_edges_remain_distinct_and_parallel_or_limited_is_unknown() {
    let original = point();
    assert!(original.is_valid_for(&key("42")));
    assert_eq!(original.edge(&original.blockers[0]), NativeRelationLockEdge::PendingOnlyConflict);
    let mut granted = original.clone();
    granted.requested_mode = Some(PostgresRelationLockMode::AccessExclusive);
    granted.blockers[0].waiting_mode = None;
    assert_eq!(granted.edge(&granted.blockers[0]), NativeRelationLockEdge::GrantedConflict);
    let mut mixed = original.clone();
    mixed.blockers[0].granted_modes.push(PostgresRelationLockMode::AccessExclusive);
    assert_eq!(mixed.edge(&mixed.blockers[0]), NativeRelationLockEdge::GrantedAndPendingConflict);
    for change in 0..7 {
        let mut changed = original.clone();
        match change {
            0 => changed.single_backend = false,
            1 => changed.blockers[0].single_backend = false,
            2 => {
                changed.actor_rows = 66;
                changed.single_backend = false;
                changed.blockers[0].single_backend = false;
            }
            3 => {
                changed.snapshot_rows = 513;
                changed.snapshot_complete = false;
            }
            4 => changed.blockers_complete = false,
            5 => changed.blockers[0].actor = None,
            _ => changed.blockers[0].granted_modes.push(PostgresRelationLockMode::Unknown("future-mode".into())),
        }
        if change == 5 {
            changed.blockers[0].single_backend = false;
        }
        assert!(changed.is_valid_for(&key("42")), "{change}");
        assert_eq!(changed.edge(&changed.blockers[0]), NativeRelationLockEdge::Unknown, "{change}");
    }
}
#[test]
fn native_provenance_rejects_reused_actor_invalid_oids_contradictory_caps_and_control_text() {
    let original = point();
    for change in 0..7 {
        let mut changed = original.clone();
        match change {
            0 => changed.waiter.session.generation = "other-generation".into(),
            1 => changed.resource.as_mut().unwrap().relation_oid = "0".into(),
            2 => changed.resource.as_mut().unwrap().database_oid = "042".into(),
            3 => changed.snapshot_complete = false,
            4 => changed.blockers.push(changed.blockers[0].clone()),
            5 => changed.requested_mode = Some(PostgresRelationLockMode::Unknown("bad\nmode".into())),
            _ => changed.waiter_matches = false,
        }
        assert!(!changed.is_valid_for(&key("42")), "{change}");
    }
    let mut prepared = original.clone();
    prepared.blockers[0].native_pid = "0".into();
    prepared.blockers[0].actor = None;
    prepared.blockers[0].single_backend = false;
    assert!(prepared.is_valid_for(&key("42")));
    assert_eq!(prepared.edge(&prepared.blockers[0]), NativeRelationLockEdge::Unknown);
    assert!(matches!(
        prepared.blockers[0].owner(),
        ActivityWaitOwner::Transaction { owner: TransactionWaitOwner::Prepared, .. }
    ));
}
