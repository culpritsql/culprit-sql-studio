use super::super::tests::{Reader, activity_frame, metadata};
use super::*;
use std::collections::{BTreeMap, BTreeSet};
fn frame() -> ActivityFrame {
    activity_frame(vec![metadata(10, "42", true), metadata(20, "42", true), metadata(30, "42", false)])
}
fn identities() -> (BTreeMap<u32, usize>, BTreeSet<usize>) {
    ([(10, 0), (20, 1), (30, 2)].into(), [0, 1].into())
}
fn row(pid: u32, mode: &str, owner: u32, pending: Option<&str>) -> Row {
    let owners = serde_json::json!([{"pid":owner.to_string(),"actor_matches":true,"single_backend":true,"granted":["AccessShareLock"],"waiting":pending.into_iter().collect::<Vec<_>>() }]);
    Row::from_fields([
        ("pid".into(), Some(pid.to_string())),
        ("observed_at".into(), Some("2020-01-01T06:00:01Z".into())),
        ("waiter_matches".into(), Some("true".into())),
        ("single_backend".into(), Some("true".into())),
        ("actor_rows".into(), Some("3".into())),
        ("snapshot_rows".into(), Some("6".into())),
        ("blockers_complete".into(), Some("true".into())),
        ("locktype".into(), Some("relation".into())),
        ("mode".into(), Some(mode.into())),
        ("database_oid".into(), Some("42".into())),
        ("relation_oid".into(), Some("12345".into())),
        ("object".into(), Some("public.orders".into())),
        ("index_name".into(), None),
        ("elapsed_ms".into(), Some("1000".into())),
        ("owners".into(), Some(owners.to_string())),
    ])
}
fn change(row: Row, field: &str, value: &str) -> Row {
    let fields = [
        "pid",
        "observed_at",
        "waiter_matches",
        "single_backend",
        "actor_rows",
        "snapshot_rows",
        "blockers_complete",
        "locktype",
        "mode",
        "database_oid",
        "relation_oid",
        "object",
        "index_name",
        "elapsed_ms",
        "owners",
    ];
    Row::from_fields(
        fields.into_iter().map(|name| (name.into(), if name == field { Some(value.into()) } else { row.text(name) })),
    )
}
#[tokio::test]
async fn one_focused_dispatch_retains_native_modes_and_edges_with_one_capped_lock_snapshot_and_no_sql_payload() {
    let mut frame = frame();
    let (by_pid, visited) = identities();
    let mut reader = Reader::new(vec![Ok(vec![
        row(10, "AccessShareLock", 20, Some("AccessExclusiveLock")),
        row(20, "AccessExclusiveLock", 30, None),
    ])]);
    assert!(!read(&mut reader, &mut frame, &by_pid, &visited).await.unwrap());
    assert_eq!(reader.calls.len(), 1);
    let (sql, limit) = &reader.calls[0];
    assert_eq!(*limit, 2);
    assert_eq!(sql.matches("pg_catalog.pg_locks").count(), 1);
    assert_eq!(sql.matches("pg_catalog.pg_blocking_pids(").count(), 1);
    assert!(sql.contains("WHERE l.pid IN (SELECT pid FROM current_actors) LIMIT 513"));
    assert!(sql.contains("retained_locks AS MATERIALIZED (SELECT * FROM lock_snapshot LIMIT 512)"));
    for guard in [
        "a.backend_start=t.backend_start",
        "a.query_start IS NOT DISTINCT FROM t.query_start",
        "a.xact_start IS NOT DISTINCT FROM t.xact_start",
        "m.database=w.database AND m.relation=w.relation",
        "WHERE t.selected",
        "a.leader_pid IN (SELECT pid FROM targets)",
        "worker.leader_pid=a.pid",
        "LIMIT 66",
        "actor_size.rows<=65",
    ] {
        assert!(sql.contains(guard), "{guard}");
    }
    assert!(!sql.contains("a.query,") && !sql.contains("SELECT a.* FROM targets"));
    let NativeLockEvidence::Postgres(point) = frame.locks[0].native.as_deref().unwrap();
    assert!(point.is_valid_for(&frame.records[0].facts.as_ref().unwrap().key));
    assert_eq!(point.resource.as_ref().unwrap().relation_oid, "12345");
    assert_eq!(point.edge(&point.blockers[0]), culprit_core::blocking::NativeRelationLockEdge::PendingOnlyConflict);
    let NativeLockEvidence::Postgres(gate) = frame.locks[1].native.as_deref().unwrap();
    assert_eq!(gate.edge(&gate.blockers[0]), culprit_core::blocking::NativeRelationLockEdge::GrantedConflict);
}
#[tokio::test]
async fn truncated_lock_or_blocker_prefix_and_changed_waiter_retain_unknown_not_a_positive_queue() {
    for (field, value, limited) in [
        ("snapshot_rows", "513", true),
        ("blockers_complete", "false", true),
        ("waiter_matches", "false", false),
        ("single_backend", "false", false),
        ("actor_rows", "66", true),
    ] {
        let mut frame = frame();
        let (by_pid, visited) = identities();
        let mut reader = Reader::new(vec![Ok(vec![
            {
                let mut changed = change(row(10, "AccessShareLock", 20, Some("AccessExclusiveLock")), field, value);
                if matches!(field, "waiter_matches" | "actor_rows") {
                    changed = change(changed, "single_backend", "false");
                }
                if field == "actor_rows" {
                    let mut owners: serde_json::Value = serde_json::from_str(&changed.text("owners").unwrap()).unwrap();
                    owners[0]["single_backend"] = serde_json::json!(false);
                    changed = change(changed, "owners", &owners.to_string());
                }
                changed
            },
            row(20, "AccessExclusiveLock", 30, None),
        ])]);
        assert_eq!(read(&mut reader, &mut frame, &by_pid, &visited).await.unwrap(), limited);
        let NativeLockEvidence::Postgres(point) = frame.locks[0].native.as_deref().unwrap();
        assert_eq!(point.edge(&point.blockers[0]), culprit_core::blocking::NativeRelationLockEdge::Unknown);
        assert_eq!(reader.calls.len(), 1);
    }
}
#[tokio::test]
async fn missing_duplicate_foreign_or_malformed_result_and_native_failure_never_start_another_read() {
    let first = row(10, "AccessShareLock", 20, Some("AccessExclusiveLock"));
    let second = row(20, "AccessExclusiveLock", 30, None);
    for rows in [
        vec![first.clone()],
        vec![first.clone(), first.clone()],
        vec![change(first.clone(), "pid", "99"), second.clone()],
        vec![
            change(
                first.clone(),
                "owners",
                "[{\"pid\":\"99\",\"actor_matches\":true,\"single_backend\":true,\"granted\":[],\"waiting\":[]}]",
            ),
            second.clone(),
        ],
        vec![change(first.clone(), "observed_at", "2020-01-01T05:59:59Z"), second.clone()],
    ] {
        let mut frame = frame();
        let (by_pid, visited) = identities();
        let mut reader = Reader::new(vec![Ok(rows)]);
        assert!(read(&mut reader, &mut frame, &by_pid, &visited).await.is_err());
        assert_eq!(reader.calls.len(), 1);
    }
    let mut frame = frame();
    let (by_pid, visited) = identities();
    let mut reader = Reader::new(vec![Err(CollectionStop::TimedOut.error())]);
    let error = read(&mut reader, &mut frame, &by_pid, &visited).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::TimedOut);
    assert_eq!(reader.calls.len(), 1);
    assert!(frame.locks.is_empty());
}
#[test]
fn actual_unpolled_relation_read_future_and_output_have_finite_storage_without_a_database() {
    let mut frame = frame();
    let (by_pid, visited) = identities();
    let mut reader = Reader::new(vec![]);
    let future = read(&mut reader, &mut frame, &by_pid, &visited);
    let storage = std::mem::size_of_val(&future);
    drop(future);
    assert!(storage <= 4096, "actual read future stores {storage} bytes");
    assert!(std::mem::size_of::<Result<bool>>() <= 128);
    assert!(reader.calls.is_empty());
}
