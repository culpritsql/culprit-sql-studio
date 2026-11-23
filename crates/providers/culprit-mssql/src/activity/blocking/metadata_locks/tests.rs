use super::*;
use crate::{
    activity::{
        blocking::tests::{self as baseline, bound, reader},
        page_targets::tests::participant,
        tests::details,
    },
    read::{ReadError, ReadResult},
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};
use culprit_core::blocking::{MetadataQueueIssue, MetadataQueueStatus};
use data::{NativeEntry, NativeTask};

fn actors() -> Vec<Rec> {
    vec![
        replace(&baseline::waiter(), &[("wait_type", Val::Text("LCK_M_SCH_S".into()))]),
        replace(
            &baseline::waiter(),
            &[
                ("session_id", Val::Int(55)),
                ("sql_handle", Val::Text("CD".repeat(64))),
                ("query_hash", Val::Text("FEDCBA9876543210".into())),
                ("wait_type", Val::Text("LCK_M_SCH_M".into())),
                ("blocking_session_id", Val::Int(66)),
            ],
        ),
        replace(&baseline::owner(), &[("session_id", Val::Int(66)), ("transaction_id", Val::Int(1066))]),
    ]
}
fn native_lock(session: i32, mode: &str, status: &str, address: u64) -> NativeLock {
    NativeLock {
        session_id: session,
        resource: MssqlObjectLockResource {
            resource_type: "OBJECT".into(),
            subtype: Some(String::new()),
            database_id: 7,
            description: Some(String::new()),
            associated_entity_id: "12345".into(),
            lock_partition: 0,
            created_without_offset: Some("2019-01-01T00:00:00".into()),
        },
        object_label: Some("dbo.orders".into()),
        mode: Some(mode.into()),
        status: Some(status.into()),
        owner_type: Some("TRANSACTION".into()),
        owner_id: Some(format!("10{session}")),
        owner_guid: None,
        owner_address: Some(format!("{address:016X}")),
        request_id: Some(0),
        execution_context_id: Some(0),
        transaction: Some(MssqlLockTransaction {
            id: format!("10{session}"),
            begun_without_offset: "2020-01-01T07:58:30".into(),
            active_local_user_transaction: Some(true),
        }),
    }
}
fn entries() -> Vec<NativeEntry> {
    let mut entries = vec![
        NativeEntry::Lock(Box::new(native_lock(42, "Sch-S", "WAIT", 42))),
        NativeEntry::Lock(Box::new(native_lock(55, "IX", "GRANT", 155))),
        NativeEntry::Lock(Box::new(native_lock(55, "Sch-M", "WAIT", 55))),
        NativeEntry::Lock(Box::new(native_lock(66, "IX", "GRANT", 66))),
    ];
    for (waiter, blocker) in [(42, 55), (55, 66)] {
        entries.push(NativeEntry::Task(NativeTask {
            task_address: format!("000000000000FF{waiter}"),
            session_id: waiter,
            execution_context_id: 0,
            owner_address: Some(format!("{:016X}", waiter as u64)),
            blocking_session_id: Some(blocker),
            blocker_execution_context_id: Some(0),
        }));
    }
    entries
}
fn fields(row: &Rec, changes: &[(&str, Val)]) -> Rec {
    let mut columns = row
        .columns()
        .filter(|(name, _)| !changes.iter().any(|(changed, _)| name == changed))
        .map(|(name, value)| (name, value.clone()))
        .collect::<Vec<_>>();
    columns.extend_from_slice(changes);
    Rec::of(&columns)
}
fn endpoint(entries: Vec<NativeEntry>) -> Vec<Rec> {
    let mut rows = vec![Rec::of(&[
        ("ordinal", Val::Int(-1)),
        ("observed_at", Val::Text("2020-01-01T06:00:00Z".into())),
        ("population_visible", Val::Bool(true)),
        ("lock_payload", Val::Text(serde_json::to_string(&entries).unwrap())),
    ])];
    for (ordinal, row) in actors().iter().enumerate() {
        let session = [42, 55, 66][ordinal];
        let checked = participant(ordinal, row);
        let checked = if ordinal == 2 {
            fields(
                &checked,
                &[
                    ("checked_session_id", Val::Null),
                    ("checked_request_id", Val::Null),
                    ("checked_request_generation", Val::Null),
                    ("checked_sql_handle", Val::Null),
                    ("checked_begin", Val::Null),
                    ("checked_end", Val::Null),
                    ("checked_database_id", Val::Null),
                    ("checked_wait_type", Val::Null),
                    ("checked_wait_resource", Val::Null),
                    ("checked_plan_handle", Val::Null),
                    ("checked_query_hash", Val::Null),
                    ("checked_plan_hash", Val::Null),
                ],
            )
        } else {
            checked
        };
        rows.push(fields(
            &checked,
            &[
                ("native_session_id", Val::Int(session)),
                ("native_login", Val::Text("2020-01-01T07:00:00.1533333".into())),
                ("current_requests", Val::Int(if ordinal == 2 { 0 } else { 1 })),
                ("request_transaction_id", if ordinal == 2 { Val::Null } else { Val::Int(1000 + session) }),
                ("population_visible", Val::Bool(true)),
                ("lock_payload", Val::Null),
            ],
        ));
    }
    rows
}
fn replies(first: Vec<Rec>, last: Vec<Rec>) -> Vec<ReadResult<Vec<Rec>>> {
    let mut replies = bound();
    replies.push(Ok(actors()));
    replies.push(Ok(first));
    replies.push(Ok(vec![
        details(42, "SELECT id FROM dbo.orders"),
        details(55, "ALTER TABLE dbo.orders ADD pending int NULL"),
    ]));
    replies.push(Ok(last));
    replies.push(Ok(vec![participant(0, &actors()[0])]));
    replies.extend(bound());
    replies
}
fn report(observation: &culprit_read::FrozenObservation) -> culprit_core::BlockingReport {
    let rows = observation
        .records
        .iter()
        .map(|record| {
            let TelemetryRecord::Activity(observation) = record else { panic!() };
            ActivityFrameRow { observation, reference: None }
        })
        .collect::<Vec<_>>();
    culprit_checks::focused_blocking::explain(&observation.description, &rows, true).unwrap()
}
#[tokio::test]
async fn selected_schema_wait_pays_two_same_reader_snapshots_and_proves_the_full_native_queue_with_an_idle_holder() {
    let focus = baseline::focus().await;
    let mut current = reader(replies(endpoint(entries()), endpoint(entries())), CollectionBudget::default());
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    let report = report(&observed);
    assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Observed);
    let queue = &report.metadata_queue.evidence.cascades[0];
    assert_eq!(queue.gatekeeper.session.id, "55");
    assert_eq!(queue.affected[0].actor.session.id, "42");
    assert_eq!(queue.holders[0].actor.session.id, "66");
    assert_eq!(queue.requested_mode, "Sch-M");
    assert_eq!(queue.holders[0].granted_modes, ["IX"]);
    assert_eq!(current.query_count(), 11, "eleven actual reader dispatches plus existing session SET");
    assert_eq!(current.io.calls[4].sql, current.io.calls[6].sql);
    assert_eq!(current.io.calls[4].limit, 4);
    let [Parameter::Text(cohort), Parameter::Integer(limit)] = current.io.calls[4].parameters.as_slice() else {
        panic!()
    };
    assert!(cohort.contains("session=\"66\"") && cohort.contains("request=\"-1\""));
    assert!(*limit > 0 && *limit <= 512);
    assert!(observed.description.target.database().unwrap().incarnation.is_none());
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
    assert_eq!(current.io.closed, 1);
}
#[tokio::test]
async fn real_native_settlement_mutations_cannot_be_repaired_by_the_equal_original_frame_or_final_database_context() {
    let focus = baseline::focus().await;
    for fault in ["transaction", "object", "resource", "mode", "edge", "actor", "mars", "hidden", "convert"] {
        let first = endpoint(entries());
        let mut last = endpoint(entries());
        if matches!(fault, "actor" | "mars" | "hidden") {
            last[2] = match fault {
                "actor" => fields(&last[2], &[("native_login", Val::Text("2020-01-01T07:00:01".into()))]),
                "mars" => fields(&last[2], &[("current_requests", Val::Int(2))]),
                _ => fields(&last[2], &[("checked_session_id", Val::Null)]),
            };
        } else {
            let mut changed = entries();
            match fault {
                "edge" => {
                    let NativeEntry::Task(task) = &mut changed[4] else { panic!() };
                    task.blocking_session_id = Some(66);
                }
                _ => {
                    let NativeEntry::Lock(lock) = &mut changed[2] else { panic!() };
                    match fault {
                        "transaction" => {
                            lock.transaction.as_mut().unwrap().begun_without_offset = "2020-01-01T07:58:31".into()
                        }
                        "object" => lock.resource.created_without_offset = Some("2020-01-01T05:59:59".into()),
                        "resource" => lock.resource.lock_partition = 1,
                        "mode" => lock.mode = Some("Sch-S".into()),
                        _ => lock.status = Some("CONVERT".into()),
                    }
                }
            }
            last = endpoint(changed);
        }
        let mut current = reader(replies(first, last), CollectionBudget::default());
        let observed = current.capture_focused_blocking(&focus).await.unwrap();
        assert!(report(&observed).metadata_queue.evidence.cascades.is_empty(), "{fault}");
        assert_eq!(observed.records.len(), 3);
        assert_eq!(current.query_count(), 11);
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn an_intent_shared_victim_reaches_the_schema_wait_in_its_existing_native_blocking_closure() {
    let focus = baseline::focus().await;
    let mut native = entries();
    let NativeEntry::Lock(lock) = &mut native[0] else { panic!() };
    lock.mode = Some("IS".into());
    let mut first = endpoint(native.clone());
    let mut last = endpoint(native);
    for rows in [&mut first, &mut last] {
        rows[1] = fields(&rows[1], &[("checked_wait_type", Val::Text("LCK_M_IS".into()))]);
    }
    let mut values = replies(first, last);
    let Ok(rows) = &mut values[3] else { panic!() };
    rows[0] = fields(&rows[0], &[("wait_type", Val::Text("LCK_M_IS".into()))]);
    let Ok(final_check) = &mut values[7] else { panic!() };
    final_check[0] = fields(&final_check[0], &[("checked_wait_type", Val::Text("LCK_M_IS".into()))]);
    let mut current = reader(values, CollectionBudget::default());
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    let evidence = report(&observed).metadata_queue.evidence;
    assert_eq!(evidence.status, MetadataQueueStatus::Observed, "{evidence:?}");
    assert_eq!(evidence.cascades.len(), 1);
    assert_eq!(evidence.cascades[0].gatekeeper.session.id, "55");
    assert_eq!(evidence.cascades[0].affected[0].actor.session.id, "42");
    assert_eq!(current.query_count(), 11);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn permission_denial_and_a_native_prefix_remain_original_gaps_instead_of_healthy_absence() {
    let focus = baseline::focus().await;
    let denied = vec![fields(&endpoint(Vec::new())[0], &[("population_visible", Val::Bool(false))])];
    let mut current = reader(replies(denied.clone(), denied), CollectionBudget::default());
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    let evidence = report(&observed).metadata_queue.evidence;
    assert_eq!(evidence.status, MetadataQueueStatus::Unavailable);
    assert!(evidence.issues.contains(&MetadataQueueIssue::NativeVisibilityUnavailable));
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
    let budget = CollectionBudget::default();
    let mut prefix = entries();
    while prefix.len() <= budget.max_snapshot_rows as usize {
        prefix.push(NativeEntry::Lock(Box::new(native_lock(66, "IX", "GRANT", 1000 + prefix.len() as u64))));
    }
    for (first, last) in [(prefix.clone(), prefix.clone()), (prefix.clone(), entries()), (entries(), prefix)] {
        let mut current = reader(replies(endpoint(first), endpoint(last)), budget.clone());
        let observed = current.capture_focused_blocking(&focus).await.unwrap();
        let evidence = report(&observed).metadata_queue.evidence;
        assert!(evidence.cascades.is_empty());
        assert!(evidence.issues.contains(&MetadataQueueIssue::LockEvidenceLimited));
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn every_new_native_error_stops_before_enrichment_or_final_projection_and_preserves_unconfirmed_shutdown() {
    let focus = baseline::focus().await;
    for position in [4, 6] {
        let mut values = replies(endpoint(entries()), endpoint(entries()));
        values[position] = Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed));
        let mut current = reader(values, CollectionBudget::default());
        current.io.shutdown = SourceShutdown::Unconfirmed;
        let error = current.capture_focused_blocking(&focus).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::CancellationUnconfirmed);
        assert_eq!(current.query_count(), position as u32 + 1);
        assert_eq!(current.close().await, SourceShutdown::Unconfirmed);
    }
}
#[tokio::test]
async fn the_original_caller_allowance_still_pays_setup_and_cannot_skip_the_last_native_guard() {
    let focus = baseline::focus().await;
    let budget = CollectionBudget { max_queries: 11, ..Default::default() };
    let allowed = crate::read::ReadSession::budget_after_setup(budget.clone()).unwrap().max_queries;
    let io = FakeRead::new(replies(endpoint(entries()), endpoint(entries()))).with_query_limit(allowed);
    let mut current = ActivityReader::new(
        io,
        crate::activity::ReadOptions { budget, identity: crate::activity::IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    let error = current.capture_focused_blocking(&focus).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    assert_eq!(current.query_count(), 10);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn ordinary_blocking_and_a_tighter_native_evidence_budget_preserve_existing_acquisition_without_extra_dispatches()
{
    let focus = baseline::focus().await;
    let mut ordinary =
        reader(baseline::replies(vec![baseline::waiter(), baseline::owner()], true, true), CollectionBudget::default());
    let observed = ordinary.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(ordinary.query_count(), 9);
    assert!(report(&observed).metadata_queue.evidence.cascades.is_empty());
    assert_eq!(ordinary.close().await, SourceShutdown::Quiescent);
    let mut values = replies(endpoint(entries()), endpoint(entries()));
    drop(values.remove(6).expect("scripted final metadata endpoint"));
    drop(values.remove(4).expect("scripted initial metadata endpoint"));
    let budget = CollectionBudget { max_snapshot_rows: 3, ..Default::default() };
    let mut limited = reader(values, budget);
    let observed = limited.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(limited.query_count(), 9);
    assert_eq!(observed.records.len(), 3);
    assert!(report(&observed).metadata_queue.evidence.issues.contains(&MetadataQueueIssue::NativeDetailsNotRecorded));
    assert_eq!(limited.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn a_lost_original_lease_fences_every_subsequent_selected_dispatch_instead_of_reopening_after_optional_enrichment()
 {
    let focus = baseline::focus().await;
    for retired_after in [5, 6, 7, 8] {
        let mut values = replies(endpoint(entries()), endpoint(entries()));
        if retired_after == 6 {
            values[5] = Err(ReadError::Source(culprit_core::Error::new(
                culprit_core::ErrorCode::PermissionDenied,
                "controlled optional SQL denial",
            )));
        }
        let io = FakeRead::new(values).with_generation(1).change_generation_at(retired_after);
        let mut current = ActivityReader::new(
            io,
            crate::activity::ReadOptions {
                budget: CollectionBudget::default(),
                identity: crate::activity::IdentityDetail::Endpoint,
            },
            "fixture.invalid".into(),
            1433,
        );
        let error = current.capture_focused_blocking(&focus).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceChanged, "{retired_after}");
        assert_eq!(current.query_count(), retired_after, "no native dispatch after the original lease changed");
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
    let mut values = replies(endpoint(entries()), endpoint(entries()));
    values[5] = Ok(Vec::new()); // Completed same-lease payload-shape failure: independent native edges still stand.
    let io = FakeRead::new(values).with_generation(1);
    let mut current = ActivityReader::new(
        io,
        crate::activity::ReadOptions {
            budget: CollectionBudget::default(),
            identity: crate::activity::IdentityDetail::Endpoint,
        },
        "fixture.invalid".into(),
        1433,
    );
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(report(&observed).metadata_queue.evidence.status, MetadataQueueStatus::Observed);
    assert!(
        observed
            .records
            .iter()
            .filter_map(|row| if let TelemetryRecord::Activity(point) = row { Some(point) } else { None })
            .filter(|point| point.key.request.is_some())
            .all(|point| !point.statement.is_available())
    );
    assert_eq!(current.query_count(), 11);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn the_documented_granted_status_uses_the_same_existing_mode_proof() {
    let mut native = entries();
    for entry in &mut native {
        if let NativeEntry::Lock(lock) = entry
            && lock.status.as_deref() == Some("GRANT")
        {
            lock.status = Some("GRANTED".into());
        }
    }
    let focus = baseline::focus().await;
    let mut current = reader(replies(endpoint(native.clone()), endpoint(native)), CollectionBudget::default());
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(report(&observed).metadata_queue.evidence.status, MetadataQueueStatus::Observed);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[test]
fn the_actual_read_session_focused_capture_future_and_output_have_bounded_inline_storage_without_starting_work() {
    fn storage<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) = storage(
        |(reader, focus): (&'static mut ActivityReader<crate::read::ReadSession>, &'static ActivityRequestFocus)| {
            reader.capture_focused_blocking(focus)
        },
    );
    assert!(future <= 64 * 1024, "actual held selected source future {future}");
    assert!(output <= 16 * 1024, "actual source output {output}");
}

#[tokio::test]
async fn too_many_requested_lock_points_keep_the_existing_native_actor_without_selecting_one_resource_or_exceeding_the_shared_record_cap()
 {
    let mut native = entries();
    for context in 1..=65 {
        let mut lock = native_lock(42, "Sch-S", "WAIT", 2000 + context as u64);
        lock.execution_context_id = Some(context);
        native.push(NativeEntry::Lock(Box::new(lock)));
    }
    let focus = baseline::focus().await;
    let mut current = reader(replies(endpoint(native.clone()), endpoint(native)), CollectionBudget::default());
    let observed = current.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(observed.records.len(), 3);
    assert!(report(&observed).metadata_queue.evidence.issues.contains(&MetadataQueueIssue::LockEvidenceLimited));
    assert!(observed.records.iter().all(TelemetryRecord::is_valid));
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}
