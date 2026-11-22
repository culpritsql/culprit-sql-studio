use super::*;
use crate::{
    activity::{
        page_targets::tests::participant,
        tests::{details, metadata},
    },
    instance::test_support::{epoch, metadata as platform},
    read::ReadResult,
    resources::test_support::{FakeRead, replace},
    value::Val,
};

pub(crate) fn authority(edition: i64) -> Vec<ReadResult<Vec<Rec>>> {
    if edition == 5 {
        vec![Ok(vec![crate::activity::database_context::tests::row(5)])]
    } else {
        vec![Ok(vec![platform()]), Ok(vec![epoch(100)]), Ok(vec![crate::activity::database_context::tests::row(3)])]
    }
}
pub(crate) fn row(edition: i64) -> Rec {
    let raw = replace(&metadata(42), &[("edition", Val::Int(edition)), ("blocking_session_id", Val::Int(0))]);
    let mut fields = raw.columns().map(|(key, value)| (key.to_owned(), value.clone())).collect::<Vec<_>>();
    fields.extend([
        ("worker_visibility".into(), Val::Int(1)),
        ("worker_row_kind".into(), Val::Int(1)),
        ("worker_task_address".into(), Val::Text("0000000000000001".into())),
        ("worker_task_state".into(), Val::Text("RUNNING".into())),
        ("worker_scheduler_id".into(), Val::Int(0)),
        ("worker_node_id".into(), Val::Int(0)),
        ("worker_scheduler_queue_count".into(), Val::Int(1)),
        ("worker_session_id".into(), Val::Int(42)),
        ("worker_request_id".into(), Val::Int(0)),
        ("worker_execution_context_id".into(), Val::Int(1)),
        ("assigned_worker_address".into(), Val::Text("0000000000000002".into())),
        ("worker_current_task_address".into(), Val::Text("0000000000000001".into())),
        ("worker_created_ms_ticks".into(), Val::Int(10)),
        ("task_bound_ms_ticks".into(), Val::Int(11)),
        ("worker_state".into(), Val::Text("RUNNING".into())),
        ("is_preemptive".into(), Val::Int(0)),
        ("worker_waiting_task_address".into(), Val::Null),
        ("worker_wait_session_id".into(), Val::Null),
        ("worker_wait_execution_context_id".into(), Val::Null),
        ("worker_wait_request_id".into(), Val::Null),
        ("worker_wait_scheduler_id".into(), Val::Null),
        ("worker_wait_node_id".into(), Val::Null),
        ("worker_wait_type".into(), Val::Null),
        ("worker_wait_ms".into(), Val::Null),
        ("worker_blocking_task_address".into(), Val::Null),
        ("worker_blocking_session_id".into(), Val::Null),
        ("worker_blocking_execution_context_id".into(), Val::Null),
    ]);
    Rec::of(&fields.iter().map(|(key, value)| (key.as_str(), value.clone())).collect::<Vec<_>>())
}
pub(crate) fn replies(edition: i64) -> Vec<ReadResult<Vec<Rec>>> {
    let mut replies = if edition == 5 { vec![Ok(vec![crate::grants::tests::platform()])] } else { vec![] };
    replies.extend(authority(edition));
    let native = row(edition);
    let check = participant(0, &native);
    replies.extend([
        Ok(vec![native]),
        Ok(vec![check.clone()]),
        Ok(vec![details(42, "SELECT value FROM dbo.orders WHERE id=42")]),
        Ok(vec![check]),
    ]);
    replies.extend(authority(edition));
    replies.extend(authority(edition));
    replies
}
pub(crate) async fn context_from_owner(edition: i64, shared: bool) -> NativeReadContext {
    let mut replies = if edition == 5 { vec![Ok(vec![crate::grants::tests::platform()])] } else { vec![] };
    replies.extend(authority(edition));
    let mut io = FakeRead::new(replies).with_generation(1);
    let owner = crate::local_source::Context::read(&mut io, "fixture.invalid", 1433).await.unwrap();
    let mut context = owner.completed(&owner).unwrap();
    if shared {
        let member = owner.source_instance().unwrap();
        context.target = CollectionTarget::Shared(member.clone());
        context.partition = if let Some(instance) = &owner.instance {
            SourcePartition::InstanceLocal { member: Some(member), epoch: Some(instance.capture_epoch().unwrap()) }
        } else {
            owner.source_partition().unwrap()
        };
        context.database_writable = None;
    }
    context
}
pub(crate) async fn expected_from_owner(edition: i64, shared: bool) -> SourceReadExecutionScope {
    SourceReadExecutionScope::from_context(
        SourceReadQuestion::WorkerStarvation,
        &context_from_owner(edition, shared).await,
    )
    .unwrap()
}
fn activity_reader(replies: Vec<ReadResult<Vec<Rec>>>) -> ActivityReader<FakeRead> {
    reader_with_io(FakeRead::new(replies).with_generation(1))
}
fn reader_with_io(io: FakeRead) -> ActivityReader<FakeRead> {
    ActivityReader::new(
        io,
        super::super::ReadOptions {
            identity: super::super::IdentityDetail::Endpoint,
            budget: CollectionBudget::default(),
        },
        "fixture.invalid".into(),
        1433,
    )
}
#[tokio::test]
async fn native_worker_projection_reuses_request_decoder_and_two_batched_checks_without_task_to_dop_math() {
    let expected = expected_from_owner(3, false).await;
    let mut reader = activity_reader(replies(3));
    let captured = reader.capture_worker_starvation(&expected).await.unwrap();
    assert_eq!(captured.point.tasks.len(), 1);
    assert_eq!(captured.point.requests.len(), 1);
    assert!(captured.point.requests[0].is_qualified());
    assert!(captured.point.requests[0].observation.statement.is_available());
    assert_eq!(reader.query_count(), 10);
    assert!(reader.verify_worker_starvation(&captured.owner, &captured.point.context.observed_until).await.unwrap());
    assert_eq!(reader.query_count(), 13);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    assert_eq!(reader.io.closed, 1);
    let inventory = &reader.io.calls[3].sql;
    for native in
        ["sys.dm_os_workers", "sys.dm_os_tasks", "sys.dm_os_waiting_tasks", "worker.worker_address=task.worker_address"]
    {
        assert!(inventory.contains(native));
    }
    assert!(
        !inventory.contains("parallel_worker_count")
            && !inventory.contains("query_plan")
            && !inventory.contains("sys.dm_exec_sql_text")
    );
    assert_eq!(reader.io.calls.iter().filter(|call| call.sql.contains("sys.dm_exec_sql_text")).count(), 1);
}
#[tokio::test]
async fn every_native_error_stops_followup_checks_and_final_context_even_if_underlying_reader_could_reopen() {
    for index in [3, 4, 5, 6] {
        let mut replies = replies(3);
        replies[index] = Err(ReadError::Source(culprit_core::Error::new(
            culprit_core::ErrorCode::QueryFailed,
            "controlled native failure",
        )));
        let expected = expected_from_owner(3, false).await;
        let mut reader = activity_reader(replies);
        assert!(reader.capture_worker_starvation(&expected).await.is_err());
        assert_eq!(reader.io.calls.len(), index + 1);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        assert_eq!(reader.io.closed, 1);
    }
}
#[tokio::test]
async fn changed_generation_statement_or_missing_request_keeps_raw_assignment_and_withholds_current_sql_attribution() {
    for change in ["missing", "generation", "statement"] {
        let mut replies = replies(3);
        let native = row(3);
        let checked = participant(0, &native);
        let changes = match change {
            "missing" => vec![("checked_session_id", Val::Null), ("checked_request_id", Val::Null)],
            "generation" => vec![("checked_request_generation", Val::Text("2020-01-01T07:58:02".into()))],
            _ => vec![("checked_begin", Val::Int(2))],
        };
        replies[6] = Ok(vec![replace(&checked, &changes)]);
        let expected = expected_from_owner(3, false).await;
        let mut reader = activity_reader(replies);
        let captured = reader.capture_worker_starvation(&expected).await.unwrap();
        assert!(!captured.point.requests[0].is_qualified());
        assert!(!captured.point.requests[0].observation.statement.is_available());
        assert_eq!(WorkerStarvationEvidence::from_point(&captured.point).unwrap().assigned_workers, 1);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn permission_unknown_role_and_transport_generation_change_cannot_publish_a_worker_point() {
    let expected = expected_from_owner(3, false).await;
    let mut denied = replies(3);
    denied[3] = Ok(vec![replace(&row(3), &[("worker_visibility", Val::Int(0))])]);
    let mut reader = activity_reader(denied);
    assert!(reader.capture_worker_starvation(&expected).await.is_err());
    assert_eq!(reader.io.calls.len(), 4);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let mut reader = reader_with_io(FakeRead::new(replies(3)).with_generation(1).change_generation_at(4));
    assert!(reader.capture_worker_starvation(&expected).await.is_err());
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
#[test]
fn nullable_worker_endpoint_and_sessionless_wait_decode_do_not_borrow_a_request_or_caller() {
    let missing = replace(
        &row(3),
        &[
            ("worker_current_task_address", Val::Null),
            ("worker_created_ms_ticks", Val::Null),
            ("task_bound_ms_ticks", Val::Null),
            ("worker_state", Val::Null),
            ("is_preemptive", Val::Null),
        ],
    );
    let point = decode::row(&missing, 1).unwrap();
    assert!(point.task.unwrap().assigned_worker().is_none());
    let wait = replace(
        &row(3),
        &[
            ("session_id", Val::Null),
            ("request_id", Val::Null),
            ("worker_waiting_task_address", Val::Text("000000000000000A".into())),
            ("worker_wait_type", Val::Text("THREADPOOL".into())),
        ],
    );
    let decoded = decode::row(&wait, 2).unwrap();
    assert!(decoded.request.is_none());
    assert!(decoded.wait.unwrap().request.is_none());
}

#[tokio::test]
async fn unknown_role_unsupported_layout_and_native_denial_refuse_without_followup_or_zero_population() {
    let expected = expected_from_owner(3, false).await;
    let mut unknown = replies(3);
    unknown[2] = Ok(vec![replace(&crate::activity::database_context::tests::row(3), &[("updateability", Val::Null)])]);
    let mut reader = activity_reader(unknown);
    assert!(reader.capture_worker_starvation(&expected).await.is_err());
    assert_eq!(reader.query_count(), 3);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let mut malformed = replies(3);
    let missing = Rec::of(
        &row(3)
            .columns()
            .filter(|(name, _)| *name != "worker_state")
            .map(|(name, value)| (name, value.clone()))
            .collect::<Vec<_>>(),
    );
    malformed[3] = Ok(vec![missing]);
    let mut reader = activity_reader(malformed);
    assert!(reader.capture_worker_starvation(&expected).await.is_err());
    assert_eq!(reader.query_count(), 4);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let mut denied = replies(3);
    denied[3] = Err(ReadError::Source(culprit_core::Error::new(
        culprit_core::ErrorCode::PermissionDenied,
        "controlled native denial",
    )));
    let mut reader = activity_reader(denied);
    let error = reader.capture_worker_starvation(&expected).await.err().unwrap();
    assert_eq!(error.code, culprit_core::ErrorCode::PermissionDenied);
    assert_eq!(reader.query_count(), 4);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
