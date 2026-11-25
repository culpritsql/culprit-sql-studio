use super::*;
use crate::activity::current_request_work::tests::{self as native, Io};
use crate::read::ReadError;
use crate::resources::test_support::{FakeRead, replace};
use crate::value::Val;
use culprit_core::{Error, ErrorCode};
use std::sync::atomic::Ordering;

async fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::CurrentRequestWork { focus: Box::new(native::focus().await) },
        budget: CollectionBudget { max_queries: 11, ..Default::default() },
    }
}
async fn drain(source: &mut Source<Io>) -> Vec<TelemetryRecord> {
    let CollectionRead::Page { records, next_cursor } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert!(next_cursor.is_none());
    records
}
#[tokio::test]
async fn actual_frozen_source_pages_and_finish_reuse_the_same_held_native_reader_on_writable_and_readable_roles() {
    for writable in [true, false] {
        let mut request = request().await;
        request.selection =
            CollectionSelection::CurrentRequestWork { focus: Box::new(native::focus_for_role(writable).await) };
        let mut replies = native::replies();
        for rows in replies.iter_mut().flatten() {
            for row in rows {
                *row = replace(
                    row,
                    &[("updateability", Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into()))],
                );
            }
        }
        let io = native::io(replies);
        let trace = io.trace.clone();
        let mut source = Source::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
        let description = source.describe().await.unwrap();
        assert!(description.matches_request(&request));
        assert_eq!(drain(&mut source).await.len(), 1);
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 10);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn strict_total_caller_cannot_spend_the_reserved_setup_slot_on_final_identity_work() {
    let mut request = request().await;
    request.budget.max_queries = 10;
    let limit = ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
    let mut io = native::io(native::replies());
    io.inner = FakeRead::new(native::replies()).with_query_limit(limit);
    let trace = io.trace.clone();
    let mut source = Source::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 9);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    let budget = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(
        (budget.max_queries, budget.max_snapshot_rows, budget.statement_timeout_ms, budget.total_timeout_ms),
        (11, 1, 1000, 5000)
    );
}
#[tokio::test]
async fn cancellation_failure_and_unconfirmed_close_keep_the_original_owner_and_dispatch_no_retry() {
    for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
        let request = request().await;
        let mut replies = native::replies();
        replies[4] = Err(ReadError::Stopped(stop));
        let io = native::io(replies);
        let trace = io.trace.clone();
        let mut source = Source::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stop);
        let count = source.query_count();
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    }
    let request = request().await;
    let mut io = native::io(native::replies());
    io.inner = FakeRead::new(native::replies()).pause_on_query(4);
    io.inner.shutdown = SourceShutdown::Unconfirmed;
    let trace = io.trace.clone();
    let mut source = Source::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(1), source.describe()).await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
    assert!(source.finish().await.is_err());
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn unknown_original_scope_or_role_refuses_before_reader_work_and_declared_recipe_stays_unqualified() {
    for unknown in ["role", "scope"] {
        let mut request = request().await;
        let CollectionSelection::CurrentRequestWork { focus } = &mut request.selection else { panic!() };
        if unknown == "role" {
            focus.database_writable = None;
        } else {
            focus.partition = SourcePartition::Target;
        }
        assert!(Source::with_io(native::io(vec![]), &request, "fixture.invalid".into(), 1433).is_err());
    }
    let recipe = crate::telemetry::recipes().into_iter().find(|r| r.id == "mssql.current-request-work").unwrap();
    assert!(matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)));
    assert_eq!(recipe.version, 2);
    assert_eq!(recipe.budget.max_queries, 11);
}

#[tokio::test]
async fn cache_native_error_stops_before_a_retired_owner_can_require_replacement_setup_or_publish() {
    for cache_endpoint in [4, 5] {
        for code in [ErrorCode::PermissionDenied, ErrorCode::QueryFailed, ErrorCode::Unsupported] {
            let request = request().await;
            let mut replies = native::replies();
            replies[cache_endpoint] = Err(ReadError::Source(Error::new(code, "controlled native cache failure")));
            let io = native::io(replies);
            let trace = io.trace.clone();
            let mut source = Source::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
            assert_eq!(source.describe().await.unwrap_err().code, code);
            assert!(trace.retired_after_error.load(Ordering::SeqCst));
            let expected_reads = cache_endpoint as u32 + 1;
            assert_eq!(source.query_count(), expected_reads);
            assert!(trace.calls.lock().unwrap().last().unwrap().contains("FROM sys.dm_exec_query_stats stats"));
            // No original source/rows/completion can be published from this failed description.
            assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
            assert!(source.finish().await.is_err());
            assert!(source.describe().await.is_err());
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
            assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
            assert_eq!(source.query_count(), expected_reads);
            assert_eq!(trace.calls.lock().unwrap().len(), expected_reads as usize);
            assert_eq!(trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        }
    }
    let recipe = crate::telemetry::recipes().into_iter().find(|r| r.id == "mssql.current-request-work").unwrap();
    assert!(matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)));
    assert_eq!(recipe.budget.max_queries, 11);
}
