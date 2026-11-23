use super::*;
use crate::activity::current_request_work::tests as fixture;
use crate::activity::readable_secondary_recovery::tests::{focus_and_scope, replies};
use crate::resources::test_support::FakeRead;
use std::sync::atomic::Ordering;

async fn request() -> CollectionRequest {
    let (focus, execution) = focus_and_scope().await;
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::default()),
    }
}
#[tokio::test]
async fn frozen_redo_source_pages_and_finishes_the_exact_original_native_context_once() {
    let request = request().await;
    let io = fixture::io(replies());
    let trace = io.trace.clone();
    let mut source = with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    assert!(description.target.database().unwrap().incarnation.is_none());
    assert_eq!(
        description.observation.snapshot().unwrap().coverage_state().unwrap(),
        culprit_core::SourceState::Partial
    );
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 1);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 14, "the existing production owner separately pays SET");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn strict_caller_fourteen_cannot_skip_final_native_authority_and_the_recipe_stays_unqualified() {
    let mut request = request().await;
    request.budget.max_queries = 14;
    let reader_limit = ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
    assert_eq!(reader_limit, 13);
    let mut io = fixture::io(replies());
    io.inner = FakeRead::new(replies()).with_query_limit(reader_limit);
    let trace = io.trace.clone();
    let mut source = with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 13);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    let recipe = crate::telemetry::recipes()
        .into_iter()
        .find(|recipe| recipe.id == "mssql.readable-secondary-recovery")
        .unwrap();
    assert_eq!(recipe.admission, RecipeAdmission::Unqualified);
    assert_eq!(recipe.budget.max_queries, 15);
}
#[tokio::test]
async fn unknown_role_member_or_foreign_execution_scope_is_refused_before_reader_work() {
    for defect in ["role", "member", "execution"] {
        let mut request = request().await;
        let CollectionSelection::SourceRead { focus, execution } = &mut request.selection else { panic!() };
        let SourceReadPrerequisite::ObservedReadableSecondaryReader { request: original } = &mut focus.prerequisite
        else {
            panic!()
        };
        match defect {
            "role" => original.database_writable = None,
            "member" => original.partition = SourcePartition::Target,
            _ => {
                let SourceReadExecutionScope::Local(scope) = execution.as_mut() else { panic!() };
                scope.database_writable = true;
            }
        }
        let io = fixture::io(vec![]);
        let trace = io.trace.clone();
        assert!(with_io(io, &request, "fixture.invalid".into(), 1433).is_err(), "{defect}");
        assert!(trace.calls.lock().unwrap().is_empty());
        assert_eq!(trace.closes.load(Ordering::SeqCst), 0);
    }
}
