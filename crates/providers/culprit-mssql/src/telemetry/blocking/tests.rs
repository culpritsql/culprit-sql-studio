use super::*;
use crate::activity::blocking::tests::{focus, owner, reader, replies, unconfirmed_reader, waiter};
use crate::read::ReadSession;

#[test]
fn the_selected_schema_lock_envelope_pays_twelve_total_and_preserves_every_tighter_caller_limit() {
    let budget = bounded_budget(&CollectionBudget::default());
    assert_eq!(budget.max_queries, 12);
    assert_eq!(super::super::activity::bounded_budget(&CollectionBudget::default()).max_queries, 8);
    assert_eq!(ReadSession::budget_after_setup(budget.clone()).unwrap().max_queries, 11);
    assert_eq!(budget.statement_timeout_ms, 1_000);
    assert_eq!(budget.total_timeout_ms, 5_000);
    assert_eq!(budget.max_snapshot_rows, 500);
    let strict = CollectionBudget {
        max_queries: 4,
        max_snapshot_rows: 3,
        page_rows: 1,
        statement_timeout_ms: 200,
        total_timeout_ms: 400,
        ..Default::default()
    };
    assert_eq!(bounded_budget(&strict), strict);
    let recipes = crate::telemetry::recipes();
    let recipe = recipes.iter().find(|recipe| recipe.id == "mssql.blocking").unwrap();
    assert_eq!(recipe.selector, RecipeSelector::FocusedBlocking);
    assert_eq!(
        recipe.version, 3,
        "Root-owned revision: two selected native endpoint reads invalidate revision2 receipts"
    );
    assert_eq!(recipe.purpose, RecipePurpose::Investigation);
    assert_eq!(recipe.budget, bounded_budget(&CollectionBudget::upper_limits()));
    assert!(
        matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)),
        "source-validated built-in safety is distinct from measured-cost authority"
    );
    assert!(recipes.iter().all(|recipe| !matches!(recipe.admission, RecipeAdmission::Measured(_))));
}

#[tokio::test]
async fn a_tighter_total_caller_allowance_cannot_skip_the_last_native_authority_guard() {
    let original = focus().await;
    let values = replies(vec![waiter(), owner()], true, true);
    let budget = bounded_budget(&CollectionBudget { max_queries: 9, ..Default::default() });
    let reader_budget = ReadSession::budget_after_setup(budget.clone()).unwrap();
    let io = crate::resources::test_support::FakeRead::new(values).with_query_limit(reader_budget.max_queries);
    let mut current = ActivityReader::new(
        io,
        ReadOptions { budget, identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    let error = current.capture_focused_blocking(&original).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    assert_eq!(current.query_count(), 8);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn the_real_frozen_adapter_pages_once_and_reuses_its_completed_native_final_bound() {
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::FocusedBlocking { focus: Box::new(focus().await) },
        budget: bounded_budget(&CollectionBudget { page_rows: 1, ..Default::default() }),
    };
    let reader = reader(replies(vec![waiter(), owner()], true, true), request.budget.clone());
    let mut source = from_reader(reader, &request).unwrap();
    assert_eq!(source.query_count(), 0);
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    let captured_queries = source.query_count();
    assert_eq!(captured_queries, 9);
    let mut cursor = None;
    let mut observed = Vec::new();
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::ActivityObservations, cursor.as_deref()).await.unwrap()
        else {
            panic!()
        };
        assert!(records.len() <= 1);
        observed.extend(records);
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(observed.len(), 2);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), captured_queries, "paging/finalization cannot start a second native read");
    assert!(source.describe().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn incomplete_final_native_proof_and_unconfirmed_shutdown_never_publish_a_verified_capture() {
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::FocusedBlocking { focus: Box::new(focus().await) },
        budget: bounded_budget(&CollectionBudget::default()),
    };
    let mut values = replies(vec![waiter(), owner()], true, true);
    values[8] = Err(crate::read::ReadError::Stopped(CollectionStop::CancellationUnconfirmed));
    let reader = unconfirmed_reader(values, request.budget.clone());
    let mut source = from_reader(reader, &request).unwrap();
    assert_eq!(
        CollectionStop::from_error(&source.describe().await.err().unwrap()),
        CollectionStop::CancellationUnconfirmed
    );
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
}
