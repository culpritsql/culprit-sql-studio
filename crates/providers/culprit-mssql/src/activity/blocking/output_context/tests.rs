use super::*;
use crate::{
    activity::{
        IdentityDetail,
        blocking::tests::{focus, reader, replies, waiter},
        page_server_reads::Availability,
        tests::metadata,
    },
    resources::test_support::replace,
    value::{Rec, Val},
};
fn native_owner() -> Rec {
    replace(
        &metadata(55),
        &[
            ("edition", Val::Int(3)),
            ("replica_id", Val::Null),
            ("blocking_session_id", Val::Int(0)),
            ("blocker_generation", Val::Null),
            ("wait_type", Val::Text("ASYNC_NETWORK_IO".into())),
            ("wait_ms", Val::Int(37)),
        ],
    )
}
fn raw(row: &Rec, name: &str) -> Val {
    row.columns().find(|(field, _)| *field == name).unwrap().1.clone()
}
fn native_waiter(count: Option<i64>) -> Rec {
    let owner = native_owner();
    let original = waiter();
    let mut columns = original.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    columns.extend([
        ("output_context_collected", Val::Int(1)),
        ("output_request_population_visible", Val::Int(1)),
        ("output_owner_current_requests", count.map_or(Val::Null, Val::Int)),
        ("output_owner_request_id", raw(&owner, "request_id")),
        ("output_owner_request_generation", raw(&owner, "request_generation")),
        ("output_owner_wait_type", raw(&owner, "wait_type")),
        ("output_owner_wait_ms", raw(&owner, "wait_ms")),
        ("output_owner_sql_handle", raw(&owner, "sql_handle")),
        ("output_owner_statement_start", raw(&owner, "statement_start")),
        ("output_owner_statement_end", raw(&owner, "statement_end")),
        ("output_owner_request_database_id", raw(&owner, "request_database_id")),
    ]);
    Rec::of(&columns)
}
#[tokio::test]
async fn one_batched_original_cohort_precedes_count_and_preserves_the_ordinary_metadata_projection() {
    let focus = focus().await;
    let (sql, cohort) = query(&focus, IdentityDetail::Endpoint, Availability::NotApplicable).unwrap();
    assert_eq!(sql.matches("JOIN sys.dm_exec_requests orr").count(), 1);
    assert_eq!(sql.matches("@output_cohort.nodes('/cohort/session')").count(), 1);
    let group = sql.split("FROM @output_cohort.nodes").nth(1).unwrap().split(") output_roster").next().unwrap();
    assert!(group.contains("os.session_id=oc.x.value('@id[1]','int')"));
    assert!(group.contains("os.login_time="));
    assert!(!group.contains("database_id=DB_ID()"));
    assert!(!group.contains("TOP"));
    assert!(!group.contains("OVER("));
    assert!(cohort.matches("<session ").count() <= MAX_COHORT_SESSIONS);
    assert!(cohort.contains(&format!("id=\"{}\"", focus.original.key.session.id)));
    for owner in focus.original.wait.iter().flat_map(|wait| &wait.owners).filter_map(ActivityWaitOwner::session) {
        assert!(cohort.contains(&format!("id=\"{}\"", owner.id)));
    }
    let ordinary = super::super::super::sql::metadata(IdentityDetail::Endpoint, Availability::NotApplicable);
    assert!(!ordinary.contains("output_cohort"));
    assert!(!ordinary.contains("output_owner_"));
}
#[tokio::test]
async fn the_same_focused_reader_retains_native_session_output_and_one_request_without_an_extra_dispatch() {
    let focus = focus().await;
    let mut reader =
        reader(replies(vec![native_waiter(Some(1)), native_owner()], true, true), CollectionBudget::default());
    let capture = reader.capture_focused_blocking(&focus).await.unwrap();
    assert_eq!(reader.query_count(), 9);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let rows = capture
        .records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(actor) = row else { panic!() };
            ActivityFrameRow { observation: actor, reference: None }
        })
        .collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&capture.description, &rows, true).unwrap();
    let proof = culprit_core::blocking::output_blocking(&frame).unwrap();
    assert_eq!(proof.status, culprit_core::blocking::OutputBlockingStatus::Observed);
    let [case] = proof.cases.as_slice() else { panic!() };
    assert_eq!(case.owner.current_requests.as_deref(), Some("1"));
    assert_eq!(case.owner.wait_ms.as_deref(), Some("37"));
    assert!(matches!(case.dependency, culprit_core::blocking::OutputBlockingDependency::NativeSession));
}
#[tokio::test]
async fn null_or_multiple_native_requests_keep_session_blocking_without_output_request_attribution() {
    for count in [None, Some(2)] {
        let focus = focus().await;
        let mut reader =
            reader(replies(vec![native_waiter(count), native_owner()], true, true), CollectionBudget::default());
        let capture = reader.capture_focused_blocking(&focus).await.unwrap();
        assert_eq!(reader.query_count(), 9);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        let rows = capture
            .records
            .iter()
            .map(|row| {
                let TelemetryRecord::Activity(actor) = row else { panic!() };
                ActivityFrameRow { observation: actor, reference: None }
            })
            .collect::<Vec<_>>();
        let frame = ActivityFrameEvidence::new(&capture.description, &rows, true).unwrap();
        let proof = culprit_core::blocking::output_blocking(&frame).unwrap();
        assert_ne!(proof.status, culprit_core::blocking::OutputBlockingStatus::Observed);
        assert!(proof.cases.is_empty());
        assert!(ActivityEvidence::blocking_for_frame(&frame).iter().any(|edge| edge.owner_observed));
    }
}

#[tokio::test]
async fn unsupported_or_null_complete_visibility_never_promotes_a_visible_single_output_row() {
    for visibility in [Val::Int(0), Val::Null] {
        let focus = focus().await;
        let row = replace(&native_waiter(Some(1)), &[("output_request_population_visible", visibility)]);
        let mut reader = reader(replies(vec![row, native_owner()], true, true), CollectionBudget::default());
        let captured = reader.capture_focused_blocking(&focus).await.unwrap();
        assert_eq!(reader.query_count(), 9);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        let rows = captured
            .records
            .iter()
            .map(|row| {
                let TelemetryRecord::Activity(actor) = row else { panic!() };
                ActivityFrameRow { observation: actor, reference: None }
            })
            .collect::<Vec<_>>();
        let frame = ActivityFrameEvidence::new(&captured.description, &rows, true).unwrap();
        assert!(culprit_core::blocking::output_blocking(&frame).unwrap().cases.is_empty());
    }
}
