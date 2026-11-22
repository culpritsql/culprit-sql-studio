//! Drive the actual shared native reader: no duplicate SQL builder, source or decoder.
use super::*;
use crate::telemetry::retained_definitions::{RetainedPlans, bounded_budget};
use culprit_evidence::analytical::test_support::{TvpLoopCase, relational_input_question_fixture_for_owner};

fn authority(at: &str, writable: bool) -> Rec {
    replace(
        &crate::activity::database_context::tests::row(3),
        &[
            ("observed_at", Val::Text(at.into())),
            ("selected_database_id", Val::Int(7)),
            ("catalog_database_id", Val::Int(7)),
            ("updateability", Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into())),
        ],
    )
}
#[derive(Clone, Copy)]
enum Failure {
    None,
    ForeignPlan,
    Cancelled,
}
async fn fixture(
    total: u32,
    failure: Failure,
) -> (StreamedSnapshot<RetainedPlans, StreamingRead>, CollectionRequest, CachedStatementContext) {
    let before = authority("2020-01-01T12:00:03Z", true);
    let metadata = crate::instance::test_support::metadata();
    let epoch = crate::instance::test_support::epoch(100);
    let mut probe = FakeRead::new(vec![Ok(vec![metadata.clone()]), Ok(vec![epoch.clone()]), Ok(vec![before.clone()])]);
    let owner = crate::local_source::Context::read(&mut probe, "fixture.invalid", 1433).await.unwrap();
    let owner = owner.completed(&owner).unwrap();
    let original = relational_input_question_fixture_for_owner(TvpLoopCase::RepeatedAccess, &owner);
    let SourceReadPrerequisite::ObservedCurrentRequestPlan { context, .. } = &original.focus.prerequisite else {
        panic!()
    };
    let selected = context.as_ref().clone();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead {
            focus: Box::new(original.focus),
            execution: Box::new(
                SourceReadExecutionScope::from_context(SourceReadQuestion::RelationalInputContext, &owner).unwrap(),
            ),
        },
        budget: bounded_budget(&CollectionBudget { max_queries: total, ..CollectionBudget::default() }),
    };
    let mut row = replace(
        &super::connected_cache::frame_row(SnapshotDataset::RetainedPlans, true),
        &[
            ("database_id", Val::Int(7)),
            ("plan_handle", Val::Text(selected.identity.plan_handle.clone())),
            ("sql_handle", Val::Text(selected.identity.sql_handle.clone())),
            ("statement_start_offset", Val::Int(selected.identity.statement_start_bytes.into())),
            ("statement_end_offset", Val::Int(selected.identity.statement_end_bytes.into())),
            ("creation_time", Val::Text(selected.identity.created_without_offset.clone())),
            ("plan_generation_num", Val::Int(selected.identity.plan_generation.parse().unwrap())),
            ("query_hash", Val::Text(selected.query_hash.clone().unwrap())),
        ],
    );
    let expected = crate::cached_statements::context(&row).unwrap();
    let replies = vec![
        Ok(vec![metadata.clone()]),
        Ok(vec![epoch.clone()]),
        Ok(vec![before]),
        Ok(vec![crate::resources::test_support::instance_pressure_row()]),
        Ok(vec![crate::retained_definitions::tests::feature(1, 1)]),
        Ok(vec![Rec::of(&[("value", Val::Int(1))])]),
        Ok(vec![crate::resources::test_support::instance_pressure_row()]),
        Ok(vec![crate::relational_inputs::tests::binding_row(&expected)]),
        Ok(vec![metadata]),
        Ok(vec![epoch]),
        Ok(vec![authority("2020-01-01T12:00:04Z", true)]),
    ];
    let reader_queries = crate::read::ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
    if matches!(failure, Failure::ForeignPlan) {
        row = replace(&row, &[("plan_handle", Val::Text("0xDEAD".into()))]);
    }
    let mut native = FakeRead::new(replies).with_query_limit(reader_queries);
    if matches!(failure, Failure::Cancelled) {
        native.shutdown = SourceShutdown::Unconfirmed;
    }
    let io = StreamingRead {
        native,
        batches: vec![batch(row, true)].into(),
        starts: 0,
        pause_batches: matches!(failure, Failure::Cancelled),
    };
    (StreamedSnapshot::with_io(io, &request, "fixture.invalid".into(), 1433, RetainedPlans).unwrap(), request, selected)
}
async fn consume(source: &mut StreamedSnapshot<RetainedPlans, StreamingRead>) -> Result<Vec<TelemetryRecord>> {
    let CollectionRead::Page { records, next_cursor } =
        source.read_page(TelemetryStream::ObjectDefinitions, None).await?
    else {
        panic!()
    };
    assert!(next_cursor.is_none());
    Ok(records)
}
#[tokio::test]
async fn selected_original_uses_one_native_payload_and_binding_reader_with_exact_parameters_and_no_azure_shortcut() {
    let (mut source, request, selected) = fixture(13, Failure::None).await;
    let initial = source.describe().await.unwrap();
    assert!(initial.matches_request(&request));
    let records = consume(&mut source).await.unwrap();
    assert_eq!(records.len(), 1);
    let completion = source.finish().await.expect("selected retained source final native proof");
    let completed = initial.completed(completion).expect("selected retained completed description").unwrap();
    assert!(matches!(completed.observation, SourceObservation::SourceRead { .. }));
    assert_eq!(source.query_count(), 12);
    assert_eq!(source.inner.reader().io.starts, 1);
    let calls = &source.inner.reader().io.native.calls;
    let payloads = calls.iter().filter(|call| call.sql.contains("SELECT TOP (@P1) qs.plan_handle")).collect::<Vec<_>>();
    let [payload] = payloads.as_slice() else { panic!() };
    assert!(matches!(&payload.parameters[1],Parameter::Text(value) if value == &selected.identity.plan_handle));
    assert!(matches!(&payload.parameters[2],Parameter::Text(value) if value == &selected.identity.sql_handle));
    assert!(
        matches!(&payload.parameters[3],Parameter::Integer(value) if *value == i64::from(selected.identity.statement_start_bytes))
    );
    assert!(
        matches!(&payload.parameters[4],Parameter::Integer(value) if *value == i64::from(selected.identity.statement_end_bytes))
    );
    assert_eq!(calls.iter().filter(|call| call.sql == crate::relational_inputs::binding_check_sql()).count(), 1);
    for call in calls {
        assert!(!call.sql.contains("sys.query_store"));
        let without_profile_availability = call.sql.replace("OBJECT_ID(N'sys.dm_exec_query_profiles')", "NULL");
        assert!(
            !without_profile_availability.contains("dm_exec_query_profiles"),
            "the existing layout probe may check availability but must not read operator profiles"
        );
    }
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn total_thirteen_pays_setup_once_and_twelve_cannot_skip_final_native_identity() {
    let (mut source, request, _) = fixture(12, Failure::None).await;
    assert_eq!(request.budget.max_queries, 12);
    source.describe().await.unwrap();
    consume(&mut source).await.unwrap();
    let error = source.finish().await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 11);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 11);
    let ceiling = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(ceiling.max_queries, 13);
}
#[tokio::test]
async fn foreign_native_locator_or_cancelled_stream_never_retries_or_lends_final_authority() {
    let (mut source, _, _) = fixture(13, Failure::ForeignPlan).await;
    source.describe().await.unwrap();
    assert_eq!(CollectionStop::from_error(&consume(&mut source).await.unwrap_err()), CollectionStop::SourceChanged);
    let calls = source.query_count();
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), calls);
    let (mut source, _, _) = fixture(13, Failure::Cancelled).await;
    source.describe().await.unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(1), consume(&mut source)).await.is_err());
    let calls = source.query_count();
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), calls);
}
