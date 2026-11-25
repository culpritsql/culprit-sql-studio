use super::*;
use crate::{
    activity::{
        ReadOptions,
        memory::{
            focus::tests::{original_focus, replies},
            tests::{context, reader_with_identity, row},
        },
    },
    read::ReadError,
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};
fn output_request() -> CollectionRequest {
    let mut native = original_focus();
    native.original.wait.as_mut().unwrap().class = None;
    native.original.wait.as_mut().unwrap().native_type = Some("ASYNC_NETWORK_IO".into());
    let original = CollectionDescription {
        target: native.target.clone(),
        partition: native.partition.clone(),
        source: native.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            database_writable: native.database_writable,
            epoch: Some(native.epoch.clone()),
            at: native.original_observed_at.clone(),
            observed_from: native.original_observed_at.clone(),
            observed_until: native.original_observed_at.clone(),
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            scope: culprit_core::MeasurementScope::Database,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(native.source.clone(), 1),
        }),
    };
    let exported = ExportedRecord::new(&original, &native.parent.capture, native.original.clone().into()).unwrap();
    native.parent =
        DiagnosticEvidenceRef::from_export(&original, &digest(b"native-output-original-manifest"), &exported, 0)
            .unwrap();
    assert!(native.matches_parent(&original, &native.original, &native.parent));
    let execution = SourceReadExecutionScope::from_context(
        SourceReadQuestion::OutputGrantRetention,
        &NativeReadContext::from_activity(&original).unwrap(),
    )
    .unwrap();
    let focus = SourceReadFocus {
        basis: Box::new(native.parent.clone()),
        target: native.target.clone(),
        question: SourceReadQuestion::OutputGrantRetention,
        prerequisite: SourceReadPrerequisite::ObservedClientOutputWait { request: Box::new(native) },
    };
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::default()),
    };
    request.validate().unwrap();
    request
}
fn output() -> Rec {
    replace(&row(), &[("wait_type", Val::Text("ASYNC_NETWORK_IO".into())), ("blocking_session_id", Val::Int(0))])
}
fn source(request: &CollectionRequest, rows: Vec<crate::read::ReadResult<Vec<Rec>>>) -> MemorySource<FakeRead> {
    MemorySource::from_reader(reader_with_identity(rows, IdentityDetail::AzureDatabase), request).unwrap()
}
#[tokio::test]
async fn output_question_reuses_exact_native_memory_rows_queries_scopes_and_original_close_without_a_second_census() {
    let request = output_request();
    let mut source = source(&request, replies(vec![output()], true));
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    assert_eq!(description.partition, SourcePartition::Target);
    let SourceObservation::SourceRead { execution, snapshot, .. } = &description.observation else { panic!() };
    assert!(execution.has_qualified_local_owner());
    assert_eq!(snapshot.records, 1);
    assert_eq!(source.query_count(), 5);
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    let exported = ExportedRecord::new(
        &description,
        &CaptureIdentity { observation_id: "output-source".into(), revision: None },
        records[0].clone(),
    )
    .unwrap();
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::ExecutionGrant(grant), .. }) =
        &exported.record
    else {
        panic!()
    };
    assert_eq!(grant.request.as_ref().unwrap().wait.as_ref().unwrap().native_type.as_deref(), Some("ASYNC_NETWORK_IO"));
    assert_eq!(grant.granted_kibibytes.as_deref(), Some("65536"));
    assert!(grant.request.as_ref().unwrap().client.is_some());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(
        source.query_count(),
        6,
        "same six Azure reads as existing focused memory, plus real SET when using ReadSession"
    );
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 6);
}
#[tokio::test]
async fn foreign_execution_stops_before_layout_or_grants_and_a_final_clock_or_member_change_cannot_verify() {
    let mut request = output_request();
    let CollectionSelection::SourceRead { execution, .. } = &mut request.selection else { panic!() };
    let SourceReadExecutionScope::Local(scope) = execution.as_mut() else { panic!() };
    scope.database_writable = false;
    let mut wrong = source(&request, replies(vec![output()], true));
    assert!(wrong.describe().await.is_err());
    assert_eq!(wrong.query_count(), 2);
    assert_eq!(wrong.close().await, SourceShutdown::Quiescent);
    for (change, expected_stop) in [
        (("physical_incarnation", Val::Text("another-member".into())), None),
        (("observed_at", Val::Text("2019-01-01T06:00:00Z".into())), Some(CollectionStop::InvalidPage)),
    ] {
        let request = output_request();
        let mut native = replies(vec![output()], true);
        *native.last_mut().unwrap() = Ok(vec![replace(&context(), &[change])]);
        let mut changed = source(&request, native);
        changed.describe().await.unwrap();
        changed.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
        let completion = changed.finish().await;
        match expected_stop {
            Some(stop) => assert_eq!(CollectionStop::from_error(&completion.unwrap_err()), stop),
            None => assert_eq!(completion.unwrap(), SourceCompletion::Changed),
        }
        assert_eq!(changed.query_count(), 6);
        assert_eq!(changed.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn every_failed_native_phase_stops_followups_and_preserves_unconfirmed_retirement_without_a_reconnect() {
    for index in 0..6 {
        for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
            let request = output_request();
            let mut native = replies(vec![output()], true);
            native[index] = Err(ReadError::Stopped(stop));
            let mut io = FakeRead::new(native);
            if stop == CollectionStop::CancellationUnconfirmed {
                io.shutdown = SourceShutdown::Unconfirmed;
            }
            let reader = ActivityReader::new(
                io,
                ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::AzureDatabase },
                "fixture.invalid".into(),
                1433,
            );
            let mut source = MemorySource::from_reader(reader, &request).unwrap();
            let result = source.describe().await;
            if index == 5 {
                result.unwrap();
                source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
                assert!(source.finish().await.is_err());
            } else {
                assert!(result.is_err());
            }
            assert_eq!(source.query_count(), index as u32 + 1, "phase {index} {stop:?}");
            assert_eq!(
                source.close().await,
                if stop == CollectionStop::CancellationUnconfirmed {
                    SourceShutdown::Unconfirmed
                } else {
                    SourceShutdown::Quiescent
                }
            );
            assert!(source.describe().await.is_err());
            assert_eq!(source.query_count(), index as u32 + 1);
        }
    }
}
#[tokio::test]
async fn changed_request_or_a_missing_subject_has_no_detail_read_and_scalar_long_running_cannot_construct_the_question()
{
    let request = output_request();
    let changed = replace(&output(), &[("request_generation", Val::Text("2020-01-01T07:58:02".into()))]);
    let mut source = source(&request, replies(vec![changed], false));
    let description = source.describe().await.unwrap();
    assert_eq!(description.observation.snapshot().unwrap().records, 0);
    assert!(
        matches!(source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap(), CollectionRead::Page { records, next_cursor: None } if records.is_empty())
    );
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 5);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut scalar = request;
    let CollectionSelection::SourceRead { focus, .. } = &mut scalar.selection else { panic!() };
    let SourceReadPrerequisite::ObservedClientOutputWait { request } = &mut focus.prerequisite else { panic!() };
    request.original.wait = None;
    request.original.elapsed_ms = Some("999999999".into());
    assert!(MemorySource::from_reader(reader_with_identity(vec![], IdentityDetail::AzureDatabase), &scalar).is_err());
}

#[tokio::test]
async fn native_partial_coverage_caller_caps_and_unqualified_recipe_survive_the_same_reader_path() {
    let request = output_request();
    let foreign = replace(
        &output(),
        &[("session_id", Val::Int(43)), ("grant_session_id", Val::Int(43)), ("grant_sql_handle", Val::Null)],
    );
    let mut source = source(&request, replies(vec![output(), foreign], true));
    let description = source.describe().await.unwrap();
    assert_eq!(
        description.observation.snapshot().unwrap().coverage_state().unwrap(),
        culprit_core::SourceState::Partial
    );
    let CollectionRead::Page { records, .. } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 1);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 6);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let caller = CollectionBudget {
        max_queries: 2,
        max_snapshot_rows: 1,
        page_rows: 1,
        statement_timeout_ms: 23,
        total_timeout_ms: 230,
        ..Default::default()
    };
    let actual = bounded_budget(&caller);
    assert!(actual.max_queries <= 2);
    assert_eq!(actual.max_snapshot_rows, 1);
    assert_eq!(actual.statement_timeout_ms, 23);
    assert_eq!(actual.total_timeout_ms, 230);
    let recipe = super::super::recipes::declarations()
        .into_iter()
        .find(|recipe| recipe.id == "mssql.current-request-memory")
        .unwrap();
    assert_eq!(recipe.version, 7);
    assert!(matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)));
    assert_eq!(recipe.selector, SourceReadQuestion::OutputGrantRetention.selector());
}
