use super::*;
use crate::{
    activity::worker_starvation::tests::{context_from_owner, replies},
    resources::test_support::FakeRead,
};
use culprit_evidence::analytical::test_support::worker_starvation_basis;

/// Controlled original capture with an independently observed FakeRead owner. No native acceptance or cost claim.
fn request_in_context(context: NativeReadContext) -> CollectionRequest {
    let shared = matches!(context.target, CollectionTarget::Shared(_));
    let (mut source, mut records) = worker_starvation_basis(shared);
    source.target = context.target.clone();
    source.partition = context.partition.clone();
    if let SourceObservation::Activity(header) = &mut source.observation {
        header.at = context.observed_from.clone();
        header.observed_from = context.observed_from.clone();
        header.observed_until = context.observed_until.clone();
        header.database_writable = context.database_writable;
        header.epoch = context.partition.native_epoch().map(str::to_owned);
        // This new controlled original is built before its first canonical export; no historical row is
        // rebound. Its declared request-database label must match the independently read FakeRead owner.
        let TelemetryRecord::Activity(actor) = &mut records[0] else { panic!() };
        actor.database = context.target.database().map(|database| database.database.clone());
    } else if let SourceObservation::Snapshot(header) = &mut source.observation {
        header.observed_from = context.observed_from.clone();
        header.observed_until = context.observed_until.clone();
    } else {
        panic!("controlled worker original")
    }
    let capture = CaptureIdentity {
        observation_id: "controlled-worker-original".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-worker-outbox".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    assert!(source.partition_is_consistent());
    assert!(records[0].is_valid());
    let original = ExportedRecord::new(&source, &capture, records[0].clone()).unwrap();
    let basis =
        DiagnosticEvidenceRef::from_export(&source, &culprit_core::telemetry::digest(b"worker-original"), &original, 0)
            .unwrap();
    let focus = SourceReadFocus {
        basis: Box::new(basis),
        target: source.target.clone(),
        question: SourceReadQuestion::WorkerStarvation,
        prerequisite: SourceReadPrerequisite::ObservedWorkerQueue,
    };
    let execution = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    assert!(focus.matches_basis(&source, &original, &execution));
    CollectionRequest {
        subject: execution.subject(),
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::upper_limits()),
    }
}
pub(in crate::telemetry) async fn request(shared: bool) -> CollectionRequest {
    request_in_context(context_from_owner(3, shared).await)
}
pub(in crate::telemetry) async fn request_for_subject(subject: &CollectionSubject) -> CollectionRequest {
    let mut context = context_from_owner(3, matches!(subject, CollectionSubject::Shared { .. })).await;
    if let CollectionSubject::Shared { resource } = subject {
        // Constructor-only native-shaped ownership relation. No native session is opened by this fixture.
        context.target = CollectionTarget::Shared(resource.clone());
        let SourcePartition::InstanceLocal { member, .. } = &mut context.partition else { panic!() };
        *member = Some(resource.clone());
    }
    request_in_context(context)
}
async fn drain(source: &mut WorkerStarvationSource<FakeRead>) -> Vec<TelemetryRecord> {
    let CollectionRead::Page { records, next_cursor } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert!(next_cursor.is_none());
    records
}
#[tokio::test]
async fn one_setup_is_inside_fourteen_and_a_tighter_caller_cannot_spend_it_on_final_verification() {
    let request = request(false).await;
    assert!(RecipeSelector::WorkerStarvation.matches(&request));
    assert!(
        !RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::WorkerStarvation)).matches(&request)
    );
    assert_eq!(request.budget.max_queries, 14);
    assert_eq!(ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries, 13);
    for (total, verified) in [(14, true), (13, false)] {
        let mut request = request.clone();
        request.budget.max_queries = total;
        let reader = ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
        let io = FakeRead::new(replies(3)).with_generation(1).with_query_limit(reader);
        let mut source = WorkerStarvationSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
        let description = source.describe().await.unwrap();
        assert!(description.partition_is_consistent());
        assert_eq!(source.query_count(), 10);
        let rows = drain(&mut source).await;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_valid());
        let finish = source.finish().await;
        if verified {
            assert_eq!(finish.unwrap(), SourceCompletion::Verified);
            assert_eq!(source.query_count(), 13);
        } else {
            assert_eq!(CollectionStop::from_error(&finish.unwrap_err()), CollectionStop::QueryLimit);
            assert_eq!(source.query_count(), 12);
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn shared_question_retains_the_native_member_and_readonly_database_execution_remains_distinct() {
    for shared in [false, true] {
        let mut request = request(shared).await;
        if !shared {
            let CollectionSelection::SourceRead { execution, .. } = &mut request.selection else { panic!() };
            let SourceReadExecutionScope::Local(scope) = execution.as_mut() else { panic!() };
            scope.database_writable = false;
        }
        let mut native = replies(3);
        if !shared {
            for index in [2, 3, 9, 12] {
                for row in native[index].as_mut().unwrap() {
                    *row = crate::resources::test_support::replace(
                        row,
                        &[("updateability", crate::value::Val::Text("READ_ONLY".into()))],
                    );
                }
            }
        }
        let io = FakeRead::new(native).with_generation(1);
        let mut source = WorkerStarvationSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
        let description = source.describe().await.unwrap();
        let rows = drain(&mut source).await;
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::MssqlWorkerStarvation(point),
            ..
        }) = &rows[0]
        else {
            panic!()
        };
        assert_eq!(matches!(description.target, CollectionTarget::Shared(_)), shared);
        assert_eq!(point.context.database_writable, if shared { None } else { Some(false) });
        assert!(point.fits_source(&description));
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn construction_with_setup_only_refuses_without_native_open_and_broad_snapshot_cannot_activate_the_question() {
    let mut request = request(false).await;
    request.budget.max_queries = 1;
    let pool = Arc::new(Pool::new(tiberius::Config::new(), None));
    let Err(error) = WorkerStarvationSource::new(pool.clone(), &request, "fixture.invalid".into(), 1433) else {
        panic!()
    };
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    assert_eq!(pool.source_shutdown(), SourceShutdown::Quiescent);
    request.budget = CollectionBudget::default();
    request.selection = CollectionSelection::Snapshot { dataset: SnapshotDataset::WorkerStarvation };
    assert!(WorkerStarvationSource::new(pool.clone(), &request, "fixture.invalid".into(), 1433).is_err());
    assert_eq!(pool.source_shutdown(), SourceShutdown::Quiescent);
    let caller = CollectionBudget {
        max_queries: 12,
        total_timeout_ms: 2000,
        statement_timeout_ms: 300,
        total_bytes: 65536,
        page_bytes: 32768,
        ..Default::default()
    };
    let bounded = bounded_budget(&caller);
    assert_eq!(bounded.max_queries, 12);
    assert_eq!(bounded.total_timeout_ms, 2000);
    assert_eq!(bounded.statement_timeout_ms, 300);
    assert_eq!(bounded.total_bytes, 65536);
    assert_eq!(bounded.page_bytes, 32768);
}
#[tokio::test]
async fn cancelled_native_read_remains_owned_until_close_and_cannot_finish_a_public_answer() {
    let request = request(false).await;
    let io = FakeRead::new(replies(3)).with_generation(1).pause_on_query(4);
    let mut source = WorkerStarvationSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(10), source.describe()).await.is_err());
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn capped_native_rosters_stay_partial_and_azure_counts_include_the_same_final_owner_check() {
    let mut request = request(false).await;
    request.budget.max_snapshot_rows = 2;
    let mut native = replies(3);
    native[3] = Ok(vec![crate::activity::worker_starvation::tests::row(3); 2]);
    let mut source = WorkerStarvationSource::with_io(
        FakeRead::new(native).with_generation(1),
        &request,
        "fixture.invalid".into(),
        1433,
    )
    .unwrap();
    let description = source.describe().await.unwrap();
    assert_eq!(
        description.observation.snapshot().unwrap().coverage_state().unwrap(),
        culprit_core::SourceState::Partial
    );
    let records = drain(&mut source).await;
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlWorkerStarvation(point),
        ..
    }) = &records[0]
    else {
        panic!()
    };
    assert!(point.tasks_limited);
    assert_eq!(point.tasks.len(), 1);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    for shared in [false, true] {
        let request = request_in_context(context_from_owner(5, shared).await);
        let mut source = WorkerStarvationSource::with_io(
            FakeRead::new(replies(5)).with_generation(1),
            &request,
            "fixture.invalid".into(),
            1433,
        )
        .unwrap();
        source.describe().await.unwrap();
        assert_eq!(source.query_count(), 7);
        drain(&mut source).await;
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 8);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
