use super::*;
use crate::native::activity::transaction_metadata::tests::cache_row_at;
use crate::progress::test_support::{self as fixture, AT, BACKEND};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
const PROCESS: &str = "2026-11-14T00:00:00Z";
struct Scripted {
    native: fixture::Reader,
    generation: u64,
    change_after: Option<usize>,
    closes: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl QueryRead for Scripted {
    fn connection_generation(&self) -> Option<u64> {
        Some(self.generation)
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        let result = self.native.query(sql, limit).await;
        if self.change_after == Some(self.native.calls.len()) {
            self.generation += 1;
        }
        result
    }
}
#[async_trait::async_trait]
impl NativeRead for Scripted {
    fn queries(&self) -> u32 {
        self.native.queries()
    }
    fn metrics(&self) -> Option<ReadWorkMetrics> {
        self.native.metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.native.close().await
    }
}
fn scripted() -> Scripted {
    scripted_at(AT)
}
fn scripted_at(at: &str) -> Scripted {
    let coverage = serde_json::to_string(&native::cleanup::coverage(false)).unwrap();
    Scripted {
        native: fixture::Reader {
            replies: [
                Ok(vec![fixture::metadata_at(180_000, "f", PROCESS, true, at)]),
                Ok(vec![fixture::member_at("127.0.0.1", at)]),
                Ok(vec![cache_row_at(Some("0"), Some("false"), at)]),
                Ok(vec![cache_row_at(Some("3"), Some("true"), at)]),
                Ok(vec![fixture::row(&[
                    ("native_source", Some(RetentionCoverage::SOURCE)),
                    ("native_values", Some(&coverage)),
                ])]),
                Ok(vec![fixture::row(&[
                    ("pid", Some("42")),
                    ("query", Some("SELECT payload FROM orders WHERE tenant=5241")),
                    ("query_bytes", Some("42")),
                ])]),
                Ok(vec![cache_row_at(Some("0"), Some("false"), at)]),
                Ok(vec![fixture::metadata_at(180_000, "f", PROCESS, true, at)]),
                Ok(vec![fixture::member_at("127.0.0.1", at)]),
            ]
            .into(),
            ..Default::default()
        },
        generation: 1,
        change_after: None,
        closes: Arc::new(AtomicUsize::new(0)),
    }
}
async fn original_at(at: &str) -> (CollectionDescription, ActivityObservation) {
    let mut io = scripted_at(at);
    let before = metadata::activity(&mut io, "fixture.invalid", 9).await.unwrap();
    let context = Context::observe(&mut io, &before).await.unwrap().unwrap().read_context().unwrap();
    let record = native_activity::decode_row(cache_row_at(Some("0"), Some("false"), at), "16384").unwrap();
    let mut frame = native_activity::ActivityFrame { at: at.into(), records: vec![record], locks: vec![] };
    native_activity::transaction_metadata::classify(&mut frame);
    let original = frame.observations().pop().unwrap();
    let source = CollectionDescription {
        target: context.target.clone(),
        partition: context.partition.clone(),
        source: "postgres-activity".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: context.partition.native_epoch().map(str::to_owned),
            database_writable: context.database_writable,
            observed_from: context.observed_from.clone(),
            observed_until: context.observed_until.clone(),
            at: at.into(),
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ClientBackends,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected("postgres-activity".into(), 1),
        }),
    };
    (source, original)
}
async fn request() -> CollectionRequest {
    let (source, original) = original_at(AT).await;
    let context = NativeReadContext::from_activity(&source).unwrap();
    let parent = fixture::focus_from(&source, original.clone());
    let focus = SourceReadFocus {
        basis: Box::new(parent.parent),
        target: source.target.clone(),
        question: SourceReadQuestion::TransactionMetadata,
        prerequisite: SourceReadPrerequisite::ObservedTransactionMetadataWait {
            key: original.key,
            statement_identity: original.statement_identity.unwrap(),
            mechanism: TransactionMetadataWait::Subtransaction,
        },
    };
    let execution = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::default()),
    };
    request.validate().unwrap();
    request
}
fn frozen(io: Scripted, request: &CollectionRequest) -> FrozenSource<Reader<Scripted>> {
    FrozenSource::new(Reader { io, host: "fixture.invalid".into(), port: 9, verified: None, generation: None }, request)
        .unwrap()
}
#[tokio::test]
async fn held_source_keeps_real_native_flag_and_separate_shared_counters_without_paging_or_finish_reads() {
    let request = request().await;
    let io = scripted();
    let closes = io.closes.clone();
    let mut source = frozen(io, &request);
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    assert!(description.partition_is_consistent());
    assert_eq!(description.partition, SourcePartition::Target);
    assert!(description.observation.execution_context().unwrap().has_qualified_local_owner());
    assert_eq!(source.query_count(), 9, "the fake excludes only the separately paid real SET");
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    let [TelemetryRecord::Operational(row)] = records.as_slice() else { panic!() };
    let OperationalFact::PostgresTransactionMetadata(point) = &row.fact else { panic!() };
    assert_eq!(point.caches[0].backend_id, 7);
    assert_eq!(point.caches[0].cached_subxids, Some(3));
    assert_eq!(point.caches[0].overflowed, Some(true));
    assert_eq!(point.request_end, BlockingRequestState::SameStatement);
    assert_eq!(point.slrus.len(), 3);
    assert!(point.retention.holders.is_empty());
    assert!(point.fits_source_scope(&description));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 9);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 9);
}
#[tokio::test]
async fn missing_or_changed_original_request_opens_no_cache_retainer_or_sql_dispatch_and_never_proves_recovery() {
    let request = request().await;
    for missing in [false, true] {
        let mut io = scripted();
        io.native.replies[2] = if missing {
            Ok(vec![fixture::row(&[("observed_at", Some(AT)), ("pid", None)])])
        } else {
            Ok(vec![fixture::actor(BACKEND, "2026-11-15T09:30:00Z")])
        };
        let before = io.native.replies.pop_front().unwrap();
        let member = io.native.replies.pop_front().unwrap();
        let actor = io.native.replies.pop_front().unwrap();
        io.native.replies = [
            before,
            member,
            actor,
            Ok(vec![fixture::metadata_with("f", PROCESS)]),
            Ok(vec![fixture::member("127.0.0.1")]),
        ]
        .into();
        let mut source = frozen(io, &request);
        let description = source.describe().await.unwrap();
        assert_eq!(source.query_count(), 5);
        assert_eq!(description.observation.snapshot().unwrap().records, 0);
        let CollectionRead::Page { records, next_cursor: None } =
            source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
        else {
            panic!("the existing frozen source returns one final page")
        };
        assert!(records.is_empty());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 5);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn every_failed_native_phase_or_recycled_transport_stops_before_any_followup_and_retains_original_close() {
    let request = request().await;
    for index in 0..9 {
        for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
            let mut io = scripted();
            let closes = io.closes.clone();
            io.native.replies[index] = Err(stop.error());
            io.native.unconfirmed = stop == CollectionStop::CancellationUnconfirmed;
            let mut source = frozen(io, &request);
            assert!(source.describe().await.is_err(), "phase {index} stop {stop:?}");
            assert_eq!(source.query_count(), index as u32 + 1, "phase {index} stop {stop:?}");
            assert!(source.finish().await.is_err());
            assert_eq!(source.query_count(), index as u32 + 1);
            assert_eq!(
                source.close().await,
                if stop == CollectionStop::CancellationUnconfirmed {
                    SourceShutdown::Unconfirmed
                } else {
                    SourceShutdown::Quiescent
                }
            );
            assert_eq!(closes.load(Ordering::SeqCst), 1);
        }
    }
    for index in 1..=9 {
        let mut io = scripted();
        let closes = io.closes.clone();
        io.change_after = Some(index);
        let mut source = frozen(io, &request);
        assert!(source.describe().await.is_err(), "generation changes after dispatch {index}");
        assert_eq!(source.query_count(), index as u32);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn permission_version_original_owner_and_final_native_role_or_clock_cannot_borrow_an_available_point() {
    let request = request().await;
    for (row, expected) in [
        (fixture::metadata_on_version(150_000, "f", PROCESS), 1),
        (fixture::metadata_with_visibility(180_000, "f", PROCESS, false), 1),
    ] {
        let mut io = scripted();
        io.native.replies[0] = Ok(vec![row]);
        let mut source = frozen(io, &request);
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), expected);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut foreign = request.clone();
    let CollectionSelection::SourceRead { execution, .. } = &mut foreign.selection else { panic!() };
    let SourceReadExecutionScope::Local(scope) = execution.as_mut() else { panic!() };
    scope.database_writable = false;
    let mut source = frozen(scripted(), &foreign);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 2);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut io = scripted();
    io.native.replies[7] = Ok(vec![fixture::metadata_with("t", PROCESS)]);
    let mut source = frozen(io, &request);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 8);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut io = scripted();
    io.native.replies[8] = Ok(vec![fixture::member("127.0.0.2")]);
    let mut source = frozen(io, &request);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 9);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[test]
fn source_budget_intersects_every_existing_caller_ceiling_without_baseline_or_cost_promotion() {
    let requested = CollectionBudget {
        max_queries: 3,
        max_snapshot_rows: 1,
        page_rows: 1,
        statement_timeout_ms: 20,
        total_timeout_ms: 200,
        ..Default::default()
    };
    let bounded = bounded_budget(&requested);
    assert_eq!(bounded.max_queries, 3);
    assert_eq!(bounded.max_snapshot_rows, 1);
    assert_eq!(bounded.page_rows, 1);
    assert_eq!(bounded.statement_timeout_ms, 20);
    assert_eq!(bounded.total_timeout_ms, 200);
    let upper = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(upper.max_queries, 12);
    assert_eq!(upper.max_snapshot_rows, 32);
    assert_eq!(upper.statement_timeout_ms, 1000);
    assert_eq!(upper.total_timeout_ms, 5000);
    let recipes = super::super::recipes::declarations();
    let recipe = recipes.iter().find(|recipe| recipe.id == "postgres.transaction-metadata").unwrap();
    assert_eq!(recipe.purpose, RecipePurpose::Investigation);
    assert_eq!(recipe.version, 2);
    assert!(matches!(recipe.admission, RecipeAdmission::Unqualified));
    assert_eq!(recipe.selector, SourceReadQuestion::TransactionMetadata.selector());
}
#[test]
fn actual_concrete_source_futures_and_output_stay_bounded_without_polling_or_a_database() {
    use std::future::Future;
    fn sizes<A, F: Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (storage, output) = sizes(|(reader, request): (&'static mut ReadSession, &'static CollectionRequest)| {
        capture(reader, "fixture.invalid", 9, request)
    });
    assert!(storage <= 4096, "actual held capture future storage: {storage}");
    assert!(output <= 4096, "actual held capture output: {output}");
    let (storage, output) = sizes(
        |(reader, meta, request, key, statement): (
            &'static mut ReadSession,
            &'static Metadata,
            &'static CollectionRequest,
            &'static ActivityKey,
            &'static NativeIdentity,
        )| observe_point(reader, meta, request, key, statement, 42),
    );
    assert!(storage <= 4096, "actual native point future storage: {storage}");
    assert!(output <= 128, "actual native point output: {output}");
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::TransactionMetadata },
        budget: CollectionBudget::default(),
    };
    let mut io = scripted();
    let future = capture(&mut io, "fixture.invalid", 9, &request);
    assert!(std::mem::size_of_val(&future) <= 4096);
    drop(future);
    assert_eq!(io.queries(), 0, "constructing the actual source future performs no fake native work");
}

const FRESH_AT: &str = "2026-11-15T10:01:10.000003Z";
const SECOND_AT: &str = "2026-11-15T10:01:11.000003Z";
async fn continuation_request() -> CollectionRequest {
    let first_request = request().await;
    let first = capture(&mut scripted(), "fixture.invalid", 9, &first_request).await.unwrap();
    let capture_id = CaptureIdentity {
        observation_id: "controlled-first-native-point".into(),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"controlled-native-outbox"),
            owner_epoch: "1".into(),
            sequence: "2".into(),
        }),
    };
    let first_row = ExportedRecord::new(&first.description, &capture_id, first.records[0].clone()).unwrap();
    let previous = DiagnosticEvidenceRef::from_export(
        &first.description,
        &digest(b"controlled-first-native-manifest"),
        &first_row,
        0,
    )
    .unwrap();
    let (source, actor) = original_at(FRESH_AT).await;
    let request = fixture::focus_from(&source, actor);
    let focus = SourceReadFocus {
        basis: Box::new(request.parent.clone()),
        target: source.target.clone(),
        question: SourceReadQuestion::TransactionMetadata,
        prerequisite: SourceReadPrerequisite::ObservedTransactionMetadataContinuation {
            continuation: Box::new(TransactionMetadataContinuation {
                previous,
                request: Box::new(request),
                mechanism: TransactionMetadataWait::Subtransaction,
            }),
        },
    };
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &NativeReadContext::from_activity(&source).unwrap())
            .unwrap();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::default()),
    };
    request.validate().unwrap();
    request
}
#[tokio::test]
async fn continuation_uses_the_same_native_reader_query_envelope_and_completed_original_proof() {
    let request = continuation_request().await;
    let mut source = frozen(scripted_at(SECOND_AT), &request);
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 1);
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PostgresTransactionMetadata(point),
        ..
    }) = &records[0]
    else {
        panic!()
    };
    assert_eq!(point.observed_at, canonical_time(SECOND_AT).unwrap());
    assert_eq!(point.request_end, BlockingRequestState::SameStatement);
    assert_eq!(source.query_count(), 9, "the SAME fake reader excludes only the separately paid real SET");
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 9);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let connector = Connector::new(&culprit_core::ConnectionSpec {
        engine: Some(culprit_core::Engine::Postgres),
        host: "fixture.invalid".into(),
        port: Some(9),
        ..Default::default()
    })
    .unwrap();
    let recipes = super::super::recipes::declarations();
    let selected_recipes = recipes.iter().filter(|recipe| recipe.accepts(&request)).collect::<Vec<_>>();
    assert_eq!(selected_recipes.len(), 1);
    assert_eq!(selected_recipes[0].id, "postgres.transaction-metadata");
    assert_eq!(selected_recipes[0].version, 2);
    assert!(matches!(selected_recipes[0].admission, RecipeAdmission::Unqualified));
    let selected = super::super::source(connector, &request).unwrap();
    assert_eq!(selected.query_count(), 0, "the catalog routes the same closed constructor without native work");
}
#[tokio::test]
async fn continuation_refuses_older_native_clock_and_stale_wait_without_reading_unrelated_context() {
    let request = continuation_request().await;
    let mut old = frozen(scripted(), &request);
    let error = old.describe().await.unwrap_err();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::SourceChanged)));
    assert_eq!(old.query_count(), 2);
    assert_eq!(old.close().await, SourceShutdown::Quiescent);
    let mut io = scripted_at(SECOND_AT);
    let before = io.native.replies.pop_front().unwrap();
    let member = io.native.replies.pop_front().unwrap();
    io.native.replies = [
        before,
        member,
        Ok(vec![crate::native::activity::transaction_metadata::tests::cache_row_with_wait(
            Some("0"),
            Some("false"),
            SECOND_AT,
            None,
            None,
        )]),
        Ok(vec![fixture::metadata_at(180_000, "f", PROCESS, true, SECOND_AT)]),
        Ok(vec![fixture::member_at("127.0.0.1", SECOND_AT)]),
    ]
    .into();
    let mut ended = frozen(io, &request);
    let description = ended.describe().await.unwrap();
    assert_eq!(description.observation.snapshot().unwrap().records, 0);
    assert_eq!(
        ended.query_count(),
        5,
        "only original/role/member before and final guards; no SLRU/cache/retainer/SQL work"
    );
    let CollectionRead::Page { records, next_cursor: None } =
        ended.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!("the empty native source still requires its declared EOF page")
    };
    assert!(records.is_empty());
    assert_eq!(ended.query_count(), 5, "offline EOF consumption performs no replacement native work");
    assert_eq!(ended.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(ended.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn continuation_preserves_caller_query_limit_first_error_and_unconfirmed_shutdown_without_replacement_work() {
    let mut request = continuation_request().await;
    request.budget.max_queries = 8;
    let mut io = scripted_at(SECOND_AT);
    io.native.query_limit = Some(8);
    let mut source = frozen(io, &request);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 8);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 8);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let request = continuation_request().await;
    let mut io = scripted_at(SECOND_AT);
    io.native.replies[4] = Err(CollectionStop::CancellationUnconfirmed.error());
    io.native.unconfirmed = true;
    let mut source = frozen(io, &request);
    assert_eq!(
        source.describe().await.unwrap_err().detail,
        Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::CancellationUnconfirmed))
    );
    assert_eq!(source.query_count(), 5);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 5);
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
}
