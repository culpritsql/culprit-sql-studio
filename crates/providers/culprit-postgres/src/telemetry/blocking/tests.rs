use super::*;
use crate::progress::test_support::{self as fixture, AT, BACKEND, REQUEST, Reader as NativeReader};
use crate::rows::Row;
use culprit_core::Provider;
use std::collections::VecDeque;

mod maintenance;

const PROCESS: &str = "2026-11-14T00:00:00Z";

fn actor_row(pid: &str, state: &str, database_oid: &str, database: &str, waiters: Option<&str>) -> Row {
    fixture::row(&[
        ("observed_at", Some(AT)),
        ("pid", Some(pid)),
        ("backend_start", Some(BACKEND)),
        ("backend_type", Some("client backend")),
        ("query_start", Some(REQUEST)),
        ("transaction_start", Some(REQUEST)),
        ("database_oid", Some(database_oid)),
        ("database", Some(database)),
        ("state", Some(state)),
        ("elapsed_ms", Some("60000")),
        ("transaction_ms", Some("60000")),
        ("wait_event_type", (pid == "42").then_some("Lock")),
        ("wait_event", (pid == "42").then_some("relation")),
        ("blocking_waiters", waiters),
        ("application_name", Some("native fixture client")),
    ])
}
fn waiter() -> Row {
    actor_row("42", "active", "16384", "shop", None)
}
fn owner() -> Row {
    actor_row("43", "idle in transaction", "16384", "shop", Some("[42]"))
}
fn edge(ids: &str) -> Row {
    fixture::row(&[("pid", Some("42")), ("owners", Some(ids))])
}
fn lock() -> Row {
    lock_point("relation", "AccessExclusiveLock", &[("0", false), ("43", true)])
}
fn lock_point(locktype: &str, mode: &str, owners: &[(&str, bool)]) -> Row {
    let relation = locktype == "relation";
    let matched_owners = owners.iter().filter(|(_, matched)| *matched).count();
    let snapshot_rows = (matched_owners + 1).to_string();
    let blockers = owners
        .iter()
        .map(|(pid, matched)| {
            serde_json::json!({
                "pid": pid,
                "actor_matches": matched,
                "single_backend": matched,
                "active": false,
                "wait_event_type": null,
                "wait_event": null,
                "granted": if relation && *matched { vec!["AccessShareLock"] } else { vec![] },
                "waiting": Vec::<String>::new(),
            })
        })
        .collect::<Vec<_>>();
    let blockers = serde_json::to_string(&blockers).unwrap();
    fixture::row(&[
        ("pid", Some("42")),
        ("observed_at", Some(AT)),
        ("waiter_matches", Some("true")),
        ("single_backend", Some("true")),
        ("actor_rows", Some(&snapshot_rows)),
        ("snapshot_rows", Some(&snapshot_rows)),
        ("blockers_complete", Some("true")),
        ("mode", Some(mode)),
        ("locktype", Some(locktype)),
        ("database_oid", relation.then_some("16384")),
        ("relation_oid", relation.then_some("16420")),
        ("object", relation.then_some("public.orders")),
        ("index_name", None),
        ("elapsed_ms", Some(if relation { "60000" } else { "1" })),
        ("owners", Some(&blockers)),
    ])
}
fn sql() -> Row {
    fixture::row(&[("pid", Some("42")), ("query", Some("SELECT id FROM orders")), ("query_bytes", Some("21"))])
}
fn request(focus: ActivityRequestFocus) -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::FocusedBlocking { focus: Box::new(focus) },
        budget: bounded_budget(&CollectionBudget::default()),
    }
}
fn focus() -> ActivityRequestFocus {
    let original = native_activity::decode_row(waiter(), "16384").unwrap().facts.unwrap();
    fixture::focus_from(&fixture::original_source(), original)
}
async fn qualified_focus() -> ActivityRequestFocus {
    let mut reader = NativeReader {
        replies: [Ok(vec![fixture::metadata_with("f", PROCESS)]), Ok(vec![fixture::member("127.0.0.1")])].into(),
        ..Default::default()
    };
    let before = metadata::activity(&mut reader, "fixture.invalid", 9).await.unwrap();
    let context = Context::observe(&mut reader, &before).await.unwrap().unwrap().read_context().unwrap();
    let mut source = fixture::original_source();
    source.partition = context.partition;
    let SourceObservation::Activity(snapshot) = &mut source.observation else { unreachable!() };
    snapshot.epoch = Some(before.activity_epoch());
    snapshot.database_writable = context.database_writable;
    snapshot.observed_from = context.observed_from;
    snapshot.observed_until = context.observed_until;
    snapshot.at = AT.into();
    snapshot.coverage = StateSourceCoverage::observed(source.source.clone(), 1, false);
    let original = native_activity::decode_row(waiter(), "16384").unwrap().facts.unwrap();
    fixture::focus_from(&source, original)
}
fn scripted() -> NativeReader {
    NativeReader {
        replies: VecDeque::from([
            Ok(vec![fixture::metadata_with("f", PROCESS)]),
            Ok(vec![fixture::member("127.0.0.1")]),
            Ok(vec![waiter()]),
            Ok(vec![edge("[0,43]")]),
            Ok(vec![owner()]),
            Ok(vec![lock()]),
            Ok(vec![sql()]),
            Ok(vec![waiter()]),
            Ok(vec![fixture::metadata_with("f", PROCESS)]),
            Ok(vec![fixture::member("127.0.0.1")]),
        ]),
        ..Default::default()
    }
}
fn source(io: NativeReader, request: &CollectionRequest) -> FrozenSource<Reader<NativeReader>> {
    FrozenSource::new(Reader { io, host: "fixture.invalid".into(), port: 9, verified: None, profile: None }, request)
        .unwrap()
}

#[tokio::test]
async fn original_native_member_and_request_bind_all_owner_roles_through_one_frozen_capture() {
    let request = request(qualified_focus().await);
    let mut source = source(scripted(), &request);
    let description = source.describe().await.unwrap();
    let SourceObservation::FocusedBlocking { result, snapshot } = &description.observation else { unreachable!() };
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(result.request_end, BlockingRequestState::SameStatement);
    assert_eq!(snapshot.observations, 2);
    assert!(!snapshot.coverage.complete_scope);
    let native_queries = source.query_count();
    assert_eq!(native_queries, 10);
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::ActivityObservations, None).await.unwrap()
    else {
        panic!()
    };
    let observations = records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(row) = row else { panic!() };
            row.as_ref().clone()
        })
        .collect::<Vec<_>>();
    let edges = ActivityEvidence::blocking_observations(&observations).unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().any(|edge| matches!(
        edge.owner,
        ActivityWaitOwner::Transaction { owner: TransactionWaitOwner::Prepared, .. }
    ) && !edge.owner_observed));
    assert!(edges.iter().any(|edge| edge.owner_observed));
    let native_lock = observations[0].wait.as_ref().unwrap().resources.as_ref().unwrap()[0].native.as_deref();
    let Some(culprit_core::blocking::NativeLockEvidence::Postgres(point)) = native_lock else {
        panic!("original focused native lock point")
    };
    assert_eq!(point.resource.as_ref().unwrap().relation_oid, "16420");
    assert!(point.blockers.iter().any(|owner| owner.native_pid == "0" && owner.actor.is_none()));
    assert!(point.blockers.iter().any(|owner| owner.native_pid == "43" && owner.actor.is_some()));
    let idle_owner = point.blockers.iter().find(|owner| owner.native_pid == "43").unwrap();
    let context = idle_owner.output_context.as_ref().unwrap();
    assert_eq!(context.actor.as_ref(), Some(&observations[1].key));
    assert_eq!(context.current_requests.as_deref(), Some("0"));
    assert!(context.native_wait.is_none(), "a native NULL wait does not become client-output pressure");
    assert!(observations[1].idle && observations[1].statement_identity.is_none());
    assert!(
        matches!(observations[1].statement, DefinitionPayload::Unavailable { .. }),
        "an idle lock holder's previous SQL is not a statement-acquisition claim"
    );
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), native_queries, "frozen paging and finish cannot read the database again");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), native_queries, "a closed source cannot reopen native work");
}

#[tokio::test]
async fn a_real_qualified_current_capture_can_be_an_explicit_later_parent_without_upgrading_the_original() {
    let original = focus();
    let captured = capture(&mut scripted(), "fixture.invalid", 9, &request(original.clone())).await.unwrap();
    let SourceObservation::FocusedBlocking { result, .. } = &captured.description.observation else { unreachable!() };
    assert_eq!(result.binding, FocusedRequestBinding::UnverifiedOriginalScope);
    assert_eq!(result.focus.partition, SourcePartition::Target);
    let TelemetryRecord::Activity(row) = &captured.records[0] else { panic!() };
    let current = fixture::focus_from(&captured.description, row.as_ref().clone());
    assert!(current.matches_parent(&captured.description, row, &current.parent));
    let later = capture(&mut scripted(), "fixture.invalid", 9, &request(current)).await.unwrap();
    let SourceObservation::FocusedBlocking { result, .. } = &later.description.observation else { unreachable!() };
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(
        original.partition,
        SourcePartition::Target,
        "there is no production loop or retrospective source repair"
    );
}

#[tokio::test]
async fn request_reuse_missing_participants_and_native_scope_changes_never_borrow_sql_or_owner() {
    let focus = qualified_focus().await;
    for changed in
        [fixture::actor(BACKEND, "2026-11-15T09:30:00Z"), fixture::row(&[("observed_at", Some(AT)), ("pid", None)])]
    {
        let mut reader = scripted();
        reader.replies = [
            reader.replies.pop_front().unwrap(),
            reader.replies.pop_front().unwrap(),
            Ok(vec![changed]),
            Ok(vec![fixture::metadata_with("f", PROCESS)]),
            Ok(vec![fixture::member("127.0.0.1")]),
        ]
        .into();
        let captured = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.unwrap();
        let SourceObservation::FocusedBlocking { result, .. } = &captured.description.observation else {
            unreachable!()
        };
        assert!(matches!(
            result.binding,
            FocusedRequestBinding::ParticipantMissing | FocusedRequestBinding::ParticipantChanged
        ));
        assert!(captured.records.is_empty());
        assert!(reader.calls.iter().all(|sql| !sql.contains("pg_blocking_pids") && !sql.contains("AS query,")));
    }
    for field in ["member", "process", "role"] {
        let mut reader = scripted();
        match field {
            "member" => reader.replies[1] = Ok(vec![fixture::member("127.0.0.2")]),
            "process" => reader.replies[0] = Ok(vec![fixture::metadata_with("f", "2026-11-14T01:00:00Z")]),
            _ => reader.replies[0] = Ok(vec![fixture::metadata_with("t", PROCESS)]),
        }
        let error = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceChanged, "{field}");
        assert_eq!(reader.calls.len(), 2, "scope refusal precedes actor/payload and blocker work");
    }
}

#[tokio::test]
async fn missing_selected_visibility_or_initial_native_owner_stops_before_optional_blocker_and_sql_reads() {
    let focus = qualified_focus().await;
    for missing in ["actor", "member"] {
        let mut reader = scripted();
        if missing == "actor" {
            reader.replies[2] = Ok(vec![fixture::row(&[
                ("observed_at", Some(AT)),
                ("pid", Some("42")),
                ("database_oid", Some("16384")),
                ("database", Some("shop")),
                // The real projection coalesces hidden a.state to a.backend_type, never SQL NULL.
                ("state", Some("client backend")),
                ("backend_start", None),
            ])]);
        } else {
            reader.replies[1] =
                Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
        }
        let error = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.err().unwrap();
        if missing == "actor" {
            assert_eq!(error.code, ErrorCode::PermissionDenied);
            assert_eq!(reader.calls.len(), 3);
        } else {
            assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceUnavailable);
            assert_eq!(reader.calls.len(), 2);
        }
        assert!(reader.calls.iter().all(|sql| !sql.contains("pg_blocking_pids") && !sql.contains("AS query,")));
    }
}

#[tokio::test]
async fn final_statistics_visibility_loss_is_explicit_in_the_retained_focused_report() {
    let mut reader = scripted();
    reader.replies[8] = Ok(vec![fixture::metadata_with_visibility(180_000, "f", PROCESS, false)]);
    let captured = capture(&mut reader, "fixture.invalid", 9, &request(qualified_focus().await)).await.unwrap();
    let rows = captured
        .records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(observation) = row else { panic!() };
            ActivityFrameRow { observation, reference: None }
        })
        .collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(&captured.description, &rows, true).unwrap();
    let SourceObservation::FocusedBlocking { snapshot, .. } = &captured.description.observation else { panic!() };
    assert_eq!(snapshot.coverage.error_code, Some(ErrorCode::PermissionDenied));
    assert_eq!(report.binding, FocusedRequestBinding::Matched, "the independently observed original key stays exact");
    assert_eq!(report.source_state, culprit_core::SourceState::Partial);
    assert!(
        report.limitations.iter().any(|limitation| limitation == &culprit_core::t!("view.blocking.limit.visibility"))
    );
}

#[tokio::test]
async fn ending_during_enrichment_retains_earlier_facts_and_its_explicit_final_boundary() {
    let focus = qualified_focus().await;
    for (last, expected) in [
        (fixture::actor(BACKEND, "2026-11-15T09:30:00Z"), BlockingRequestState::Changed),
        (fixture::row(&[("observed_at", Some(AT)), ("pid", None)]), BlockingRequestState::Missing),
    ] {
        let mut reader = scripted();
        reader.replies[7] = Ok(vec![last]);
        let captured = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.unwrap();
        let SourceObservation::FocusedBlocking { result, .. } = &captured.description.observation else {
            unreachable!()
        };
        assert_eq!(result.request_end, expected);
        assert_eq!(result.membership, FocusedRequestMembership::Observed);
        assert_eq!(captured.records.len(), 2);
    }
}

#[tokio::test]
async fn participant_limit_is_a_partial_positive_while_native_stops_abort_without_fallback() {
    let focus = qualified_focus().await;
    let mut limited = request(focus.clone());
    limited.budget.max_snapshot_rows = 1;
    limited.budget.page_rows = 1;
    let mut reader = scripted();
    reader.replies.remove(4);
    reader.replies.remove(4); // No owner context or lock detail fits this prefix.
    let captured = capture(&mut reader, "fixture.invalid", 9, &limited).await.unwrap();
    let SourceObservation::FocusedBlocking { result, snapshot } = &captured.description.observation else {
        unreachable!()
    };
    assert_eq!(result.closure.status, BlockingClosureStatus::ParticipantLimit);
    assert_eq!(snapshot.coverage.stopped, Some(CollectionStop::RowLimit));
    assert_eq!(captured.records.len(), 1);
    assert!(reader.calls.iter().all(|sql| !sql.contains("owner_context AS") && !sql.contains("pg_catalog.pg_locks")));
    for stop in
        [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::QueryLimit, CollectionStop::ByteLimit]
    {
        let mut reader = scripted();
        reader.replies[4] = Err(stop.error());
        let error = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), stop);
        assert_eq!(reader.calls.len(), 5);
    }
}

#[test]
fn lazy_recipe_and_every_caller_dimension_remain_stricter_without_a_runtime_or_connection() {
    for requested in [CollectionBudget::default(), CollectionBudget::upper_limits()] {
        let profile = bounded_budget(&requested);
        profile.validate().expect("source profile preserves coherent page/total byte bounds");
        assert_eq!(profile.total_bytes, MAX_BLOCKING_BYTES);
        assert_eq!(profile.page_bytes, MAX_BLOCKING_BYTES);
        assert!(profile.page_rows <= requested.page_rows);
        assert!(profile.max_queries <= requested.max_queries);
    }
    let provider = crate::PostgresProvider::new(&culprit_core::ConnectionSpec {
        engine: Some(culprit_core::Engine::Postgres),
        host: "fixture.invalid".into(),
        port: Some(9),
        database: Some("shop".into()),
        ..Default::default()
    })
    .unwrap();
    let request = request(focus());
    assert_eq!(provider.telemetry_source(&request).unwrap().query_count(), 0);
    let descriptor = crate::telemetry_recipes().into_iter().find(|recipe| recipe.id == "postgres.blocking").unwrap();
    assert_eq!(descriptor.purpose, RecipePurpose::Investigation);
    assert!(!matches!(descriptor.admission, RecipeAdmission::Measured(_)));
    assert!(!RecipeSelector::Collection(TelemetryCollection::Activity).matches(&request));
    let mut strict = request.budget.clone();
    strict.max_queries = 1;
    strict.max_snapshot_rows = 1;
    strict.page_rows = 1;
    strict.statement_timeout_ms = 1;
    strict.total_timeout_ms = 1;
    strict.page_bytes = 1024;
    strict.total_bytes = 1024;
    strict.validate().unwrap();
    assert_eq!(bounded_budget(&strict), strict);
}

struct Factory(std::sync::Mutex<Option<NativeReader>>);
#[async_trait::async_trait]
impl culprit_collector::capture::SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        Ok(Box::new(source(self.0.lock().unwrap().take().expect("one owned source acquisition"), request)))
    }
}

#[tokio::test]
async fn the_generic_capture_owner_requires_confirmed_close_and_never_publishes_a_failed_provisional_closure() {
    use culprit_collector::capture::collect_once;
    use culprit_read::admission::SourceGate;
    use culprit_read::cancellation::CollectionCancellation;
    let requested = request(qualified_focus().await);
    let directory = tempfile::tempdir().unwrap();
    let gate = SourceGate::default();
    let complete = collect_once(
        &gate,
        &Factory(std::sync::Mutex::new(Some(scripted()))),
        requested.clone(),
        &directory.path().join("complete"),
        &CollectionCancellation::default(),
    )
    .await
    .unwrap();
    assert_eq!(complete.status, CollectionStatus::Complete);
    assert_eq!(complete.source_queries, 10);
    assert_eq!(complete.files.iter().map(|file| file.rows).sum::<u32>(), 2);
    assert!(!gate.is_closed());

    let mut unknown = scripted();
    unknown.unconfirmed = true;
    let gate = SourceGate::default();
    let incomplete = collect_once(
        &gate,
        &Factory(std::sync::Mutex::new(Some(unknown))),
        requested.clone(),
        &directory.path().join("unconfirmed"),
        &CollectionCancellation::default(),
    )
    .await
    .unwrap();
    assert_eq!(incomplete.status, CollectionStatus::Incomplete(CollectionStop::CancellationUnconfirmed));
    assert_eq!(incomplete.source_queries, 10);
    assert!(gate.is_closed(), "an uncertain native owner cannot be reused by another question");

    let mut stopped = scripted();
    stopped.query_limit = Some(4);
    let gate = SourceGate::default();
    let incomplete = collect_once(
        &gate,
        &Factory(std::sync::Mutex::new(Some(stopped))),
        requested,
        &directory.path().join("caller-limit"),
        &CollectionCancellation::default(),
    )
    .await
    .unwrap();
    assert_eq!(incomplete.status, CollectionStatus::Incomplete(CollectionStop::QueryLimit));
    assert_eq!(incomplete.source_queries, 4);
    assert!(incomplete.description.is_none());
    assert_eq!(incomplete.files.iter().map(|file| file.rows).sum::<u32>(), 0);
    assert!(!gate.is_closed(), "the same source's confirmed close settles an ordinary budget stop");
}

#[tokio::test]
async fn the_final_native_member_process_and_role_guard_runs_before_any_completed_frame_can_escape() {
    let focus = qualified_focus().await;
    for field in ["member", "process", "role"] {
        let mut reader = scripted();
        match field {
            "member" => reader.replies[9] = Ok(vec![fixture::member("127.0.0.2")]),
            "process" => reader.replies[8] = Ok(vec![fixture::metadata_with("f", "2026-11-14T01:00:00Z")]),
            _ => reader.replies[8] = Ok(vec![fixture::metadata_with("t", PROCESS)]),
        }
        let error = capture(&mut reader, "fixture.invalid", 9, &request(focus.clone())).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceChanged, "{field}");
        assert_eq!(reader.calls.len(), if field == "member" { 10 } else { 9 });
    }
    let mut unavailable = focus;
    unavailable.database_writable = None;
    let mut reader = scripted();
    let error = capture(&mut reader, "fixture.invalid", 9, &request(unavailable)).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceUnavailable);
    assert_eq!(reader.calls.len(), 2, "unmatched original role provenance cannot be filled by a current read");
}

/// The shared guard delegates to the actual scripted native reader; it does not interpret any SQL or output.
struct ProfiledBlockingRead {
    native: NativeReader,
    guard: culprit_read::SourceProfileGuard,
    active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    retire_after: Option<usize>,
}
#[async_trait::async_trait]
impl crate::read::QueryRead for ProfiledBlockingRead {
    fn connection_generation(&self) -> Option<u64> {
        Some(1)
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        self.guard.check_connection(Some(1))?;
        let result = self.native.query(sql, limit).await;
        if self.retire_after == Some(self.native.calls.len()) {
            self.active.store(false, std::sync::atomic::Ordering::Release);
        }
        result
    }
}
#[async_trait::async_trait]
impl crate::read::NativeRead for ProfiledBlockingRead {
    fn queries(&self) -> u32 {
        self.native.queries()
    }
    fn metrics(&self) -> Option<ReadWorkMetrics> {
        self.native.metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.native.close().await
    }
}
#[tokio::test]
async fn builtin_focused_blocking_uses_the_same_native_reader_and_refuses_every_retired_dispatch_or_unknown_major() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let original = qualified_focus().await;
    let scope = DiagnosticScope {
        target: original.target.clone(),
        partition: original.partition.clone(),
        source: original.source.clone(),
        native_epoch: Some(original.epoch.clone()),
        population: DiagnosticPopulation::Activity {
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
        },
        measurement_scope: culprit_core::MeasurementScope::Database,
    };
    let request = request(original);
    let recipe = crate::telemetry::declarations().into_iter().find(|recipe| recipe.id == "postgres.blocking").unwrap();
    let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else { panic!() };
    for stop in std::iter::once(None).chain((1..=10).map(Some)) {
        let active = Arc::new(AtomicBool::new(true));
        let flag = active.clone();
        let admission = SourceProfileAdmission::for_builtin(
            digest(b"original-blocking-route"),
            policy.as_ref().clone(),
            Some(RecipeCostScope::Activity(scope.clone())),
            request.budget.clone(),
            move || flag.load(Ordering::Acquire),
        )
        .unwrap();
        assert!(admission.qualification().is_none());
        let guard = culprit_read::SourceProfileGuard::new(&request, &admission).unwrap();
        let mut reader = ProfiledBlockingRead { native: scripted(), guard: guard.clone(), active, retire_after: stop };
        let capture = capture_with_profile(&mut reader, "fixture.invalid", 9, &request, Some(&guard)).await;
        if let Some(stop) = stop {
            assert!(capture.is_err(), "retired after native dispatch {stop}");
            assert_eq!(reader.native.calls.len(), stop);
            reader.active.store(true, Ordering::Release);
            assert!(guard.ensure_active().is_err());
        } else {
            let captured = capture.unwrap();
            let SourceObservation::FocusedBlocking { result, .. } = &captured.description.observation else { panic!() };
            assert_eq!(result.binding, FocusedRequestBinding::Matched);
            assert_eq!(reader.native.calls.len(), 10);
            assert_eq!(captured.records.len(), 2);
        }
    }
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"original-blocking-route"),
        *policy,
        Some(RecipeCostScope::Activity(scope)),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    let guard = culprit_read::SourceProfileGuard::new(&request, &admission).unwrap();
    let mut reader = ProfiledBlockingRead {
        native: scripted(),
        guard: guard.clone(),
        active: Arc::new(AtomicBool::new(true)),
        retire_after: None,
    };
    reader.native.replies[0] = Ok(vec![fixture::metadata_on_version(170_000, "f", PROCESS)]);
    assert!(capture_with_profile(&mut reader, "fixture.invalid", 9, &request, Some(&guard)).await.is_err());
    assert_eq!(reader.native.calls.len(), 1, "unsupported major is refused before member/population work");
}
