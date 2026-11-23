//! Same native readers and full caller accounting; local ownership cannot manufacture global or cross-database scope.
use super::*;
use culprit_core::{FileScope, MeasurementScope, ResetMarker, telemetry::CounterUnit};
const LOG_READ: usize = 6;
const IO_READ: usize = 10;
const VERSION_READ: usize = 12;
const PVS_LAYOUT: usize = 14;
const PVS_ADR: usize = 15;
const PVS_READ: usize = 16;
const FULL_READER_QUERIES: u32 = 21;
const FULL_TOTAL_QUERIES: u32 = FULL_READER_QUERIES + 1;
const OPERATIONAL_SOURCES: [&str; 5] = [
    culprit_core::source_id::LOG_CAPACITY,
    culprit_core::source_id::FILE_CAPACITY,
    culprit_core::source_id::FILE_LATENCY,
    culprit_core::source_id::VERSION_STORE,
    culprit_core::source_id::PERSISTENT_VERSION_STORE,
];
fn log_row() -> Rec {
    replace(&space_row(), &[("database_name", Val::Text("shop".into()))])
}
fn io_rows(edition: i64) -> Vec<Rec> {
    let row = replace(
        &crate::storage_io::tests::row(),
        &[("database_id", Val::Int(7)), ("engine_edition", Val::Int(edition))],
    );
    let log = replace(&row, &[("file_id", Val::Int(2)), ("native_type", Val::Text("1".into()))]);
    vec![row, log]
}
fn version_row() -> Rec {
    Rec::of(&[
        ("native_rows", Val::Int(1)),
        ("known_rows", Val::Int(1)),
        ("reserved_space_kb", Val::Int(9_007_199_254_740_993)),
    ])
}
fn full_replies(edition: i64, writable: bool) -> Vec<ReadResult<Vec<Rec>>> {
    let mut rows = replies(edition, writable);
    rows[LOG_READ] = Ok(vec![log_row()]);
    rows[IO_READ] = Ok(io_rows(edition));
    rows[VERSION_READ] = Ok(vec![version_row()]);
    rows[PVS_LAYOUT] =
        Ok(vec![crate::version_store::persistent::tests::layout(edition, if edition == 8 { 12 } else { 16 }, 1, 1)]);
    rows.insert(PVS_ADR, Ok(vec![Rec::of(&[("adr", Val::Int(1))])]));
    rows.insert(PVS_READ, Ok(vec![crate::version_store::persistent::tests::row()]));
    rows
}
fn full_request() -> CollectionRequest {
    let mut request = request();
    request.budget.max_queries = FULL_TOTAL_QUERIES;
    request
}
fn source_state<'a>(description: &'a CollectionDescription, source: &str) -> &'a StateSourceCoverage {
    description.observation.snapshot().unwrap().sources.iter().find(|coverage| coverage.source == source).unwrap()
}
fn operational(rows: &[TelemetryRecord]) -> Vec<OperationalObservation> {
    rows.iter()
        .map(|row| {
            let TelemetryRecord::Operational(row) = row else { panic!() };
            row.clone()
        })
        .collect()
}

#[tokio::test]
async fn boxed_and_mi_log_io_and_version_families_share_the_existing_native_readers_and_local_owner() {
    for (edition, writable) in [(2, true), (3, true), (4, true), (8, true), (3, false), (8, false)] {
        let mut source = file_source(full_replies(edition, writable), &full_request());
        let description = source.describe().await.unwrap();
        let context = NativeReadContext::from_snapshot(&description).unwrap();
        assert!(context.has_qualified_local_owner());
        assert!(description.target.database().unwrap().incarnation.is_none());
        assert_eq!(context.database_writable, None, "Snapshot retains its existing public role semantics");
        for name in OPERATIONAL_SOURCES {
            let coverage = source_state(&description, name);
            assert_eq!(coverage.state, SourceState::Collected, "{name} edition {edition}");
            assert!(coverage.complete_scope && coverage.records > 0);
        }
        for coverage in &description.observation.snapshot().unwrap().sources {
            if !OPERATIONAL_SOURCES.contains(&coverage.source.as_str()) {
                assert_eq!(
                    (coverage.state, coverage.error_code),
                    (SourceState::Unavailable, Some(ErrorCode::Unsupported))
                );
            }
        }
        let rows = drain(&mut source).await;
        assert_eq!(rows.len(), 8);
        for row in operational(&rows) {
            if let OperationalFact::FileIo(file) = row.fact {
                assert_eq!(file.scope, FileScope::Database);
                assert_eq!(file.reset, ResetMarker::Unknown, "a point cannot manufacture a native interval reset");
            }
        }
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), FULL_READER_QUERIES);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
        let capture =
            CaptureIdentity { observation_id: format!("local-operational-{edition}-{writable}"), revision: None };
        for row in &rows {
            let exported = ExportedRecord::new(&description, &capture, row.clone()).unwrap();
            assert_eq!(
                serde_json::from_slice::<ExportedRecord>(&serde_json::to_vec(&exported).unwrap()).unwrap(),
                exported
            );
        }
        let mut ordinary = FakeRead::new(vec![
            Ok(vec![log_row()]),
            Ok(io_rows(edition)),
            Ok(vec![version_row()]),
            Ok(vec![crate::version_store::persistent::tests::layout(
                edition,
                if edition == 8 { 12 } else { 16 },
                1,
                1,
            )]),
            Ok(vec![Rec::of(&[("adr", Val::Int(1))])]),
            Ok(vec![crate::version_store::persistent::tests::row()]),
        ]);
        let log = crate::operations::native::read(&mut ordinary, crate::operations::native::Kind::Log, 500)
            .await
            .unwrap()
            .observations(crate::operations::native::Kind::Log)
            .unwrap();
        let io = crate::storage_io::observations(
            crate::storage_io::read(&mut ordinary, crate::storage_io::Selection::Database, 500).await.unwrap(),
        );
        let version = crate::version_store::observation(crate::version_store::read(&mut ordinary).await.unwrap());
        let persistent = crate::version_store::persistent::observation(
            crate::version_store::persistent::read(&mut ordinary).await.unwrap().unwrap(),
        );
        let expected = log.into_iter().chain(io).chain([version, persistent]).collect::<Vec<_>>();
        let actual = operational(&rows)
            .into_iter()
            .filter(|row| row.source != culprit_core::source_id::FILE_CAPACITY)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "dispatch must not copy or alter the scan's native decoder");
        for (source_index, ordinary_index) in
            [(LOG_READ, 0), (IO_READ, 1), (VERSION_READ, 2), (PVS_LAYOUT, 3), (PVS_ADR, 4), (PVS_READ, 5)]
        {
            assert_eq!(source.io.calls[source_index].sql, ordinary.calls[ordinary_index].sql);
        }
        assert!(source.io.calls.iter().all(|call| !call.sql.contains("sys.dm_io_virtual_file_stats(NULL")));
        let storage = StorageRead::from_source(&description, operational(&rows), true).unwrap();
        let report = culprit_checks::storage::explain(&storage).unwrap();
        assert_eq!(report.source_state, SourceState::Collected);
        let versions =
            operational(&rows).into_iter().find(|row| row.source == culprit_core::source_id::VERSION_STORE).unwrap();
        let OperationalFact::Counter(version) = versions.fact else { panic!() };
        assert_eq!(version.value.integer(), Some(9_007_199_254_740_993));
        assert_eq!(
            (version.scope, version.unit, version.reset),
            (MeasurementScope::Database, CounterUnit::Kibibytes, ResetMarker::Unknown)
        );
    }
}

#[tokio::test]
async fn unknown_or_disabled_versions_remain_distinct_from_observed_zero_and_independent_sources() {
    for case in ["permission", "null", "disabled", "missing-layout", "pvs-null"] {
        let mut replies = full_replies(3, true);
        match case {
            "permission" => {
                replies[VERSION_READ] =
                    Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "controlled version source denied")))
            }
            "null" => replies[VERSION_READ] = Ok(vec![replace(&version_row(), &[("reserved_space_kb", Val::Null)])]),
            "disabled" => {
                replies[PVS_ADR] = Ok(vec![Rec::of(&[("adr", Val::Int(0))])]);
                let _ = replies.remove(PVS_READ);
            }
            "missing-layout" => {
                replies[PVS_LAYOUT] = Ok(vec![crate::version_store::persistent::tests::layout(3, 16, 0, 1)]);
                replies.drain(PVS_ADR..=PVS_READ);
            }
            "pvs-null" => {
                replies[PVS_READ] = Ok(vec![replace(
                    &crate::version_store::persistent::tests::row(),
                    &[("persistent_version_store_size_kb", Val::Null)],
                )])
            }
            _ => unreachable!(),
        }
        let mut source = file_source(replies, &full_request());
        let description = source.describe().await.unwrap();
        let rows = drain(&mut source).await;
        for name in [
            culprit_core::source_id::LOG_CAPACITY,
            culprit_core::source_id::FILE_CAPACITY,
            culprit_core::source_id::FILE_LATENCY,
        ] {
            assert_eq!(source_state(&description, name).state, SourceState::Collected, "{case}");
        }
        if matches!(case, "permission" | "null") {
            let coverage = source_state(&description, culprit_core::source_id::VERSION_STORE);
            assert_eq!(coverage.state, SourceState::Unavailable);
            assert!(!coverage.complete_scope);
            assert!(operational(&rows).iter().all(|row| row.source != culprit_core::source_id::VERSION_STORE));
        }
        if case == "disabled" {
            assert_eq!(
                source_state(&description, culprit_core::source_id::PERSISTENT_VERSION_STORE).state,
                SourceState::NotApplicable
            );
        }
        if case == "missing-layout" {
            let coverage = source_state(&description, culprit_core::source_id::PERSISTENT_VERSION_STORE);
            assert_eq!(coverage.state, SourceState::Unavailable);
            assert_eq!(coverage.stopped, Some(CollectionStop::SourceUnavailable));
        }
        if case == "pvs-null" {
            let samples = operational(&rows);
            let row =
                samples.iter().find(|row| row.source == culprit_core::source_id::PERSISTENT_VERSION_STORE).unwrap();
            let OperationalFact::MssqlPersistentVersionStore(sample) = &row.fact else { panic!() };
            assert_eq!(sample.off_row_kibibytes, None);
            assert!(crate::version_store::persistent::signal(sample).is_none());
        }
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified, "{case}");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut replies = full_replies(8, false);
    replies[VERSION_READ] = Ok(vec![Rec::of(&[
        ("native_rows", Val::Int(0)),
        ("known_rows", Val::Int(0)),
        ("reserved_space_kb", Val::Null),
    ])]);
    let mut source = file_source(replies, &full_request());
    let description = source.describe().await.unwrap();
    let rows = drain(&mut source).await;
    assert_eq!(source_state(&description, culprit_core::source_id::VERSION_STORE).state, SourceState::Collected);
    let sample =
        operational(&rows).into_iter().find(|row| row.source == culprit_core::source_id::VERSION_STORE).unwrap();
    let OperationalFact::Counter(counter) = sample.fact else { panic!() };
    assert_eq!(
        counter.value.integer(),
        Some(0),
        "the existing aggregate's known empty population alone establishes zero"
    );
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn foreign_log_or_file_database_and_shared_io_never_cross_the_local_original() {
    for (ordinal, row) in [
        (LOG_READ, replace(&log_row(), &[("database_name", Val::Text("foreign".into()))])),
        (IO_READ, replace(&io_rows(3)[0], &[("database_id", Val::Int(8))])),
        (IO_READ, replace(&io_rows(3)[0], &[("database_name", Val::Text("foreign".into()))])),
        (IO_READ, replace(&io_rows(3)[0], &[("shared", Val::Int(1))])),
    ] {
        let mut replies = full_replies(3, true);
        replies[ordinal] = Ok(vec![row]);
        let mut source = file_source(replies, &full_request());
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
        assert_eq!(source.query_count(), ordinal as u32 + 1);
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut replies = full_replies(3, true);
    replies[IO_READ] = Ok(vec![replace(&io_rows(3)[0], &[("database_name", Val::Null), ("file_name", Val::Null)])]);
    let mut source = file_source(replies, &full_request());
    source.describe().await.unwrap();
    let rows = drain(&mut source).await;
    let row = operational(&rows).into_iter().find(|row| row.source == culprit_core::source_id::FILE_LATENCY).unwrap();
    let OperationalFact::FileIo(file) = row.fact else { panic!() };
    assert_eq!(file.native_database_id, "7");
    assert_eq!(file.database, None);
    assert_eq!(file.native_name, None);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn each_added_native_phase_keeps_first_error_retirement_and_no_later_dispatch() {
    for ordinal in [LOG_READ, IO_READ, VERSION_READ, PVS_LAYOUT, PVS_ADR, PVS_READ] {
        for (stop, changed) in [
            (CollectionStop::Cancelled, false),
            (CollectionStop::TimedOut, false),
            (CollectionStop::CancellationUnconfirmed, false),
            (CollectionStop::SourceUnavailable, true),
        ] {
            let mut replies = full_replies(3, true);
            let original = if changed {
                Error::new(ErrorCode::PermissionDenied, "controlled original family failure")
            } else {
                stop.error()
            };
            replies[ordinal] = Err(ReadError::Source(original.clone()));
            let mut source = file_source(replies, &full_request());
            if changed {
                source.io = source.io.change_generation_at(ordinal as u32 + 1);
            }
            if stop == CollectionStop::CancellationUnconfirmed {
                source.io.shutdown = SourceShutdown::Unconfirmed;
            }
            let error = source.describe().await.unwrap_err();
            assert_eq!((error.code, error.detail, error.message), (original.code, original.detail, original.message));
            assert_eq!(source.query_count(), ordinal as u32 + 1);
            assert!(source.finish().await.is_err());
            assert_eq!(
                source.close().await,
                if stop == CollectionStop::CancellationUnconfirmed {
                    SourceShutdown::Unconfirmed
                } else {
                    SourceShutdown::Quiescent
                }
            );
        }
    }
}

#[tokio::test]
async fn exact_total_includes_setup_and_stricter_caller_cannot_receive_a_verified_original() {
    let mut request = full_request();
    request.budget.max_queries = FULL_TOTAL_QUERIES - 1;
    let mut source = file_source(full_replies(8, true), &request);
    source.describe().await.unwrap();
    drain(&mut source).await;
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), FULL_READER_QUERIES - 1);
    let calls = source.query_count();
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), calls);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    for ordinal in [LOG_READ, IO_READ, VERSION_READ, PVS_READ] {
        let mut source = file_source(full_replies(3, true), &full_request());
        source.io = source.io.pause_on_query(ordinal + 1);
        let mut task = Box::pin(source.describe());
        std::future::poll_fn(|context| match task.as_mut().poll(context) {
            std::task::Poll::Pending => std::task::Poll::Ready(()),
            std::task::Poll::Ready(_) => panic!("the original native phase must remain held"),
        })
        .await;
        drop(task);
        assert_eq!(source.query_count(), ordinal as u32 + 1);
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
    }
}

#[test]
fn the_local_family_policy_is_closed_and_the_selected_catalog_stays_unqualified() {
    let admitted = point::families(SnapshotDataset::Operational)
        .unwrap()
        .into_iter()
        .filter(|family| super::super::allows(*family))
        .map(|family| family.source().name)
        .collect::<Vec<_>>();
    assert_eq!(admitted, OPERATIONAL_SOURCES);
    let recipe = crate::telemetry::recipes().into_iter().find(|recipe| recipe.id == "mssql.operational").unwrap();
    assert!(matches!(recipe.admission, RecipeAdmission::Unqualified));
    assert_eq!(recipe.purpose, RecipePurpose::Baseline);
    for max_queries in [1, 7, FULL_TOTAL_QUERIES - 1, FULL_TOTAL_QUERIES, 64] {
        let mut caller = full_request();
        caller.budget.max_queries = max_queries;
        assert_eq!(native_budget(&caller), caller.budget, "local routing must not widen or replace a caller ceiling");
        assert!(recipe.accepts(&caller));
    }
}
