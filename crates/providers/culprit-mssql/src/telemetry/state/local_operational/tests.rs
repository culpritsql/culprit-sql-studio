//! Local file-census refusals retain their native premises while independent Operational readers report unavailable.
use super::super::*;
use crate::{
    instance::test_support::{epoch, metadata},
    operations::native::test_support::{file_row, space_row},
    read::ReadResult,
    resources::test_support::{FakeRead, instance_pressure_row, replace},
    value::{Rec, Val},
};
use culprit_core::{Error, SourceState};
use std::future::Future;

const FROM: &str = "2020-01-01T06:02:00Z";
const UNTIL: &str = "2020-01-01T06:02:01Z";
const FINAL: &str = "2020-01-01T06:03:00Z";
const FILE_ADMISSION: usize = 7;
const FILE_READ: usize = 8;
const CLOSING_CLOCK: usize = 15;
const FINAL_PLATFORM: usize = 16;
const FINAL_EPOCH: usize = 17;
const FINAL_AUTHORITY: usize = 18;
const READER_QUERIES: u32 = 19;
mod families;

fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational },
        budget: CollectionBudget { page_rows: 2, ..Default::default() },
    }
}
fn authority(edition: i64, at: &str, writable: bool) -> Rec {
    replace(
        &crate::activity::database_context::tests::row(edition),
        &[
            ("observed_at", Val::Text(at.into())),
            ("updateability", Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into())),
        ],
    )
}
fn platform(edition: i64) -> Rec {
    replace(&metadata(), &[("edition", Val::Int(edition))])
}
fn file_rows() -> Vec<Rec> {
    let group = replace(&space_row(), &[("database_name", Val::Text("shop".into()))]);
    let data = replace(&file_row(), &[("database_name", Val::Text("shop".into())), ("database_id", Val::Int(7))]);
    let log = replace(
        &data,
        &[
            ("native_id", Val::Int(2)),
            ("name", Val::Text("shop-log".into())),
            ("file_type", Val::Int(1)),
            ("file_type_desc", Val::Text("LOG".into())),
            ("data_space_id", Val::Int(0)),
            ("filegroup_read_only", Val::Null),
            ("size_pages", Val::Int(32)),
            ("used_pages", Val::Null),
        ],
    );
    vec![group, data, log]
}
fn replies(edition: i64, writable: bool) -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![Rec::of(&[("edition", Val::Int(edition)), ("configured_log_rate_capability", Val::Int(0))])]),
        Ok(vec![platform(edition)]),
        Ok(vec![epoch(100)]),
        Ok(vec![authority(edition, "2020-01-01T06:00:00Z", writable)]),
        Ok(vec![super::super::tests::clock(true, FROM)]),
        Ok(vec![replace(&instance_pressure_row(), &[("observed_at", Val::Text(FROM.into()))])]),
        unsupported_family(),
        Ok(vec![replace(&instance_pressure_row(), &[("observed_at", Val::Text(FROM.into()))])]),
        Ok(file_rows()),
        Ok(vec![replace(&instance_pressure_row(), &[("observed_at", Val::Text(FROM.into()))])]),
        unsupported_family(),
        Ok(vec![replace(&instance_pressure_row(), &[("observed_at", Val::Text(FROM.into()))])]),
        unsupported_family(),
        Ok(vec![replace(&instance_pressure_row(), &[("observed_at", Val::Text(FROM.into()))])]),
        unsupported_family(),
        Ok(vec![super::super::tests::clock(true, UNTIL)]),
        Ok(vec![platform(edition)]),
        Ok(vec![epoch(100)]),
        Ok(vec![authority(edition, FINAL, writable)]),
    ]
}
fn unsupported_family() -> ReadResult<Vec<Rec>> {
    Err(ReadError::Source(Error::new(ErrorCode::Unsupported, "controlled independent family unavailable")))
}
fn file_source(rows: Vec<ReadResult<Vec<Rec>>>, request: &CollectionRequest) -> StateSource<FakeRead> {
    StateSource::with_io(
        FakeRead::new(rows).with_generation(1).with_query_limit(request.budget.max_queries.saturating_sub(1)),
        request,
        "fixture.invalid".into(),
        1433,
    )
    .unwrap()
}
async fn drain(source: &mut StateSource<FakeRead>) -> Vec<TelemetryRecord> {
    let mut rows = vec![];
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::OperationalSamples, cursor.as_deref()).await.unwrap()
        else {
            panic!("the complete original census must have complete delivery pages")
        };
        rows.extend(records);
        cursor = next_cursor;
        if cursor.is_none() {
            return rows;
        }
    }
}
fn coverage(source: &CollectionDescription) -> &StateSourceCoverage {
    source
        .observation
        .snapshot()
        .unwrap()
        .sources
        .iter()
        .find(|source| source.source == crate::sources::FILE_CAPACITY.name)
        .unwrap()
}
fn storage(source: &CollectionDescription, rows: &[TelemetryRecord]) -> StorageRead {
    StorageRead::from_source(
        source,
        rows.iter()
            .map(|row| {
                let TelemetryRecord::Operational(row) = row else { panic!("native file observation") };
                row.clone()
            })
            .collect(),
        true,
    )
    .unwrap()
}

#[tokio::test]
async fn boxed_and_managed_instance_files_keep_local_only_owner_and_one_existing_decoder() {
    for (edition, writable) in [(2, true), (3, true), (4, true), (8, true), (3, false), (8, false)] {
        let mut source = file_source(replies(edition, writable), &request());
        let description = source.describe().await.unwrap();
        let context = NativeReadContext::from_snapshot(&description).unwrap();
        assert!(context.has_qualified_local_owner(), "edition {edition}, writable {writable}");
        assert!(description.target.database().unwrap().incarnation.is_none());
        assert_eq!(context.database_writable, None, "ordinary Snapshot does not borrow acquisition role");
        let header = description.observation.snapshot().unwrap();
        assert_eq!(header.records, 3);
        assert_eq!(
            (coverage(&description).state, coverage(&description).complete_scope),
            (SourceState::Collected, true)
        );
        assert!(header.sources.iter().filter(|source| source.source != crate::sources::FILE_CAPACITY.name).all(
            |source| source.state == SourceState::Unavailable && source.error_code == Some(ErrorCode::Unsupported)
        ));
        let rows = drain(&mut source).await;
        let mut baseline = FakeRead::new(vec![Ok(file_rows())]);
        let expected = crate::operations::native::read(&mut baseline, crate::operations::native::Kind::Filegroups, 500)
            .await
            .unwrap()
            .observations(crate::operations::native::Kind::Filegroups)
            .unwrap()
            .into_iter()
            .map(TelemetryRecord::Operational)
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "qualification must not replace the normal file decoder");
        let capture = CaptureIdentity { observation_id: format!("local-file-{edition}-{writable}"), revision: None };
        for row in &rows {
            let exported = ExportedRecord::new(&description, &capture, row.clone()).unwrap();
            assert_eq!(
                serde_json::from_slice::<ExportedRecord>(&serde_json::to_vec(&exported).unwrap()).unwrap(),
                exported
            );
        }
        let report = culprit_checks::storage::explain(&storage(&description, &rows)).unwrap();
        let summary = report.file_summary.as_ref().unwrap();
        assert_eq!(
            (summary.data_file_count, summary.log_file_count, summary.other_file_count),
            (Some(1), Some(1), Some(0))
        );
        assert_eq!(summary.data_allocated_bytes.as_deref(), Some("1048576"));
        assert_eq!(summary.data_used_bytes.as_deref(), Some("524288"));
        assert_eq!(summary.log_catalog_allocated_bytes.as_deref(), Some("262144"));
        assert_eq!(report.source_state, SourceState::Partial, "the independent DMV log source remains unavailable");
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), READER_QUERIES);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
        assert_eq!(source.io.calls[FILE_READ].sql, baseline.calls[0].sql);
        assert_eq!(source.io.calls[FILE_READ].limit, 501);
        // This fake records templates before dispatch. The shared ReadQuery owner marks every buffered and
        // streaming monitoring batch; its fixture_dispatch_tests pin the actual wire provenance.
        assert!(source.io.calls.iter().all(|call| !call.sql.contains("sys.master_files")
            && !call.sql.contains("FROM sys.dm_user_db_resource_governance")));
        assert_eq!(
            source.io.calls.iter().filter(|call| call.sql.contains("FROM sys.dm_db_log_space_usage")).count(),
            1
        );
        assert_eq!(
            source
                .io
                .calls
                .iter()
                .filter(|call| call.sql.contains("sys.dm_io_virtual_file_stats(DB_ID(), NULL)"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn absent_role_native_generation_or_unsupported_platform_never_reads_files() {
    for changes in [
        vec![("updateability", Val::Null)],
        vec![("database_guid", Val::Null)],
        vec![("recovery_fork_guid", Val::Null)],
        vec![("database_file_guid", Val::Null)],
    ] {
        let mut rows = replies(3, true);
        rows[3] = Ok(vec![replace(&authority(3, FROM, true), &changes)]);
        let mut source = file_source(rows, &request());
        let error = source.describe().await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceUnavailable);
        assert_eq!(source.query_count(), 4);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    for (edition, major) in [(6, "16"), (3, "10")] {
        let mut rows = replies(edition, true);
        rows[1] = Ok(vec![replace(&platform(edition), &[("major", Val::Text(major.into()))])]);
        let mut source = file_source(rows, &request());
        assert_eq!(source.describe().await.err().unwrap().code, ErrorCode::Unsupported);
        assert_eq!(source.query_count(), 2);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn foreign_database_file_or_group_never_survives_the_original_source_guard() {
    for (index, change, expected) in [
        (1, ("database_id", Val::Int(8)), CollectionStop::SourceChanged),
        (0, ("database_name", Val::Text("foreign".into())), CollectionStop::SourceChanged),
        (1, ("database_name", Val::Text("foreign".into())), CollectionStop::SourceChanged),
        (2, ("native_id", Val::Int(1)), CollectionStop::InvalidPage),
    ] {
        let mut native = file_rows();
        native[index] = replace(&native[index], &[change]);
        let mut rows = replies(3, true);
        rows[FILE_READ] = Ok(native);
        let mut source = file_source(rows, &request());
        let result = source.describe().await;
        if expected == CollectionStop::SourceChanged {
            assert_eq!(CollectionStop::from_error(&result.err().unwrap()), expected);
            assert_eq!(source.query_count(), FILE_READ as u32 + 1);
        } else {
            let description = result.unwrap();
            assert_eq!(coverage(&description).state, SourceState::Unavailable);
            assert!(drain(&mut source).await.is_empty());
            assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn changed_database_recovery_member_process_role_or_final_clock_refuses_completion() {
    for (ordinal, change) in [
        (FINAL_AUTHORITY, ("database_guid", Val::Text("recreated".into()))),
        (FINAL_AUTHORITY, ("recovery_fork_guid", Val::Text("recovered".into()))),
        (FINAL_AUTHORITY, ("database_file_guid", Val::Text("replaced".into()))),
        (FINAL_AUTHORITY, ("selected_database_id", Val::Int(8))),
        (FINAL_AUTHORITY, ("updateability", Val::Text("READ_ONLY".into()))),
        (FINAL_AUTHORITY, ("updateability", Val::Null)),
        (FINAL_AUTHORITY, ("observed_at", Val::Text("2020-01-01T06:01:00Z".into()))),
        (FINAL_PLATFORM, ("server_name", Val::Text("foreign\\NATIVE".into()))),
        (FINAL_PLATFORM, ("process_id", Val::Int(43))),
        (FINAL_EPOCH, ("started_at_ticks", Val::Int(101))),
    ] {
        let mut rows = replies(3, true);
        let current = rows[ordinal].as_ref().unwrap()[0].clone();
        rows[ordinal] = Ok(vec![replace(&current, &[change])]);
        let mut source = file_source(rows, &request());
        let description = source.describe().await.unwrap();
        let rows = drain(&mut source).await;
        assert_eq!(rows.len(), 3, "refusal must not erase the original collected facts");
        assert!(storage(&description, &rows).is_valid());
        match source.finish().await {
            Ok(completion) => assert_ne!(completion, SourceCompletion::Verified),
            Err(error) => assert!(matches!(
                CollectionStop::from_error(&error),
                CollectionStop::SourceChanged | CollectionStop::InvalidPage
            )),
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn native_permission_gap_and_null_quantities_do_not_become_complete_zero_capacity() {
    let mut rows = replies(3, true);
    rows[FILE_READ] = Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "fixture file permission denied")));
    let mut source = file_source(rows, &request());
    let description = source.describe().await.unwrap();
    assert_eq!(
        (coverage(&description).state, coverage(&description).error_code),
        (SourceState::Unavailable, Some(ErrorCode::PermissionDenied))
    );
    let rows = drain(&mut source).await;
    assert!(rows.is_empty());
    let report = culprit_checks::storage::explain(&storage(&description, &rows)).unwrap();
    assert_eq!(report.file_summary.unwrap().data_file_count, None);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified, "held permission gap is not transport loss");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);

    let mut rows = replies(3, true);
    let mut files = file_rows();
    files[1] = replace(&files[1], &[("used_pages", Val::Null), ("database_is_snapshot", Val::Null)]);
    rows[FILE_READ] = Ok(files);
    let mut source = file_source(rows, &request());
    let description = source.describe().await.unwrap();
    let rows = drain(&mut source).await;
    let report = culprit_checks::storage::explain(&storage(&description, &rows)).unwrap();
    let summary = report.file_summary.unwrap();
    assert_eq!(summary.data_file_count, Some(1));
    assert_eq!(summary.data_allocated_bytes, None, "unknown snapshot basis cannot establish allocated bytes");
    assert_eq!(summary.data_used_bytes, None, "native FILEPROPERTY NULL stays unknown");
    assert_eq!(summary.log_catalog_allocated_bytes.as_deref(), Some("262144"));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn caller_row_byte_and_query_limits_preserve_no_complete_census_claim() {
    let mut request = request();
    request.budget.max_snapshot_rows = 2;
    let mut source = file_source(replies(3, true), &request);
    let description = source.describe().await.unwrap();
    assert_eq!(coverage(&description).state, SourceState::Unavailable);
    assert_eq!(coverage(&description).stopped, Some(CollectionStop::RowLimit));
    assert!(drain(&mut source).await.is_empty());
    assert_eq!(source.io.calls[FILE_READ].limit, 3);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);

    let mut request = self::request();
    request.budget.page_bytes = 1024;
    let mut source = file_source(replies(3, true), &request);
    source.describe().await.unwrap();
    assert!(matches!(
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap(),
        CollectionRead::Stopped(CollectionStop::ByteLimit)
    ));
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);

    let mut request = self::request();
    request.budget.max_queries = READER_QUERIES;
    let mut source = file_source(replies(3, true), &request);
    source.describe().await.unwrap();
    assert_eq!(drain(&mut source).await.len(), 3);
    assert_eq!(CollectionStop::from_error(&source.finish().await.err().unwrap()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), READER_QUERIES - 1);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn native_failure_and_transport_change_stop_before_any_later_dispatch() {
    for (ordinal, stop, changed) in [
        (FILE_ADMISSION, CollectionStop::Cancelled, false),
        (FILE_READ, CollectionStop::CancellationUnconfirmed, false),
        (FILE_READ, CollectionStop::TimedOut, false),
        (FILE_READ, CollectionStop::QueryLimit, false),
        (FILE_READ, CollectionStop::SourceUnavailable, true),
    ] {
        let mut rows = replies(3, true);
        let original =
            if changed { Error::new(ErrorCode::PermissionDenied, "original failed file read") } else { stop.error() };
        rows[ordinal] = Err(ReadError::Source(original.clone()));
        let mut source = file_source(rows, &request());
        if changed {
            source.io = source.io.change_generation_at(ordinal as u32 + 1);
        }
        if stop == CollectionStop::CancellationUnconfirmed {
            source.io.shutdown = SourceShutdown::Unconfirmed;
        }
        let actual = source.describe().await.err().unwrap();
        assert_eq!((actual.code, actual.detail, actual.message), (original.code, original.detail, original.message));
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
        assert_eq!(source.io.closed, 1);
    }
    let mut source = file_source(replies(3, true), &request());
    source.io = source.io.change_generation_at(FILE_READ as u32 + 1);
    assert_eq!(CollectionStop::from_error(&source.describe().await.err().unwrap()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), FILE_READ as u32 + 1);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn cancellation_during_file_read_keeps_original_close_ownership() {
    let mut source = file_source(replies(3, true), &request());
    source.io = source.io.pause_on_query(FILE_READ + 1);
    let mut description = Box::pin(source.describe());
    std::future::poll_fn(|context| match description.as_mut().poll(context) {
        std::task::Poll::Pending => std::task::Poll::Ready(()),
        std::task::Poll::Ready(_) => panic!("fixture file read must remain pending until its caller retires"),
    })
    .await;
    drop(description);
    assert_eq!(source.query_count(), FILE_READ as u32 + 1);
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(source.io.closed, 1);
}

#[tokio::test]
async fn public_file_metadata_does_not_borrow_view_definition_or_foreign_engine_authority() {
    let mut rows = replies(3, true);
    rows[4] = Ok(vec![super::super::tests::clock(false, FROM)]);
    rows[CLOSING_CLOCK] = Ok(vec![super::super::tests::clock(false, UNTIL)]);
    let mut source = file_source(rows, &request());
    let description = source.describe().await.unwrap();
    assert_eq!((coverage(&description).state, coverage(&description).complete_scope), (SourceState::Collected, true));
    assert_eq!(drain(&mut source).await.len(), 3);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);

    let mut request = request();
    request.subject = CollectionSubject::Shared {
        resource: SharedResourceIdentity {
            engine: culprit_core::Engine::Postgres,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new("foreign-machine"),
            native: NativeIdentity::new("foreign-instance"),
        },
    };
    let result = StateSource::with_io(FakeRead::new(vec![]), &request, "fixture.invalid".into(), 1433);
    assert!(result.is_err(), "foreign-engine ownership is refused before any native dispatch");
}

#[test]
fn the_existing_caller_budget_still_pays_setup_before_file_acquisition() {
    let mut caller = request();
    caller.budget.max_queries = 10;
    let native = native_budget(&caller);
    assert_eq!(native, caller.budget, "Operational keeps its existing caller ceilings");
    let reader = ReadSession::budget_after_setup(native).unwrap();
    assert_eq!(reader.max_queries, 9);
    let mut one = caller.budget;
    one.max_queries = 1;
    assert_eq!(
        CollectionStop::from_error(&ReadSession::budget_after_setup(one).err().unwrap()),
        CollectionStop::QueryLimit
    );
}
