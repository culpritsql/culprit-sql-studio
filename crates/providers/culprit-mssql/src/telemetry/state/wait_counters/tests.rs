use super::*;
use crate::{
    instance::test_support::{epoch, metadata},
    read::ReadResult,
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};
use culprit_core::{Error, ErrorCode, MeasurementScope, ResetMarker, SourceState};

const FROM: &str = "2020-01-01T06:00:01Z";
const UNTIL: &str = "2020-01-01T06:00:02Z";
const FINAL: &str = "2020-01-01T06:00:03Z";

fn request(total: u32) -> CollectionRequest {
    let mut request = super::super::shared_tests::request();
    request.selection = CollectionSelection::Snapshot { dataset: SnapshotDataset::WaitCounters };
    request.budget.max_queries = total;
    request
}
fn member(edition: i64, at: &str) -> Rec {
    replace(&metadata(), &[("edition", Val::Int(edition)), ("observed_at", Val::Text(at.into()))])
}
fn replies(edition: i64) -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![member(edition, "2020-01-01T06:00:00Z")]),
        Ok(vec![epoch(100)]),
        Ok(vec![super::super::tests::clock(false, FROM)]),
        Ok(vec![
            crate::waits::tests::row("WRITELOG", 9_007_199_254_740_993),
            crate::waits::tests::row("SLEEP_TASK", 10),
        ]),
        Ok(vec![super::super::tests::clock(false, UNTIL)]),
        Ok(vec![member(edition, FINAL)]),
        Ok(vec![epoch(100)]),
    ]
}
fn source(rows: Vec<ReadResult<Vec<Rec>>>, total: u32) -> StateSource<FakeRead> {
    let request = request(total);
    let budget = ReadSession::budget_after_setup(bounded_budget(&request.budget)).unwrap();
    let reader = FakeRead::new(rows).with_generation(1).with_query_limit(budget.max_queries);
    StateSource::with_io(reader, &request, "unrelated-database-route.invalid".into(), 1433).unwrap()
}
async fn drain(source: &mut StateSource<FakeRead>) -> Vec<TelemetryRecord> {
    let mut rows = vec![];
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::WaitCounterSamples, cursor.as_deref()).await.unwrap()
        else {
            panic!("actual wait-counter page")
        };
        rows.extend(records);
        cursor = next_cursor;
        if cursor.is_none() {
            return rows;
        }
    }
}

#[tokio::test]
async fn boxed_and_mi_instance_waits_reuse_server_reader_and_seal_exact_scope_and_unknown_reset() {
    for edition in [2, 3, 4, 8] {
        let mut source = source(replies(edition), 8);
        let original = source.describe().await.unwrap();
        assert!(original.matches_request(&request(8)));
        assert_eq!(original.source, "shared-wait-counter-state");
        assert!(original.is_partition_qualified());
        let CollectionTarget::Shared(target) = &original.target else { panic!() };
        let SourcePartition::InstanceLocal { member: Some(owner), epoch: Some(epoch) } = &original.partition else {
            panic!()
        };
        assert_eq!(target, owner);
        assert!(valid_digest(epoch));
        let context = NativeReadContext::from_snapshot(&original).unwrap();
        assert!(context.database_writable.is_none(), "shared server waits do not borrow a member database role");
        let records = drain(&mut source).await;
        assert_eq!(records.len(), 2, "idle types stay in raw evidence");
        let capture = CaptureIdentity { observation_id: format!("native-instance-waits-{edition}"), revision: None };
        let counters = records
            .iter()
            .map(|record| {
                let exported = ExportedRecord::new(&original, &capture, record.clone()).unwrap();
                let TelemetryRecord::WaitCounter(row) = exported.record else { panic!() };
                assert_eq!(row.scope, MeasurementScope::Server);
                assert_eq!(row.reset, ResetMarker::Unknown);
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(counters[0].wait_time_ms, "9007199254740993");
        assert!(WaitCounterSnapshot::from_source(&original, counters, true).unwrap().is_valid());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count() + ReadSession::SESSION_SETUP_QUERIES, 8);
        assert_eq!(source.io.calls.iter().filter(|call| call.sql.contains("FROM sys.dm_os_wait_stats")).count(), 1);
        assert!(
            !source
                .io
                .calls
                .iter()
                .any(|call| call.sql.contains("sys.dm_db_wait_stats") || call.sql.contains("sys.dm_db_resource_stats"))
        );
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
    }
}

#[tokio::test]
async fn foreign_instance_and_missing_owned_lease_refuse_before_wait_dispatch() {
    let mut foreign = request(8);
    let CollectionSubject::Shared { resource } = &mut foreign.subject else { panic!() };
    resource.native.id = "another-native-instance".into();
    let mut reader =
        StateSource::with_io(FakeRead::new(replies(3)).with_generation(1), &foreign, "fixture.invalid".into(), 1433)
            .unwrap();
    assert_eq!(CollectionStop::from_error(&reader.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(reader.query_count(), 2);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let mut reader =
        StateSource::with_io(FakeRead::new(replies(3)), &request(8), "fixture.invalid".into(), 1433).unwrap();
    assert_eq!(CollectionStop::from_error(&reader.describe().await.unwrap_err()), CollectionStop::SourceUnavailable);
    assert_eq!(reader.query_count(), 2);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn changed_process_epoch_member_or_final_clock_cannot_verify_the_original_waits() {
    for changed in ["process", "epoch", "member", "clock"] {
        let mut replies = replies(3);
        match changed {
            "process" => replies[5] = Ok(vec![replace(&member(3, FINAL), &[("process_id", Val::Int(43))])]),
            "epoch" => replies[6] = Ok(vec![epoch(101)]),
            "member" => {
                replies[5] =
                    Ok(vec![replace(&member(3, FINAL), &[("server_name", Val::Text("another-server".into()))])])
            }
            "clock" => replies[5] = Ok(vec![member(3, FROM)]),
            _ => unreachable!(),
        }
        let mut source = source(replies, 8);
        source.describe().await.unwrap();
        drain(&mut source).await;
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Changed, "{changed}");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn denied_wait_population_is_unavailable_while_a_complete_zero_population_stays_empty() {
    for denied in [false, true] {
        let mut replies = replies(8);
        replies[3] = if denied {
            Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "fixture permission denial")))
        } else {
            Ok(vec![])
        };
        let mut source = source(replies, 8);
        let original = source.describe().await.unwrap();
        let coverage = &original.observation.snapshot().unwrap().sources[0];
        assert_eq!(coverage.state, if denied { SourceState::Unavailable } else { SourceState::Empty });
        assert_eq!(coverage.error_code, denied.then_some(ErrorCode::PermissionDenied));
        assert_eq!(coverage.complete_scope, !denied);
        assert_eq!(coverage.records, 0);
        let rows = drain(&mut source).await;
        assert!(rows.is_empty());
        assert!(WaitCounterSnapshot::from_source(&original, vec![], true).unwrap().is_valid());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 7);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn native_cancellation_stops_the_same_owner_before_clock_or_final_dispatch() {
    for stopped in [CollectionStop::Cancelled, CollectionStop::CancellationUnconfirmed, CollectionStop::TimedOut] {
        let mut replies = replies(3);
        replies[3] = Err(ReadError::Stopped(stopped));
        let mut source = source(replies, 8);
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stopped);
        assert_eq!(source.query_count(), 4);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
    }
}

#[tokio::test]
async fn retired_transport_preserves_original_error_without_replacement_native_work() {
    let mut replies = replies(3);
    replies[3] = Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "original optional denial")));
    let mut source = source(replies, 8);
    source.io = source.io.change_generation_at(4);
    assert_eq!(source.describe().await.unwrap_err().code, ErrorCode::PermissionDenied);
    assert_eq!(source.query_count(), 4, "no closing clock, final recheck or replacement query after retirement");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    for retired_after in [3, 4, 5] {
        let mut source = self::source(self::replies(3), 8);
        source.io = source.io.change_generation_at(retired_after);
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
        assert_eq!(source.query_count(), retired_after, "no next dispatch after a clock or data read loses its lease");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn a_tighter_caller_cannot_omit_the_final_epoch_or_publish_a_native_prefix() {
    let mut source = source(replies(3), 7);
    source.describe().await.unwrap();
    drain(&mut source).await;
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 6);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let caller = CollectionBudget {
        max_queries: 4,
        max_snapshot_rows: 2,
        page_rows: 1,
        page_bytes: 512,
        total_bytes: 1024,
        statement_timeout_ms: 100,
        total_timeout_ms: 1000,
        ..Default::default()
    };
    assert_eq!(bounded_budget(&caller), caller);
    let mut replies = self::replies(3);
    replies[3] = Err(ReadError::Stopped(CollectionStop::RowLimit));
    let mut source = self::source(replies, 8);
    let original = source.describe().await.unwrap();
    let coverage = &original.observation.snapshot().unwrap().sources[0];
    assert_eq!(coverage.state, SourceState::Unavailable);
    assert_eq!(coverage.stopped, Some(CollectionStop::RowLimit));
    assert!(drain(&mut source).await.is_empty());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn unconfirmed_close_remains_visible_and_closed_source_cannot_supply_more_pages() {
    let mut source = source(replies(3), 8);
    source.io.shutdown = SourceShutdown::Unconfirmed;
    source.describe().await.unwrap();
    drain(&mut source).await;
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
    assert_eq!(source.io.closed, 1);
    assert!(source.read_page(TelemetryStream::WaitCounterSamples, None).await.is_err());
}

#[tokio::test]
async fn unsupported_platform_or_native_version_cannot_start_a_server_wait_census() {
    for (edition, major) in [(5, "16"), (3, "10")] {
        let mut replies = replies(edition);
        replies[0] = Ok(vec![replace(&member(edition, "2020-01-01T06:00:00Z"), &[("major", Val::Text(major.into()))])]);
        let mut source = source(replies, 8);
        assert_eq!(source.describe().await.unwrap_err().code, ErrorCode::Unsupported);
        assert_eq!(source.query_count(), 1);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
