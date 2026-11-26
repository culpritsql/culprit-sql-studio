use super::*;
use crate::{
    oom_events::tests::{platform, replace, row},
    resources::{pool::test_support::identity_row, test_support::FakeRead},
    value::{Rec, Val},
};
use culprit_core::{Engine, SourceState};

fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Shared {
            resource: SharedResourceIdentity {
                engine: Engine::Mssql,
                kind: SharedResourceKind::Pool,
                namespace: NativeIdentity::new("fixture-server").qualified("kind", "azure-sql-logical-server"),
                native: NativeIdentity::new("fixture-pool"),
            },
        },
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::MemoryFailures },
        budget: CollectionBudget { page_rows: 1, ..Default::default() },
    }
}
fn source(after: Rec, failure: Option<ReadError>) -> StateSource<FakeRead> {
    source_with_request(after, failure, &request())
}
fn source_with_request(after: Rec, failure: Option<ReadError>, request: &CollectionRequest) -> StateSource<FakeRead> {
    let mut replies = vec![
        Ok(vec![platform(5)]),
        Ok(vec![identity_row()]),
        Ok(vec![super::tests::clock(false, "2020-01-01T06:02:00Z")]),
        Ok(vec![super::tests::pressure(false)]),
        Ok(vec![platform(5)]),
        failure.map_or_else(|| Ok(vec![row(), row()]), Err),
    ];
    replies.extend([Ok(vec![super::tests::clock(false, "2020-01-01T06:02:01Z")]), Ok(vec![after])]);
    let reader_budget = ReadSession::budget_after_setup(native_budget(request)).unwrap();
    let io = FakeRead::new(replies).with_query_limit(reader_budget.max_queries);
    StateSource::with_io(io, request, "member-alias.invalid".into(), 1433).unwrap()
}

#[tokio::test]
async fn nine_total_queries_keep_setup_and_both_clocks_without_widening_a_stricter_caller() {
    let limits = native_budget(&request());
    assert_eq!(limits.max_queries, 9);
    assert_eq!(limits.max_snapshot_rows, 128);
    assert_eq!(limits.statement_timeout_ms, 3000);
    assert_eq!(limits.total_timeout_ms, 10_000);
    for limit in [9, 8] {
        let mut requested = request();
        requested.budget.max_queries = limit;
        let mut source = source_with_request(identity_row(), None, &requested);
        source.describe().await.unwrap();
        let _ = source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
        let _ = source.read_page(TelemetryStream::OperationalSamples, Some("1")).await.unwrap();
        if limit == 9 {
            assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        } else {
            assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
        }
        assert_eq!(source.query_count(), limit - 1, "the reader cannot spend its reserved session setup slot");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn constructing_shared_oom_and_pool_operational_profiles_is_read_free_with_independent_caps() {
    let pool =
        Arc::new(crate::conn::Pool::new(crate::test_support::config(1, "never-opened", "unused", "culprit"), None));
    let mut source = StateSource::new(pool, &request(), "unused.invalid".into(), 1).unwrap();
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut other = request();
    other.selection = CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational };
    assert!(pool_operational::selected(&other));
    let limits = native_budget(&other);
    assert_eq!(limits.max_queries, 22, "shared tempdb retains its own admission and native point");
    assert_eq!(limits.max_snapshot_rows, other.budget.max_snapshot_rows);
    assert_eq!(limits.page_bytes, 512 * 1024);
    assert_eq!(limits.total_bytes, 8 * 1024 * 1024);
    assert_eq!(limits.statement_timeout_ms, 1000);
    assert_eq!(limits.total_timeout_ms, 15000);
    assert_eq!(native_budget(&request()).max_queries, 9, "the OOM ledger remains independent");
    let mut source = StateSource::with_io(FakeRead::new(vec![]), &other, "route".into(), 1433).unwrap();
    assert_eq!(source.request.budget, limits);
    assert!(source.io.calls.is_empty(), "constructing a pool profile never acquires its native owner");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    other.budget = CollectionBudget {
        max_queries: 7,
        max_snapshot_rows: 4,
        page_rows: 1,
        page_bytes: 1024,
        total_bytes: 2048,
        max_pages: 3,
        max_sql_bytes: 1024,
        max_plan_bytes: 1024,
        statement_timeout_ms: 20,
        total_timeout_ms: 40,
    };
    other.budget.validate().unwrap();
    let bounded = native_budget(&other);
    assert_eq!(bounded, other.budget, "every stricter caller dimension stays selected");
}

#[tokio::test]
async fn explicit_pool_capture_pages_a_frozen_native_multiset_and_rechecks_the_exact_owner() {
    let mut source = source(identity_row(), None);
    let description = source.describe().await.unwrap();
    assert!(
        matches!(description.target, CollectionTarget::Shared(ref target) if target.kind==SharedResourceKind::Pool)
    );
    let snapshot = description.observation.snapshot().unwrap();
    assert_eq!(snapshot.dataset, SnapshotDataset::MemoryFailures);
    assert_eq!(snapshot.records, 2);
    assert_eq!(snapshot.sources[0].state, SourceState::Collected);
    let mut positions = vec![];
    let mut keys = vec![];
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::OperationalSamples, cursor.as_deref()).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(records.len(), 1);
        for record in records {
            let TelemetryRecord::Operational(observation) = record else { panic!() };
            keys.push(observation.key);
            let OperationalFact::MssqlOutOfMemory(fact) = observation.fact else { panic!() };
            positions.push(fact.position);
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(positions, [0, 1]);
    assert_ne!(keys[0], keys[1]);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 8);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn a_changed_member_role_or_pool_cannot_publish_the_buffer_as_verified() {
    for after in [
        replace(identity_row(), "pool_name", Val::Text("changed-pool".into())),
        replace(identity_row(), "physical_incarnation", Val::Text("changed-process".into())),
    ] {
        let mut source = source(after, None);
        source.describe().await.unwrap();
        let _ = source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
        let _ = source.read_page(TelemetryStream::OperationalSamples, Some("1")).await.unwrap();
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Changed);
        source.close().await;
    }
}

#[tokio::test]
async fn terminal_native_reads_stop_before_final_clock_and_identity_sql() {
    for stop in [CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed, CollectionStop::ByteLimit] {
        let mut source = source(identity_row(), Some(ReadError::Stopped(stop)));
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stop);
        assert_eq!(source.query_count(), 6);
        assert!(source.finish().await.is_err());
        source.close().await;
    }
}

#[test]
fn memory_failure_subject_refusals_and_the_independent_pool_operational_route_stay_explicit() {
    for fault in ["database", "node", "foreign-engine"] {
        let mut invalid = request();
        match fault {
            "database" => invalid.subject = CollectionSubject::Database,
            "node" => {
                let CollectionSubject::Shared { resource } = &mut invalid.subject else { panic!() };
                resource.kind = SharedResourceKind::Node;
            }
            "foreign-engine" => {
                let CollectionSubject::Shared { resource } = &mut invalid.subject else { panic!() };
                resource.engine = Engine::Postgres;
            }
            _ => unreachable!(),
        }
        assert!(StateSource::with_io(FakeRead::new(vec![]), &invalid, "route".into(), 1433).is_err(), "{fault}");
    }
    let families = point::shared_families(SnapshotDataset::MemoryFailures).unwrap();
    assert_eq!(families.len(), 1);
    assert!(matches!(families[0], point::Family::MemoryFailures));
    assert!(
        !point::shared_families(SnapshotDataset::Operational)
            .unwrap()
            .iter()
            .any(|family| matches!(family, point::Family::MemoryFailures))
    );
    let mut operational = request();
    operational.selection = CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational };
    assert!(pool_operational::selected(&operational));
    let source = StateSource::with_io(FakeRead::new(vec![]), &operational, "route".into(), 1433).unwrap();
    assert_eq!(source.request.selection, operational.selection);
    assert_eq!(source.request.budget.max_queries, 22);
    assert!(source.io.calls.is_empty());
    let mut refused = operational;
    refused.selection = CollectionSelection::Snapshot { dataset: SnapshotDataset::Tuning };
    assert!(StateSource::with_io(FakeRead::new(vec![]), &refused, "route".into(), 1433).is_err());
}
