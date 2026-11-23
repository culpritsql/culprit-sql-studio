use super::*;
use crate::telemetry::{
    check_constraints::CheckConstraints, columns::Columns, foreign_keys::ForeignKeys,
    index_operations::IndexOperations, indexes::Indexes, objects::Objects, partitions::Partitions,
    statistics::Statistics, tables::Tables, triggers::Triggers,
};

const FROM: &str = "2020-01-01T06:02:00Z";
const UNTIL: &str = "2020-01-01T06:02:01Z";

fn authority(edition: i64, writable: bool, at: &str) -> Rec {
    replace(
        &crate::telemetry::history_tests::authority(edition),
        &[
            ("selected_database_id", Val::Int(5)),
            ("catalog_database_id", Val::Int(5)),
            ("observed_at", Val::Text(at.into())),
            ("major", Val::Int(if edition == 8 { 12 } else { 16 })),
            ("updateability", Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into())),
        ],
    )
}
fn native_owner(edition: i64, row: Rec) -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![replace(
            &crate::telemetry::history_tests::native_platform(edition),
            &[("major", Val::Text(crate::read::integer(&row, "major").unwrap().to_string()))],
        )]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![row]),
    ]
}
fn script<D: SnapshotRecipe>(edition: i64, before: Rec, after: Rec) -> Vec<ReadResult<Vec<Rec>>> {
    let mut replies = vec![Ok(vec![Rec::of(&[("edition", Val::Int(edition))])])];
    replies.extend(native_owner(edition, before));
    if D::REQUIRES_FULL_METADATA {
        replies.push(Ok(vec![clock(FROM)]));
    }
    replies.push(Ok(vec![crate::resources::test_support::instance_pressure_row()]));
    if D::DATASET == SnapshotDataset::IndexOperations {
        replies.push(Ok(vec![crate::index_operations::tests::layout_row(true, &[])]));
    }
    replies.push(Ok(vec![crate::resources::test_support::instance_pressure_row()]));
    replies.extend(native_owner(edition, after));
    if D::REQUIRES_FULL_METADATA {
        replies.push(Ok(vec![clock(UNTIL)]));
    }
    replies
}
fn source<D: SnapshotRecipe>(
    recipe: D,
    native: FakeRead,
    batches: Vec<ReadResult<ReadBatch>>,
    budget: CollectionBudget,
) -> StreamedSnapshot<D, StreamingRead> {
    let io = StreamingRead { native, batches: batches.into(), starts: 0, pause_batches: false };
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: D::DATASET },
        budget,
    };
    StreamedSnapshot::with_io(io, &request, "fixture.invalid".into(), 1433, recipe).unwrap()
}
async fn completes<D: SnapshotRecipe>(recipe: D, row: Rec, edition: i64, writable: bool) {
    let native =
        FakeRead::new(script::<D>(edition, authority(edition, writable, FROM), authority(edition, writable, UNTIL)))
            .with_generation(1);
    let mut source = source(recipe, native, vec![batch(row, true)], CollectionBudget::default());
    assert_eq!(source.query_count(), 0);
    let initial = source.describe().await.unwrap();
    assert!(initial.target.database().unwrap().incarnation.is_none());
    assert!(initial.partition.is_qualified());
    assert!(matches!(initial.partition, SourcePartition::InstanceLocal { .. }));
    let CollectionRead::Page { records, next_cursor } = source.read_page(D::DATASET.stream(), None).await.unwrap()
    else {
        panic!("catalog page")
    };
    assert_eq!(records.len(), 1);
    assert!(next_cursor.is_none());
    let completion = source.finish().await.unwrap();
    let completed = initial.completed(completion).unwrap().unwrap();
    let context = NativeReadContext::from_snapshot(&completed).unwrap();
    assert!(context.has_qualified_local_owner());
    assert_eq!(context.database_writable, Some(writable));
    assert_eq!(context.observed_from, canonical_time(FROM).unwrap());
    assert_eq!(context.observed_until, canonical_time(UNTIL).unwrap());
    assert_eq!(context.partition, initial.partition);
    assert!(matches!(completed.observation, SourceObservation::LocalResource { .. }));
    assert_eq!(source.inner.reader().io.starts, 1);
    assert!(source.query_count() + crate::read::ReadSession::SESSION_SETUP_QUERIES <= 13);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(source.inner.reader().io.native.closed, 1);
}

#[tokio::test]
async fn every_catalog_uses_the_same_stream_and_decoders_under_exact_boxed_or_managed_native_ownership() {
    for edition in [2, 3, 4, 8] {
        for writable in [true, false] {
            completes(Tables, crate::tables::tests::row(), edition, writable).await;
            completes(Indexes, crate::indexes::tests::row(), edition, writable).await;
            completes(IndexOperations, crate::index_operations::tests::row(), edition, writable).await;
            completes(Partitions, crate::partitions::tests::row(), edition, writable).await;
            completes(Columns, crate::columns::tests::row(), edition, writable).await;
            completes(Statistics, crate::statistics::tests::row(), edition, writable).await;
            completes(ForeignKeys, crate::foreign_keys::tests::row(), edition, writable).await;
            completes(CheckConstraints, crate::check_constraints::tests::row(), edition, writable).await;
            completes(Objects, crate::recorded::tests::row(), edition, writable).await;
            completes(Triggers, crate::triggers::tests::row(), edition, writable).await;
        }
    }
}

#[tokio::test]
async fn changed_file_recovery_creation_member_process_role_or_transport_cannot_certify_a_drained_catalog() {
    for edition in [3, 8] {
        for (field, value) in [
            ("database_file_guid", Val::Text("different-native-file".into())),
            ("database_guid", Val::Text("different-native-database".into())),
            ("recovery_fork_guid", Val::Text("different-native-fork".into())),
            ("database_created_without_offset", Val::Text("2020-01-01T05:00:00".into())),
            ("updateability", Val::Text("READ_ONLY".into())),
        ] {
            let after = replace(&authority(edition, true, UNTIL), &[(field, value)]);
            let native =
                FakeRead::new(script::<Tables>(edition, authority(edition, true, FROM), after)).with_generation(1);
            let mut source =
                source(Tables, native, vec![batch(crate::tables::tests::row(), true)], CollectionBudget::default());
            source.describe().await.unwrap();
            source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
            assert_eq!(
                CollectionStop::from_error(&source.finish().await.unwrap_err()),
                CollectionStop::SourceChanged,
                "{field}"
            );
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
        for change in ["member", "process", "transport"] {
            let mut replies =
                script::<Tables>(edition, authority(edition, true, FROM), authority(edition, true, UNTIL));
            if change == "member" {
                let Ok(rows) = &replies[7] else { panic!() };
                replies[7] = Ok(vec![replace(&rows[0], &[("server_name", Val::Text("different-member".into()))])]);
            } else if change == "process" {
                replies[8] = Ok(vec![crate::instance::test_support::epoch(101)]);
            }
            let native = FakeRead::new(replies).with_generation(1);
            let native = if change == "transport" { native.change_generation_at(8) } else { native };
            let mut source =
                source(Tables, native, vec![batch(crate::tables::tests::row(), true)], CollectionBudget::default());
            source.describe().await.unwrap();
            source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
            assert_eq!(
                CollectionStop::from_error(&source.finish().await.unwrap_err()),
                CollectionStop::SourceChanged,
                "{change}"
            );
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}

#[tokio::test]
async fn missing_native_owner_role_or_transport_stops_before_pressure_layout_and_stream() {
    for field in ["database_file_guid", "database_guid", "recovery_fork_guid", "updateability"] {
        let mut replies = script::<Tables>(8, authority(8, true, FROM), authority(8, true, UNTIL));
        replies[3] = Ok(vec![replace(&authority(8, true, FROM), &[(field, Val::Null)])]);
        let native = FakeRead::new(replies).with_generation(1);
        let mut source = source(Tables, native, vec![], CollectionBudget::default());
        assert!(source.describe().await.is_err(), "{field}");
        assert_eq!(source.query_count(), 4);
        assert_eq!(source.inner.reader().io.starts, 0);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let native = FakeRead::new(script::<Tables>(8, authority(8, true, FROM), authority(8, true, UNTIL)));
    let mut source = source(Tables, native, vec![], CollectionBudget::default());
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceUnavailable);
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.inner.reader().io.starts, 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn hidden_initial_metadata_never_becomes_an_empty_catalog_and_lost_final_visibility_stays_partial() {
    for final_visibility in [false, true] {
        let mut replies = script::<Tables>(8, authority(8, true, FROM), authority(8, true, UNTIL));
        let position = if final_visibility { 10 } else { 4 };
        replies[position] = Ok(vec![replace(&clock(UNTIL), &[("metadata_visibility", Val::Int(0))])]);
        let native = FakeRead::new(replies).with_generation(1);
        let mut source =
            source(Tables, native, vec![batch(crate::tables::tests::row(), true)], CollectionBudget::default());
        if !final_visibility {
            assert_eq!(source.describe().await.unwrap_err().code, culprit_core::ErrorCode::PermissionDenied);
            assert_eq!(source.inner.reader().io.starts, 0);
        } else {
            let initial = source.describe().await.unwrap();
            source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap();
            let completed = initial.completed(source.finish().await.unwrap()).unwrap().unwrap();
            let coverage = &completed.observation.snapshot().unwrap().sources[0];
            assert_eq!(coverage.state, culprit_core::SourceState::Partial);
            assert!(!coverage.complete_scope);
            assert_eq!(coverage.error_code, Some(culprit_core::ErrorCode::PermissionDenied));
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn only_a_drained_single_stream_can_recheck_and_empty_scope_is_confirmed_without_interleaved_sql() {
    for empty in [false, true] {
        let native =
            FakeRead::new(script::<Tables>(8, authority(8, true, FROM), authority(8, true, UNTIL))).with_generation(1);
        let batches = if empty {
            vec![Ok(ReadBatch { rows: vec![], complete: true })]
        } else {
            vec![batch(crate::tables::tests::row(), false), Ok(ReadBatch { rows: vec![], complete: true })]
        };
        let mut source = source(Tables, native, batches, CollectionBudget::default());
        let initial = source.describe().await.unwrap();
        let CollectionRead::Page { next_cursor, .. } =
            source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
        else {
            panic!()
        };
        if !empty {
            let count = source.query_count();
            assert!(source.finish().await.is_err());
            assert_eq!(source.query_count(), count, "no final metadata query while native cursor is active");
            source.read_page(TelemetryStream::OperationalSamples, next_cursor.as_deref()).await.unwrap();
        }
        let completed = initial.completed(source.finish().await.unwrap()).unwrap().unwrap();
        let coverage = &completed.observation.snapshot().unwrap().sources[0];
        assert_eq!(
            coverage.state,
            if empty { culprit_core::SourceState::Empty } else { culprit_core::SourceState::Collected }
        );
        assert_eq!(source.inner.reader().io.starts, 1);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn foreign_rows_late_errors_and_a_caller_that_cannot_pay_final_proof_never_publish_complete_catalogs() {
    for case in ["foreign-row", "late-error", "caller-12"] {
        let mut native =
            FakeRead::new(script::<Tables>(8, authority(8, true, FROM), authority(8, true, UNTIL))).with_generation(1);
        if case == "late-error" {
            native.shutdown = SourceShutdown::Unconfirmed;
        }
        let native = if case == "caller-12" { native.with_query_limit(11) } else { native };
        let row = if case == "foreign-row" {
            replace(&crate::tables::tests::row(), &[("database_id", Val::Int(6))])
        } else {
            crate::tables::tests::row()
        };
        let batches = if case == "late-error" {
            vec![Err(crate::read::ReadError::Stopped(CollectionStop::CancellationUnconfirmed))]
        } else {
            vec![batch(row, true)]
        };
        let budget =
            CollectionBudget { max_queries: if case == "caller-12" { 12 } else { 13 }, ..CollectionBudget::default() };
        let mut source = source(Tables, native, batches, budget);
        source.describe().await.unwrap();
        let page = source.read_page(TelemetryStream::OperationalSamples, None).await;
        let stop = if case == "caller-12" {
            page.unwrap();
            CollectionStop::from_error(&source.finish().await.unwrap_err())
        } else {
            CollectionStop::from_error(&page.unwrap_err())
        };
        assert_eq!(
            stop,
            match case {
                "foreign-row" => CollectionStop::SourceChanged,
                "late-error" => CollectionStop::CancellationUnconfirmed,
                _ => CollectionStop::QueryLimit,
            }
        );
        let count = source.query_count();
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(
            source.close().await,
            if case == "late-error" { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
        );
        assert_eq!(source.inner.reader().io.native.closed, 1);
    }
}

#[tokio::test]
async fn unsupported_boxed_profile_cannot_borrow_managed_or_azure_catalog_admission() {
    let mut replies = script::<Tables>(3, authority(3, true, FROM), authority(3, true, UNTIL));
    let Ok(platform) = &mut replies[1] else { panic!() };
    platform[0] = replace(&platform[0], &[("major", Val::Text("17".into()))]);
    let Ok(before) = &mut replies[3] else { panic!() };
    before[0] = replace(&before[0], &[("major", Val::Int(17))]);
    let native = FakeRead::new(replies).with_generation(1);
    let mut source = source(Tables, native, vec![], CollectionBudget::default());
    assert_eq!(source.describe().await.unwrap_err().code, culprit_core::ErrorCode::Unsupported);
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.inner.reader().io.starts, 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

mod native;
