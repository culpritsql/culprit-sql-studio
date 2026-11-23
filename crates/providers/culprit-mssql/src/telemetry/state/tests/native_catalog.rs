use super::*;

const FROM: &str = "2020-01-01T06:02:00Z";
const UNTIL: &str = "2020-01-01T06:02:01Z";
const FINAL: &str = "2020-01-01T06:02:02Z";

fn authority(edition: i64, writable: Option<bool>, at: &str) -> Rec {
    replace(
        &crate::telemetry::history_tests::authority(edition),
        &[
            ("selected_database_id", Val::Int(5)),
            ("catalog_database_id", Val::Int(5)),
            ("major", Val::Int(if edition == 8 { 12 } else { 16 })),
            ("observed_at", Val::Text(at.into())),
            (
                "updateability",
                writable
                    .map(|value| Val::Text(if value { "READ_WRITE" } else { "READ_ONLY" }.into()))
                    .unwrap_or(Val::Null),
            ),
        ],
    )
}
fn native_owner(edition: i64, row: Rec) -> Vec<crate::read::ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![replace(
            &crate::telemetry::history_tests::native_platform(edition),
            &[("major", Val::Text(if edition == 8 { "12" } else { "16" }.into()))],
        )]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![row]),
    ]
}
fn script(edition: i64, visible: bool, links: bool, empty: bool, after: Rec) -> Vec<crate::read::ReadResult<Vec<Rec>>> {
    let mut replies = vec![Ok(vec![Rec::of(&[("edition", Val::Int(edition))])])];
    replies.extend(native_owner(edition, authority(edition, Some(true), FROM)));
    replies.push(Ok(vec![clock(visible, FROM)]));
    if visible {
        replies.push(Ok(vec![crate::resources::test_support::instance_pressure_row()]));
        replies.push(Ok(if empty { vec![] } else { vec![crate::missing_indexes::tests::row()] }));
        if !empty {
            replies.push(if links {
                Ok(vec![])
            } else {
                Err(ReadError::Source(culprit_core::Error::new(
                    ErrorCode::PermissionDenied,
                    "controlled native associations unavailable",
                )))
            });
        }
    }
    replies.push(Ok(vec![clock(visible, UNTIL)]));
    replies.extend(native_owner(edition, after));
    replies
}
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::MissingIndexes },
        budget: crate::telemetry::streamed::catalog_budget(&CollectionBudget::default()),
    }
}
fn capture(native: FakeRead, request: &CollectionRequest) -> StateSource<FakeRead> {
    StateSource::with_io(native, request, "fixture.invalid".into(), 1433).unwrap()
}
async fn drain(source: &mut StateSource<FakeRead>) -> Vec<TelemetryRecord> {
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!("bounded hints EOF")
    };
    records
}

#[tokio::test]
async fn native_missing_index_hints_use_the_existing_decoder_and_known_read_write_owner_with_full_budget() {
    for edition in [2, 3, 4, 8] {
        let native =
            FakeRead::new(script(edition, true, true, false, authority(edition, Some(true), FINAL))).with_generation(1);
        let mut source = capture(native, &request());
        let description = source.describe().await.unwrap();
        assert!(matches!(description.observation, SourceObservation::LocalResource { database_writable: true, .. }));
        assert!(description.target.database().unwrap().incarnation.is_none());
        assert!(NativeReadContext::from_snapshot(&description).unwrap().has_qualified_local_owner());
        let rows = drain(&mut source).await;
        assert_eq!(rows.len(), 1);
        let TelemetryRecord::Operational(row) = &rows[0] else { panic!() };
        let OperationalFact::MssqlMissingIndex(hint) = &row.fact else { panic!() };
        assert_eq!(hint.table.database_id, 5);
        assert_eq!(hint.equality, ["tenant]id", "owner, name"]);
        assert_eq!(hint.query_hashes, Some(vec![]));
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 12, "plus the real source SET1 fits the shared13 ceiling");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.closed, 1);
    }
}

#[tokio::test]
async fn native_catalog_hints_keep_empty_partial_and_invisible_metadata_distinct() {
    for (visible, links, empty, expected) in [
        (true, true, true, SourceState::Empty),
        (true, false, false, SourceState::Partial),
        (false, true, true, SourceState::Unavailable),
    ] {
        let native =
            FakeRead::new(script(8, visible, links, empty, authority(8, Some(true), FINAL))).with_generation(1);
        let mut source = capture(native, &request());
        let description = source.describe().await.unwrap();
        let coverage = &description.observation.snapshot().unwrap().sources[0];
        assert_eq!(coverage.state, expected);
        assert_eq!(coverage.complete_scope, expected == SourceState::Empty);
        let rows = drain(&mut source).await;
        assert_eq!(rows.is_empty(), empty);
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn native_hints_refuse_read_only_unknown_role_and_foreign_rows_before_optional_association_work() {
    for writable in [Some(false), None] {
        let mut replies = script(8, true, true, false, authority(8, Some(true), FINAL));
        replies[3] = Ok(vec![authority(8, writable, FROM)]);
        let mut source = capture(FakeRead::new(replies).with_generation(1), &request());
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), 4);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut replies = script(8, true, true, false, authority(8, Some(true), FINAL));
    replies[6] = Ok(vec![replace(&crate::missing_indexes::tests::row(), &[("database_id", Val::Int(6))])]);
    let mut source = capture(FakeRead::new(replies).with_generation(1), &request());
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 7, "no association SQL after contradictory native DB identity");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn catalog_hint_final_owner_caller_and_failed_shutdown_cannot_be_replaced_or_certified() {
    for case in ["fork", "role", "caller-12", "unconfirmed"] {
        let after = match case {
            "fork" => {
                replace(&authority(8, Some(true), FINAL), &[("recovery_fork_guid", Val::Text("another-fork".into()))])
            }
            "role" => authority(8, Some(false), FINAL),
            _ => authority(8, Some(true), FINAL),
        };
        let mut replies = script(8, true, true, false, after);
        if case == "unconfirmed" {
            replies[5] = Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed));
        }
        let mut native = FakeRead::new(replies).with_generation(1);
        if case == "caller-12" {
            native = native.with_query_limit(11);
        }
        if case == "unconfirmed" {
            native.shutdown = SourceShutdown::Unconfirmed;
        }
        let mut request = request();
        if case == "caller-12" {
            request.budget.max_queries = 12;
        }
        let mut source = capture(native, &request);
        if case == "unconfirmed" {
            assert_eq!(
                CollectionStop::from_error(&source.describe().await.unwrap_err()),
                CollectionStop::CancellationUnconfirmed
            );
        } else {
            source.describe().await.unwrap();
            drain(&mut source).await;
            assert!(source.finish().await.is_err(), "{case}");
        }
        let count = source.query_count();
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(
            source.close().await,
            if case == "unconfirmed" { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
        );
    }
}
