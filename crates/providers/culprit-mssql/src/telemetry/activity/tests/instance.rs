//! Controlled native-shaped transport exercises production reader/decoder/closure; not platform/cost acceptance.
use super::*;
use crate::{
    activity::census_scope::CensusScope,
    read::{NativeRead, Parameter, ReadError, ReadResult},
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};
const FROM: &str = "2020-01-01T06:00:00Z";
const UNTIL: &str = "2020-01-01T06:00:01Z";
type Calls = std::sync::Arc<std::sync::Mutex<Vec<(Vec<i64>, usize)>>>;
struct ObservedRead {
    native: FakeRead,
    calls: Calls,
}
#[async_trait::async_trait]
impl NativeRead for ObservedRead {
    async fn query(&mut self, sql: &str, parameters: Vec<Parameter>, limit: usize) -> ReadResult<Vec<Rec>> {
        let integers = parameters
            .iter()
            .map(|parameter| match parameter {
                Parameter::Integer(value) => *value,
                _ => panic!("this controlled native metadata fixture expects only integer arguments"),
            })
            .collect();
        self.calls.lock().unwrap().push((integers, limit));
        self.native.query(sql, parameters, limit).await
    }
    fn connection_generation(&self) -> Option<u64> {
        self.native.connection_generation()
    }
    fn queries(&self) -> u32 {
        self.native.queries()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.native.close().await
    }
}

async fn instance() -> SharedResourceIdentity {
    instance_on(8).await
}
async fn instance_on(edition: i64) -> SharedResourceIdentity {
    let mut io = FakeRead::new(vec![Ok(vec![platform(edition)]), Ok(vec![crate::instance::test_support::epoch(100)])])
        .with_generation(1);
    crate::instance::read(&mut io).await.unwrap().0.identity
}
fn platform(edition: i64) -> Rec {
    replace(
        &crate::instance::test_support::metadata(),
        &[("edition", Val::Int(edition)), ("major", Val::Text(if edition == 8 { "12" } else { "16" }.into()))],
    )
}
fn authority(edition: i64, at: &str) -> Rec {
    replace(
        &crate::activity::database_context::tests::row(edition),
        &[("major", Val::Int(if edition == 8 { 12 } else { 16 })), ("observed_at", Val::Text(at.into()))],
    )
}
fn actor(request: i64, database: &str, native_database: i64) -> Rec {
    replace(
        &crate::activity::tests::metadata(42),
        &[
            ("edition", Val::Int(8)),
            ("major", Val::Int(12)),
            ("scope_basis", Val::Int(6)),
            ("request_id", Val::Int(request)),
            ("session_database", Val::Text(database.into())),
            ("request_database_id", Val::Int(native_database)),
            ("elapsed_ms", Val::Int(120000)),
        ],
    )
}
fn actor_on(edition: i64, request: i64, database: &str, native_database: i64) -> Rec {
    replace(
        &actor(request, database, native_database),
        &[("edition", Val::Int(edition)), ("major", Val::Int(if edition == 8 { 12 } else { 16 }))],
    )
}
fn script(edition: i64, rows: Vec<Rec>) -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![platform(edition)]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![authority(edition, FROM)]),
        Ok(rows),
        Ok(vec![platform(edition)]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![authority(edition, UNTIL)]),
    ]
}
fn frozen(io: FakeRead, request: &CollectionRequest) -> culprit_read::FrozenSource<NativeActivityReader<ObservedRead>> {
    observed_source(io, request).0
}
fn observed_source(
    io: FakeRead,
    request: &CollectionRequest,
) -> (culprit_read::FrozenSource<NativeActivityReader<ObservedRead>>, Calls) {
    let calls: Calls = Default::default();
    let reader = ActivityReader::new(
        ObservedRead { native: io, calls: calls.clone() },
        ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    )
    .with_census_scope(CensusScope::from_request(request).unwrap());
    (
        culprit_read::FrozenSource::new(NativeActivityReader { reader, verified: None, profile: None }, request)
            .unwrap(),
        calls,
    )
}

async fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Shared { resource: instance().await },
        selection: CollectionSelection::Activity,
        budget: bounded_budget(&CollectionBudget::default()),
    }
}
async fn drain(source: &mut culprit_read::FrozenSource<NativeActivityReader<ObservedRead>>) -> Vec<TelemetryRecord> {
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::ActivityObservations, cursor.as_deref()).await.unwrap()
        else {
            panic!()
        };
        rows.extend(records);
        cursor = next_cursor;
        if cursor.is_none() {
            return rows;
        }
    }
}

#[tokio::test]
async fn actual_reader_uses_all_instance_parameter_preserves_cross_database_mars_and_completes_without_sql_enrichment()
{
    let request = request().await;
    let (mut source, calls) = observed_source(
        FakeRead::new(script(8, vec![actor(0, "shop", 7), actor(1, "other_db", 8)])).with_generation(1),
        &request,
    );
    let description = source.describe().await.unwrap();
    let CollectionSubject::Shared { resource } = &request.subject else { panic!() };
    assert_eq!(description.target, CollectionTarget::Shared(resource.clone()));
    assert!(description.has_qualified_instance_activity());
    let native = description.observation.activity().unwrap();
    assert_eq!(native.scope, culprit_core::MeasurementScope::Server);
    assert_eq!(native.database_writable, None);
    let records = drain(&mut source).await;
    assert_eq!(records.len(), 2);
    for record in &records {
        let TelemetryRecord::Activity(actor) = record else { panic!() };
        assert_eq!(actor.scope, ActivityScopeBasis::InstanceMember);
    }
    assert_eq!(source.query_count(), 7, "the real source additionally pays SET1 under the unchanged8 ceiling");
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 7);
    assert_eq!(calls.lock().unwrap()[3], (vec![501, 0], 501));
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn wrong_requested_member_edition_unknown_role_or_native_process_stops_without_promoting_a_database_frame() {
    for case in ["target", "edition", "role", "process", "frame-population"] {
        let mut request = request().await;
        let mut replies = script(8, vec![actor(0, "other_db", 8)]);
        match case {
            "target" => {
                let CollectionSubject::Shared { resource } = &mut request.subject else { panic!() };
                resource.native.id = "different-instance".into();
            }
            "edition" => {
                replies[0] = Ok(vec![platform(6)]);
            }
            "role" => replies[2] = Ok(vec![replace(&authority(8, FROM), &[("updateability", Val::Null)])]),
            "process" => {
                replies[1] =
                    Ok(vec![replace(&crate::instance::test_support::epoch(100), &[("started_at_ticks", Val::Null)])])
            }
            "frame-population" => replies[3] = Ok(vec![replace(&actor(0, "shop", 7), &[("scope_basis", Val::Int(1))])]),
            _ => unreachable!(),
        }
        let mut source = frozen(FakeRead::new(replies).with_generation(1), &request);
        assert!(source.describe().await.is_err(), "{case}");
        assert!(source.query_count() <= 4);
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn process_role_recovery_or_transport_changes_and_late_errors_do_not_complete_or_reopen_an_instance_census() {
    for case in ["process", "role", "recovery", "transport", "late-error", "caller7"] {
        let mut request = request().await;
        let mut replies = script(8, vec![actor(0, "other_db", 8)]);
        match case {
            "process" => replies[5] = Ok(vec![crate::instance::test_support::epoch(101)]),
            "role" => {
                replies[6] =
                    Ok(vec![replace(&authority(8, UNTIL), &[("updateability", Val::Text("READ_ONLY".into()))])])
            }
            "recovery" => {
                replies[6] = Ok(vec![replace(
                    &authority(8, UNTIL),
                    &[("recovery_fork_guid", Val::Text("another-native-fork".into()))],
                )])
            }
            "late-error" => replies[6] = Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)),
            _ => (),
        }
        let mut io = FakeRead::new(replies).with_generation(1);
        if case == "transport" {
            io = io.change_generation_at(4);
        }
        if case == "caller7" {
            request.budget.max_queries = 7;
            io = io.with_query_limit(6);
        }
        if case == "late-error" {
            io.shutdown = SourceShutdown::Unconfirmed;
        }
        let mut source = frozen(io, &request);
        assert!(source.describe().await.is_err(), "{case}");
        if case == "transport" {
            assert_eq!(source.query_count(), 4, "no native dispatch under a replacement transport");
        }
        let count = source.query_count();
        assert!(source.describe().await.is_err());
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(
            source.close().await,
            if case == "late-error" { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
        );
    }
}
#[tokio::test]
async fn empty_partial_prefix_and_cancelled_census_keep_native_coverage_and_original_lifecycle_distinct() {
    for prefix in [false, true] {
        let request = request().await;
        let rows = if prefix {
            (0..501)
                .map(|id| {
                    let row = actor(0, "other_db", 8);
                    replace(&row, &[("session_id", Val::Int(id + 10))])
                })
                .collect()
        } else {
            vec![replace(&actor(0, "shop", 7), &[("session_id", Val::Null)])]
        };
        let mut source = frozen(FakeRead::new(script(8, rows)).with_generation(1), &request);
        let description = source.describe().await.unwrap();
        let native = description.observation.activity().unwrap();
        assert_eq!(native.coverage.complete_scope, !prefix);
        assert_eq!(
            native.coverage.state,
            if prefix { culprit_core::SourceState::Partial } else { culprit_core::SourceState::Empty }
        );
        // The original caller's byte/page limits page the same 500-row frame; no native statement is repeated.
        let records = drain(&mut source).await;
        assert_eq!(records.len(), if prefix { 500 } else { 0 });
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let request = request().await;
    let mut io = FakeRead::new(script(8, vec![actor(0, "shop", 7)])).with_generation(1).pause_on_query(4);
    io.shutdown = SourceShutdown::Unconfirmed;
    let mut source = frozen(io, &request);
    assert!(tokio::time::timeout(std::time::Duration::from_millis(10), source.describe()).await.is_err());
    assert_eq!(source.query_count(), 4);
    assert!(source.describe().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
    assert_eq!(source.query_count(), 4);
}

#[tokio::test]
async fn constructor_refuses_foreign_engine_node_or_pool_before_any_native_source_work() {
    for case in ["foreign-engine", "node", "pool"] {
        let mut request = request().await;
        let CollectionSubject::Shared { resource } = &mut request.subject else { panic!() };
        match case {
            "foreign-engine" => resource.engine = culprit_core::Engine::Postgres,
            "node" => resource.kind = SharedResourceKind::Node,
            "pool" => resource.kind = SharedResourceKind::Pool,
            _ => unreachable!(),
        }
        let pool = std::sync::Arc::new(crate::conn::Pool::new(tiberius::Config::new(), None));
        assert!(ActivitySource::new(pool, &request, "fixture.invalid".into(), 9).is_err(), "{case}");
    }
}

#[tokio::test]
async fn limited_visibility_retains_instance_rows_only_as_an_explicit_partial_population() {
    let request = request().await;
    let mut replies = script(8, vec![actor(0, "other_db", 8)]);
    replies[2] = Ok(vec![replace(&authority(8, FROM), &[("database_wide", Val::Int(0))])]);
    replies[6] = Ok(vec![replace(&authority(8, UNTIL), &[("database_wide", Val::Int(0))])]);
    let mut source = frozen(FakeRead::new(replies).with_generation(1), &request);
    let description = source.describe().await.unwrap();
    let frame = description.observation.activity().unwrap();
    assert!(description.has_qualified_instance_activity());
    assert_eq!(frame.coverage.state, culprit_core::SourceState::Partial);
    assert!(!frame.coverage.complete_scope);
    assert_eq!(frame.coverage.error_code, Some(culprit_core::ErrorCode::PermissionDenied));
    assert_eq!(frame.coverage.stopped, Some(CollectionStop::SourceUnavailable));
    assert_eq!(drain(&mut source).await.len(), 1);
    assert_eq!(source.query_count(), 7);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn boxed_and_managed_instance_census_uses_the_same_known_native_profile_role_and_unfiltered_parameter() {
    for edition in crate::instance::SUPPORTED_EDITIONS {
        for writable in [true, false] {
            let resource = instance_on(*edition).await;
            let request = CollectionRequest {
                subject: CollectionSubject::Shared { resource: resource.clone() },
                selection: CollectionSelection::Activity,
                budget: bounded_budget(&CollectionBudget::default()),
            };
            let mut replies = script(*edition, vec![actor_on(*edition, 0, "other_db", 8)]);
            if !writable {
                for index in [2, 3, 6] {
                    let Ok(rows) = &mut replies[index] else { unreachable!() };
                    for row in rows {
                        *row = replace(row, &[("updateability", Val::Text("READ_ONLY".into()))]);
                    }
                }
            }
            let (mut source, calls) = observed_source(FakeRead::new(replies).with_generation(1), &request);
            let description = source.describe().await.unwrap();
            assert!(description.has_qualified_instance_activity());
            assert_eq!(description.target, CollectionTarget::Shared(resource));
            assert!(description.target.database().is_none());
            assert_eq!(description.observation.activity().unwrap().database_writable, None);
            let rows = drain(&mut source).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
            assert_eq!(source.query_count(), 7);
            assert_eq!(calls.lock().unwrap()[3], (vec![501, 0], 501));
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}
#[tokio::test]
async fn unknown_azure_or_older_native_profile_never_becomes_an_instance_census_or_a_managed_default() {
    let resource = instance_on(3).await;
    for (edition, major) in [(0, 16), (1, 16), (5, 16), (6, 16), (7, 16), (9, 16), (11, 16), (12, 16), (3, 10)] {
        let request = CollectionRequest {
            subject: CollectionSubject::Shared { resource: resource.clone() },
            selection: CollectionSelection::Activity,
            budget: bounded_budget(&CollectionBudget::default()),
        };
        let native = replace(&platform(edition), &[("major", Val::Text(major.to_string()))]);
        let mut replies = vec![Ok(vec![native])];
        if edition == 5 {
            replies.push(Ok(vec![authority(5, FROM)]));
        }
        let (mut source, calls) = observed_source(FakeRead::new(replies).with_generation(1), &request);
        assert!(source.describe().await.is_err(), "native{edition}/{major}");
        assert!(source.query_count() <= 2);
        assert!(
            calls.lock().unwrap().iter().all(|(parameters, _)| parameters != &[501, 0]),
            "no instance census was dispatched for native{edition}/{major}",
        );
        let count = source.query_count();
        assert!(source.describe().await.is_err());
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn boxed_instance_keeps_exact_database_acquisition_witness_final_fences_and_stricter_caller() {
    for case in [
        "missing-file",
        "missing-recovery",
        "missing-role",
        "wrong-target",
        "changed-role",
        "changed-process",
        "changed-transport",
        "late-error",
        "caller7",
    ] {
        let mut request = CollectionRequest {
            subject: CollectionSubject::Shared { resource: instance_on(3).await },
            selection: CollectionSelection::Activity,
            budget: bounded_budget(&CollectionBudget::default()),
        };
        let mut replies = script(3, vec![actor_on(3, 0, "other_db", 8)]);
        match case {
            "missing-file" | "missing-recovery" | "missing-role" => {
                let field = match case {
                    "missing-file" => "database_file_guid",
                    "missing-recovery" => "recovery_fork_guid",
                    _ => "updateability",
                };
                replies[2] = Ok(vec![replace(&authority(3, FROM), &[(field, Val::Null)])]);
            }
            "wrong-target" => {
                let CollectionSubject::Shared { resource } = &mut request.subject else { unreachable!() };
                resource.native.id = "other-native-instance".into();
            }
            "changed-role" => {
                replies[6] =
                    Ok(vec![replace(&authority(3, UNTIL), &[("updateability", Val::Text("READ_ONLY".into()))])])
            }
            "changed-process" => replies[5] = Ok(vec![crate::instance::test_support::epoch(101)]),
            "late-error" => replies[6] = Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)),
            _ => (),
        }
        let mut io = FakeRead::new(replies).with_generation(1);
        if case == "changed-transport" {
            io = io.change_generation_at(4);
        }
        if case == "caller7" {
            request.budget.max_queries = 7;
            io = io.with_query_limit(6);
        }
        if case == "late-error" {
            io.shutdown = SourceShutdown::Unconfirmed;
        }
        let mut source = frozen(io, &request);
        assert!(source.describe().await.is_err(), "{case}");
        let count = source.query_count();
        if case == "changed-transport" {
            assert_eq!(count, 4);
        }
        assert!(source.describe().await.is_err());
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), count);
        assert_eq!(
            source.close().await,
            if case == "late-error" { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
        );
    }
}
