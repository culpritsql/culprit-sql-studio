//! Scripted native facts exercise the real StateSource and existing decoders; no cost receipt/readiness is fabricated.
use super::*;
use crate::{
    read::ReadResult,
    resources::{
        pool::test_support::settings_member,
        test_support::{FakeRead, replace},
    },
    value::{Rec, Val},
};
use culprit_core::{Engine, MeasurementScope, SourceState};
const BEFORE: &str = "2020-01-01T06:00:00Z";
const AT: &str = "2020-01-01T06:00:01Z";
const UNTIL: &str = "2020-01-01T06:00:02Z";
const FINAL: &str = "2020-01-01T06:00:03Z";
const LOGICAL: &str = "11111111-1111-1111-1111-111111111111";
const PHYSICAL: &str = "22222222-2222-2222-2222-222222222222";
fn extend(row: &Rec, fields: &[(&str, Val)]) -> Rec {
    let mut values =
        row.columns().map(|(key, value)| (key.to_owned(), value.clone())).collect::<std::collections::BTreeMap<_, _>>();
    values.extend(fields.iter().map(|(key, value)| (key.to_string(), value.clone())));
    Rec::of(&values.iter().map(|(key, value)| (key.as_str(), value.clone())).collect::<Vec<_>>())
}
fn member(at: &str) -> Rec {
    extend(
        &settings_member(at),
        &[
            ("selected_database_id", Val::Int(7)),
            ("database_id", Val::Int(7)),
            ("pool_engine_edition", Val::Int(5)),
            ("native_tier", Val::Text("General Purpose".into())),
            ("server_performance_state", Val::Int(1)),
            ("metadata_visibility", Val::Int(1)),
        ],
    )
}
fn process() -> Rec {
    Rec::of(&[
        ("local_process_id", Val::Int(10)),
        ("local_started_ticks", Val::Int(100)),
        ("local_now_ticks", Val::Int(200)),
        ("local_started_without_offset", Val::Text("2019-12-31T23:00:00.000".into())),
        ("local_epoch_visible", Val::Bool(true)),
        ("replica_id", Val::Null),
    ])
}
fn roster() -> Vec<Rec> {
    vec![Rec::of(&[
        ("database_id", Val::Int(7)),
        ("database_name", Val::Text("fixture-db".into())),
        ("server_name", Val::Text("fixture-server".into())),
        ("logical_database_guid", Val::Text(LOGICAL.into())),
        ("physical_database_guid", Val::Text(PHYSICAL.into())),
        ("primary_group_id", Val::Int(3)),
    ])]
}
fn counter() -> Rec {
    extend(
        &crate::performance::tests::instance_row(AT),
        &[
            ("engine_edition", Val::Int(5)),
            ("database_edition", Val::Text("General Purpose".into())),
            ("edition", Val::Int(5)),
            ("process_id", Val::Int(10)),
            ("started_at_ticks", Val::Int(100)),
            ("ms_ticks", Val::Int(200)),
            ("updateability", Val::Text("READ_WRITE".into())),
        ],
    )
}
fn counters() -> Vec<Rec> {
    let base = counter();
    vec![
        base.clone(),
        replace(
            &base,
            &[
                ("native_object", Val::Text("SQLServer:General Statistics".into())),
                ("native_name", Val::Text("User Connections".into())),
                ("native_type", Val::Int(65_792)),
            ],
        ),
        replace(
            &base,
            &[
                ("native_object", Val::Text("SQLServer:Access Methods".into())),
                ("native_name", Val::Text("Page Splits/sec".into())),
                ("native_type", Val::Int(272_696_576)),
            ],
        ),
        replace(
            &base,
            &[
                ("native_object", Val::Text("SQLServer:Workload Group Stats".into())),
                ("native_name", Val::Text("Queued requests".into())),
                ("native_instance", Val::Text("native-user-workload".into())),
                ("native_type", Val::Int(65_792)),
                ("rg_group_id", Val::Int(3)),
                ("rg_pool_id", Val::Int(2)),
                ("rg_name", Val::Text("native-user-workload".into())),
                ("rg_statistics_started_without_offset", Val::Text("2019-12-31T23:00:00.000".into())),
            ],
        ),
    ]
}
fn file() -> Rec {
    replace(
        &crate::storage_io::tests::row(),
        &[
            ("database_id", Val::Int(7)),
            ("database_name", Val::Text("fixture-db".into())),
            ("engine_edition", Val::Int(5)),
            ("database_edition", Val::Text("General Purpose".into())),
        ],
    )
}
fn pool_pressure() -> Rec {
    extend(
        &crate::resources::pool::test_support::row(AT),
        &[
            ("observed_at", Val::Text(AT.into())),
            ("avg_cpu_percent", Val::Float(10.0)),
            ("avg_instance_cpu_percent", Val::Float(10.0)),
            ("avg_data_io_percent", Val::Float(10.0)),
            ("avg_log_write_percent", Val::Float(10.0)),
        ],
    )
}

fn replies() -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![Rec::of(&[("edition", Val::Int(5))])]),
        Ok(vec![member(BEFORE)]),
        Ok(vec![process()]),
        Ok(roster()),
        Ok(vec![pool_pressure()]),
        Ok(vec![crate::memory_clerks::tests::row()]),
        Ok(vec![pool_pressure()]),
        Ok(crate::schedulers::fixture_columns()),
        Ok(vec![crate::schedulers::tests::row()]),
        Ok(vec![pool_pressure()]),
        Ok(counters()),
        Ok(vec![pool_pressure()]),
        Ok(vec![file()]),
        Ok(vec![member(UNTIL)]),
        Ok(vec![process()]),
        Ok(roster()),
        Ok(vec![member(FINAL)]),
        Ok(vec![process()]),
        Ok(roster()),
    ]
}
fn request(total: u32) -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Shared {
            resource: SharedResourceIdentity {
                engine: Engine::Mssql,
                kind: SharedResourceKind::Pool,
                namespace: NativeIdentity::new("fixture-server").qualified("kind", "azure-sql-logical-server"),
                native: NativeIdentity::new("fixture-pool"),
            },
        },
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational },
        budget: bounded_budget(&CollectionBudget { max_queries: total, ..Default::default() }),
    }
}
fn scripted_source(rows: Vec<ReadResult<Vec<Rec>>>, total: u32) -> StateSource<FakeRead> {
    let request = request(total);
    let queries = ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
    StateSource::with_io(
        FakeRead::new(rows).with_generation(1).with_query_limit(queries),
        &request,
        "fixture.invalid".into(),
        1433,
    )
    .unwrap()
}
async fn drain(source: &mut StateSource<FakeRead>) -> Vec<TelemetryRecord> {
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::OperationalSamples, cursor.as_deref()).await.unwrap()
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
async fn exact_pool_anchor_process_and_members_bracket_existing_readers_without_relabelling_raw_scope() {
    let mut source = scripted_source(replies(), 20);
    let original = source.describe().await.unwrap();
    let rows = drain(&mut source).await;
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 19);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let authority = mssql_pool_current_authority(&original).unwrap().unwrap();
    assert!(authority.native.as_ref().unwrap().before.observed().is_some());
    let CollectionSubject::Shared { resource } = request(20).subject else { panic!() };
    assert_eq!(original.target, CollectionTarget::Shared(resource));
    assert_eq!(original.partition, SourcePartition::Target);
    assert_eq!(original.continuity, SourceContinuity::Unverified);
    let capture = CaptureIdentity { observation_id: "pool-native-scripted".into(), revision: None };
    for row in &rows {
        ExportedRecord::new(&original, &capture, row.clone()).unwrap();
    }
    let observations = rows
        .iter()
        .filter_map(|row| match row {
            TelemetryRecord::Operational(row) => Some(row.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(MssqlPoolOperationalRead { source: original.clone(), observations: observations.clone() }.is_valid());
    for row in &observations {
        if let OperationalFact::CounterSet(sample) = &row.fact {
            assert_eq!(sample.scope, MeasurementScope::Server);
        }
    }
    assert_eq!(source.io.calls.iter().filter(|call| call.sql.contains("sys.dm_os_memory_clerks")).count(), 1);
    assert_eq!(source.io.calls.iter().filter(|call| call.sql.contains("sys.dm_os_performance_counters")).count(), 1);
    let file_call =
        source.io.calls.iter().find(|call| call.sql.contains("sys.dm_io_virtual_file_stats(NULL, NULL)")).unwrap();
    assert!(file_call.sql.contains("g.database_id=vfs.database_id"));
    assert!(file_call.sql.contains("vfs.database_id=DB_ID() AND df.file_id=vfs.file_id"));
    assert!(!file_call.sql.contains("sys.master_files") && !file_call.sql.contains("DB_NAME(vfs"));
    let native = original.observation.snapshot().unwrap();
    let perf =
        native.sources.iter().find(|source| source.source == culprit_core::source_id::PERFORMANCE_COUNTERS).unwrap();
    let perfs = observations.iter().filter(|row| row.source == perf.source).collect::<Vec<_>>();
    let report = culprit_checks::performance_counters::explain_source(&original, perf, &perfs, true).unwrap();
    assert_eq!(report.population, culprit_core::PerformanceCounterPopulation::SharedPool);
    let resources = culprit_checks::instance_resources::explain_source(&original, &observations, true).unwrap();
    assert_eq!(resources.schedulers.unwrap().entries.len(), 1);
    assert!(resources.groups.is_empty()); // no instance-pressure/CPU verdict is invented.
}
#[tokio::test]
async fn nineteen_cannot_pay_for_set_and_every_final_scope_check_and_tighter_caps_survive() {
    let mut source = scripted_source(replies(), 19);
    source.describe().await.unwrap();
    drain(&mut source).await;
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 18);
    source.close().await;
    let requested = CollectionBudget {
        max_queries: 4,
        max_snapshot_rows: 3,
        page_rows: 2,
        page_bytes: 1024,
        total_bytes: 2048,
        statement_timeout_ms: 250,
        total_timeout_ms: 750,
        ..Default::default()
    };
    let bounded = bounded_budget(&requested);
    assert_eq!((bounded.max_queries, bounded.max_snapshot_rows, bounded.page_rows), (4, 3, 2));
    assert_eq!(
        (bounded.page_bytes, bounded.total_bytes, bounded.statement_timeout_ms, bounded.total_timeout_ms),
        (1024, 2048, 250, 750)
    );
}
#[tokio::test]
async fn changed_anchor_role_process_epoch_roster_and_reused_database_ids_refuse_before_publication() {
    for (index, fields) in [
        (13, vec![("pool_name", Val::Text("foreign-pool".into()))]),
        (13, vec![("updateability", Val::Text("READ_ONLY".into()))]),
        (13, vec![("server_performance_state", Val::Null)]),
        (14, vec![("local_started_ticks", Val::Int(101))]),
        (14, vec![("local_process_id", Val::Int(11))]),
        (15, vec![("physical_database_guid", Val::Text("33333333-3333-3333-3333-333333333333".into()))]),
        (15, vec![("logical_database_guid", Val::Text("44444444-4444-4444-4444-444444444444".into()))]),
        (15, vec![("primary_group_id", Val::Int(4))]),
    ] {
        let mut data = replies();
        let rows = data[index].as_mut().unwrap();
        rows[0] = replace(&rows[0], &fields);
        let mut source = scripted_source(data, 20);
        assert!(source.describe().await.is_err(), "{index}");
        assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
        source.close().await;
    }
}
#[tokio::test]
async fn transport_change_or_missing_epoch_never_borrows_an_anchor_or_stable_compute_identity() {
    let mut source = scripted_source(replies(), 20);
    source.io = source.io.change_generation_at(11);
    assert!(source.describe().await.is_err());
    source.close().await;
    let mut data = vec![Ok(vec![Rec::of(&[("edition", Val::Int(5))])])];
    for at in [BEFORE, UNTIL, FINAL] {
        data.push(Ok(vec![member(at)]));
        data.push(Ok(vec![replace(
            &process(),
            &[
                ("local_epoch_visible", Val::Bool(false)),
                ("local_started_ticks", Val::Null),
                ("local_now_ticks", Val::Null),
                ("local_started_without_offset", Val::Null),
            ],
        )]));
    }
    let mut source = scripted_source(data, 20);
    let original = source.describe().await.unwrap();
    assert!(
        original
            .observation
            .snapshot()
            .unwrap()
            .sources
            .iter()
            .all(|coverage| coverage.state == SourceState::Unavailable
                && coverage.records == 0
                && !coverage.complete_scope)
    );
    assert!(drain(&mut source).await.is_empty());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert!(!source.io.calls.iter().any(
        |call| call.sql.contains("sys.dm_os_memory_clerks") || call.sql.contains("sys.dm_os_performance_counters")
    ));
    source.close().await;
}
#[tokio::test]
async fn effective_permission_unknown_or_denied_does_not_open_any_pool_family_or_claim_empty_success() {
    for permission in [Val::Null, Val::Int(0)] {
        let data = vec![
            Ok(vec![Rec::of(&[("edition", Val::Int(5))])]),
            Ok(vec![replace(&member(BEFORE), &[("server_performance_state", permission.clone())])]),
            Ok(vec![replace(&member(UNTIL), &[("server_performance_state", permission.clone())])]),
            Ok(vec![replace(&member(FINAL), &[("server_performance_state", permission)])]),
        ];
        let mut source = scripted_source(data, 20);
        let original = source.describe().await.unwrap();
        let header = original.observation.snapshot().unwrap();
        assert_eq!(header.coverage_state().unwrap(), SourceState::Unavailable);
        assert!(drain(&mut source).await.is_empty());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 4);
        source.close().await;
    }
}
#[tokio::test]
async fn foreign_file_or_counter_epoch_is_not_relabelled_as_pool_and_empty_family_remains_unavailable() {
    let mut data = replies();
    data[12] = Ok(vec![replace(&file(), &[("database_id", Val::Int(999))])]);
    let mut source = scripted_source(data, 20);
    assert!(source.describe().await.is_err());
    source.close().await;
    let mut data = replies();
    let mut values = counters();
    values[0] = replace(&values[0], &[("process_id", Val::Int(999))]);
    data[10] = Ok(values);
    let mut source = scripted_source(data, 20);
    assert!(source.describe().await.is_err());
    source.close().await;
    let mut data = replies();
    data[5] = Ok(vec![]);
    let mut source = scripted_source(data, 20);
    let original = source.describe().await.unwrap();
    let header = original.observation.snapshot().unwrap();
    assert_eq!(
        header.sources.iter().find(|coverage| coverage.source == culprit_core::source_id::MEMORY_CLERKS).unwrap().state,
        SourceState::Unavailable
    );
    assert_eq!(header.coverage_state().unwrap(), SourceState::Partial);
    drain(&mut source).await;
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    source.close().await;
}
#[tokio::test]
async fn cancellation_and_deadline_stop_the_same_owner_without_later_dispatch_or_a_successful_body() {
    for stop in [
        CollectionStop::Cancelled,
        CollectionStop::TimedOut,
        CollectionStop::CancellationUnconfirmed,
        CollectionStop::ByteLimit,
    ] {
        let mut data = replies();
        data[10] = Err(crate::read::ReadError::Stopped(stop));
        let mut source = scripted_source(data, 20);
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), 11);
        assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut source = scripted_source(replies(), 20);
    source.io = source.io.pause_on_query(11);
    assert!(tokio::time::timeout(std::time::Duration::from_millis(5), source.describe()).await.is_err());
    assert_eq!(source.query_count(), 11);
    assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
    source.close().await;
}

#[tokio::test]
async fn pool_pressure_not_an_idle_anchor_controls_every_new_family_and_missing_or_stale_values_refuse() {
    for row in [
        replace(&pool_pressure(), &[("avg_cpu_percent", Val::Float(95.0))]),
        replace(&pool_pressure(), &[("avg_instance_cpu_percent", Val::Float(95.0))]),
        replace(&pool_pressure(), &[("max_worker_percent", Val::Float(90.0))]),
        replace(&pool_pressure(), &[("avg_data_io_percent", Val::Null)]),
        replace(&pool_pressure(), &[("sample_at", Val::Text("2020-01-01T05:00:00Z".into()))]),
    ] {
        let mut data = replies();
        data[4] = Ok(vec![row]);
        let mut source = scripted_source(data, 20);
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), 5);
        assert!(source.io.calls[4].sql.contains("sys.dm_elastic_pool_resource_stats"));
        assert!(!source.io.calls.iter().any(|call| call.sql.contains("sys.dm_os_memory_clerks")));
        source.close().await;
    }
}
#[tokio::test]
async fn a_known_readonly_operational_anchor_keeps_its_role_and_unknown_or_changing_role_refuses() {
    let mut data = replies();
    for index in [1, 13, 16] {
        data[index].as_mut().unwrap()[0] = replace(
            &member(if index == 1 {
                BEFORE
            } else if index == 13 {
                UNTIL
            } else {
                FINAL
            }),
            &[("updateability", Val::Text("READ_ONLY".into()))],
        );
    }
    for index in [10, 12] {
        for row in data[index].as_mut().unwrap() {
            *row = extend(row, &[("updateability", Val::Text("READ_ONLY".into()))]);
        }
    }
    let mut source = scripted_source(data, 20);
    let original = source.describe().await.unwrap();
    assert_eq!(NativeReadContext::from_snapshot(&original).unwrap().database_writable, Some(false));
    drain(&mut source).await;
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    source.close().await;
    let mut data = replies();
    data[1] = Ok(vec![replace(&member(BEFORE), &[("updateability", Val::Null)])]);
    let mut source = scripted_source(data, 20);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 2);
    source.close().await;
}
#[tokio::test]
async fn a_native_error_that_retires_the_reader_stops_before_replacement_setup_or_another_family() {
    let mut data = replies();
    data[5] =
        Err(culprit_core::Error::new(culprit_core::ErrorCode::PermissionDenied, "scripted native permission failure")
            .into());
    let mut source = scripted_source(data, 20);
    source.io = source.io.change_generation_at(6);
    let error = source.describe().await.unwrap_err();
    assert_eq!(error.code, culprit_core::ErrorCode::PermissionDenied);
    assert_eq!(source.query_count(), 6);
    assert!(!source.io.calls.iter().any(|call| call.sql.contains("sys.dm_os_schedulers")));
    assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
    source.close().await;
}

#[test]
fn the_closed_pool_route_is_only_an_unqualified_baseline_recipe_not_a_measured_or_automatic_admission() {
    let recipe = crate::telemetry::recipes().into_iter().find(|recipe| recipe.id == "mssql.pool-operational").unwrap();
    assert!(matches!(recipe.admission, RecipeAdmission::Unqualified));
    assert_eq!(recipe.purpose, RecipePurpose::Baseline);
    assert_eq!(recipe.budget.max_queries, 20);
}

mod original;
