//! The counter route reuses its sibling Settings test's SAME held streaming reader and guard.
use super::*;

const POINT: &str = "2026-11-15T10:00:00.015001Z";
const ENTRY: &str = "2026-11-13T05:00:00.000001Z";
const EXTREMA: &str = "2026-11-15T09:00:00.000002Z";

fn counter_request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::StatementCounters },
        budget: statement_profile::bounded_budget(&CollectionBudget { page_rows: 1, ..Default::default() }),
    }
}
fn metadata_row(writable: bool, at: &str, max: Option<&str>) -> Row {
    let columns = serde_json::to_string(native::statements::GUARDED_COLUMNS).unwrap();
    test_support::row(&[
        ("build", Some("PostgreSQL 18.0")),
        ("database_oid", Some("16384")),
        ("database", Some("shop")),
        ("version", Some("180000")),
        ("block_bytes", Some("8192")),
        ("standby", Some(if writable { "f" } else { "t" })),
        ("started_at", Some(START)),
        ("observed_at", Some(at)),
        ("full_visibility", Some("t")),
        ("replay_pause_requested_access", Some("f")),
        ("replay_pause_state_access", Some("f")),
        ("pgss_schema", Some("public")),
        ("pgss_columns", Some(&columns)),
        ("pgss_info", Some("t")),
        ("pgss_version", Some("1.12")),
        ("pgss_max_entries", max),
    ])
}
fn point(user: &str, toplevel: bool) -> Row {
    test_support::row(&[
        ("userid", Some(user)),
        ("dbid", Some("16384")),
        ("queryid", Some("-9223372036854775808")),
        ("toplevel", Some(if toplevel { "t" } else { "f" })),
        ("observed_at", Some(POINT)),
        ("stats_since", Some(ENTRY)),
        ("minmax_stats_since", Some(EXTREMA)),
        ("calls", Some("9007199254740993")),
        ("rows", Some("0")),
        ("total_exec_time", Some("3.5")),
        ("mean_exec_time", Some("0.5")),
        ("min_exec_time", Some("0.1")),
        ("max_exec_time", Some("1")),
        ("stddev_exec_time", Some("0.2")),
        ("plans", Some("0")),
        ("temp_blks_read", Some("0")),
        ("temp_blks_written", Some("7")),
    ])
}
async fn counter_reader(
    request: &CollectionRequest,
    writable: bool,
) -> (PostgresSnapshotReader<Read>, Arc<AtomicBool>) {
    let (mut reader, active) = reader(request, writable).await;
    reader.recipe = Recipe::Statements;
    reader.io.original.replies = [
        Ok(vec![metadata_row(writable, FROM, Some("5000"))]),
        Ok(vec![test_support::member_at("127.0.0.1", INITIAL_MEMBER)]),
        Ok(vec![point("10", true), point("11", false)]),
        Ok(vec![metadata_row(writable, FINAL_METADATA, Some("5000"))]),
        Ok(vec![test_support::member_at("127.0.0.1", UNTIL)]),
    ]
    .into();
    (reader, active)
}
fn counter(record: &TelemetryRecord) -> &StatementCounterSample {
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::StatementCounters(sample), ..
    }) = record
    else {
        panic!("native counter point only")
    };
    sample
}
#[tokio::test]
async fn same_held_counter_reader_uses_six_charges_exact_native_entries_and_no_text_or_page_dispatch() {
    for writable in [true, false] {
        let request = counter_request();
        let (reader, _) = counter_reader(&request, writable).await;
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let original = source.describe().await.unwrap();
        assert!(matches!(original.partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) }));
        assert!(original.target.database().unwrap().incarnation.is_none());
        let rows = read_stream(&mut source, TelemetryStream::OperationalSamples).await.unwrap();
        assert_eq!(source.query_count(), 4, "only setup/metadata/member/body before final witnesses");
        assert_eq!(rows.len(), 2);
        let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
        assert_eq!(source.query_count(), 6);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.reader().io.original.calls.len(), 5);
        assert_eq!(
            source.reader().io.original.calls.iter().filter(|sql| sql.contains("pg_stat_statements(false)")).count(),
            1
        );
        assert!(
            source
                .reader()
                .io
                .original
                .calls
                .iter()
                .all(|sql| !sql.contains("pg_stat_statements(true)") && !sql.contains("octet_length(s.query)"))
        );
        let native = completed.observation.snapshot_acquisition_context().unwrap();
        assert_eq!(native.partition, completed.partition);
        assert_eq!(native.database_writable, Some(writable));
        assert_eq!(counter(&rows[0]).reset, culprit_core::ResetMarker::At(canonical_time(ENTRY).unwrap()));
        assert_eq!(
            counter(&rows[0]).distributions[0].extrema_reset,
            culprit_core::ResetMarker::At(canonical_time(EXTREMA).unwrap())
        );
        assert_ne!(
            counter(&rows[0]).identity,
            counter(&rows[1]).identity,
            "users and nesting are independent populations"
        );
        assert!(counter(&rows[0]).totals.iter().any(|metric| metric.key == RuntimeMetricKey::Executions
            && metric.total == MeasurementValue::Integer("9007199254740993".into())));
        let capture = CaptureIdentity { observation_id: "native-guarded-counters".into(), revision: None };
        let mut observations = Vec::new();
        for row in rows {
            let exported = ExportedRecord::new(&completed, &capture, row).unwrap();
            let replayed: ExportedRecord = serde_json::from_slice(&serde_json::to_vec(&exported).unwrap()).unwrap();
            assert_eq!(exported, replayed);
            let TelemetryRecord::Operational(observation) = replayed.record else { panic!() };
            observations.push(observation);
        }
        let counters = StatementCounterSnapshot::from_source(&completed, observations).unwrap();
        assert!(counters.is_valid());
        assert_eq!(counters.context.as_deref(), Some(native));
    }
}
#[tokio::test]
async fn native_entry_bound_not_a_small_database_limit_controls_admission_before_the_srf() {
    for maximum in [None, Some("0"), Some("5001"), Some("2147483647")] {
        let mut request = counter_request();
        request.budget.max_snapshot_rows = 1;
        let (mut reader, _) = counter_reader(&request, true).await;
        reader.io.original.replies[0] = Ok(vec![metadata_row(true, FROM, maximum)]);
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        assert_eq!(source.describe().await.unwrap_err().code, ErrorCode::Unsupported);
        assert_eq!(source.query_count(), 2, "setup and original metadata only");
        assert!(source.reader().io.original.calls.iter().all(|sql| !sql.contains("pg_stat_statements(false)")));
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn missing_visibility_or_extension_catalog_never_admits_native_counter_work() {
    for case in ["visibility", "catalog"] {
        let request = counter_request();
        let (mut reader, _) = counter_reader(&request, true).await;
        if case == "catalog" {
            reader.io.original.replies[0] = Ok(vec![test_support::metadata_at(180006, "f", START, true, FROM)]);
        } else {
            // The actual missing-permission metadata row also has no extension catalog.
            reader.io.original.replies[0] = Ok(vec![test_support::metadata_at(180006, "f", START, false, FROM)]);
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        assert_eq!(
            source.describe().await.unwrap_err().code,
            if case == "visibility" { ErrorCode::PermissionDenied } else { ErrorCode::Unsupported }
        );
        assert_eq!(source.query_count(), 2);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn caller_five_and_row_prefix_cannot_complete_the_new_counter_population_or_refill() {
    for case in ["queries", "rows"] {
        let mut request = counter_request();
        if case == "queries" {
            request.budget.max_queries = 5;
        } else {
            request.budget.max_snapshot_rows = 1;
        }
        let (reader, _) = counter_reader(&request, true).await;
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        source.describe().await.unwrap();
        if case == "queries" {
            assert_eq!(read_stream(&mut source, TelemetryStream::OperationalSamples).await.unwrap().len(), 2);
            assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
            assert_eq!(source.query_count(), 5);
        } else {
            assert_eq!(
                CollectionStop::from_error(
                    &read_stream(&mut source, TelemetryStream::OperationalSamples).await.unwrap_err()
                ),
                CollectionStop::RowLimit
            );
            assert_eq!(source.query_count(), 4);
        }
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn counter_context_changes_and_retirement_withdraw_the_same_held_population_without_replacement() {
    for case in ["max", "member", "role", "transport", "retire", "late"] {
        let request = counter_request();
        let (mut reader, active) = counter_reader(&request, true).await;
        match case {
            "max" => reader.io.original.replies[3] = Ok(vec![metadata_row(true, FINAL_METADATA, Some("5001"))]),
            "member" => reader.io.original.replies[4] = Ok(vec![test_support::member_at("127.0.0.2", UNTIL)]),
            "role" => reader.io.original.replies[3] = Ok(vec![metadata_row(false, FINAL_METADATA, Some("5000"))]),
            "transport" => reader.io.change_after = Some(3),
            "retire" => reader.io.retire_after = Some(3),
            "late" => reader.io.late_failure = true,
            _ => unreachable!(),
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        source.describe().await.unwrap();
        if matches!(case, "transport" | "retire" | "late") {
            assert!(read_stream(&mut source, TelemetryStream::OperationalSamples).await.is_err(), "{case}");
            assert_eq!(source.query_count(), 4);
        } else {
            read_stream(&mut source, TelemetryStream::OperationalSamples).await.unwrap();
            assert!(source.finish().await.is_err(), "{case}");
        }
        assert!(source.finish().await.is_err());
        active.store(true, Ordering::Release);
        if matches!(case, "transport" | "retire") {
            assert!(source.reader().profile.as_ref().unwrap().ensure_active().is_err());
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert!(source.reader().io.original.calls.len() <= 5);
    }
}
#[tokio::test]
async fn completed_empty_counter_capture_is_a_point_roster_not_a_healthy_workload_interval() {
    let request = counter_request();
    let (mut reader, _) = counter_reader(&request, true).await;
    reader.io.original.replies[2] = Ok(vec![]);
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    let original = source.describe().await.unwrap();
    assert!(read_stream(&mut source, TelemetryStream::OperationalSamples).await.unwrap().is_empty());
    let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
    let snapshot = completed.observation.snapshot().unwrap();
    assert_eq!(snapshot.records, 0);
    assert_eq!(completed.observation.range().unwrap(), TelemetryRange::Current);
    assert_eq!(source.query_count(), 6);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[test]
fn counter_recipe_bounds_intersect_every_caller_limit_and_never_make_definitions_or_cost_ready() {
    let upper = CollectionBudget::upper_limits();
    let bounded = statement_profile::bounded_budget(&upper);
    bounded.validate().unwrap();
    assert_eq!((bounded.max_queries, bounded.max_snapshot_rows), (6, 5000));
    for mut budget in [
        upper.clone(),
        CollectionBudget { max_queries: 5, ..upper.clone() },
        CollectionBudget { max_snapshot_rows: 1, ..upper.clone() },
        CollectionBudget {
            page_bytes: 1024,
            total_bytes: 1024,
            max_sql_bytes: 1024,
            max_plan_bytes: 1024,
            ..upper.clone()
        },
    ] {
        budget.page_rows = budget.page_rows.min(budget.max_snapshot_rows);
        let actual = statement_profile::bounded_budget(&budget);
        actual.validate().unwrap();
        assert_eq!(actual, actual.intersection(&budget));
    }
    let recipes = crate::telemetry_recipes();
    let counters = recipes.iter().find(|recipe| recipe.id == "postgres.statement-counters").unwrap();
    assert_eq!(counters.version, 3);
    assert!(!matches!(counters.admission, RecipeAdmission::Measured(_)));
    assert!(
        !crate::telemetry_profile_selectors().contains(&RecipeSelector::Collection(TelemetryCollection::Snapshot(
            SnapshotDataset::StatementDefinitions
        )))
    );
    assert!(matches!(
        recipes.iter().find(|recipe| recipe.id == "postgres.statement-definitions").unwrap().admission,
        RecipeAdmission::Unqualified
    ));
}

#[tokio::test]
async fn expanded_counter_records_repage_through_the_shared_byte_owner_with_emitted_cursors() {
    let mut request = counter_request();
    request.budget.page_rows = 20;
    request.budget.max_snapshot_rows = 20;
    request.budget.page_bytes = 8192;
    let (mut reader, _) = counter_reader(&request, true).await;
    reader.io.original.replies[2] = Ok((10..30).map(|user| point(&user.to_string(), true)).collect());
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    let original = source.describe().await.unwrap();
    let capture = CaptureIdentity { observation_id: "expanded-native-counter-page".into(), revision: None };
    let mut cursor = None;
    let mut emitted = 0usize;
    let mut pages = 0usize;
    loop {
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::OperationalSamples, cursor.as_deref()).await.unwrap()
        else {
            panic!()
        };
        assert!(!records.is_empty());
        let bytes = records
            .iter()
            .map(|row| {
                serde_json::to_vec(&ExportedRecord::new(&original, &capture, row.clone()).unwrap()).unwrap().len() + 1
            })
            .sum::<usize>();
        assert!(bytes <= request.budget.page_bytes as usize, "the real exported envelope fits the page");
        emitted += records.len();
        pages += 1;
        assert_eq!(source.query_count(), 4, "splitting records cannot dispatch or refill native work");
        if let Some(next) = &next_cursor {
            assert_eq!(next, &emitted.to_string());
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(pages > 1, "one native value-sized batch expands beyond one exported-record page");
    assert_eq!(emitted, 20);
    let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
    assert_eq!(completed.observation.snapshot().unwrap().records, 20);
    assert_eq!(source.query_count(), 6);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn retirement_between_buffered_record_pages_withholds_remaining_rows_and_final_dispatches() {
    let mut request = counter_request();
    request.budget.page_rows = 20;
    request.budget.max_snapshot_rows = 20;
    request.budget.page_bytes = 8192;
    let (mut reader, active) = counter_reader(&request, true).await;
    reader.io.original.replies[2] = Ok((10..30).map(|user| point(&user.to_string(), true)).collect());
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    let CollectionRead::Page { records, next_cursor: Some(cursor) } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert!(!records.is_empty() && records.len() < 20);
    assert_eq!(cursor, records.len().to_string());
    active.store(false, Ordering::Release);
    assert_eq!(
        CollectionStop::from_error(
            &source.read_page(TelemetryStream::OperationalSamples, Some(&cursor)).await.unwrap_err()
        ),
        CollectionStop::SourceChanged
    );
    active.store(true, Ordering::Release);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.reader().io.original.calls.len(), 3);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(source.reader().io.original.closed);
}

#[tokio::test]
async fn explicit_definitions_reader_binds_the_same_held_owner_and_native_entry_identity_while_staying_unqualified() {
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::StatementDefinitions },
        budget: statement_profile::definitions_budget(&CollectionBudget::default()),
    };
    let (mut reader, _) = reader(&request, true).await;
    reader.profile = None;
    reader.recipe = Recipe::Definitions;
    let sql = "SELECT total_cents FROM public.orders WHERE customer_id = 9264";
    reader.io.original.replies = [
        Ok(vec![metadata_row(true, FROM, Some("5000"))]),
        Ok(vec![test_support::member_at("127.0.0.1", INITIAL_MEMBER)]),
        Ok(vec![
            test_support::row(&[
                ("userid", Some("10")),
                ("dbid", Some("16384")),
                ("queryid", Some("-9223372036854775808")),
                ("toplevel", Some("t")),
                ("observed_at", Some(POINT)),
                ("stats_since", Some(ENTRY)),
                ("minmax_stats_since", Some(EXTREMA)),
                ("query", Some(sql)),
                ("source_text_bytes", Some(&sql.len().to_string())),
            ]),
            test_support::row(&[
                ("userid", Some("11")),
                ("dbid", Some("16384")),
                ("queryid", Some("-9223372036854775808")),
                ("toplevel", Some("f")),
                ("observed_at", Some(POINT)),
                ("stats_since", Some(ENTRY)),
                ("minmax_stats_since", Some(EXTREMA)),
                ("query", Some("<insufficient privilege>")),
                ("source_text_bytes", Some("24")),
            ]),
        ]),
        Ok(vec![metadata_row(true, FINAL_METADATA, Some("5000"))]),
        Ok(vec![test_support::member_at("127.0.0.1", UNTIL)]),
    ]
    .into();
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    let original = source.describe().await.unwrap();
    assert!(matches!(original.partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) }));
    let rows = read_stream(&mut source, TelemetryStream::ObjectDefinitions).await.unwrap();
    assert_eq!(rows.len(), 2);
    let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
    assert_eq!(source.query_count(), 6);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let context = completed.observation.snapshot_acquisition_context().unwrap();
    assert!(context.has_qualified_local_owner());
    assert_eq!(context.database_writable, Some(true));
    let TelemetryRecord::ObjectDefinition(first) = &rows[0] else { panic!() };
    let ObjectDefinitionFact::Statement(first_def) = &first.fact else { panic!() };
    assert_eq!(first_def.origin, StatementOrigin::Application);
    assert!(first_def.sql.is_available());
    assert!(!serde_json::to_string(&first_def.sql).unwrap().contains("9264"));
    let TelemetryRecord::ObjectDefinition(second) = &rows[1] else { panic!() };
    let ObjectDefinitionFact::Statement(second_def) = &second.fact else { panic!() };
    assert_eq!(second_def.origin, StatementOrigin::Unknown);
    assert_eq!(second_def.sql, DefinitionPayload::Unavailable { reason: PayloadUnavailable::Restricted });
    assert_ne!(first_def.identity, second_def.identity);
}
