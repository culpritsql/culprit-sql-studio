//! Scripted boxed/Managed Instance replies exercise the same source, decoder, cursor and completion owner.
//! These are adapter controls, not native Managed Instance compatibility or cost acceptance.
use super::*;
use crate::{
    read::{ReadError, ReadResult},
    resources::test_support::instance_pressure_row,
};
use culprit_core::RuntimePopulationMembership;

fn description(edition: i64, partitioned: bool) -> Vec<ReadResult<Vec<Rec>>> {
    let mut metadata = description_rows();
    metadata[3] = Ok(vec![options(edition, partitioned)]);
    metadata[5] = Ok(vec![]);
    metadata[6] = Ok(vec![Rec::of(&[("plan_id", Val::Int(4))])]);
    let mut rows = vec![
        Ok(vec![native_platform(edition)]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![authority(edition)]),
        Ok(vec![instance_pressure_row()]),
    ];
    rows.extend(metadata.drain(3..).enumerate().filter_map(|(index, row)| (partitioned || index != 2).then_some(row)));
    rows
}
fn closing(edition: i64, partitioned: bool) -> Vec<ReadResult<Vec<Rec>>> {
    let mut rows = description(edition, partitioned);
    let mut closing = vec![Ok(vec![instance_pressure_row()])];
    closing.extend(rows.drain(4..));
    closing.extend([
        Ok(vec![native_platform(edition)]),
        Ok(vec![crate::instance::test_support::epoch(100)]),
        Ok(vec![replace(&authority(edition), &[("observed_at", Val::Text("2020-01-01T06:00:02Z".into()))])]),
    ]);
    closing
}
fn query_definition() -> Rec {
    crate::query_store::tests::definition_row("SELECT retained_value FROM dbo.owned_table")
}
fn plan_definition() -> Rec {
    let row = crate::query_store::tests::summary_row();
    let mut fields = row.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    fields.extend([
        ("id", Val::Int(4)),
        ("engine_version", Val::Text("16.0".into())),
        ("compatibility_level", Val::Int(160)),
        ("source_bytes", Val::Null),
        ("payload", Val::Null),
        ("encrypted", Val::Bool(false)),
        ("restricted", Val::Bool(false)),
    ]);
    Rec::of(&fields)
}
fn lengths(row: &Rec) -> Rec {
    Rec::of(
        &row.columns()
            .filter(|(name, _)| matches!(*name, "id" | "source_bytes"))
            .map(|(name, value)| (name, value.clone()))
            .collect::<Vec<_>>(),
    )
}
fn complete_replies(edition: i64, partitioned: bool, runtime: Vec<Rec>, waits: Vec<Rec>) -> Vec<ReadResult<Vec<Rec>>> {
    let has_definitions = !runtime.is_empty() || !waits.is_empty();
    let mut rows = description(edition, partitioned);
    rows.extend([Ok(vec![instance_pressure_row()]), Ok(runtime), Ok(vec![instance_pressure_row()]), Ok(waits)]);
    for definition in [query_definition(), plan_definition()] {
        rows.push(Ok(vec![instance_pressure_row()]));
        if has_definitions {
            rows.extend([Ok(vec![lengths(&definition)]), Ok(vec![definition])]);
        }
    }
    rows.extend(closing(edition, partitioned));
    rows
}
async fn drain(source: &mut Source<FakeRead>) -> Vec<TelemetryRecord> {
    let mut all = vec![];
    for stream in [
        TelemetryStream::QueryRuntime,
        TelemetryStream::QueryWaits,
        TelemetryStream::QueryDefinitions,
        TelemetryStream::PlanDefinitions,
    ] {
        let CollectionRead::Page { records, next_cursor: None } = source.read_page(stream, None).await.unwrap() else {
            panic!("one complete controlled native page")
        };
        all.extend(records);
    }
    all
}
#[tokio::test]
async fn boxed_history_keeps_original_data_ids_and_exact_native_rows_when_unknown_membership_completes() {
    for edition in [3, 8] {
        let native_runtime = [7, 9].map(|group| {
            replace(&identified_runtime(group, 1_000 + group), &[("count_executions", Val::Int(9_007_199_254_740_993))])
        });
        let replies = complete_replies(
            edition,
            true,
            native_runtime.to_vec(),
            [7, 9].map(|group| identified_wait(group, group)).to_vec(),
        );
        let mut source = Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
        let original = source.describe().await.unwrap();
        let history = original.query_history().unwrap();
        assert_eq!(source.query_count(), 8);
        assert_eq!(original.partition, SourcePartition::Target);
        assert!(original.target.database().unwrap().incarnation.is_none());
        assert!(history.acquisition_owner.as_ref().unwrap().has_qualified_local_owner());
        assert!(history.population_catalog.is_none());
        assert_eq!(history.population_binding, QueryHistoryPopulationBinding::Unknown);
        let records = drain(&mut source).await;
        let capture = CaptureIdentity { observation_id: "boxed-original-source".into(), revision: None };
        let exported =
            records.into_iter().map(|row| ExportedRecord::new(&original, &capture, row).unwrap()).collect::<Vec<_>>();
        let completion = source.finish().await.unwrap();
        assert_eq!(
            source.query_count(),
            26,
            "one held authority/metadata owner plus existing body and pressure phases"
        );
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        let completed = original.completed(completion).unwrap().unwrap();
        let history = completed.query_history().unwrap();
        assert!(matches!(
            history.population_catalog.as_ref().unwrap().membership,
            RuntimePopulationMembership::ObservedWindow { .. }
        ));
        assert_eq!(history.dimensions["replica_group_id"], ["7", "9"]);
        assert!(!history.population_catalog.as_ref().unwrap().has_complete_membership());
        assert!(
            history
                .population_catalog
                .as_ref()
                .unwrap()
                .populations
                .iter()
                .all(|row| !row.native_properties.contains_key("role_history"))
        );
        assert_eq!(completed.target, original.target);
        assert_eq!(completed.partition, original.partition);
        for original in &exported {
            let replay = ExportedRecord::new(&completed, &capture, original.record.clone()).unwrap();
            assert_eq!(&replay, original, "completion does not rewrite or re-export original record bytes");
            assert_eq!(replay.payload_sha256().unwrap(), original.payload_sha256().unwrap());
        }
        let TelemetryRecord::QueryRuntime(runtime) = &exported[0].record else { panic!() };
        assert_eq!(runtime.executions, "9007199254740993");
        assert_eq!(source.session.closed, 1);
        assert_eq!(source.session.calls.iter().filter(|call| call.sql == sql::REPLICAS).count(), 2);
    }
}
#[tokio::test]
async fn empty_catalog_and_empty_runtime_remain_unknown_while_positive_column_absence_is_unpartitioned() {
    for edition in [3, 8] {
        for partitioned in [true, false] {
            let mut source = Source::with_io(
                FakeRead::new(complete_replies(edition, partitioned, vec![], vec![])),
                &request(),
                "fixture.invalid".into(),
                1433,
            )
            .unwrap();
            let original = source.describe().await.unwrap();
            assert!(drain(&mut source).await.is_empty());
            let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
            let history = completed.query_history().unwrap();
            assert_eq!(source.query_count(), if partitioned { 22 } else { 20 });
            if partitioned {
                assert_eq!(history.population_binding, QueryHistoryPopulationBinding::Unknown);
                assert!(history.population_catalog.is_none());
            } else {
                assert_eq!(history.population_binding, QueryHistoryPopulationBinding::Unpartitioned);
                assert!(history.population_catalog.as_ref().unwrap().has_complete_membership());
                assert!(source.session.calls.iter().all(|call| call.sql != sql::REPLICAS));
                let raw = &source.session.calls[8].sql;
                assert!(raw.contains("CAST(NULL AS bigint) AS replica_group_id"));
                assert!(!raw.contains("r.replica_group_id"));
            }
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}
#[tokio::test]
async fn missing_native_dimension_or_invalid_original_body_cannot_add_membership() {
    for edition in [3, 8] {
        for changes in [
            vec![("replica_group_id", Val::Null)],
            vec![("first_execution_at", Val::Text("invalid-native-time".into()))],
        ] {
            let mut rows = description(edition, true);
            rows.extend([
                Ok(vec![instance_pressure_row()]),
                Ok(vec![replace(&identified_runtime(7, 1_000), &changes)]),
            ]);
            let mut source = Source::with_io(FakeRead::new(rows), &request(), "fixture.invalid".into(), 1433).unwrap();
            let original = source.describe().await.unwrap();
            let result = source.read_page(TelemetryStream::QueryRuntime, None).await;
            assert!(result.is_err() || matches!(result, Ok(CollectionRead::Stopped(_))));
            assert!(source.observed_runtime_groups.is_empty());
            assert!(source.finish().await.is_err());
            assert!(original.query_history().unwrap().population_catalog.is_none());
            assert_eq!(source.query_count(), 10);
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}
#[tokio::test]
async fn waits_cannot_invent_a_group_not_observed_in_the_complete_runtime_body() {
    for edition in [3, 8] {
        let mut source = Source::with_io(
            FakeRead::new(complete_replies(
                edition,
                true,
                vec![identified_runtime(7, 1_000)],
                vec![identified_wait(9, 1)],
            )),
            &request(),
            "fixture.invalid".into(),
            1433,
        )
        .unwrap();
        let original = source.describe().await.unwrap();
        drain(&mut source).await;
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Changed);
        assert!(original.query_history().unwrap().population_catalog.is_none());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn overflow_pages_do_not_turn_a_truncated_all_group_prefix_into_membership() {
    for edition in [3, 8] {
        let mut rows = description(edition, true);
        rows.extend([
            Ok(vec![instance_pressure_row()]),
            Ok(vec![identified_runtime(7, 1_000), identified_runtime(9, 1_001)]),
            Ok(vec![instance_pressure_row()]),
            Err(ReadError::Stopped(CollectionStop::TimedOut)),
        ]);
        let mut request = request();
        request.budget.page_rows = 1;
        let mut source = Source::with_io(FakeRead::new(rows), &request, "fixture.invalid".into(), 1433).unwrap();
        let original = source.describe().await.unwrap();
        let CollectionRead::Page { records, next_cursor: Some(cursor) } =
            source.read_page(TelemetryStream::QueryRuntime, None).await.unwrap()
        else {
            panic!("bounded plan-range continuation")
        };
        assert!(records.is_empty());
        assert!(source.observed_runtime_groups.is_empty());
        assert!(matches!(
            source.read_page(TelemetryStream::QueryRuntime, Some(&cursor)).await.unwrap(),
            CollectionRead::Stopped(CollectionStop::TimedOut)
        ));
        assert!(!source.session.calls[11].sql.contains("r.replica_group_id=@P3"));
        assert_eq!(
            source.session.calls[11].parameters.len(),
            4,
            "all-group cursor binds only its plan range, never a synthetic group"
        );
        assert!(source.finish().await.is_err());
        assert!(original.query_history().unwrap().population_catalog.is_none());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn final_native_owner_or_schema_change_withholds_the_same_original_capture() {
    for edition in [3, 8] {
        for change in ["process", "recovery", "updateability", "capability"] {
            let mut replies = complete_replies(edition, true, vec![], vec![]);
            let end = replies.len();
            match change {
                "process" => replies[end - 2] = Ok(vec![crate::instance::test_support::epoch(101)]),
                "recovery" | "updateability" => {
                    let Ok(rows) = &mut replies[end - 1] else { panic!() };
                    rows[0] = replace(
                        &rows[0],
                        &[(
                            if change == "recovery" { "recovery_fork_guid" } else { "updateability" },
                            Val::Text(if change == "recovery" { "other-fork" } else { "READ_ONLY" }.into()),
                        )],
                    );
                }
                "capability" => {
                    let Ok(rows) = &mut replies[end - 7] else { panic!() };
                    rows[0] = replace(&rows[0], &[("replicas", Val::Int(0))]);
                    assert!(replies.remove(end - 5).is_ok(), "the original final replica catalog was readable");
                }
                _ => unreachable!(),
            }
            let mut source =
                Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
            let original = source.describe().await.unwrap();
            drain(&mut source).await;
            let result = source.finish().await;
            assert!(result.is_err() || matches!(result, Ok(SourceCompletion::Changed)), "{change}");
            assert!(original.query_history().unwrap().population_catalog.is_none());
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}

#[tokio::test]
async fn a_native_group_roster_larger_than_the_existing_header_bound_withholds_completion_without_trimming() {
    for edition in [3, 8] {
        let mut rows = description(edition, true);
        rows.extend([
            Ok(vec![instance_pressure_row()]),
            Ok((0..=MAX_QUERY_HISTORY_DIMENSION_VALUES as i64)
                .map(|group| identified_runtime(group, 1000 + group))
                .collect()),
        ]);
        let mut source = Source::with_io(FakeRead::new(rows), &request(), "fixture.invalid".into(), 1433).unwrap();
        let original = source.describe().await.unwrap();
        assert!(matches!(
            source.read_page(TelemetryStream::QueryRuntime, None).await.unwrap(),
            CollectionRead::Stopped(CollectionStop::RowLimit)
        ));
        assert!(source.finish().await.is_err());
        assert!(original.query_history().unwrap().population_catalog.is_none());
        assert_eq!(source.query_count(), 10);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn positive_native_unpartitioned_rows_keep_no_synthetic_group_and_own_the_same_definition_namespace() {
    for edition in [3, 8] {
        let runtime = replace(&identified_runtime(7, 1000), &[("replica_group_id", Val::Null)]);
        let wait = replace(&identified_wait(7, 37), &[("replica_group_id", Val::Null)]);
        let mut source = Source::with_io(
            FakeRead::new(complete_replies(edition, false, vec![runtime], vec![wait])),
            &request(),
            "fixture.invalid".into(),
            1433,
        )
        .unwrap();
        let original = source.describe().await.unwrap();
        let records = drain(&mut source).await;
        let TelemetryRecord::QueryRuntime(row) = &records[0] else { panic!() };
        assert!(!row.key.native.qualifiers.contains_key("replica_group_id"));
        assert!(row.key.references.is_resolved());
        assert_eq!(row.metric(&RuntimeMetricKey::Duration).unwrap().unit, RuntimeMetricUnit::Microseconds);
        let TelemetryRecord::QueryWaits(wait) = &records[1] else { panic!() };
        assert!(!wait.key.native.qualifiers.contains_key("replica_group_id"));
        assert_eq!(wait.key.references, row.key.references);
        assert_eq!(wait.key.cohort, row.key.cohort);
        assert_eq!(wait.category, "3");
        assert_eq!(wait.unit, RuntimeMetricUnit::Milliseconds);
        assert_eq!(wait.total, MeasurementValue::Integer("37".into()));
        assert!(wait.last.is_none() && wait.standard_deviation.is_none());
        let final_source = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
        assert_eq!(
            final_source.query_history().unwrap().population_binding,
            QueryHistoryPopulationBinding::Unpartitioned
        );
        let capture = CaptureIdentity { observation_id: "native-unpartitioned-history".into(), revision: None };
        for row in records {
            let first = ExportedRecord::new(&original, &capture, row).unwrap();
            assert_eq!(first, ExportedRecord::new(&final_source, &capture, first.record.clone()).unwrap());
        }
        assert_eq!(source.query_count(), 24);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn denied_metadata_or_unknown_required_native_layout_stops_before_interval_or_body_work() {
    for edition in [3, 8] {
        for changes in [
            vec![("metadata_visibility", Val::Int(0))],
            vec![("metadata_visibility", Val::Null)],
            vec![("replicas", Val::Null)],
        ] {
            let mut replies = description(edition, true);
            let Ok(rows) = &mut replies[4] else { panic!() };
            rows[0] = replace(&rows[0], &changes);
            let mut source =
                Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
            assert!(source.describe().await.is_err());
            assert_eq!(source.query_count(), 5);
            assert!(
                source
                    .session
                    .calls
                    .iter()
                    .all(|call| !call.sql.contains("FROM sys.query_store_runtime_stats_interval"))
            );
            assert!(source.describe().await.is_err());
            assert_eq!(source.query_count(), 5);
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}

fn with_read_mode(mut replies: Vec<ReadResult<Vec<Rec>>>, mode: &str) -> Vec<ReadResult<Vec<Rec>>> {
    for rows in replies.iter_mut().filter_map(|reply| reply.as_mut().ok()) {
        for row in rows {
            if row.columns().any(|(name, _)| name == "updateability") {
                *row = replace(row, &[("updateability", Val::Text("READ_ONLY".into()))]);
            }
            if row.columns().any(|(name, _)| name == "actual_state_desc") {
                *row = replace(row, &[("actual_state_desc", Val::Text(mode.into()))]);
            }
        }
    }
    replies
}

#[tokio::test]
async fn readonly_local_profiles_preserve_historical_populations_without_claiming_the_current_member_executed_them() {
    for edition in [3, 8] {
        for (mode, capture) in [("READ_ONLY", "false"), ("READ_CAPTURE_SECONDARY", "true")] {
            let replies = with_read_mode(
                complete_replies(edition, true, vec![identified_runtime(7, 1000)], vec![identified_wait(7, 37)]),
                mode,
            );
            let mut source =
                Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
            let original = source.describe().await.unwrap();
            let history = original.query_history().unwrap();
            assert_eq!(history.native_properties["actual_state"], mode);
            assert_eq!(history.native_properties["local_capture_permitted"], capture);
            assert_eq!(history.acquisition_owner.as_ref().unwrap().database_writable, Some(false));
            assert!(history.population_catalog.is_none());
            let records = drain(&mut source).await;
            let completed = original.completed(source.finish().await.unwrap()).unwrap().unwrap();
            assert_eq!(completed.target, original.target);
            assert_eq!(completed.partition, SourcePartition::Target);
            assert!(completed.target.database().unwrap().incarnation.is_none());
            let history = completed.query_history().unwrap();
            assert_eq!(history.acquisition_owner.as_ref().unwrap().database_writable, Some(false));
            assert!(!history.population_catalog.as_ref().unwrap().has_complete_membership());
            assert_eq!(history.dimensions["replica_group_id"], ["7"]);
            let capture = CaptureIdentity { observation_id: "scripted-readonly-history".into(), revision: None };
            for row in records {
                let original_row = ExportedRecord::new(&original, &capture, row).unwrap();
                assert_eq!(
                    ExportedRecord::new(&completed, &capture, original_row.record.clone()).unwrap(),
                    original_row
                );
            }
            assert_eq!(source.query_count(), 26);
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
            assert_eq!(source.session.closed, 1);
        }
    }
}

#[tokio::test]
async fn a_secondary_capture_mode_without_the_native_population_column_is_refused_before_body_reads() {
    for edition in [3, 8] {
        let replies = with_read_mode(description(edition, false), "READ_CAPTURE_SECONDARY");
        let mut source = Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
        assert_eq!(source.describe().await.err().unwrap().code, culprit_core::ErrorCode::Unsupported);
        assert_eq!(source.query_count(), 6);
        assert!(source.session.calls.iter().all(|call| !call.sql.contains("FROM culprit_runtime")));
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), 6, "refused description cannot restart acquisition");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn missing_native_wait_catalog_remains_an_explicit_gap_without_a_wait_or_replacement_dispatch() {
    for edition in [3, 8] {
        let mut replies = complete_replies(edition, false, vec![], vec![]);
        let Ok(settings) = &mut replies[4] else { panic!() };
        settings[0] = replace(&settings[0], &[("waits", Val::Int(0))]);
        let mut source = Source::with_io(FakeRead::new(replies), &request(), "fixture.invalid".into(), 1433).unwrap();
        let original = source.describe().await.unwrap();
        assert_eq!(original.query_history().unwrap().waits.as_ref().unwrap().capture, WaitCaptureState::Unavailable);
        assert!(
            matches!(source.read_page(TelemetryStream::QueryRuntime, None).await.unwrap(), CollectionRead::Page { records, next_cursor: None } if records.is_empty())
        );
        let before = source.query_count();
        assert!(matches!(
            source.read_page(TelemetryStream::QueryWaits, None).await.unwrap(),
            CollectionRead::Stopped(CollectionStop::SourceUnavailable)
        ));
        assert_eq!(source.query_count(), before);
        assert!(source.finish().await.is_err());
        assert_eq!(
            source.query_count(),
            before,
            "unavailable family cannot be healed with a metadata or pressure read"
        );
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn the_same_total_caller_budget_reserves_setup_and_cannot_erase_the_required_final_owner_read() {
    for edition in [3, 8] {
        for total in [24, 25] {
            let mut request = request();
            request.budget.max_queries = total;
            let reader_budget = crate::read::ReadSession::budget_after_setup(request.budget.clone()).unwrap();
            assert_eq!(reader_budget.max_queries, total - 1);
            let runtime = replace(&identified_runtime(7, 1000), &[("replica_group_id", Val::Null)]);
            let wait = replace(&identified_wait(7, 37), &[("replica_group_id", Val::Null)]);
            let replies = complete_replies(edition, false, vec![runtime], vec![wait]);
            let reader = FakeRead::new(replies).with_query_limit(reader_budget.max_queries);
            let mut source = Source::with_io(reader, &request, "fixture.invalid".into(), 1433).unwrap();
            let original = source.describe().await.unwrap();
            drain(&mut source).await;
            let completion = source.finish().await;
            if total == 25 {
                assert!(original.completed(completion.unwrap()).unwrap().is_some());
                assert_eq!(source.query_count(), 24);
            } else {
                assert_eq!(CollectionStop::from_error(&completion.err().unwrap()), CollectionStop::QueryLimit);
                assert_eq!(source.query_count(), 23);
                assert!(source.finish().await.is_err());
                assert_eq!(
                    source.query_count(),
                    23,
                    "a tighter caller cannot purchase a replacement final authority point"
                );
            }
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
            assert_eq!(source.session.closed, 1);
        }
    }
}
