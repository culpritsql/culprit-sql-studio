use super::*;
use crate::{
    activity::{ReadOptions, current_request_work::tests as current, worker_starvation::tests as workers},
    instance::test_support::{epoch, metadata},
    read::{ReadError, ReadResult},
    resources::test_support::replace,
    value::{Rec, Val},
};
use std::sync::atomic::Ordering;

pub(crate) const SELECTED_SQL: &str = "SELECT id FROM dbo.items WHERE secret='private-profile-literal'";
pub(crate) const XML: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT id FROM dbo.items WHERE secret='private-profile-literal'" QueryHash="0x0123456789ABCDEF" QueryPlanHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="true" EstimateRows="100" EstimatedTotalSubtreeCost="1"><RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="999" ActualExecutions="1" ActualCPUms="987" /></RunTimeInformation><TableScan><Object Database="[shop]" Schema="[dbo]" Table="[items]"/><Predicate><ScalarOperator ScalarString="[secret]='private-profile-literal'"/></Predicate></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
pub(crate) fn cache(at: &str, generation: i64) -> Rec {
    let row = replace(
        &current::cache(at, generation),
        &[
            ("plan", Val::Text(XML.into())),
            ("plan_bytes", Val::Int((XML.encode_utf16().count() * 2) as i64)),
            ("statement", Val::Text(SELECTED_SQL.into())),
            ("statement_bytes", Val::Int((SELECTED_SQL.encode_utf16().count() * 2) as i64)),
            ("text_prefix", Val::Text(SELECTED_SQL.into())),
            ("source_text_bytes", Val::Int((SELECTED_SQL.encode_utf16().count() * 2) as i64)),
        ],
    );
    let mut fields = row.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    fields.extend([("module_database_id", Val::Null), ("module_object_id", Val::Null), ("module_number", Val::Null)]);
    Rec::of(&fields)
}
pub(crate) fn layout(visible: bool, available: bool) -> Rec {
    Rec::of(&[
        ("metadata_visibility", Val::Bool(visible)),
        ("last_actual", Val::Int(1)),
        ("live_xml", Val::Int(1)),
        ("profiles", Val::Int(1)),
        ("thread_profiles", Val::Int(i64::from(available))),
        ("request_page_server_read_capability", Val::Int(2)),
    ])
}
pub(crate) fn task(at: &str, address: i64) -> Rec {
    replace(
        &workers::row(3),
        &[
            ("observed_at", Val::Text(at.into())),
            ("session_generation", Val::Text("2020-01-01T04:00:00.0030000".into())),
            ("request_generation", Val::Text("2020-01-01T05:59:00.0030000".into())),
            ("transaction_id", Val::Null),
            ("transaction_start", Val::Null),
            ("sql_handle", Val::Text("AABB".into())),
            ("worker_task_address", Val::Text(format!("{address:016X}"))),
            ("worker_current_task_address", Val::Text(format!("{address:016X}"))),
            ("assigned_worker_address", Val::Text(format!("{:016X}", address + 100))),
            ("worker_execution_context_id", Val::Int(address)),
            ("blocking_session_id", Val::Int(0)),
            ("wait_type", Val::Text("CXCONSUMER".into())),
        ],
    )
}
pub(crate) fn profile(thread: i64, rows: i64) -> Rec {
    Rec::of(&[
        ("row_kind", Val::Text("profile".into())),
        ("profiles_observed_from", Val::Text("2020-01-01T06:00:00.3Z".into())),
        ("node_id", Val::Int(0)),
        ("thread_id", Val::Int(thread)),
        ("task_address", Val::Text(format!("{thread:016X}"))),
        ("physical_operator_name", Val::Text("Table Scan".into())),
        ("row_count", Val::Int(rows)),
        ("actual_read_row_count", Val::Int(rows)),
        ("estimate_row_count", Val::Int(100)),
        ("elapsed_time_ms", Val::Int(12)),
        ("cpu_time_ms", Val::Int(0)),
        ("logical_read_count", Val::Int(2)),
        ("physical_read_count", Val::Null),
        ("read_ahead_count", Val::Null),
        ("write_page_count", Val::Int(0)),
        ("end_of_scan_count", Val::Int(0)),
        ("scan_count", Val::Int(1)),
        ("rebind_count", Val::Int(0)),
        ("rewind_count", Val::Int(0)),
        ("open_time", Val::Int(0)),
        ("close_time", Val::Int(0)),
        ("database_id", Val::Int(7)),
        ("object_id", Val::Int(101)),
        ("index_id", Val::Int(0)),
    ])
}
pub(crate) fn guard() -> Rec {
    let row = current::point("2020-01-01T06:00:00.5Z");
    let mut fields = row.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    fields.extend([
        ("row_kind", Val::Text("guard".into())),
        ("profiles_observed_from", Val::Text("2020-01-01T06:00:00.3Z".into())),
        ("session_established", Val::Bool(true)),
        ("request_count", Val::Int(1)),
        ("selected_database_id", Val::Int(7)),
        ("selected_database_name", Val::Text("shop".into())),
        ("reader_session_id", Val::Int(90)),
        ("reader_session_generation", Val::Text("2020-01-01T03:00:00.0030000".into())),
        ("database_updateability", Val::Text("READ_WRITE".into())),
        ("request_page_server_read_capability", Val::Int(2)),
    ]);
    Rec::of(&fields)
}
pub(crate) fn replies() -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![current::authority("2020-01-01T06:00:00Z", Some("READ_WRITE"))]),
        Ok(vec![current::point("2020-01-01T06:00:00.1Z")]),
        Ok(vec![task("2020-01-01T06:00:00.15Z", 1), task("2020-01-01T06:00:00.15Z", 2)]),
        Ok(vec![layout(true, true)]),
        Ok(vec![cache("2020-01-01T06:00:00.2Z", 4)]),
        Ok(vec![profile(1, 1), profile(2, 9007199254740993), guard()]),
        Ok(vec![task("2020-01-01T06:00:00.6Z", 1), task("2020-01-01T06:00:00.6Z", 2)]),
        Ok(vec![cache("2020-01-01T06:00:00.7Z", 4)]),
        Ok(vec![current::point("2020-01-01T06:00:00.8Z")]),
        Ok(vec![crate::relational_inputs::tests::binding_row(
            &crate::cached_statements::context(&cache("2020-01-01T06:00:00.7Z", 4)).unwrap(),
        )]),
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![current::authority("2020-01-01T06:00:01Z", Some("READ_WRITE"))]),
    ]
}
pub(crate) fn reader(io: current::Io) -> ActivityReader<current::Io> {
    let recipe = crate::telemetry::recipes()
        .into_iter()
        .find(|recipe| {
            recipe.selector
                == RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::CurrentExecutionProfile))
        })
        .expect("selected execution profile recipe");
    ActivityReader::new(
        io,
        ReadOptions {
            budget: recipe.budget.intersection(&CollectionBudget::default()),
            identity: IdentityDetail::Endpoint,
        },
        "fixture.invalid".into(),
        1433,
    )
}
#[tokio::test]
async fn exact_held_request_threads_and_selected_parser_context_reuse_native_owners_without_cached_runtime() {
    for writable in [true, false] {
        let focus = current::focus_for_role(writable).await;
        let mut native = replies();
        for response in &mut native {
            if let Ok(rows) = response {
                for row in rows {
                    *row = replace(
                        row,
                        &[
                            ("updateability", Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into())),
                            (
                                "database_updateability",
                                Val::Text(if writable { "READ_WRITE" } else { "READ_ONLY" }.into()),
                            ),
                        ],
                    );
                }
            }
        }
        let mut reader = reader(current::io(native));
        let point = reader.capture_current_execution_profile(&focus).await.unwrap();
        assert!(point.is_valid());
        assert_eq!(reader.query_count(), 15);
        assert!(CurrentExecutionProfileEvidence::from_point(&point).unwrap().has_relational_input_context());
        let MssqlExecutionProfiles::Observed { rows, operators, .. } = &point.profiles else { panic!() };
        assert_eq!(rows[1].counters.rows_returned.as_deref(), Some("9007199254740993"));
        assert!(rows[0].counters.physical_reads.is_none());
        let MssqlExecutionOperators::Observed { nodes, .. } = operators else { panic!() };
        assert_eq!(nodes[0].kind, culprit_core::NodeKind::Scan);
        assert_eq!(nodes[0].parallel, Some(true));
        assert!(nodes[0].object.as_deref().unwrap().contains("items"));
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert!(evidence.nodes[0].rows_returned.uneven());
        assert_eq!(evidence.nodes[0].threads[0].task_context, ExecutionTaskContext::CheckedAtBounds);
        let encoded = serde_json::to_string(&point).unwrap();
        assert!(!encoded.contains("private-profile-literal"));
        let compact = serde_json::to_value(nodes).unwrap();
        assert!(compact[0].get("act_rows").is_none());
        assert!(compact[0].get("runtime").is_none(), "cached actual counters never become current execution facts");
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        assert_eq!(reader.io.trace.closes.load(Ordering::SeqCst), 1);
        let calls = reader.io.trace.calls.lock().unwrap();
        assert_eq!(calls.iter().filter(|sql| sql.contains("FROM sys.dm_exec_query_stats stats")).count(), 2);
        assert_eq!(calls.iter().filter(|sql| sql.contains("worker.worker_address=task.worker_address")).count(), 2);
        assert_eq!(calls.iter().filter(|sql| sql.contains("FROM sys.dm_exec_query_profiles p")).count(), 1);
        assert!(calls.iter().all(|sql| !sql.contains("STATISTICS PROFILE ON")
            && !sql.contains("SET STATISTICS XML")
            && !sql.contains("total_worker_time")));
    }
}
#[tokio::test]
async fn empty_hidden_and_absent_profiling_preserve_request_work_without_healthy_zero_or_enabling() {
    for unavailable in ["empty", "absent", "hidden"] {
        let mut native = replies();
        if unavailable == "empty" {
            native[7] = Ok(vec![guard()]);
        } else {
            native[5] = Ok(vec![layout(unavailable != "hidden", false)]);
            native.remove(7).unwrap();
        }
        let mut reader = reader(current::io(native));
        let point = reader.capture_current_execution_profile(&current::focus().await).await.unwrap();
        assert_eq!(point.work.as_ref().unwrap().reads.as_deref(), Some("9007199254740993"));
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert!(evidence.nodes.is_empty());
        if unavailable == "empty" {
            assert!(matches!(&point.profiles,MssqlExecutionProfiles::Observed{rows,..} if rows.is_empty()));
        } else {
            assert!(matches!(point.profiles, MssqlExecutionProfiles::Unavailable { .. }));
        }
        assert_eq!(reader.query_count(), if unavailable == "empty" { 15 } else { 14 });
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn native_errors_and_cancellation_stop_before_any_replacement_client_or_final_dispatch() {
    for index in [4, 5, 6, 7, 8, 9, 10, 11, 14] {
        let mut native = replies();
        native[index] = Err(ReadError::Source(culprit_core::Error::new(
            culprit_core::ErrorCode::QueryFailed,
            "controlled native failure",
        )));
        let mut reader = reader(current::io(native));
        assert!(reader.capture_current_execution_profile(&current::focus().await).await.is_err());
        assert_eq!(reader.query_count(), index as u32 + 1);
        assert_eq!(reader.io.trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
    for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
        let mut native = replies();
        native[7] = Err(ReadError::Stopped(stop));
        let mut reader = reader(current::io(native));
        assert_eq!(
            CollectionStop::from_error(
                &reader.capture_current_execution_profile(&current::focus().await).await.unwrap_err()
            ),
            stop
        );
        assert_eq!(reader.query_count(), 8);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn replaced_actor_plan_or_cache_generation_never_becomes_a_bound_operator_or_recovered_request() {
    for defect in ["login", "request", "statement", "plan", "cache-generation", "guard-plan"] {
        let mut native = replies();
        if defect == "cache-generation" {
            native[9] = Ok(vec![cache("2020-01-01T06:00:00.7Z", 5)]);
        } else if defect == "guard-plan" {
            native[7] =
                Ok(vec![profile(1, 1), replace(&guard(), &[("checked_plan_handle", Val::Text("EEFF".into()))])]);
        } else {
            let changes = match defect {
                "login" => vec![("checked_session_generation", Val::Text("2020-01-01T04:00:01.0030000".into()))],
                "request" => vec![("checked_request_generation", Val::Text("2020-01-01T05:59:01.0030000".into()))],
                "statement" => vec![("checked_sql_handle", Val::Text("EEFF".into()))],
                _ => vec![("checked_plan_handle", Val::Text("EEFF".into()))],
            };
            native[10] = Ok(vec![replace(&current::point("2020-01-01T06:00:00.8Z"), &changes)]);
        }
        if defect != "guard-plan" {
            native.remove(11).unwrap();
        }
        let mut reader = reader(current::io(native));
        let point = reader.capture_current_execution_profile(&current::focus().await).await.unwrap();
        if matches!(defect, "login" | "request" | "statement") {
            assert_eq!(point.membership, FocusedRequestMembership::NotObserved);
            assert!(point.work.is_none());
        } else if defect == "cache-generation" {
            assert!(matches!(
                point.profiles,
                MssqlExecutionProfiles::Observed { operators: MssqlExecutionOperators::Unavailable { .. }, .. }
            ));
        } else {
            assert!(matches!(
                point.profiles,
                MssqlExecutionProfiles::Unavailable { reason: MssqlExecutionProfilesUnavailable::PlanChanged }
            ));
        }
        assert_eq!(reader.query_count(), if defect == "guard-plan" { 15 } else { 14 });
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn reused_worker_tokens_and_unaligned_endpoint_clocks_withhold_attribution_or_refuse() {
    for defect in ["creation", "binding", "address", "clock", "database", "generation"] {
        let mut native = replies();
        match defect {
            "clock" => native[8] = Ok(vec![task("2020-01-01T06:00:00.1Z", 1)]),
            "database" => {
                native[7] = Ok(vec![profile(1, 1), replace(&guard(), &[("selected_database_id", Val::Int(8))])])
            }
            "generation" => (),
            _ => {
                let field = match defect {
                    "creation" => "worker_created_ms_ticks",
                    "binding" => "task_bound_ms_ticks",
                    _ => "worker_current_task_address",
                };
                let value = if defect == "address" { Val::Text("00000000000000FF".into()) } else { Val::Int(999) };
                native[8] = Ok(vec![
                    replace(&task("2020-01-01T06:00:00.6Z", 1), &[(field, value)]),
                    task("2020-01-01T06:00:00.6Z", 2),
                ]);
            }
        }
        let mut io = current::io(native);
        if defect == "generation" {
            io.change_at = Some(8);
        }
        let mut reader = reader(io);
        let result = reader.capture_current_execution_profile(&current::focus().await).await;
        if matches!(defect, "clock" | "database" | "generation") {
            assert!(result.is_err(), "{defect}");
        } else {
            let point = result.unwrap();
            let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
            assert_eq!(evidence.nodes[0].threads[0].task_context, ExecutionTaskContext::Changed);
            assert!(evidence.nodes[0].threads[0].endpoint_wait.is_none());
        }
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn profile_native_null_negative_width_duplicate_and_lookahead_have_explicit_outcomes() {
    for defect in ["null", "negative", "width", "duplicate", "lookahead"] {
        let mut native = replies();
        let row = match defect {
            "null" => replace(&profile(1, 1), &[("row_count", Val::Null)]),
            "negative" => profile(1, -1),
            "width" => replace(&profile(1, 1), &[("row_count", Val::Text("9223372036854775808".into()))]),
            _ => profile(1, 1),
        };
        native[7] = Ok(if defect == "duplicate" {
            vec![row.clone(), row, guard()]
        } else if defect == "lookahead" {
            vec![row, profile(2, 10), profile(3, 20), guard()]
        } else {
            vec![row, profile(2, 10), guard()]
        });
        let mut reader = reader(current::io(native));
        if defect == "lookahead" {
            reader.options.budget.max_snapshot_rows = 4;
        }
        let result = reader.capture_current_execution_profile(&current::focus().await).await;
        if matches!(defect, "width" | "duplicate") {
            assert!(result.is_err(), "{defect}");
        } else {
            let point = result.unwrap();
            let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
            if defect == "lookahead" {
                assert!(evidence.profiles_limited);
                assert_eq!(evidence.profile_rows, 2);
            } else {
                assert!(!evidence.nodes[0].rows_returned.uneven());
                assert_eq!(evidence.nodes[0].rows_returned.interpreted, 1);
            }
        }
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[test]
fn selected_native_future_is_bounded_without_opening_or_polling_a_client() {
    fn bytes<Args, F, Fut>(_: F) -> usize
    where
        F: FnOnce(Args) -> Fut,
    {
        std::mem::size_of::<Fut>()
    }
    let size = bytes(
        |(reader, focus): (&'static mut ActivityReader<crate::read::ReadSession>, &'static ActivityRequestFocus)| {
            reader.capture_current_execution_profile(focus)
        },
    );
    assert!(size <= 4096, "selected held native future: {size}");
    assert!(std::mem::size_of::<Result<Box<MssqlCurrentExecutionProfilePoint>>>() <= 4096);
}

mod relational_inputs;

#[tokio::test]
async fn unknown_or_internal_selected_origin_keeps_native_profiles_but_never_spends_module_verification_or_claims_input_context()
 {
    for flags in [
        [("engine_internal", Val::Int(0)), ("cloud_internal", Val::Null)],
        [("engine_internal", Val::Null), ("cloud_internal", Val::Int(0))],
        [("engine_internal", Val::Int(1)), ("cloud_internal", Val::Int(0))],
    ] {
        let mut native = replies();
        native[6] = Ok(vec![replace(&cache("2020-01-01T06:00:00.2Z", 4), &flags)]);
        native[9] = Ok(vec![replace(&cache("2020-01-01T06:00:00.7Z", 4), &flags)]);
        native.remove(11).unwrap(); // The genuine source predicate refuses provenance before requesting its independent verifier.
        let mut reader = reader(current::io(native));
        let point = reader.capture_current_execution_profile(&current::focus().await).await.unwrap();
        assert!(point.is_valid());
        assert!(matches!(point.profiles, MssqlExecutionProfiles::Observed { .. }));
        assert!(point.relational_inputs.is_none());
        assert!(!CurrentExecutionProfileEvidence::from_point(&point).unwrap().has_relational_input_context());
        assert_eq!(reader.query_count(), 14);
        assert!(reader.io.inner.calls.iter().all(|call| call.sql != crate::relational_inputs::binding_check_sql()));
        assert_eq!(reader.io.trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
