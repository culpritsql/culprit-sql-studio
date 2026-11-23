//! The actual shared reader/parser/verifier over bounded controlled replies, never a native cost qualification.
use super::*;
use crate::read::Parameter;
use culprit_core::plan_facts::{RelationalInputKind, RelationalInputProvenance};

const SQL: &str = "SELECT r.id FROM @Rows r";
const PREFIX: &str =
    "CREATE PROC dbo.consume @secret nvarchar(100)='private-parameter-default',@Rows dbo.RowsType READONLY AS ";
const XML: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT r.id FROM @Rows r" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="1"><TableScan><Object Table="[@Rows]" Alias="[r]"/></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
fn offset() -> u32 {
    (PREFIX.encode_utf16().count() * 2).try_into().unwrap()
}
async fn selected_focus() -> ActivityRequestFocus {
    let identity = mssql_request_statement_identity("AABB", offset(), -1, 7).unwrap();
    current::focus_for_statement(true, &identity, SQL).await
}
fn module_cache(at: &str) -> Rec {
    let header = format!("{PREFIX}{SQL}");
    replace(
        &cache(at, 4),
        &[
            ("statement_start_offset", Val::Int(i64::from(offset()))),
            ("statement", Val::Text(SQL.into())),
            ("statement_bytes", Val::Int((SQL.encode_utf16().count() * 2) as i64)),
            ("plan", Val::Text(XML.into())),
            ("plan_bytes", Val::Int((XML.encode_utf16().count() * 2) as i64)),
            ("text_prefix", Val::Text(header.clone())),
            ("source_text_bytes", Val::Int((header.encode_utf16().count() * 2) as i64)),
            ("module_database_id", Val::Int(7)),
            ("module_object_id", Val::Int(1001)),
            ("module_number", Val::Int(1)),
        ],
    )
}
fn native() -> Vec<ReadResult<Vec<Rec>>> {
    let mut native = replies();
    native[3] = Ok(vec![replace(
        &current::point("2020-01-01T06:00:00.1Z"),
        &[("checked_begin", Val::Int(i64::from(offset())))],
    )]);
    native[6] = Ok(vec![module_cache("2020-01-01T06:00:00.2Z")]);
    let rows = replace(
        &profile(0, 10000),
        &[
            ("task_address", Val::Text("0000000000000001".into())),
            ("estimate_row_count", Val::Int(1)),
            ("end_of_scan_count", Val::Int(1)),
            ("open_time", Val::Int(100)),
            ("close_time", Val::Int(110)),
        ],
    );
    native[7] = Ok(vec![rows, replace(&guard(), &[("checked_begin", Val::Int(i64::from(offset())))])]);
    native[9] = Ok(vec![module_cache("2020-01-01T06:00:00.7Z")]);
    native[10] = Ok(vec![replace(
        &current::point("2020-01-01T06:00:00.8Z"),
        &[("checked_begin", Val::Int(i64::from(offset())))],
    )]);
    native
}
#[tokio::test]
async fn original_module_and_same_request_plan_use_the_shared_parser_and_one_independent_binding_read() {
    let focus = selected_focus().await;
    let mut reader = reader(current::io(native()));
    let point = reader.capture_current_execution_profile(&focus).await.unwrap();
    assert!(point.is_valid());
    assert_eq!(reader.query_count(), 15);
    let inputs = point.relational_inputs.as_ref().unwrap();
    assert_eq!(inputs.module_parameters.as_ref().unwrap().object_id, 1001);
    let proof = inputs.proof.as_ref().unwrap();
    assert!(proof.matches_execution(&point, inputs));
    assert_eq!(proof.bindings.len(), 1);
    assert_eq!(proof.bindings[0].node_id, 0);
    assert_eq!(proof.bindings[0].input.qualified_kind(), RelationalInputKind::TableValuedParameter);
    assert_eq!(proof.bindings[0].input.provenance, RelationalInputProvenance::NativeParameterMetadata);
    let MssqlExecutionProfiles::Observed { operators, .. } = &point.profiles else { panic!() };
    assert_eq!(operators.node(0).unwrap().relational_input, Some(proof.bindings[0].input));
    let calls = &reader.io.inner.calls;
    assert!(calls[6].sql.contains("st.objectid AS module_object_id"));
    assert_eq!(calls[6].sql, calls[9].sql);
    let (Parameter::Text(selected), Parameter::Text(rechecked)) = (&calls[6].parameters[0], &calls[9].parameters[0])
    else {
        panic!()
    };
    assert_eq!(selected, rechecked);
    assert!(selected.contains(&format!("begin=\"{}\"", offset())));
    assert_eq!(calls[11].sql, crate::relational_inputs::binding_check_sql());
    assert_eq!(calls[11].limit, 2);
    let Parameter::Text(plans) = &calls[11].parameters[0] else { panic!() };
    assert_eq!(plans, "<plans><plan handle=\"0xccdd\"/></plans>");
    assert_eq!(calls.iter().filter(|call| call.sql == crate::relational_inputs::binding_check_sql()).count(), 1);
    assert!(calls.iter().all(|call| !call.sql.contains("sys.parameters") && !call.sql.contains("sys.sql_modules")));
    let encoded = serde_json::to_string(&point).unwrap();
    assert!(!encoded.contains("private-parameter-default"));
    assert!(!encoded.contains("CREATE PROC"));
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn missing_truncated_foreign_or_changed_original_header_never_mints_a_type_witness() {
    for defect in ["missing", "truncated", "foreign", "number", "changed"] {
        let mut native = native();
        let values = match defect {
            "missing" => vec![("statement", Val::Null), ("statement_bytes", Val::Null)],
            "truncated" => vec![
                ("text_prefix", Val::Text("CREATE PROC dbo.consume @Rows dbo.RowsType READONLY".into())),
                ("text_complete", Val::Bool(false)),
            ],
            "foreign" => vec![("module_database_id", Val::Int(8))],
            "number" => vec![("module_number", Val::Int(2))],
            _ => vec![("module_object_id", Val::Int(1002))],
        };
        native[9] = Ok(vec![replace(&module_cache("2020-01-01T06:00:00.7Z"), &values)]);
        if defect != "changed" {
            native.remove(11).unwrap();
        }
        let mut reader = reader(current::io(native));
        let result = reader.capture_current_execution_profile(&selected_focus().await).await;
        if defect == "changed" {
            assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::SourceChanged);
        } else {
            let point = result.unwrap();
            assert!(point.relational_inputs.is_none(), "{defect}");
            assert!(!CurrentExecutionProfileEvidence::from_point(&point).unwrap().has_relational_input_context());
            if let MssqlExecutionProfiles::Observed { operators, .. } = &point.profiles {
                assert!(
                    operators
                        .node(0)
                        .and_then(|node| node.relational_input)
                        .is_none_or(|input| input.qualified_kind() != RelationalInputKind::TableValuedParameter)
                );
            }
        }
        assert_eq!(reader.io.trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn reused_missing_or_unconfirmed_plan_sql_verification_stops_before_final_native_work() {
    for defect in ["changed-token", "missing-token", "cancelled", "timed-out", "unconfirmed"] {
        let mut native = native();
        native[11] = match defect {
            "changed-token" => Ok(vec![Rec::of(&[
                ("plan_handle", Val::Text("0xCCDD".into())),
                ("sql_handle", Val::Text("0xEEFF".into())),
            ])]),
            "missing-token" => Ok(vec![]),
            "cancelled" => Err(ReadError::Stopped(CollectionStop::Cancelled)),
            "timed-out" => Err(ReadError::Stopped(CollectionStop::TimedOut)),
            _ => Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)),
        };
        let mut io = current::io(native);
        if defect == "unconfirmed" {
            io.inner.shutdown = SourceShutdown::Unconfirmed;
        }
        let mut reader = reader(io);
        let error = reader.capture_current_execution_profile(&selected_focus().await).await.unwrap_err();
        if defect == "changed-token" {
            assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceChanged);
        }
        assert_eq!(reader.query_count(), 12);
        assert_eq!(reader.io.trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        assert_eq!(
            reader.close().await,
            if defect == "unconfirmed" { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
        );
    }
}
#[tokio::test]
async fn ordinary_current_work_never_requests_module_columns_or_spends_profile_verification() {
    let mut reader = ActivityReader::new(
        current::io(current::replies()),
        ReadOptions { budget: CollectionBudget::default(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    reader.capture_current_request_work(&current::focus().await).await.unwrap();
    assert_eq!(reader.query_count(), 10);
    {
        let calls = reader.io.trace.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .all(|sql| !sql.contains("module_object_id") && sql != crate::relational_inputs::binding_check_sql())
        );
    }
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
