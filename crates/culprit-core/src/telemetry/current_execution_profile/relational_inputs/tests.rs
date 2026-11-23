//! Structural canonical bindings, not native acquisition or cost qualification.
use super::*;
use crate::plan_facts::{RelationalInput, RelationalInputKind, RelationalInputProvenance};

fn bound_point(known: bool) -> MssqlCurrentExecutionProfilePoint {
    let mut point = super::super::tests::point();
    let sql = "SELECT r.id FROM @Rows r";
    let xml = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT r.id FROM @Rows r" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="1"><TableScan><Object Table="[@Rows]" Alias="[r]"/></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let context = Box::new(CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: "0xCCDD".into(),
            sql_handle: "0xAABB".into(),
            statement_start_bytes: 0,
            statement_end_bytes: -1,
            created_without_offset: "2020-01-01T11:59:00.003".into(),
            plan_generation: "4".into(),
        },
        database_id: 7,
        query_hash: Some("0x0123456789ABCDEF".into()),
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: Some((sql.len() * 2).to_string()),
        source_text_complete: true,
    });
    point.statement = DefinitionPayload::sql(sql, crate::Engine::Mssql);
    let work = point.work.as_mut().unwrap();
    work.current_plan = MssqlCurrentRequestPlan::Observed {
        observed_from: "2020-01-01T12:00:00.56Z".into(),
        observed_until: "2020-01-01T12:00:00.79Z".into(),
        context: context.clone(),
        plan: DefinitionPayload::plan(xml, crate::Engine::Mssql),
    };
    let MssqlExecutionProfiles::Observed { operators, .. } = &mut point.profiles else { panic!() };
    let MssqlCurrentRequestPlan::Observed { plan: DefinitionPayload::Available { sha256, .. }, .. } =
        &work.current_plan
    else {
        panic!()
    };
    *operators = MssqlExecutionOperators::Observed {
        context: context.clone(),
        plan_sha256: sha256.clone(),
        nodes: vec![MssqlExecutionOperator {
            node_id: 0,
            parent_node_id: None,
            kind: crate::NodeKind::Scan,
            parallel: Some(false),
            inner_join: None,
            hash_build_input: None,
            relational_input: Some(RelationalInput {
                kind: if known { RelationalInputKind::TableValuedParameter } else { RelationalInputKind::Unknown },
                provenance: if known {
                    RelationalInputProvenance::NativeParameterMetadata
                } else {
                    RelationalInputProvenance::Unresolved
                },
            }),
            native_operator: "Table Scan".into(),
            object: Some("@Rows".into()),
            index: None,
            predicate: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            seek_predicate: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        }],
    };
    let mut inputs = MssqlExecutionRelationalInputs {
        context,
        module_parameters: known.then(|| MssqlModuleParameters {
            database_id: 7,
            object_id: 1001,
            module_number: 1,
            sql_handle: "0xAABB".into(),
            table_parameters: [("@Rows".into(), vec!["dbo".into(), "RowsType".into()])].into(),
        }),
        statement_utf16_bytes: (sql.encode_utf16().count() * 2).to_string(),
        statement_native_sha256: digest(sql.as_bytes()),
        plan_utf16_bytes: (xml.encode_utf16().count() * 2).to_string(),
        plan_native_sha256: digest(xml.as_bytes()),
        proof: None,
    };
    if known {
        inputs.proof = Some(Box::new(
            MssqlRelationalInputProof::for_execution(
                &point,
                &inputs,
                vec![MssqlRelationalInputBinding {
                    node_id: 0,
                    input: RelationalInput {
                        kind: RelationalInputKind::TableValuedParameter,
                        provenance: RelationalInputProvenance::NativeParameterMetadata,
                    },
                }],
            )
            .unwrap(),
        ));
    }
    point.relational_inputs = Some(Box::new(inputs));
    assert!(point.is_valid());
    point
}
#[test]
fn same_point_native_input_proof_roundtrips_without_granting_runtime_values_or_cost_authority() {
    let point = bound_point(true);
    let bytes = serde_json::to_vec(&point).unwrap();
    let stored: MssqlCurrentExecutionProfilePoint = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(stored, point);
    assert!(stored.is_valid());
    assert!(CurrentExecutionProfileEvidence::from_point(&stored).unwrap().has_relational_input_context());
    assert!(stored.relational_inputs.as_ref().unwrap().proof.is_some());
}
#[test]
fn unknown_type_and_unavailable_profiling_preserve_full_input_context_but_missing_payloads_do_not() {
    let mut point = bound_point(false);
    assert!(point.relational_inputs.as_ref().unwrap().proof.is_none());
    point.profiles =
        MssqlExecutionProfiles::Unavailable { reason: MssqlExecutionProfilesUnavailable::LayoutUnavailable };
    assert!(point.is_valid());
    assert!(CurrentExecutionProfileEvidence::from_point(&point).unwrap().has_relational_input_context());
    point.relational_inputs = None;
    assert!(point.is_valid());
    assert!(!CurrentExecutionProfileEvidence::from_point(&point).unwrap().has_relational_input_context());
    let mut point = bound_point(true);
    point.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized };
    assert!(!point.is_valid());
}
#[test]
fn original_parent_owner_clock_cache_and_payload_substitution_cannot_reuse_a_positive_type_proof() {
    for defect in
        ["parent", "owner", "clock", "cache", "module", "statement", "plan", "native-hash", "node", "missing-proof"]
    {
        let mut point = bound_point(true);
        match defect {
            "parent" => point.focus.parent.manifest_sha256 = digest(b"foreign-activity-original"),
            "owner" => point.context.partition = SourcePartition::Target,
            "clock" => point.context.observed_until = "2020-01-01T12:00:02Z".into(),
            "cache" => point.relational_inputs.as_mut().unwrap().context.identity.plan_generation = "5".into(),
            "module" => point.relational_inputs.as_mut().unwrap().module_parameters.as_mut().unwrap().object_id = 1002,
            "statement" => {
                point.statement = DefinitionPayload::sql("SELECT r.id FROM @Rows r WHERE r.id>0", crate::Engine::Mssql)
            }
            "plan" => {
                let MssqlCurrentRequestPlan::Observed { plan, .. } = &mut point.work.as_mut().unwrap().current_plan
                else {
                    panic!()
                };
                *plan = DefinitionPayload::plan("<ShowPlanXML/>", crate::Engine::Mssql);
            }
            "native-hash" => {
                point.relational_inputs.as_mut().unwrap().plan_native_sha256 = digest(b"another-original-plan")
            }
            "node" => point.relational_inputs.as_mut().unwrap().proof.as_mut().unwrap().bindings[0].node_id = 1,
            _ => point.relational_inputs.as_mut().unwrap().proof = None,
        }
        assert!(!point.is_valid(), "{defect}");
    }
}
#[test]
fn ordinary_redaction_is_idempotent_and_does_not_reclassify_or_drop_the_native_input_witness() {
    let mut point = bound_point(true);
    let original = point.relational_inputs.clone();
    point.redact();
    assert!(point.is_valid());
    assert_eq!(point.relational_inputs, original);
    let once = serde_json::to_vec(&point).unwrap();
    point.redact();
    assert_eq!(serde_json::to_vec(&point).unwrap(), once);
}

fn serial_rows_point(known: bool) -> MssqlCurrentExecutionProfilePoint {
    let mut point = bound_point(known);
    let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
    rows.truncate(1);
    rows[0].physical_operator = "Table Scan".into();
    rows[0].counters.rows_returned = Some("10000".into());
    rows[0].counters.estimated_rows = Some("1".into());
    rows[0].counters.scans = Some("1".into());
    rows[0].counters.end_of_scans = Some("1".into());
    rows[0].counters.rebinds = Some("0".into());
    rows[0].counters.rewinds = Some("0".into());
    rows[0].open_ticks = Some("100".into());
    rows[0].close_ticks = Some("120".into());
    assert!(point.is_valid());
    point
}
#[test]
fn same_serial_scan_compares_native_output_not_input_size_and_keeps_exact_bigints_and_zero() {
    for (returned, estimated, exceeds) in
        [("10000", "1", true), ("0", "0", false), ("9007199254740993", "9007199254740992", true), ("1", "10000", false)]
    {
        let mut point = serial_rows_point(true);
        let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
        rows[0].counters.rows_returned = Some(returned.into());
        rows[0].counters.rows_read = Some("9223372036854775807".into());
        rows[0].counters.estimated_rows = Some(estimated.into());
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        let comparison = evidence.nodes[0].input_rows.as_ref().unwrap();
        assert_eq!(
            comparison,
            &ExecutionInputRows::SerialScan { returned: returned.into(), estimated: estimated.into() }
        );
        assert_eq!(comparison.exceeds_estimate(), exceeds);
        assert!(evidence.is_valid());
    }
}
#[test]
fn repeated_parallel_partial_unknown_and_unobserved_ancestors_withhold_the_execution_denominator() {
    for defect in [
        "scan", "end", "rebind", "rewind", "ticks", "parallel", "threads", "parent", "flag", "limited", "null",
        "negative",
    ] {
        let mut point = serial_rows_point(true);
        let MssqlExecutionProfiles::Observed { rows, operators, limited, .. } = &mut point.profiles else { panic!() };
        let MssqlExecutionOperators::Observed { nodes, .. } = operators else { panic!() };
        match defect {
            "scan" => rows[0].counters.scans = Some("2".into()),
            "end" => rows[0].counters.end_of_scans = None,
            "rebind" => rows[0].counters.rebinds = Some("2".into()),
            "rewind" => rows[0].counters.rewinds = Some("1".into()),
            "ticks" => rows[0].close_ticks = Some("0".into()),
            "parallel" => nodes[0].parallel = Some(true),
            "threads" => {
                let mut row = rows[0].clone();
                row.thread_id += 1;
                rows.push(row);
            }
            "parent" => nodes[0].parent_node_id = Some(999),
            "flag" => nodes[0].parallel = None,
            "limited" => *limited = true,
            "null" => rows[0].counters.estimated_rows = None,
            _ => rows[0].counters.rows_returned = Some("-1".into()),
        }
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert!(matches!(evidence.nodes[0].input_rows, Some(ExecutionInputRows::Unavailable { .. })), "{defect}");
        assert!(evidence.is_valid());
    }
}
