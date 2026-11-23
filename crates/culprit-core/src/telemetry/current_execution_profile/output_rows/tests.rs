use super::*;

fn native_point() -> MssqlCurrentExecutionProfilePoint {
    // Native counter/identity fixture with its client host label anonymized. The original required-TVP
    // assertion failed; this recorded shape is not a successful native or causal proof.
    serde_json::from_str(include_str!("native_lightweight_point.json")).unwrap()
}
fn input(evidence: &CurrentExecutionProfileEvidence) -> &ExecutionProfileNodeEvidence {
    evidence.nodes.iter().find(|node| node.node_id == 2).unwrap()
}

#[test]
fn actual_lightweight_native_512_keeps_unknown_kind_null_scans_zero_clocks_and_strict_refusal() {
    let point = native_point();
    assert!(point.is_valid());
    let original = point.clone();
    let MssqlExecutionProfiles::Observed { rows, .. } = &point.profiles else { panic!() };
    let row = rows.iter().find(|row| row.node_id == 2).unwrap();
    assert_eq!(row.counters.scans, None);
    assert_eq!(row.open_ticks.as_deref(), Some("0"));
    assert_eq!(row.close_ticks.as_deref(), Some("0"));
    assert!(point.relational_inputs.as_ref().unwrap().proof.is_none());
    let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
    assert!(evidence.is_valid());
    let node = input(&evidence);
    assert_eq!(
        node.operator.as_ref().unwrap().relational_input.unwrap().qualified_kind(),
        crate::plan_facts::RelationalInputKind::Unknown
    );
    assert_eq!(
        node.input_rows,
        Some(ExecutionInputRows::Unavailable { reason: ExecutionInputRowsUnavailable::RepeatedOrUnknownScans })
    );
    assert_eq!(
        node.output_so_far,
        Some(OutputRowsSoFar {
            returned: "512".into(),
            estimated: "1".into(),
            normalization_unavailable: ExecutionInputRowsUnavailable::RepeatedOrUnknownScans
        })
    );
    assert_eq!(point, original);
    let read: CurrentExecutionProfileEvidence =
        serde_json::from_slice(&serde_json::to_vec(&evidence).unwrap()).unwrap();
    assert_eq!(read, evidence);
}

#[test]
fn known_repeated_traversals_and_exact_zero_or_large_counts_remain_cumulative_not_single_scan_values() {
    for (returned, estimate) in [("512", "1"), ("0", "0"), ("9007199254740993", "9007199254740992")] {
        let mut point = native_point();
        let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
        let row = rows.iter_mut().find(|row| row.node_id == 2).unwrap();
        row.counters.rows_returned = Some(returned.into());
        row.counters.estimated_rows = Some(estimate.into());
        row.counters.scans = Some("2".into());
        row.counters.rebinds = Some("2".into());
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert!(evidence.is_valid());
        let node = input(&evidence);
        assert!(matches!(node.input_rows, Some(ExecutionInputRows::Unavailable { .. })));
        let output = node.output_so_far.as_ref().unwrap();
        assert_eq!((output.returned.as_str(), output.estimated.as_str()), (returned, estimate));
    }
}

#[test]
fn partial_parallel_multi_thread_missing_or_negative_counts_withhold_output_and_foreign_owner_refuses() {
    for defect in ["partial", "parallel", "two-threads", "null", "negative"] {
        let mut point = native_point();
        let MssqlExecutionProfiles::Observed { rows, operators, limited, .. } = &mut point.profiles else { panic!() };
        match defect {
            "partial" => *limited = true,
            "parallel" => {
                let MssqlExecutionOperators::Observed { nodes, .. } = operators else { panic!() };
                nodes.iter_mut().find(|node| node.node_id == 2).unwrap().parallel = Some(true);
            }
            "two-threads" => {
                let mut row = rows.iter().find(|row| row.node_id == 2).unwrap().clone();
                row.thread_id = 1;
                rows.push(row);
            }
            _ => {
                rows.iter_mut().find(|row| row.node_id == 2).unwrap().counters.rows_returned =
                    if defect == "null" { None } else { Some("-1".into()) }
            }
        }
        let evidence = CurrentExecutionProfileEvidence::from_point(&point).unwrap();
        assert!(evidence.is_valid());
        assert!(input(&evidence).output_so_far.is_none(), "{defect}");
    }
    let mut point = native_point();
    point.context.partition = SourcePartition::Target;
    assert!(CurrentExecutionProfileEvidence::from_point(&point).is_err());
}

#[test]
fn saved_output_pair_cannot_replace_range_values_strict_normalization_or_partial_scope() {
    let original = CurrentExecutionProfileEvidence::from_point(&native_point()).unwrap();
    for defect in ["value", "reason", "strict", "partial"] {
        let mut changed = original.clone();
        let node = changed.nodes.iter_mut().find(|node| node.node_id == 2).unwrap();
        match defect {
            "value" => node.output_so_far.as_mut().unwrap().returned = "511".into(),
            "reason" => {
                node.output_so_far.as_mut().unwrap().normalization_unavailable =
                    ExecutionInputRowsUnavailable::LimitedProfiles
            }
            "strict" => {
                node.input_rows = Some(ExecutionInputRows::SerialScan { returned: "512".into(), estimated: "1".into() })
            }
            _ => changed.profiles_limited = true,
        }
        assert!(!changed.is_valid(), "{defect}");
    }
}
