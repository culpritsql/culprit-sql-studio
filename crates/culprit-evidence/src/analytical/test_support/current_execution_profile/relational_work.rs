//! A single consistent Activity→Work→selected-profile corpus; only canonical readers produce authenticated handles.
use super::*;
use culprit_core::plan_facts::{RelationalInput, RelationalInputKind, RelationalInputProvenance};
const SQL: &str = "SELECT r.id FROM @Rows r";
const HEADER: &str = "CREATE PROC dbo.consume @Rows dbo.RowsType READONLY AS ";
const DECLARE: &str = "DECLARE @Rows TABLE(id int); ";
#[derive(Clone, Copy, Debug)]
pub enum ExecutionRelationalCase {
    TableValuedParameter,
    LocalTableVariable,
    Healthy,
    UnknownKind,
    RepeatedScan,
    Parallel,
    MissingCounts,
    Limited,
    Spill,
    Ended,
    ReusedPlan,
    MissingBody,
    Lightweight,
    LightweightRepeated,
    LightweightUnknownKind,
    LightweightLimited,
}
impl ExecutionRelationalCase {
    pub const ALL: [Self; 16] = [
        Self::TableValuedParameter,
        Self::LocalTableVariable,
        Self::Healthy,
        Self::UnknownKind,
        Self::RepeatedScan,
        Self::Parallel,
        Self::MissingCounts,
        Self::Limited,
        Self::Spill,
        Self::Ended,
        Self::ReusedPlan,
        Self::MissingBody,
        Self::Lightweight,
        Self::LightweightRepeated,
        Self::LightweightUnknownKind,
        Self::LightweightLimited,
    ];
    fn prefix(self) -> &'static str {
        match self {
            Self::LocalTableVariable => DECLARE,
            Self::UnknownKind | Self::LightweightUnknownKind => "",
            _ => HEADER,
        }
    }
}
pub fn current_execution_profile_relational_fixture(case: ExecutionRelationalCase) -> ExecutionProfileFixture {
    let (source, mut rows) = basis_with_exchange_wait(None);
    let TelemetryRecord::Activity(original) = &mut rows[0] else { panic!() };
    original.statement = DefinitionPayload::sql(SQL, Engine::Mssql);
    original.statement_identity = Some(
        mssql_request_statement_identity("AABB", (case.prefix().encode_utf16().count() * 2).try_into().unwrap(), -1, 7)
            .unwrap(),
    );
    fixture_from_basis((source, rows), |focus, execution| {
        current_execution_profile_relational_result(focus, execution, case)
    })
}
pub fn current_execution_profile_relational_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: ExecutionRelationalCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let original = if matches!(case, ExecutionRelationalCase::Ended) {
        ExecutionProfileCase::ChangedRequest
    } else {
        ExecutionProfileCase::Observed
    };
    let (source, mut rows) = current_execution_profile_result(focus, execution, original);
    if matches!(case, ExecutionRelationalCase::Ended) {
        return (source, rows);
    }
    let TelemetryRecord::Operational(row) = &mut rows[0] else { panic!() };
    let OperationalFact::MssqlCurrentExecutionProfile(point) = &mut row.fact else { panic!() };
    assert_eq!(point.statement, DefinitionPayload::sql(SQL, Engine::Mssql));
    let local = matches!(case, ExecutionRelationalCase::LocalTableVariable);
    let unknown =
        matches!(case, ExecutionRelationalCase::UnknownKind | ExecutionRelationalCase::LightweightUnknownKind);
    let lightweight = matches!(
        case,
        ExecutionRelationalCase::Lightweight
            | ExecutionRelationalCase::LightweightRepeated
            | ExecutionRelationalCase::LightweightUnknownKind
            | ExecutionRelationalCase::LightweightLimited
    );
    let parallel = matches!(case, ExecutionRelationalCase::Parallel);
    let declaration = if local { r#"<StmtSimple StatementText="DECLARE @Rows TABLE(id int)"/>"# } else { "" };
    let xml = format!(
        r#"<ShowPlanXML><BatchSequence><Batch><Statements>{declaration}<StmtSimple StatementText="SELECT r.id FROM @Rows r" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="{parallel}" EstimateRows="1" EstimatedTotalSubtreeCost="1"><TableScan><Object Table="[@Rows]" Alias="[r]"/></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
    );
    let work = point.work.as_mut().unwrap();
    let MssqlCurrentRequestPlan::Observed { context, plan, .. } = &mut work.current_plan else { panic!() };
    context.source_text_bytes =
        Some(((case.prefix().encode_utf16().count() + SQL.encode_utf16().count()) * 2).to_string());
    context.source_text_complete = true;
    *plan = DefinitionPayload::plan(&xml, Engine::Mssql);
    let DefinitionPayload::Available { sha256, .. } = plan else { panic!() };
    let input = RelationalInput {
        kind: if unknown {
            RelationalInputKind::Unknown
        } else if local {
            RelationalInputKind::LocalTableVariable
        } else {
            RelationalInputKind::TableValuedParameter
        },
        provenance: if unknown {
            RelationalInputProvenance::Unresolved
        } else if local {
            RelationalInputProvenance::NativeDeclaration
        } else {
            RelationalInputProvenance::NativeParameterMetadata
        },
    };
    let MssqlExecutionProfiles::Observed { rows: profiles, operators, limited, .. } = &mut point.profiles else {
        panic!()
    };
    profiles.truncate(1);
    let profile = &mut profiles[0];
    profile.counters.rows_returned = Some(
        if lightweight {
            "512"
        } else if matches!(case, ExecutionRelationalCase::Healthy) {
            "0"
        } else {
            "10000"
        }
        .into(),
    );
    profile.counters.rows_read = profile.counters.rows_returned.clone();
    profile.counters.estimated_rows = Some("1".into());
    profile.counters.end_of_scans = Some("1".into());
    profile.counters.scans = Some(if matches!(case, ExecutionRelationalCase::RepeatedScan) { "2" } else { "1" }.into());
    profile.counters.rebinds = Some("0".into());
    profile.counters.rewinds = Some("0".into());
    profile.counters.spill_write_pages =
        Some(if matches!(case, ExecutionRelationalCase::Spill) { "16" } else { "0" }.into());
    if matches!(case, ExecutionRelationalCase::MissingCounts) {
        profile.counters.estimated_rows = None;
    }
    profile.open_ticks = Some("100".into());
    profile.close_ticks = Some("120".into());
    if lightweight {
        profile.counters.scans = None;
        profile.counters.rebinds = Some("1".into());
        profile.counters.rows_read = None;
        profile.open_ticks = Some("0".into());
        profile.close_ticks = Some("0".into());
        if matches!(case, ExecutionRelationalCase::LightweightRepeated) {
            profile.counters.scans = Some("2".into());
            profile.counters.rebinds = Some("2".into());
        }
    }
    *limited = matches!(case, ExecutionRelationalCase::Limited | ExecutionRelationalCase::LightweightLimited);
    *operators = MssqlExecutionOperators::Observed {
        context: context.clone(),
        plan_sha256: sha256.clone(),
        nodes: vec![MssqlExecutionOperator {
            node_id: 0,
            parent_node_id: None,
            kind: NodeKind::Scan,
            parallel: Some(parallel),
            inner_join: None,
            hash_build_input: None,
            relational_input: Some(input),
            native_operator: "Table Scan".into(),
            object: Some("@Rows".into()),
            index: None,
            predicate: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            seek_predicate: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        }],
    };
    let mut capsule = MssqlExecutionRelationalInputs {
        context: context.clone(),
        module_parameters: (!local && !unknown).then(|| MssqlModuleParameters {
            database_id: 7,
            object_id: 1001,
            module_number: 1,
            sql_handle: "0xAABB".into(),
            table_parameters: [("@Rows".into(), vec!["dbo".into(), "RowsType".into()])].into(),
        }),
        statement_utf16_bytes: (SQL.encode_utf16().count() * 2).to_string(),
        statement_native_sha256: digest(SQL.as_bytes()),
        plan_utf16_bytes: (xml.encode_utf16().count() * 2).to_string(),
        plan_native_sha256: digest(xml.as_bytes()),
        proof: None,
    };
    if !unknown {
        capsule.proof = Some(Box::new(
            MssqlRelationalInputProof::for_execution(
                point,
                &capsule,
                vec![MssqlRelationalInputBinding { node_id: 0, input }],
            )
            .unwrap(),
        ));
    }
    point.relational_inputs = Some(Box::new(capsule));
    if matches!(case, ExecutionRelationalCase::ReusedPlan | ExecutionRelationalCase::MissingBody) {
        point.relational_inputs = None;
        let MssqlExecutionProfiles::Observed { operators, .. } = &mut point.profiles else { panic!() };
        *operators =
            MssqlExecutionOperators::Unavailable { reason: MssqlExecutionOperatorsUnavailable::PlanUnavailable };
        let work = point.work.as_mut().unwrap();
        if matches!(case, ExecutionRelationalCase::ReusedPlan) {
            work.current_plan =
                MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::ChangedDuringRead };
        } else {
            point.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized };
            let MssqlCurrentRequestPlan::Observed { plan, .. } = &mut work.current_plan else { panic!() };
            *plan = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized };
        }
    }
    row.key = point.key().unwrap();
    assert!(point.fits_source(&source));
    (source, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn consistent_originals_preserve_serial_kind_runtime_counts_and_every_explicit_missing_boundary() {
        for case in ExecutionRelationalCase::ALL {
            let fixture = current_execution_profile_relational_fixture(case);
            assert!(fixture.episode.is_valid());
            assert!(fixture.answer.is_valid());
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlCurrentExecutionProfile(point),
                ..
            }) = &fixture.row.record
            else {
                panic!()
            };
            assert!(point.fits_source(&fixture.source));
            assert_eq!(CurrentExecutionProfileEvidence::from_point(point).unwrap(), fixture.answer.evidence);
            let read: ExportedRecord = serde_json::from_slice(&serde_json::to_vec(&fixture.row).unwrap()).unwrap();
            assert_eq!(read, fixture.row);
            if matches!(
                case,
                ExecutionRelationalCase::Ended
                    | ExecutionRelationalCase::ReusedPlan
                    | ExecutionRelationalCase::MissingBody
            ) {
                assert!(!fixture.answer.evidence.has_relational_input_context());
                assert!(fixture.answer.evidence.nodes.iter().all(|node| node.input_rows.is_none()));
            } else {
                let node = &fixture.answer.evidence.nodes[0];
                let input = node.operator.as_ref().unwrap().relational_input.unwrap();
                if matches!(
                    case,
                    ExecutionRelationalCase::UnknownKind | ExecutionRelationalCase::LightweightUnknownKind
                ) {
                    assert_eq!(input.qualified_kind(), RelationalInputKind::Unknown);
                    assert!(point.relational_inputs.as_ref().unwrap().proof.is_none());
                } else {
                    let inputs = point.relational_inputs.as_ref().unwrap();
                    assert!(inputs.proof.as_ref().unwrap().matches_execution(point, inputs));
                }
            }
        }
    }
}
