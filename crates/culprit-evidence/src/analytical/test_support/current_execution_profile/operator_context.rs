//! Controlled native points for one shared operator/task context policy, reused by pure/Gold/both-store controls.
use super::*;
#[derive(Clone, Copy, Debug)]
pub enum ParallelOperatorCase {
    IoRead,
    LockOwnerUnknown,
    ExchangeOnly,
    MultipleNodes,
    ChangedWorker,
    LimitedProfiles,
    LimitedTasks,
    NoPlan,
    UnknownCounts,
    NegativeCounts,
    NoPhysicalReads,
    Serial,
}
impl ParallelOperatorCase {
    pub const ALL: [Self; 12] = [
        Self::IoRead,
        Self::LockOwnerUnknown,
        Self::ExchangeOnly,
        Self::MultipleNodes,
        Self::ChangedWorker,
        Self::LimitedProfiles,
        Self::LimitedTasks,
        Self::NoPlan,
        Self::UnknownCounts,
        Self::NegativeCounts,
        Self::NoPhysicalReads,
        Self::Serial,
    ];
}
pub fn parallel_operator_fixture(case: ParallelOperatorCase) -> ExecutionProfileFixture {
    fixture_from_basis(basis_with_exchange_wait(Some("CXCONSUMER")), |focus, execution| {
        parallel_operator_result(focus, execution, case)
    })
}
pub fn parallel_operator_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: ParallelOperatorCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (source, mut records) = current_execution_profile_result(focus, execution, ExecutionProfileCase::Observed);
    let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
    let OperationalFact::MssqlCurrentExecutionProfile(point) = &mut row.fact else { panic!() };
    let after = point.workers_after.as_mut().unwrap();
    after.tasks[0].wait = None;
    let MssqlExecutionProfiles::Observed { rows, operators, limited, .. } = &mut point.profiles else { panic!() };
    rows[1].counters.physical_reads = Some("64".into());
    rows[1].counters.rows_read = Some("1000".into());
    rows[1].counters.rows_returned = Some("10".into());
    match case {
        ParallelOperatorCase::IoRead => (),
        ParallelOperatorCase::LockOwnerUnknown => {
            after.tasks[1].wait.as_mut().unwrap().native_type = "LCK_M_S".into();
            after.tasks[1].wait.as_mut().unwrap().blocking_session_id = Some(99);
        }
        ParallelOperatorCase::ExchangeOnly => {
            after.tasks[1].wait.as_mut().unwrap().native_type = "CXCONSUMER".into();
        }
        ParallelOperatorCase::MultipleNodes => {
            let mut other = rows[1].clone();
            other.node_id = 1;
            other.physical_operator = "Filter".into();
            other.counters.physical_reads = None;
            other.counters.logical_reads = None;
            other.counters.rows_read = None;
            rows.push(other);
            let MssqlExecutionOperators::Observed { nodes, plan_sha256, .. } = operators else { panic!() };
            let mut other = nodes[0].clone();
            other.node_id = 1;
            other.kind = NodeKind::Other;
            other.native_operator = "Filter".into();
            other.object = None;
            nodes[0].parent_node_id = Some(1);
            nodes.push(other);
            let MssqlCurrentRequestPlan::Observed { plan, .. } = &mut point.work.as_mut().unwrap().current_plan else {
                panic!()
            };
            *plan = DefinitionPayload::plan(MULTIPLE_NODE_XML, Engine::Mssql);
            let DefinitionPayload::Available { sha256, .. } = plan else { panic!() };
            *plan_sha256 = sha256.clone();
        }
        ParallelOperatorCase::ChangedWorker => {
            after.tasks[1].worker.as_mut().unwrap().created_ms_ticks = Some("11".into());
        }
        ParallelOperatorCase::LimitedProfiles => *limited = true,
        ParallelOperatorCase::LimitedTasks => after.limited = true,
        ParallelOperatorCase::NoPlan => {
            *operators =
                MssqlExecutionOperators::Unavailable { reason: MssqlExecutionOperatorsUnavailable::PlanUnavailable }
        }
        ParallelOperatorCase::UnknownCounts => rows[1].counters.physical_reads = None,
        ParallelOperatorCase::NegativeCounts => rows[1].counters.physical_reads = Some("-1".into()),
        ParallelOperatorCase::NoPhysicalReads => rows[1].counters.physical_reads = Some("0".into()),
        ParallelOperatorCase::Serial => {
            let MssqlExecutionOperators::Observed { nodes, .. } = operators else { panic!() };
            for node in nodes {
                node.parallel = Some(false);
            }
        }
    }
    row.key = point.key().unwrap();
    assert!(point.fits_source(&source));
    (source, records)
}

// The same SQL has one scan below a filter, and the checked task can serve both. This is not a second execution.
const MULTIPLE_NODE_XML: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT id FROM dbo.items WHERE tenant=124" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="1" PhysicalOp="Filter" LogicalOp="Filter" Parallel="true" EstimateRows="10" EstimatedTotalSubtreeCost="1"><Filter StartupExpression="false"><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="true" EstimateRows="10" EstimatedTotalSubtreeCost="1"><TableScan><Object Schema="[dbo]" Table="[items]"/></TableScan></RelOp></Filter></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
