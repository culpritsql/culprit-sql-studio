//! ClickHouse EXPLAIN PLAN JSON records a physical step tree. It supplies no actual run or portable cost.
use culprit_core::{Engine, Error, NodeKind, Plan, PlanKind, PlanNode, Result, message};
use serde_json::Value;

pub(crate) fn recognized(value: &Value) -> bool {
    let Some(root) = value.pointer("/0/Plan") else {
        return false;
    };
    root.get("Node Id").is_some()
        || root
            .get("Node Type")
            .and_then(Value::as_str)
            .is_some_and(|op| op.starts_with("ReadFrom") || matches!(op, "Expression" | "Aggregating" | "Sorting"))
}
pub fn parse(raw: &str) -> Result<Plan> {
    let invalid = || Error::invalid(message!("provider.clickhouse.plan"));
    let value: Value = serde_json::from_str(raw).map_err(|_| invalid())?;
    if !recognized(&value) || value.as_array().is_none_or(|plans| plans.len() != 1) {
        return Err(invalid());
    }
    let root = node(&value[0]["Plan"], 0, &mut 0)?;
    Ok(Plan {
        engine: Engine::Clickhouse,
        kind: PlanKind::Estimated,
        statement: None,
        total_cost: 0.0,
        root,
        warnings: vec![],
        index_hints: vec![],
        props: Default::default(),
        facts: culprit_core::PlanFacts { estimated_cost_unavailable: true, ..Default::default() },
        raw: Some(raw.into()),
        insights: None,
        view: None,
    })
}
fn node(value: &Value, depth: usize, count: &mut u32) -> Result<PlanNode> {
    if depth >= culprit_core::MAX_PLAN_DEPTH || *count >= 4096 {
        return Err(culprit_core::plan::too_deep());
    }
    let invalid = || Error::invalid(message!("provider.clickhouse.plan"));
    let op = value.get("Node Type").and_then(Value::as_str).filter(|v| !v.is_empty()).ok_or_else(invalid)?;
    *count += 1;
    let id = *count;
    let kind = match op {
        "Aggregating" | "MergingAggregated" => NodeKind::Aggregate,
        "Sorting" | "MergeSorting" => NodeKind::Sort,
        "Join" | "FilledJoin" => NodeKind::Join,
        "Filter" | "Expression" => NodeKind::Compute,
        s if s.starts_with("ReadFrom") => NodeKind::Scan,
        _ => NodeKind::Other,
    };
    let children = match value.get("Plans") {
        None => vec![],
        Some(rows) => {
            rows.as_array().ok_or_else(invalid)?.iter().map(|row| node(row, depth + 1, count)).collect::<Result<_>>()?
        }
    };
    Ok(PlanNode {
        id,
        op: op.into(),
        kind,
        detail: value.get("Description").and_then(Value::as_str).map(str::to_owned),
        object: None,
        index: None,
        seek_predicate: None,
        predicate: None,
        est_rows: 0.0,
        est_executions: 1.0,
        act_rows: None,
        act_executions: None,
        rows_read: None,
        table_rows: None,
        subtree_cost: 0.0,
        self_cost: 0.0,
        act_time_ms: None,
        runtime: None,
        warnings: vec![],
        props: Default::default(),
        facts: Default::default(),
        children,
    })
}
