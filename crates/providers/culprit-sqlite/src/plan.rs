//! Native EXPLAIN ownership remains in the provider; supplied rows use the shared source-free parser.
use crate::map_error;
use culprit_core::{Plan, Result, t};
use culprit_plans::sqlite::plan_from_rows;
use rusqlite::Connection;

/// The plan of a statement, never run. A statement with placeholders (`?`, as logs and fingerprints write it) is planned
/// with NULL in each: EXPLAIN QUERY PLAN still runs nothing, and the plan says it had no values.
pub fn explain(conn: &Connection, statement: &str) -> Result<Plan> {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {statement}")).map_err(|e| map_error(&e))?;
    let parameters = stmt.parameter_count();
    let nulls = std::iter::repeat_n(rusqlite::types::Null, parameters);
    let rows: Vec<(i64, i64, String)> = stmt
        .query_map(rusqlite::params_from_iter(nulls), |r| Ok((r.get(0)?, r.get(1)?, r.get(3)?)))
        .map_err(|e| map_error(&e))?
        .collect::<rusqlite::Result<_>>()
        .map_err(|e| map_error(&e))?;
    let mut plan = plan_from_rows(statement, &rows)?;
    if parameters > 0 {
        let reason = t!("provider.sqlite.plan.without_values");
        plan.facts_mut().generic_plan_reason = Some(reason.clone());
        plan.props.insert("Generic Plan".into(), reason);
    }
    Ok(plan)
}
