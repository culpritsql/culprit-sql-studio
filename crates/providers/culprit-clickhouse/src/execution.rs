use crate::client::{Client, invalid_response};
use culprit_core::{Engine, Error, ErrorCode, ExplainMode, Plan, Result, ResultSet, limits, message, sql};
use serde::Deserialize;
use serde_json::Value;
use std::time::Instant;

fn read(sql: &str) -> Result<&str> {
    let statement = sql::single_statement(sql, Engine::Clickhouse)?;
    if !sql::reads_only(statement, Engine::Clickhouse) {
        return Err(Error::invalid(message!("provider.clickhouse.readonly")));
    }
    Ok(statement)
}
#[derive(Deserialize)]
struct Column {
    name: String,
}
#[derive(Deserialize)]
struct ResultTable {
    meta: Vec<Column>,
    data: Vec<Vec<Value>>,
}
pub async fn run(client: &Client, statement: &str, requested: usize) -> Result<ResultSet> {
    let started = Instant::now();
    let statement = read(statement)?;
    let limit = requested.clamp(1, limits::MAX_RESULT_ROWS);
    let body =
        client.dispatch(format!("/* culprit */ {statement}\nFORMAT JSONCompact"), vec![], Some(limit + 1)).await?;
    let table: ResultTable = serde_json::from_slice(&body).map_err(|_| invalid_response())?;
    let mut budget = limits::ResultBudget::new(limit);
    let mut rows = Vec::new();
    for row in table.data {
        let bytes = serde_json::to_vec(&row).map_err(|_| invalid_response())?.len();
        if !budget.retain(bytes) {
            break;
        }
        rows.push(row);
    }
    Ok(ResultSet {
        columns: table.meta.into_iter().map(|c| c.name).collect(),
        rows,
        truncated: budget.truncated(),
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        messages: budget.notice().into_iter().collect(),
    })
}
pub async fn explain(client: &Client, statement: &str, mode: ExplainMode) -> Result<Plan> {
    if mode != ExplainMode::Estimated {
        return Err(Error::new(ErrorCode::Unsupported, message!("core.engines.clickhouse.plan_format")));
    }
    let statement = read(statement)?;
    let result = run(client, &format!("EXPLAIN PLAN json=1 {statement}"), 1).await?;
    let raw = result.rows.first().and_then(|r| r.first()).and_then(Value::as_str).ok_or_else(invalid_response)?;
    let mut plan = culprit_plans::clickhouse::parse(raw)?;
    plan.statement = Some(statement.into());
    Ok(plan)
}
