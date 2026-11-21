//! Desktop plans capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use culprit_core::{ExplainMode, Plan, PlanDiff, Result};
use culprit_engine::export::{ExportFile, ExportFormat, ExportOptions};

#[tauri::command]
pub(crate) async fn explain(ws: Ws<'_>, name: String, sql: String, mode: ExplainMode, confirmed: bool) -> Result<Plan> {
    ws.explain(&name, &sql, mode, confirmed).await
}
#[tauri::command]
pub(crate) async fn query_plan(
    ws: Ws<'_>,
    name: String,
    query_id: String,
    plan_id: Option<String>,
    report_id: Option<String>,
) -> Result<Plan> {
    let mut plan = ws.query_plan_at(&name, &query_id, plan_id.as_deref(), report_id.as_deref()).await?;
    plan.raw = None;
    Ok(plan)
}
/// The actual plan of a query's last run as the engine kept it; never runs the query.
#[tauri::command]
pub(crate) async fn last_actual_plan(
    ws: Ws<'_>,
    name: String,
    query_id: String,
    plan_id: Option<String>,
    report_id: Option<String>,
) -> Result<Plan> {
    let mut plan = ws.last_actual_plan_at(&name, &query_id, plan_id.as_deref(), report_id.as_deref()).await?;
    plan.raw = None;
    Ok(plan)
}
/// How far along a session's running statement is (polled about once a second while visible).
#[tauri::command]
pub(crate) async fn query_progress(
    ws: Ws<'_>,
    name: String,
    session: String,
    with_plan: bool,
) -> Result<culprit_core::QueryProgress> {
    ws.query_progress(&name, &session, with_plan).await
}
/// A plan pasted or opened in the Plans view (.sqlplan, EXPLAIN JSON or text, MySQL JSON or tree).
#[tauri::command]
pub(crate) async fn parse_plan(text: String) -> Result<Plan> {
    culprit_engine::run_blocking(move || culprit_engine::plans::parse_plan(&text)).await
}
/// `connection`, when the plans came from one, lets scenarios name stale statistics from its latest scan.
#[tauri::command]
pub(crate) async fn diff_plans(
    ws: Ws<'_>,
    a: Plan,
    b: Plan,
    name_a: String,
    name_b: String,
    connection: Option<String>,
) -> Result<PlanDiff> {
    blocking_workspace(ws.inner().clone(), move |ws| Ok(ws.diff_plans(&a, &name_a, &b, &name_b, connection.as_deref())))
        .await
}
#[tauri::command]
pub(crate) async fn export_plan(
    plan: Plan,
    title: String,
    format: ExportFormat,
    options: ExportOptions,
) -> Result<ExportFile> {
    culprit_engine::run_blocking(move || culprit_engine::export_plan(&plan, &title, format, &options)).await
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportDiffRequest {
    a: Plan,
    b: Plan,
    name_a: String,
    name_b: String,
    format: ExportFormat,
    options: ExportOptions,
    connection: Option<String>,
}
#[tauri::command]
pub(crate) async fn export_diff(ws: Ws<'_>, request: ExportDiffRequest) -> Result<ExportFile> {
    let ExportDiffRequest { a, b, name_a, name_b, format, options, connection } = request;
    blocking_workspace(ws.inner().clone(), move |ws| {
        let diff = ws.diff_plans(&a, &name_a, &b, &name_b, connection.as_deref());
        culprit_engine::export_diff(&a, &b, &diff, &name_a, &name_b, format, &options)
    })
    .await
}
#[tauri::command]
pub(crate) async fn export_query_plan(
    ws: Ws<'_>,
    name: String,
    report_id: String,
    query_id: String,
    plan_id: Option<String>,
    format: ExportFormat,
    options: ExportOptions,
) -> Result<ExportFile> {
    ws.export_query_plan_at(&name, &report_id, &query_id, plan_id.as_deref(), format, &options).await
}
