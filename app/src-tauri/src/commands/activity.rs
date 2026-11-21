//! Desktop activity capability; native work remains with the existing Workspace owner.
use super::reports::requested_window;
use super::workspace::{Ws, blocking_workspace};
use culprit_core::{LiveState, Result, SessionActionOutcome, SessionActionRequest};

#[tauri::command]
pub(crate) async fn live(ws: Ws<'_>, name: String) -> Result<LiveState> {
    ws.live(&name).await
}
#[tauri::command]
pub(crate) async fn dashboard_live(
    ws: Ws<'_>,
    name: String,
    dashboard: culprit_core::Dashboard,
) -> Result<culprit_core::DashboardLive> {
    ws.dashboard_live(&name, dashboard).await
}
/// Where the latest scan's database time went, by wait category with the queries behind each.
#[tauri::command]
pub(crate) async fn waits(
    ws: Ws<'_>,
    name: String,
    window_minutes: Option<u32>,
) -> Result<Option<culprit_core::WaitsReport>> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.waits(&name, requested_window(window_minutes))).await
}
/// What the server waits on right now, sampled for `seconds`.
#[tauri::command]
pub(crate) async fn wait_sample(ws: Ws<'_>, name: String, seconds: u32) -> Result<culprit_core::WaitSample> {
    ws.wait_sample(&name, seconds).await
}
#[tauri::command]
pub(crate) async fn interrupt_session(ws: Ws<'_>, request: SessionActionRequest) -> Result<SessionActionOutcome> {
    ws.interrupt_session(&request).await
}
/// Recent deadlocks on a connection, explained (pattern, statements, fixes).
#[tauri::command]
pub(crate) async fn deadlocks(ws: Ws<'_>, name: String, since_hours: f64) -> Result<culprit_core::DeadlockReport> {
    ws.deadlocks(&name, since_hours).await
}
/// Deadlocks in pasted or dropped text: SQL Server .xdl XML, InnoDB status, Postgres log lines. No connection needed.
#[tauri::command]
pub(crate) async fn parse_deadlock(text: String) -> Result<Vec<culprit_core::Deadlock>> {
    culprit_engine::run_blocking(move || culprit_engine::deadlocks::parse_deadlock(&text)).await
}
