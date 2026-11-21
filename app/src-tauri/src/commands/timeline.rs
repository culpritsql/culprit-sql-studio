//! Desktop timeline capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use culprit_core::Result;

/// What changed between two periods on a connection (Query Store intervals, or Culprit's own scans).
#[tauri::command]
pub(crate) async fn changes(
    ws: Ws<'_>,
    name: String,
    request: culprit_engine::changes::ChangeRequest,
) -> Result<culprit_core::Changes> {
    ws.changes(&name, &request).await
}
/// What changed around a connection over a period: marks, schema, settings, restarts and cloud throttling.
#[tauri::command]
pub(crate) async fn events(
    ws: Ws<'_>,
    name: String,
    request: culprit_engine::EventsRequest,
) -> Result<culprit_engine::EventLog> {
    ws.events(&name, &request).await
}
/// What the cloud measured for a connection's server and when it held the server back (through `aws` / `az`).
#[tauri::command]
pub(crate) async fn cloud(
    ws: Ws<'_>,
    name: String,
    request: culprit_engine::EventsRequest,
) -> Result<culprit_core::CloudSignals> {
    ws.cloud(&name, &request).await
}
/// Put a moment (a deploy, a migration) on a connection's timeline.
#[tauri::command]
pub(crate) async fn mark(ws: Ws<'_>, name: String, text: String, at: Option<String>) -> Result<culprit_engine::Mark> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.mark(&name, &text, at.as_deref(), "app")).await
}
#[tauri::command]
pub(crate) async fn unmark(ws: Ws<'_>, name: String, id: String) -> Result<culprit_engine::Mark> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.unmark(&name, &id)).await
}
