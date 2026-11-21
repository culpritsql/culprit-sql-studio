//! Desktop discovery capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use crate::requests;
use culprit_core::Result;
use culprit_engine::{ConnectionInfo, DiscoveredDatabase};
use tauri::State;

#[derive(Default)]
pub(crate) struct DiscoveryRequests(requests::WindowRequests);
#[tauri::command]
pub(crate) fn begin_discovery(
    window: tauri::WebviewWindow,
    requests: State<'_, DiscoveryRequests>,
    request_id: String,
) -> Result<()> {
    requests.0.begin(window.label(), request_id)
}
#[tauri::command]
pub(crate) fn cancel_discovery(
    window: tauri::WebviewWindow,
    requests: State<'_, DiscoveryRequests>,
    request_id: String,
) {
    requests.0.cancel(window.label(), &request_id);
}
#[tauri::command]
pub(crate) async fn discover_azure(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    requests: State<'_, DiscoveryRequests>,
    terms: Vec<String>,
    request_id: String,
) -> Result<culprit_engine::DiscoveryResult> {
    requests.0.run(window.label(), &request_id, ws.discover_azure(&terms)).await
}
#[tauri::command]
pub(crate) async fn discover_aws(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    requests: State<'_, DiscoveryRequests>,
    terms: Vec<String>,
    regions: Vec<String>,
    request_id: String,
) -> Result<culprit_engine::DiscoveryResult> {
    requests.0.run(window.label(), &request_id, ws.discover_aws(&regions, &terms)).await
}
#[tauri::command]
pub(crate) async fn add_discovered(ws: Ws<'_>, items: Vec<DiscoveredDatabase>) -> Result<Vec<ConnectionInfo>> {
    blocking_workspace(ws.inner().clone(), move |ws| items.iter().map(|d| ws.add_discovered(d)).collect()).await
}
impl DiscoveryRequests {
    pub(crate) fn close(&self, window: &str) {
        self.0.close(window);
    }
}
