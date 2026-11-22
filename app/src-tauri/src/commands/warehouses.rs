//! Archive capability: typed metadata plus source-free evidence reads, one existing request owner per window.
use super::workspace::{Ws, blocking_workspace};
use crate::requests;
use culprit_core::*;
use culprit_read::cancellation::CollectionCancellation;
use tauri::State;
pub(crate) struct WarehouseRequests(requests::WindowRequests);
impl Default for WarehouseRequests {
    fn default() -> Self {
        Self(requests::WindowRequests::serialized())
    }
}
impl WarehouseRequests {
    pub fn close(&self, window: &str) {
        self.0.close(window);
    }
}
#[tauri::command]
pub(crate) fn begin_warehouse_read(
    window: tauri::WebviewWindow,
    requests: State<'_, WarehouseRequests>,
    request_id: String,
) -> Result<()> {
    requests.0.begin(window.label(), request_id)
}
#[tauri::command]
pub(crate) fn cancel_warehouse_read(
    window: tauri::WebviewWindow,
    requests: State<'_, WarehouseRequests>,
    request_id: String,
) {
    requests.0.cancel(window.label(), &request_id);
}
#[tauri::command]
pub(crate) async fn warehouse_connections(ws: Ws<'_>) -> Result<Vec<WarehouseConnectionInfo>> {
    blocking_workspace(ws.inner().clone(), |ws| ws.warehouse_connections()).await
}
#[tauri::command]
pub(crate) async fn add_warehouse_connection(
    ws: Ws<'_>,
    input: SaveWarehouseConnection,
) -> Result<WarehouseConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.add_warehouse_connection(input)).await
}
#[tauri::command]
pub(crate) async fn remove_warehouse_connection(ws: Ws<'_>, connection: WarehouseConnectionRef) -> Result<()> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.remove_warehouse_connection(&connection)).await
}
#[derive(Default)]
struct ReadCancellation(CollectionCancellation);
impl Drop for ReadCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[tauri::command]
pub(crate) async fn warehouse_resources(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    requests: State<'_, WarehouseRequests>,
    connection: WarehouseConnectionRef,
    request: PublishedResourceRequest,
    request_id: String,
) -> Result<PublishedResources> {
    requests
        .0
        .run(window.label(), &request_id, async {
            let cancellation = ReadCancellation::default();
            ws.warehouse_resources(&connection, &request, &cancellation.0).await
        })
        .await
}
#[tauri::command]
pub(crate) async fn warehouse_incident(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    requests: State<'_, WarehouseRequests>,
    connection: WarehouseConnectionRef,
    resource: PublishedResourceSelector,
    around: String,
    request_id: String,
) -> Result<WarehouseIncidentView> {
    requests
        .0
        .run(window.label(), &request_id, async {
            let cancellation = ReadCancellation::default();
            ws.warehouse_incident(&connection, &resource, &around, &cancellation.0)
                .await
                .map(WarehouseIncidentView::new)
        })
        .await
}
#[cfg(test)]
mod tests;
