//! Desktop editor capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use crate::requests;
use culprit_core::{Result, ResultSet};
use tauri::State;

pub(crate) struct PreviewRequests(requests::WindowRequests);
impl Default for PreviewRequests {
    fn default() -> Self {
        Self(requests::WindowRequests::serialized())
    }
}
#[tauri::command]
pub(crate) async fn run_sql(ws: Ws<'_>, name: String, sql: String, limit: usize, confirmed: bool) -> Result<ResultSet> {
    ws.run(&name, &sql, limit.min(10_000), confirmed).await
}
#[tauri::command]
pub(crate) fn begin_preview(
    window: tauri::WebviewWindow,
    requests: State<'_, PreviewRequests>,
    request_id: String,
) -> Result<()> {
    requests.0.begin(window.label(), request_id)
}
#[tauri::command]
pub(crate) fn cancel_preview(window: tauri::WebviewWindow, requests: State<'_, PreviewRequests>, request_id: String) {
    requests.0.cancel(window.label(), &request_id);
}
#[tauri::command]
pub(crate) async fn preview_table(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    requests: State<'_, PreviewRequests>,
    request_id: String,
    name: String,
    options: culprit_core::TablePreviewOptions,
) -> Result<culprit_core::TablePreview> {
    requests.0.run(window.label(), &request_id, ws.preview_table(&name, &options)).await
}
/// Every check Culprit can run, so the app can say what it looked at and found fine.
#[tauri::command]
pub(crate) fn checks(ws: Ws<'_>) -> Vec<culprit_core::CheckInfo> {
    ws.checks()
}
/// What a statement does, for the question asked before it changes data on production. Pure; no connection.
#[tauri::command]
pub(crate) fn check_statement(engine: culprit_core::Engine, sql: String) -> culprit_core::sql::StatementCheck {
    culprit_core::sql::check(&sql, engine)
}
/// Lint migration or DDL SQL offline against the connection's latest saved snapshot (no database access).
#[tauri::command]
pub(crate) async fn lint_sql(ws: Ws<'_>, name: String, sql: String) -> Result<Vec<culprit_core::Finding>> {
    blocking_workspace(ws.inner().clone(), move |ws| {
        static CACHE: std::sync::Mutex<Option<(String, String, std::sync::Arc<culprit_core::Snapshot>)>> =
            std::sync::Mutex::new(None);
        let latest_id = culprit_engine::reports::latest_summary(&ws.paths().reports_dir(&name), None)
            .map(|summary| summary.id)
            .unwrap_or_default();
        let cached = CACHE.lock().ok().and_then(|guard| {
            guard
                .as_ref()
                .filter(|(cached_name, cached_id, _)| {
                    !latest_id.is_empty() && cached_name == &name && cached_id == &latest_id
                })
                .map(|(_, _, snapshot)| snapshot.clone())
        });
        if let Some(snapshot) = cached {
            return Ok(culprit_engine::lint_sql(&snapshot, &sql));
        }
        let Some(snapshot) = ws.latest_snapshot(&name)? else {
            return Ok(Vec::new());
        };
        let snap = std::sync::Arc::new(snapshot);
        if let Ok(mut guard) = CACHE.lock() {
            *guard = Some((name, snap.id.clone(), std::sync::Arc::clone(&snap)));
        }
        Ok(culprit_engine::lint_sql(&snap, &sql))
    })
    .await
}
#[tauri::command]
pub(crate) async fn catalog(ws: Ws<'_>, name: String) -> Result<culprit_engine::catalog::Catalog> {
    ws.catalog(&name).await
}
#[tauri::command]
pub(crate) async fn saved_queries(ws: Ws<'_>) -> Result<Vec<culprit_engine::saved::SavedQuery>> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.saved_queries()).await
}
#[tauri::command]
pub(crate) async fn save_query(
    ws: Ws<'_>,
    query: culprit_engine::saved::SaveQuery,
) -> Result<culprit_engine::saved::SavedQuery> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.save_query(query)).await
}
#[tauri::command]
pub(crate) async fn delete_saved_query(ws: Ws<'_>, name: String) -> Result<()> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.delete_saved_query(&name)).await
}
#[tauri::command]
pub(crate) async fn saved_query_ran(ws: Ws<'_>, name: String) -> Result<()> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.saved_query_ran(&name)).await
}
impl PreviewRequests {
    pub(crate) fn close(&self, window: &str) {
        self.0.close(window);
    }
}
