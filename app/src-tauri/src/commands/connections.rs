//! Desktop connections capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use crate::requests;
use culprit_core::Result;
use culprit_engine::SshConfig;
use culprit_engine::{
    ConnectionInfo, ConnectionSettings, ImportCandidate, ImportSource, NewConnection, ParsedConnection,
};
use tauri::State;

pub(crate) struct ConnectionTests(requests::WindowRequests);
impl Default for ConnectionTests {
    fn default() -> Self {
        Self(requests::WindowRequests::serialized())
    }
}
#[tauri::command]
pub(crate) async fn connections(ws: Ws<'_>) -> Result<Vec<ConnectionInfo>> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.connections()).await
}
#[tauri::command]
pub(crate) fn parse_connection(
    input: String,
    engine: Option<culprit_core::Engine>,
    ssh: Option<bool>,
) -> Result<ParsedConnection> {
    if ssh.unwrap_or(false) {
        culprit_engine::parse_connection_with_ssh(&input, engine)
    } else {
        culprit_engine::parse_connection(&input, engine)
    }
}
#[tauri::command]
pub(crate) async fn test_connection(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    tests: State<'_, ConnectionTests>,
    input: NewConnection,
    request_id: String,
) -> Result<culprit_core::ObservedProbeAttempt> {
    let observation = ws.begin_provider_read(culprit_core::ProviderReadKind::Probe)?;
    let operation = observation.operation()?;
    let result = tests.0.run(window.label(), &request_id, ws.test_connection(&input, operation)).await;
    Ok(culprit_engine::project_probe_attempt(observation.finish_call(result)))
}
#[tauri::command]
pub(crate) fn begin_connection_test(
    window: tauri::WebviewWindow,
    tests: State<'_, ConnectionTests>,
    request_id: String,
) -> Result<()> {
    tests.0.begin(window.label(), request_id)
}
#[tauri::command]
pub(crate) fn cancel_connection_test(
    window: tauri::WebviewWindow,
    tests: State<'_, ConnectionTests>,
    request_id: String,
) {
    tests.0.cancel(window.label(), &request_id);
}
#[tauri::command]
pub(crate) async fn test_ssh(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    tests: State<'_, ConnectionTests>,
    name: String,
    ssh: Option<SshConfig>,
    request_id: String,
) -> Result<culprit_core::ObservedProbeAttempt> {
    let observation = ws.begin_provider_read(culprit_core::ProviderReadKind::Probe)?;
    let operation = observation.operation()?;
    let result = tests.0.run(window.label(), &request_id, ws.test_ssh(&name, ssh, operation)).await;
    Ok(culprit_engine::project_probe_attempt(observation.finish_call(result)))
}
#[tauri::command]
pub(crate) async fn set_ssh(ws: Ws<'_>, name: String, ssh: Option<SshConfig>) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_ssh(&name, ssh)).await
}
#[tauri::command]
pub(crate) async fn add_connection(ws: Ws<'_>, input: NewConnection) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.add_connection(input)).await
}
#[tauri::command]
pub(crate) async fn remove_connection(ws: Ws<'_>, name: String) -> Result<()> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.remove_connection(&name)).await
}
#[tauri::command]
pub(crate) async fn set_connection_settings(
    ws: Ws<'_>,
    name: String,
    settings: ConnectionSettings,
) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_connection_settings(&name, &settings)).await
}
#[tauri::command]
pub(crate) async fn test_connection_settings(
    ws: Ws<'_>,
    window: tauri::WebviewWindow,
    tests: State<'_, ConnectionTests>,
    name: String,
    settings: ConnectionSettings,
    request_id: String,
) -> Result<culprit_core::ObservedProbeAttempt> {
    let observation = ws.begin_provider_read(culprit_core::ProviderReadKind::Probe)?;
    let operation = observation.operation()?;
    let result =
        tests.0.run(window.label(), &request_id, ws.test_connection_settings(&name, &settings, operation)).await;
    Ok(culprit_engine::project_probe_attempt(observation.finish_call(result)))
}
#[tauri::command]
pub(crate) async fn rename_connection(ws: Ws<'_>, old: String, new: String) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.rename_connection(&old, &new)).await
}
#[tauri::command]
pub(crate) async fn set_password(ws: Ws<'_>, name: String, password: String) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_password(&name, &password)).await
}
#[tauri::command]
pub(crate) async fn import_candidates(ws: Ws<'_>, source: Option<ImportSource>) -> Result<Vec<ImportCandidate>> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.import_candidates(source)).await
}
#[tauri::command]
pub(crate) async fn import_connections(ws: Ws<'_>, items: Vec<ImportCandidate>) -> Result<Vec<ConnectionInfo>> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.import_connections(&items)).await
}
#[tauri::command]
pub(crate) async fn set_query_log(ws: Ws<'_>, name: String, path: Option<String>) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_query_log(&name, path.as_deref().map(std::path::Path::new)))
        .await
}
impl ConnectionTests {
    pub(crate) fn close(&self, window: &str) {
        self.0.close(window);
    }
}
