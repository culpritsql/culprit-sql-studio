//! Desktop updates capability; native work remains with the existing Workspace owner.
use culprit_core::{Error, ErrorCode, Result};
use std::sync::Mutex;
use std::time::Duration;
use tauri::Emitter;
use tauri::{AppHandle, State};
use tauri_plugin_updater::Update;
use tauri_plugin_updater::UpdaterExt;

/// A newer release the last check found, kept for the install.
#[derive(Default)]
pub(crate) struct PendingUpdate(Mutex<Option<Update>>);
/// A newer release than this build: its version, and its notes when it has some.
#[derive(serde::Serialize)]
pub(crate) struct UpdateInfo {
    version: String,
    current: String,
    notes: Option<String>,
}
#[derive(Clone, serde::Serialize)]
pub(crate) struct UpdateProgress {
    downloaded: u64,
    total: Option<u64>,
}
/// Updates come from the latest published release's `latest.json` (tauri.conf.json), or from
/// CULPRIT_UPDATE_ENDPOINT for a mirror or a test. Whatever serves it, an update installs only when its signature
/// matches the public key built into the app.
fn updater(app: &AppHandle) -> Result<tauri_plugin_updater::Updater> {
    let failed = |e: tauri_plugin_updater::Error| {
        Error::new(
            ErrorCode::ConnectionFailed,
            culprit_core::message!("ui.updates.check_failed", error = e.to_string()),
        )
    };
    let mut builder = app.updater_builder().timeout(Duration::from_secs(30));
    if let Ok(endpoint) = std::env::var("CULPRIT_UPDATE_ENDPOINT") {
        let url =
            endpoint.parse().map_err(|_| Error::invalid(culprit_core::message!("ui.updates.endpoint_invalid")))?;
        builder = builder.endpoints(vec![url]).map_err(failed)?;
    }
    builder.build().map_err(failed)
}
/// Whether a newer release is out. Only ever asked by the app itself, at most once a day, or when the person checks.
#[tauri::command]
pub(crate) async fn check_update(app: AppHandle, pending: State<'_, PendingUpdate>) -> Result<Option<UpdateInfo>> {
    let update = updater(&app)?.check().await.map_err(|e| {
        Error::new(
            ErrorCode::ConnectionFailed,
            culprit_core::message!("ui.updates.check_failed", error = e.to_string()),
        )
    })?;
    let info = update.as_ref().map(|u| UpdateInfo {
        version: u.version.clone(),
        current: u.current_version.clone(),
        notes: u.body.clone().filter(|b| !b.trim().is_empty()),
    });
    *pending.0.lock().unwrap_or_else(|e| e.into_inner()) = update;
    Ok(info)
}
/// Downloads the update the last check found, verifies its signature and installs it, reporting progress as
/// `update-progress`. The app keeps running the old version until `restart_app`, when the person chooses.
#[tauri::command]
pub(crate) async fn install_update(app: AppHandle, pending: State<'_, PendingUpdate>) -> Result<()> {
    let update = pending
        .0
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .ok_or_else(|| Error::not_found(culprit_core::message!("ui.updates.no_pending")))?;
    let mut downloaded = 0u64;
    let mut reported = 0u64;
    update
        .download_and_install(
            |chunk, total| {
                downloaded += chunk as u64;
                // About a hundred events for the whole download, not one per chunk.
                let step = total.map_or(256 * 1024, |t| (t / 100).max(1));
                if downloaded - reported >= step || total == Some(downloaded) {
                    reported = downloaded;
                    let _ = app.emit("update-progress", UpdateProgress { downloaded, total });
                }
            },
            || {},
        )
        .await
        .map_err(|e| {
            Error::new(ErrorCode::Internal, culprit_core::message!("ui.updates.install_failed", error = e.to_string()))
        })?;
    Ok(())
}
#[tauri::command]
pub(crate) fn restart_app(app: AppHandle) {
    app.restart();
}
