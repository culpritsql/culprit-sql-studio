//! Desktop files capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use crate::screenshot;
use culprit_core::{Error, Result};
use culprit_engine::ConnectionInfo;
use tauri::AppHandle;

#[tauri::command]
pub(crate) async fn save_file(name: String, contents: String) -> Result<String> {
    culprit_engine::run_blocking(move || {
        culprit_engine::files::save_to_downloads(&name, &contents).map(|path| path.display().to_string())
    })
    .await
}
/// Copy a picture of part of the window to the clipboard as PNG; nothing is written to disk.
#[tauri::command]
pub(crate) async fn screenshot(webview: tauri::Webview, area: screenshot::Area) -> Result<()> {
    screenshot::copy(&webview, area).await
}
/// Choose a SQLite database file in a native dialog, for the new-connection form. Culprit opens the dialog itself, so the
/// window gets back only the path the person picked, as a connection string Culprit reads as SQLite whatever the file is
/// named. `None` when the dialog was cancelled.
#[tauri::command]
pub(crate) async fn choose_database_file(app: AppHandle) -> Result<Option<String>> {
    use tauri_plugin_dialog::DialogExt;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_title("Choose a SQLite database file")
        .add_filter("SQLite databases", &culprit_engine::DATABASE_FILE_EXTENSIONS)
        .pick_file(move |picked| {
            let _ = sender.send(picked);
        });
    let Some(picked) = receiver.await.ok().flatten() else { return Ok(None) };
    let path = picked.into_path().map_err(|e| Error::invalid(format!("That file can't be read from here: {e}")))?;
    Ok(Some(culprit_engine::database_file_input(&path)))
}
#[tauri::command]
pub(crate) async fn choose_ca_certificate(app: AppHandle) -> Result<Option<String>> {
    use tauri_plugin_dialog::DialogExt;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_title(culprit_core::t!("ui.connection_security.choose_ca"))
        .add_filter(culprit_core::t!("ui.connection_security.certificate_files"), &["pem", "crt", "cer"])
        .pick_file(move |picked| {
            let _ = sender.send(picked);
        });
    let Some(picked) = receiver.await.ok().flatten() else { return Ok(None) };
    let path =
        picked.into_path().map_err(|_| Error::invalid(culprit_core::t!("ui.connection_security.unreadable_ca")))?;
    Ok(Some(path.to_string_lossy().into_owned()))
}
#[tauri::command]
pub(crate) async fn choose_ssh_identity(app: AppHandle) -> Result<Option<String>> {
    use tauri_plugin_dialog::DialogExt;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    app.dialog().file().set_title("Choose an OpenSSH identity file").pick_file(move |picked| {
        let _ = sender.send(picked);
    });
    let Some(picked) = receiver.await.ok().flatten() else { return Ok(None) };
    let path =
        picked.into_path().map_err(|_| Error::invalid(culprit_core::message!("ui.ssh_fields.identity_unreadable")))?;
    Ok(Some(path.to_string_lossy().into_owned()))
}
/// Choose the connection's query log in a native file dialog. Culprit opens the dialog itself, so the window never gets
/// file system access: only the file the person picked reaches the workspace, which checks it holds statements before
/// saving it. `None` when the dialog was cancelled.
#[tauri::command]
pub(crate) async fn choose_query_log(app: AppHandle, ws: Ws<'_>, name: String) -> Result<Option<ConnectionInfo>> {
    use tauri_plugin_dialog::DialogExt;
    // Start where the log is, or beside the database file.
    let start = culprit_engine::Workspace::query_log_folder(&ws.connection(&name)?);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut dialog = app
        .dialog()
        .file()
        .set_title("Choose the log your framework writes")
        .add_filter("Logs", &["log", "jsonl", "ndjson", "txt"]);
    if let Some(dir) = start {
        dialog = dialog.set_directory(dir);
    }
    dialog.pick_file(move |picked| {
        let _ = sender.send(picked);
    });
    let Some(picked) = receiver.await.ok().flatten() else { return Ok(None) };
    let path = picked.into_path().map_err(|e| Error::invalid(format!("That file can't be read from here: {e}")))?;
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_query_log(&name, Some(&path)).map(Some)).await
}
