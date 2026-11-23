//! The native picker delegates only the selected XML. Imported labels never select a database or native source.
use crate::requests;
use culprit_core::{
    NativeLogAnalysis, NativeLogFormat, NativeLogLimits, Result,
    limits::{OperationGuard, OperationHandle},
    telemetry::CollectionStop,
};
use culprit_read::cancellation::CollectionCancellation;
use tauri::State;
use tauri_plugin_dialog::DialogExt;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct EventImports(pub(crate) requests::WindowRequests);
impl Default for EventImports {
    fn default() -> Self {
        Self(requests::WindowRequests::serialized())
    }
}

#[tauri::command]
pub(crate) fn begin_native_events(
    window: tauri::WebviewWindow,
    requests: State<'_, EventImports>,
    request_id: String,
) -> Result<()> {
    requests.0.begin(window.label(), request_id)
}
#[tauri::command]
pub(crate) fn cancel_native_events(
    window: tauri::WebviewWindow,
    requests: State<'_, EventImports>,
    request_id: String,
) {
    requests.0.cancel(window.label(), &request_id);
}
#[tauri::command]
pub(crate) async fn open_native_events(
    app: tauri::AppHandle,
    window: tauri::WebviewWindow,
    requests: State<'_, EventImports>,
    request_id: String,
) -> Result<Option<NativeLogAnalysis>> {
    let cancellation = CollectionCancellation::default();
    requests
        .0
        .run_settled(window.label(), &request_id, &cancellation, |permit| async {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            app.dialog()
                .file()
                .set_title(culprit_core::t!("native_events.import.title"))
                .add_filter(culprit_core::t!("native_events.import.filter"), &["xml"])
                .pick_file(move |picked| {
                    let _ = sender.send(picked);
                });
            let picked = tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
                picked = receiver => picked.map_err(|_| unreadable())?,
            };
            let Some(picked) = picked else { return Ok(None) };
            let path = picked.into_path().map_err(|_| unreadable())?;
            settle_import(permit, &cancellation, move |operation| {
                culprit_engine::native_logs::analyze_file_with(
                    &path,
                    NativeLogFormat::MssqlExtendedEventsXml,
                    NativeLogLimits::default(),
                    operation,
                )
            })
            .await
            .map(Some)
        })
        .await
}

/// The SAME request slot follows admitted CPU/file work through settlement, including caller drop.
async fn settle_import(
    permit: Option<OwnedSemaphorePermit>,
    cancellation: &CollectionCancellation,
    read: impl FnOnce(&OperationHandle) -> Result<NativeLogAnalysis> + Send + 'static,
) -> Result<NativeLogAnalysis> {
    let guard = OperationGuard::new(culprit_engine::native_logs::LOG_IMPORT_TIMEOUT)?;
    let operation = guard.handle();
    let language = culprit_core::i18n::current();
    let work = culprit_engine::run_blocking(move || {
        let _permit = permit;
        operation.check()?;
        culprit_core::i18n::scope(&language.code, || read(&operation))
    });
    tokio::pin!(work);
    tokio::select! { biased;
        _ = cancellation.cancelled() => {
            drop(guard);
            let _ = work.await;
            Err(CollectionStop::Cancelled.error())
        },
        result = &mut work => result,
    }
}
fn unreadable() -> culprit_core::Error {
    culprit_core::Error::invalid(culprit_core::t!("native_events.import.unreadable"))
}

#[cfg(test)]
mod tests;
