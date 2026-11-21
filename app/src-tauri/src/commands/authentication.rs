//! Desktop authentication capability; native work remains with the existing Workspace owner.
use super::workspace::Ws;
use crate::sign_in_page;
use culprit_auth::{DeviceCodePrompt, SignInEvent};
use culprit_core::Result;
use std::sync::Mutex;
use tauri::{AppHandle, State};
use tauri::{Emitter, Manager};

/// The device code being shown, so "Open the sign-in page" opens Microsoft's page and nothing the webview asks for.
#[derive(Default)]
pub(crate) struct SignIn(Mutex<Option<DeviceCodePrompt>>);
#[tauri::command]
pub(crate) fn cancel_sign_in(ws: Ws) {
    ws.cancel_sign_in();
}
#[tauri::command]
pub(crate) fn open_sign_in_page(sign_in: State<SignIn>) -> Result<()> {
    sign_in_page::open_pending(&sign_in.0)
}
pub(crate) fn on_sign_in(handle: &AppHandle, event: SignInEvent) {
    if let Some(state) = handle.try_state::<SignIn>()
        && let Ok(mut current) = state.0.lock()
    {
        *current = match &event {
            SignInEvent::Code { prompt } => Some(prompt.clone()),
            _ => None,
        };
    }
    let _ = handle.emit("entra-sign-in", &event);
}
