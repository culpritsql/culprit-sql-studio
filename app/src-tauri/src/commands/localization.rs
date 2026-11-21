//! Desktop localization capability; native work remains with the existing Workspace owner.

/// Word what the app reads from now on (findings, notes, plan insights) in `code`, the language the app is shown in.
/// Whether Culprit has that language; English stays otherwise.
#[tauri::command]
pub(crate) fn set_language(code: String) -> bool {
    culprit_core::i18n::set_language(&code)
}
