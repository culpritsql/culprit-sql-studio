//! Desktop approvals capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use culprit_core::Result;
use culprit_engine::{AgentAccess, ApprovalRequest, ConnectionInfo};
use std::collections::HashSet;
use std::sync::Mutex;
use tauri::State;

#[tauri::command]
pub(crate) async fn set_agent_access(ws: Ws<'_>, name: String, access: AgentAccess) -> Result<ConnectionInfo> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.set_agent_access(&name, access)).await
}
/// Approval requests already announced to the person, so the dock/taskbar only asks for attention once per request.
#[derive(Default)]
pub(crate) struct Announced(Mutex<HashSet<String>>);
/// Statements coding agents are waiting to run. Polled by the window while it is open; that also tells `culprit mcp`
/// someone is here to answer.
#[tauri::command]
pub(crate) async fn pending_approvals(
    window: tauri::Window,
    ws: Ws<'_>,
    announced: State<'_, Announced>,
) -> Result<Vec<ApprovalRequest>> {
    let pending = blocking_workspace(ws.inner().clone(), |ws| ws.pending_approvals()).await?;
    let mut seen = announced.0.lock().unwrap_or_else(|e| e.into_inner());
    if pending.iter().any(|r| !seen.contains(&r.id)) {
        let _ = window.request_user_attention(Some(tauri::UserAttentionType::Informational));
    }
    *seen = pending.iter().map(|r| r.id.clone()).collect();
    Ok(pending)
}
#[tauri::command]
pub(crate) async fn decide_approval(ws: Ws<'_>, id: String, hash: String, approved: bool) -> Result<()> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.decide_approval(&id, &hash, approved)).await
}
