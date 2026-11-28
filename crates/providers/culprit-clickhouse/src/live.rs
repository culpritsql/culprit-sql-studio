use crate::{
    client::Client,
    identity::{Identity, instant},
    queries,
};
use culprit_core::{LiveState, Result, Session, SessionStatementContext, t};
use serde::Deserialize;
#[derive(Deserialize)]
struct Process {
    node: String,
    query_id: String,
    user: String,
    current_database: String,
    elapsed: f64,
    read_rows: u64,
    sql_text: String,
}
pub(crate) async fn read(client: &Client) -> Result<LiveState> {
    let client = client.session()?;
    let owner = Identity::read(&client).await?;
    let rows: Vec<Process> =
        client.rows(queries::LIVE, vec![("row_limit", "201".into()), ("text_bytes", "4096".into())]).await?;
    if rows.iter().any(|r| r.node != owner.node || r.current_database != owner.database) {
        return Err(crate::client::invalid_response());
    }
    let mut notes = vec![t!("provider.clickhouse.live")];
    if rows.len() > 200 {
        notes.push(t!("provider.clickhouse.truncated"));
    }
    let sessions = rows
        .into_iter()
        .take(200)
        .map(|r| Session {
            id: r.query_id,
            user: Some(r.user),
            database: Some(r.current_database),
            sql: Some(r.sql_text),
            sql_context: Some(SessionStatementContext::CurrentRequest),
            elapsed_ms: Some(r.elapsed * 1000.0),
            reads: Some(r.read_rows as f64),
            state: "running".into(),
            progress_eligible: Some(false),
            activity_key: None,
            action_target: None,
            application: None,
            host: None,
            command: None,
            wait: None,
            wait_ms: None,
            wait_resources: None,
            blocked_by: None,
            transaction_ms: None,
            cpu_ms: None,
            waiting_on_lock: None,
        })
        .collect();
    Ok(LiveState {
        taken_at: instant(owner.clock()?),
        sessions,
        notes,
        activity_census: None,
        session_census: None,
        waits: vec![],
        session_actions: vec![],
        session_actions_note: None,
        session_action_route: None,
        blocking: None,
    })
}
