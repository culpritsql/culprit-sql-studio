//! Selected native request/session points through Activity's one projection and decoder.
use super::*;
#[derive(Clone)]
pub(super) struct Frame {
    pub rows: Vec<ActivityObservation>,
    pub at: String,
    pub limited: bool,
}
pub(super) async fn root<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus: &ActivityRequestFocus,
    instrumented: bool,
    max_sql: usize,
) -> Result<Frame> {
    let id = session_id(&focus.original.key.session.id)?;
    let sql = native::activity::selected_request_query(native::activity::payload::COLUMNS, 2);
    decode(
        reader
            .read(
                &sql,
                vec![Value::UInt(max_sql as u64), Value::UInt(id), Value::from(meta.connection.database_name.clone())],
                2,
            )
            .await?,
        meta,
        instrumented,
        Some(max_sql),
        1,
    )
}
pub(super) async fn selected<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    ids: &[u64],
    instrumented: bool,
    limit: usize,
) -> Result<Frame> {
    let sql = native::activity::selected_participants_query("NULL AS info", ids.len(), limit + 1)?;
    let parameters = ids.iter().copied().map(Value::UInt).collect();
    let rows = reader.read(&sql, parameters, limit + 1).await?;
    for row in &rows {
        let id = native::integer(row, "id")?;
        if !ids.contains(&id) || row.native_text("thread_type")?.as_deref() != Some("FOREGROUND") {
            return Err(CollectionStop::SourceChanged.error());
        }
    }
    decode(rows, meta, instrumented, None, limit)
}
pub(super) async fn selected_with_statements<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    ids: &[u64],
    max_sql: usize,
) -> Result<Frame> {
    let columns = native::activity::payload::current_participant_columns();
    let sql = native::activity::selected_participants_query(&columns, ids.len(), ids.len() + 1)?;
    let mut parameters = vec![Value::UInt(max_sql as u64)];
    parameters.extend(ids.iter().copied().map(Value::UInt));
    let raw = reader.read(&sql, parameters, ids.len() + 1).await?;
    for row in &raw {
        if !ids.contains(&native::integer(row, "id")?)
            || row.native_text("thread_type")?.as_deref() != Some("FOREGROUND")
        {
            return Err(CollectionStop::SourceChanged.error());
        }
    }
    decode(raw, meta, true, Some(max_sql), ids.len())
}

fn decode(raw: Vec<Rec>, meta: &Metadata, instrumented: bool, max_sql: Option<usize>, limit: usize) -> Result<Frame> {
    if raw.len() > limit + 1 {
        return Err(native::invalid());
    }
    let limited = raw.len() > limit;
    let mut at = None;
    let mut rows = Vec::new();
    for row in raw.iter().take(limit) {
        let stamp = canonical_time(&native::text(row, "observed_at")?)?;
        if at.as_ref().is_some_and(|at| at != &stamp) {
            return Err(native::invalid());
        }
        at = Some(stamp);
        let mut actor = native::activity::decode(row, &meta.connection.database_name, instrumented)?
            .ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        if let Some(max_sql) = max_sql.filter(|_| actor.key.request.is_some()) {
            actor.statement = native::activity::payload::decode(row, max_sql)?.0;
        }
        if !actor.is_valid() {
            return Err(native::invalid());
        }
        rows.push(actor);
    }
    ActivityEvidence::validate(&rows)?;
    Ok(Frame { rows, at: at.unwrap_or(canonical_time(&meta.connection.at)?), limited })
}
pub(super) fn same_request(row: &ActivityObservation, focus: &ActivityRequestFocus) -> bool {
    row.key == focus.original.key && row.statement_identity.as_ref() == Some(&focus.statement_identity)
}
pub(super) fn request_state(frame: &Frame, focus: &ActivityRequestFocus) -> BlockingRequestState {
    match frame.rows.iter().find(|row| row.key.session.id == focus.original.key.session.id) {
        None => BlockingRequestState::Missing,
        Some(row) if same_request(row, focus) => BlockingRequestState::SameStatement,
        Some(_) => BlockingRequestState::Changed,
    }
}
pub(super) fn session_id(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0 && *id <= u64::from(u32::MAX) && id.to_string() == value)
        .ok_or_else(native::invalid)
}
