//! One bounded focused projection reuses the native activity owner and does not change baseline/Live lock SQL.
use super::*;
use culprit_core::blocking::{
    MAX_FOCUSED_LOCK_SNAPSHOT_ROWS, MAX_NATIVE_RELATION_LOCK_MODES, NativeBlockingOwnerContext, NativeLockEvidence,
    PostgresLockBlocker, PostgresLockPoint, PostgresRelationLockMode, PostgresRelationLockTag,
};
use serde::Deserialize;
mod sql;
pub(crate) use sql::recovery_projection;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    pid: String,
    actor_matches: bool,
    single_backend: bool,
    granted: Vec<String>,
    waiting: Vec<String>,
    active: bool,
    wait_event_type: Option<String>,
    wait_event: Option<String>,
}

/// True means a retained lock/mode/edge prefix cannot qualify a queue mechanism.
pub(super) async fn read<R: QueryRead>(
    reader: &mut R,
    frame: &mut ActivityFrame,
    by_pid: &BTreeMap<u32, usize>,
    visited: &BTreeSet<usize>,
) -> Result<bool> {
    if visited.is_empty() {
        return Ok(false);
    }
    let query = sql::query(frame, by_pid, visited)?;
    let rows = reader.query(&query, visited.len()).await?;
    let mut observed = BTreeSet::new();
    let mut limited = false;
    for row in rows {
        let id = pid(&row)?;
        let index = *by_pid.get(&id).filter(|index| visited.contains(index)).ok_or_else(invalid)?;
        if !observed.insert(id) {
            return Err(invalid());
        }
        let expected = frame.records[index].facts.as_ref().ok_or_else(invalid)?.key.clone();
        let at = canonical_time(&text(&row, "observed_at")?)?;
        if at < canonical_time(&frame.at)? {
            return Err(CollectionStop::SourceChanged.error());
        }
        let actor_rows = text(&row, "actor_rows")?.parse::<u32>().map_err(|_| invalid())?;
        let snapshot_rows = text(&row, "snapshot_rows")?.parse::<u32>().map_err(|_| invalid())?;
        let snapshot_complete = snapshot_rows <= MAX_FOCUSED_LOCK_SNAPSHOT_ROWS;
        let blockers_complete = boolean(&row, "blockers_complete")?;
        limited |= !snapshot_complete || !blockers_complete || actor_rows > MAX_BLOCKING_PARTICIPANTS;
        let resource = if row.text("locktype").as_deref() == Some("relation") {
            Some(PostgresRelationLockTag {
                database_oid: text(&row, "database_oid")?,
                relation_oid: text(&row, "relation_oid")?,
            })
        } else {
            None
        };
        let requested_mode = resource
            .as_ref()
            .map(|_| text(&row, "mode").map(|raw| PostgresRelationLockMode::from_native(&raw)))
            .transpose()?;
        let raw: Vec<Owner> = serde_json::from_str(&text(&row, "owners")?).map_err(|_| invalid())?;
        if raw.len() > MAX_BLOCKING_LOOKUPS as usize {
            return Err(CollectionStop::RowLimit.error());
        }
        let mut blockers = Vec::new();
        for owner in raw {
            let pid = owner.pid.parse::<u32>().map_err(|_| invalid())?;
            if pid.to_string() != owner.pid
                || owner.granted.len() > MAX_NATIVE_RELATION_LOCK_MODES
                || owner.waiting.len() > 1
            {
                return Err(invalid());
            }
            let actor = if owner.actor_matches {
                let index = *by_pid.get(&pid).ok_or_else(invalid)?;
                Some(frame.records[index].facts.as_ref().ok_or_else(invalid)?.key.clone())
            } else {
                None
            };
            let owner_identity = ActivityWaitOwner::Session {
                id: owner.pid.clone(),
                generation: actor.as_ref().map(|key| key.session.generation.clone()),
            };
            let output_context = actor
                .as_ref()
                .map(|key| {
                    let index = *by_pid.get(&pid).ok_or_else(invalid)?;
                    let original = frame.records[index].facts.as_ref().ok_or_else(invalid)?;
                    let native_wait = match (owner.wait_event_type.as_ref(), owner.wait_event.as_ref()) {
                        (Some(kind), Some(event)) => Some(format!("{kind}: {event}")),
                        (Some(kind), None) => Some(kind.clone()),
                        _ => None,
                    };
                    Ok::<_, culprit_core::Error>(NativeBlockingOwnerContext {
                        owner: owner_identity.clone(),
                        actor: Some(key.clone()),
                        statement: original.statement_identity.clone(),
                        native_wait,
                        wait_ms: None,
                        current_requests: Some(if owner.active { "1" } else { "0" }.into()),
                        complete_request_population: owner.single_backend,
                    })
                })
                .transpose()?;
            blockers.push(PostgresLockBlocker {
                native_pid: owner.pid,
                actor,
                single_backend: owner.single_backend,
                granted_modes: owner.granted.iter().map(|raw| PostgresRelationLockMode::from_native(raw)).collect(),
                waiting_mode: owner.waiting.first().map(|raw| PostgresRelationLockMode::from_native(raw)),
                output_context,
            });
        }
        let point = PostgresLockPoint {
            observed_at: at,
            waiter: expected.clone(),
            waiter_matches: boolean(&row, "waiter_matches")?,
            single_backend: boolean(&row, "single_backend")?,
            actor_rows,
            resource,
            requested_mode,
            snapshot_rows,
            snapshot_limit: MAX_FOCUSED_LOCK_SNAPSHOT_ROWS,
            snapshot_complete,
            blockers_complete,
            blockers,
        };
        if !point.is_valid_for(&expected) {
            return Err(invalid());
        }
        let resource = culprit_core::LockWait {
            session: id.to_string(),
            mode: row.text("mode"),
            resource: row.text("locktype"),
            object: row.text("object"),
            index: row.text("index_name"),
            native: Some(Box::new(NativeLockEvidence::Postgres(Box::new(point)))),
        };
        if let Some(wait) = frame.records[index].facts.as_mut().and_then(|facts| facts.wait.as_mut()) {
            wait.elapsed_ms = optional_counter(&row, "elapsed_ms")?;
            wait.resources.get_or_insert_with(Vec::new).push(resource.clone());
        }
        frame.locks.push(resource);
    }
    if observed.len() != visited.len() {
        return Err(invalid());
    }
    Ok(limited)
}
fn boolean(row: &Row, name: &str) -> Result<bool> {
    match text(row, name)?.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests;
