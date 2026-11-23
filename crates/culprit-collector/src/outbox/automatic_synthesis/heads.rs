//! One bounded retained-context metadata and pin owner. A hint never replaces canonical EOF verification.
use super::*;
use crate::synthesis::{MAX_SYNTHESIS_RETAINED_HEADS, MAX_SYNTHESIS_RETAINED_VERSIONS};
use rusqlite::{Connection, Transaction, params};

pub(super) struct Hint {
    pub capture: DiagnosticCaptureRef,
    pub source: CollectionDescription,
    pub from: String,
    pub until: String,
    pub family: String,
}
impl Hint {
    pub fn matches(&self, capture: &SynthesisCapture) -> bool {
        self.capture == *capture.original()
            && self.source == *capture.source()
            && self.from == capture.collected_from()
            && self.until == capture.collected_until()
    }
}
fn pin_owner(key: &str) -> String {
    format!("synthesis:context-head:{key}")
}
pub(super) fn retain(
    tx: &Transaction<'_>,
    original: &DiagnosticCaptureRef,
    source: &CollectionDescription,
    scope: &str,
    family: &str,
    from: &str,
    until: &str,
) -> Result<()> {
    if !original.is_valid()
        || source.captured_range()?.scope_id != original.source_scope_id
        || ObservationBounds::new(from, until).is_none()
        || !valid_digest(scope)
        || !valid_digest(family)
    {
        return Err(integrity());
    }
    let revision = original.capture.revision.as_ref().ok_or_else(integrity)?;
    let epoch = revision.owner_epoch.parse::<i64>().map_err(|_| integrity())?;
    let sequence = revision.sequence.parse::<i64>().map_err(|_| integrity())?;
    let key = original.key()?;
    tx.execute(
        "INSERT OR IGNORE INTO synthesis_context_heads VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            key,
            scope,
            family,
            original.capture.observation_id,
            capture_journal::encode(original)?,
            capture_journal::encode(source)?,
            from,
            until,
            epoch,
            sequence
        ],
    )
    .map_err(storage)?;
    tx.execute(
        "INSERT OR IGNORE INTO diagnostic_pins VALUES(?1,?2)",
        params![pin_owner(&key), original.capture.observation_id],
    )
    .map_err(storage)?;
    trim(tx, Some((scope, family)), MAX_SYNTHESIS_RETAINED_VERSIONS)?;
    trim(tx, None, MAX_SYNTHESIS_RETAINED_HEADS)
}
fn trim(tx: &Transaction<'_>, selected: Option<(&str, &str)>, keep: u32) -> Result<()> {
    let removed: Vec<String> = match selected {
        Some((scope, family)) => {
            let mut query = tx.prepare("SELECT head_key FROM synthesis_context_heads WHERE scope_key=?1 AND family_key=?2 ORDER BY epoch DESC,sequence DESC LIMIT -1 OFFSET ?3").map_err(storage)?;
            query
                .query_map(params![scope, family, keep], |row| row.get(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<_>>()
                .map_err(storage)?
        }
        None => {
            let mut query = tx
                .prepare(
                    "SELECT head_key FROM synthesis_context_heads ORDER BY epoch DESC,sequence DESC LIMIT -1 OFFSET ?1",
                )
                .map_err(storage)?;
            query
                .query_map([keep], |row| row.get(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<_>>()
                .map_err(storage)?
        }
    };
    for key in removed {
        remove_key(tx, &key)?;
    }
    Ok(())
}
fn remove_key(tx: &Transaction<'_>, key: &str) -> Result<()> {
    tx.execute("DELETE FROM diagnostic_pins WHERE owner=?1", [pin_owner(key)]).map_err(storage)?;
    tx.execute("DELETE FROM synthesis_context_heads WHERE head_key=?1", [key]).map_err(storage)?;
    Ok(())
}
pub(super) fn remove(tx: &Transaction<'_>, id: &str) -> Result<()> {
    let keys: Vec<String> = {
        let mut query = tx
            .prepare("SELECT head_key FROM synthesis_context_heads WHERE observation_id=?1 LIMIT ?2")
            .map_err(storage)?;
        query
            .query_map(params![id, MAX_SYNTHESIS_RETAINED_HEADS], |row| row.get(0))
            .map_err(storage)?
            .collect::<rusqlite::Result<_>>()
            .map_err(storage)?
    };
    for key in keys {
        remove_key(tx, &key)?;
    }
    Ok(())
}
pub(super) fn select(conn: &Connection, scope: &str) -> Result<Vec<Hint>> {
    let mut query = conn.prepare("SELECT capture_json,source_json,collected_from,collected_until,family_key FROM synthesis_context_heads WHERE scope_key=?1 ORDER BY epoch DESC,sequence DESC LIMIT ?2").map_err(storage)?;
    let rows = query
        .query_map(params![scope, MAX_SYNTHESIS_RETAINED_HEADS], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    rows.into_iter()
        .map(|(capture, source, from, until, family)| {
            let capture: DiagnosticCaptureRef = capture_journal::decode(&capture)?;
            let source: CollectionDescription = capture_journal::decode(&source)?;
            if !capture.is_valid()
                || source.captured_range()?.scope_id != capture.source_scope_id
                || ObservationBounds::new(&from, &until).is_none()
                || !valid_digest(&family)
            {
                return Err(integrity());
            }
            Ok(Hint { capture, source, from, until, family })
        })
        .collect()
}
