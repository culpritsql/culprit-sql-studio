//! Selected schema-lock details reuse this held Activity reader and its exact final native context guard.
use super::*;
use crate::read::{Parameter, ReadError};
use culprit_core::{LockWait, blocking::*};
use data::{NativeLock, Snapshot};
use std::collections::BTreeMap;
mod data;
mod proof;
mod sql;

// Conservative native buffering admission; no page/caller limit is raised. Native manager work remains Unqualified.
const GUARD_BYTES: u32 = 4096;
const ACTOR_BYTES: u32 = 1024;
const ENTRY_BYTES: u32 = 4096;
pub(super) fn selected(frame: &ActivityFrame, focus: &ActivityRequestFocus) -> bool {
    let original_is_waiting =
        frame.records.iter().any(|row| row.facts.key == focus.original.key && row.facts.waiting_on_lock == Some(true));
    original_is_waiting
        && frame.records.iter().any(|row| {
            row.facts.waiting_on_lock == Some(true)
                && row
                    .facts
                    .wait
                    .as_ref()
                    .and_then(|wait| wait.native_type.as_deref())
                    .is_some_and(|wait| matches!(wait, "LCK_M_SCH_S" | "LCK_M_SCH_M"))
        })
}
pub(super) fn snapshot_limit(frame: &ActivityFrame, budget: &CollectionBudget) -> Option<u32> {
    let actors = u32::try_from(frame.records.len()).ok()?;
    if actors == 0
        || actors > MAX_BLOCKING_PARTICIPANTS
        || actors + 1 > budget.max_snapshot_rows
        || frame.records.iter().map(|row| &row.facts.key.session.id).collect::<BTreeSet<_>>().len()
            != frame.records.len()
    {
        return None;
    }
    let bytes = budget.page_bytes.checked_sub(GUARD_BYTES + actors * ACTOR_BYTES)?;
    let limit =
        (bytes / ENTRY_BYTES).saturating_sub(1).min(budget.max_snapshot_rows).min(MAX_FOCUSED_LOCK_SNAPSHOT_ROWS);
    (limit > 0).then_some(limit)
}
fn selectors(frame: &ActivityFrame) -> Result<String> {
    let mut sessions = BTreeSet::new();
    let mut xml = String::from("<activity>");
    for (ordinal, row) in frame.records.iter().enumerate() {
        let key = &row.facts.key;
        let session = key.session.id.parse::<i16>().map_err(|_| invalid())?;
        if session <= 0 || session.to_string() != key.session.id || !sessions.insert(&key.session) {
            return Err(invalid());
        }
        let login = crate::sql_time::native_time(&key.session.generation)?;
        let request =
            key.request.as_ref().map(|request| request.id.parse::<i32>()).transpose().map_err(|_| invalid())?;
        if request.is_some_and(|request| request < 0) {
            return Err(invalid());
        }
        xml.push_str(&format!(
            "<row ordinal=\"{ordinal}\" session=\"{session}\" login=\"{login}\" request=\"{}\"/>",
            request.unwrap_or(-1)
        ));
    }
    xml.push_str("</activity>");
    Ok(xml)
}
pub(super) async fn read<R: NativeRead>(
    reader: &mut ActivityReader<R>,
    frame: &ActivityFrame,
    limit: u32,
) -> Result<Snapshot> {
    let rows = reader
        .io
        .query(
            &sql::query(),
            vec![Parameter::Text(selectors(frame)?), Parameter::Integer(i64::from(limit))],
            frame.records.len() + 1,
        )
        .await
        .map_err(ReadError::into_error)?;
    Snapshot::decode(rows, frame, limit)
}
pub(super) fn attach(frame: &mut ActivityFrame, first: &Snapshot, last: &Snapshot) -> Result<()> {
    if first.limit != last.limit || ObservationBounds::new(&first.at, &last.at).is_none() {
        return Err(CollectionStop::SourceChanged.error());
    }
    let additions = proof::points(frame, first, last)?;
    for (ordinal, locks) in additions {
        frame.records[ordinal]
            .facts
            .wait
            .as_mut()
            .ok_or_else(invalid)?
            .resources
            .get_or_insert_with(Vec::new)
            .extend(locks);
    }
    Ok(())
}
#[cfg(test)]
pub(super) mod tests;
