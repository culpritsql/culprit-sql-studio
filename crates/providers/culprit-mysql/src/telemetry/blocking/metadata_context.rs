//! Optional deeper orchestration reuses the held reader, one-hop decoder and paired native participant owner.
use super::*;
use crate::Flavor;
use crate::native::locks::metadata_context as tickets;
use culprit_core::{LockWait, blocking::*};
const SOURCE: &str = "mysql-metadata-lock-context";

pub(super) async fn observe<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus: &ActivityRequestFocus,
    budget: &CollectionBudget,
) -> Result<Observation> {
    let root_sql = (budget.max_sql_bytes as usize).min(budget.total_bytes as usize / 8);
    let root = participants::root(reader, meta, focus, true, root_sql).await?;
    let state = participants::request_state(&root, focus);
    if state != BlockingRequestState::SameStatement {
        return Ok(Observation {
            frame: participants::Frame { rows: vec![], at: root.at, limited: root.limited },
            binding: if state == BlockingRequestState::Missing {
                FocusedRequestBinding::ParticipantMissing
            } else {
                FocusedRequestBinding::ParticipantChanged
            },
            membership: FocusedRequestMembership::NotObserved,
            request_end: BlockingRequestState::NotRead,
            status: BlockingClosureStatus::NotRead,
        });
    }
    // One guard row and one native lookahead are inside the unchanged caller/native response-row ceiling.
    let limit = budget.max_snapshot_rows.saturating_sub(2).min(MAX_BLOCKING_PARTICIPANTS - 2);
    if limit == 0 {
        return Err(CollectionStop::RowLimit.error());
    }
    let before = edges::read(reader, meta, focus, budget.max_snapshot_rows).await?;
    let first_tickets = tickets::read(reader, &meta.connection.database_name, native_thread(focus)?, limit).await?;
    let mut selected = before.participants(focus, budget.max_snapshot_rows)?;
    let mut ids = selected.ids.into_iter().collect::<BTreeSet<_>>();
    for id in first_tickets.participant_ids() {
        if ids.contains(&id) {
            continue;
        }
        if ids.len() == budget.max_snapshot_rows as usize {
            selected.limited = true;
            continue;
        }
        ids.insert(id);
    }
    let ids = ids.into_iter().collect::<Vec<_>>();
    // Bound combined current SQL, not only each field. Sleeping/completed previous SQL stays uncollected.
    let context_sql = (budget.max_sql_bytes as usize).min(budget.total_bytes as usize / (8 * ids.len().max(1)));
    let first = participants::selected_with_statements(reader, meta, &ids, context_sql).await?;
    let after = edges::read(reader, meta, focus, budget.max_snapshot_rows).await?;
    let last_tickets = tickets::read(reader, &meta.connection.database_name, native_thread(focus)?, limit).await?;
    let end = participants::selected_with_statements(reader, meta, &ids, context_sql).await?;
    ordered(&root.at, &first_tickets.at)?;
    ordered(&first_tickets.at, &first.at)?;
    ordered(&first.at, &last_tickets.at)?;
    ordered(&last_tickets.at, &end.at)?;
    let point = point(meta, focus, &first, &end, &first_tickets, &last_tickets, selected.limited)?;
    let request_end = participants::request_state(&end, focus);
    let (mut frame, status) =
        edges::correlate(root, first.clone(), &end, before, after, selected.limited, focus, budget.max_snapshot_rows)?;
    for row in &mut frame.rows {
        if row.key != focus.original.key
            && let Some(current) = first.rows.iter().find(|current| current.key == row.key)
            && end.rows.iter().any(|last| same_actor(current, last))
        {
            row.statement = current.statement.clone();
        }
    }
    for actor in &first.rows {
        if actor.key.session == focus.original.key.session && actor.key != focus.original.key {
            continue;
        }
        if point.accepts_actor(&actor.key) && !frame.rows.iter().any(|known| known.key == actor.key) {
            frame.rows.push(actor.clone());
        }
    }
    let original = frame.rows.iter_mut().find(|row| row.key == focus.original.key).ok_or_else(native::invalid)?;
    let wait = original.wait.as_mut().ok_or_else(native::invalid)?;
    let resources = wait.resources.get_or_insert_with(Vec::new);
    if resources.len() >= MAX_ACTIVITY_LOCK_RESOURCES {
        return Err(CollectionStop::RowLimit.error());
    }
    resources.push(LockWait {
        session: original.key.session.id.clone(),
        mode: None,
        resource: Some("METADATA".into()),
        object: None,
        index: None,
        native: Some(Box::new(NativeLockEvidence::MysqlMetadata(Box::new(point)))),
    });
    Ok(Observation {
        frame,
        binding: FocusedRequestBinding::Matched,
        membership: FocusedRequestMembership::Observed,
        request_end,
        status,
    })
}
fn same_actor(first: &ActivityObservation, last: &ActivityObservation) -> bool {
    first.key == last.key && first.statement_identity == last.statement_identity && first.idle == last.idle
}
fn point(
    meta: &Metadata,
    focus: &ActivityRequestFocus,
    first: &participants::Frame,
    last: &participants::Frame,
    before: &tickets::Snapshot,
    after: &tickets::Snapshot,
    limited: bool,
) -> Result<MysqlMetadataLockPoint> {
    let subject_matches = participants::request_state(first, focus) == BlockingRequestState::SameStatement;
    let subject_settled =
        subject_matches && participants::request_state(last, focus) == BlockingRequestState::SameStatement;
    let mut retained = before.tickets.clone();
    for ticket in &mut retained {
        let current = ticket.owner_process_id.as_ref().and_then(|id| {
            first.rows.iter().find(|row| {
                &row.key.session.id == id
                    && Some(row.key.session.generation.as_str()) == ticket.owner_thread_id.as_deref()
            })
        });
        if let Some(current) = current.filter(|_| subject_matches) {
            ticket.actor = Some(current.key.clone());
            ticket.matched = true;
            ticket.settled = after.complete()
                && after.tickets.iter().any(|next| tickets::same_ticket(ticket, next))
                && last.rows.iter().any(|next| same_actor(current, next));
        }
    }
    let complete = before.complete() && after.complete() && !limited && !first.limited && !last.limited;
    let availability = if !complete {
        MysqlMetadataLockAvailability::Limited
    } else if retained.is_empty() {
        MysqlMetadataLockAvailability::Unavailable
    } else {
        MysqlMetadataLockAvailability::Observed
    };
    let point = MysqlMetadataLockPoint {
        origin: match meta.connection.flavor {
            Flavor::Mysql => MysqlMetadataLockOrigin::MysqlPerformanceSchema,
            Flavor::Mariadb => MysqlMetadataLockOrigin::MariadbPerformanceSchema,
        },
        observed_at: before.at.clone(),
        settled_at: after.at.clone(),
        subject: focus.original.key.clone(),
        subject_matches,
        subject_settled,
        availability,
        snapshot_rows: before.rows.max(after.rows),
        snapshot_limit: before.limit,
        snapshot_complete: complete,
        tickets: retained,
    };
    if !point.is_valid_for(&focus.original.key) {
        return Err(native::invalid());
    }
    Ok(point)
}
pub(super) fn source() -> &'static str {
    SOURCE
}
pub(super) fn limited(frame: &participants::Frame) -> bool {
    frame.limited || frame.rows.iter().flat_map(|row|row.wait.iter()).flat_map(|wait|wait.resources.iter().flatten())
        .filter_map(|lock|lock.native.as_deref()).any(|point|matches!(point,NativeLockEvidence::MysqlMetadata(point) if point.availability==MysqlMetadataLockAvailability::Limited))
}
pub(super) fn observation(
    request: &CollectionRequest,
    context: &NativeReadContext,
    snapshot: ActivitySnapshotDescription,
    result: ActivityBlockingFocusResult,
    rows: &[ActivityObservation],
) -> Result<SourceObservation> {
    let CollectionSelection::SourceRead { focus, .. } = &request.selection else {
        return Err(native::invalid());
    };
    Ok(SourceObservation::MetadataLockContext {
        snapshot,
        focus_key: focus.key()?,
        execution: Box::new(context.clone()),
        result: Box::new(MetadataLockContextDescription {
            request: result,
            cohort: rows.iter().map(|row| row.key.clone()).collect(),
        }),
    })
}
#[cfg(test)]
mod tests;
