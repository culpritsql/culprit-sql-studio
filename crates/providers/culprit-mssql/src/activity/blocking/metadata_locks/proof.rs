//! Endpoint comparisons retain raw native resource/mode/owner facts; acquiring SQL is never inferred.
use super::*;
use data::NativeTask;

pub(super) fn points(
    frame: &ActivityFrame,
    first: &Snapshot,
    last: &Snapshot,
) -> Result<BTreeMap<usize, Vec<LockWait>>> {
    let mut result = BTreeMap::new();
    for (ordinal, row) in frame.records.iter().enumerate().filter(|(_, row)| row.facts.waiting_on_lock == Some(true)) {
        let existing = row.facts.wait.as_ref().and_then(|wait| wait.resources.as_ref()).map_or(0, Vec::len);
        if existing >= MAX_ACTIVITY_LOCK_RESOURCES {
            continue;
        }
        if !first.visible {
            let point = unavailable_point(row, first, last, MssqlObjectLockAvailability::PermissionDenied);
            if !point.is_valid_for(&row.facts.key) {
                return Err(invalid());
            }
            result.insert(
                ordinal,
                vec![LockWait {
                    session: row.facts.key.session.id.clone(),
                    mode: None,
                    resource: None,
                    object: None,
                    index: None,
                    native: Some(Box::new(NativeLockEvidence::MssqlObject(Box::new(point)))),
                }],
            );
            continue;
        }
        let locks = first
            .locks()
            .filter(|lock| {
                row.facts.key.session.id == lock.session_id.to_string()
                    && matches!(lock.status.as_deref(), Some("WAIT" | "CONVERT"))
            })
            .collect::<Vec<_>>();
        if locks.len() > MAX_ACTIVITY_LOCK_RESOURCES.saturating_sub(existing) {
            let point = unavailable_point(row, first, last, MssqlObjectLockAvailability::Limited);
            result.insert(
                ordinal,
                vec![LockWait {
                    session: row.facts.key.session.id.clone(),
                    mode: None,
                    resource: None,
                    object: None,
                    index: None,
                    native: Some(Box::new(NativeLockEvidence::MssqlObject(Box::new(point)))),
                }],
            );
            continue;
        }
        let mut recorded = Vec::new();
        for lock in locks {
            let point = point(frame, ordinal, lock, first, last)?;
            recorded.push(LockWait {
                session: row.facts.key.session.id.clone(),
                mode: lock.mode.clone(),
                resource: Some("OBJECT".into()),
                object: lock.object_label.clone(),
                index: None,
                native: Some(Box::new(NativeLockEvidence::MssqlObject(Box::new(point)))),
            });
        }
        if !recorded.is_empty() {
            result.insert(ordinal, recorded);
        }
    }
    Ok(result)
}
fn point(
    frame: &ActivityFrame,
    ordinal: usize,
    lock: &NativeLock,
    first: &Snapshot,
    last: &Snapshot,
) -> Result<MssqlObjectLockPoint> {
    let row = &frame.records[ordinal];
    let actor = first.actors.get(&ordinal).ok_or_else(invalid)?;
    let tasks = first.tasks(lock);
    let blockers = blockers(frame, lock, &tasks, first, last)?;
    let point = MssqlObjectLockPoint {
        observed_at: first.at.clone(),
        settled_at: last.at.clone(),
        waiter: row.facts.key.clone(),
        waiter_matches: actor.matches_transaction(row, lock),
        waiter_settled: last.complete()
            && last.locks().any(|candidate| candidate.same_native_lock(lock))
            && last.actors.get(&ordinal).is_some_and(|actor| actor.matches_transaction(row, lock)),
        waiter_current_requests: actor.requests.clone(),
        waiter_complete_request_population: first.visible
            && actor.session.as_ref() == Some(&row.facts.key.session)
            && actor.requests.is_some(),
        waiter_transaction: lock.transaction.clone(),
        resource: Some(lock.resource.clone()),
        requested_mode: lock.mode.as_deref().map(MssqlObjectLockMode::from_native),
        owner_type: lock.owner_type.clone(),
        owner_id: lock.owner_id.clone(),
        owner_guid: lock.owner_guid.clone(),
        requested_status: lock.status.clone(),
        lock_owner_address: lock.owner_address.clone(),
        lock_request_id: lock.request_id.map(|id| id.to_string()),
        lock_execution_context_id: lock.execution_context_id.map(|id| id.to_string()),
        availability: if !first.visible || !last.visible {
            MssqlObjectLockAvailability::PermissionDenied
        } else if !first.complete() || !last.complete() {
            MssqlObjectLockAvailability::Limited
        } else {
            MssqlObjectLockAvailability::Observed
        },
        snapshot_rows: first.entries.len() as u32,
        snapshot_limit: first.limit,
        snapshot_complete: first.complete(),
        blockers_complete: first.complete()
            && last.complete()
            && !tasks.is_empty()
            && tasks
                .iter()
                .all(|task| task.blocking_session_id.is_some_and(|id| id > 0 && first.actor(frame, id).is_some()))
            && blockers.iter().all(|owner| owner.owner_id.is_some()),
        blockers,
    };
    if !point.is_valid_for(&row.facts.key) {
        return Err(invalid());
    }
    Ok(point)
}
fn blockers(
    frame: &ActivityFrame,
    lock: &NativeLock,
    tasks: &[&NativeTask],
    first: &Snapshot,
    last: &Snapshot,
) -> Result<Vec<MssqlObjectLockBlocker>> {
    let ids = tasks.iter().filter_map(|task| task.blocking_session_id).filter(|id| *id > 0).collect::<BTreeSet<_>>();
    ids.into_iter().map(|id| blocker(frame, lock, id, tasks, first, last)).collect()
}
fn blocker(
    frame: &ActivityFrame,
    lock: &NativeLock,
    id: i32,
    tasks: &[&NativeTask],
    first: &Snapshot,
    last: &Snapshot,
) -> Result<MssqlObjectLockBlocker> {
    let (row, actor) = match first.actor(frame, id) {
        Some(pair) => pair,
        None => return Ok(unavailable_owner(id, tasks)),
    };
    let original =
        first.locks().filter(|owner| owner.session_id == id && owner.resource == lock.resource).collect::<Vec<_>>();
    let final_locks =
        last.locks().filter(|owner| owner.session_id == id && owner.resource == lock.resource).collect::<Vec<_>>();
    let native = unique_owner(&original);
    let pending = original.iter().filter(|owner| !is_granted(owner)).copied().collect::<Vec<_>>();
    let waiting = if pending.len() == 1 { Some(pending[0]) } else { None };
    let owner =
        ActivityWaitOwner::Session { id: id.to_string(), generation: Some(row.facts.key.session.generation.clone()) };
    let mut granted = original
        .iter()
        .filter(|owner| is_granted(owner))
        .filter_map(|owner| owner.mode.as_deref())
        .map(MssqlObjectLockMode::from_native)
        .collect::<Vec<_>>();
    granted.sort();
    granted.dedup();
    let settled = last.complete()
        && original.len() == final_locks.len()
        && original.iter().all(|owner| final_locks.iter().any(|after| owner.same_native_lock(after)))
        && native.is_some_and(|native| {
            last.actor(frame, id).is_some_and(|(row, actor)| actor.matches_transaction(row, native))
        })
        && same_edges(lock, id, tasks, last);
    let owner = MssqlObjectLockBlocker {
        context: actor.context(row, owner),
        transaction: native.and_then(|native| native.transaction.clone()),
        owner_type: native.and_then(|native| native.owner_type.clone()),
        owner_id: native.and_then(|native| native.owner_id.clone()),
        owner_guid: native.and_then(|native| native.owner_guid.clone()),
        granted_modes: granted,
        waiting_mode: waiting.and_then(|owner| owner.mode.as_deref()).map(MssqlObjectLockMode::from_native),
        waiting_status: waiting.and_then(|owner| owner.status.clone()),
        native_wait_edges: edges(id, tasks),
        matched: native.is_some_and(|native| actor.matches_transaction(row, native)) && pending.len() <= 1,
        settled,
    };
    if !owner.is_valid() {
        return Err(invalid());
    }
    Ok(owner)
}
fn unique_owner<'a>(locks: &[&'a NativeLock]) -> Option<&'a NativeLock> {
    let first = *locks.first()?;
    locks
        .iter()
        .all(|lock| {
            lock.owner_type == first.owner_type
                && lock.owner_id == first.owner_id
                && lock.owner_guid == first.owner_guid
                && lock.transaction == first.transaction
        })
        .then_some(first)
}
fn same_edges(lock: &NativeLock, id: i32, original: &[&NativeTask], last: &Snapshot) -> bool {
    let before = original.iter().copied().filter(|task| task.blocking_session_id == Some(id)).collect::<Vec<_>>();
    let after = last.tasks(lock).into_iter().filter(|task| task.blocking_session_id == Some(id)).collect::<Vec<_>>();
    before.len() == after.len() && before.iter().all(|task| after.contains(task))
}
fn edges(id: i32, tasks: &[&NativeTask]) -> Vec<MssqlObjectLockWaitEdge> {
    tasks
        .iter()
        .copied()
        .filter(|task| task.blocking_session_id == Some(id))
        .filter_map(|task| {
            Some(MssqlObjectLockWaitEdge {
                task_address: task.task_address.clone(),
                owner_address: task.owner_address.clone()?,
                waiter_execution_context_id: task.execution_context_id.to_string(),
                blocking_session_id: id.to_string(),
                blocker_execution_context_id: task.blocker_execution_context_id.map(|id| id.to_string()),
            })
        })
        .collect()
}
fn unavailable_owner(id: i32, tasks: &[&NativeTask]) -> MssqlObjectLockBlocker {
    MssqlObjectLockBlocker {
        context: NativeBlockingOwnerContext {
            owner: ActivityWaitOwner::Session { id: id.to_string(), generation: None },
            actor: None,
            statement: None,
            native_wait: None,
            wait_ms: None,
            current_requests: None,
            complete_request_population: false,
        },
        transaction: None,
        owner_type: None,
        owner_id: None,
        owner_guid: None,
        granted_modes: Vec::new(),
        waiting_mode: None,
        waiting_status: None,
        native_wait_edges: edges(id, tasks),
        matched: false,
        settled: false,
    }
}

fn unavailable_point(
    row: &ActivityRecord,
    first: &Snapshot,
    last: &Snapshot,
    availability: MssqlObjectLockAvailability,
) -> MssqlObjectLockPoint {
    MssqlObjectLockPoint {
        observed_at: first.at.clone(),
        settled_at: last.at.clone(),
        waiter: row.facts.key.clone(),
        waiter_matches: false,
        waiter_settled: false,
        waiter_current_requests: None,
        waiter_complete_request_population: false,
        waiter_transaction: None,
        resource: None,
        requested_mode: None,
        owner_type: None,
        owner_id: None,
        owner_guid: None,
        requested_status: None,
        lock_owner_address: None,
        lock_request_id: None,
        lock_execution_context_id: None,
        availability,
        snapshot_rows: first.entries.len() as u32,
        snapshot_limit: first.limit,
        snapshot_complete: false,
        blockers_complete: false,
        blockers: Vec::new(),
    }
}

// Classify only the native GRANT token and its documented GRANTED spelling.
// No unknown or conversion status is treated as a granted request.
fn is_granted(lock: &NativeLock) -> bool {
    matches!(lock.status.as_deref(), Some("GRANT" | "GRANTED"))
}
