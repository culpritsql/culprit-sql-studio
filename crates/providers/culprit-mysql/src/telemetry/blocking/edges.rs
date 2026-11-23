//! Pair exact native edges with both selected native session points; current SQL is not acquiring SQL.
use super::*;
use crate::native::locks::selected::{self, Edge, Family};

pub(super) struct Roster {
    edges: Vec<Edge>,
    limited: bool,
}
/// Paired selected native edge rosters retain their original acquisition order.
pub(super) struct RosterPair {
    pub before: Roster,
    pub after: Roster,
}
pub(super) struct Participants {
    pub ids: Vec<u64>,
    pub limited: bool,
}
impl Roster {
    pub fn participants(&self, focus: &ActivityRequestFocus, cap: u32) -> Result<Participants> {
        let root = participants::session_id(&focus.original.key.session.id)?;
        let mut ids = BTreeSet::from([root]);
        let owners = self.edges.iter().map(|edge| edge.pair.blocking).collect::<BTreeSet<_>>();
        let limited = owners.len().saturating_add(1) > cap as usize;
        ids.extend(owners.into_iter().take(cap.saturating_sub(1) as usize));
        Ok(Participants { ids: ids.into_iter().collect(), limited })
    }
    fn same_edges(&self, other: &Self) -> bool {
        self.edges.len() == other.edges.len()
            && self.edges.iter().all(|edge| other.edges.iter().any(|next| edge.same_point(next)))
    }
}
pub(super) async fn read<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus: &ActivityRequestFocus,
    cap: u32,
) -> Result<Roster> {
    let root = participants::session_id(&focus.original.key.session.id)?;
    let thread = native_thread(focus)?;
    let limit = cap.saturating_sub(1) as usize;
    let mut roster = Roster { edges: Vec::new(), limited: false };
    for family in [Family::Data, Family::Metadata] {
        let point = selected::read(reader, meta, family, root, thread, limit).await?;
        roster.limited |= point.limited;
        for edge in point.edges {
            if edge.pair.session != root || edge.waiter_thread != thread || edge.pair.blocking == root {
                return Err(native::invalid());
            }
            if !roster.edges.iter().any(|known| known.same_point(&edge)) {
                roster.edges.push(edge);
            }
        }
    }
    Ok(roster)
}
pub(super) fn correlate(
    root: participants::Frame,
    mut first: participants::Frame,
    end: &participants::Frame,
    rosters: RosterPair,
    participant_limit: bool,
    focus: &ActivityRequestFocus,
    cap: u32,
) -> Result<(participants::Frame, BlockingClosureStatus)> {
    let RosterPair { before, after } = rosters;
    if participants::request_state(&first, focus) != BlockingRequestState::SameStatement
        || participants::request_state(end, focus) != BlockingRequestState::SameStatement
    {
        // A later request on the same thread cannot donate its lock edges to the original victim.
        return Ok((root, BlockingClosureStatus::LockEvidenceLimit));
    }
    let original = root.rows.into_iter().next().ok_or_else(native::invalid)?;
    let mut status = if participant_limit {
        BlockingClosureStatus::ParticipantLimit
    } else if before.limited
        || after.limited
        || first.limited
        || end.limited
        || before.edges.is_empty()
        || !before.same_edges(&after)
    {
        BlockingClosureStatus::LockEvidenceLimit
    } else {
        BlockingClosureStatus::Observed
    };
    let mut observed = Vec::new();
    let mut owners = Vec::new();
    let mut resources = Vec::new();
    let mut identified = Vec::new();
    for edge in &before.edges {
        let owner = ActivityWaitOwner::Session {
            id: edge.pair.blocking.to_string(),
            generation: Some(edge.owner_thread.to_string()),
        };
        if !owners.contains(&owner) {
            owners.push(owner);
        }
        let resource = edge.pair.lock_wait();
        if !resources.contains(&resource) {
            resources.push(resource);
        }
        if let Some(resource) = &edge.pair.identified_resource
            && !identified.contains(resource)
        {
            identified.push(resource.clone());
        }
        if !after.edges.iter().any(|next| edge.same_point(next)) {
            continue;
        }
        let matches = |row: &&ActivityObservation| {
            row.key.session.id == edge.pair.blocking.to_string()
                && row.key.session.generation == edge.owner_thread.to_string()
        };
        if let (Some(row), Some(_)) = (first.rows.iter().find(matches), end.rows.iter().find(matches)) {
            let mut row = row.clone();
            row.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
            if row.waiting_on_lock == Some(true) && status == BlockingClosureStatus::Observed {
                status = BlockingClosureStatus::DepthLimit;
            }
            if !observed.iter().any(|known: &ActivityObservation| known.key == row.key) {
                observed.push(row);
            }
        } else if status == BlockingClosureStatus::Observed {
            status = BlockingClosureStatus::LockEvidenceLimit;
        }
    }
    // A changed/ended root never substitutes its later statement into the incident. Preserve the checked earlier
    // native request point, while the independent final request guard keeps closure completeness honest.
    let mut waiter =
        first.rows.iter().find(|row| participants::same_request(row, focus)).cloned().unwrap_or(original.clone());
    waiter.statement = original.statement;
    let state = waiter.state.clone();
    let wait = waiter.wait.get_or_insert_with(|| ActivityWait {
        class: None,
        native_type: Some(state),
        elapsed_ms: None,
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    wait.owners = owners;
    wait.resources = Some(resources);
    wait.identified_resources = identified;
    if !wait.owners.is_empty() {
        waiter.waiting_on_lock = Some(true);
    }
    let mut rows = vec![waiter];
    rows.extend(observed);
    let selection = ActivityEvidence::bounded_context_for(&rows, &BTreeSet::from([focus.original.key.clone()]), cap)?;
    if selection.closure.status.is_limited() {
        status = selection.closure.status;
    }
    rows.retain(|row| selection.keys.contains(&row.key));
    first.rows = rows;
    Ok((first, status))
}
