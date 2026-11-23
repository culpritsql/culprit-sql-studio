//! Ticket presence is useful context; no native pending-blocker pointer is exposed by these tables.
use super::*;

pub(super) fn explain(
    frame: &ActivityFrameEvidence<'_>,
    header: &MetadataLockContextDescription,
) -> Result<MetadataQueueEvidence> {
    if !header.accepts_frame(frame) {
        return Err(invalid());
    }
    let mut issues = BTreeSet::from([MetadataQueueIssue::PendingBlockerNotExposed]);
    let mut candidates = Vec::new();
    if !frame.read_complete() {
        issues.insert(MetadataQueueIssue::IncompleteRead);
    }
    if !frame.is_qualified() || !frame.context().has_qualified_local_owner() {
        issues.insert(MetadataQueueIssue::UnqualifiedOriginal);
    }
    let point = header.point(frame);
    if let Some(point) = point {
        if !point.subject_matches
            || !point.subject_settled
            || header.request.request_end != BlockingRequestState::SameStatement
        {
            issues.insert(MetadataQueueIssue::PointChanged);
        }
        if point.availability == MysqlMetadataLockAvailability::Limited || !point.snapshot_complete {
            issues.insert(MetadataQueueIssue::LockEvidenceLimited);
        } else if point.availability != MysqlMetadataLockAvailability::Observed {
            issues.insert(MetadataQueueIssue::NativeVisibilityUnavailable);
        }
        if issues.len() == 1 {
            append(frame, point, &mut candidates, &mut issues)?;
        }
    } else {
        issues.insert(MetadataQueueIssue::NativeDetailsNotRecorded);
    }
    Ok(MetadataQueueEvidence {
        status: MetadataQueueStatus::Unavailable,
        candidates,
        cascades: vec![],
        issues: issues.into_iter().collect(),
    })
}
fn append(
    frame: &ActivityFrameEvidence<'_>,
    point: &MysqlMetadataLockPoint,
    cases: &mut Vec<MetadataQueueCandidate>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Result<()> {
    let roots = point
        .tickets
        .iter()
        .filter(|ticket| ticket.actor.as_ref() == Some(&point.subject) && ticket.status == "PENDING")
        .collect::<Vec<_>>();
    let [root] = roots.as_slice() else {
        issues.insert(MetadataQueueIssue::UnknownModeOrEdge);
        return Ok(());
    };
    if !qualified(frame, root) || !root.known_mode() || !root.known_duration() {
        issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
        return Ok(());
    }
    let same = point.tickets.iter().filter(|ticket| ticket.resource == root.resource).collect::<Vec<_>>();
    let mut pending = BTreeMap::<ActivityKey, Vec<&MysqlMetadataLockTicket>>::new();
    let mut granted = BTreeMap::<ActivityKey, Vec<&MysqlMetadataLockTicket>>::new();
    for ticket in same {
        let Some(actor) = ticket.actor.as_ref() else {
            issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
            continue;
        };
        if actor == &point.subject {
            continue;
        }
        if !qualified(frame, ticket) {
            issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
            continue;
        }
        if !ticket.known_mode() || !ticket.known_duration() || !matches!(ticket.status.as_str(), "GRANTED" | "PENDING")
        {
            issues.insert(MetadataQueueIssue::UnknownModeOrEdge);
            continue;
        }
        if ticket.duration == "TRANSACTION" && ticket.transaction.as_ref().is_none_or(|event| !event.is_active()) {
            issues.insert(MetadataQueueIssue::NativeLifetimeNotRecorded);
        }
        match ticket.status.as_str() {
            "PENDING" if ticket.mode == "EXCLUSIVE" => pending.entry(actor.clone()).or_default().push(ticket),
            "GRANTED" => granted.entry(actor.clone()).or_default().push(ticket),
            _ => {}
        }
    }
    if pending.keys().any(|key| granted.contains_key(key)) {
        issues.insert(MetadataQueueIssue::MixedGrantedAndPending);
    }
    let holders = granted
        .into_iter()
        .filter(|(actor, _)| !pending.contains_key(actor))
        .map(|(actor, tickets)| actor_case(actor, &tickets))
        .collect::<Vec<_>>();
    let gates = pending.into_iter().map(|(actor, tickets)| actor_case(actor, &tickets)).collect::<Vec<_>>();
    if gates.is_empty() || holders.is_empty() {
        return Ok(());
    }
    cases.push(MetadataQueueCandidate {
        key: digest(
            &serde_json::to_vec(&(
                "metadata-ticket-candidate-v1",
                frame.source().captured_range()?.scope_id,
                &point.observed_at,
                &point.subject,
                &root.resource,
            ))
            .map_err(|_| invalid())?,
        ),
        observed_at: point.observed_at.clone(),
        resource: root.resource.clone(),
        affected: point.subject.clone(),
        requested_mode: root.mode.clone(),
        pending_exclusive: gates,
        granted_holders: holders,
    });
    Ok(())
}
fn qualified(frame: &ActivityFrameEvidence<'_>, ticket: &MysqlMetadataLockTicket) -> bool {
    ticket.is_qualified_actor()
        && ticket
            .actor
            .as_ref()
            .is_some_and(|actor| frame.row(actor).is_some() && frame.has_session_generation_authority(&actor.session))
}
fn actor_case(actor: ActivityKey, tickets: &[&MysqlMetadataLockTicket]) -> MetadataQueueCandidateActor {
    MetadataQueueCandidateActor {
        actor,
        modes: tickets.iter().map(|ticket| ticket.mode.clone()).collect::<BTreeSet<_>>().into_iter().collect(),
        durations: tickets.iter().map(|ticket| ticket.duration.clone()).collect::<BTreeSet<_>>().into_iter().collect(),
        requesting_events: tickets
            .iter()
            .filter_map(|ticket| ticket.owner_event_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}
