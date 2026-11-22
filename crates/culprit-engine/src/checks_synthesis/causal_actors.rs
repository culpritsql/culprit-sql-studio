//! Project existing native causal roles. A relationship proves neither exclusive cause nor acquiring SQL.
use super::*;
mod metadata_queue;
use culprit_core::page_wait_report::{PageTaskHolderExplanation, PageTaskHolderReport};

#[derive(Default)]
pub(super) struct Actors {
    pub culprits: Vec<DerivedIncidentQuery>,
    pub candidates: Vec<DerivedIncidentQuery>,
}

pub(super) fn blocking(original: &dyn OriginalProjection, report: &BlockingReport) -> Result<Actors> {
    let SourceObservation::FocusedBlocking { result, .. } = &original.source().observation else {
        return Err(invalid());
    };
    let mut actors = metadata_queue::blocking(original, report)?;
    if result.binding != FocusedRequestBinding::Matched
        || result.membership != FocusedRequestMembership::Observed
        || !report.context.has_qualified_local_owner()
    {
        return Ok(actors);
    }
    let waiter = &result.focus.original.key;
    let Some(position) = original
        .records()
        .iter()
        .position(|row| matches!(&row.row().record, TelemetryRecord::Activity(point) if point.key == *waiter))
    else {
        return Err(invalid());
    };
    for relationship in &report.relationships {
        if &relationship.waiter != waiter
            || relationship.reports_waiter_session_id()
            || metadata_queue::already_qualified_gate(&actors, &relationship.owner)
        {
            continue;
        }
        let known = match &relationship.owner {
            ActivityWaitOwner::Session { .. } => relationship.owner_observed,
            ActivityWaitOwner::Transaction { native_id, .. } => native_id != "0",
            ActivityWaitOwner::Unknown { .. } => false,
        };
        if !known {
            continue;
        }
        let mut caller = None;
        if let Some(session) = relationship.owner.session() {
            let owner_rows = original
                .records()
                .iter()
                .enumerate()
                .filter_map(|(position, row)| match &row.row().record {
                    TelemetryRecord::Activity(point) if point.key.session == session => {
                        Some((position, point.as_ref()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let [(position, point)] = owner_rows.as_slice() {
                caller = DerivedCaller::from_activity(point, &original.reference(*position)?)?;
                if let Some(current) = queries::from_record(original, *position)?
                    && current.sql.is_available()
                    && point.key.request.is_some()
                {
                    actors.candidates.push(current);
                }
            }
        }
        actors.culprits.push(actor(
            original,
            position,
            DerivedQueryIdentity::NativeWaitOwner(relationship.owner.clone()),
            DerivedCausalRoleKind::NativeBlockingOwner,
            caller,
        )?);
    }
    actors.culprits = incidents::distinct(actors.culprits)?;
    actors.candidates = incidents::distinct(actors.candidates)?;
    Ok(actors)
}

pub(super) fn holder(
    original: &dyn OriginalProjection,
    position: usize,
    report: &PageTaskHolderReport,
    entry: &PageTaskHolderExplanation,
) -> Result<Option<DerivedIncidentQuery>> {
    if report.target_state != Some(PageWriterTargetState::CurrentMapped)
        || entry.request_state != PageWaitRequestState::SameStatement
        || entry.wait_state != PageWaitRelation::OriginalObserved
        || entry.relation != PageTaskHolderRelation::QualifiedRequest
    {
        return Ok(None);
    }
    let Some(request) = &entry.request else { return Err(invalid()) };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PageTaskHolders(observation),
        ..
    }) = &original.records().get(position).ok_or_else(invalid)?.row().record
    else {
        return Err(invalid());
    };
    let point = match entry.bound {
        culprit_core::page_wait_report::PageTaskHolderBound::Before => &observation.before,
        culprit_core::page_wait_report::PageTaskHolderBound::After => &observation.after,
    };
    let native = point.holder.request.as_ref().filter(|native| native.key == *request).ok_or_else(invalid)?;
    let caller = queries::qualified_caller(native, &original.reference(position)?);
    actor(
        original,
        position,
        DerivedQueryIdentity::Activity { actor: request.clone(), statement: None },
        DerivedCausalRoleKind::NativeReportedTaskHolder,
        caller,
    )
    .map(Some)
}

pub(super) fn reservation(
    original: &dyn OriginalProjection,
    position: usize,
    entry: &MemoryParticipantExplanation,
    request: &ActivityObservation,
) -> Result<Option<DerivedIncidentQuery>> {
    // Caller already selected the canonical same-resource roster containing the affected waiter.
    if entry.mechanism != Some(MemoryParticipantMechanism::ReservationDuringClientOutputWait)
        || entry.qualified_request.as_ref() != Some(&request.key)
    {
        return Ok(None);
    }
    actor(
        original,
        position,
        DerivedQueryIdentity::Activity { actor: request.key.clone(), statement: None },
        DerivedCausalRoleKind::AdmissionReservationContributor,
        queries::qualified_caller(request, &original.reference(position)?),
    )
    .map(Some)
}

fn actor(
    original: &dyn OriginalProjection,
    position: usize,
    identity: DerivedQueryIdentity,
    kind: DerivedCausalRoleKind,
    caller: Option<DerivedCaller>,
) -> Result<DerivedIncidentQuery> {
    let query = DerivedIncidentQuery {
        causal_role: Some(DerivedCausalRole::new(kind)),
        identity: Some(identity),
        sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        caller,
        evidence: vec![original.reference(position)?],
    };
    query.is_valid().then_some(query).ok_or_else(invalid)
}
