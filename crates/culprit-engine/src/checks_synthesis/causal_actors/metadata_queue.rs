//! Consume the one typed native proof. A holder's current SQL is never relabeled as acquiring SQL.
use super::*;

pub(super) fn blocking(original: &dyn OriginalProjection, report: &BlockingReport) -> Result<Actors> {
    let SourceObservation::FocusedBlocking { result, .. } = &original.source().observation else {
        return Err(invalid());
    };
    let mut actors = Actors::default();
    for case in &report.metadata_queue.evidence.cascades {
        let Some(affected) = case.affected.iter().find(|affected| affected.actor == result.focus.original.key) else {
            continue;
        };
        let affected_position = position(original, &affected.actor)?;
        let gate_position = position(original, &case.gatekeeper)?;
        let mut gate = queries::from_record(original, gate_position)?.ok_or_else(invalid)?;
        if matches!(&gate.identity, Some(DerivedQueryIdentity::Activity { statement: None, .. })) {
            gate.sql = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
        }
        gate.causal_role = Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativePendingRelationLockGatekeeper));
        gate.evidence.push(original.reference(affected_position)?);
        if !gate.is_valid() {
            return Err(invalid());
        }
        actors.culprits.push(gate);
        for holder in &case.holders {
            let holder_position = position(original, &holder.actor)?;
            let TelemetryRecord::Activity(point) = &original.records()[holder_position].row().record else {
                return Err(invalid());
            };
            let caller = DerivedCaller::from_activity(point, &original.reference(holder_position)?)?;
            let mut held = actor(
                original,
                holder_position,
                DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
                    id: holder.actor.session.id.clone(),
                    generation: Some(holder.actor.session.generation.clone()),
                }),
                DerivedCausalRoleKind::NativeGrantedRelationLockHolder,
                caller,
            )?;
            held.evidence.push(original.reference(gate_position)?);
            actors.culprits.push(held);
            if point.key.request.is_some()
                && let Some(current) = queries::from_record(original, holder_position)?
                && current.sql.is_available()
            {
                actors.candidates.push(current);
            }
        }
    }
    actors.culprits = incidents::distinct(actors.culprits)?;
    actors.candidates = incidents::distinct(actors.candidates)?;
    Ok(actors)
}
pub(super) fn already_qualified_gate(actors: &Actors, owner: &ActivityWaitOwner) -> bool {
    let Some(session) = owner.session() else {
        return false;
    };
    actors.culprits.iter().any(|query| {
        matches!(&query.causal_role,Some(role) if role.kind==DerivedCausalRoleKind::NativePendingRelationLockGatekeeper)
            && matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session==session)
    })
}
fn position(original: &dyn OriginalProjection, key: &ActivityKey) -> Result<usize> {
    original
        .records()
        .iter()
        .position(|row| matches!(&row.row().record,TelemetryRecord::Activity(point) if &point.key==key))
        .ok_or_else(invalid)
}
