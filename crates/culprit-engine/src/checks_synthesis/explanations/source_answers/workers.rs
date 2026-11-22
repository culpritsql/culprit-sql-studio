//! Select only checked actors from the retained native worker point. SQL and caller labels remain context.
use super::*;

pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &WorkerStarvationAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let Some((original, position)) = resolve(originals, &answer.point)? else { return Ok(()) };
    if original.original() != &answer.result
        || original.source().observation.execution_context() != Some(&answer.result_context)
    {
        return Err(invalid());
    }
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlWorkerStarvation(point),
        ..
    }) = &original.records()[position].row().record
    else {
        return Err(invalid());
    };
    let report = culprit_checks::worker_starvation::explain(answer, point)?;
    explanation.mechanism =
        Some(std::iter::once(report.summary).chain(report.mechanisms).collect::<Vec<_>>().join(" "));
    explanation.action = Some(report.investigation);
    explanation.verify = Some(report.verify);
    explanation.limitations.extend(report.limitations);
    append_affected(point, answer, explanation)?;
    append_contributors(point, answer, explanation)?;
    append_blocking_owners(point, answer, explanation)?;
    explanation.affected = super::super::super::incidents::distinct(std::mem::take(&mut explanation.affected))?;
    explanation.culprits = super::super::super::incidents::distinct(std::mem::take(&mut explanation.culprits))?;
    Ok(())
}

fn shared_request(request: &ActivityObservation, answer: &WorkerStarvationAnswer) -> Result<DerivedIncidentQuery> {
    let mut query = queries::from_qualified_request(request, answer.point.clone());
    query.identity = Some(DerivedQueryIdentity::SharedActivity {
        member_id: answer.worker_scope.telemetry_id()?,
        member_kind: answer.worker_scope.kind,
        actor: request.key.clone(),
        statement: request.statement_identity.clone(),
        database: request.database.clone(),
    });
    Ok(query)
}

fn append_affected(
    point: &MssqlWorkerStarvationPoint,
    answer: &WorkerStarvationAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    for actor in answer.evidence.affected_requests.iter().take(MAX_PRESENTED_WORKER_ACTORS) {
        let request = point.qualified_request(actor).ok_or_else(invalid)?;
        explanation.affected.push(shared_request(request, answer)?);
    }
    Ok(())
}

fn append_contributors(
    point: &MssqlWorkerStarvationPoint,
    answer: &WorkerStarvationAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let mut selected = std::collections::BTreeSet::new();
    for contribution in &answer.evidence.contributions {
        if selected.contains(&contribution.request) {
            continue;
        }
        if selected.len() == MAX_PRESENTED_WORKER_ACTORS {
            break;
        }
        selected.insert(contribution.request.clone());
        let request = point.qualified_request(&contribution.request).ok_or_else(invalid)?;
        explanation.candidates.push(shared_request(request, answer)?);
        // Native assignment to a scheduler with current queue evidence is a resource contribution. Affected
        // request identity or a parallel-task count alone never gives this causal role or acquiring SQL.
        if !answer.evidence.contributions.iter().any(|row| {
            row.request == request.key
                && row.queue_scheduler_id.is_some()
                && matches!(
                    row.kind,
                    WorkerContributionKind::AssignedParallelWorkers | WorkerContributionKind::PreemptiveAssignedWorkers
                )
        }) || request.waiting_on_lock == Some(true)
            || answer.evidence.affected_requests.contains(&request.key)
        {
            continue;
        }
        explanation.culprits.push(DerivedIncidentQuery {
            causal_role: Some(DerivedCausalRole::new(DerivedCausalRoleKind::AssignedWorkerContributor)),
            identity: Some(DerivedQueryIdentity::SharedActivity {
                member_id: answer.worker_scope.telemetry_id()?,
                member_kind: answer.worker_scope.kind,
                actor: request.key.clone(),
                statement: None,
                database: request.database.clone(),
            }),
            sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            caller: queries::qualified_caller(request, &answer.point),
            evidence: vec![answer.point.clone()],
        });
    }
    Ok(())
}

fn append_blocking_owners(
    point: &MssqlWorkerStarvationPoint,
    answer: &WorkerStarvationAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let mut owners = std::collections::BTreeSet::new();
    for edge in &answer.evidence.blocking {
        let key = serde_json::to_vec(&edge.owner).map_err(|_| invalid())?;
        if owners.contains(&key) {
            continue;
        }
        if owners.len() == MAX_PRESENTED_WORKER_ACTORS {
            break;
        }
        owners.insert(key);
        let Some(session) = edge.owner.session() else { return Err(invalid()) };
        let owners = point
            .requests
            .iter()
            .filter(|row| row.is_qualified() && row.observation.key.session == session)
            .collect::<Vec<_>>();
        let caller = match owners.as_slice() {
            [owner] => queries::qualified_caller(&owner.observation, &answer.point),
            _ => None,
        };
        explanation.culprits.push(DerivedIncidentQuery {
            causal_role: Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner)),
            identity: Some(DerivedQueryIdentity::NativeWaitOwner(edge.owner.clone())),
            sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            caller,
            evidence: vec![answer.point.clone()],
        });
    }
    Ok(())
}
