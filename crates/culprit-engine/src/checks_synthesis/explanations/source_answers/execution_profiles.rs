//! Matched current-execution context belongs to the affected/candidate request. It never becomes a culprit.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &CurrentExecutionProfileAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let Some((original, position)) = resolve(originals, &answer.observation)? else { return Ok(()) };
    if original.original() != &answer.result
        || original.source().observation.execution_context() != Some(&answer.result_context)
    {
        return Err(invalid());
    }
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &original.records()[position].row().record
    else {
        return Err(invalid());
    };
    let report = culprit_checks::current_execution_profile::explain(answer, point)?;
    explanation.mechanism =
        Some(std::iter::once(report.summary).chain(report.mechanisms).collect::<Vec<_>>().join(" "));
    explanation.action = Some(report.investigation);
    explanation.verify = Some(report.verify);
    explanation.limitations.extend(report.limitations);
    if point.binding == FocusedRequestBinding::Matched && point.membership == FocusedRequestMembership::Observed {
        let candidate = DerivedIncidentQuery {
            causal_role: None,
            identity: Some(DerivedQueryIdentity::Activity {
                actor: point.focus.original.key.clone(),
                statement: Some(point.focus.statement_identity.clone()),
            }),
            sql: point.statement.clone(),
            // Caller remains the database-reported label on the earlier exact original, at that clock.
            caller: queries::qualified_caller(&point.focus.original, &answer.basis),
            evidence: vec![answer.basis.clone(), answer.observation.clone()],
        };
        explanation.affected.push(candidate.clone());
        explanation.candidates.push(candidate);
    }
    Ok(())
}
