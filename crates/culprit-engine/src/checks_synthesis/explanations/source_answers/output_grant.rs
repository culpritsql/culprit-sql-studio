//! A held reservation is explained once by the memory interpreter; it does not establish a client/network culprit.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &OutputGrantRetentionAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let mut matching = originals.iter().copied().filter(|original| original.original() == &answer.result);
    let Some(original) = matching.next() else {
        explanation.limitations.push(culprit_core::t!("view.output_grant_retention.limit.read"));
        return Ok(());
    };
    if matching.next().is_some() {
        return Err(invalid());
    }
    let read = original.operational_read()?;
    let coverage = source(&read, &read.description.source)?;
    let report = culprit_checks::output_grant_retention::explain(answer, &read, Some(&coverage))?;
    explanation.mechanism = Some(report.summary);
    explanation.action = Some(report.investigation);
    explanation.verify = Some(report.verify);
    explanation.limitations.extend(report.limitations);
    if report.status != OutputGrantRetentionStatus::Observed {
        return Ok(());
    }
    let Some(subject_key) = &report.subject_key else { return Err(invalid()) };
    let resources = report
        .memory
        .admission_resources
        .iter()
        .filter(|resource| resource.participants.iter().any(|participant| &participant.observation_key == subject_key))
        .collect::<Vec<_>>();
    for (position, row) in original.records().iter().enumerate() {
        let TelemetryRecord::Operational(observation) = &row.row().record else { return Err(invalid()) };
        let OperationalFact::ExecutionGrant(grant) = &observation.fact else { return Err(invalid()) };
        if &observation.key == subject_key
            || grant.state != ExecutionGrantState::Granted
            || !grant
                .granted_kibibytes
                .as_deref()
                .is_some_and(|value| value.parse::<u64>().is_ok_and(|value| value > 0))
            || !resources.iter().any(|resource| {
                resource.participants.iter().any(|participant| participant.observation_key == observation.key)
            })
        {
            continue;
        }
        let Some(entry) = report.memory.entries.iter().find(|entry| entry.observation_key == observation.key) else {
            continue;
        };
        let Some(actor) = grant.request.as_ref().filter(|actor| entry.qualified_request.as_ref() == Some(&actor.key))
        else {
            continue;
        };
        explanation.candidates.push(queries::from_qualified_request(actor, original.reference(position)?));
    }
    // These are concurrent reservations, not causal actors for the original request's output wait.
    explanation.candidates = super::super::super::incidents::distinct(std::mem::take(&mut explanation.candidates))?;
    Ok(())
}
