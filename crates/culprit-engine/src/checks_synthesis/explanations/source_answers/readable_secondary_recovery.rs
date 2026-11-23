//! Exact native recovery blocking identifies a session role; the observed SQL remains a separate candidate.
use super::*;
pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &ReadableSecondaryRecoveryAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    let selected = answer.observation.as_ref().map(|reference| resolve(originals, reference)).transpose()?.flatten();
    let point = selected
        .map(|(original, position)| {
            if original.original() != &answer.result
                || original.source().observation.execution_context() != Some(&answer.result_context)
            {
                return Err(invalid());
            }
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::ReadableSecondaryRecovery(point),
                ..
            }) = &original.records()[position].row().record
            else {
                return Err(invalid());
            };
            if !point.fits_source(original.source()) {
                return Err(invalid());
            }
            Ok(point.as_ref())
        })
        .transpose()?;
    let report = culprit_checks::readable_secondary_recovery::explain(answer, point)?;
    explanation.mechanism = Some(report.summary);
    explanation.action = Some(report.investigation);
    explanation.verify = Some(report.verify);
    for limitation in report.limitations {
        if !explanation.limitations.contains(&limitation) {
            explanation.limitations.push(limitation);
        }
    }
    if let (Some(point), Some(reference)) = (point, &answer.observation) {
        for reader in report.readers {
            let role = match reader.role {
                RecoveryReaderRole::GrantedRelationHolder => DerivedCausalRoleKind::NativeGrantedRelationLockHolder,
                RecoveryReaderRole::NativeRedoBlocker => DerivedCausalRoleKind::NativeBlockingOwner,
            };
            explanation.culprits.push(DerivedIncidentQuery {
                causal_role: Some(DerivedCausalRole::new(role)),
                identity: Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
                    id: reader.session.id,
                    generation: Some(reader.session.generation),
                })),
                sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
                caller: None,
                evidence: vec![reference.clone(), answer.basis.clone()],
            });
        }
        // It is the current request context, never the SQL that acquired a blocking lock or caused cancellation.
        if point.membership == FocusedRequestMembership::Observed {
            explanation.candidates.push(queries::from_qualified_request(&point.focus.original, answer.basis.clone()));
        }
    }
    Ok(())
}
