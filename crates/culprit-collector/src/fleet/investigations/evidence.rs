//! Optional native questions wait for absent sealed evidence; they never repair corrupt evidence or acknowledge it.
use crate::delivery::recovery::RecoveryStore;
use crate::diagnostics::resolve_evidence;
use crate::outbox::Outbox;
use culprit_core::{Error, ErrorDetail, Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;

pub(super) async fn resolve_optional_evidence(
    outbox: &mut Outbox,
    connection: &str,
    reference: &DiagnosticEvidenceRef,
    recovery: Option<&dyn RecoveryStore>,
    cancellation: &CollectionCancellation,
) -> Result<Option<(CollectionDescription, ExportedRecord)>> {
    match resolve_evidence(outbox, connection, reference, recovery, cancellation).await {
        Err(error) if evidence_unavailable(&error) => Ok(None),
        evidence => evidence,
    }
}

pub(super) fn evidence_unavailable(error: &Error) -> bool {
    matches!(
        error.detail,
        Some(ErrorDetail::Outbox(OutboxProblem::MissingCapture))
            | Some(ErrorDetail::Delivery(DeliveryProblem::NotReady))
    )
}

/// One outcome owner keeps unconfirmed/missing evidence distinct from an actual verifier refusal.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ExecutionAdmissionStatus {
    Verified,
    EvidenceUnavailable,
    Refused(DiagnosticAnalysisIssue),
}

/// Resolve native-cost evidence only for measured authority. Built-in safety is revalidated by the selected
/// trusted source registration, with the same authenticated parent/focus and native capture guards.
pub(super) async fn verify_execution_admission(
    backend: &dyn crate::fleet::FleetBackend,
    outbox: &mut Outbox,
    connection: &str,
    admission: &RecipeExecutionAdmission,
    recovery: Option<&dyn RecoveryStore>,
    cancellation: &CollectionCancellation,
) -> Result<ExecutionAdmissionStatus> {
    if cancellation.is_cancelled() || !admission.is_valid() {
        return Ok(ExecutionAdmissionStatus::EvidenceUnavailable);
    }
    match admission {
        RecipeExecutionAdmission::BuiltinSafety { policy, .. } => {
            backend.verify_builtin_safety(connection, policy).map(|verified| {
                if verified {
                    ExecutionAdmissionStatus::Verified
                } else {
                    ExecutionAdmissionStatus::Refused(DiagnosticAnalysisIssue::PolicyChanged)
                }
            })
        }
        RecipeExecutionAdmission::Measured { receipt } => {
            let Some((source, row)) =
                resolve_optional_evidence(outbox, connection, &receipt.evidence, recovery, cancellation).await?
            else {
                return Ok(ExecutionAdmissionStatus::EvidenceUnavailable);
            };
            if cancellation.is_cancelled() {
                return Ok(ExecutionAdmissionStatus::EvidenceUnavailable);
            }
            backend.verify_cost(connection, receipt, &source, &row).map(|verified| {
                if verified {
                    ExecutionAdmissionStatus::Verified
                } else {
                    ExecutionAdmissionStatus::Refused(DiagnosticAnalysisIssue::CostUnqualified)
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::investigations::tests::support::Harness;
    use std::sync::atomic::Ordering;
    #[tokio::test]
    async fn a_false_builtin_verifier_is_a_policy_refusal_while_measured_cost_refusal_keeps_its_real_meaning() {
        for builtin in [false, true] {
            let mut harness = Harness::new(1).await;
            if builtin {
                let recipe = &harness.backend.recipe;
                harness.backend.recipe.admission = RecipeAdmission::BuiltinSafety(Box::new(BuiltinRecipeSafety {
                    id: "fixture.source-safety".into(),
                    version: 1,
                    recipe_id: recipe.id.clone(),
                    recipe_version: recipe.version,
                    purpose: recipe.purpose,
                    selector: recipe.selector,
                    component: harness.backend.component.clone(),
                    templates_sha256: digest(b"actual-controlled-reader"),
                    budget: recipe.budget.clone(),
                }));
            }
            let candidate = harness.prepare(&CollectionCancellation::default()).await.unwrap().pop().unwrap();
            harness.backend.accept = false;
            let result = verify_execution_admission(
                &harness.backend,
                &mut harness.outbox,
                "saved-source",
                &candidate.intent.qualification,
                None,
                &CollectionCancellation::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                ExecutionAdmissionStatus::Refused(if builtin {
                    DiagnosticAnalysisIssue::PolicyChanged
                } else {
                    DiagnosticAnalysisIssue::CostUnqualified
                })
            );
            assert_eq!(candidate.intent.qualification.cost_evidence().is_none(), builtin);
            let before = (
                harness.backend.verified.load(Ordering::SeqCst),
                harness.backend.builtin_verified.load(Ordering::SeqCst),
            );
            let cancellation = CollectionCancellation::default();
            cancellation.cancel();
            assert_eq!(
                verify_execution_admission(
                    &harness.backend,
                    &mut harness.outbox,
                    "saved-source",
                    &candidate.intent.qualification,
                    None,
                    &cancellation
                )
                .await
                .unwrap(),
                ExecutionAdmissionStatus::EvidenceUnavailable
            );
            assert_eq!(
                before,
                (
                    harness.backend.verified.load(Ordering::SeqCst),
                    harness.backend.builtin_verified.load(Ordering::SeqCst)
                )
            );
            assert_eq!(harness.backend.calls.captured.load(Ordering::SeqCst), 0);
            assert_eq!(harness.backend.calls.opened.load(Ordering::SeqCst), 0);
        }
    }
}
