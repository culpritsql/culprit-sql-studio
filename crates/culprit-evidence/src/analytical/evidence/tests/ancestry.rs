use super::*;
use culprit_core::{DiagnosticAncestryRole, DiagnosticAncestryTarget};
#[test]
fn shared_canonical_history_uses_one_walk_for_temporary_answers_recovery_and_investigation_execution_owners() {
    let mut cases = test_support::IncidentHistoryCase::ALL
        .iter()
        .map(|case| test_support::incident_history_evidence(*case))
        .collect::<Vec<_>>();
    cases.push(test_support::shared_investigation_evidence());
    let mut answer = false;
    let mut result = false;
    let mut recovery = false;
    let mut shared_owner = false;
    for (_, records) in cases {
        for record in records {
            let TelemetryRecord::Operational(observation) = record else {
                continue;
            };
            let OperationalFact::DiagnosticRevision(revision) = observation.fact else {
                continue;
            };
            assert!(revision.is_valid());
            let links = revision.referenced_evidence().unwrap();
            for link in links {
                match link.target {
                    DiagnosticAncestryTarget::Record(reference) => assert!(reference.is_valid()),
                    DiagnosticAncestryTarget::Capture(capture) => assert!(capture.is_valid()),
                }
                answer |= link.role == DiagnosticAncestryRole::AnswerEvidence;
                result |= link.role == DiagnosticAncestryRole::SourceResult;
                recovery |= link.role == DiagnosticAncestryRole::Recovery;
                shared_owner |= link.role == DiagnosticAncestryRole::SharedExecutionOwner;
            }
        }
    }
    assert!(
        answer && result && recovery && shared_owner,
        "the shared typed corpus reaches all explicit ancestry channels"
    );
}
