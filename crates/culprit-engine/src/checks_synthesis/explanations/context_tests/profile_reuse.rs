//! Shared Gold and incident history interpret the same verified originals; no new warehouse/native reader.
use super::*;
use test_support::ExecutionProfileCase;

#[tokio::test]
async fn authenticated_shared_context_is_explicit_in_history_gold_and_source_free_developer_fields() {
    for case in [ExecutionProfileCase::Observed, ExecutionProfileCase::Unavailable] {
        let (basis, work, profile, history) = execution_profiles::captures_with_input_context(case, 896_001).await;
        let expected = profile.verified_original().unwrap().reference(0).unwrap();
        let diagnosis = project(&[&history, &basis, &work, &profile], vec![]).unwrap();
        diagnosis.validate().unwrap();
        let incident = &diagnosis.incidents[0];
        assert!(incident.entry.reasons.contains(&t!("view.execution_profile.reused_input_context")));
        assert!(!incident.entry.reasons.contains(&t!("view.incident_history.source_stage", state = "deferred")));
        assert!(incident.entry.evidence.iter().any(|link| {
            link.reference.as_ref() == Some(&expected) && link.label == t!("view.execution_profile.reuse_evidence")
        }));
        assert!(!incident.affected_queries.is_empty());
        assert!(
            incident.culprit_queries.is_empty(),
            "unknown input kind and shared acquisition are not a culprit verdict"
        );
        assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
        let read: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&diagnosis).unwrap()).unwrap();
        assert_eq!(read, diagnosis);
    }
}
#[tokio::test]
async fn missing_profile_original_never_uses_recorded_reuse_to_fabricate_runtime_or_culprit_evidence() {
    let (basis, work, _profile, history) =
        execution_profiles::captures_with_input_context(ExecutionProfileCase::Observed, 896_021).await;
    let diagnosis = project(&[&history, &basis, &work], vec![]).unwrap();
    let incident = &diagnosis.incidents[0];
    assert!(incident.culprit_queries.is_empty());
    assert!(!incident.mechanism.as_ref().is_some_and(|text| text.contains("9007199254740993")));
    assert!(
        incident.entry.reasons.contains(&t!("view.execution_profile.reused_input_context")),
        "recorded acquisition and current raw-input availability are different"
    );
}
