use super::*;
use culprit_core::{ComponentVersion, telemetry::DeliveryProblem};
use culprit_evidence::{
    analytical::{
        evidence::EvidenceReader,
        indexes::ProjectionIndexes,
        test_support::{self, ReadOnlyFixture},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::RecoveryRead},
};
use std::sync::Arc;
async fn service() -> (EvidenceService, ReadOnlyFixture) {
    let (source, rows) = test_support::informational_query_evidence();
    let fixture = ReadOnlyFixture::capture(source, rows).await;
    let service = EvidenceService {
        recovery: fixture.transport(),
        contract: fixture.contract.clone(),
        reader: EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport()))),
    };
    (service, fixture)
}
fn policy() -> QueryHelperPolicy {
    QueryHelperPolicy {
        name: "Pilot".into(),
        analyzer: ComponentVersion::package("culprit-checks", "1", None),
        rules_version: "rules-2".into(),
        history_from: "2019-01-01T00:00:00Z".into(),
    }
}
#[tokio::test]
async fn configured_helpers_use_the_real_root_and_same_action_proof_allowance_without_source_or_payload_reads() {
    let (service, fixture) = service().await;
    let root = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    let token = CollectionCancellation::default();
    let mut session = service.session(EvidenceReadBudget::default(), &token).unwrap();
    let bound = session.query_helper_binding(policy()).await.unwrap();
    assert_eq!(bound.recovery_store_id, root.id());
    assert_eq!(bound.contract_id, fixture.contract.id().unwrap());
    assert!(session.completed_metrics().proof_bytes > 0);
    assert_eq!(session.completed_metrics().queries, 0);
    assert_eq!(session.completed_metrics().selected_rows, 0);
    assert_eq!(session.completed_metrics().recovery_objects, 0);
    assert!(
        session.read_metadata(&fixture.manifest.range_id).await.unwrap().is_read_complete(),
        "binding and subsequent metadata share the same selected namespace/session"
    );
}
#[tokio::test]
async fn missing_foreign_or_tampered_root_never_yields_a_query_binding_and_failed_action_cannot_retry() {
    for foreign in [false, true] {
        let (service, fixture) = service().await;
        {
            let mut stored = fixture.objects.objects.lock().unwrap();
            if foreign {
                let value = stored.get_mut("recovery.json").unwrap();
                let mut root: serde_json::Value = serde_json::from_slice(value).unwrap();
                root["contract"]["pipeline"] = serde_json::json!("foreign");
                *value = serde_json::to_vec(&root).unwrap();
            } else {
                stored.remove("recovery.json");
            }
        }
        let token = CollectionCancellation::default();
        let mut session = service.session(EvidenceReadBudget::default(), &token).unwrap();
        assert!(session.query_helper_binding(policy()).await.is_err());
        let error = session.query_helper_binding(policy()).await.unwrap_err();
        assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    }
}
#[tokio::test]
async fn cancelled_or_exhausted_metadata_budget_does_not_return_even_an_offline_binding() {
    let (service, _) = service().await;
    let token = CollectionCancellation::default();
    token.cancel();
    assert!(service.query_helper_binding(policy(), EvidenceReadBudget::default(), &token).await.is_err());
    let budget = EvidenceReadBudget { max_proof_bytes: 1, ..Default::default() };
    assert!(service.query_helper_binding(policy(), budget, &CollectionCancellation::default()).await.is_err());
}
