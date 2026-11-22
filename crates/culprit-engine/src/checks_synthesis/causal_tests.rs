use super::*;
use crate::collected_evidence::RetainedEvidenceFixture as Fixture;
use culprit_evidence::analytical::{
    evidence::EvidenceFamily,
    test_support::{self, BlockingCase, GrantOutputCase, PageTaskHolderCase},
};

#[tokio::test]
async fn exact_native_blocking_owner_is_established_while_its_current_sql_remains_separate_context() {
    for case in
        [BlockingCase::Matched, BlockingCase::IdleOwner, BlockingCase::RequestChanged, BlockingCase::RequestMissing]
    {
        let (source, records) = test_support::blocking_evidence(case);
        let fixture = Fixture::new(source, records).await;
        let captured = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        let original = captured.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert_eq!(explanation.attribution_status(), DerivedAttributionStatus::Established, "{case:?}");
        assert_eq!(explanation.culprits.len(), 1);
        let culprit = &explanation.culprits[0];
        assert_eq!(culprit.causal_role.as_ref().unwrap().kind, DerivedCausalRoleKind::NativeBlockingOwner);
        assert!(
            matches!(&culprit.identity, Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session { id, .. })) if id == "43")
        );
        assert!(!culprit.sql.is_available(), "current/last SQL never becomes the statement that acquired the lock");
        assert!(culprit.is_valid());
        assert!(explanation.candidates.iter().all(|candidate| candidate.causal_role.is_none()));
        assert_eq!(explanation.candidates.len(), usize::from(case != BlockingCase::IdleOwner));
        assert!(!explanation.limitations.is_empty());
    }
}
#[tokio::test]
async fn victim_only_unknown_owner_zero_transaction_or_unverified_original_never_establishes_a_culprit() {
    for case in [
        BlockingCase::HiddenOwner,
        BlockingCase::PreparedOwner,
        BlockingCase::UnverifiedOriginal,
        BlockingCase::ParticipantGone,
        BlockingCase::Unavailable,
    ] {
        let (source, records) = test_support::blocking_evidence(case);
        let fixture = Fixture::new(source, records).await;
        let captured = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        let original = captured.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert!(explanation.culprits.is_empty(), "{case:?}");
        assert_ne!(explanation.attribution_status(), DerivedAttributionStatus::Established, "{case:?}");
    }
}
#[tokio::test]
async fn native_reported_task_holder_survives_unavailable_sql_without_promoting_a_writer_or_changed_target() {
    for (case, expected) in [
        (PageTaskHolderCase::QualifiedBoth, 1),
        (PageTaskHolderCase::QualifiedOne, 1),
        (PageTaskHolderCase::DifferentHolders, 2),
        (PageTaskHolderCase::SqlUnavailable, 1),
        (PageTaskHolderCase::OversizedSql, 1),
        (PageTaskHolderCase::TaskOnly, 0),
        (PageTaskHolderCase::OtherDatabase, 0),
        (PageTaskHolderCase::ChangedRequest, 0),
        (PageTaskHolderCase::ChangedPage, 0),
        (PageTaskHolderCase::Ambiguous, 0),
    ] {
        let (source, records) = test_support::page_task_holder_evidence(case);
        let fixture = Fixture::new(source, records).await;
        let captured = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
        let original = captured.verified_original().unwrap();
        let explanation = explanations::holders(&original, 0).unwrap();
        assert_eq!(explanation.culprits.len(), expected, "{case:?}");
        for culprit in &explanation.culprits {
            assert_eq!(culprit.causal_role.as_ref().unwrap().kind, DerivedCausalRoleKind::NativeReportedTaskHolder);
            assert!(culprit.is_valid());
            assert!(!culprit.sql.is_available());
            assert!(matches!(culprit.identity, Some(DerivedQueryIdentity::Activity { statement: None, .. })));
        }
        assert!(!explanation.limitations.is_empty());
    }
}
#[tokio::test]
async fn canonical_same_resource_reservation_is_an_explicit_contributor_and_unrelated_mars_peer_is_withheld() {
    for (case, expected) in [
        (GrantOutputCase::Qualified, 1),
        (GrantOutputCase::SqlUnavailable, 1),
        (GrantOutputCase::SharedLoginEstablished, 1),
        (GrantOutputCase::NegativeMarsPeer, 0),
        (GrantOutputCase::MissingStatementIdentity, 0),
        (GrantOutputCase::MissingResource, 0),
        (GrantOutputCase::UnknownOwner, 0),
    ] {
        let (source, records) = test_support::grant_output_evidence(case);
        let fixture = Fixture::new(source, records).await;
        let captured = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
        let original = captured.verified_original().unwrap();
        let waiter = original
            .records()
            .iter()
            .position(|record| {
                matches!(&record.row().record,
            TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::ExecutionGrant(grant), .. })
            if grant.state == ExecutionGrantState::Waiting)
            })
            .unwrap();
        let explanation = explanations::request_memory(&original, waiter).unwrap();
        assert_eq!(explanation.culprits.len(), expected, "{case:?}");
        for culprit in &explanation.culprits {
            assert_eq!(
                culprit.causal_role.as_ref().unwrap().kind,
                DerivedCausalRoleKind::AdmissionReservationContributor
            );
            assert!(culprit.is_valid());
            assert!(!culprit.sql.is_available());
            assert_ne!(culprit.evidence, vec![original.reference(waiter).unwrap()]);
        }
        assert!(explanation.candidates.iter().all(|query| query.causal_role.is_none()));
        assert!(!explanation.limitations.is_empty());
    }
}
