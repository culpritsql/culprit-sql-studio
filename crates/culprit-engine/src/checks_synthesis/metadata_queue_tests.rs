//! Actual canonical replay reaches the same typed report and Gold role adapter, without a provider.
use super::*;
use crate::collected_evidence::RetainedEvidenceFixture as Fixture;
use culprit_core::blocking::{MetadataQueueIssue, MetadataQueueStatus};
use culprit_evidence::analytical::{
    evidence::EvidenceFamily,
    test_support::{self, MetadataQueueCase},
};

#[tokio::test]
async fn canonical_pending_gate_and_granted_holder_have_distinct_roles_sql_callers_and_original_refs() {
    for case in [MetadataQueueCase::Observed, MetadataQueueCase::MissingSql, MetadataQueueCase::IdleHolder] {
        let (source, records) = test_support::metadata_queue_evidence(case);
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        let opened = fixture.recovery_object_reads();
        let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        test_support::assert_blocking_capture(&capture, &source, &records);
        let original = capture.verified_original().unwrap();
        let report = crate::collected_resources::blocking(&capture).unwrap().report;
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Observed);
        let proof = &report.metadata_queue.evidence.cascades[0];
        assert_eq!(proof.gatekeeper.session.id, "43");
        assert_eq!(proof.holders[0].actor.session.id, "44");
        assert_eq!(proof.affected[0].actor.session.id, "42");
        assert_eq!(proof.holders[0].granted_modes, ["AccessShareLock"]);
        assert_eq!(proof.requested_mode, "AccessExclusiveLock");
        let explanation = explanations::blocking(&original).unwrap();
        assert_eq!(explanation.attribution_status(), DerivedAttributionStatus::Established);
        assert_eq!(explanation.culprits.len(), 2, "no duplicate generic gate owner");
        let gate = explanation
            .culprits
            .iter()
            .find(|query| {
                query.causal_role.as_ref().unwrap().kind == DerivedCausalRoleKind::NativePendingRelationLockGatekeeper
            })
            .unwrap();
        let holder = explanation
            .culprits
            .iter()
            .find(|query| {
                query.causal_role.as_ref().unwrap().kind == DerivedCausalRoleKind::NativeGrantedRelationLockHolder
            })
            .unwrap();
        assert!(matches!(&gate.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session.id=="43"));
        assert_eq!(gate.sql.is_available(), case != MetadataQueueCase::MissingSql);
        assert!(
            matches!(&holder.identity,Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session{id,..})) if id=="44")
        );
        assert!(!holder.sql.is_available(), "unrelated current SQL is not the acquiring statement");
        for actor in [gate, holder] {
            assert!(actor.is_valid());
            assert!(actor.caller.is_some());
            assert_eq!(actor.evidence.len(), 2);
        }
        assert_eq!(explanation.candidates.iter().any(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session.id=="44")),case!=MetadataQueueCase::IdleHolder);
        assert!(explanation.candidates.iter().all(|query| query.causal_role.is_none()));
        assert_eq!(
            explanation.victim_summary.as_deref(),
            Some(report.metadata_queue.explanations[0].victim_summary.as_str())
        );
        assert_eq!(explanation.mechanism.as_deref(), Some(report.metadata_queue.explanations[0].mechanism.as_str()));
        let victim = queries::from_record(&original, 0).unwrap().unwrap();
        assert!(victim.causal_role.is_none());
        assert!(!explanation.culprits.iter().any(|query| query.identity == victim.identity));
        assert_eq!(fixture.recovery_object_reads(), opened);
    }
}
#[tokio::test]
async fn labels_modes_parallel_groups_native_prefixes_and_reused_actors_cannot_manufacture_a_cascade() {
    use MetadataQueueCase::*;
    for case in [
        DifferentRelation,
        DifferentPoint,
        UnknownMode,
        MixedMode,
        ParallelGroup,
        LockPrefix,
        OwnerPrefix,
        ReusedOwner,
        RequestChanged,
        PermissionLimited,
        UnverifiedOriginal,
        PreparedOwner,
        NoNativeDetails,
    ] {
        let (source, records) = test_support::metadata_queue_evidence(case);
        let fixture = Fixture::new(source, records).await;
        let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        let report = crate::collected_resources::blocking(&capture).unwrap().report;
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable, "{case:?}");
        assert!(report.metadata_queue.evidence.cascades.is_empty(), "{case:?}");
        assert!(!report.metadata_queue.evidence.issues.is_empty(), "{case:?}");
        let original = capture.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert!(explanation.victim_summary.is_none(), "{case:?}");
        assert!(
            explanation.culprits.iter().all(|query| query
                .causal_role
                .as_ref()
                .is_none_or(|role| role.kind == DerivedCausalRoleKind::NativeBlockingOwner)),
            "{case:?}"
        );
        assert_eq!(capture.records().len(), 3, "raw original facts remain independently useful");
    }
}
#[tokio::test]
async fn missing_canonical_rows_keep_partial_queue_evidence_and_causal_sql_cannot_borrow_a_holder_or_victim_identity() {
    let (source, records) = test_support::metadata_queue_evidence(MetadataQueueCase::Observed);
    let fixture = Fixture::new(source, records).await;
    fixture.remove_first_row(TelemetryStream::ActivityObservations);
    let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    assert!(!capture.is_read_complete());
    let report = crate::collected_resources::blocking(&capture).unwrap().report;
    assert!(report.metadata_queue.evidence.cascades.is_empty());
    assert!(report.metadata_queue.evidence.issues.contains(&MetadataQueueIssue::IncompleteRead));

    let (source, records) = test_support::metadata_queue_evidence(MetadataQueueCase::Observed);
    let fixture = Fixture::new(source, records).await;
    let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let original = capture.verified_original().unwrap();
    let actors = explanations::blocking(&original).unwrap();
    let mut holder = actors
        .culprits
        .iter()
        .find(|query| {
            query.causal_role.as_ref().unwrap().kind == DerivedCausalRoleKind::NativeGrantedRelationLockHolder
        })
        .unwrap()
        .clone();
    holder.sql = DefinitionPayload::sql("SELECT unrelated_current_work", Engine::Postgres);
    assert!(!holder.is_valid());
    let mut gate = actors
        .culprits
        .iter()
        .find(|query| {
            query.causal_role.as_ref().unwrap().kind == DerivedCausalRoleKind::NativePendingRelationLockGatekeeper
        })
        .unwrap()
        .clone();
    gate.evidence = vec![original.reference(0).unwrap()];
    assert!(!gate.is_valid(), "the affected row alone cannot authenticate pending-request SQL");
    gate.evidence = actors
        .culprits
        .iter()
        .find(|query| {
            query.causal_role.as_ref().unwrap().kind == DerivedCausalRoleKind::NativePendingRelationLockGatekeeper
        })
        .unwrap()
        .evidence
        .clone();
    if let Some(DerivedQueryIdentity::Activity { statement, .. }) = &mut gate.identity {
        *statement = None;
    }
    assert!(!gate.is_valid(), "available pending SQL needs its original statement identity");
}

#[test]
fn native_lock_points_cannot_be_published_as_baseline_or_beyond_their_original_source_clock() {
    let (source, records) = test_support::metadata_queue_evidence(MetadataQueueCase::Observed);
    let capture = CaptureIdentity { observation_id: "native-lock-source-fit".into(), revision: None };
    assert!(ExportedRecord::new(&source, &capture, records[0].clone()).is_ok());
    let mut baseline = source.clone();
    baseline.observation = SourceObservation::Activity(source.observation.activity().unwrap().clone());
    assert!(ExportedRecord::new(&baseline, &capture, records[0].clone()).is_err());
    let mut later = records[0].clone();
    let TelemetryRecord::Activity(point) = &mut later else { unreachable!() };
    let culprit_core::blocking::NativeLockEvidence::Postgres(native) =
        point.wait.as_mut().unwrap().resources.as_mut().unwrap()[0].native.as_deref_mut().unwrap()
    else {
        panic!("expected original PostgreSQL lock point")
    };
    native.observed_at = "2020-01-01T12:00:03Z".into();
    assert!(later.is_valid(), "the native row is shaped correctly but belongs outside this source clock");
    assert!(ExportedRecord::new(&source, &capture, later).is_err());
}

#[tokio::test]
async fn pg_cloud_standby_permission_and_single_object_lock_never_invent_escalation_or_healthy_absence() {
    // 1. Hot-standby / managed cloud read-replica scope preserves exact relation-lock queue actors when observed.
    let (mut standby_source, standby_records) = test_support::metadata_queue_evidence(MetadataQueueCase::Observed);
    if let SourceObservation::FocusedBlocking { snapshot, result } = &mut standby_source.observation {
        snapshot.database_writable = Some(false);
        result.focus.database_writable = Some(false);
    }
    let standby_fixture = Fixture::new(standby_source, standby_records).await;
    let standby_capture = standby_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let standby_report = crate::collected_resources::blocking(&standby_capture).unwrap().report;
    assert_eq!(standby_report.metadata_queue.evidence.status, MetadataQueueStatus::Observed);
    let standby_original = standby_capture.verified_original().unwrap();
    let standby_explanation = explanations::blocking(&standby_original).unwrap();
    assert_eq!(standby_explanation.culprits.len(), 2);
    assert!(!standby_explanation.mechanism.as_deref().unwrap_or_default().to_ascii_lowercase().contains("escalat"));

    // 2. Direct two-actor relation lock without a pending AccessExclusive gatekeeper keeps MetadataQueueStatus::Unavailable
    // and attributes only NativeBlockingOwner.
    let (direct_source, direct_records) = test_support::blocking_evidence(test_support::BlockingCase::Matched);
    let direct_fixture = Fixture::new(direct_source, direct_records).await;
    let direct_capture = direct_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let direct_report = crate::collected_resources::blocking(&direct_capture).unwrap().report;
    assert_eq!(direct_report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
    assert!(direct_report.metadata_queue.evidence.cascades.is_empty());
    let direct_original = direct_capture.verified_original().unwrap();
    let direct_explanation = explanations::blocking(&direct_original).unwrap();
    assert!(
        direct_explanation
            .culprits
            .iter()
            .all(|q| { q.causal_role.as_ref().is_none_or(|r| r.kind == DerivedCausalRoleKind::NativeBlockingOwner) })
    );
}

mod mssql;
