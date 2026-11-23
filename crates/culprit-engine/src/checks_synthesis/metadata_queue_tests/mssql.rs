//! Genuine canonical reopening exercises the existing common queue proof and Gold actor roles.
use super::*;
use culprit_core::blocking::{MetadataQueueResource, NativeLockEvidence};
use culprit_evidence::analytical::test_support::{MssqlMetadataQueueCase as Case, mssql_metadata_queue_evidence};
#[tokio::test]
async fn canonical_schema_queue_keeps_victim_gate_and_holder_sql_callers_and_causes_separate_without_a_global_incarnation()
 {
    for case in [Case::Observed, Case::RunningHolder, Case::MissingSql, Case::NoLabel, Case::CoexistingOwnerPoint] {
        let (source, records) = mssql_metadata_queue_evidence(case);
        assert!(source.target.database().unwrap().incarnation.is_none());
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        let opened = fixture.recovery_object_reads();
        let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        test_support::assert_blocking_capture(&capture, &source, &records);
        let report = crate::collected_resources::blocking(&capture).unwrap().report;
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Observed, "{case:?}");
        let queue = &report.metadata_queue.evidence.cascades[0];
        assert!(
            matches!(&queue.resource,MetadataQueueResource::MssqlObject(resource) if resource.database_id==7 && resource.associated_entity_id=="12345")
        );
        assert_eq!(queue.object_label.is_some(), case != Case::NoLabel);
        assert_eq!(queue.gatekeeper.session.id, "43");
        assert_eq!(queue.holders[0].actor.session.id, "44");
        assert_eq!(queue.affected[0].actor.session.id, "42");
        let original = capture.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert_eq!(explanation.attribution_status(), DerivedAttributionStatus::Established);
        assert_eq!(explanation.culprits.len(), 2);
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
        assert_eq!(gate.sql.is_available(), case != Case::MissingSql);
        assert!(!holder.sql.is_available(), "the granted transaction does not prove its acquiring SQL");
        assert!(
            matches!(&holder.identity,Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session{id,..})) if id=="44")
        );
        for actor in [gate, holder] {
            assert!(actor.is_valid() && actor.caller.is_some());
            assert_eq!(actor.evidence.len(), 2);
        }
        assert!(explanation.candidates.iter().all(|query| query.causal_role.is_none()));
        assert_eq!(explanation.candidates.iter().any(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session.id=="44")),case==Case::RunningHolder);
        assert!(explanation.action.is_some() && explanation.verify.is_some());
        assert_eq!(fixture.recovery_object_reads(), opened);
        let encoded = serde_json::to_string(&capture.records()).unwrap();
        assert!(!encoded.contains("queue-holder-private"));
    }
}
#[tokio::test]
async fn resource_groups_conversion_mars_reused_transactions_prefixes_missing_native_edges_and_ended_requests_refuse_a_schema_cascade()
 {
    for (source, records) in test_support::mssql_metadata_queue_evidence_cases().into_iter().skip(5) {
        let fixture = Fixture::new(source, records).await;
        let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        let report = crate::collected_resources::blocking(&capture).unwrap().report;
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
        assert!(
            report.metadata_queue.evidence.cascades.is_empty() && !report.metadata_queue.evidence.issues.is_empty()
        );
        let original = capture.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert!(explanation.culprits.iter().all(|query| {
            query.causal_role.as_ref().is_none_or(|role| role.kind == DerivedCausalRoleKind::NativeBlockingOwner)
        }));
        assert_eq!(capture.records().len(), 3, "native and contextual facts remain useful without the full mechanism");
    }
}
#[tokio::test]
async fn incomplete_canonical_frame_cannot_recover_missing_queue_facts_from_current_or_named_objects() {
    let (source, records) = mssql_metadata_queue_evidence(Case::Observed);
    let fixture = Fixture::new(source, records).await;
    fixture.remove_first_row(TelemetryStream::ActivityObservations);
    let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let report = crate::collected_resources::blocking(&capture).unwrap().report;
    assert!(!capture.is_read_complete());
    assert!(report.metadata_queue.evidence.cascades.is_empty());
    assert!(report.metadata_queue.evidence.issues.contains(&MetadataQueueIssue::IncompleteRead));
}
#[test]
fn actual_native_database_resource_namespace_and_both_endpoint_clocks_are_part_of_original_source_fit() {
    let (source, records) = mssql_metadata_queue_evidence(Case::Observed);
    let capture = CaptureIdentity { observation_id: "schema-lock-source-fit".into(), revision: None };
    assert!(ExportedRecord::new(&source, &capture, records[0].clone()).is_ok());
    for fault in ["database", "clock", "baseline", "engine"] {
        let mut source = source.clone();
        let mut row = records[0].clone();
        match fault {
            "baseline" => {
                source.observation = SourceObservation::Activity(source.observation.activity().unwrap().clone())
            }
            "engine" => {
                source.target = DatabaseIdentity::server(Engine::Postgres, "db", "fixture.invalid", 5432, None).into()
            }
            _ => {
                let TelemetryRecord::Activity(actor) = &mut row else { panic!() };
                let NativeLockEvidence::MssqlObject(point) =
                    actor.wait.as_mut().unwrap().resources.as_mut().unwrap()[0].native.as_deref_mut().unwrap()
                else {
                    panic!()
                };
                if fault == "database" {
                    point.resource.as_mut().unwrap().database_id = 8;
                } else {
                    point.settled_at = "2020-01-01T12:00:03Z".into();
                }
            }
        }
        assert!(ExportedRecord::new(&source, &capture, row).is_err(), "{fault}");
    }
}
