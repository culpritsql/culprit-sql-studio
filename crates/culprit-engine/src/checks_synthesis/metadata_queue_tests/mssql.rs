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

#[tokio::test]
async fn azure_mi_readonly_permission_and_non_escalated_object_locks_preserve_exact_boundaries() {
    // 1. Azure SQL Database (with logical incarnation), Managed Instance, and ReadScale (database_writable = false)
    // preserve exact NativePendingRelationLockGatekeeper + NativeGrantedRelationLockHolder culprits without inventing escalation.
    for (label, incarnation, kind, writable) in [
        ("azure-sql-db", Some("azure-logical-db-guid"), SharedResourceKind::Node, Some(true)),
        ("managed-instance", None, SharedResourceKind::Instance, Some(true)),
        ("read-scale-secondary", Some("azure-logical-db-guid"), SharedResourceKind::Node, Some(false)),
    ] {
        let (mut source, records) = mssql_metadata_queue_evidence(Case::Observed);
        if let CollectionTarget::Database(db) = &mut source.target {
            db.incarnation = incarnation.map(str::to_owned);
        }
        if let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut source.partition {
            member.kind = kind;
        }
        if let SourceObservation::FocusedBlocking { snapshot, result } = &mut source.observation {
            snapshot.database_writable = writable;
            result.focus.target = source.target.clone();
            result.focus.partition = source.partition.clone();
            result.focus.database_writable = writable;
        }
        let fixture = Fixture::new(source.clone(), records).await;
        let capture = fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
        let report = crate::collected_resources::blocking(&capture).unwrap().report;
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Observed, "{label}");
        let original = capture.verified_original().unwrap();
        let explanation = explanations::blocking(&original).unwrap();
        assert_eq!(explanation.attribution_status(), DerivedAttributionStatus::Established, "{label}");
        assert_eq!(explanation.culprits.len(), 2, "{label}");
        assert!(
            !explanation.mechanism.as_deref().unwrap_or_default().to_ascii_lowercase().contains("escalat"),
            "{label}: an object lock queue must not claim lock escalation without native proof"
        );
    }

    // 2. PermissionDenied (missing VIEW DATABASE/SERVER PERFORMANCE STATE) never fabricates healthy absence or a queue.
    let (denied_source, denied_records) = mssql_metadata_queue_evidence(Case::PermissionDenied);
    let denied_fixture = Fixture::new(denied_source, denied_records).await;
    let denied_capture = denied_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let denied_report = crate::collected_resources::blocking(&denied_capture).unwrap().report;
    assert_eq!(denied_report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
    assert!(denied_report.metadata_queue.evidence.cascades.is_empty());
    assert!(!denied_report.metadata_queue.evidence.issues.is_empty());

    // 3. A single granted object-level X lock blocking a reader (without a pending Sch-M gatekeeper waiting on an earlier holder)
    // attributes only NativeBlockingOwner and never claims lock escalation or a metadata-lock queue.
    let (mut two_actor_source, mut two_actor_records) = mssql_metadata_queue_evidence(Case::Observed);
    let holder = two_actor_records.remove(2);
    two_actor_records.truncate(1);
    {
        let TelemetryRecord::Activity(victim) = &mut two_actor_records[0] else { panic!() };
        let TelemetryRecord::Activity(holder_obs) = &holder else { panic!() };
        let holder_owner = ActivityWaitOwner::Session {
            id: holder_obs.key.session.id.clone(),
            generation: Some(holder_obs.key.session.generation.clone()),
        };
        victim.wait.as_mut().unwrap().owners = vec![holder_owner.clone()];
        let NativeLockEvidence::MssqlObject(point) =
            victim.wait.as_mut().unwrap().resources.as_mut().unwrap()[0].native.as_deref_mut().unwrap()
        else {
            panic!()
        };
        point.blockers[0].context.owner = holder_owner;
        point.blockers[0].context.actor = None;
        point.blockers[0].context.statement = None;
        point.blockers[0].context.native_wait = None;
        point.blockers[0].context.wait_ms = None;
        point.blockers[0].context.current_requests = Some("0".into());
        point.blockers[0].granted_modes = vec![culprit_core::blocking::MssqlObjectLockMode::from_native("X")];
        point.blockers[0].waiting_mode = None;
        point.blockers[0].waiting_status = None;
    }
    two_actor_records.push(holder);
    let mut parent_source = two_actor_source.clone();
    {
        let SourceObservation::FocusedBlocking { snapshot, result } = &mut two_actor_source.observation else {
            panic!()
        };
        snapshot.native_rows = 2;
        snapshot.observations = 2;
        snapshot.coverage.records = 2;
        let TelemetryRecord::Activity(victim_after) = &two_actor_records[0] else { panic!() };
        result.focus.original = (**victim_after).clone();
        let parent_capture = result.focus.parent.capture.clone();
        parent_source.source = culprit_core::source_id::SESSIONS.into();
        parent_source.observation = SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: snapshot.epoch.clone(),
            database_writable: snapshot.database_writable,
            observed_from: "2020-01-01T12:00:00Z".into(),
            at: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:00Z".into(),
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(culprit_core::source_id::SESSIONS.into(), 1),
        });
        let mut parent_victim = two_actor_records[0].clone();
        if let TelemetryRecord::Activity(obs) = &mut parent_victim {
            obs.wait.as_mut().unwrap().resources = None;
        }
        let exported = ExportedRecord::new(&parent_source, &parent_capture, parent_victim).unwrap();
        result.focus.parent =
            DiagnosticEvidenceRef::from_export(&parent_source, &digest(b"mssql-two-actor-parent"), &exported, 0)
                .unwrap();
    }
    let obj_fixture = Fixture::new(two_actor_source, two_actor_records).await;
    let obj_capture = obj_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let obj_report = crate::collected_resources::blocking(&obj_capture).unwrap().report;
    assert_eq!(obj_report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
    assert!(obj_report.metadata_queue.evidence.cascades.is_empty());
    let obj_original = obj_capture.verified_original().unwrap();
    let obj_explanation = explanations::blocking(&obj_original).unwrap();
    assert_eq!(obj_explanation.culprits.len(), 1);
    assert_eq!(
        obj_explanation.culprits[0].causal_role.as_ref().unwrap().kind,
        DerivedCausalRoleKind::NativeBlockingOwner
    );
}
