//! Actual owned relation-lock queues through the existing provider/factory/sealed-parent path.
use crate::native_test_support::{self as owned, MaintenanceFixture, RelationQueueCase, RelationQueueRole};
use culprit_core::{
    blocking::{MetadataQueueResource, MetadataQueueStatus, NativeLockEvidence},
    telemetry::*,
};
mod captures;
#[cfg(test)]
mod footprints;

#[tokio::test]
#[ignore = "requires the nonce-owned PG18 supervisor and a new private CULPRIT_NATIVE_SAFETY_RECEIPT_DIR; no production/CX"]
async fn native_relation_lock_queue_cascade_competing_partial_ended_parallel_and_two_catalog_populations() {
    for case in [RelationQueueCase::Small, RelationQueueCase::Medium, RelationQueueCase::Parallel] {
        Box::pin(MaintenanceFixture::run_relation_queue(case, |fixture| Box::pin(prove(fixture)))).await;
    }
}
async fn prove(fixture: &mut MaintenanceFixture) {
    Box::pin(fixture.prepare_relation_queue()).await;
    let case = fixture.relation_queue_case();
    let mut original = Box::pin(captures::original(fixture)).await;
    let original_query_count = original.capture().manifest.source_queries;
    assert_eq!(original_query_count, 6);
    let started = std::time::Instant::now();
    let observed = Box::pin(captures::blocking(fixture, &mut original, false)).await;
    let observed_receipt = captures::receipt(&observed, started.elapsed());
    owned::write_automatic_receipt(case.name(), "native-observed-source.json", &observed_receipt).unwrap();
    let report = Box::new(captures::report(&observed));
    if case == RelationQueueCase::Medium {
        assert!(observed.records.iter().any(|row|matches!(&row.record,TelemetryRecord::Activity(point) if point.wait.as_ref().and_then(|wait|wait.resources.as_ref()).is_some_and(|resources|resources.iter().any(|resource|matches!(resource.native.as_deref(),Some(NativeLockEvidence::Postgres(native)) if native.snapshot_rows>=32))))),"the medium source must actually encounter the owned lock population");
    }
    assert_exact_native_participants(fixture, &observed);
    if case == RelationQueueCase::Parallel {
        assert_native_parallel_refusal(fixture, &observed, &report);
    } else {
        assert_native_cascade(fixture, &report);
    }
    let mut source_queries = original_query_count + observed.manifest.source_queries;
    let partial = if case == RelationQueueCase::Small {
        let started = std::time::Instant::now();
        let partial = Box::pin(captures::blocking(fixture, &mut original, true)).await;
        source_queries += partial.manifest.source_queries;
        let partial_receipt = captures::receipt(&partial, started.elapsed());
        owned::write_automatic_receipt(case.name(), "native-partial-source.json", &partial_receipt).unwrap();
        let partial_report = Box::new(captures::report(&partial));
        assert!(partial.records.len() <= 2);
        assert_eq!(partial.manifest.request.budget.max_snapshot_rows, 2);
        assert!(partial_report.closure.is_limited());
        assert!(partial_report.metadata_queue.evidence.cascades.is_empty());
        assert_eq!(partial_report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
        Some(partial_receipt)
    } else {
        None
    };
    Box::pin(fixture.release_relation_queue()).await;
    let started = std::time::Instant::now();
    let ended = Box::pin(captures::blocking(fixture, &mut original, false)).await;
    source_queries += ended.manifest.source_queries;
    let ended_receipt = captures::receipt(&ended, started.elapsed());
    owned::write_automatic_receipt(case.name(), "native-ended-source.json", &ended_receipt).unwrap();
    let ended_report = Box::new(captures::report(&ended));
    assert!(matches!(
        ended_report.binding,
        FocusedRequestBinding::ParticipantChanged | FocusedRequestBinding::ParticipantMissing
    ));
    assert!(ended_report.metadata_queue.evidence.cascades.is_empty());
    assert!(ended.records.is_empty(), "acknowledged fixture completion does not preserve an active old request");
    assert!(
        Box::pin(fixture.retire_relation_queue()).await,
        "awaited actor commands and actual native absence are both required"
    );
    Box::pin(original.verify_retained()).await;
    assert_eq!(
        serde_json::to_value(captures::report(&observed)).unwrap(),
        serde_json::to_value(report.as_ref()).unwrap(),
        "retained original has no current-schema/actor repair after genuine retirement"
    );
    assert!(source_queries <= if case == RelationQueueCase::Small { 81 } else { 56 });
    let proof = serde_json::to_value(&report.metadata_queue).unwrap();
    owned::write_automatic_receipt(case.name(),"original-native-source.json",&serde_json::json!({
        "case":case.name(),"actual_original_scope":original.capture().source(),"original_parent_manifest_sha256":original.capture().manifest_sha256,
        "original_parent_source_queries":original_query_count,"observed_source":observed_receipt,"partial_source":partial,"ended_source":ended_receipt,
        "original_queue_report":proof,"fixture":fixture.relation_receipt(),"source_queries_including_each_setup":source_queries,
        "source_query_cap":if case==RelationQueueCase::Small {81}else{56},"source_captures":if case==RelationQueueCase::Small {4}else{3},
        "original_after_actor_exit_reverified":true,"actual_native_parallel_refusal":case==RelationQueueCase::Parallel,
        "native_mode_is_not_ddl_verdict":true,"holder_acquiring_sql_proved":false,"native_internal_work_bound":false,"cost_qualified":false
    })).unwrap();
}
fn assert_exact_native_participants(fixture: &MaintenanceFixture, capture: &super::owned_fixture::Captured) {
    assert_eq!(
        capture.records.len(),
        3,
        "only the selected upstream closure is acquired, not a concurrent same-name queue"
    );
    let expected = [RelationQueueRole::Holder, RelationQueueRole::Gatekeeper, RelationQueueRole::Affected]
        .map(|role| fixture.relation_identity(role).clone());
    for row in &capture.records {
        let TelemetryRecord::Activity(point) = &row.record else { unreachable!() };
        assert!(expected.contains(&point.key.session));
    }
    let SourceObservation::FocusedBlocking { result, .. } = &capture.source().observation else { unreachable!() };
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(result.membership, FocusedRequestMembership::Observed);
    assert_eq!(result.request_end, BlockingRequestState::SameStatement);
    assert_eq!(result.closure.status, BlockingClosureStatus::Observed);
}
fn assert_native_cascade(fixture: &MaintenanceFixture, report: &culprit_core::BlockingReport) {
    assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Observed);
    assert_eq!(report.metadata_queue.evidence.cascades.len(), 1);
    let case = &report.metadata_queue.evidence.cascades[0];
    assert_eq!(case.gatekeeper.session, *fixture.relation_identity(RelationQueueRole::Gatekeeper));
    assert_eq!(case.requested_mode, "AccessExclusiveLock");
    assert_eq!(case.holders.len(), 1);
    assert_eq!(case.affected.len(), 1);
    assert_eq!(case.holders[0].actor.session, *fixture.relation_identity(RelationQueueRole::Holder));
    assert_eq!(case.holders[0].granted_modes, ["AccessShareLock"]);
    assert_eq!(case.affected[0].actor.session, *fixture.relation_identity(RelationQueueRole::Affected));
    assert_eq!(case.affected[0].requested_mode, "AccessShareLock");
    let MetadataQueueResource::PostgresRelation(tag) = &case.resource;
    assert_eq!(tag.database_oid, fixture.relation_database_oid());
    assert_eq!(tag.relation_oid, fixture.relation_oid());
    assert_ne!(tag.relation_oid, fixture.competing_relation_oid());
    assert_eq!(report.metadata_queue.explanations.len(), 1);
}
fn assert_native_parallel_refusal(
    fixture: &MaintenanceFixture,
    capture: &super::owned_fixture::Captured,
    report: &culprit_core::BlockingReport,
) {
    assert!(report.metadata_queue.evidence.cascades.is_empty());
    assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable);
    let gate = capture
        .records
        .iter()
        .find_map(|row| match &row.record {
            TelemetryRecord::Activity(point)
                if point.key.session == *fixture.relation_identity(RelationQueueRole::Gatekeeper) =>
            {
                Some(point)
            }
            _ => None,
        })
        .unwrap();
    let native = gate
        .wait
        .as_ref()
        .unwrap()
        .resources
        .as_ref()
        .unwrap()
        .iter()
        .find_map(|resource| resource.native.as_deref().map(|NativeLockEvidence::Postgres(point)| point.as_ref()))
        .unwrap();
    assert!(
        native.waiter_matches && native.single_backend && native.actor_rows > 3,
        "parallel workers must still be represented at the actual source projection"
    );
    assert!(
        native.blockers.iter().any(|owner| owner
            .actor
            .as_ref()
            .is_some_and(|actor| actor.session == *fixture.relation_identity(RelationQueueRole::Holder))
            && !owner.single_backend),
        "a changed/hidden holder or plan-only Gather is not parallel acceptance"
    );
}
