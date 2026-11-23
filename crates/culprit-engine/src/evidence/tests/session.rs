use super::{super::EvidenceSession, lifecycle::fixture::Fixture};
use culprit_core::{
    ErrorDetail,
    telemetry::{CollectionStop, DeliveryProblem},
};
use culprit_evidence::analytical::evidence::EvidenceFamily;
use culprit_evidence::analytical::evidence::EvidenceGapReason;
use culprit_evidence::analytical::evidence::EvidenceReadBudget;
use culprit_evidence::analytical::evidence::RecoveryPayloadBudget;
use culprit_read::cancellation::CollectionCancellation;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::Instant;

fn io_counts(fixture: &Fixture) -> (usize, usize) {
    (fixture.objects.reads.lock().unwrap().len(), fixture.store.requests.load(Ordering::SeqCst))
}

fn payload_reads(fixture: &Fixture) -> usize {
    fixture.objects.reads.lock().unwrap().iter().filter(|key| key.starts_with("payloads/")).count()
}

async fn assert_poisoned(session: &mut EvidenceSession<'_>, fixture: &Fixture) {
    let before = io_counts(fixture);
    let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(io_counts(fixture), before, "a closed action cannot open recovery or warehouse work again");
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn selected_manifest_rows_consume_the_action_budget_even_when_no_verified_rows_return() {
    for unpublished in [false, true] {
        let fixture = Fixture::new().await;
        if unpublished {
            fixture.store.publication.lock().unwrap().take();
        } else {
            fixture.store.omit_first_row();
        }
        let cancellation = CollectionCancellation::default();
        let mut session = fixture
            .service
            .session(EvidenceReadBudget { max_rows: 3, ..fixture.request.budget }, &cancellation)
            .unwrap();
        let capture = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
        assert_eq!(capture.metrics().selected_rows, 2);
        assert_eq!(capture.metrics().returned_rows, if unpublished { 0 } else { 1 });
        assert_eq!(capture.records().len(), if unpublished { 0 } else { 1 });
        assert_eq!(
            capture.gaps()[0].reason,
            if unpublished { EvidenceGapReason::NotPublished } else { EvidenceGapReason::MissingRows }
        );
        let warehouse_before = fixture.store.requests.load(Ordering::SeqCst);
        let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
        assert_eq!(
            fixture.store.requests.load(Ordering::SeqCst),
            warehouse_before,
            "remaining rows are checked before warehouse access"
        );
        assert_poisoned(&mut session, &fixture).await;
    }
}

#[tokio::test]
async fn distinct_selected_families_can_complete_in_one_action_without_charging_unselected_rows() {
    let (source, records) = culprit_evidence::analytical::test_support::query_evidence();
    let families = [EvidenceFamily::QueryDefinitions, EvidenceFamily::PlanDefinitions];
    let expected =
        families.map(|family| records.iter().filter(|record| record.stream() == family.stream()).count() as u32);
    assert!(expected.iter().all(|count| *count > 0));
    let selected_rows = expected.iter().sum();
    assert!((selected_rows as usize) < records.len(), "runtime and waits remain outside this selection");
    let fixture = Fixture::capture(source, records).await;
    let cancellation = CollectionCancellation::default();
    let mut session = fixture
        .service
        .session(EvidenceReadBudget { max_rows: selected_rows, ..fixture.request.budget }, &cancellation)
        .unwrap();
    for (family, rows) in families.into_iter().zip(expected) {
        let capture = session.read(&fixture.request.range_id, &[family]).await.unwrap();
        assert!(capture.is_read_complete(), "{:?}", capture.gaps());
        assert_eq!(capture.metrics().selected_rows, rows);
        assert_eq!(capture.records().len(), rows as usize);
        assert!(capture.records().iter().all(|record| record.row().record.stream() == family.stream()));
    }
    assert_poisoned(&mut session, &fixture).await;
}

#[tokio::test]
async fn warehouse_queries_complete_responses_and_projection_proofs_share_one_action_allowance() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let mut calibration = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
    let captured = calibration.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
    let used = calibration.completed_metrics().clone();
    assert!(used.queries > 1 && used.response_bytes >= 1024 && used.proof_bytes > 0);
    assert!(used.proof_bytes > captured.metrics().proof_bytes, "the action also pays authenticated metadata reads");
    let largest_row = u64::from(fixture.largest_row_response_bytes);
    assert!(largest_row > 0);
    let original_history = fixture.objects.snapshot();
    for limit in ["queries", "responses", "proofs"] {
        let mut budget = fixture.request.budget;
        match limit {
            "queries" => budget.max_queries = used.queries * 2 - 1,
            "responses" => {
                // Every first-read prefix plus a required row fits. Charging the actual response leaves less
                // than the largest authenticated row bound, so the next read cannot receive a fresh allowance.
                budget.max_bytes = used.response_bytes.checked_add(largest_row).unwrap() - 1;
                budget.page_bytes = u64::from(budget.page_bytes).min(budget.max_bytes).try_into().unwrap();
            }
            "proofs" => budget.max_proof_bytes = used.proof_bytes * 2 - 1,
            _ => unreachable!(),
        }
        let cancellation = CollectionCancellation::default();
        let mut session = fixture.service.session(budget, &cancellation).unwrap();
        let before = fixture.store.requests.load(Ordering::SeqCst);
        let first = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_or_else(|error| {
            panic!(
                "first {limit} read must fit: wire={}, planned_row={largest_row}, budget={budget:?}, error={error:?}",
                used.response_bytes
            )
        });
        assert_eq!(first.metrics().queries, used.queries);
        assert_eq!(first.metrics().response_bytes, used.response_bytes);
        assert_eq!(
            first.metrics().proof_bytes,
            captured.metrics().proof_bytes,
            "capture metrics retain their documented projection scope"
        );
        assert_eq!(session.completed_metrics().proof_bytes, used.proof_bytes);
        if limit == "responses" {
            assert_eq!(budget.max_bytes - first.metrics().response_bytes, largest_row - 1);
        }
        let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)), "{limit}");
        if limit == "queries" {
            assert_eq!(fixture.store.requests.load(Ordering::SeqCst) - before, budget.max_queries as usize);
        }
        assert_poisoned(&mut session, &fixture).await;
    }
    assert_eq!(fixture.objects.snapshot(), original_history);
}

#[tokio::test]
async fn whole_recovery_objects_and_bytes_are_not_refilled_for_the_next_capture_read() {
    let (source, records) = culprit_evidence::analytical::test_support::object_definition_recovery_only_evidence();
    let mut fixture = Fixture::capture(source, records).await;
    fixture.request.families = vec![EvidenceFamily::ObjectDefinitions];
    fixture.request.budget.recovery_payloads = Some(RecoveryPayloadBudget::default());
    fixture.objects.allow_payloads.store(true, Ordering::SeqCst);
    let calibration = fixture.service.read(&fixture.request, &CollectionCancellation::default()).await.unwrap();
    assert!(calibration.is_read_complete());
    assert_eq!(calibration.metrics().recovery_objects, 1);
    let object_bytes = calibration.metrics().recovery_bytes;
    assert!(object_bytes > 1024);
    let original_history = fixture.objects.snapshot();
    for objects_exhausted in [true, false] {
        let mut budget = fixture.request.budget;
        budget.recovery_payloads = Some(RecoveryPayloadBudget {
            max_objects: if objects_exhausted { 1 } else { 3 },
            max_bytes: if objects_exhausted { object_bytes * 3 } else { object_bytes * 2 - 1 },
        });
        let cancellation = CollectionCancellation::default();
        let mut session = fixture.service.session(budget, &cancellation).unwrap();
        let before = payload_reads(&fixture);
        let first = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
        assert!(first.is_read_complete());
        assert_eq!(first.metrics().recovery_objects, 1);
        assert_eq!(first.metrics().recovery_bytes, object_bytes);
        let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
        assert_eq!(payload_reads(&fixture) - before, 1, "the second object's full byte count is checked before open");
        assert_poisoned(&mut session, &fixture).await;
    }
    assert_eq!(fixture.objects.snapshot(), original_history);
}

#[tokio::test(start_paused = true)]
async fn idle_time_exhausts_the_original_deadline_before_any_new_recovery_or_warehouse_access() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let mut session = fixture
        .service
        .session(EvidenceReadBudget { timeout_ms: 100, ..fixture.request.budget }, &cancellation)
        .unwrap();
    session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    let before = io_counts(&fixture);
    let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::CollectionStopped(CollectionStop::TimedOut)));
    assert_eq!(io_counts(&fixture), before);
    assert_poisoned(&mut session, &fixture).await;
}

#[tokio::test(start_paused = true)]
async fn later_recovery_work_gets_only_the_unspent_absolute_time_and_drops_its_owner_on_timeout() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let mut session = fixture
        .service
        .session(EvidenceReadBudget { timeout_ms: 100, ..fixture.request.budget }, &cancellation)
        .unwrap();
    session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
    tokio::time::advance(Duration::from_millis(80)).await;
    fixture.objects.stall.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let before = io_counts(&fixture);
    let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::CollectionStopped(CollectionStop::TimedOut)));
    assert_eq!(Instant::now() - started, Duration::from_millis(20), "read two cannot restart the caller's deadline");
    assert!(io_counts(&fixture).0 > before.0, "the test must start the owned recovery operation");
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
    fixture.objects.stall.store(false, Ordering::SeqCst);
    assert_poisoned(&mut session, &fixture).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_a_later_owned_payload_body_and_permanently_closes_the_session() {
    let (source, records) = culprit_evidence::analytical::test_support::object_definition_recovery_only_evidence();
    let mut fixture = Fixture::capture(source, records).await;
    fixture.request.families = vec![EvidenceFamily::ObjectDefinitions];
    fixture.request.budget.recovery_payloads = Some(RecoveryPayloadBudget::default());
    fixture.objects.allow_payloads.store(true, Ordering::SeqCst);
    let cancellation = CollectionCancellation::default();
    let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
    session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap();
    fixture.objects.stall_payload_body.store(true, Ordering::SeqCst);
    let original_history = fixture.objects.snapshot();
    let before = payload_reads(&fixture);
    let (result, _) = tokio::join!(session.read(&fixture.request.range_id, &fixture.request.families), async {
        fixture.objects.payload_started.notified().await;
        assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 1);
        cancellation.cancel();
    });
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(payload_reads(&fixture) - before, 1);
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
    assert_poisoned(&mut session, &fixture).await;
    assert_eq!(fixture.objects.snapshot(), original_history);
}

#[tokio::test]
async fn a_cancelled_or_failed_action_cannot_retry_even_when_the_original_failure_is_removed() {
    for cancelled in [true, false] {
        let fixture = Fixture::new().await;
        let cancellation = CollectionCancellation::default();
        if cancelled {
            cancellation.cancel();
        } else {
            fixture.store.denied.store(true, Ordering::SeqCst);
        }
        let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
        let before = io_counts(&fixture);
        let error = session.read(&fixture.request.range_id, &fixture.request.families).await.unwrap_err();
        if cancelled {
            assert_eq!(error.detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
            assert_eq!(io_counts(&fixture), before);
        } else {
            assert_eq!(error.code, culprit_core::ErrorCode::PermissionDenied);
            assert!(io_counts(&fixture).1 > before.1);
            fixture.store.denied.store(false, Ordering::SeqCst);
        }
        assert_poisoned(&mut session, &fixture).await;
    }
}

#[tokio::test]
async fn exact_original_reference_binding_keeps_native_position_and_rejects_changed_manifest_or_scope() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let capture = fixture.service.read(&fixture.request, &cancellation).await.unwrap();
    let reference = crate::collected_resources::references::from_record(&capture, &capture.records()[0]).unwrap();
    let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
    let replay = session.read_reference(&reference.capture_ref(), &fixture.request.families).await.unwrap();
    assert_eq!(
        crate::collected_resources::references::selected_record(&replay, &reference).unwrap().unwrap().row(),
        capture.records()[0].row()
    );
    for scope in [false, true] {
        let mut changed = reference.capture_ref();
        if scope {
            changed.source_scope_id = culprit_core::telemetry::digest(b"another original scope");
        } else {
            changed.manifest_sha256 = culprit_core::telemetry::digest(b"another original manifest");
        }
        let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
        let error = session.read_reference(&changed, &fixture.request.families).await.unwrap_err();
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
        assert_poisoned(&mut session, &fixture).await;
    }
}
#[tokio::test]
async fn exact_metadata_reference_keeps_header_binding_and_failed_publication_explicit() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let capture = fixture.service.read(&fixture.request, &cancellation).await.unwrap();
    let reference = culprit_core::telemetry::DiagnosticCaptureRef {
        capture: capture.capture().clone(),
        manifest_sha256: capture.capture_manifest_sha256().into(),
        source_scope_id: capture.source().captured_range().unwrap().scope_id,
    };
    fixture.store.publication.lock().unwrap().take();
    let before = io_counts(&fixture);
    let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
    let crate::evidence::EvidenceMetadataLookup::Available(metadata) =
        session.lookup_metadata_reference(&reference).await.unwrap()
    else {
        panic!("the actual header is committed")
    };
    assert_eq!(metadata.capture(), &reference.capture);
    assert_eq!(metadata.metrics().queries, 1);
    assert_eq!(metadata.gaps()[0].reason, EvidenceGapReason::NotPublished);
    assert_eq!(io_counts(&fixture).1 - before.1, 1, "header-only ancestry neither audits nor reads native rows");
    assert_eq!(payload_reads(&fixture), 0);
}
#[tokio::test]
async fn cancelled_exact_ancestry_read_cannot_reopen_the_action_or_publish_a_partial_native_record() {
    let fixture = Fixture::new().await;
    let cancellation = CollectionCancellation::default();
    let capture = fixture.service.read(&fixture.request, &cancellation).await.unwrap();
    let reference = crate::collected_resources::references::from_record(&capture, &capture.records()[0]).unwrap();
    let before = io_counts(&fixture);
    cancellation.cancel();
    let mut session = fixture.service.session(fixture.request.budget, &cancellation).unwrap();
    let error = session.read_reference(&reference.capture_ref(), &fixture.request.families).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    assert_eq!(io_counts(&fixture), before);
    assert_poisoned(&mut session, &fixture).await;
}

#[tokio::test]
async fn three_materialized_gold_families_share_one_action_row_allowance_without_default_or_source_reads() {
    let (source, records) = culprit_evidence::analytical::test_support::derived_diagnosis_evidence_cases().remove(0);
    let fixture = Fixture::capture(source, records).await;
    let families = [EvidenceFamily::DerivedIncidents, EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges];
    let cancellation = CollectionCancellation::default();
    let mut session =
        fixture.service.session(EvidenceReadBudget { max_rows: 3, ..fixture.request.budget }, &cancellation).unwrap();
    for family in families {
        let capture = session.read(&fixture.request.range_id, &[family]).await.unwrap();
        assert!(capture.is_read_complete(), "{:?}", capture.gaps());
        assert_eq!(capture.metrics().selected_rows, 1);
        assert_eq!(capture.records().len(), 1);
        assert_eq!(capture.records()[0].row().record.stream(), family.stream());
        assert_eq!(capture.metrics().recovery_objects, 0);
    }
    assert_poisoned(&mut session, &fixture).await;
}

#[tokio::test]
async fn an_original_not_committed_in_a_valid_root_is_a_paid_lookup_value_and_does_not_refill_or_poison_other_reads() {
    let fixture = Fixture::new().await;
    let token = CollectionCancellation::default();
    let first = fixture.service.read(&fixture.request, &token).await.unwrap();
    let original = culprit_core::telemetry::DiagnosticCaptureRef {
        capture: first.capture().clone(),
        manifest_sha256: first.capture_manifest_sha256().into(),
        source_scope_id: first.source().captured_range().unwrap().scope_id,
    };
    let (source, rows) = culprit_evidence::analytical::test_support::memory_failure_evidence();
    let next = fixture.append_capture(source, rows, 100_001).await;
    let next_reference = culprit_core::telemetry::DiagnosticCaptureRef {
        capture: next.manifest().capture.clone(),
        manifest_sha256: next.manifest().capture_manifest_sha256.clone(),
        source_scope_id: next.manifest().source.captured_range().unwrap().scope_id,
    };
    fixture.objects.remove_range_index(&fixture.request.range_id);
    let mut session = fixture.service.session(fixture.request.budget, &token).unwrap();
    let before = io_counts(&fixture);
    assert!(matches!(
        session.lookup_metadata_reference(&original).await.unwrap(),
        crate::evidence::EvidenceMetadataLookup::NotCommitted
    ));
    let paid_missing = session.completed_metrics().proof_bytes;
    assert!(paid_missing > 0, "the current root/index lookup bytes are charged even without a warehouse query");
    assert_eq!(io_counts(&fixture).1, before.1);
    let crate::evidence::EvidenceMetadataLookup::Available(metadata) =
        session.lookup_metadata_reference(&next_reference).await.unwrap()
    else {
        panic!("this actual original is committed")
    };
    assert_eq!(metadata.capture(), &next_reference.capture);
    assert!(session.completed_metrics().proof_bytes > paid_missing);
    assert_eq!(session.completed_metrics().queries, 1);
    let mut limited = fixture
        .service
        .session(EvidenceReadBudget { max_proof_bytes: paid_missing - 1, ..fixture.request.budget }, &token)
        .unwrap();
    assert_eq!(
        limited.lookup_metadata_reference(&original).await.unwrap_err().detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::Capacity))
    );
    let stopped = io_counts(&fixture);
    assert!(limited.lookup_metadata_reference(&next_reference).await.is_err());
    assert_eq!(
        io_counts(&fixture),
        stopped,
        "capacity failure remains poisoned; the missing outcome did not grant an allowance"
    );
    let mut strict = fixture.service.session(fixture.request.budget, &token).unwrap();
    assert_eq!(
        strict.read_reference(&original, &fixture.request.families).await.unwrap_err().detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::NotReady))
    );
    assert_poisoned(&mut strict, &fixture).await;
}

#[tokio::test]
async fn missing_unvalidated_root_corrupt_manifest_and_denied_publication_never_become_soft_original_absence() {
    for failure in ["root", "manifest", "permission"] {
        let fixture = Fixture::new().await;
        let token = CollectionCancellation::default();
        let actual = fixture.service.read(&fixture.request, &token).await.unwrap();
        let reference = culprit_core::telemetry::DiagnosticCaptureRef {
            capture: actual.capture().clone(),
            manifest_sha256: actual.capture_manifest_sha256().into(),
            source_scope_id: actual.source().captured_range().unwrap().scope_id,
        };
        match failure {
            "root" => fixture.objects.remove_history(),
            "manifest" => fixture.objects.corrupt_manifest(),
            "permission" => fixture.store.denied.store(true, Ordering::SeqCst),
            _ => unreachable!(),
        }
        let mut session = fixture.service.session(fixture.request.budget, &token).unwrap();
        let error = session.lookup_metadata_reference(&reference).await.unwrap_err();
        if failure == "permission" {
            assert_eq!(error.code, culprit_core::ErrorCode::PermissionDenied);
        } else {
            assert!(matches!(
                error.detail,
                Some(ErrorDetail::Delivery(DeliveryProblem::NotReady | DeliveryProblem::Integrity))
            ));
        }
        assert_poisoned(&mut session, &fixture).await;
    }
}
