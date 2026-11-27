//! Existing archive staging/projection/EOF proofs exercise the actual resource+window service; no native source runs.
use super::lifecycle::fixture::Fixture;
use super::*;
use culprit_core::{telemetry::*, *};
use std::sync::atomic::Ordering;

async fn fixture() -> (Fixture, WarehouseWindowQuery) {
    let (source, rows) = crate::collected_evidence::window_test_original(6, Some("window-original-epoch"));
    let fixture = Fixture::capture(source, rows).await;
    let resources = fixture
        .service
        .resources(
            &PublishedResourceRequest::default(),
            EvidenceReadBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    assert!(resources.discovery_complete);
    let request = WarehouseWindowQuery {
        resource: resources.resources.into_iter().next().unwrap().selector,
        window: PublishedResourceWindow { from: "2020-01-01T06:00:00Z".into(), until: "2020-01-01T08:00:00Z".into() },
        as_of: "2030-01-01T00:00:00Z".into(),
        view: WarehouseWindowView::CompatibleWorkload,
        after: None,
        page_rows: DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS,
    };
    (fixture, request)
}

#[tokio::test]
async fn late_native_intervals_are_merged_through_one_session_and_the_real_findings_pipeline() {
    let (fixture, request) = fixture().await;
    let (source, rows) = crate::collected_evidence::window_test_original(7, Some("window-original-epoch"));
    let appended = fixture.append_capture(source, rows, 100_001).await;
    let setup_reads = fixture.objects.reads.lock().unwrap().clone();
    assert!(
        appended
            .manifest()
            .batches
            .iter()
            .any(|batch| setup_reads.contains(&format!("payloads/{}.ndjson", batch.sha256))),
        "the genuine archive encoder authenticates canonical originals before this read action starts"
    );
    let read_start = setup_reads.len();
    let before = fixture.objects.snapshot();
    let budget = EvidenceReadBudget::default();
    assert!(budget.recovery_payloads.is_none(), "this action does not opt into native payload hydration");
    let report = fixture.service.window(&request, budget, &CollectionCancellation::default()).await.unwrap();
    assert!(report.discovery_complete);
    assert_eq!(report.originals.len(), 2);
    assert_eq!(report.workload.analysis().unwrap().sections.len(), 1);
    let section = &report.workload.analysis().unwrap().sections[0];
    assert_eq!(section.inputs.len(), 2);
    assert_eq!(section.observed.from, canonical_time(&request.window.from).unwrap());
    assert_eq!(section.observed.until, canonical_time(&request.window.until).unwrap());
    assert!(section.native_scope_qualified);
    assert!(!section.analysis.findings.is_empty(), "existing rules analyze the genuinely joined native workload");
    assert!(report.gaps.iter().any(|gap| matches!(gap, WarehouseWindowGap::RawWithoutPublishedAnalysis { .. })));
    assert!(report.published.is_empty(), "raw-only is explicit, never a fabricated empty Gold result");
    assert_eq!(fixture.objects.snapshot(), before, "window reading cannot acquire a history writer");
    let reads = fixture.objects.reads.lock().unwrap();
    let action_reads = &reads[read_start..];
    assert!(!action_reads.is_empty(), "the action must authenticate the original metadata/projection proofs");
    assert!(
        action_reads.iter().all(|key| !key.starts_with("payloads/")),
        "this read-only action cannot hydrate optional native payloads: {action_reads:?}"
    );
    assert_eq!(report.archive_work.recovery_objects, 0);
    assert_eq!(report.archive_work.recovery_bytes, 0);
}

#[tokio::test]
async fn independent_native_epochs_remain_separate_even_for_one_discovered_resource_route() {
    let (fixture, request) = fixture().await;
    let (source, rows) = crate::collected_evidence::window_test_original(7, Some("another-window-epoch"));
    fixture.append_capture(source, rows, 100_001).await;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(report.workload.analysis().unwrap().sections.len(), 2);
    assert!(report.workload.analysis().unwrap().sections.iter().all(|section| section.inputs.len() == 1));
    assert_ne!(
        report.workload.analysis().unwrap().sections[0].scope_id,
        report.workload.analysis().unwrap().sections[1].scope_id
    );
    assert!(
        report
            .workload
            .analysis()
            .unwrap()
            .gaps
            .iter()
            .any(|gap| matches!(gap, CollectedWindowGap::SectionDoesNotCoverWindow { .. }))
    );
}

#[tokio::test]
async fn exact_native_window_filters_an_authenticated_later_interval_without_borrowing_acquisition_time() {
    let (fixture, request) = fixture().await;
    let (source, rows) = crate::collected_evidence::window_test_original(12, Some("window-original-epoch"));
    fixture.append_capture(source, rows, 100_001).await;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(report.workload.analysis().unwrap().sections.len(), 1);
    assert_eq!(report.workload.analysis().unwrap().sections[0].inputs.len(), 1);
    assert_eq!(
        report.workload.analysis().unwrap().sections[0].observed.until,
        canonical_time("2020-01-01T07:00:00Z").unwrap()
    );
    assert!(!report.workload.analysis().unwrap().gaps.is_empty());
}

#[tokio::test]
async fn an_unpublished_original_cannot_become_a_healthy_empty_window() {
    let (fixture, request) = fixture().await;
    *fixture.store.publication.lock().unwrap() = None;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(report.workload.analysis().unwrap().sections.is_empty());
    assert!(report.gaps.iter().any(|gap| matches!(gap, WarehouseWindowGap::NoPublishedOriginals)));
    assert!(
        report.workload.analysis().unwrap().gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::NoNativeIntervals))
    );
}

#[tokio::test]
async fn namespace_and_whole_action_budget_refusals_do_not_start_new_read_allowances() {
    let (fixture, mut request) = fixture().await;
    let valid = request.clone();
    request.resource.namespace_id = digest(b"another-independent-recovery-store");
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let error = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::DestinationChanged)));
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
    let budget = EvidenceReadBudget { max_queries: 1, ..Default::default() };
    let error = fixture.service.window(&valid, budget, &CollectionCancellation::default()).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert!(fixture.store.requests.load(Ordering::SeqCst) <= before + 1);
}

#[tokio::test]
async fn cancelled_window_work_closes_the_owned_recovery_read_and_cannot_publish_a_partial_report() {
    let (fixture, request) = fixture().await;
    let before = fixture.objects.snapshot();
    fixture.objects.stall.store(true, Ordering::SeqCst);
    let cancellation = CollectionCancellation::default();
    let read = fixture.service.window(&request, EvidenceReadBudget::default(), &cancellation);
    let (result, _) = tokio::join!(read, async {
        while fixture.objects.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.objects.snapshot(), before);
}

#[tokio::test]
async fn selected_gold_reopens_exact_ancestry_and_keeps_resolved_history_without_manufacturing_runtime() {
    let scenario = super::incidents::scenario().await;
    let fixture = &scenario.fixture;
    let resources = fixture
        .service
        .resources(
            &PublishedResourceRequest::default(),
            EvidenceReadBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    let query = WarehouseWindowQuery {
        resource: resources.resources[0].selector.clone(),
        window: PublishedResourceWindow { from: scenario.request.from.clone(), until: scenario.request.until.clone() },
        as_of: scenario.request.recorded_before.clone(),
        view: WarehouseWindowView::CompatibleWorkload,
        after: None,
        page_rows: DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS,
    };
    let before = fixture.objects.snapshot();
    let report = fixture
        .service
        .window(&query, IncidentRequest::default_budget(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(report.discovery_complete);
    assert!(!report.published.is_empty());
    assert!(report.incidents.history.entries.iter().any(|entry| entry.lifecycle == DiagnosticLifecycle::Resolved));
    assert!(
        report
            .incidents
            .relationships
            .iter()
            .all(|link| link.availability == IncidentEvidenceAvailability::Authenticated)
    );
    assert!(!report.incidents.incidents.is_empty());
    assert!(
        report.workload.analysis().unwrap().sections.is_empty(),
        "native activity is not manufactured into query runtime intervals"
    );
    assert!(
        report.published.iter().any(|analysis| !analysis.native_window_qualified),
        "unplaced native input clocks remain explicit"
    );
    assert!(report.published.iter().all(|analysis| matches!(analysis.rules, WindowPublishedRules::OtherRules { .. })));
    assert_eq!(fixture.objects.snapshot(), before);
}

#[test]
fn window_query_and_clock_qualification_require_explicit_utc_without_acquisition_substitution() {
    let clock =
        WindowNativeClock::Observation { from: "2020-01-01T06:00:00Z".into(), until: "2020-01-01T06:00:00Z".into() };
    let window = PublishedResourceWindow { from: "2020-01-01T06:00:00Z".into(), until: "2020-01-01T07:00:00Z".into() };
    assert_eq!(clock.overlaps(&window).unwrap(), Some(true));
    assert_eq!(WindowNativeClock::Unknown.overlaps(&window).unwrap(), None);
    let offset_free =
        WindowNativeClock::Observation { from: "2020-01-01T06:00:00".into(), until: "2020-01-01T06:00:00".into() };
    assert!(offset_free.overlaps(&window).is_err());
    let after = WindowNativeClock::Interval { from: window.until.clone(), until: "2020-01-01T08:00:00Z".into() };
    assert_eq!(after.overlaps(&window).unwrap(), Some(false));
}

#[tokio::test]
async fn cpu_interpretation_awaits_its_one_owner_and_withholds_a_result_after_cancellation() {
    let (fixture, _) = fixture().await;
    let cancellation = CollectionCancellation::default();
    let mut session = fixture.service.session(EvidenceReadBudget::default(), &cancellation).unwrap();
    let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = active.clone();
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_finished = finished.clone();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let task = session.analyze(move |fence| {
        started.store(true, Ordering::SeqCst);
        while fence.check().is_ok() {
            std::thread::yield_now();
        }
        worker_finished.store(true, Ordering::SeqCst);
        Ok(42u32)
    });
    let (result, _) = tokio::join!(task, async {
        while !active.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });
    assert!(finished.load(Ordering::SeqCst), "the admitted CPU owner settles before the action returns");
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(
        fixture.store.requests.load(Ordering::SeqCst),
        before,
        "pure interpretation makes no warehouse/source call"
    );
}

#[derive(Clone, Copy)]
enum PublishedCase {
    Genuine,
    ForgedPosition,
    ConflictingBody,
}
async fn publish_current_findings(fixture: &Fixture, sequence: u64, case: PublishedCase) {
    let token = CollectionCancellation::default();
    let mut session = fixture.service.session(IncidentRequest::default_budget(), &token).unwrap();
    let original = session
        .read(
            &fixture.request.range_id,
            &[
                EvidenceFamily::QueryRuntime,
                EvidenceFamily::QueryWaits,
                EvidenceFamily::QueryDefinitions,
                EvidenceFamily::PlanDefinitions,
            ],
        )
        .await
        .unwrap();
    let synthesizer = crate::ChecksSynthesizer::new(
        std::sync::Arc::new(culprit_checks::Pipeline::builtin()),
        crate::checks_synthesis::BUILTIN_RULES_VERSION,
    )
    .unwrap();
    let display =
        DerivedDisplayContext { connection_alias: "window-source".into(), environment: None, database_name: None };
    let mut diagnosis = synthesizer.synthesize_captures(&original, &[], original.collected_until(), &display).unwrap();
    assert!(!diagnosis.findings.is_empty(), "the actual installed pure rules produce this controlled finding");
    if matches!(case, PublishedCase::ForgedPosition) {
        let reference = diagnosis.findings[0].inputs.first_mut().expect("canonical finding has row evidence");
        reference.row_ordinal += 1;
        assert!(reference.is_valid(), "the forged row pointer is structurally valid, but not authenticated");
    }
    if matches!(case, PublishedCase::ConflictingBody) {
        diagnosis.findings[0].finding.summary.push_str(" Conflicting imported interpretation.");
    }
    diagnosis.validate().unwrap();
    let source = CollectionDescription {
        target: original.source().target.clone(),
        partition: original.source().partition.clone(),
        source: DERIVED_DIAGNOSIS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::DerivedDiagnosis(Box::new(diagnosis.description.clone())),
    };
    fixture.append_capture(source, diagnosis.into_records().collect(), sequence).await;
}

#[tokio::test]
async fn actual_published_findings_and_raw_window_analysis_reuse_the_same_authenticated_input() {
    let (fixture, request) = fixture().await;
    publish_current_findings(&fixture, 100_001, PublishedCase::Genuine).await;
    let before = fixture.objects.snapshot();
    let report = fixture
        .service
        .window(&request, IncidentRequest::default_budget(), &CollectionCancellation::default())
        .await
        .unwrap();
    let published = report.published.iter().find(|analysis| !analysis.findings.is_empty()).unwrap();
    assert!(published.read_complete);
    assert!(published.native_window_qualified);
    assert!(matches!(published.rules, WindowPublishedRules::MatchesCurrentBuiltin));
    assert_eq!(report.workload.analysis().unwrap().sections.len(), 1);
    assert!(report.gaps.iter().all(|gap| !matches!(gap, WarehouseWindowGap::RawWithoutPublishedAnalysis { .. })));
    assert_eq!(fixture.objects.snapshot(), before);
}

#[tokio::test]
async fn a_canonically_published_finding_cannot_substitute_another_original_row_position() {
    let (fixture, request) = fixture().await;
    publish_current_findings(&fixture, 100_001, PublishedCase::ForgedPosition).await;
    let before = fixture.objects.snapshot();
    let error = fixture
        .service
        .window(&request, IncidentRequest::default_budget(), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    assert_eq!(fixture.objects.snapshot(), before);
}

#[tokio::test]
async fn a_changed_immutable_manifest_is_refused_before_any_derived_or_native_window_result() {
    let (fixture, request) = fixture().await;
    fixture.objects.corrupt_manifest();
    let error = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
}

#[tokio::test]
async fn public_window_future_keeps_archive_and_analysis_state_in_its_owned_heap_frame() {
    let (fixture, request) = fixture().await;
    let cancellation = CollectionCancellation::default();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let read = fixture.service.window(&request, EvidenceReadBudget::default(), &cancellation);
    assert!(
        std::mem::size_of_val(&read) <= 4096,
        "nested canonical readers must not inflate every client caller's stack"
    );
    drop(read);
    assert_eq!(
        fixture.store.requests.load(Ordering::SeqCst),
        before,
        "an unpolled capability makes no native/warehouse call"
    );
}

#[tokio::test]
async fn identical_complete_gold_copies_retain_both_receipts_without_multiplying_findings() {
    let (fixture, request) = fixture().await;
    publish_current_findings(&fixture, 100_001, PublishedCase::Genuine).await;
    publish_current_findings(&fixture, 100_002, PublishedCase::Genuine).await;
    let report = fixture
        .service
        .window(&request, IncidentRequest::default_budget(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(report.published.len(), 1);
    assert_eq!(report.published[0].copies.len(), 2);
    assert_ne!(report.published[0].copies[0], report.published[0].copies[1]);
    assert_eq!(report.published[0].findings.len(), report.published[0].context.description.counts.findings as usize);
}

#[tokio::test]
async fn conflicting_complete_imports_with_the_same_analysis_identity_are_not_ranked_by_arrival() {
    let (fixture, request) = fixture().await;
    publish_current_findings(&fixture, 100_001, PublishedCase::Genuine).await;
    publish_current_findings(&fixture, 100_002, PublishedCase::ConflictingBody).await;
    let error = fixture
        .service
        .window(&request, IncidentRequest::default_budget(), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Conflict)));
}

#[tokio::test]
async fn dropped_window_caller_cannot_release_the_original_read_slot_before_its_cpu_owner_settles() {
    let (fixture, _) = fixture().await;
    let fixture = Arc::new(fixture);
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = crate::evidence::EvidenceReadPermit::from(gate.clone().acquire_owned().await.unwrap());
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let settled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_settled = settled.clone();
    let reading = fixture.clone();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let task = tokio::spawn(async move {
        let cancellation = CollectionCancellation::default();
        let mut session =
            reading.service.session(EvidenceReadBudget::default(), &cancellation).unwrap().retaining(Some(permit));
        session
            .analyze(move |_| {
                entered.send(()).unwrap();
                released.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
                worker_settled.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), started).await.unwrap().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(gate.available_permits(), 0, "the dropped awaiter cannot release the actual worker's slot");
    assert!(gate.clone().try_acquire_owned().is_err());
    assert!(!settled.load(Ordering::SeqCst));
    release.send(()).unwrap();
    let _next =
        tokio::time::timeout(std::time::Duration::from_secs(2), gate.clone().acquire_owned()).await.unwrap().unwrap();
    assert!(settled.load(Ordering::SeqCst));
    assert_eq!(
        fixture.store.requests.load(Ordering::SeqCst),
        before,
        "this is the actual pure CPU owner, with no read"
    );
}

#[tokio::test(start_paused = true)]
async fn started_cpu_uses_the_original_action_deadline_and_cannot_publish_after_it_expires() {
    let (fixture, _) = fixture().await;
    let cancellation = CollectionCancellation::default();
    let budget = EvidenceReadBudget { timeout_ms: 100, ..Default::default() };
    let mut session = fixture.service.session(budget, &cancellation).unwrap();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let settled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_settled = settled.clone();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let work = session.analyze(move |_| {
        entered.send(()).unwrap();
        released.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        worker_settled.store(true, Ordering::SeqCst);
        Ok(42)
    });
    let (result, _) = tokio::join!(work, async {
        started.await.unwrap();
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        assert!(!settled.load(Ordering::SeqCst));
        release.send(()).unwrap();
    });
    assert!(settled.load(Ordering::SeqCst), "timeout refuses publication only after the admitted worker settles");
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::TimedOut)));
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn concurrent_caller_languages_reach_the_same_owned_cpu_boundary_without_process_language_mutation() {
    async fn render(code: &str) -> (String, String) {
        let (fixture, _) = fixture().await;
        let cancellation = CollectionCancellation::default();
        culprit_core::i18n::scope_async(code, async {
            let mut session = fixture.service.session(EvidenceReadBudget::default(), &cancellation).unwrap();
            session
                .analyze(|_| {
                    Ok((culprit_core::i18n::current().code.clone(), culprit_core::t!("core.changes.unavailable")))
                })
                .await
                .unwrap()
        })
        .await
    }
    let expected = |code| culprit_core::i18n::scope(code, || culprit_core::t!("core.changes.unavailable"));
    let (french, english) = tokio::join!(render("fr"), render("en"));
    assert_ne!(expected("fr"), expected("en"), "this catalog pin must exercise translated text");
    assert_eq!(french, ("fr".into(), expected("fr")));
    assert_eq!(english, ("en".into(), expected("en")));
    assert_eq!(render("en").await, english, "a reused blocking thread must restore its scoped language");
}

#[tokio::test]
async fn window_telemetry_projects_authenticated_resource_samples_within_compatible_workload_action() {
    let (fixture, request) = fixture().await;
    let (base_source, _) = crate::collected_evidence::window_test_original(6, Some("window-original-epoch"));
    let resource_source = CollectionDescription {
        partition: base_source.partition,
        target: base_source.target,
        source: "native-resources".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::RetainedSamples(SampleHistoryDescription {
            from: "2020-01-01T06:00:00Z".into(),
            until: "2020-01-01T06:15:00Z".into(),
            observed_at: "2020-01-01T06:16:00Z".into(),
            available_from: Some("2020-01-01T05:55:00Z".into()),
            available_until: Some("2020-01-01T06:15:30Z".into()),
            expected_samples: 2,
            nominal_period_ms: Some(15_000),
        }),
    };
    let make_sample = |at: &str, cpu: f64, mem: f64| {
        TelemetryRecord::ResourceSample(ResourceMeasurement {
            mssql_managed_instance: None,
            data_io_accounting: DataIoAccounting::DataFiles,
            at: at.into(),
            replica_id: None,
            replica_role: None,
            metrics: vec![
                ResourceMetricSample {
                    key: "cpu".into(),
                    unit: ResourceUnit::Percent,
                    scope: MeasurementScope::Database,
                    statistic: ResourceStatistic::Mean,
                    period_ms: Some(15_000),
                    limit_basis: ResourceLimitBasis::DatabaseLimit,
                    value: Some(cpu),
                },
                ResourceMetricSample {
                    key: "memory".into(),
                    unit: ResourceUnit::Percent,
                    scope: MeasurementScope::Database,
                    statistic: ResourceStatistic::Mean,
                    period_ms: Some(15_000),
                    limit_basis: ResourceLimitBasis::DatabaseLimit,
                    value: Some(mem),
                },
            ],
        })
    };
    fixture
        .append_capture(
            resource_source,
            vec![make_sample("2020-01-01T06:05:00Z", 74.5, 68.0), make_sample("2020-01-01T06:00:15Z", 42.0, 64.0)],
            100_050,
        )
        .await;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    let telemetry = report.telemetry.as_ref().expect("resource samples populate window telemetry");
    assert_eq!(telemetry.resources.len(), 1);
    assert!(telemetry.resources[0].read_complete);
    assert_eq!(telemetry.resources[0].samples.len(), 2);
    assert_eq!(telemetry.resources[0].samples[0].at, canonical_time("2020-01-01T06:00:15Z").unwrap());
    assert_eq!(telemetry.resources[0].samples[0].cpu_pct, Some(42.0));
    assert_eq!(telemetry.resources[0].samples[1].at, canonical_time("2020-01-01T06:05:00Z").unwrap());
    assert_eq!(telemetry.resources[0].samples[1].cpu_pct, Some(74.5));
    assert_eq!(telemetry.resources[0].samples[1].memory_pct, Some(68.0));
}

mod volume;

mod missing;

mod placement;

mod clients;

mod artifacts;
