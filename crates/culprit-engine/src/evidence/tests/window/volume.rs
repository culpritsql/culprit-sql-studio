//! The normal published-answer view does not silently reconstruct a busy raw history. Same canonical archive/EOF.
use super::*;
use crate::collected_resources::references;

fn add_query_roster(source: &CollectionDescription, rows: &mut Vec<TelemetryRecord>, count: u32) {
    let template = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(query) => Some(query.clone()),
            _ => None,
        })
        .unwrap();
    for number in 0..count {
        let mut query = template.clone();
        query.identity.query_id = (1000 + number).to_string();
        query.id = query.identity.definition_id(source).unwrap();
        rows.push(TelemetryRecord::QueryDefinition(query));
    }
}
async fn busy_archive() -> (Fixture, WarehouseWindowQuery) {
    let (source, mut rows) = crate::collected_evidence::window_test_original(6, Some("window-original-epoch"));
    add_query_roster(&source, &mut rows, 1000);
    let fixture = Fixture::capture(source, rows).await;
    for (sequence, hour) in (7..18).enumerate() {
        let (source, mut rows) = crate::collected_evidence::window_test_original(hour, Some("window-original-epoch"));
        add_query_roster(&source, &mut rows, 1000);
        fixture.append_capture(source, rows, 100_001 + sequence as u64).await;
    }
    publish_current_findings(&fixture, 100_100, PublishedCase::Genuine).await;
    let resources = fixture
        .service
        .resources(
            &PublishedResourceRequest { page_rows: 1, ..Default::default() },
            EvidenceReadBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    let request = WarehouseWindowQuery {
        resource: resources.resources[0].selector.clone(),
        window: PublishedResourceWindow { from: "2020-01-01T06:00:00Z".into(), until: "2020-01-01T18:00:00Z".into() },
        as_of: "2030-01-01T00:00:00Z".into(),
        view: WarehouseWindowView::PublishedAnswers,
        page_rows: DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS,
        after: None,
    };
    (fixture, request)
}

#[tokio::test]
async fn default_published_answers_keep_large_raw_rosters_unselected_and_explicit_workload_keeps_its_original_limits() {
    let (fixture, mut request) = busy_archive().await;
    let row_start = fixture.store.row_requests.lock().unwrap().len();
    let query_start = fixture.store.requests.load(Ordering::SeqCst);
    let before = fixture.objects.snapshot();
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(matches!(report.workload, WindowWorkload::NotRequested));
    assert!(report.discovery_complete);
    assert!(report.next.is_none());
    assert_eq!(report.published.len(), 1);
    assert!(!report.published[0].findings.is_empty(), "real existing rules supplied the published answer");
    assert!(report.published[0].read_complete);
    assert!(report.published[0].native_window_qualified);
    let queries = fixture.store.requests.load(Ordering::SeqCst) - query_start;
    assert_eq!(report.archive_work.queries as usize, queries, "one allowance includes discovery/publication/lineage");
    assert!(queries < 32, "one answer does not authenticate every unrelated raw runtime capture: {queries}");
    {
        let reads = fixture.store.row_requests.lock().unwrap();
        let native = reads[row_start..]
            .iter()
            .filter(|(stream, _, _)| {
                !matches!(
                    stream,
                    TelemetryStream::DerivedIncidents
                        | TelemetryStream::DerivedFindings
                        | TelemetryStream::DerivedChanges
                )
            })
            .collect::<Vec<_>>();
        assert!(!native.is_empty(), "actual emitted query/cause refs are still independently authenticated");
        assert!(native.iter().all(|(_, after, until)| until - after <= 3), "no entire1000-query family: {native:?}");
        assert!(report.archive_work.selected_rows < 32);
        assert_eq!(report.archive_work.recovery_objects, 0);
        assert_eq!(fixture.objects.snapshot(), before);
    }
    request.view = WarehouseWindowView::CompatibleWorkload;
    let error = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)),
        "twelve1000-query originals exceed the unchanged whole-action row/proof allowance; no smaller prefix is called a full workload"
    );
    assert_eq!(EvidenceReadBudget::default().max_rows, 10_000);
    assert_eq!(EvidenceReadBudget::default().max_queries, 128);
    assert_eq!(EvidenceReadBudget::default().max_proof_bytes, 4 * 1024 * 1024);
    assert_eq!(fixture.objects.snapshot(), before);
}

#[tokio::test]
async fn published_pages_keep_fixed_namespace_window_cutoff_and_explicit_continuation_without_runtime_reconstruction() {
    let (fixture, mut request) = fixture().await;
    for sequence in [100_100, 100_101, 100_102] {
        publish_current_findings(&fixture, sequence, PublishedCase::Genuine).await;
    }
    request.view = WarehouseWindowView::PublishedAnswers;
    let mut pages = Vec::new();
    loop {
        let report = fixture
            .service
            .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
            .await
            .unwrap();
        assert!(matches!(report.workload, WindowWorkload::NotRequested));
        assert_eq!(report.published.len(), 1);
        assert_eq!(report.request.as_of, request.as_of);
        assert_eq!(report.discovery_complete, report.next.is_none());
        assert_eq!(
            report.gaps.iter().any(|gap| matches!(gap, WarehouseWindowGap::MorePublishedAnalyses)),
            report.next.is_some()
        );
        pages.push(report.published[0].context.capture.capture.clone());
        let Some(next) = report.next else { break };
        if let Some(previous) = &request.after {
            assert!(next.position > previous.position);
        }
        request.after = Some(next);
    }
    assert_eq!(pages.len(), 3);
    let mut sorted = pages.clone();
    sorted.sort_by_key(|capture| capture.observation_id.clone());
    sorted.dedup();
    assert_eq!(sorted.len(), 3, "no repeated or silently skipped canonical analysis captures");
    request.window.until = "2020-01-01T09:00:00Z".into();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    assert!(
        fixture
            .service
            .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
            .await
            .is_err()
    );
    assert_eq!(
        fixture.store.requests.load(Ordering::SeqCst),
        before,
        "a cursor for another window fails before warehouse discovery"
    );
}

#[tokio::test]
async fn repeated_gold_links_share_one_exact_row_selection_and_do_not_reopen_native_families() {
    let (fixture, mut request) = fixture().await;
    for sequence in [100_100, 100_101] {
        publish_current_findings(&fixture, sequence, PublishedCase::Genuine).await;
    }
    request.view = WarehouseWindowView::PublishedAnswers;
    request.page_rows = 2;
    let start = fixture.store.row_requests.lock().unwrap().len();
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(report.published.len(), 1);
    assert_eq!(report.published[0].copies.len(), 2);
    let reads = fixture.store.row_requests.lock().unwrap();
    let mut by_stream: Vec<(TelemetryStream, usize)> = Vec::new();
    for (stream, _, _) in &reads[start..] {
        if !matches!(
            stream,
            TelemetryStream::DerivedIncidents | TelemetryStream::DerivedFindings | TelemetryStream::DerivedChanges
        ) {
            if let Some((_, count)) = by_stream.iter_mut().find(|(known, _)| known == stream) {
                *count += 1;
            } else {
                by_stream.push((*stream, 1));
            }
        }
    }
    assert!(!by_stream.is_empty());
    assert!(
        by_stream.iter().all(|(_, count)| *count == 1),
        "same original row receipts are not acquired twice: {by_stream:?}"
    );
    assert!(matches!(report.workload, WindowWorkload::NotRequested));
}

#[tokio::test]
async fn selected_records_consume_the_same_action_rows_and_a_failed_allowance_cannot_be_reopened() {
    let (fixture, _) = fixture().await;
    let token = CollectionCancellation::default();
    let mut setup = fixture.service.session(EvidenceReadBudget::default(), &token).unwrap();
    let original = setup.read(&fixture.request.range_id, &[EvidenceFamily::QueryDefinitions]).await.unwrap();
    let reference = references::from_record(&original, &original.records()[0]).unwrap();
    let budget = EvidenceReadBudget { max_rows: 2, page_rows: 1, ..Default::default() };
    let mut session = fixture.service.session(budget, &token).unwrap();
    for _ in 0..2 {
        assert!(session.read_records(std::slice::from_ref(&reference)).await.unwrap().is_selection_complete());
    }
    assert_eq!(session.completed_metrics().selected_rows, 2);
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let error = session.read_records(std::slice::from_ref(&reference)).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
    assert!(session.read_records(&[reference]).await.is_err());
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn cancellation_of_exact_row_metadata_settles_the_existing_reader_and_cannot_publish_selected_rows() {
    let (fixture, _) = fixture().await;
    let token = CollectionCancellation::default();
    let mut setup = fixture.service.session(EvidenceReadBudget::default(), &token).unwrap();
    let original = setup.read(&fixture.request.range_id, &[EvidenceFamily::QueryDefinitions]).await.unwrap();
    let reference = references::from_record(&original, &original.records()[0]).unwrap();
    let mut session = fixture.service.session(EvidenceReadBudget::default(), &token).unwrap();
    fixture.objects.stall.store(true, Ordering::SeqCst);
    let (result, _) = tokio::join!(session.read_records(std::slice::from_ref(&reference)), async {
        while fixture.objects.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        token.cancel();
    });
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
    let before = fixture.store.requests.load(Ordering::SeqCst);
    assert!(session.read_records(&[reference]).await.is_err());
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn raw_only_history_is_not_reconstructed_or_presented_as_a_healthy_empty_published_answer() {
    let (fixture, mut request) = fixture().await;
    request.view = WarehouseWindowView::PublishedAnswers;
    let before = fixture.store.row_requests.lock().unwrap().len();
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(matches!(report.workload, WindowWorkload::NotRequested));
    assert!(report.published.is_empty());
    assert!(report.gaps.iter().any(|gap| matches!(gap, WarehouseWindowGap::NoPublishedAnalyses)));
    assert_eq!(fixture.store.row_requests.lock().unwrap().len(), before);
    assert!(report.next.is_none());
}
