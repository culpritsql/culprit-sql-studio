use super::*;
use culprit_evidence::analytical::evidence::EvidenceGapReason;

#[tokio::test]
async fn missing_native_input_is_an_explicit_unplaced_partial_published_answer_without_current_or_neighbor_substitution()
 {
    let (fixture, mut request) = fixture().await;
    publish_current_findings(&fixture, 100_100, PublishedCase::Genuine).await;
    fixture.objects.remove_range_index(&fixture.request.range_id);
    request.view = WarehouseWindowView::PublishedAnswers;
    let before = fixture.objects.snapshot();
    let row_start = fixture.store.row_requests.lock().unwrap().len();
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(matches!(report.workload, WindowWorkload::NotRequested));
    assert_eq!(report.published.len(), 1);
    let published = &report.published[0];
    assert!(
        !published.findings.is_empty(),
        "the actual sealed interpretation remains available with its unavailable lineage"
    );
    assert!(!published.read_complete);
    assert!(!published.native_window_qualified);
    assert!(matches!(published.native_clock, WindowNativeClock::Unknown));
    assert!(report.gaps.iter().any(|gap| matches!(gap, WarehouseWindowGap::UnavailableAnalysisInputs { .. })));
    assert!(report.incidents.gaps.iter().any(|gap| matches!(gap, IncidentReadGap::CaptureUnavailable { gaps, .. }
        if gaps.iter().any(|gap| gap.reason == EvidenceGapReason::OriginalNotCommitted))));
    assert!(
        fixture.store.row_requests.lock().unwrap()[row_start..].iter().all(|(stream, _, _)| matches!(
            stream,
            TelemetryStream::DerivedIncidents | TelemetryStream::DerivedFindings | TelemetryStream::DerivedChanges
        )),
        "missing native data cannot be recovered by opening neighboring raw rows"
    );
    assert_eq!(fixture.objects.snapshot(), before);
}
