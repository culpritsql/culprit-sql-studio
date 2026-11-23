//! Genuine archive original proofs exercise friendly resolution and injected source-free client delegation.
use super::*;
use crate::evidence::NamedWindowQuery;
#[tokio::test]
async fn friendly_window_name_and_analysis_share_one_namespace_deadline_and_cumulative_read_allowance() {
    let (fixture, mut query) = fixture().await;
    publish_current_findings(&fixture, 100_100, PublishedCase::Genuine).await;
    query.view = WarehouseWindowView::PublishedAnswers;
    let resources = fixture
        .service
        .resources(
            &PublishedResourceRequest::default(),
            EvidenceReadBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    let name = &resources.resources[0].database_name;
    let request = NamedWindowQuery {
        resource_name: name,
        window: &query.window,
        as_of: &query.as_of,
        view: query.view,
        page_rows: query.page_rows,
        after: None,
    };
    let calls = fixture.store.requests.load(Ordering::SeqCst);
    let before = fixture.objects.snapshot();
    let mut report = fixture
        .service
        .window_named(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(report.request.resource, query.resource);
    assert_eq!(
        report.archive_work.queries as usize,
        fixture.store.requests.load(Ordering::SeqCst) - calls,
        "name lookup, namespace, publication and exact row proofs are one allowance"
    );
    assert!(!report.published[0].findings.is_empty());
    assert!(report.qualifications.iter().all(|reason| !reason.contains(&fixture.request.range_id)));
    assert!(!report.published[0].findings[0].placement.qualification.is_empty());
    let before_refresh = serde_json::to_value(&report).unwrap();
    report.prepare_for_reader();
    assert_eq!(
        serde_json::to_value(&report).unwrap(),
        before_refresh,
        "reader projection is idempotent and never changes native evidence"
    );
    assert_eq!(fixture.objects.snapshot(), before);
    let budget = EvidenceReadBudget { max_queries: 1, ..Default::default() };
    let calls = fixture.store.requests.load(Ordering::SeqCst);
    let error = fixture.service.window_named(&request, budget, &CollectionCancellation::default()).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert!(
        fixture.store.requests.load(Ordering::SeqCst) <= calls + 1,
        "analysis cannot renew the discovery allowance"
    );
}
#[tokio::test]
async fn the_injected_window_capability_uses_actual_canonical_published_answers_and_explicit_continuation() {
    let (fixture, mut query) = fixture().await;
    for sequence in [100_100, 100_101] {
        publish_current_findings(&fixture, sequence, PublishedCase::Genuine).await;
    }
    query.view = WarehouseWindowView::PublishedAnswers;
    let first = crate::WarehouseIncidentReader::window(&fixture.service, &query, &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(first.next.is_some());
    assert!(!first.discovery_complete);
    assert!(first.qualifications.iter().any(|reason| reason == &t!("window.reader.more")));
    let first_capture = first.published[0].context.capture.clone();
    query.after = first.next;
    let second = crate::WarehouseIncidentReader::window(&fixture.service, &query, &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(second.next.is_none());
    assert!(second.discovery_complete);
    assert_ne!(second.published[0].context.capture, first_capture);
    assert_eq!(second.request.as_of, query.as_of);
    assert!(matches!(second.workload, WindowWorkload::NotRequested));
}
#[tokio::test]
async fn invalid_friendly_names_do_not_open_unrelated_or_native_work() {
    let (fixture, query) = fixture().await;
    let calls = fixture.store.requests.load(Ordering::SeqCst);
    let request = NamedWindowQuery {
        resource_name: "unsafe\nname",
        window: &query.window,
        as_of: &query.as_of,
        view: WarehouseWindowView::PublishedAnswers,
        page_rows: 1,
        after: None,
    };
    assert!(
        fixture
            .service
            .window_named(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
            .await
            .is_err()
    );
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), calls);
}
