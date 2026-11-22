use super::*;
use culprit_core::{
    PublishedResourceCursor, PublishedResourcePosition, PublishedResourceRequest, PublishedResourceWindow,
};
use culprit_evidence::analytical::evidence::PublishedResourceQuery;
use culprit_evidence::delivery::contract::Contract;
fn request() -> PublishedResourceQuery {
    PublishedResourceQuery {
        selection: PublishedResourceRequest {
            as_of: "2026-11-22T12:00:00Z".into(),
            window: Some(PublishedResourceWindow {
                from: "2020-01-01T00:00:00Z".into(),
                until: "2026-11-22T12:00:00Z".into(),
            }),
            after: None,
            page_rows: 2,
        },
        recovery_store_id: digest(b"actual authenticated recovery namespace"),
    }
}
#[tokio::test]
async fn published_resource_search_uses_existing_table_parameterized_namespace_window_and_paid_lookahead() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(AdxReadStore::new(query.clone()).unwrap());
    let contract =
        Contract::new("pipeline".into(), "shard".into(), store.destination().into(), store.catalog().digest().into())
            .unwrap();
    let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(Arc::new(Blobs::default()))));
    let mut request = request();
    request.selection.after = Some(PublishedResourceCursor {
        selection_key: request.key(&contract).unwrap(),
        position: PublishedResourcePosition {
            resource_id: digest(b"resource"),
            source_id: digest(b"source"),
            range_id: digest(b"range"),
        },
    });
    query.replies.lock().unwrap().push_back(Reply::Rows(vec![]));
    let page = reader.resource_candidates(&contract, &request, budget()).await.unwrap();
    assert!(page.candidates.is_empty() && page.next.is_none());
    assert_eq!(page.metrics.queries, 1);
    assert_eq!(page.metrics.selected_rows, 0);
    let requests = query.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].row_limit, 3);
    assert_eq!(requests[0].byte_limit, budget().page_bytes as usize);
    assert!(
        requests[0].text.contains("CulpritPublishedRangesV1")
            && requests[0].text.contains("RecoveryStoreId")
            && requests[0].text.contains("ResourceKind")
    );
    assert!(!requests[0].text.contains(&request.recovery_store_id) && !requests[0].text.contains("2026-11-22"));
    assert!(
        requests[0].parameters.contains_key("pContract")
            && requests[0].parameters.contains_key("pRecovery")
            && requests[0].parameters.contains_key("pAsOf")
    );
    assert!(requests[0].text.contains("CollectedFrom") && requests[0].text.contains("CollectedUntil"));
    drop(requests);
    request.recovery_store_id = digest(b"different namespace");
    assert!(reader.resource_candidates(&contract, &request, budget()).await.is_err());
    assert_eq!(query.requests.lock().unwrap().len(), 1, "foreign cursor refuses before store dispatch");
}
#[tokio::test]
async fn genuine_publication_projection_roundtrips_through_the_existing_physical_decoder() {
    let (source, records) = test_support::informational_query_evidence();
    let fixture = Fixture::new(source.clone(), records).await;
    let mut request = request();
    request.selection.window = None;
    request.recovery_store_id = fixture.range.store_id().into();
    fixture.query.replies.lock().unwrap().push_back(Reply::Rows(vec![fixture.publication.clone()]));
    let page =
        fixture.reader().resource_candidates(&fixture.range.manifest().contract, &request, budget()).await.unwrap();
    assert_eq!(page.candidates.len(), 1);
    assert!(page.candidates[0].matches_original(&fixture.range).unwrap());
    assert_eq!(page.candidates[0].position.resource_id, source.target.telemetry_id().unwrap());
    assert_eq!(page.candidates[0].position.source_id, source.captured_range().unwrap().scope_id);
    assert_eq!(page.metrics.recovery_objects, 0, "candidate discovery does not read native bodies");
    assert!(page.next.is_none());
}
