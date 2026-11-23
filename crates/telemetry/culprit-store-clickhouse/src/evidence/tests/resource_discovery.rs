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
        database: None,
        clock: culprit_evidence::analytical::evidence::PublishedResourceClock::Acquisition,
        content: culprit_evidence::analytical::evidence::PublishedResourceContent::All,
    }
}
#[tokio::test]
async fn published_resource_search_uses_existing_table_parameterized_namespace_window_and_paid_lookahead() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(ClickHouseReadStore::new(query.clone()).unwrap());
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
    query.replies.lock().unwrap().push_back(vec![]);
    let page = reader.resource_candidates(&contract, &request, EvidenceReadBudget::default()).await.unwrap();
    assert!(page.candidates.is_empty() && page.next.is_none());
    assert_eq!(page.metrics.queries, 1);
    assert_eq!(page.metrics.selected_rows, 0);
    {
        let requests = query.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].row_limit, 3);
        assert_eq!(requests[0].byte_limit, EvidenceReadBudget::default().page_bytes as usize);
        assert!(
            requests[0].text.contains("CulpritPublishedRangesV1")
                && requests[0].text.contains("RecoveryStoreId")
                && requests[0].text.contains("ResourceKind")
        );
        assert!(!requests[0].text.contains(&request.recovery_store_id) && !requests[0].text.contains("2026-11-22"));
        assert!(
            requests[0].parameters.contains_key("contract")
                && requests[0].parameters.contains_key("recovery")
                && requests[0].parameters.contains_key("as_of")
        );
        assert!(requests[0].text.contains("CollectedFrom") && requests[0].text.contains("CollectedUntil"));
    }
    request.recovery_store_id = digest(b"different namespace");
    assert!(reader.resource_candidates(&contract, &request, EvidenceReadBudget::default()).await.is_err());
    assert_eq!(query.requests.lock().unwrap().len(), 1, "foreign cursor refuses before store dispatch");
}
#[tokio::test]
async fn genuine_publication_projection_roundtrips_through_the_existing_physical_decoder() {
    let (source, records) = test_support::informational_query_evidence();
    let fixture = Fixture::new_for_streams(
        source.clone(),
        records,
        &[TelemetryStream::QueryRuntime, TelemetryStream::QueryWaits],
    )
    .await;
    let mut request = request();
    request.selection.window = None;
    request.recovery_store_id = fixture.range.store_id().into();
    fixture.query.replies.lock().unwrap().push_back(vec![fixture.publication.clone()]);
    let page = fixture
        .reader
        .resource_candidates(&fixture.range.manifest().contract, &request, EvidenceReadBudget::default())
        .await
        .unwrap();
    assert_eq!(page.candidates.len(), 1);
    assert!(page.candidates[0].matches_original(&fixture.range).unwrap());
    assert_eq!(page.candidates[0].position.resource_id, source.target.telemetry_id().unwrap());
    assert_eq!(
        page.candidates[0].database,
        Some(culprit_core::IncidentDatabaseSelector::from_route(source.target.database().unwrap()).unwrap())
    );
    assert_eq!(page.candidates[0].position.source_id, source.captured_range().unwrap().scope_id);
    assert_eq!(page.metrics.recovery_objects, 0, "candidate discovery does not read native bodies");
    assert!(page.next.is_none());
}

#[tokio::test]
async fn selected_native_window_uses_the_same_publication_owner_and_preserves_late_unknown_and_context_candidates() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(ClickHouseReadStore::new(query.clone()).unwrap());
    let contract =
        Contract::new("pipeline".into(), "shard".into(), store.destination().into(), store.catalog().digest().into())
            .unwrap();
    let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(Arc::new(Blobs::default()))));
    let mut request = request();
    let database = culprit_core::IncidentDatabaseSelector::from_key(&digest(b"selected database route")).unwrap();
    request.database = Some(database.clone());
    request.clock = culprit_evidence::analytical::evidence::PublishedResourceClock::NativeWindowCandidates;
    query.replies.lock().unwrap().push_back(vec![]);
    let page = reader.resource_candidates(&contract, &request, EvidenceReadBudget::default()).await.unwrap();
    assert_eq!(page.metrics.queries, 1);
    let calls = query.requests.lock().unwrap();
    let [call] = calls.as_slice() else { panic!() };
    assert_eq!(call.row_limit, 3);
    assert!(!call.text.contains(database.key()), "route remains a bound value, not interpolated SQL/KQL");
    assert_eq!(call.parameters["database"], database.key());
    assert!(call.text.contains("DatabaseSelector={database:String}"));
    assert!(call.text.contains("isNull(IntervalFrom) OR isNull(IntervalUntil)"));
    assert!(call.text.contains("IntervalFrom<=fromUnixTimestamp64Nano({until:Int64})"));
    assert!(!call.text.contains("AND CollectedFrom<fromUnixTimestamp64Nano({until:Int64})"));
}

#[tokio::test]
async fn published_answer_headers_use_the_existing_clickhouse_query_with_bound_observation_kind() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(ClickHouseReadStore::new(query.clone()).unwrap());
    let contract =
        Contract::new("pipeline".into(), "shard".into(), store.destination().into(), store.catalog().digest().into())
            .unwrap();
    let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(Arc::new(Blobs::default()))));
    let mut request = request();
    request.content = culprit_evidence::analytical::evidence::PublishedResourceContent::DerivedDiagnosis;
    query.replies.lock().unwrap().push_back(vec![]);
    let page = reader.resource_candidates(&contract, &request, EvidenceReadBudget::default()).await.unwrap();
    assert_eq!(page.metrics.queries, 1);
    let calls = query.requests.lock().unwrap();
    assert!(calls[0].text.contains("JSONExtractString(SourceDescription,'observation','kind')={observation:String}"));
    assert!(!calls[0].text.contains("derived-diagnosis"));
    assert_eq!(calls[0].parameters["observation"], "derived-diagnosis");
    assert_eq!(calls[0].row_limit, request.selection.page_rows as usize + 1);
}
#[tokio::test]
async fn actual_derived_publication_headers_survive_the_same_decoder_while_raw_hints_are_refused() {
    let (source, output) = test_support::empty_derived_diagnosis_output(true);
    let fixture = Fixture::new_for_streams(
        source,
        output.into_records().collect(),
        &[TelemetryStream::DerivedIncidents, TelemetryStream::DerivedFindings, TelemetryStream::DerivedChanges],
    )
    .await;
    let mut request = request();
    request.selection.window = None;
    request.recovery_store_id = fixture.range.store_id().into();
    request.content = culprit_evidence::analytical::evidence::PublishedResourceContent::DerivedDiagnosis;
    fixture.query.replies.lock().unwrap().push_back(vec![fixture.publication.clone()]);
    let page = fixture
        .reader
        .resource_candidates(&fixture.range.manifest().contract, &request, EvidenceReadBudget::default())
        .await
        .unwrap();
    assert_eq!(page.candidates.len(), 1);
    assert!(page.candidates[0].matches_original(&fixture.range).unwrap());
    let (source, rows) = test_support::informational_query_evidence();
    let raw =
        Fixture::new_for_streams(source, rows, &[TelemetryStream::QueryRuntime, TelemetryStream::QueryWaits]).await;
    request.recovery_store_id = raw.range.store_id().into();
    raw.query.replies.lock().unwrap().push_back(vec![raw.publication.clone()]);
    assert!(
        raw.reader
            .resource_candidates(&raw.range.manifest().contract, &request, EvidenceReadBudget::default())
            .await
            .is_err()
    );
}
