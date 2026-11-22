use super::*;
use culprit_core::IncidentDatabaseSelector;
use culprit_evidence::delivery::contract::Contract;

fn request() -> IncidentDiscoveryRequest {
    IncidentDiscoveryRequest {
        selection: IncidentCandidateSelection::Around {
            database: IncidentDatabaseSelector::from_key(&digest(b"exact configured route")).unwrap(),
            from: "2020-01-01T12:00:00Z".into(),
            until: "2020-01-01T12:10:00Z".into(),
        },
        recorded_before: "2020-01-01T14:00:00Z".into(),
        after: None,
        page_rows: 2,
    }
}
#[tokio::test]
async fn database_time_discovery_is_parameterized_single_query_bounded_and_not_native_population_proof() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(ClickHouseReadStore::new(query.clone()).unwrap());
    let contract =
        Contract::new("pipeline".into(), "shard".into(), store.destination().into(), store.catalog().digest().into())
            .unwrap();
    let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(Arc::new(Blobs::default()))));
    query.replies.lock().unwrap().push_back(vec![]);
    let request = request();
    let result = reader.incident_candidates(&contract, &request, EvidenceReadBudget::default()).await.unwrap();
    assert!(result.candidates.is_empty() && result.next.is_none());
    assert_eq!(result.metrics.queries, 1);
    assert_eq!(result.metrics.selected_rows, 0, "candidate rows never masquerade as manifest-selected native rows");
    assert_eq!(result.metrics.recovery_objects, 0);
    let requests = query.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].row_limit, 3);
    assert_eq!(requests[0].byte_limit, EvidenceReadBudget::default().page_bytes as usize);
    assert!(!requests[0].text.contains(request.selection.database().key()));
    assert!(!requests[0].text.contains("2020-01-01"));
    assert!(requests[0].parameters["database"] == request.selection.database().key());
    assert!(requests[0].text.contains("OriginalObservedFrom") && requests[0].text.contains("OriginalIntervalFrom"));
    assert!(!requests[0].text.contains("NativeFirstSeenAt >=") && !requests[0].text.contains("NativeLastSeenAt >="));
}
#[tokio::test]
async fn exact_episode_cursor_does_not_narrow_unsigned_revisions_or_reuse_a_foreign_selection() {
    let query = Arc::new(Queries::default());
    let store = Arc::new(ClickHouseReadStore::new(query.clone()).unwrap());
    let contract =
        Contract::new("pipeline".into(), "shard".into(), store.destination().into(), store.catalog().digest().into())
            .unwrap();
    let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(Arc::new(Blobs::default()))));
    let mut request = request();
    request.selection = IncidentCandidateSelection::Episode {
        database: request.selection.database().clone(),
        episode_id: digest(b"original episode"),
        controller_version: u32::MAX,
    };
    request.after = Some(IncidentCandidateCursor {
        request_sha256: request.key().unwrap(),
        position: IncidentCandidatePosition {
            episode_id: digest(b"original episode"),
            controller_version: u32::MAX,
            revision: u64::MAX.to_string(),
            range_id: digest(b"one original range"),
            batch_id: digest(b"one original batch"),
            row_ordinal: 1,
        },
    });
    query.replies.lock().unwrap().push_back(vec![]);
    reader.incident_candidates(&contract, &request, EvidenceReadBudget::default()).await.unwrap();
    {
        let requests = query.requests.lock().unwrap();
        assert!(requests[0].text.contains("length(toString(Revision))"));
        assert!(!requests[0].text.contains(&u64::MAX.to_string()));
    }
    request.recorded_before = "2020-01-01T15:00:00Z".into();
    assert!(reader.incident_candidates(&contract, &request, EvidenceReadBudget::default()).await.is_err());
    assert_eq!(query.requests.lock().unwrap().len(), 1, "invalid cursor cannot dispatch a second query");
}

#[tokio::test]
async fn original_gold_incident_replays_through_existing_manifest_proof_and_whole_typed_decoder_with_partial_inputs_independent()
 {
    for partial in [false, true] {
        let (source, derived) = test_support::derived_diagnosis_output(false, partial);
        let records = derived.into_records().collect::<Vec<_>>();
        let fixture = Fixture::new_for_stream(source.clone(), records.clone(), TelemetryStream::DerivedIncidents).await;
        let rows = fixture.rows.clone();
        fixture.replies(vec![rows.clone()]);
        let capture = fixture
            .reader
            .read_capture(&fixture.range, &[EvidenceFamily::DerivedIncidents], EvidenceReadBudget::default(), None)
            .await
            .unwrap();
        assert!(capture.is_read_complete(), "{:?}", capture.gaps());
        assert_eq!(capture.records().len(), 1);
        let original = records.iter().find(|row| matches!(row, TelemetryRecord::DerivedIncident(_))).unwrap();
        assert_eq!(
            capture.records()[0].row(),
            &ExportedRecord::new(&source, capture.capture(), original.clone()).unwrap()
        );
        let SourceObservation::DerivedDiagnosis(header) = &capture.source().observation else { panic!() };
        assert_eq!(header.input_read_complete, !partial);
        let TelemetryRecord::DerivedIncident(row) = &capture.records()[0].row().record else { panic!() };
        assert!(row.culprit_queries.is_empty() && row.mechanism.is_none());
        assert_eq!(capture.metrics().recovery_objects, 0, "retained Gold DTOs do not read substitute native payloads");
        assert!(!serde_json::to_string(&capture.records()[0].row()).unwrap().contains("private-gold-canary"));
        let mut request = request();
        request.selection = IncidentCandidateSelection::Around {
            database: row.database.clone().unwrap(),
            from: "2020-01-01T12:00:00Z".into(),
            until: "2020-01-01T12:10:00Z".into(),
        };
        fixture.query.replies.lock().unwrap().push_back(rows);
        let candidates = fixture
            .reader
            .incident_candidates(&fixture.range.manifest().contract, &request, EvidenceReadBudget::default())
            .await
            .unwrap();
        assert_eq!(candidates.candidates.len(), 1);
        assert_eq!(candidates.candidates[0].original, row.revision_reference);
        assert_eq!(candidates.charged_candidate_rows, 1);
        assert_eq!(candidates.metrics.selected_rows, 0);
    }
}
