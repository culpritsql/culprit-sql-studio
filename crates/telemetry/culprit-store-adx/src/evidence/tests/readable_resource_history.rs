use super::*;
#[tokio::test]
async fn readable_member_same_clock_captures_keep_observation_ids_through_the_normal_publication_and_proof_reader() {
    let mut ids = std::collections::BTreeSet::new();
    for (sequence, member) in
        [(770010, "91487244-730d-4b83-8e49-d482241c7288"), (770011, "55555555-5555-5555-5555-555555555555")]
    {
        let (source, records) = test_support::readable_resource_history_case(Some(member));
        let catalog = Arc::new(SchemaCatalog::new().unwrap());
        let recovery = Recovery::capture("adx:test", catalog.digest(), source.clone(), records.clone(), sequence);
        let fixture = Fixture::from_recovery(catalog, Arc::new(recovery)).await;
        fixture.replies(vec![fixture.rows.clone()]);
        let captured = fixture
            .reader()
            .read_capture(&fixture.range, &[EvidenceFamily::ResourceSamples], EvidenceReadBudget::default(), None)
            .await
            .unwrap();
        assert!(captured.is_read_complete(), "{:?}", captured.gaps());
        assert_eq!(captured.source(), &source);
        assert_eq!(captured.records().len(), 1);
        let expected = ExportedRecord::new(&source, captured.capture(), records[0].clone()).unwrap();
        assert_eq!(captured.records()[0].row(), &expected);
        assert!(ids.insert(expected.record_id.clone().unwrap()));
        let TelemetryRecord::ResourceSample(sample) = &captured.records()[0].row().record else { panic!() };
        assert!(sample.replica_id.is_none());
        assert_eq!(
            captured.source().observation.native_coverage_state().unwrap(),
            Some(culprit_core::SourceState::Partial)
        );
        assert_eq!(captured.metrics().recovery_objects, 0);
        // Each read authenticates one exact RangeId/BatchId/ordinal. It does not pool these rows by DB/time.
    }
    assert_eq!(ids.len(), 2);
}
