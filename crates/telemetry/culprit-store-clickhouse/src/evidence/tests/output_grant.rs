use super::*;
#[tokio::test]
async fn original_output_grants_keep_the_same_native_namespace_caller_sql_null_and_partial_facts_through_canonical_proof_replay()
 {
    for &case in test_support::OutputGrantCase::ALL {
        let (source, records) = test_support::output_grant_evidence(case);
        let fixture =
            Fixture::new_for_stream(source.clone(), records.clone(), TelemetryStream::OperationalSamples).await;
        *fixture.recovery.fault.lock().unwrap() = Fault::None;
        fixture.replies(fixture.rows.chunks(1).map(<[_]>::to_vec).collect());
        let capture = fixture
            .reader
            .read_capture(
                &fixture.range,
                &[EvidenceFamily::OperationalSamples],
                EvidenceReadBudget {
                    page_rows: 1,
                    recovery_payloads: Some(RecoveryPayloadBudget::default()),
                    ..Default::default()
                },
                Some(fixture.recovery.as_ref()),
            )
            .await
            .unwrap();
        test_support::assert_operational_capture(&capture, &source, &records);
        assert!(capture.is_read_complete(), "retained EOF does not upgrade native coverage: {case:?}");
        assert_eq!(capture.source(), &source);
        assert_eq!(capture.source().partition, SourcePartition::Target);
        assert!(RequestMemorySnapshot::acquisition_context(capture.source()).unwrap().has_qualified_local_owner());
        assert!(!serde_json::to_string(&capture).unwrap().contains("output-private-canary"));
    }
}
