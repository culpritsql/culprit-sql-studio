use super::*;
#[tokio::test]
async fn canonical_output_blocking_keeps_native_point_roster_generations_and_context_without_native_reopening() {
    use test_support::OutputBlockingCaseFixture as Case;
    for case in [
        Case::Mssql,
        Case::PgRelation,
        Case::PgSessionOnly,
        Case::MissingSql,
        Case::Mars,
        Case::UnknownRoster,
        Case::Parallel,
        Case::LockPrefix,
    ] {
        let fixture_data = test_support::output_blocking_fixture(case);
        let source = fixture_data.source;
        let records = fixture_data.records;
        let fixture =
            Fixture::new_for_stream(source.clone(), records.clone(), TelemetryStream::ActivityObservations).await;
        fixture.replies(fixture.rows.chunks(1).map(<[_]>::to_vec).collect());
        let capture = fixture
            .reader
            .read_capture(
                &fixture.range,
                &[EvidenceFamily::ActivityObservations],
                EvidenceReadBudget {
                    page_rows: 1,
                    recovery_payloads: Some(RecoveryPayloadBudget::default()),
                    ..Default::default()
                },
                Some(fixture.recovery.as_ref()),
            )
            .await
            .unwrap();
        assert!(capture.is_read_complete(), "{case:?}: {:?}", capture.gaps());
        assert_eq!(capture.source(), &source);
        assert_eq!(capture.records().len(), records.len());
        for (expected, actual) in records.iter().zip(capture.records()) {
            assert_eq!(expected, &actual.row().record);
        }
        assert!(!serde_json::to_string(&capture).unwrap().contains("output-lock-private-canary"));
    }
}
