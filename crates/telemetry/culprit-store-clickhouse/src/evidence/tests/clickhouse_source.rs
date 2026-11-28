use super::*;

#[tokio::test]
async fn a_clickhouse_failure_and_its_redacted_query_family_survive_publication() {
    let (source, rows) = test_support::clickhouse_failure_with_family();
    let fixture = Fixture::new(source.clone(), rows.clone()).await;
    fixture.replies(fixture.rows.iter().map(|row| vec![row.clone()]).collect());
    let capture = fixture
        .reader
        .read_capture(
            &fixture.range,
            &[EvidenceFamily::OperationalSamples],
            EvidenceReadBudget { page_rows: 1, ..Default::default() },
            None,
        )
        .await
        .unwrap();
    test_support::assert_operational_capture(&capture, &source, &rows);
    assert!(matches!(*fixture.recovery.fault.lock().unwrap(), Fault::Stall));
}
