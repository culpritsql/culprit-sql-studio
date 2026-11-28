use super::*;

#[tokio::test]
async fn clickhouse_failure_round_trips_through_the_warehouse_with_its_original_identity() {
    let catalog = LogicalCatalog::new().unwrap();
    let (source, record) = test_support::clickhouse_failure();
    assert!(matches!(
        DiagnosticOriginalCondition::from_record(&source, &record),
        Some(DiagnosticOriginalCondition::ClickHouse { incident: ClickHouseIncident::PartsLimit })
    ));
    let recovery =
        test_support::Recovery::capture("clickhouse:ops", catalog.digest(), source, vec![record.clone()], 90000);
    let range = reopen(&recovery, &recovery.manifest.range_id).await.unwrap();
    let batch = &range.manifest().batches[0];
    let schema = catalog.get(batch.stream).unwrap();
    let expected = ExportedRecord::new(&range.manifest().source, &range.manifest().capture, record).unwrap();
    let input = Envelope::committed(&range, &batch.batch_id).unwrap().input(1, &expected).unwrap();
    let values = schema
        .fields()
        .iter()
        .zip(schema.project(&input).unwrap())
        .map(|(field, value)| (field.name.into(), value))
        .collect();
    let decode::Decoded::Record(actual) = decode::record(&range, batch.stream, &values, &batch.rows[0]).unwrap() else {
        panic!("expected retained ClickHouse observation")
    };
    assert_eq!(*actual, expected);
}
