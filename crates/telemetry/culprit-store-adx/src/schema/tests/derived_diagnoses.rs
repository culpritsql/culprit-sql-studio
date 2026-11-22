use super::*;
use culprit_core::telemetry::{ExportedRecord, TelemetryStream};
use culprit_evidence::analytical::envelope::Envelope;
use culprit_evidence::analytical::test_support;
use culprit_evidence::delivery::recovery::reopen;

#[tokio::test]
async fn gold_typed_details_developer_columns_and_partial_shared_originals_use_the_existing_proof_projection() {
    let catalog = crate::catalog::SchemaCatalog::new().unwrap();
    let cases = test_support::derived_diagnosis_evidence_cases();
    assert_eq!(cases.len(), 3);
    for (source, records) in cases {
        let recovery =
            test_support::Recovery::capture("adx:gold-test", catalog.digest(), source.clone(), records.clone(), 1);
        let range = reopen(&recovery, &recovery.manifest.range_id).await.unwrap();
        assert_eq!(range.manifest().source_queries, 0);
        for record in records {
            let row = ExportedRecord::new(&source, &range.manifest().capture, record).unwrap();
            let schema = catalog.get(row.record.stream()).unwrap();
            let envelope = Envelope::committed(&range, &row.batch_id).unwrap();
            let input = envelope.input(1, &row).unwrap();
            let values = schema.project(&input).unwrap();
            let wire: Value = serde_json::from_slice(&schema.encode(&values).unwrap()).unwrap();
            let returned = schema.columns().iter().map(|column| wire[&column.name].clone()).collect::<Vec<_>>();
            assert_eq!(schema.returned_digest(&returned).unwrap(), projection::digest(&values).unwrap());
            let logical = schema.logical_values(&returned).unwrap();
            test_support::assert_derived_projection(&logical, &row.record);
            let column = schema.columns().iter().position(|column| column.name == "Details").unwrap();
            let mut altered = returned.clone();
            altered[column] = json!("{}");
            assert_ne!(schema.returned_digest(&altered).unwrap(), projection::digest(&values).unwrap());
            assert!(
                !culprit_evidence::analytical::semantics::stream_semantics(row.record.stream())
                    .unwrap()
                    .latest_revision
            );
        }
    }
    for stream in [TelemetryStream::DerivedIncidents, TelemetryStream::DerivedFindings, TelemetryStream::DerivedChanges]
    {
        assert!(culprit_evidence::analytical::semantics::stream_semantics(stream).unwrap().current.is_none());
    }
}
