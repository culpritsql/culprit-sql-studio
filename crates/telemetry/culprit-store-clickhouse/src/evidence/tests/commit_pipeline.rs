use super::*;
use test_support::{CommitPipelineCase as Case, commit_pipeline_fixture};

fn family(source: &CollectionDescription) -> EvidenceFamily {
    if source.observation.collection() == TelemetryCollection::Activity {
        EvidenceFamily::ActivityObservations
    } else {
        EvidenceFamily::OperationalSamples
    }
}
#[tokio::test]
async fn commit_log_original_points_keep_native_scopes_roles_nulls_totals_and_independent_clocks() {
    for case in [
        Case::Exact,
        Case::Partial,
        Case::UnknownRole,
        Case::ChangedRole,
        Case::FileReplaced,
        Case::LogCapacityDenied,
        Case::Postgres17,
        Case::Postgres18,
        Case::MysqlTotals,
    ] {
        for original in commit_pipeline_fixture(case).originals {
            let source = original.source;
            let records = original.records;
            let family = family(&source);
            let fixture = Fixture::new_for_stream(source.clone(), records.clone(), family.stream()).await;
            fixture.replies(fixture.rows.iter().map(|row| vec![row.clone()]).collect());
            *fixture.recovery.fault.lock().unwrap() = Fault::None;
            let capture = fixture
                .reader
                .read_capture(
                    &fixture.range,
                    &[family],
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
            assert_eq!(capture.captured_rows(family.stream()), Some(records.len() as u32));
            for (row, record) in capture.records().iter().zip(&records) {
                assert_eq!(row.row(), &ExportedRecord::new(&source, capture.capture(), record.clone()).unwrap());
            }
        }
    }
}
#[tokio::test]
async fn changed_transaction_byte_or_native_clock_cannot_reuse_original_canonical_receipts() {
    let original = commit_pipeline_fixture(Case::Exact).originals.remove(1);
    let source = original.source;
    let records = original.records;
    let family = family(&source);
    let fixture = Fixture::new_for_stream(source.clone(), records.clone(), family.stream()).await;
    let schema = fixture.catalog.get(family.stream()).unwrap();
    let column = schema.columns().iter().position(|column| column.name == "Details").unwrap();
    for (pointer, value) in [
        ("/oldest_confirmed/log_bytes_used", json!("117")),
        ("/oldest_confirmed/observed_time_without_offset", json!("2020-01-01T14:00:00.000001")),
    ] {
        let mut rows = fixture.rows.clone();
        let mut details: Value = serde_json::from_str(rows[0][column].as_str().unwrap()).unwrap();
        *details.pointer_mut(pointer).unwrap() = value;
        rows[0][column] = json!(serde_json::to_string(&details).unwrap());
        fixture.replies(vec![rows]);
        let error = fixture
            .reader
            .read_capture(&fixture.range, &[family], EvidenceReadBudget::default(), None)
            .await
            .unwrap_err();
        assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)), "{pointer}");
    }
}
#[tokio::test]
async fn missing_file_row_preserves_original_census_count_instead_of_an_empty_or_complete_allocation_verdict() {
    let original = commit_pipeline_fixture(Case::Exact).originals.remove(3);
    let source = original.source;
    let records = original.records;
    let family = family(&source);
    let fixture = Fixture::new_for_stream(source.clone(), records.clone(), family.stream()).await;
    let mut rows = fixture.rows.clone();
    rows.remove(0);
    fixture.replies(vec![rows]);
    let capture =
        fixture.reader.read_capture(&fixture.range, &[family], EvidenceReadBudget::default(), None).await.unwrap();
    assert!(!capture.is_read_complete());
    assert_eq!(capture.source(), &source);
    assert_eq!(capture.captured_rows(family.stream()), Some(records.len() as u32));
    assert_eq!(capture.records().len() + 1, records.len());
    assert!(capture.gaps().iter().any(|gap| gap.reason == EvidenceGapReason::MissingRows));
    assert_eq!(capture.metrics().recovery_objects, 0);
}

#[tokio::test]
async fn interval_counter_corpus_retains_exact_native_reset_scope_values_and_original_rows_in_both_store_codecs() {
    use test_support::{CommitPipelineIntervalCase as IntervalCase, commit_pipeline_interval_fixture};
    for case in IntervalCase::ALL {
        for input in commit_pipeline_interval_fixture(case).originals {
            let family = family(&input.source);
            let fixture = Fixture::new_for_stream(input.source.clone(), input.records.clone(), family.stream()).await;
            fixture.replies(fixture.rows.iter().map(|row| vec![row.clone()]).collect());
            *fixture.recovery.fault.lock().unwrap() = Fault::None;
            let capture = fixture
                .reader
                .read_capture(
                    &fixture.range,
                    &[family],
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
            assert_eq!(capture.source(), &input.source);
            assert_eq!(capture.records().len(), input.records.len());
            for (row, record) in capture.records().iter().zip(&input.records) {
                assert_eq!(row.row(), &ExportedRecord::new(&input.source, capture.capture(), record.clone()).unwrap());
            }
        }
    }
}
