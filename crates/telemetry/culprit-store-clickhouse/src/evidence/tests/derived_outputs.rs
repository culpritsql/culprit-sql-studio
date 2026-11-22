use super::*;
const GOLD: [EvidenceFamily; 3] =
    [EvidenceFamily::DerivedIncidents, EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges];

#[tokio::test]
async fn all_materialized_gold_outputs_reuse_original_projection_proofs_with_partial_and_shared_inputs() {
    for (source, records) in test_support::derived_diagnosis_evidence_cases() {
        let fixture =
            Fixture::new_for_streams(source.clone(), records.clone(), &GOLD.map(EvidenceFamily::stream)).await;
        fixture.replies(GOLD.into_iter().map(|family| fixture.rows_for(family.stream())).collect());
        let captured =
            fixture.reader.read_capture(&fixture.range, &GOLD, EvidenceReadBudget::default(), None).await.unwrap();
        test_support::assert_derived_capture(&captured, &source, &records);
        assert_eq!(captured.metrics().selected_rows, 3);
        assert_eq!(captured.metrics().queries, 5, "one publication, membership and three selected data pages");
        assert!(
            matches!(*fixture.recovery.fault.lock().unwrap(), Fault::Stall),
            "no substitute native payload reader may run"
        );
    }
}
#[tokio::test]
async fn absent_finding_or_change_rows_remain_missing_and_changed_details_or_clocks_cannot_verify() {
    let (source, output) = test_support::derived_diagnosis_output(false, false);
    let records = output.into_records().collect::<Vec<_>>();
    let fixture = Fixture::new_for_streams(source.clone(), records.clone(), &GOLD.map(EvidenceFamily::stream)).await;
    for family in [EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges] {
        fixture.replies(vec![vec![]]);
        let captured =
            fixture.reader.read_capture(&fixture.range, &[family], EvidenceReadBudget::default(), None).await.unwrap();
        assert!(!captured.is_read_complete());
        assert!(captured.records().is_empty());
        assert_eq!(captured.captured_rows(family.stream()), Some(1));
        assert_eq!(captured.metrics().selected_rows, 1);
        assert!(captured.gaps().iter().any(|gap| gap.reason == EvidenceGapReason::MissingRows));
        assert_eq!(captured.derived_description().unwrap().counts.findings, 1);
        assert_eq!(captured.derived_description().unwrap().counts.changes, 1);
        let schema = fixture.catalog.get(family.stream()).unwrap();
        for column in ["Details", "RecordedAt"] {
            let mut rows = fixture.rows_for(family.stream());
            let position = schema.columns().iter().position(|field| field.name == column).unwrap();
            if column == "Details" {
                let mut details: Value = serde_json::from_str(rows[0][position].as_str().unwrap()).unwrap();
                details["analysis_id"] = json!(digest(b"foreign retained analysis"));
                rows[0][position] = json!(serde_json::to_string(&details).unwrap());
            } else {
                rows[0][position] = json!("1577880120000000000");
            }
            fixture.replies(vec![rows]);
            let error = fixture
                .reader
                .read_capture(&fixture.range, &[family], EvidenceReadBudget::default(), None)
                .await
                .unwrap_err();
            assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)), "{column}");
        }
    }
}
#[tokio::test]
async fn declared_empty_gold_families_preserve_unavailable_output_coverage_and_do_not_query_dummy_rows() {
    for unavailable in [false, true] {
        let (source, output) = test_support::empty_derived_diagnosis_output(unavailable);
        let records = output.into_records().collect::<Vec<_>>();
        let fixture =
            Fixture::new_for_streams(source.clone(), records.clone(), &GOLD.map(EvidenceFamily::stream)).await;
        fixture.replies(vec![]);
        let captured =
            fixture.reader.read_capture(&fixture.range, &GOLD, EvidenceReadBudget::default(), None).await.unwrap();
        test_support::assert_derived_capture(&captured, &source, &records);
        for family in GOLD {
            assert_eq!(captured.captured_rows(family.stream()), Some(0));
        }
        assert_eq!(captured.metrics().queries, 2);
        assert_eq!(captured.metrics().pages, 0);
        let header = captured.derived_description().unwrap();
        assert_eq!(header.output_coverage.findings == culprit_core::DerivedOutputCoverage::Evaluated, !unavailable);
        assert_eq!(header.output_coverage.changes == culprit_core::DerivedOutputCoverage::Evaluated, !unavailable);
    }
}
