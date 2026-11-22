use super::*;
use crate::analytical::{indexes::ProjectionIndexes, test_support::ReadOnlyFixture};
use std::sync::Arc;
const GOLD: &[EvidenceFamily] =
    &[EvidenceFamily::DerivedIncidents, EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges];

#[tokio::test]
async fn every_gold_dto_reuses_the_same_typed_decoder_original_receipt_and_independent_header_clocks() {
    for (source, records) in test_support::derived_diagnosis_evidence_cases() {
        let fixture = ReadOnlyFixture::capture(source.clone(), records.clone()).await;
        let recovery =
            crate::delivery::object_store::ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone())
                .await
                .unwrap();
        let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
        let reader = EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport())));
        let capture = reader.read_capture(&range, GOLD, EvidenceReadBudget::default(), None).await.unwrap();
        test_support::assert_derived_capture(&capture, &source, &records);
        assert_eq!(capture.metrics().selected_rows, 3);
        let header = capture.derived_description().unwrap();
        assert_eq!(header.recorded_at, "2020-01-01T12:01:00Z");
        assert_eq!(header.observed_from, "2020-01-01T12:00:00Z");
        assert_eq!(header.observed_until, "2020-01-01T12:00:01Z");
        assert_eq!(capture.derived_findings().next().unwrap().cause, None);
    }
}
#[tokio::test]
async fn authenticated_zero_output_preserves_evaluated_and_unavailable_coverage_without_dummy_rows() {
    for unavailable in [false, true] {
        let (source, output) = test_support::empty_derived_diagnosis_output(unavailable);
        let fixture = ReadOnlyFixture::capture(source.clone(), output.into_records().collect()).await;
        let recovery =
            crate::delivery::object_store::ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone())
                .await
                .unwrap();
        let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
        let reader = EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport())));
        let capture = reader.read_capture(&range, GOLD, EvidenceReadBudget::default(), None).await.unwrap();
        test_support::assert_derived_capture(&capture, &source, &[]);
        for family in GOLD {
            assert_eq!(capture.captured_rows(family.stream()), Some(0));
        }
        let header = capture.derived_description().unwrap();
        assert_eq!(header.output_coverage.findings == culprit_core::DerivedOutputCoverage::Evaluated, !unavailable);
        assert_eq!(header.output_coverage.changes == culprit_core::DerivedOutputCoverage::Evaluated, !unavailable);
        assert_eq!(capture.metrics().queries, 2);
        assert_eq!(capture.metrics().pages, 0);
    }
}
#[tokio::test]
async fn selecting_findings_does_not_relabel_unselected_incidents_or_changes_as_empty() {
    let (source, records) = test_support::derived_diagnosis_evidence_cases().remove(0);
    let fixture = ReadOnlyFixture::capture(source, records).await;
    let recovery =
        crate::delivery::object_store::ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone())
            .await
            .unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport())));
    let capture = reader
        .read_capture(&range, &[EvidenceFamily::DerivedFindings], EvidenceReadBudget::default(), None)
        .await
        .unwrap();
    assert!(capture.is_read_complete());
    assert_eq!(capture.derived_findings().count(), 1);
    assert_eq!(capture.derived_incidents().count(), 0);
    assert_eq!(capture.derived_changes().count(), 0);
    assert_eq!(capture.captured_rows(TelemetryStream::DerivedIncidents), None);
    assert_eq!(capture.captured_rows(TelemetryStream::DerivedChanges), None);
    assert_eq!(capture.derived_description().unwrap().counts.incidents, 1);
    assert_eq!(capture.derived_description().unwrap().counts.changes, 1);
    assert_eq!(capture.metrics().selected_rows, 1);
}
