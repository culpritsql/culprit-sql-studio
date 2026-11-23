//! Collector control rows use the ordinary exact publication/membership/EOF reader, never a database-health shortcut.
use super::*;
use test_support::{CollectorConnectionCase, collector_connection_evidence};

#[tokio::test]
async fn configured_connection_history_keeps_original_failure_unknown_and_settlement_through_normal_readback() {
    for case in CollectorConnectionCase::ALL {
        let (source, records) = collector_connection_evidence(case);
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        fixture.replies(vec![fixture.rows.clone()]);
        let capture = fixture
            .reader()
            .read_capture(&fixture.range, &[EvidenceFamily::CollectorConnections], EvidenceReadBudget::default(), None)
            .await
            .unwrap();
        assert!(capture.is_read_complete());
        assert_eq!(capture.source(), &source);
        assert!(capture.source().target.database().is_none());
        assert!(!capture.source().is_partition_qualified());
        assert_eq!(capture.source().target.measurement_scope(), culprit_core::MeasurementScope::Unknown);
        assert_eq!(capture.captured_rows(TelemetryStream::CollectorConnections), Some(1));
        assert_eq!(
            capture.records()[0].row(),
            &ExportedRecord::new(&source, capture.capture(), records[0].clone()).unwrap()
        );
        let row = capture.collector_connections().next().unwrap();
        let expected = match case {
            CollectorConnectionCase::Failure | CollectorConnectionCase::QueryDenied => {
                CollectorConnectionOutcome::Failure
            }
            CollectorConnectionCase::CompleteCapture
            | CollectorConnectionCase::Success
            | CollectorConnectionCase::AuthenticatedReuse => CollectorConnectionOutcome::Success,
            _ => CollectorConnectionOutcome::Unknown,
        };
        assert_eq!(row.connection_outcome(), expected);
        assert_eq!(
            row.original_status,
            if case == CollectorConnectionCase::CompleteCapture {
                Some(CollectionStatus::Complete)
            } else if case == CollectorConnectionCase::Interrupted {
                None
            } else {
                Some(CollectionStatus::Incomplete(CollectionStop::SourceUnavailable))
            }
        );
        assert!(row.native.is_none());
        assert_eq!(capture.metrics().recovery_objects, 0);
    }
}
