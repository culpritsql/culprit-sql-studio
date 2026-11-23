//! The shared native settings corpus traverses the real physical encoder/proof reader, including empty/partial frames.
use super::*;
use test_support::{
    GlobalSettingsFindingCase, SettingsFindingCase, global_settings_finding_evidence, settings_finding_evidence,
};

#[tokio::test]
async fn ordinary_settings_keep_exact_values_scope_acquisition_and_empty_coverage_in_the_canonical_codec() {
    let mut cases = SettingsFindingCase::ALL
        .into_iter()
        .map(|case| (format!("{case:?}"), settings_finding_evidence(case)))
        .collect::<Vec<_>>();
    for format in [NativeSettingsFormat::MysqlGlobalVariables, NativeSettingsFormat::MariaDbGlobalVariables] {
        cases.extend(
            GlobalSettingsFindingCase::ALL
                .into_iter()
                .map(|case| (format!("{format:?}/{case:?}"), global_settings_finding_evidence(format, case))),
        );
    }
    for (case, (source, records)) in cases {
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        fixture.replies(fixture.rows.iter().map(|row| vec![row.clone()]).collect());
        *fixture.recovery.fault.lock().unwrap() = Fault::None;
        let capture = fixture
            .reader()
            .read_capture(
                &fixture.range,
                &[EvidenceFamily::SettingObservations],
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
        assert_eq!(capture.captured_rows(TelemetryStream::SettingObservations), Some(records.len() as u32));
        assert_eq!(source.partition, SourcePartition::Target);
        assert!(source.target.database().unwrap().incarnation.is_none());
        assert!(source.observation.snapshot_acquisition_context().unwrap().has_qualified_local_owner());
        for (row, record) in capture.records().iter().zip(&records) {
            assert_eq!(row.row(), &ExportedRecord::new(&source, capture.capture(), record.clone()).unwrap());
        }
    }
}
