use super::*;
#[tokio::test]
async fn original_transaction_metadata_uses_the_same_canonical_decoder_for_native_flags_resets_scope_sql_and_partial_or_empty_sources()
 {
    for case in test_support::TransactionMetadataCase::ALL {
        let (source, records) = test_support::transaction_metadata_evidence(*case);
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        *fixture.recovery.fault.lock().unwrap() = Fault::None;
        fixture.reply_families(&[EvidenceFamily::OperationalSamples], 1);
        let capture = fixture
            .reader()
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
        assert_eq!(capture.source(), &source);
        assert_eq!(capture.records().len(), records.len());
        assert!(capture.is_read_complete(), "canonical EOF and native source partiality stay independent: {case:?}");
        assert!(!serde_json::to_string(&capture).unwrap().contains("transaction-private-canary"));
        if let Some(row) = capture.records().first() {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::PostgresTransactionMetadata(point),
                ..
            }) = &row.row().record
            else {
                panic!()
            };
            assert!(point.fits_source_scope(capture.source()));
            assert_eq!(point.slrus[0].blocks_hit.as_deref(), Some("9223372036854775807"));
            assert_eq!(point.retention.holders[0].horizons[0].native_value, "1234");
        }
    }
}
#[tokio::test]
async fn edited_cache_status_database_scope_or_native_xmin_cannot_borrow_the_original_point_receipt() {
    let (source, records) =
        test_support::transaction_metadata_evidence(test_support::TransactionMetadataCase::Overflow);
    let fixture = Fixture::new(source.clone(), records.clone()).await;
    let schema = fixture.catalog.get(TelemetryStream::OperationalSamples).unwrap();
    let details_column = schema.columns().iter().position(|column| column.name == "Details").unwrap();
    for (path, value) in [
        ("/caches/1/overflowed", json!(false)),
        ("/slrus/0/blocks_read", json!("43")),
        ("/slrus/0/stats_reset", json!("2020-01-01T10:00:00.000001Z")),
        ("/database_oid", json!("7")),
        ("/retention/holders/0/horizons/0/native_value", json!("1235")),
    ] {
        let mut rows = fixture.rows.clone();
        let mut details: Value = serde_json::from_str(rows[0][details_column].as_str().unwrap()).unwrap();
        *details.pointer_mut(path).unwrap() = value;
        rows[0][details_column] = json!(details.to_string());
        fixture.replies(vec![rows]);
        let error = fixture
            .reader()
            .read_capture(
                &fixture.range,
                &[EvidenceFamily::OperationalSamples],
                EvidenceReadBudget { page_rows: 1, ..Default::default() },
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    }
}

#[tokio::test]
async fn retained_slru_pair_corpus_preserves_native_bigints_resets_independent_bounds_and_shared_fields() {
    let original = test_support::transaction_metadata_original(TransactionMetadataWait::Subtransaction);
    for case in test_support::TransactionMetadataSlruCase::ALL {
        for (source, records) in
            test_support::transaction_metadata_slru_pair(&original.focus, &original.execution, *case)
        {
            let fixture = Fixture::new(source.clone(), records.clone()).await;
            *fixture.recovery.fault.lock().unwrap() = Fault::None;
            fixture.reply_families(&[EvidenceFamily::OperationalSamples], 1);
            let capture = fixture
                .reader()
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
            assert!(capture.is_read_complete());
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::PostgresTransactionMetadata(point),
                ..
            }) = &capture.records()[0].row().record
            else {
                panic!()
            };
            assert_eq!(point.slrus.len(), 3);
            assert!(point.fits_source_scope(capture.source()));
        }
    }
}

#[tokio::test]
async fn bounded_continuation_history_uses_the_same_canonical_native_body_decoder_and_keeps_both_answers() {
    for case in test_support::TransactionMetadataContinuationCase::ALL {
        let (source, records) = test_support::transaction_metadata_continuation_history(*case);
        let fixture = Fixture::new(source.clone(), records.clone()).await;
        *fixture.recovery.fault.lock().unwrap() = Fault::None;
        fixture.reply_families(&[EvidenceFamily::OperationalSamples], 1);
        let capture = fixture
            .reader()
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
        test_support::assert_diagnostic_capture(&capture, &source, &records);
        assert!(capture.is_read_complete(), "{case:?}");
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::DiagnosticRevision(revision),
            ..
        }) = &capture.records()[0].row().record
        else {
            panic!()
        };
        assert_eq!(revision.episode.source_reads.len(), MAX_TRANSACTION_METADATA_OBSERVATIONS);
        let first = revision.episode.source_reads.get(SourceReadQuestion::TransactionMetadata).unwrap();
        let second = revision.episode.source_reads.latest(SourceReadQuestion::TransactionMetadata).unwrap();
        assert_ne!(first.key(&revision.episode.id).unwrap(), second.key(&revision.episode.id).unwrap());
        assert!(matches!(first.state, SourceReadStageState::Answered { .. }));
        assert!(matches!(second.state, SourceReadStageState::Answered { .. }));
        assert!(!serde_json::to_string(&capture).unwrap().contains("transaction-private-canary"));
    }
}
