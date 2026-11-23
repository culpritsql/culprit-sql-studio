use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn readable_recovery_preserves_native_partiality_and_exact_private_context_while_rejecting_edited_public_rows() {
    for &case in test_support::ReadableSecondaryCase::ALL {
        let (source, records) = test_support::readable_secondary_evidence(case);
        let fixture =
            Fixture::new_for_stream(source.clone(), records.clone(), TelemetryStream::OperationalSamples).await;
        *fixture.recovery.fault.lock().unwrap() = Fault::None;
        fixture.replies(fixture.rows.iter().map(|row| vec![row.clone()]).collect());
        let capture = fixture
            .reader
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
        if let Some(first) = fixture.rows.first() {
            let opens = fixture.recovery.object_reads.load(Ordering::SeqCst);
            let schema = fixture.catalog.get(TelemetryStream::OperationalSamples).unwrap();
            let column = schema.columns().iter().position(|column| column.name == "Details").unwrap();
            let mut edited = first.clone();
            let mut details: Value = serde_json::from_str(edited[column].as_str().unwrap()).unwrap();
            details["checked_until"] = json!("1990-01-01T00:00:00Z");
            edited[column] = json!(details.to_string());
            fixture.replies(vec![vec![edited]]);
            let error = fixture
                .reader
                .read_capture(
                    &fixture.range,
                    &[EvidenceFamily::OperationalSamples],
                    EvidenceReadBudget {
                        recovery_payloads: Some(RecoveryPayloadBudget::default()),
                        ..Default::default()
                    },
                    Some(fixture.recovery.as_ref()),
                )
                .await
                .unwrap_err();
            assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)), "{case:?}");
            assert_eq!(
                fixture.recovery.object_reads.load(Ordering::SeqCst),
                opens,
                "unproven rows cannot open private payloads"
            );
        }
    }
}
