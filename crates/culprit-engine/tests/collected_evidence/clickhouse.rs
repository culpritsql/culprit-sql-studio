use super::*;
use culprit_evidence::analytical::{evidence::EvidenceFamily, test_support::clickhouse_failure_with_family};

#[tokio::test]
async fn retained_failed_insert_yields_an_actionable_finding_from_its_native_row() {
    let (source, records) = clickhouse_failure_with_family();
    let fixture = Fixture::new(source, records).await;
    let capture = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
    verify_incident_journal(&capture);
    let pipeline = std::sync::Arc::new(culprit_checks::Pipeline::builtin());
    let synth = culprit_engine::ChecksSynthesizer::new(pipeline, "clickhouse-test").unwrap();
    let display = culprit_core::DerivedDisplayContext {
        connection_alias: "sales".into(),
        environment: None,
        database_name: Some("sales".into()),
    };
    let diagnosis = synth.synthesize_captures(&capture, &[], "2025-01-01T12:02:00Z", &display).unwrap();
    let retained = diagnosis
        .findings
        .iter()
        .find(|row| row.finding.check == "clickhouse-health")
        .expect("retained ClickHouse failure");
    assert_eq!(retained.inputs.len(), 1);
    assert!(retained.finding.summary.contains("TOO_MANY_PARTS"));
    assert!(retained.finding.summary.contains("failed-insert"));
    assert!(retained.finding.fix.is_some());
    let query = retained.query.as_ref().expect("associated family from the same capture");
    assert_eq!(query.identity.query_id, "941122");
    assert!(
        matches!(&query.sql, DefinitionPayload::Available{text,..} if text.contains("sales.orders") && !text.contains("9264"))
    );
}

fn verify_incident_journal(capture: &culprit_evidence::analytical::evidence::EvidenceCapture) {
    use culprit_evidence::original_projection::OriginalProjection;
    let original = capture.verified_original().unwrap();
    let references = (0..original.records().len()).map(|index| original.reference(index).unwrap()).collect::<Vec<_>>();
    let rows = original
        .records()
        .iter()
        .zip(&references)
        .map(|(row, reference)| DiagnosticOriginalRow { reference, record: &row.row().record })
        .collect::<Vec<_>>();
    let update =
        observe_diagnostic_original(original.source(), original.original(), &rows, None, &[], "2025-01-01T12:02:00Z")
            .unwrap();
    assert_eq!(update.revisions.len(), 1);
    let episode = &update.revisions[0].episode;
    assert!(matches!(
        episode.signal,
        DiagnosticSignal::OriginalObservation {
            condition: DiagnosticOriginalCondition::ClickHouse { incident: ClickHouseIncident::PartsLimit },
            ..
        }
    ));
    assert!(references.contains(&episode.first));
    assert_eq!(episode.first, episode.latest);
}
