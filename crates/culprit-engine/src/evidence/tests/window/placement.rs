//! Canonical archived rows retain per-row original/native clocks, independently from the synthesis envelope.
use super::*;
use std::sync::Arc;

async fn two_original_diagnosis() -> (Fixture, WarehouseWindowQuery, DerivedDiagnosis) {
    let (fixture, mut request) = fixture().await;
    let (source, rows) = crate::collected_evidence::window_test_original(12, Some("window-original-epoch"));
    let other = fixture.append_capture(source, rows, 100_001).await;
    let token = CollectionCancellation::default();
    let mut session = fixture.service.session(EvidenceReadBudget::default(), &token).unwrap();
    let families = [
        EvidenceFamily::QueryDefinitions,
        EvidenceFamily::PlanDefinitions,
        EvidenceFamily::QueryRuntime,
        EvidenceFamily::QueryWaits,
    ];
    let first = session.read(&fixture.request.range_id, &families).await.unwrap();
    let second = session.read(&other.manifest().range_id, &families).await.unwrap();
    let synth = crate::ChecksSynthesizer::new(
        Arc::new(culprit_checks::Pipeline::builtin()),
        crate::checks_synthesis::BUILTIN_RULES_VERSION,
    )
    .unwrap();
    let display =
        DerivedDisplayContext { connection_alias: "window-source".into(), environment: None, database_name: None };
    let diagnosis = synth.synthesize_captures(&first, &[&second], first.collected_until(), &display).unwrap();
    assert!(!diagnosis.findings.is_empty());
    assert_eq!(
        diagnosis.description.inputs.iter().filter(|input| input.role == DerivedInputRole::Observation).count(),
        2
    );
    request.view = WarehouseWindowView::PublishedAnswers;
    (fixture, request, diagnosis)
}
async fn publish(fixture: &Fixture, mut diagnosis: DerivedDiagnosis) {
    diagnosis.description.counts = DerivedDiagnosisCounts {
        incidents: diagnosis.incidents.len() as u32,
        findings: diagnosis.findings.len() as u32,
        changes: diagnosis.changes.len() as u32,
    };
    diagnosis.validate().unwrap();
    let token = CollectionCancellation::default();
    let mut session = fixture.service.session(EvidenceReadBudget::default(), &token).unwrap();
    let original = session.read(&fixture.request.range_id, &[EvidenceFamily::QueryRuntime]).await.unwrap();
    let source = CollectionDescription {
        target: original.source().target.clone(),
        partition: original.source().partition.clone(),
        source: DERIVED_DIAGNOSIS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::DerivedDiagnosis(Box::new(diagnosis.description.clone())),
    };
    fixture.append_capture(source, diagnosis.into_records().collect(), 100_010).await;
}

#[tokio::test]
async fn a_finding_selects_only_its_exact_original_clock_from_multiple_authenticated_primary_inputs() {
    let (fixture, mut request, diagnosis) = two_original_diagnosis().await;
    let first = diagnosis.description.inputs[0].capture.clone();
    assert!(
        diagnosis
            .findings
            .iter()
            .all(|finding| finding.referenced_evidence().all(|reference| reference.capture_ref() == first))
    );
    publish(&fixture, diagnosis).await;
    let before = fixture.objects.snapshot();
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    let analysis = &report.published[0];
    assert!(matches!(analysis.native_clock, WindowNativeClock::Unknown), "independent primary clocks do not collapse");
    let row = &analysis.findings[0];
    assert!(row.placement.read_complete);
    assert_eq!(row.placement.overlaps_window, Some(true));
    let WindowRowTime::Originals { inputs } = &row.placement.time else { panic!() };
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].original, first);
    assert!(matches!(&inputs[0].native_clock, WindowNativeClock::Interval { from, until }
        if from == &canonical_time("2020-01-01T06:00:00Z").unwrap()
        && until == &canonical_time("2020-01-01T07:00:00Z").unwrap()));
    request.window =
        PublishedResourceWindow { from: "2020-01-01T10:00:00Z".into(), until: "2020-01-01T11:00:00Z".into() };
    let outside = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    assert_eq!(outside.published[0].findings[0].placement.overlaps_window, Some(false));
    assert_eq!(fixture.objects.snapshot(), before);
}

#[tokio::test]
async fn capture_wide_findings_retain_all_original_clocks_without_fabricating_a_representative_row() {
    let (fixture, request, mut diagnosis) = two_original_diagnosis().await;
    let finding = &mut diagnosis.findings[0];
    finding.inputs.clear();
    finding.query = None;
    finding.cause = None;
    publish(&fixture, diagnosis).await;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    let row = &report.published[0].findings[0];
    let WindowRowTime::Originals { inputs } = &row.placement.time else { panic!() };
    assert_eq!(inputs.len(), 2);
    assert_ne!(inputs[0].original, inputs[1].original);
    assert_eq!(row.placement.overlaps_window, Some(true));
    assert!(row.finding.inputs.is_empty(), "capture-wide scope does not invent row authority");
}

#[tokio::test]
async fn recorded_changes_keep_their_own_uncertain_utc_bounds_without_borrowing_a_native_execution_clock() {
    let (fixture, request, mut diagnosis) = two_original_diagnosis().await;
    let mut event = ChangeEvent::new(
        EventKind::Setting,
        EventSource::Scans,
        "controlled-setting",
        "2020-01-01T07:30:00Z",
        "Controlled original comparison event",
    );
    event.after = Some("2020-01-01T05:30:00Z".into());
    let change = DerivedChange {
        analysis_id: diagnosis.description.analysis_id.clone(),
        database: diagnosis.findings[0].database.clone(),
        display: diagnosis.description.display.clone(),
        change: event,
        inputs: vec![],
    };
    diagnosis.changes.push(change);
    diagnosis.description.output_coverage.changes = DerivedOutputCoverage::Evaluated;
    publish(&fixture, diagnosis).await;
    let report = fixture
        .service
        .window(&request, EvidenceReadBudget::default(), &CollectionCancellation::default())
        .await
        .unwrap();
    let row = &report.published[0].changes[0];
    assert_eq!(row.change.change.source, EventSource::Scans);
    assert_eq!(row.placement.overlaps_window, Some(true));
    assert!(matches!(&row.placement.time, WindowRowTime::RecordedChange { at, after: Some(after), until: None }
        if at == &canonical_time("2020-01-01T07:30:00Z").unwrap()
        && after == &canonical_time("2020-01-01T05:30:00Z").unwrap()));
    assert!(row.change.inputs.is_empty(), "typed capture-wide events do not fabricate an evidence row");
}
