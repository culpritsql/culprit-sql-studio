use crate::analytical::{catalog::LogicalCatalog, envelope::Envelope, test_support};
use crate::delivery::recovery::reopen;
use culprit_core::telemetry::{ExportedRecord, SourceObservation, TelemetryStream};
use serde_json::{Value, json};

#[test]
fn every_derived_row_rejects_labels_that_differ_from_the_authenticated_analysis_header() {
    let (source, output) = test_support::derived_diagnosis_output(false, false);
    let capture = test_support::fixture().0.capture;
    for mut record in output.into_records() {
        assert!(ExportedRecord::new(&source, &capture, record.clone()).is_ok());
        let display = match &mut record {
            culprit_core::telemetry::TelemetryRecord::DerivedIncident(row) => &mut row.display,
            culprit_core::telemetry::TelemetryRecord::DerivedFinding(row) => &mut row.display,
            culprit_core::telemetry::TelemetryRecord::DerivedChange(row) => &mut row.display,
            _ => unreachable!(),
        };
        display.connection_alias = "Another approved resource alias".into();
        assert!(ExportedRecord::new(&source, &capture, record).is_err());
    }
}

#[tokio::test]
async fn derived_projection_is_dispatched_by_the_same_catalog_and_keeps_partial_and_shared_scope_explicit() {
    let catalog = LogicalCatalog::new().unwrap();
    let cases = test_support::derived_diagnosis_evidence_cases();
    assert_eq!(cases.len(), 3);
    for (source, records) in cases {
        let recovery =
            test_support::Recovery::capture("logical:gold", catalog.digest(), source.clone(), records.clone(), 1);
        let range = reopen(&recovery, &recovery.manifest.range_id).await.unwrap();
        let SourceObservation::DerivedDiagnosis(header) = &source.observation else { panic!() };
        for record in records {
            let row = ExportedRecord::new(&source, &range.manifest().capture, record).unwrap();
            let input = Envelope::committed(&range, &row.batch_id).unwrap().input(1, &row).unwrap();
            let schema = catalog.get(row.record.stream()).unwrap();
            let values = schema.project(&input).unwrap();
            let logical = schema
                .columns()
                .iter()
                .zip(&values)
                .map(|(column, value)| (column.name.clone(), value.clone()))
                .collect();
            test_support::assert_derived_projection(&logical, &row.record);
            let encoded: Value = serde_json::from_slice(&schema.encode(&values).unwrap()).unwrap();
            assert_eq!(encoded["InputReadComplete"], header.input_read_complete);
            assert_eq!(
                serde_json::from_str::<Value>(encoded["ChangeEvaluations"].as_str().unwrap()).unwrap(),
                serde_json::to_value(&header.change_evaluations).unwrap()
            );
            assert_eq!(
                serde_json::from_str::<Value>(encoded["FindingEvaluations"].as_str().unwrap()).unwrap(),
                serde_json::to_value(&header.finding_evaluations).unwrap()
            );
            assert_eq!(
                serde_json::from_str::<Value>(encoded["ContextIssues"].as_str().unwrap()).unwrap(),
                serde_json::to_value(&header.context_issues).unwrap()
            );
            assert_eq!(encoded["OriginalObservedFrom"], "2020-01-01T12:00:00.000000000Z");
            let restored: Value = serde_json::from_str(encoded["Details"].as_str().unwrap()).unwrap();
            assert_eq!(restored, serde_json::to_value(&row.record).unwrap()["data"]);
            assert_eq!(encoded["DefinitionPayloads"], "[]");
        }
    }
}
#[test]
fn every_derived_table_requires_the_common_developer_contract_and_does_not_modify_raw_field_schemas() {
    let catalog = LogicalCatalog::new().unwrap();
    assert_eq!(
        catalog.schemas().count(),
        TelemetryStream::ALL.into_iter().filter(|stream| stream.delivery_required()).count()
    );
    for (stream, name) in [
        (TelemetryStream::DerivedIncidents, "CulpritIncidentsV1"),
        (TelemetryStream::DerivedFindings, "CulpritFindingsV1"),
        (TelemetryStream::DerivedChanges, "CulpritChangesV1"),
    ] {
        assert_eq!(catalog.get(stream).unwrap().table(), name);
    }
    let schema = catalog.get(TelemetryStream::DerivedIncidents).unwrap();
    for name in [
        "VictimSummary",
        "AffectedQueries",
        "CulpritQueries",
        "CandidateQueries",
        "AttributionStatus",
        "Mechanism",
        "DeveloperAction",
        "Verification",
        "Caller",
        "OriginalReference",
        "InputReferences",
        "Details",
    ] {
        assert!(schema.fields().iter().any(|field| field.name == name), "{name}");
    }
    let native = catalog.get(TelemetryStream::OperationalSamples).unwrap();
    assert_eq!(native.table(), "CulpritOperationalV1");
    assert!(!native.fields().iter().any(|field| field.name == "VictimSummary"));
    let mut incomplete = json!({"record":{"data":{}},"_culprit":{"derived":{}},"_projection":{}});
    assert!(schema.project(&incomplete).is_err());
    incomplete["record"]["data"]["victim_summary"] = json!("Unknown original victim");
    assert!(
        schema.project(&incomplete).is_err(),
        "required authority and independent fields cannot be filled by a label"
    );
}

#[test]
fn authenticated_unresolved_revision_exposes_unopened_ancestry_without_claiming_analyzed_query_evidence() {
    let (mut source, mut output) = test_support::unresolved_derived_diagnosis_output();
    assert_eq!(output.incidents[0].entry.analysis, culprit_core::telemetry::DiagnosticAnalysisStatus::Unresolved);
    let original = output.incidents[0].entry.evidence[0].reference.clone().unwrap();
    let revision = output.incidents[0].revision_reference.clone();
    output.description.inputs.retain(|input| input.capture == revision.capture_ref());
    output.description.counts = culprit_core::DerivedDiagnosisCounts { incidents: 1, findings: 0, changes: 0 };
    output.description.analysis_id = output.description.expected_analysis_id().unwrap();
    output.findings.clear();
    output.changes.clear();
    output.incidents[0].analysis_id = output.description.analysis_id.clone();
    output.incidents[0].affected_queries.clear();
    assert!(!output.description.contains_capture_for(&original));
    assert!(output.validate().is_ok());
    source.observation = SourceObservation::DerivedDiagnosis(Box::new(output.description.clone()));
    let capture = test_support::fixture().0.capture;
    let row = ExportedRecord::new(&source, &capture, output.clone().into_records().next().unwrap()).unwrap();
    let culprit_core::telemetry::TelemetryRecord::DerivedIncident(incident) = row.record else { panic!() };
    assert_eq!(incident.entry.original, Some(revision.clone()));
    assert_eq!(incident.entry.evidence[0].reference, Some(original.clone()));
    assert!(incident.affected_queries.is_empty());
    assert!(incident.culprit_queries.is_empty());
    output.incidents[0].affected_queries.push(culprit_core::DerivedIncidentQuery {
        causal_role: None,
        identity: None,
        sql: culprit_core::telemetry::DefinitionPayload::Unavailable {
            reason: culprit_core::telemetry::PayloadUnavailable::NotCollected,
        },
        caller: None,
        evidence: vec![original],
    });
    assert!(output.validate().is_err(), "actually attributed query evidence must still be a selected input capture");
}

#[test]
fn capture_wide_findings_keep_mandatory_capture_authority_without_dummy_rows_or_unproved_causes() {
    let (_, mut output) = test_support::derived_diagnosis_output(false, false);
    output.findings[0].inputs.clear();
    assert!(output.validate().is_ok(), "the fully authenticated input captures remain declared in the header");
    output.findings[0].cause = Some(culprit_core::DerivedCause { summary: "Unproved cause".into(), evidence: vec![] });
    assert!(output.validate().is_err(), "capture-wide basis does not establish a cause without its own evidence");
    output.findings[0].cause = None;
    output.description.output_coverage.changes = culprit_core::DerivedOutputCoverage::ContextUnavailable {
        reason: culprit_core::DerivedOutputGap::CatalogComparisonUnavailable,
    };
    assert!(output.validate().is_err(), "unavailable comparison cannot carry a manufactured change row");
    output.changes.clear();
    output.description.counts.changes = 0;
    assert!(output.validate().is_ok());
    assert!(!output.description.output_coverage.limitations().is_empty());
}

#[test]
fn an_incident_entry_original_is_the_revision_receipt_not_its_native_ancestry() {
    let (_, mut output) = test_support::unresolved_derived_diagnosis_output();
    assert_eq!(output.incidents[0].entry.original.as_ref(), Some(&output.incidents[0].revision_reference));
    assert!(output.validate().is_ok());
    output.incidents[0].entry.original = output.incidents[0].entry.evidence[0].reference.clone();
    assert!(output.validate().is_err());
}

#[tokio::test]
async fn zero_finding_rows_retain_locked_and_partial_evaluation_in_the_same_authenticated_range_header() {
    use culprit_core::{DerivedContextIssue, DerivedFindingEvaluations};
    let (mut source, mut output) = test_support::derived_diagnosis_output(false, false);
    output.incidents.clear();
    output.findings.clear();
    output.changes.clear();
    output.description.counts = Default::default();
    output.description.finding_evaluations =
        DerivedFindingEvaluations { population_count: 2, omitted_populations: 2, ..Default::default() };
    output.description.context_issues = vec![DerivedContextIssue::PartialCatalog];
    output.description.analysis_id = output.description.expected_analysis_id().unwrap();
    output.validate().unwrap();
    source.observation = SourceObservation::DerivedDiagnosis(Box::new(output.description.clone()));
    let catalog = LogicalCatalog::new().unwrap();
    let recovery = test_support::Recovery::capture("logical:gold", catalog.digest(), source.clone(), vec![], 1);
    let range = reopen(&recovery, &recovery.manifest.range_id).await.unwrap();
    let SourceObservation::DerivedDiagnosis(restored) = &range.manifest().source.observation else { panic!() };
    assert_eq!(**restored, output.description);
    assert_eq!(restored.counts.findings, 0);
    assert_eq!(restored.display.connection_alias, "Orders production");
    assert_eq!(restored.display.database_name, None, "an absent approved name stays unknown even with zero rows");
    assert_eq!(restored.finding_evaluations.omitted_populations, 2);
    assert!(restored.input_read_complete);
    assert!(!restored.limitations().is_empty());
}

#[tokio::test]
async fn zero_change_rows_retain_settings_only_evaluation_through_the_same_range_header() {
    use culprit_core::{
        DerivedCatalogProvenance, DerivedChangeEvaluation, DerivedDiagnosisCounts, DerivedInputRole,
        DerivedOutputCoverage,
    };
    let (mut source, mut output) = test_support::derived_diagnosis_output(false, false);
    output.incidents.clear();
    output.findings.clear();
    output.changes.clear();
    output.description.counts = DerivedDiagnosisCounts::default();
    let mut previous = output.description.inputs[0].clone();
    previous.capture.capture.observation_id.push_str("-settings-before");
    previous.role = DerivedInputRole::Catalog;
    previous.catalog = Some(DerivedCatalogProvenance {
        observed_from: "2020-01-01T10:00:00Z".into(),
        observed_until: "2020-01-01T10:00:01Z".into(),
        native_time: None,
    });
    let mut current = previous.clone();
    current.capture.capture.observation_id.push_str("-after");
    current.catalog.as_mut().unwrap().observed_from = "2020-01-01T11:00:00Z".into();
    current.catalog.as_mut().unwrap().observed_until = "2020-01-01T11:00:01Z".into();
    output.description.inputs = vec![previous, current];
    output.description.change_evaluations = vec![DerivedChangeEvaluation::Settings {
        previous_input: 0,
        current_input: 1,
        observed_after: "2020-01-01T10:00:00Z".into(),
        observed_until: "2020-01-01T11:00:01Z".into(),
    }];
    output.description.output_coverage.changes = DerivedOutputCoverage::Evaluated;
    output.description.analysis_id = output.description.expected_analysis_id().unwrap();
    output.validate().unwrap();
    source.observation = SourceObservation::DerivedDiagnosis(Box::new(output.description.clone()));
    let catalog = LogicalCatalog::new().unwrap();
    let recovery = test_support::Recovery::capture("logical:gold", catalog.digest(), source, vec![], 1);
    let range = reopen(&recovery, &recovery.manifest.range_id).await.unwrap();
    let SourceObservation::DerivedDiagnosis(header) = &range.manifest().source.observation else { panic!() };
    assert_eq!(header.change_evaluations, output.description.change_evaluations);
    assert_eq!(header.counts.changes, 0);
    assert!(!header.limitations().is_empty());
}
