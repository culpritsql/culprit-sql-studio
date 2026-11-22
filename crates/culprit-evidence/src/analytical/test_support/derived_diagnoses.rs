//! Controlled derived outputs consume the existing original revision corpus, not invented native observations.
use super::*;
use culprit_core::{
    AnalysisOrigin, ComponentVersion, DerivedAttributionStatus, DerivedCause, DerivedChange, DerivedDiagnosis,
    DerivedDiagnosisCounts, DerivedDiagnosisDescription, DerivedDisplayContext, DerivedFinding, DerivedGuidance,
    DerivedIncident, DerivedIncidentQuery, DerivedInputReference, DerivedInputRole, DerivedOutputCoverage,
    DerivedOutputGap, IncidentDatabaseSelector, IncidentHistoryEntry,
    events::{ChangeEvent, EventKind, EventSource},
    finding::{
        Actionability, Category, Confidence, Finding, Impact, Severity, Subject, SubjectKind, VerificationBasis,
    },
};

pub fn derived_diagnosis_output(shared: bool, partial: bool) -> (CollectionDescription, DerivedDiagnosis) {
    let (source, records) = diagnostic_evidence_cases().remove(if shared { 2 } else { 0 });
    from_history(source, records, partial)
}

pub fn unresolved_derived_diagnosis_output() -> (CollectionDescription, DerivedDiagnosis) {
    let (source, records) = super::incident_history_evidence(super::IncidentHistoryCase::Prefix);
    from_history(source, records, false)
}

fn from_history(
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    partial: bool,
) -> (CollectionDescription, DerivedDiagnosis) {
    let record = records.into_iter().find(|row| matches!(row, TelemetryRecord::Operational(_))).unwrap();
    let TelemetryRecord::Operational(original) = &record else { unreachable!() };
    let OperationalFact::DiagnosticRevision(revision) = &original.fact else { unreachable!() };
    let mut capture = fixture().0.capture;
    capture.observation_id = format!("controlled-gold-revision-{}", source.target.telemetry_id().unwrap());
    capture.revision.as_mut().unwrap().sequence = "2".into();
    let exported = ExportedRecord::new(&source, &capture, record.clone()).unwrap();
    let revision_reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled diagnosis manifest"), &exported, 0).unwrap();
    let mut inputs = vec![DerivedInputReference {
        capture: revision_reference.capture_ref(),
        role: DerivedInputRole::Revision,
        catalog: None,
    }];
    for reference in &revision.inputs {
        if !inputs.iter().any(|input| input.capture == reference.capture_ref()) {
            inputs.push(DerivedInputReference {
                capture: reference.capture_ref(),
                role: DerivedInputRole::Observation,
                catalog: None,
            });
        }
    }
    let mut description = DerivedDiagnosisDescription {
        analysis_id: String::new(),
        analyzer: ComponentVersion::package("controlled-gold-analyzer", "1", None),
        rules_version: "controlled-rules-1".into(),
        language: "en".into(),
        recorded_at: "2020-01-01T12:01:00Z".into(),
        observed_from: "2020-01-01T12:00:00Z".into(),
        observed_until: "2020-01-01T12:00:01Z".into(),
        inputs,
        origin: AnalysisOrigin::Collected,
        finding_evaluations: Default::default(),
        change_evaluations: vec![],
        context_issues: vec![],
        input_read_complete: !partial,
        counts: DerivedDiagnosisCounts { incidents: 1, findings: 1, changes: 1 },
        output_coverage: DerivedOutputCoverageSet {
            incidents: DerivedOutputCoverage::Evaluated,
            findings: DerivedOutputCoverage::Evaluated,
            changes: DerivedOutputCoverage::Evaluated,
        },
    };
    description.analysis_id = description.expected_analysis_id().unwrap();
    let database = source.target.database().map(IncidentDatabaseSelector::from_route).transpose().unwrap();
    let display = DerivedDisplayContext {
        connection_alias: "Orders production".into(),
        environment: Some("prod".into()),
        database_name: None,
    };
    let entry = IncidentHistoryEntry {
        controller_version: revision.controller_version,
        revision_id: revision.id.clone(),
        episode_id: revision.episode.id.clone(),
        target_id: source.target.telemetry_id().unwrap(),
        target_scope: source.target.measurement_scope(),
        scope_id: revision.episode.scope.key().unwrap(),
        source: revision.episode.scope.source.clone(),
        scope: revision.episode.scope.measurement_scope,
        scope_label: "Original selected population".into(),
        revision: revision.episode.revision.clone(),
        previous_revision: revision.previous_revision.clone(),
        predecessor_selected: false,
        lifecycle: revision.episode.lifecycle,
        analysis: revision.episode.analysis,
        title: "A request waited".into(),
        phase: "Original analysis pending".into(),
        first_seen_at: revision.episode.first_seen_at.clone(),
        last_seen_at: revision.episode.last_seen_at.clone(),
        recorded_at: revision.analyzed_at.clone(),
        original: Some(revision_reference.clone()),
        evidence: vec![culprit_core::IncidentHistoryEvidence {
            label: "Original native evidence".into(),
            capture: revision.episode.first.capture_ref(),
            reference: Some(revision.episode.first.clone()),
            availability: "Not reopened".into(),
        }],
        input_references: revision.inputs.len() as u32,
        followup_recipe: None,
        followup_recipe_version: None,
        issue: revision.episode.issue.clone(),
        reasons: vec![],
        next_step: "Inspect the qualified original evidence".into(),
    };
    let incident = DerivedIncident {
        analysis_id: description.analysis_id.clone(),
        database: database.clone(),
        display: display.clone(),
        entry,
        original_interval: None,
        revision_reference,
        victim_summary: "The original request was waiting".into(),
        affected_queries: vec![DerivedIncidentQuery {
            causal_role: None,
            identity: None,
            sql: DefinitionPayload::sql("SELECT 'private-gold-canary'", source.target.engine()),
            caller: None,
            evidence: vec![revision.episode.first.clone()],
        }],
        culprit_queries: vec![],
        candidate_queries: vec![],
        attribution_status: DerivedAttributionStatus::Unestablished,
        mechanism: None,
        developer_action: "Inspect the recorded evidence before choosing a change".into(),
        verification: "Compare a new qualified original observation".into(),
        caller: None,
    };
    let finding = Finding {
        id: "controlled:subject".into(),
        check: "controlled".into(),
        category: Category::Queries,
        severity: Severity::Low,
        actionability: Actionability::Information,
        confidence: Confidence::High,
        title: "Original evidence retained".into(),
        summary: "This selected original is available for analysis".into(),
        subject: Subject { kind: SubjectKind::Database, name: "Selected database".into() },
        impact: Impact::default(),
        metrics: vec![],
        queries: vec![],
        fix: None,
        verify: vec!["Compare original references".into()],
        verification_basis: VerificationBasis::Rescan,
        caveats: vec!["A retained observation does not establish an exclusive cause".into()],
        last_seen: None,
        inactive: false,
        facts: Default::default(),
    };
    let finding = DerivedFinding {
        analysis_id: description.analysis_id.clone(),
        database: database.clone(),
        display: display.clone(),
        finding,
        inputs: vec![revision.episode.first.clone()],
        query: None,
        cause: None::<DerivedCause>,
        guidance: DerivedGuidance::HistoricalRequiresFreshVerification,
    };
    let change = DerivedChange {
        analysis_id: description.analysis_id.clone(),
        database,
        display,
        change: ChangeEvent::new(
            EventKind::Mark,
            EventSource::Mark,
            "controlled-mark",
            "2020-01-01T12:00:00Z",
            "Deployment marked",
        ),
        inputs: vec![revision.episode.first.clone()],
    };
    let output =
        DerivedDiagnosis { description, incidents: vec![incident], findings: vec![finding], changes: vec![change] };
    output.validate().unwrap();
    let source = CollectionDescription {
        target: source.target,
        partition: source.partition,
        source: DERIVED_DIAGNOSIS_SOURCE.into(),
        observation: SourceObservation::DerivedDiagnosis(Box::new(output.description.clone())),
        continuity: SourceContinuity::Unverified,
    };
    (source, output)
}

pub fn derived_diagnosis_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    [(false, false), (false, true), (true, false)]
        .into_iter()
        .map(|(shared, partial)| {
            let (source, output) = derived_diagnosis_output(shared, partial);
            (source, output.into_records().collect())
        })
        .collect()
}

pub fn assert_derived_projection(values: &serde_json::Map<String, serde_json::Value>, record: &TelemetryRecord) {
    use serde_json::{Value, json};
    let details = values.get("Details").unwrap();
    assert_eq!(values["DetailState"], "retained");
    assert_eq!(values["DetailSha256"], digest(&serde_json::to_vec(details).unwrap()));
    assert_eq!(values["DefinitionPayloads"], json!([]), "redacted Gold SQL is directly retained, not a native payload");
    assert_eq!(details, &serde_json::to_value(record).unwrap()["data"]);
    assert!(!details.to_string().contains("private-gold-canary"));
    assert_eq!(values["ConnectionAlias"], "Orders production");
    assert!(values["DatabaseName"].is_null(), "native ownership never supplies an unapproved display name");
    match record {
        TelemetryRecord::DerivedIncident(row) => {
            assert_eq!(values["EpisodeId"], row.entry.episode_id);
            assert_eq!(values["VictimSummary"], row.victim_summary);
            assert_eq!(values["AttributionStatus"], "unestablished");
            assert_eq!(values["CulpritQueries"], json!([]));
            assert_eq!(values["AffectedQueries"], json!(row.affected_queries));
            assert_eq!(values["Caller"], Value::Null);
            assert_eq!(values["Lifecycle"], json!(row.entry.lifecycle));
            assert_eq!(values["EnrichmentStatus"], json!(row.entry.analysis));
            assert_eq!(values["OriginalReference"], json!(row.revision_reference));
            assert_eq!(values["OriginalScope"], json!(row.entry.scope));
            assert_eq!(values["OriginalIntervalFrom"], Value::Null);
            assert_eq!(values["OriginalIntervalUntil"], Value::Null);
        }
        TelemetryRecord::DerivedFinding(row) => {
            assert_eq!(values["FindingId"], row.finding.id);
            assert_eq!(values["Guidance"], "historical-requires-fresh-verification");
            assert_eq!(values["EstablishedCause"], Value::Null);
            assert_eq!(values["OriginalReferences"], json!(row.inputs));
        }
        TelemetryRecord::DerivedChange(row) => {
            assert_eq!(values["ChangeId"], row.change.id);
            assert_eq!(values["ChangedAt"], row.change.at);
            assert_eq!(values["Title"], row.change.title.text());
            assert_eq!(values["OriginalReferences"], json!(row.inputs));
        }
        _ => panic!("derived streams only"),
    }
}

/// The existing canonical Gold corpus with no emitted rows; successful evaluation and unavailable output differ
/// in their original header even though both physically contain three explicit zero-row batches.
pub fn empty_derived_diagnosis_output(unavailable: bool) -> (CollectionDescription, DerivedDiagnosis) {
    let (mut source, mut output) = derived_diagnosis_output(false, false);
    output.incidents.clear();
    output.findings.clear();
    output.changes.clear();
    output.description.counts = Default::default();
    if unavailable {
        output.description.output_coverage.findings =
            DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::AnalysisSourceUnavailable };
        output.description.output_coverage.changes =
            DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::CatalogComparisonUnavailable };
    }
    output.description.analysis_id = output.description.expected_analysis_id().unwrap();
    output.validate().unwrap();
    source.observation = SourceObservation::DerivedDiagnosis(Box::new(output.description.clone()));
    (source, output)
}

pub fn assert_derived_capture(
    capture: &crate::analytical::evidence::EvidenceCapture,
    source: &CollectionDescription,
    records: &[TelemetryRecord],
) {
    let SourceObservation::DerivedDiagnosis(expected) = &source.observation else { panic!("Gold source only") };
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    assert_eq!(capture.source(), source);
    assert_eq!(capture.derived_description(), Some(expected.as_ref()));
    assert_eq!(capture.records().len(), records.len());
    assert_eq!(capture.derived_incidents().count(), expected.counts.incidents as usize);
    assert_eq!(capture.derived_findings().count(), expected.counts.findings as usize);
    assert_eq!(capture.derived_changes().count(), expected.counts.changes as usize);
    for record in records {
        let expected = ExportedRecord::new(source, capture.capture(), record.clone()).unwrap();
        let actual = capture
            .records()
            .iter()
            .find(|row| row.row().record_id == expected.record_id && row.row().batch_id == expected.batch_id)
            .unwrap();
        assert_eq!(actual.row(), &expected);
    }
    for (stream, count) in [
        (TelemetryStream::DerivedIncidents, expected.counts.incidents),
        (TelemetryStream::DerivedFindings, expected.counts.findings),
        (TelemetryStream::DerivedChanges, expected.counts.changes),
    ] {
        assert_eq!(capture.captured_rows(stream), Some(count));
    }
    assert_eq!(capture.metrics().recovery_objects, 0, "retained Gold DTOs do not borrow native payloads");
    assert!(!serde_json::to_string(capture.records()).unwrap().contains("private-gold-canary"));
}
