use super::*;
use crate::{Cli, Command};
use clap::Parser;
use culprit_core::Engine;
#[test]
fn ordinary_incident_request_requires_database_time_and_archive_config_but_no_manual_hash_or_source_credentials() {
    let cli = Cli::try_parse_from([
        "culprit",
        "incident",
        "orders",
        "--around",
        "2020-01-01T12:00:00Z",
        "--config",
        "archive.json",
    ])
    .unwrap();
    let Command::Incident(args) = cli.command else { panic!() };
    assert_eq!(args.resource, "orders");
    assert_eq!(args.around, "2020-01-01T12:00:00Z");
    assert!(Cli::try_parse_from(["culprit", "incident", "orders", "--config", "archive.json"]).is_err());
    assert!(Cli::try_parse_from(["culprit", "incident", "orders", "--around", "2020-01-01T12:00:00Z"]).is_err());
    assert!(
        Cli::try_parse_from([
            "culprit",
            "incident",
            "orders",
            "--around",
            "2020-01-01T12:00:00Z",
            "--config",
            "archive.json",
            "--range",
            "manual-hash"
        ])
        .is_err()
    );
}
fn incident_report() -> culprit_core::IncidentReadReport {
    use culprit_core::{
        DerivedAttributionStatus, DerivedDisplayContext, DerivedIncident, DerivedIncidentQuery,
        IncidentDatabaseSelector, IncidentHistoryReport, IncidentReadReport,
        collected_analysis::CollectedEvidenceMetrics, telemetry::*,
    };
    let samples: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/incident-history.sample.json")).unwrap();
    let history: IncidentHistoryReport = serde_json::from_value(samples["ongoing"].clone()).unwrap();
    let mut entry = history.entries[0].clone();
    let mut original = entry.evidence.iter().find_map(|row| row.reference.clone()).unwrap();
    original.stream = TelemetryStream::OperationalSamples;
    original.batch_id = batch_id(&original.capture.observation_id, original.stream).unwrap();
    original.native_key =
        NativeRecordIdentity::Operational { source: DIAGNOSTICS_SOURCE.into(), key: entry.revision_id.clone() };
    entry.original = Some(original.clone());
    let incident = DerivedIncident {
        analysis_id: digest(b"controlled formatter analysis"),
        database: Some(IncidentDatabaseSelector::from_key(&digest(b"controlled route")).unwrap()),
        display: DerivedDisplayContext { connection_alias: "alias".into(), environment: None, database_name: None },
        entry,
        original_interval: None,
        revision_reference: original,
        victim_summary: "observed victim\n\u{1b}native label".into(),
        affected_queries: vec![],
        culprit_queries: vec![],
        candidate_queries: vec![DerivedIncidentQuery {
            causal_role: None,
            identity: None,
            sql: DefinitionPayload::sql("SELECT name FROM dbo.orders", Engine::Mssql),
            caller: None,
            evidence: vec![history.entries[0].evidence[0].reference.clone().unwrap()],
        }],
        attribution_status: DerivedAttributionStatus::CandidatesOnly,
        mechanism: None,
        developer_action: "bounded action".into(),
        verification: "verify the original native scope".into(),
        caller: None,
    };
    IncidentReadReport {
        database: incident.database.clone().unwrap(),
        from: "2020-01-01T12:00:00Z".into(),
        until: "2020-01-01T12:10:00Z".into(),
        recorded_before: "2020-01-01T13:00:00Z".into(),
        discovery_complete: false,
        incidents: vec![incident],
        analyses: vec![],
        history,
        relationships: vec![],
        gaps: vec![culprit_core::IncidentReadGap::CandidateLimit],
        archive_work: CollectedEvidenceMetrics {
            selected_rows: 0,
            queries: 0,
            pages: 0,
            returned_rows: 0,
            response_bytes: 0,
            proof_bytes: 0,
            recovery_objects: 0,
            recovery_bytes: 0,
        },
        candidate_rows: 0,
    }
}
#[test]
fn human_incident_output_keeps_victim_candidate_and_culprit_fields_distinct_and_escapes_database_text() {
    let report = incident_report();
    let text = observations::incidents(&report);
    assert!(text.contains(&t!("incident.cli.culprit_unknown")));
    assert!(!text.contains("Supported culprit query:"));
    assert!(text.contains(&t!("incident.cli.candidate", query = "SELECT name FROM dbo.orders")));
    assert!(text.contains(&t!("incident.cli.mechanism_unknown")));
    assert!(text.contains(&t!("evidence.cli.read_incomplete")));
    assert!(!text.contains('\u{1b}'));
    assert!(text.contains("\\u{1b}"));
    let json = serde_json::to_value(&report).unwrap();
    let row = &json["incidents"][0];
    for key in [
        "victim_summary",
        "affected_queries",
        "culprit_queries",
        "candidate_queries",
        "mechanism",
        "developer_action",
        "verification",
        "attribution_status",
    ] {
        assert!(row.get(key).is_some(), "{key}");
    }
    assert_eq!(row["attribution_status"], "candidates-only");
}

fn analysis_context(report: &culprit_core::IncidentReadReport) -> culprit_core::IncidentAnalysisContext {
    use culprit_core::*;
    let (_, mut diagnosis) = culprit_evidence::analytical::test_support::derived_diagnosis_output(false, false);
    let description = &mut diagnosis.description;
    description.input_read_complete = false;
    description.context_issues =
        vec![DerivedContextIssue::MissingOriginal, DerivedContextIssue::AcquisitionCatalogOnly];
    description.finding_evaluations = DerivedFindingEvaluations {
        checks: vec![DerivedCheckEvaluation {
            check: "controlled-native-check".into(),
            status: CheckStatus::Unavailable,
            reason: None,
            missing: vec![],
            assessed_queries: 0,
        }],
        populations: vec![],
        population_count: 1,
        omitted_populations: 1,
    };
    description.analysis_id = description.expected_analysis_id().unwrap();
    assert!(description.is_valid());
    IncidentAnalysisContext {
        capture: report.incidents[0].revision_reference.capture_ref(),
        description: diagnosis.description,
    }
}

#[test]
fn human_incidents_preserve_context_and_evaluation_limitations_even_without_output_coverage_gaps() {
    let mut report = incident_report();
    report.analyses = vec![analysis_context(&report)];
    assert!(report.analyses[0].description.output_coverage.limitations().is_empty());
    let text = observations::incidents(&report);
    for reason in report.analyses[0].description.limitations() {
        assert!(text.contains(&culprit_core::format::escaped_preview(&reason, 1024).text), "{reason}");
    }
    assert!(text.contains(&t!("incident.cli.inputs_partial")));
    let description = &mut report.analyses[0].description;
    description.input_read_complete = true;
    description.context_issues.clear();
    description.finding_evaluations = Default::default();
    description.analysis_id = description.expected_analysis_id().unwrap();
    assert!(description.is_valid());
    let complete = observations::incidents(&report);
    assert!(!complete.contains(&t!("view.synthesis.context.limited_checks")));
    assert!(!complete.contains(&t!("incident.cli.inputs_partial")));
}

fn native_owner(report: &culprit_core::IncidentReadReport) -> culprit_core::DerivedIncidentQuery {
    use culprit_core::telemetry::*;
    let mut query = report.incidents[0].candidate_queries[0].clone();
    query.sql = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    query.identity = Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
        id: "43".into(),
        generation: Some("native-owner-login-generation".into()),
    }));
    query.causal_role = Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner));
    query.caller = Some(DerivedCaller {
        client: Some(ActivityClient {
            application: Some("native-app".into()),
            user: None,
            host: Some("native-host".into()),
        }),
        session_context: Some(Box::new(ActivitySessionContext {
            lifecycle: SessionLifecycle::Idle,
            login_age_ms: None,
            last_request_end_age_ms: None,
            client_library: Some("native-library".into()),
            client_process_id: Some("4294967296".into()),
            native: None,
        })),
        evidence: query.evidence[0].clone(),
    });
    assert!(query.is_valid());
    query
}

#[test]
fn human_incidents_keep_native_role_and_db_caller_separate_from_sql_and_render_catalog_authority_for_the_reader() {
    let mut report = incident_report();
    report.incidents[0].culprit_queries = vec![native_owner(&report)];
    report.incidents[0].attribution_status = culprit_core::DerivedAttributionStatus::Established;
    let before = serde_json::to_value(&report).unwrap();
    culprit_core::i18n::scope("fr", || {
        let text = observations::incidents(&report);
        assert!(
            text.contains(&t!("incident.cli.causal_role", explanation = t!("view.synthesis.cause.blocking_owner")))
        );
        assert!(text.contains("native-app"));
        assert!(text.contains("native-host"));
        assert!(text.contains("native-library"));
        assert!(text.contains("4294967296"));
        assert!(text.contains(&t!("incident.cli.caller_qualification")));
        assert!(text.contains(&t!("incident.cli.caller_unavailable")), "the independent candidate has no caller");
        assert!(!text.contains(&t!("incident.cli.culprit_unknown")));
        assert!(!text.contains("native-owner-login-generation"));
        for reference in &report.incidents[0].culprit_queries[0].evidence {
            assert!(!text.contains(&reference.manifest_sha256));
            assert!(!text.contains(&reference.payload_sha256));
        }
    });
    assert_eq!(serde_json::to_value(&report).unwrap(), before, "formatting never rewrites the retained source/cache");
}

#[test]
fn human_native_role_and_caller_fields_escape_controls_and_truncate_unicode_without_changing_unknowns() {
    let mut report = incident_report();
    let mut query = native_owner(&report);
    let role = "native\n\u{1b} role ".repeat(300);
    query.causal_role.as_mut().unwrap().explanation = culprit_core::LocalizedText::raw(role.clone());
    let caller = query.caller.as_mut().unwrap();
    let application = format!("{}\n\u{1b}end-native-application", "界".repeat(120));
    caller.client.as_mut().unwrap().application = Some(application.clone());
    caller.client.as_mut().unwrap().host = Some("host\n\u{1b}label".into());
    caller.session_context.as_deref_mut().unwrap().client_library = None;
    report.incidents[0].culprit_queries = vec![query];
    report.incidents[0].attribution_status = culprit_core::DerivedAttributionStatus::Established;
    let text = observations::incidents(&report);
    assert!(text.contains(&culprit_core::format::escaped_preview(&role, 1024).text));
    assert!(text.contains(&culprit_core::format::escaped_preview(&application, 100).text));
    assert!(text.contains("host\\n\\u{1b}label"));
    assert!(!text.contains('\u{1b}'));
    assert!(!text.contains("end-native-application"));
    assert!(text.contains(&t!(
        "incident.cli.caller_session",
        library = t!("evidence.cli.page_holders.not_recorded"),
        process = "4294967296"
    )));
    assert!(text.contains(&t!("incident.cli.caller_unavailable")));
}
