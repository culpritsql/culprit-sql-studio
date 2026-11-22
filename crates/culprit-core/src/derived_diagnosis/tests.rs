use super::*;
use crate::{
    DatabaseIdentity, Engine, MeasurementScope,
    events::{EventKind, EventSource},
};

fn capture(id: &str) -> DiagnosticCaptureRef {
    DiagnosticCaptureRef {
        capture: CaptureIdentity {
            observation_id: id.into(),
            revision: Some(CaptureRevision {
                outbox_id: "controlled-owner".into(),
                owner_epoch: "1".into(),
                sequence: "1".into(),
            }),
        },
        manifest_sha256: digest(id.as_bytes()),
        source_scope_id: digest(b"controlled-scope"),
    }
}
fn reference(id: &str) -> DiagnosticEvidenceRef {
    let capture = capture(id);
    DiagnosticEvidenceRef {
        capture: capture.capture.clone(),
        manifest_sha256: capture.manifest_sha256,
        source_scope_id: capture.source_scope_id,
        stream: TelemetryStream::OperationalSamples,
        native_key: NativeRecordIdentity::Operational { source: DIAGNOSTICS_SOURCE.into(), key: digest(b"revision") },
        batch_id: batch_id(id, TelemetryStream::OperationalSamples).unwrap(),
        record_id: digest(b"record"),
        payload_sha256: digest(b"payload"),
        representation_version: REPRESENTATION_VERSION,
        row_ordinal: 0,
    }
}
fn description() -> DerivedDiagnosisDescription {
    let mut description = DerivedDiagnosisDescription {
        analysis_id: String::new(),
        analyzer: ComponentVersion {
            component: "controlled-analyzer".into(),
            version: Some("1".into()),
            revision: None,
        },
        rules_version: "rules-1".into(),
        language: "en".into(),
        display: display(),
        recorded_at: "2026-11-21T11:00:00Z".into(),
        observed_from: "2026-11-21T10:00:00Z".into(),
        observed_until: "2026-11-21T10:01:00Z".into(),
        inputs: vec![DerivedInputReference {
            capture: capture("original"),
            role: DerivedInputRole::Revision,
            catalog: None,
        }],
        origin: AnalysisOrigin::Collected,
        finding_evaluations: Default::default(),
        change_evaluations: vec![],
        context_issues: vec![],
        input_read_complete: true,
        counts: DerivedDiagnosisCounts::default(),
        output_coverage: DerivedOutputCoverageSet {
            incidents: DerivedOutputCoverage::Evaluated,
            findings: DerivedOutputCoverage::Evaluated,
            changes: DerivedOutputCoverage::Evaluated,
        },
    };
    description.analysis_id = description.expected_analysis_id().unwrap();
    description
}
fn display() -> DerivedDisplayContext {
    DerivedDisplayContext {
        connection_alias: "Production alias".into(),
        environment: Some("prod".into()),
        database_name: None,
    }
}
fn database() -> DatabaseIdentity {
    DatabaseIdentity::server(Engine::Mssql, "app", "fixture.invalid", 1433, None)
}
fn incident() -> DerivedIncident {
    let reference = reference("original");
    DerivedIncident {
        analysis_id: description().analysis_id,
        database: Some(IncidentDatabaseSelector::from_route(&database()).unwrap()),
        display: display(),
        entry: IncidentHistoryEntry {
            controller_version: 1,
            revision_id: digest(b"revision"),
            episode_id: digest(b"episode"),
            target_id: database().telemetry_id().unwrap(),
            target_scope: MeasurementScope::Database,
            scope_id: digest(b"scope"),
            source: "activity".into(),
            scope: MeasurementScope::Database,
            scope_label: "Selected database".into(),
            revision: "2".into(),
            previous_revision: Some("1".into()),
            predecessor_selected: false,
            lifecycle: DiagnosticLifecycle::Active,
            analysis: DiagnosticAnalysisStatus::Partial,
            title: "Native wait retained".into(),
            phase: "Partial analysis".into(),
            first_seen_at: "2026-11-21T10:00:00".into(),
            last_seen_at: "2026-11-21T10:01:00".into(),
            recorded_at: "2026-11-21T10:02:00Z".into(),
            original: None,
            evidence: vec![],
            input_references: 1,
            followup_recipe: None,
            followup_recipe_version: None,
            issue: Some(DiagnosticAnalysisIssue::EvidenceUnavailable),
            reasons: vec![],
            next_step: "Inspect retained original evidence".into(),
        },
        original_interval: None,
        revision_reference: reference,
        victim_summary: "A request waited".into(),
        affected_queries: vec![],
        culprit_queries: vec![],
        candidate_queries: vec![],
        attribution_status: DerivedAttributionStatus::Unestablished,
        mechanism: None,
        developer_action: "Inspect original evidence".into(),
        verification: "Compare a new qualified observation".into(),
        caller: None,
    }
}
#[test]
fn route_selector_removes_only_incarnation_and_never_normalizes_names_or_endpoints() {
    let mut route = database();
    let key = IncidentDatabaseSelector::from_route(&route).unwrap();
    route.incarnation = Some("another-incarnation".into());
    assert_eq!(IncidentDatabaseSelector::from_route(&route).unwrap(), key);
    route.database = "APP".into();
    assert_ne!(IncidentDatabaseSelector::from_route(&route).unwrap(), key);
    assert!(serde_json::from_str::<IncidentDatabaseSelector>(&format!("\"{}\"", "A".repeat(64))).is_err());
    assert_eq!(serde_json::from_value::<IncidentDatabaseSelector>(serde_json::to_value(&key).unwrap()).unwrap(), key);
}
#[test]
fn empty_capture_and_partial_selected_inputs_are_valid_without_a_dummy_row_or_healthy_claim() {
    let mut output =
        DerivedDiagnosis { description: description(), incidents: vec![], findings: vec![], changes: vec![] };
    assert!(output.validate().is_ok());
    assert_eq!(output.clone().into_records().count(), 0);
    output.description.input_read_complete = false;
    assert!(output.validate().is_ok());
    assert!(!output.description.input_read_complete);
    output.description.counts.incidents = 1;
    assert!(output.validate().is_err());
}
#[test]
fn capture_membership_does_not_claim_row_authentication_and_exact_manifest_change_is_rejected() {
    let header = description();
    let mut row = reference("original");
    assert!(header.contains_capture_for(&row));
    row.row_ordinal = 99;
    assert!(
        header.contains_capture_for(&row),
        "the input resolver, not header membership, authenticates row positions"
    );
    row.manifest_sha256 = digest(b"tampered original");
    assert!(!header.contains_capture_for(&row));
    let mut header = header;
    header.inputs.push(header.inputs[0].clone());
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert!(!header.is_valid());
    header.inputs[1].capture.manifest_sha256 = digest(b"contradictory capture manifest");
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert!(!header.is_valid(), "one original capture cannot claim two manifest bodies");
}
#[test]
fn utc_description_and_original_interval_are_independent_of_native_offset_free_labels() {
    let mut row = incident();
    assert!(row.is_valid());
    row.original_interval = Some(TelemetryInterval {
        native_id: "native-1".into(),
        from: "2026-11-21T10:00:00".into(),
        until: "2026-11-21T10:01:00".into(),
    });
    assert!(!row.is_valid());
    row.original_interval.as_mut().unwrap().from.push('Z');
    row.original_interval.as_mut().unwrap().until.push('Z');
    assert!(row.is_valid());
    let mut header = description();
    header.observed_from = "2026-11-21T10:00:00".into();
    assert!(!header.is_valid());
    header = description();
    header.recorded_at = "2026-11-21T13:00:00+02:00".into();
    assert!(!header.is_valid());
}
#[test]
fn decimal_predecessor_lifecycle_and_enrichment_survive_without_a_false_culprit() {
    let mut row = incident();
    assert!(row.is_valid());
    assert_eq!(row.entry.previous_revision.as_deref(), Some("1"));
    row.entry.previous_revision = Some("0".into());
    assert!(!row.is_valid());
    row = incident();
    row.entry.previous_revision = Some(digest(b"not a revision number"));
    assert!(!row.is_valid());
    row = incident();
    row.attribution_status = DerivedAttributionStatus::Established;
    assert!(!row.is_valid());
    row = incident();
    row.entry.lifecycle = DiagnosticLifecycle::Resolved;
    assert!(row.is_valid());
    assert_eq!(row.entry.analysis, DiagnosticAnalysisStatus::Partial);
    assert!(row.culprit_queries.is_empty());
}
#[test]
fn version_exact_inputs_and_approved_labels_choose_analysis_identity_without_becoming_native_identity() {
    let header = description();
    let id = header.analysis_id.clone();
    let mut changed = header.clone();
    changed.rules_version = "rules-2".into();
    assert_ne!(changed.expected_analysis_id().unwrap(), id);
    changed = header.clone();
    changed.inputs[0].capture.manifest_sha256 = digest(b"new native original");
    assert_ne!(changed.expected_analysis_id().unwrap(), id);
    changed = header.clone();
    changed.display.connection_alias = "Another alias".into();
    assert_ne!(changed.expected_analysis_id().unwrap(), id);
    assert!(!changed.is_valid(), "a label change requires a newly bound analysis identity");
    let mut row = incident();
    let original_database = row.database.clone();
    row.display.connection_alias = "Another alias".into();
    assert_eq!(row.analysis_id, id);
    assert!(!row.fits_description(&header), "row labels must come from their authenticated analysis header");
    changed.analysis_id = changed.expected_analysis_id().unwrap();
    row.analysis_id = changed.analysis_id.clone();
    assert!(row.fits_description(&changed));
    assert_eq!(row.database, original_database, "display aliases do not grant native ownership");
}

#[test]
fn empty_evaluation_keeps_approved_labels_and_unknown_names_after_serialization() {
    let output = DerivedDiagnosis { description: description(), incidents: vec![], findings: vec![], changes: vec![] };
    output.validate().unwrap();
    let restored: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&output).unwrap()).unwrap();
    assert_eq!(restored.description.display, display());
    assert_eq!(restored.description.display.database_name, None);
    assert_eq!(restored.description.counts.total(), Some(0));
    assert_eq!(restored.into_records().count(), 0, "an identifiable empty result needs no invented row");
}
#[test]
fn local_output_is_not_a_native_recipe_and_unknown_target_never_becomes_current_authority() {
    assert!(TelemetryCollection::DerivedDiagnosis.is_local_output());
    assert!(TelemetryCollection::Diagnostics.is_local_output());
    assert!(!TelemetryCollection::Activity.is_local_output());
    let mut row = incident();
    let source = CollectionDescription {
        target: database().into(),
        partition: SourcePartition::Target,
        source: DERIVED_DIAGNOSIS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::DerivedDiagnosis(Box::new(description())),
    };
    let mut header = description();
    header.counts.incidents = 1;
    let mut source = source;
    source.observation = SourceObservation::DerivedDiagnosis(Box::new(header));
    assert!(
        ExportedRecord::new(
            &source,
            &capture("derived").capture,
            TelemetryRecord::DerivedIncident(Box::new(row.clone()))
        )
        .is_ok()
    );
    row.database = Some(
        IncidentDatabaseSelector::from_route(&DatabaseIdentity::server(
            Engine::Mssql,
            "foreign",
            "fixture.invalid",
            1433,
            None,
        ))
        .unwrap(),
    );
    assert!(
        ExportedRecord::new(&source, &capture("derived").capture, TelemetryRecord::DerivedIncident(Box::new(row)))
            .is_err()
    );
}
#[test]
fn change_export_redacts_direct_words_without_changing_original_event_identity_or_time() {
    let mut change = DerivedChange {
        analysis_id: description().analysis_id,
        database: Some(IncidentDatabaseSelector::from_route(&database()).unwrap()),
        display: display(),
        change: ChangeEvent::new(
            EventKind::Mark,
            EventSource::Mark,
            "deploy",
            "2026-11-21T10:00:00Z",
            "Executed SELECT 'private-change-canary'",
        ),
        inputs: vec![reference("original")],
    };
    let id = change.change.id.clone();
    let at = change.change.at.clone();
    change.redact(Engine::Mssql);
    assert!(!change.change.title.text().contains("private-change-canary"));
    assert_eq!(change.change.id, id);
    assert_eq!(change.change.at, at);
}

#[test]
fn unevaluated_zero_changes_and_capture_wide_finding_basis_remain_explicit() {
    let mut header = description();
    header.output_coverage.changes =
        DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::CatalogComparisonUnavailable };
    assert!(header.is_valid());
    assert_eq!(header.output_coverage.limitations().len(), 1);
    header.counts.changes = 1;
    assert!(!header.is_valid(), "an unavailable comparison cannot emit or grade a change");
    let value = serde_json::to_value(header.output_coverage).unwrap();
    assert_eq!(value["changes"]["state"], "context-unavailable");
    assert_eq!(value["changes"]["reason"], "catalog-comparison-unavailable");
    let original = reference("original");
    let mut change = DerivedChange {
        analysis_id: description().analysis_id,
        database: Some(IncidentDatabaseSelector::from_route(&database()).unwrap()),
        display: display(),
        change: ChangeEvent::new(
            EventKind::Mark,
            EventSource::Mark,
            "mark",
            "2026-11-21T10:00:00Z",
            "Deployment marked",
        ),
        inputs: vec![],
    };
    assert!(change.fits_description(&description()));
    change.inputs = vec![original.clone(), original];
    assert!(!change.is_valid());
    let query = DerivedIncidentQuery {
        causal_role: None,
        identity: None,
        sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        caller: None,
        evidence: vec![],
    };
    assert!(!query.is_valid(), "actual query/cause evidence never becomes a capture-wide placeholder");
}

#[test]
fn zero_findings_keep_typed_locked_evaluations_and_independent_context_gaps() {
    use crate::{CheckStatus, collected_analysis::CollectedQueryPopulation};
    let mut header = description();
    header.finding_evaluations = DerivedFindingEvaluations {
        checks: vec![DerivedCheckEvaluation {
            check: "unused-index".into(),
            status: CheckStatus::Unavailable,
            reason: None,
            missing: vec![crate::Capability::IndexUsage],
            assessed_queries: 0,
        }],
        populations: vec![DerivedPopulationEvaluation {
            input: 0,
            population: CollectedQueryPopulation::Unknown,
            checks: vec![0],
            limitations: vec![crate::collected_analysis::CollectedAnalysisLimit::MissingSchema],
            catalog_inputs: vec![],
        }],
        population_count: 1,
        omitted_populations: 0,
    };
    header.context_issues.push(DerivedContextIssue::MissingOriginal);
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert!(header.is_valid());
    assert!(header.input_read_complete, "available selected bodies reached EOF independently of missing context");
    assert_eq!(header.counts.findings, 0);
    assert_eq!(header.limitations().len(), 2);
    let restored: DerivedDiagnosisDescription = serde_json::from_slice(&serde_json::to_vec(&header).unwrap()).unwrap();
    assert_eq!(restored, header);
    header.finding_evaluations.populations[0].checks[0] = 1;
    assert!(!header.is_valid(), "an invalid check-state reference cannot become an assessed empty result");
}
#[test]
fn omitted_evaluation_detail_is_explicit_and_unknown_indices_or_totals_refuse() {
    let mut header = description();
    header.finding_evaluations.population_count = 100;
    header.finding_evaluations.omitted_populations = 100;
    assert!(header.is_valid());
    assert_eq!(header.limitations().len(), 1);
    header.finding_evaluations.omitted_populations = 99;
    assert!(!header.is_valid());
    header.finding_evaluations.omitted_populations = 100;
    header.context_issues = vec![DerivedContextIssue::PartialCatalog, DerivedContextIssue::PartialCatalog];
    assert!(!header.is_valid(), "repeated reasons are not independent evidence gaps");
}

#[test]
fn missing_context_selection_changes_analysis_identity_without_turning_a_retry_into_enrichment() {
    let before = description();
    let mut gap = before.clone();
    gap.context_issues = vec![DerivedContextIssue::MissingOriginal];
    assert_ne!(gap.expected_analysis_id().unwrap(), before.analysis_id);
    assert!(!gap.is_valid(), "the previous selected-context identity must not authenticate different context gaps");
    gap.analysis_id = gap.expected_analysis_id().unwrap();
    assert!(gap.is_valid());
    gap.finding_evaluations.population_count = 1;
    gap.finding_evaluations.omitted_populations = 1;
    assert_eq!(gap.expected_analysis_id().unwrap(), gap.analysis_id, "rendered evaluation is rules-and-input output");
}

#[test]
fn established_actor_roles_require_their_typed_native_identity_and_never_promote_observed_sql() {
    let owner = ActivityWaitOwner::Session { id: "43".into(), generation: Some("native-owner-generation".into()) };
    let culprit = DerivedIncidentQuery {
        causal_role: Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner)),
        identity: Some(DerivedQueryIdentity::NativeWaitOwner(owner)),
        sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        caller: None,
        evidence: vec![reference("original")],
    };
    assert!(culprit.is_valid());
    let mut row = incident();
    row.attribution_status = DerivedAttributionStatus::Established;
    row.culprit_queries = vec![culprit.clone()];
    assert!(row.is_valid());
    let encoded = serde_json::to_value(&row).unwrap();
    assert_eq!(encoded["culprit_queries"][0]["causal_role"]["kind"], "native-blocking-owner");
    let decoded: DerivedIncident = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, row);
    row.culprit_queries[0].sql = DefinitionPayload::sql("SELECT 'observed-current-only'", Engine::Mssql);
    assert!(!row.is_valid(), "a native blocking actor does not prove the statement that acquired the lock");
    row.culprit_queries = vec![culprit.clone()];
    row.affected_queries = vec![culprit];
    assert!(!row.is_valid(), "the typed culprit role cannot be copied into the affected-query list");
}
#[test]
fn unknown_owner_and_missing_role_shape_never_authenticate_an_established_query_cause() {
    let mut query = DerivedIncidentQuery {
        causal_role: Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner)),
        identity: Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
            id: "43".into(),
            generation: None,
        })),
        sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        caller: None,
        evidence: vec![reference("original")],
    };
    assert!(!query.is_valid());
    query.causal_role = None;
    assert!(query.is_valid(), "unqualified actor identity remains unknown context without a causal role");
    let mut old_shape = serde_json::to_value(query).unwrap();
    old_shape.as_object_mut().unwrap().remove("causal_role");
    assert!(
        serde_json::from_value::<DerivedIncidentQuery>(old_shape).is_err(),
        "the new required-nullable field has no older-shape decoder"
    );
}

#[test]
fn zero_setting_changes_keep_exact_comparison_scope_and_plan_identity_without_a_schema_absence_claim() {
    let mut header = description();
    header.inputs = ["settings-before", "settings-after"]
        .into_iter()
        .enumerate()
        .map(|(index, id)| DerivedInputReference {
            capture: capture(id),
            role: DerivedInputRole::Catalog,
            catalog: Some(DerivedCatalogProvenance {
                observed_from: format!("2026-11-21T0{}:00:00Z", index + 8),
                observed_until: format!("2026-11-21T0{}:00:01Z", index + 8),
                native_time: None,
            }),
        })
        .collect();
    let before = header.expected_analysis_id().unwrap();
    header.change_evaluations.push(DerivedChangeEvaluation::Settings {
        previous_input: 0,
        current_input: 1,
        observed_after: "2026-11-21T08:00:00Z".into(),
        observed_until: "2026-11-21T09:00:01Z".into(),
    });
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert_ne!(header.analysis_id, before);
    assert!(header.is_valid());
    assert_eq!(header.counts.changes, 0);
    assert!(header.limitations().iter().any(|reason| reason.contains("configuration values")));
    assert!(serde_json::to_vec(&header).unwrap().len() < 4 * 1024);
    for case in ["same-input", "wrong-role", "missing-input", "local-clock", "unavailable"] {
        let mut invalid = header.clone();
        let DerivedChangeEvaluation::Settings { current_input, observed_after, .. } =
            &mut invalid.change_evaluations[0]
        else {
            panic!()
        };
        match case {
            "same-input" => *current_input = 0,
            "wrong-role" => invalid.inputs[0].role = DerivedInputRole::Observation,
            "missing-input" => *current_input = 2,
            "local-clock" => *observed_after = "2026-11-21T08:00:00".into(),
            "unavailable" => {
                invalid.output_coverage.changes =
                    DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::CatalogComparisonUnavailable }
            }
            _ => unreachable!(),
        }
        invalid.analysis_id = invalid.expected_analysis_id().unwrap();
        assert!(!invalid.is_valid(), "{case}");
    }
}
#[test]
fn derived_change_redaction_preserves_saved_catalog_authority_and_raw_detail_while_locales_are_explicit() {
    let mut row = DerivedChange {
        analysis_id: description().analysis_id,
        database: None,
        display: display(),
        inputs: vec![],
        change: ChangeEvent::new(
            EventKind::Setting,
            EventSource::Scans,
            "MAXDOP",
            "2026-11-21T10:00:00Z",
            crate::message!(
                "engine.history.setting_changed",
                name = "MAXDOP",
                from = "N'private-event-canary'",
                to = "8"
            ),
        )
        .detail("native SELECT N'private-detail-canary'")
        .between("2026-11-21T09:00:00Z"),
    };
    row.redact(Engine::Mssql);
    let encoded = serde_json::to_vec(&row).unwrap();
    assert!(!String::from_utf8(encoded.clone()).unwrap().contains("private-event-canary"));
    assert!(!String::from_utf8(encoded.clone()).unwrap().contains("private-detail-canary"));
    assert!(row.change.title.source().is_some());
    assert!(row.change.detail.as_ref().unwrap().source().is_none());
    let mut restored: DerivedChange = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored, row, "deserialization preserves saved bytes and English");
    let english = restored.change.title.text().to_owned();
    crate::i18n::scope("fr", || restored.change.refresh_language());
    assert_ne!(restored.change.title.text(), english);
    assert_eq!(restored.change.detail, row.change.detail);
    restored.change.clear_rendered_text();
    row.change.clear_rendered_text();
    assert_eq!(restored, row, "explicit locale rendering changes no event identity, source, clock or redacted input");
}

#[test]
fn zero_job_events_keep_the_single_original_assessment_and_cannot_impersonate_a_settings_pair() {
    let mut header = description();
    header.inputs[0].role = DerivedInputRole::Observation;
    let assessment = crate::JobExecutionAssessment {
        read_complete: true,
        sources: vec![crate::JobExecutionSourceCoverage {
            source: TIMESCALE_JOB_RUNS_SOURCE.into(),
            state: crate::SourceState::Unavailable,
            complete_scope: false,
            native_rows: 0,
            observed_rows: 0,
            stopped: None,
            error_code: Some(crate::ErrorCode::PermissionDenied),
        }],
        run_records: 0,
        step_records: 0,
        error_records: 0,
        last_start_records: 0,
        utc_placed_records: 0,
        unplaced_records: 0,
        omitted_entries: 0,
    };
    header.change_evaluations.push(DerivedChangeEvaluation::JobExecutions { input: 0, assessment });
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert!(header.is_valid());
    let original = header.analysis_id.clone();
    let DerivedChangeEvaluation::JobExecutions { assessment, .. } = &mut header.change_evaluations[0] else { panic!() };
    assessment.read_complete = false;
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert_ne!(header.analysis_id, original, "the selected assessment participates in immutable analysis identity");
    assert!(header.is_valid());
    let encoded = serde_json::to_value(&header.change_evaluations[0]).unwrap();
    assert_eq!(encoded["scope"], "job-executions");
    assert!(encoded.get("previous_input").is_none());
    let mut impersonated = encoded;
    impersonated["scope"] = serde_json::json!("settings");
    assert!(serde_json::from_value::<DerivedChangeEvaluation>(impersonated).is_err());
    header.change_evaluations.push(header.change_evaluations[0].clone());
    header.analysis_id = header.expected_analysis_id().unwrap();
    assert!(!header.is_valid());
}

#[test]
fn shared_worker_query_retains_member_and_native_database_label_without_target_or_sql_causal_authority() {
    let actor = ActivityKey {
        session: NativeActivityIdentity { id: "42".into(), generation: "native-login".into() },
        request: Some(NativeActivityIdentity { id: "0".into(), generation: "native-request".into() }),
        transaction: None,
    };
    let member = SharedResourceIdentity {
        engine: Engine::Mssql,
        kind: SharedResourceKind::Instance,
        namespace: NativeIdentity::new("native-server"),
        native: NativeIdentity::new("native-instance"),
    };
    let mut query = DerivedIncidentQuery {
        causal_role: Some(DerivedCausalRole::new(DerivedCausalRoleKind::AssignedWorkerContributor)),
        identity: Some(DerivedQueryIdentity::SharedActivity {
            member_id: member.telemetry_id().unwrap(),
            member_kind: member.kind,
            actor,
            statement: None,
            database: Some("foreign <db>".into()),
        }),
        sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        caller: None,
        evidence: vec![reference("original")],
    };
    assert!(query.is_valid());
    let copy: DerivedIncidentQuery = serde_json::from_slice(&serde_json::to_vec(&query).unwrap()).unwrap();
    assert_eq!(copy, query);
    assert!(copy.identity.as_ref().unwrap().shared_scope_label().unwrap().contains("foreign <db>"));
    query.sql = DefinitionPayload::sql("SELECT 1", Engine::Mssql);
    assert!(!query.is_valid(), "resource assignment never proves acquiring SQL");
    query.sql = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    let Some(DerivedQueryIdentity::SharedActivity { member_id, .. }) = &mut query.identity else { panic!() };
    member_id.clear();
    assert!(!query.is_valid(), "a database label cannot replace missing shared member identity");
}
