//! Authenticated native Settings inputs use the same pure and canonical Gold path without a provider feature.
use super::*;
use culprit_evidence::{
    analytical::{
        evidence::{EvidenceFamily, EvidenceReadBudget, EvidenceReader},
        indexes::ProjectionIndexes,
        test_support::{self, ReadOnlyFixture, SettingsFindingCase as Case},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::reopen},
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const RECORDED_AT: &str = "2020-01-01T12:02:00Z";
fn display() -> DerivedDisplayContext {
    DerivedDisplayContext {
        connection_alias: "Approved native settings alias".into(),
        environment: None,
        database_name: None,
    }
}
fn policy() -> ChecksSynthesizer {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "controlled-native-settings-rules-1").unwrap()
}
async fn read(source: CollectionDescription, records: Vec<TelemetryRecord>, family: EvidenceFamily) -> EvidenceCapture {
    static NEXT_CAPTURE: AtomicU64 = AtomicU64::new(8_000_000);
    let sequence = NEXT_CAPTURE.fetch_add(1, Ordering::SeqCst);
    let fixture =
        ReadOnlyFixture::capture_at(source, records, sequence, "2020-01-01T12:00:00Z", "2020-01-01T12:00:01Z").await;
    let transport = fixture.transport();
    let recovery = ObjectRecoveryReader::open(transport.clone(), fixture.contract).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store, Arc::new(ProjectionIndexes::new(transport)));
    reader.read_capture(&range, &[family], EvidenceReadBudget::default(), None).await.unwrap()
}
async fn captured(case: Case) -> EvidenceCapture {
    let (source, records) = test_support::settings_finding_evidence(case);
    read(source, records, EvidenceFamily::SettingObservations).await
}
fn autovacuum(output: &DerivedDiagnosis) -> Option<&DerivedFinding> {
    output.findings.iter().find(|row| row.finding.check == "config" && row.finding.subject.name == "autovacuum")
}

#[tokio::test]
async fn canonical_server_setting_reaches_existing_config_check_and_gold_without_a_workload_or_executable_fix() {
    let capture = captured(Case::Disabled).await;
    assert!(capture.is_read_complete());
    assert!(capture.source().target.database().unwrap().incarnation.is_none());
    assert_eq!(capture.source().partition, SourcePartition::Target);
    let before = serde_json::to_vec(capture.source()).unwrap();
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    output.validate().unwrap();
    let finding = autovacuum(&output).expect("actual builtin ConfigCheck finding");
    assert_eq!(finding.finding.title, culprit_core::t!("check.config.autovacuum.title"));
    assert_eq!(finding.finding.summary, culprit_core::t!("check.config.autovacuum.summary"));
    assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());
    assert_eq!(finding.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
    assert!(finding.query.is_none() && finding.cause.is_none());
    assert_eq!(finding.inputs, vec![capture.verified_original().unwrap().reference(0).unwrap()]);
    assert!(matches!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated));
    let [assessment] = output.description.finding_evaluations.assessments.as_slice() else { panic!() };
    assert_eq!(assessment.scope, DerivedFindingScope::Settings);
    assert_eq!(assessment.input, 0);
    assert!(assessment.catalog_inputs.is_empty());
    assert!(output.description.finding_evaluations.checks.iter().any(|state| state.status == CheckStatus::Unavailable));
    assert_eq!(serde_json::to_vec(capture.source()).unwrap(), before);
    let rendered = serde_json::to_string(&output).unwrap();
    for private in ["fixture.invalid", "native-pg-system", "native-pg-member"] {
        assert!(!rendered.contains(private));
    }
}

#[tokio::test]
async fn enabled_null_session_partial_empty_and_unknown_native_values_keep_explicit_settings_assessment() {
    for case in Case::ALL {
        let capture = captured(case).await;
        let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        output.validate().unwrap();
        assert_eq!(autovacuum(&output).is_some(), matches!(case, Case::Disabled | Case::Partial), "{case:?}");
        let inventory = &output.description.finding_evaluations;
        assert_eq!(inventory.assessment_count, 1, "{case:?}");
        assert_eq!(inventory.assessments[0].scope, DerivedFindingScope::Settings);
        assert!(
            inventory
                .checks
                .iter()
                .any(|state| matches!(state.status, CheckStatus::Unavailable | CheckStatus::InsufficientEvidence))
        );
        if matches!(case, Case::NullValue | Case::SessionOnly | Case::Partial | Case::UnknownNature | Case::ReadOnly) {
            assert!(
                inventory.assessments[0]
                    .limitations
                    .contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingSettings)
            );
        }
        assert!(output.description.input_read_complete, "native population coverage is independent of selected EOF");
    }
}

struct SettingsWitness(Arc<AtomicBool>);
impl culprit_checks::pipeline::Prepare for SettingsWitness {
    fn name(&self) -> &str {
        "native settings pipeline witness"
    }
    fn prepare(&self, snapshot: &mut Snapshot) {
        assert!(snapshot.workload.is_none());
        assert!(snapshot.tables.is_empty() && snapshot.indexes.is_empty());
        assert!(snapshot.server.capabilities.is_empty());
        assert!(snapshot.server.cloud.is_none() && snapshot.server.platform.is_none());
        assert_eq!(snapshot.setting("autovacuum").unwrap().value, "off");
        self.0.store(true, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn installed_prepare_checks_and_rules_remain_the_single_settings_analysis_owner() {
    let capture = captured(Case::Disabled).await;
    let entered = Arc::new(AtomicBool::new(false));
    let pipeline = culprit_checks::Pipeline::builtin()
        .with_prepare(Arc::new(SettingsWitness(entered.clone())))
        .with_check(Arc::new(super::test_support::CaptureFinding { many: false }));
    let policy = ChecksSynthesizer::new(Arc::new(pipeline), "installed-settings-rules-9").unwrap();
    let output = policy.synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    assert!(entered.load(Ordering::SeqCst));
    assert!(autovacuum(&output).is_some());
    assert!(output.findings.iter().any(|row| row.finding.check == "controlled-capture"));
    assert_eq!(output.description.rules_version, "installed-settings-rules-9");
}

#[tokio::test]
async fn raw_target_snapshot_without_captured_acquisition_cannot_borrow_member_or_role_from_context() {
    let (mut source, records) = test_support::settings_finding_evidence(Case::Disabled);
    let header = source.observation.snapshot().unwrap().clone();
    source.observation = SourceObservation::Snapshot(header);
    let unqualified = read(source, records, EvidenceFamily::SettingObservations).await;
    let qualified = captured(Case::Disabled).await;
    let output = policy().synthesize_captures(&unqualified, &[&qualified], RECORDED_AT, &display()).unwrap();
    assert!(output.findings.is_empty());
    assert!(matches!(output.description.output_coverage.findings, DerivedOutputCoverage::ContextUnavailable { .. }));
    assert_eq!(output.description.finding_evaluations.assessment_count, 0);
}

#[tokio::test]
async fn settings_output_stays_english_restores_reader_locale_and_does_not_publish_session_values() {
    let (mut source, mut records) = test_support::settings_finding_evidence(Case::Disabled);
    let TelemetryRecord::Setting(mut session) = records[0].clone() else { panic!() };
    session.name = "application_name".into();
    session.scope = MeasurementScope::Session;
    session.value_in_use = Some("raw-session-private-canary".into());
    records.push(TelemetryRecord::Setting(session));
    let SourceObservation::SnapshotAcquisition { snapshot, .. } = &mut source.observation else { panic!() };
    snapshot.records = 2;
    snapshot.sources[0] = StateSourceCoverage::collected("pg_settings".into(), 2);
    let capture = read(source, records, EvidenceFamily::SettingObservations).await;
    let output = culprit_core::i18n::scope("fr", || {
        let before = culprit_core::i18n::current().code.clone();
        let result = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        assert_eq!(culprit_core::i18n::current().code, before);
        result
    });
    assert_eq!(output.description.language, "en");
    let english = culprit_core::i18n::scope("en", || culprit_core::t!("check.config.autovacuum.title"));
    assert_eq!(autovacuum(&output).unwrap().finding.title, english);
    assert!(!serde_json::to_string(&output).unwrap().contains("raw-session-private-canary"));
    assert!(serde_json::to_string(&capture.records()[1].row().record).unwrap().contains("raw-session-private-canary"));
}

#[tokio::test]
async fn a_settings_capture_cannot_erase_or_supply_unresolved_original_query_history_findings() {
    let (source, mut records) = test_support::query_evidence();
    records
        .retain(|record| !matches!(record, TelemetryRecord::QueryDefinition(_) | TelemetryRecord::PlanDefinition(_)));
    for record in &mut records {
        let key = match record {
            TelemetryRecord::QueryRuntime(row) => &mut row.key,
            TelemetryRecord::QueryWaits(row) => &mut row.key,
            _ => continue,
        };
        // Native identities survive unavailable definitions; a capture cannot declare exported IDs it omitted.
        for reference in [&mut key.references.query, &mut key.references.plan].into_iter().flatten() {
            reference.definition_id = None;
        }
    }
    let fixture =
        ReadOnlyFixture::capture_at(source, records, 8_100_000, "2020-01-01T12:00:00Z", "2020-01-01T12:00:01Z").await;
    let transport = fixture.transport();
    let recovery = ObjectRecoveryReader::open(transport.clone(), fixture.contract).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store, Arc::new(ProjectionIndexes::new(transport)));
    let query = reader
        .read_capture(
            &range,
            &[
                EvidenceFamily::QueryDefinitions,
                EvidenceFamily::PlanDefinitions,
                EvidenceFamily::QueryRuntime,
                EvidenceFamily::QueryWaits,
            ],
            EvidenceReadBudget::default(),
            None,
        )
        .await
        .unwrap();
    let settings = captured(Case::Enabled).await;
    let pipeline =
        culprit_checks::Pipeline::builtin().with_check(Arc::new(super::test_support::CaptureFinding { many: false }));
    let policy = ChecksSynthesizer::new(Arc::new(pipeline), "unchanged-unresolved-query-policy").unwrap();
    let before = policy.synthesize_captures(&query, &[], RECORDED_AT, &display()).unwrap();
    let after = policy.synthesize_captures(&query, &[&settings], RECORDED_AT, &display()).unwrap();
    assert_eq!(
        before.findings.iter().map(|row| &row.finding).collect::<Vec<_>>(),
        after.findings.iter().map(|row| &row.finding).collect::<Vec<_>>()
    );
    assert_eq!(before.description.finding_evaluations, after.description.finding_evaluations);
    assert!(after.findings.iter().any(|row| row.finding.check == "controlled-capture"));
    assert!(
        after
            .description
            .finding_evaluations
            .assessments
            .iter()
            .all(|assessment| matches!(assessment.scope, DerivedFindingScope::QueryHistory { .. }))
    );
    assert!(after.description.finding_evaluations.assessments.iter().any(|assessment| {
        assessment
            .limitations
            .contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingQueryDefinition)
    }));
}

#[tokio::test]
async fn native_readonly_role_stays_visible_and_never_becomes_a_primary_autovacuum_cause_or_fix() {
    let capture = captured(Case::ReadOnly).await;
    let context = capture.source().observation.snapshot_acquisition_context().unwrap();
    assert_eq!(context.database_writable, Some(false));
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    assert!(autovacuum(&output).is_none());
    assert!(output.description.context_issues.contains(&DerivedContextIssue::MaintenanceInactiveInRecovery));
    assert!(
        output.description.limitations().contains(&culprit_core::t!("engine.collected_analysis.recovery_settings"))
    );
    assert!(output.description.finding_evaluations.checks.iter().any(|state| state.check == "config"
        && matches!(state.status, CheckStatus::Unavailable | CheckStatus::InsufficientEvidence)));
    assert!(matches!(&capture.records()[0].row().record,
        TelemetryRecord::Setting(setting) if setting.value_in_use.as_deref() == Some("off")));
}

#[tokio::test]
async fn scan_shaped_and_retained_settings_use_one_role_policy_and_preserve_the_same_raw_native_cohort() {
    for case in [Case::Disabled, Case::ReadOnly] {
        let capture = captured(case).await;
        let original = capture.verified_original().unwrap();
        let pipeline = culprit_checks::Pipeline::builtin();
        let retained = crate::collected_evidence::analyze_settings_original(&original, &pipeline).unwrap();
        let snapshot = &retained.report.snapshot;
        let read = snapshot.operations.postgres_settings.as_ref().unwrap();
        assert_eq!(read.observations.len(), capture.records().len());
        assert_eq!(read.observations[0].value_in_use.as_deref(), Some("off"));
        let PostgresSettingsAcquisition::Completed { context } = &read.acquisition else { panic!() };
        assert_eq!(context.as_ref(), capture.source().observation.snapshot_acquisition_context().unwrap());
        assert_eq!(snapshot.setting("autovacuum").unwrap().value, "off");
        let saved = serde_json::to_vec(snapshot).unwrap();
        let restored: Snapshot = serde_json::from_slice(&saved).unwrap();
        let mut direct_snapshot = restored;
        // Neither a changed display value nor the safe-session flag can replace the original native role/value.
        direct_snapshot.settings.iter_mut().find(|setting| setting.name == "autovacuum").unwrap().value = "on".into();
        direct_snapshot.server.read_only_session = !matches!(case, Case::ReadOnly);
        let direct = pipeline.run(direct_snapshot);
        let configured = |report: &culprit_core::Report| {
            report
                .findings
                .iter()
                .find(|finding| finding.check == "config" && finding.subject.name == "autovacuum")
                .map(|finding| (finding.title.clone(), finding.summary.clone(), finding.severity))
        };
        assert_eq!(configured(&direct), configured(&retained.report));
        assert_eq!(configured(&direct).is_some(), matches!(case, Case::Disabled));
        if let Some(finding) = retained.report.findings.iter().find(|finding| finding.subject.name == "autovacuum") {
            assert!(finding.fix.as_ref().unwrap().sql.is_none());
        }
        let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        assert_eq!(autovacuum(&output).is_some(), matches!(case, Case::Disabled));
        output.validate().unwrap();
    }
}

#[tokio::test]
async fn mssql_and_managed_instance_settings_reach_config_check_with_row_lineage_and_no_executable_sql() {
    for (format, version) in
        [(NativeSettingsFormat::MssqlSettings, "16"), (NativeSettingsFormat::AzureSqlManagedInstanceSettings, "12")]
    {
        let target: CollectionTarget =
            DatabaseIdentity::server(Engine::Mssql, "shop", "fixture.invalid", 1433, None).into();
        let partition = SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Mssql,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("native-mssql-host"),
                native: NativeIdentity::new("native-mssql-instance"),
            }),
            epoch: Some(digest(b"local-mssql-recovery-epoch")),
        };
        let execution = NativeReadContext {
            target: target.clone(),
            partition: partition.clone(),
            observed_from: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:01Z".into(),
            database_writable: Some(true),
        };
        let records = vec![
            TelemetryRecord::Setting(SettingObservation {
                source: "database settings".into(),
                namespace: "sys.databases".into(),
                name: "compatibility_level".into(),
                scope: MeasurementScope::Database,
                nature: SettingNature::Configuration,
                value: Some("130".into()),
                value_in_use: None,
                value_for_secondary: None,
                unit: None,
                native_context: None,
                native_source: None,
                pending_restart: None,
                native_is_default: None,
                default_value: None,
                default_source: SettingDefaultSource::Unknown,
            }),
            TelemetryRecord::Setting(SettingObservation {
                source: "database settings".into(),
                namespace: "sys.databases".into(),
                name: "is_read_committed_snapshot_on".into(),
                scope: MeasurementScope::Database,
                nature: SettingNature::Configuration,
                value: Some("0".into()),
                value_in_use: None,
                value_for_secondary: None,
                unit: None,
                native_context: None,
                native_source: None,
                pending_restart: None,
                native_is_default: None,
                default_value: None,
                default_source: SettingDefaultSource::Unknown,
            }),
            TelemetryRecord::Setting(SettingObservation {
                source: "server settings".into(),
                namespace: "sys.configurations".into(),
                name: "cost threshold for parallelism".into(),
                scope: MeasurementScope::Server,
                nature: SettingNature::Configuration,
                value: Some("5".into()),
                value_in_use: Some("5".into()),
                value_for_secondary: None,
                unit: None,
                native_context: None,
                native_source: None,
                pending_restart: None,
                native_is_default: None,
                default_value: Some("5".into()),
                default_source: SettingDefaultSource::EngineSpec,
            }),
        ];
        let source = CollectionDescription {
            target,
            partition,
            source: "settings-state".into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::SnapshotAcquisition {
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::Settings,
                    observed_from: "2020-01-01T12:00:00Z".into(),
                    observed_until: "2020-01-01T12:00:01Z".into(),
                    records: 3,
                    sources: vec![
                        StateSourceCoverage::collected("database settings".into(), 2),
                        StateSourceCoverage::collected("server settings".into(), 1),
                    ],
                },
                execution: Box::new(execution),
                settings_context: Some(NativeSettingsContext { format, version: version.into() }),
            },
        };
        assert!(source.partition_is_consistent());
        let capture = read(source, records, EvidenceFamily::SettingObservations).await;
        let original = capture.verified_original().unwrap();
        let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        output.validate().unwrap();
        assert!(matches!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated));
        for (subject, ordinal) in
            [("compatibility-level", 0), ("read-committed-snapshot", 1), ("cost-threshold-for-parallelism", 2)]
        {
            let finding = output
                .findings
                .iter()
                .find(|row| row.finding.check == "config" && row.finding.subject.name == subject)
                .unwrap_or_else(|| panic!("missing {subject} for {format:?}"));
            assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());
            assert_eq!(finding.inputs, vec![original.reference(ordinal).unwrap()]);
        }
    }
}

mod global_variables;
