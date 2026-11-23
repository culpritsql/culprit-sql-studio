//! The same physical original and installed pure Settings handler, with explicit native formats.
use super::*;
use test_support::{GlobalSettingsFindingCase as GlobalCase, global_settings_finding_evidence};
const FORMATS: [NativeSettingsFormat; 2] =
    [NativeSettingsFormat::MysqlGlobalVariables, NativeSettingsFormat::MariaDbGlobalVariables];
async fn global(format: NativeSettingsFormat, case: GlobalCase) -> EvidenceCapture {
    let (source, records) = global_settings_finding_evidence(format, case);
    read(source, records, EvidenceFamily::SettingObservations).await
}
fn setting<'a>(output: &'a DerivedDiagnosis, name: &str) -> Option<&'a DerivedFinding> {
    output.findings.iter().find(|row| row.finding.check == "config" && row.finding.subject.name == name)
}
#[tokio::test]
async fn both_native_global_formats_reach_existing_config_check_and_common_gold_without_pg_cohort_or_workload() {
    for format in FORMATS {
        let capture = global(format, GlobalCase::Disabled).await;
        assert!(capture.is_read_complete());
        let original = capture.verified_original().unwrap();
        let before = serde_json::to_vec(capture.source()).unwrap();
        let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        output.validate().unwrap();
        let finding = setting(&output, "performance_schema").unwrap();
        assert_eq!(finding.finding.severity, Severity::High);
        assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());
        assert!(finding.query.is_none() && finding.cause.is_none());
        assert_eq!(finding.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
        assert_eq!(finding.inputs, vec![original.reference(0).unwrap()]);
        assert_eq!(output.description.finding_evaluations.assessment_count, 1);
        assert_eq!(output.description.finding_evaluations.assessments[0].scope, DerivedFindingScope::Settings);
        assert!(!output.description.context_issues.contains(&DerivedContextIssue::MaintenanceInactiveInRecovery));
        assert_eq!(serde_json::to_vec(capture.source()).unwrap(), before);
        let json = serde_json::to_string(&output).unwrap();
        assert!(!json.contains("fixture.invalid") && !json.contains("server_uuid") && !json.contains("native-process"));
    }
}
#[tokio::test]
async fn partial_unknown_null_session_and_default_authority_keep_raw_values_and_do_not_become_healthy_absence() {
    for format in FORMATS {
        for case in GlobalCase::ALL {
            let capture = global(format, case).await;
            let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
            output.validate().unwrap();
            assert_eq!(
                setting(&output, "performance_schema").is_some(),
                matches!(case, GlobalCase::Disabled | GlobalCase::Partial | GlobalCase::ReadOnly),
                "{format:?}/{case:?}"
            );
            assert_eq!(
                setting(&output, "slow_query_log").is_some(),
                case == GlobalCase::SlowLogDisabled,
                "{format:?}/{case:?}"
            );
            let assessment = &output.description.finding_evaluations.assessments[0];
            assert_eq!(assessment.scope, DerivedFindingScope::Settings);
            if matches!(
                case,
                GlobalCase::NullValue
                    | GlobalCase::SessionOnly
                    | GlobalCase::Partial
                    | GlobalCase::Empty
                    | GlobalCase::UnknownNature
                    | GlobalCase::WrongUnit
                    | GlobalCase::WrongDefault
            ) {
                assert!(
                    assessment
                        .limitations
                        .contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingSettings),
                    "{format:?}/{case:?}"
                );
            }
            assert!(output.description.input_read_complete);
            if case != GlobalCase::Empty {
                assert_eq!(capture.records().len(), 4);
            }
        }
    }
}
#[tokio::test]
async fn actual_native_product_controls_existing_advice_without_claiming_a_current_execution_or_default() {
    for format in FORMATS {
        let capture = global(format, GlobalCase::SlowLogDisabled).await;
        let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        let finding = &setting(&output, "slow_query_log").unwrap().finding;
        assert!(finding.fix.as_ref().unwrap().sql.is_none());
        assert_eq!(
            finding.fix.as_ref().unwrap().cost.is_some(),
            format == NativeSettingsFormat::MariaDbGlobalVariables
        );
        let derived = setting(&output, "slow_query_log").unwrap();
        assert!(derived.query.is_none() && derived.cause.is_none());
    }
}
#[tokio::test]
async fn global_context_cannot_be_borrowed_from_a_different_capture_or_a_raw_target_header() {
    for format in FORMATS {
        let (mut source, records) = global_settings_finding_evidence(format, GlobalCase::Disabled);
        let header = source.observation.snapshot().unwrap().clone();
        source.observation = SourceObservation::Snapshot(header);
        let raw = read(source, records, EvidenceFamily::SettingObservations).await;
        let qualified = global(format, GlobalCase::Disabled).await;
        let output = policy().synthesize_captures(&raw, &[&qualified], RECORDED_AT, &display()).unwrap();
        assert!(output.findings.is_empty());
        assert_eq!(output.description.finding_evaluations.assessment_count, 0);
        assert!(matches!(
            output.description.output_coverage.findings,
            DerivedOutputCoverage::ContextUnavailable { .. }
        ));
    }
}
struct GlobalWitness {
    format: NativeSettingsFormat,
    entered: Arc<AtomicBool>,
}
impl culprit_checks::pipeline::Prepare for GlobalWitness {
    fn name(&self) -> &str {
        "native global settings original witness"
    }
    fn prepare(&self, snapshot: &mut Snapshot) {
        assert!(snapshot.workload.is_none() && snapshot.operations.postgres_settings.is_none());
        assert!(snapshot.server.capabilities.is_empty());
        assert!(snapshot.server.cloud.is_none() && snapshot.server.platform.is_none());
        assert_eq!(snapshot.setting("performance_schema").unwrap().value, "OFF");
        assert_eq!(snapshot.server.is_mariadb(), self.format == NativeSettingsFormat::MariaDbGlobalVariables);
        self.entered.store(true, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn shared_prepare_pipeline_and_plugin_remain_the_only_settings_analysis_owner() {
    for format in FORMATS {
        let capture = global(format, GlobalCase::Disabled).await;
        let entered = Arc::new(AtomicBool::new(false));
        let pipeline = culprit_checks::Pipeline::builtin()
            .with_prepare(Arc::new(GlobalWitness { format, entered: entered.clone() }))
            .with_check(Arc::new(super::super::test_support::CaptureFinding { many: false }));
        let output = ChecksSynthesizer::new(Arc::new(pipeline), "global-settings-installed-rules")
            .unwrap()
            .synthesize_captures(&capture, &[], RECORDED_AT, &display())
            .unwrap();
        assert!(entered.load(Ordering::SeqCst));
        assert!(setting(&output, "performance_schema").is_some());
        assert!(output.findings.iter().any(|row| row.finding.check == "controlled-capture"));
    }
}
