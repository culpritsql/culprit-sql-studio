//! Ordinary configuration findings use native settings, the installed Pipeline and the common Gold rows.
use super::*;
use culprit_evidence::original_projection::OriginalProjection;

pub(super) fn supports(original: &dyn OriginalProjection) -> bool {
    original.source().observation.snapshot_acquisition_context().is_some()
        && original
            .source()
            .observation
            .snapshot()
            .is_some_and(|snapshot| snapshot.dataset == SnapshotDataset::Settings)
        && original
            .source()
            .observation
            .snapshot_settings_context()
            .is_some_and(|context| context.is_valid_for(original.source().target.engine()))
}

pub(super) fn role_limitation(original: &dyn OriginalProjection) -> Option<DerivedContextIssue> {
    if !supports(original)
        || original.source().observation.snapshot_settings_context()?.format != NativeSettingsFormat::PostgresSettings
    {
        return None;
    }
    let read = crate::collected_evidence::settings_read(original).ok()?;
    read.configuration(original.source().target.database())
        .limitations
        .contains(&PostgresSettingsAdviceLimit::RecoveryMaintenanceInactive)
        .then_some(DerivedContextIssue::MaintenanceInactiveInRecovery)
}

pub(super) fn findings(
    original: &dyn OriginalProjection,
    description: &DerivedDiagnosisDescription,
    pipeline: &culprit_checks::Pipeline,
    budget: &mut OutputBudget,
) -> Result<queries::FindingProjection> {
    let analysis = crate::collected_evidence::analyze_settings_original(original, pipeline)?;
    let mut evaluations = evaluation::Evaluations::new();
    evaluations.add_settings(&analysis, 0, original.source().target.engine())?;
    let database = original.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
    let mut findings = finding_rows::FindingRows::new();
    for finding in &analysis.report.findings {
        let inputs = references(original, finding)?;
        let derived = DerivedFinding {
            analysis_id: description.analysis_id.clone(),
            database: database.clone(),
            display: description.display.clone(),
            finding: finding.clone(),
            inputs,
            query: None,
            cause: None,
            guidance: DerivedGuidance::HistoricalRequiresFreshVerification,
        };
        findings.admit(finding, derived, original.source().target.engine(), budget)?;
    }
    Ok(queries::FindingProjection { findings: findings.finish(), evaluations: evaluations.finish() })
}
fn references(original: &dyn OriginalProjection, finding: &Finding) -> Result<Vec<DiagnosticEvidenceRef>> {
    if finding.subject.kind != SubjectKind::Setting {
        return Ok(vec![]);
    }
    original
        .records()
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            matches!(&row.row().record, TelemetryRecord::Setting(setting)
            if (setting.name == finding.subject.name
                && matches!(setting.scope, MeasurementScope::Server | MeasurementScope::Node | MeasurementScope::Database))
                || (original.source().target.engine() == Engine::Mssql
                    && mssql_setting_matches(&finding.subject.name, setting)))
        })
        .map(|(position, _)| original.reference(position))
        .collect()
}
fn mssql_setting_matches(subject: &str, setting: &SettingObservation) -> bool {
    match subject {
        "query-store" => {
            setting.namespace == "sys.database_query_store_options"
                && matches!(setting.name.as_str(), "actual_state_desc" | "readonly_reason")
        }
        "query-store-size" => {
            setting.namespace == "sys.database_query_store_options"
                && matches!(
                    setting.name.as_str(),
                    "actual_state_desc"
                        | "current_storage_size_mb"
                        | "max_storage_size_mb"
                        | "size_based_cleanup_mode_desc"
                )
        }
        "query-store-capture" => {
            setting.namespace == "sys.database_query_store_options"
                && matches!(setting.name.as_str(), "actual_state_desc" | "query_capture_mode_desc")
        }
        "query-store-waits" => {
            setting.namespace == "sys.database_query_store_options"
                && matches!(setting.name.as_str(), "actual_state_desc" | "wait_stats_capture_mode_desc")
        }
        "read-committed-snapshot" => {
            setting.namespace == "sys.databases"
                && matches!(setting.name.as_str(), "is_read_committed_snapshot_on" | "snapshot_isolation_state_desc")
        }
        "auto_create_statistics" => setting.namespace == "sys.databases" && setting.name == "is_auto_create_stats_on",
        "auto_update_statistics" => setting.namespace == "sys.databases" && setting.name == "is_auto_update_stats_on",
        "compatibility-level" => setting.namespace == "sys.databases" && setting.name == "compatibility_level",
        "cost-threshold-for-parallelism" => {
            setting.namespace == "sys.configurations" && setting.name == "cost threshold for parallelism"
        }
        "legacy-cardinality-estimation" => {
            setting.namespace == "sys.database_scoped_configurations" && setting.name == "LEGACY_CARDINALITY_ESTIMATION"
        }
        "maxdop" => {
            (setting.namespace == "sys.configurations" && setting.name == "max degree of parallelism")
                || (setting.namespace == "sys.database_scoped_configurations" && setting.name == "MAXDOP")
        }
        _ => false,
    }
}
