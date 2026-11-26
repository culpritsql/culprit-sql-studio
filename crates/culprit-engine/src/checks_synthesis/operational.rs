//! Warehouse synthesis for database-scoped Operational, PlanCacheSummary, and Cleanup captures.
use super::*;
use crate::collected_evidence::catalogs::CatalogContext;
use culprit_evidence::original_projection::OriginalProjection;

pub(super) fn supports(original: &dyn OriginalProjection) -> bool {
    crate::collected_evidence::supports_operational(original)
}

pub(super) fn findings(
    primary: &dyn OriginalProjection,
    description: &DerivedDiagnosisDescription,
    pipeline: &culprit_checks::Pipeline,
    catalogs: &CatalogContext,
    budget: &mut OutputBudget,
) -> Result<queries::FindingProjection> {
    let engine = primary.source().target.engine();
    let analysis = crate::collected_evidence::analyze_operational_original(primary, catalogs, pipeline)?;
    let mut evaluations = evaluation::Evaluations::new();
    evaluations.add_operational(&analysis, 0, catalogs.inputs(), engine)?;
    let database = primary.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
    let mut findings = finding_rows::FindingRows::new();
    for finding in &analysis.report.findings {
        let inputs = references(primary, finding)?;
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
        findings.admit(finding, derived, engine, budget)?;
    }
    Ok(queries::FindingProjection { findings: findings.finish(), evaluations: evaluations.finish() })
}

fn references(original: &dyn OriginalProjection, finding: &Finding) -> Result<Vec<DiagnosticEvidenceRef>> {
    let mut refs = Vec::new();
    for (position, row) in original.records().iter().enumerate() {
        let matches = match (&row.row().record, finding.check.as_str()) {
            (TelemetryRecord::Operational(obs), "storage-capacity" | "log-reuse-blocked") => {
                matches!(obs.fact, OperationalFact::Space(_))
            }
            (TelemetryRecord::Operational(obs), "query-family") => obs.source == source_id::PLAN_CACHE_USE,
            (TelemetryRecord::Operational(obs), "held-cleanup") => {
                matches!(obs.fact, OperationalFact::MssqlCleanupHorizon(_))
            }
            (TelemetryRecord::Operational(obs), "integrity-check-overdue") => {
                matches!(obs.fact, OperationalFact::DatabaseProperties(_))
            }
            (TelemetryRecord::Operational(obs), "backup-retention-short") => {
                matches!(obs.fact, OperationalFact::MssqlBackupHistory(_))
            }
            (TelemetryRecord::Operational(obs), "sequence-capacity") => {
                obs.source == source_id::SEQUENCE_CAPACITY
                    || matches!(&obs.fact, OperationalFact::CatalogCounters(c)
                        if matches!(c.catalog.object_type.as_str(), "sequence" | "auto-increment"))
            }
            (TelemetryRecord::Operational(obs), "operational-errors") => {
                matches!(obs.fact, OperationalFact::Counter(_))
            }
            (TelemetryRecord::WaitCounter(obs), "sync-stats-refresh") => {
                obs.wait_type == "WAIT_ON_SYNC_STATISTICS_REFRESH"
            }
            (TelemetryRecord::Deadlock(_), "deadlocks") => true,
            _ => false,
        };
        if matches {
            refs.push(original.reference(position)?);
            if refs.len() >= MAX_DERIVED_REFERENCES {
                break;
            }
        }
    }
    Ok(refs)
}
