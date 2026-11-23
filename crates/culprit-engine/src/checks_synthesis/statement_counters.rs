//! Counter-only originals retain an explicit application-analysis limitation instead of a healthy empty result.
//! When qualified same-owner SQL definitions accompany a guarded PostgreSQL counter roster, the shared pipeline
//! evaluates ordinary performance checks without inventing intervals, origins or executable schema SQL.
use super::*;
use crate::collected_evidence::{
    catalogs::CatalogContext,
    statement_counters::{StatementCounterOutcome, StatementFindingEvidence, analyze_statement_counters},
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct StatementCounterProjection {
    pub findings: Vec<DerivedFinding>,
    pub evaluations: DerivedFindingEvaluations,
    pub evaluated: bool,
}

pub(super) fn supports(original: &dyn OriginalProjection) -> bool {
    original.source().observation.snapshot().is_some_and(|header| header.dataset == SnapshotDataset::StatementCounters)
}

pub(super) fn project(
    primary: &dyn OriginalProjection,
    originals: &[&dyn OriginalProjection],
    description: &DerivedDiagnosisDescription,
    pipeline: &culprit_checks::Pipeline,
    catalogs: &CatalogContext,
    budget: &mut OutputBudget,
) -> Result<StatementCounterProjection> {
    let engine = primary.source().target.engine();
    let mut evaluations = evaluation::Evaluations::new();
    match analyze_statement_counters(primary, originals, pipeline, catalogs)? {
        StatementCounterOutcome::CounterOnly { limitations } => {
            evaluations.add_statement_counters(limitations, &[], &[], &[], engine)?;
            Ok(StatementCounterProjection { findings: vec![], evaluations: evaluations.finish(), evaluated: false })
        }
        StatementCounterOutcome::Evaluated(evaluated) => {
            let mut reasons = Vec::new();
            for limitation in &evaluated.limitations {
                if !reasons.contains(&limitation.reason) {
                    reasons.push(limitation.reason);
                }
            }
            evaluations.add_statement_counters(
                reasons,
                &evaluated.report.assessments,
                &evaluated.report.locked,
                catalogs.inputs(),
                engine,
            )?;
            let database = primary.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
            let mut findings = finding_rows::FindingRows::new();
            for finding in &evaluated.report.findings {
                let (query, inputs) = finding_evidence(finding, &evaluated.query_evidence)?;
                let derived = DerivedFinding {
                    analysis_id: description.analysis_id.clone(),
                    database: database.clone(),
                    display: description.display.clone(),
                    finding: finding.clone(),
                    inputs,
                    query,
                    cause: None,
                    guidance: DerivedGuidance::HistoricalRequiresFreshVerification,
                };
                findings.admit(finding, derived, engine, budget)?;
            }
            Ok(StatementCounterProjection {
                findings: findings.finish(),
                evaluations: evaluations.finish(),
                evaluated: true,
            })
        }
    }
}

fn finding_evidence(
    finding: &Finding,
    evidence: &BTreeMap<String, StatementFindingEvidence>,
) -> Result<(Option<DerivedQuery>, Vec<DiagnosticEvidenceRef>)> {
    let matched: Vec<&StatementFindingEvidence> =
        finding.queries.iter().filter_map(|query| evidence.get(&query.id)).collect();
    let query = match matched.as_slice() {
        [single] if finding.queries.len() == 1 => Some(DerivedQuery {
            identity: single.identity.clone(),
            reference: single.definition_ref.clone(),
            sql: single.sql.clone(),
        }),
        _ => None,
    };
    let mut inputs = Vec::new();
    let mut seen = BTreeSet::new();
    for reference in matched.iter().flat_map(|item| &item.inputs) {
        if seen.insert(reference.key()?) {
            inputs.push(reference.clone());
        }
        if inputs.len() > MAX_DERIVED_REFERENCES {
            return Err(capacity());
        }
    }
    Ok((query, inputs))
}
