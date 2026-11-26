//! Warehouse synthesis for standalone database catalog, index, constraint, sequence, and statistics captures.
use super::*;
use crate::collected_evidence::CatalogProvenance;
use culprit_core::CatalogObjectRef;
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::BTreeSet;

pub(super) fn supports(original: &dyn OriginalProjection) -> bool {
    crate::collected_evidence::supports_catalog_findings(original)
}

pub(super) fn analyze(
    primary: &dyn OriginalProjection,
    originals: &[&dyn OriginalProjection],
    pipeline: &culprit_checks::Pipeline,
) -> Result<crate::collected_evidence::RetainedCatalogAnalysis> {
    crate::collected_evidence::analyze_catalog_originals(primary, originals, pipeline)
}

pub(super) fn project(
    primary: &dyn OriginalProjection,
    originals: &[&dyn OriginalProjection],
    analysis: crate::collected_evidence::RetainedCatalogAnalysis,
    description: &DerivedDiagnosisDescription,
    budget: &mut OutputBudget,
) -> Result<queries::FindingProjection> {
    let engine = primary.source().target.engine();
    let default_schema = primary.source().target.database().map(|db| db.database.as_str()).unwrap_or_default();
    let database = primary.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
    let mut evaluations = evaluation::Evaluations::new();
    evaluations.add_catalog(&analysis, 0, &analysis.companion_inputs, engine)?;

    let mut findings = finding_rows::FindingRows::new();
    for finding in &analysis.report.findings {
        let inputs = references(originals, &analysis.provenance, finding, default_schema)?;
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

fn references(
    originals: &[&dyn OriginalProjection],
    provenance: &CatalogProvenance,
    finding: &Finding,
    default_schema: &str,
) -> Result<Vec<DiagnosticEvidenceRef>> {
    let mut positions: Vec<(u16, usize)> = Vec::new();
    let add_positions = |positions: &mut Vec<(u16, usize)>, key: &CatalogObjectRef| {
        if let Some(found) = provenance.get(key) {
            for &entry in found {
                if !positions.contains(&entry) {
                    positions.push(entry);
                }
            }
        }
    };

    if !finding.facts.catalog_refs.is_empty() {
        for key in &finding.facts.catalog_refs {
            add_positions(&mut positions, key);
        }
    } else {
        match finding.subject.kind {
            culprit_core::SubjectKind::Index => {
                if let Some((schema, table, index)) = split_index_subject(finding, default_schema) {
                    add_positions(
                        &mut positions,
                        &CatalogObjectRef::Index { schema: schema.clone(), table: table.clone(), index },
                    );
                    add_positions(&mut positions, &CatalogObjectRef::Table { schema, table });
                }
            }
            culprit_core::SubjectKind::Table => {
                if let Some((schema, table)) = resolve_table_subject(provenance, &finding.subject.name, default_schema)
                {
                    add_positions(
                        &mut positions,
                        &CatalogObjectRef::Table { schema: schema.clone(), table: table.clone() },
                    );
                    if finding.category == culprit_core::Category::Indexes {
                        for (key, rows) in provenance {
                            if matches!(key, CatalogObjectRef::Index { schema: s, table: t, .. } if s.as_str() == schema.as_str() && t.as_str() == table.as_str())
                            {
                                for &entry in rows {
                                    if !positions.contains(&entry) {
                                        positions.push(entry);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            culprit_core::SubjectKind::Database => {
                add_positions(&mut positions, &CatalogObjectRef::Sequence { name: finding.subject.name.clone() });
            }
            culprit_core::SubjectKind::Query | culprit_core::SubjectKind::Setting => {}
        }
    }

    // Preserve deterministic input/row ordering across companion captures.
    positions.sort_unstable();
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();
    for (input_idx, position) in positions {
        let Some(&original) = originals.get(usize::from(input_idx)) else { continue };
        let Some(row) = original.records().get(position) else { continue };
        if row.row().record_id.is_none() {
            continue;
        }
        let reference = original.reference(position)?;
        if seen.insert(reference.key()?) {
            refs.push(reference);
            if refs.len() >= MAX_DERIVED_REFERENCES {
                break;
            }
        }
    }
    Ok(refs)
}

fn resolve_table_subject(
    provenance: &CatalogProvenance,
    subject: &str,
    default_schema: &str,
) -> Option<(String, String)> {
    if let Some((schema, table)) = subject.split_once('.') {
        let key = CatalogObjectRef::Table { schema: schema.to_owned(), table: table.to_owned() };
        if provenance.contains_key(&key) {
            return Some((schema.to_owned(), table.to_owned()));
        }
    }
    if !default_schema.is_empty() {
        let key = CatalogObjectRef::Table { schema: default_schema.to_owned(), table: subject.to_owned() };
        if provenance.contains_key(&key) {
            return Some((default_schema.to_owned(), subject.to_owned()));
        }
    }
    for key in provenance.keys() {
        match key {
            CatalogObjectRef::Table { schema, table }
            | CatalogObjectRef::Index { schema, table, .. }
            | CatalogObjectRef::Column { schema, table, .. }
            | CatalogObjectRef::ForeignKey { schema, table, .. }
            | CatalogObjectRef::Constraint { schema, table, .. }
            | CatalogObjectRef::Statistics { schema, table, .. } => {
                if format!("{schema}.{table}") == subject || (schema == default_schema && table == subject) {
                    return Some((schema.clone(), table.clone()));
                }
            }
            CatalogObjectRef::Sequence { .. } => {}
        }
    }
    subject.split_once('.').map(|(s, t)| (s.to_owned(), t.to_owned()))
}

fn split_index_subject(finding: &Finding, default_schema: &str) -> Option<(String, String, String)> {
    if let Some(parent) = &finding.facts.table {
        let prefix = format!("{parent}.");
        if let Some(index) = finding.subject.name.strip_prefix(&prefix)
            && let Some((schema, table)) = parent.split_once('.')
        {
            return Some((schema.to_owned(), table.to_owned(), index.to_owned()));
        }
    }
    let (parent, index) = finding.subject.name.rsplit_once('.')?;
    if let Some((schema, table)) = parent.split_once('.') {
        Some((schema.to_owned(), table.to_owned(), index.to_owned()))
    } else if !default_schema.is_empty() {
        Some((default_schema.to_owned(), parent.to_owned(), index.to_owned()))
    } else {
        None
    }
}
