//! Warehouse synthesis for standalone database catalog, index, constraint, and statistics captures.
use super::*;
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
        let inputs = references(originals, &analysis.admitted_inputs, finding, default_schema)?;
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
    admitted_inputs: &[u16],
    finding: &Finding,
    default_schema: &str,
) -> Result<Vec<DiagnosticEvidenceRef>> {
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();
    for &input_idx in admitted_inputs {
        let Some(&original) = originals.get(usize::from(input_idx)) else { continue };
        for (position, row) in original.records().iter().enumerate() {
            if row.row().record_id.is_none() {
                continue;
            }
            if !record_matches_finding(&row.row().record, finding, default_schema) {
                continue;
            }
            let reference = original.reference(position)?;
            if seen.insert(reference.key()?) {
                refs.push(reference);
                if refs.len() >= MAX_DERIVED_REFERENCES {
                    return Ok(refs);
                }
            }
        }
    }
    Ok(refs)
}

fn record_matches_finding(record: &TelemetryRecord, finding: &Finding, default_schema: &str) -> bool {
    let Some((schema, table, child)) = record_subject_parts(record, default_schema) else {
        return false;
    };
    let qualified_table = format!("{schema}.{table}");
    let subject = &finding.subject.name;
    if subject.eq_ignore_ascii_case(&qualified_table) || subject.eq_ignore_ascii_case(&table) {
        return true;
    }
    if let Some(child_name) = child {
        let qualified_child = format!("{qualified_table}.{child_name}");
        if subject.eq_ignore_ascii_case(&qualified_child)
            || subject.eq_ignore_ascii_case(&child_name)
            || finding.id.ends_with(&format!(":{qualified_child}"))
            || finding.id.ends_with(&format!(":{qualified_table}.{child_name}"))
        {
            return true;
        }
    }
    // Index findings often name `schema.table (index)` or `schema.table.index` in `subject.name` or `finding.id`.
    let prefix_dot = format!("{}.", qualified_table.to_ascii_lowercase());
    let prefix_space = format!("{} ", qualified_table.to_ascii_lowercase());
    let subject_lower = subject.to_ascii_lowercase();
    subject_lower.starts_with(&prefix_dot) || subject_lower.starts_with(&prefix_space)
}

fn record_subject_parts(record: &TelemetryRecord, default_schema: &str) -> Option<(String, String, Option<String>)> {
    match record {
        TelemetryRecord::Operational(obs) => match &obs.fact {
            OperationalFact::MssqlTable(t) => Some((t.schema.clone(), t.name.clone(), None)),
            OperationalFact::MssqlIndex(i) => {
                Some((i.parent_schema.clone(), i.parent_name.clone(), Some(i.name.clone())))
            }
            OperationalFact::MssqlStatistics(s) => Some((s.schema.clone(), s.table_name.clone(), Some(s.name.clone()))),
            OperationalFact::CounterSet(set) => {
                let schema = set.properties.get("schema_name")?.clone();
                let table = set.properties.get("table_name")?.clone();
                Some((schema, table, None))
            }
            OperationalFact::CatalogCounters(c) => {
                let props = &c.catalog.properties;
                if let Some(table) = props.get("table_name") {
                    let schema = props.get("schema_name").cloned().unwrap_or_else(|| default_schema.to_owned());
                    let child = props.get("index_name").or_else(|| props.get("column_name")).cloned();
                    Some((schema, table.clone(), child))
                } else {
                    let name = props.get("name")?.clone();
                    let schema = props.get("schema_name").cloned().unwrap_or_else(|| default_schema.to_owned());
                    Some((schema, name, None))
                }
            }
            _ => None,
        },
        TelemetryRecord::ObjectDefinition(def) => match &def.fact {
            ObjectDefinitionFact::MssqlColumn(c) => {
                Some((c.schema.clone(), c.table_name.clone(), Some(c.name.clone())))
            }
            ObjectDefinitionFact::MssqlForeignKey(fk) => {
                Some((fk.parent_schema.clone(), fk.parent_table.clone(), Some(fk.name.clone())))
            }
            ObjectDefinitionFact::MssqlCheckConstraint(ck) => {
                Some((ck.parent_schema.clone(), ck.parent_table.clone(), Some(ck.name.clone())))
            }
            ObjectDefinitionFact::MssqlDmlTrigger(tr) => {
                Some((tr.parent_schema.clone(), tr.parent_name.clone(), Some(tr.name.clone())))
            }
            ObjectDefinitionFact::MssqlPartitionBoundary(pb) => Some((pb.schema.clone(), pb.table_name.clone(), None)),
            ObjectDefinitionFact::NativeCatalog(nc) => {
                let props = &nc.properties;
                let schema = props.get("schema_name").cloned().unwrap_or_else(|| default_schema.to_owned());
                let table = props.get("table_name").or_else(|| props.get("name"))?.clone();
                let child = props.get("column_name").or_else(|| props.get("name")).cloned();
                Some((schema, table, child))
            }
            _ => None,
        },
        _ => None,
    }
}
