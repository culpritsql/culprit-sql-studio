use super::*;
use culprit_core::collected_analysis::CollectedQueryAnalysisResult;
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct FindingProjection {
    pub findings: Vec<DerivedFinding>,
    pub evaluations: DerivedFindingEvaluations,
}

pub(super) fn findings(
    original: &dyn OriginalProjection,
    description: &DerivedDiagnosisDescription,
    pipeline: &culprit_checks::Pipeline,
    catalogs: &crate::collected_evidence::catalogs::CatalogContext,
    budget: &mut OutputBudget,
) -> Result<FindingProjection> {
    let analyses = crate::collected_evidence::analyze_original_with_catalogs(
        original,
        &original.original().key()?,
        pipeline,
        catalogs,
    )?;
    let database = original.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
    let mut findings: Vec<DerivedFinding> = Vec::new();
    let mut identities = BTreeMap::<&str, (&Finding, usize)>::new();
    let mut evaluations = evaluation::Evaluations::new();
    for analysis in &analyses {
        evaluations.add(analysis, 0, catalogs.inputs(), original.source().target.engine())?;
        for finding in &analysis.findings {
            let refs = finding_references(original, analysis, finding)?;
            let query = finding_query(original, analysis, finding)?;
            let mut derived = DerivedFinding {
                analysis_id: description.analysis_id.clone(),
                database: database.clone(),
                display: description.display.clone(),
                finding: finding.clone(),
                inputs: refs,
                query,
                cause: None,
                guidance: DerivedGuidance::HistoricalRequiresFreshVerification,
            };
            derived.redact(original.source().target.engine());
            // One catalog fact can recur in independent workload assessments. Coalesce only identical
            // source and projected findings; a shared ID never licenses merging populations or causes.
            if let Some((previous, position)) = identities.get(finding.id.as_str()) {
                if *previous != finding || findings[*position] != derived {
                    return Err(invalid());
                }
                continue;
            }
            budget.admit(&derived)?;
            identities.insert(&finding.id, (finding, findings.len()));
            findings.push(derived);
        }
    }
    Ok(FindingProjection { findings, evaluations: evaluations.finish() })
}
fn finding_query(
    original: &dyn OriginalProjection,
    analysis: &CollectedQueryAnalysisResult,
    finding: &Finding,
) -> Result<Option<DerivedQuery>> {
    let ids = finding.queries.iter().map(|query| query.id.as_str()).collect::<BTreeSet<_>>();
    let inputs = analysis.query_inputs.iter().filter(|input| ids.contains(input.query_id.as_str())).collect::<Vec<_>>();
    let [input] = inputs.as_slice() else { return Ok(None) };
    let Some(definition) = crate::collected_evidence::original_query(original, &input.references)? else {
        return Ok(None);
    };
    let position = original
        .records()
        .iter()
        .position(|row| {
            matches!(&row.row().record,
        TelemetryRecord::QueryDefinition(query) if query.id==definition.id)
        })
        .ok_or_else(invalid)?;
    Ok(Some(DerivedQuery {
        identity: definition.identity.clone(),
        reference: original.reference(position)?,
        sql: definition.sql.clone(),
    }))
}

fn finding_references(
    original: &dyn OriginalProjection,
    analysis: &CollectedQueryAnalysisResult,
    finding: &Finding,
) -> Result<Vec<DiagnosticEvidenceRef>> {
    let ids = finding.queries.iter().map(|query| query.id.as_str()).collect::<BTreeSet<_>>();
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let inputs = analysis.query_inputs.iter().filter(|input| ids.contains(input.query_id.as_str())).collect::<Vec<_>>();
    let mut references = Vec::new();
    let mut keys = BTreeSet::new();
    for (position, row) in original.records().iter().enumerate() {
        let Some(key) = row.row().record.measurement_key() else { continue };
        if !inputs.iter().any(|input| input.measurements.iter().any(|measurement| measurement == key)) {
            continue;
        }
        // Unresolved definitions have no record_id. Capture authority remains explicit; no dummy row substitutes.
        if row.row().record_id.is_none() {
            continue;
        }
        let reference = original.reference(position)?;
        if keys.insert(reference.key()?) {
            references.push(reference)
        }
        if references.len() > MAX_DERIVED_REFERENCES {
            return Err(capacity());
        }
    }
    Ok(references)
}

pub(super) fn from_record(original: &dyn OriginalProjection, position: usize) -> Result<Option<DerivedIncidentQuery>> {
    let record = original.records().get(position).ok_or_else(invalid)?;
    let reference = original.reference(position)?;
    match &record.row().record {
        TelemetryRecord::Activity(observation) => {
            let rows = original
                .records()
                .iter()
                .filter_map(|row| match &row.row().record {
                    TelemetryRecord::Activity(observation) => Some(ActivityFrameRow { observation, reference: None }),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let frame = ActivityFrameEvidence::new(original.source(), &rows, original.is_read_complete())?;
            if !frame.has_session_generation_authority(&observation.key.session) {
                return Ok(None);
            }
            Ok(Some(DerivedIncidentQuery {
                causal_role: None,
                identity: Some(DerivedQueryIdentity::Activity {
                    actor: observation.key.clone(),
                    statement: observation.statement_identity.clone(),
                }),
                sql: observation.statement.clone(),
                caller: DerivedCaller::from_activity(observation, &reference)?,
                evidence: vec![reference],
            }))
        }
        TelemetryRecord::QueryRuntime(runtime) => {
            let Some(query) = &runtime.key.references.query else { return Ok(None) };
            let native =
                NativeQueryIdentity { query_id: query.native.id.clone(), qualifiers: query.native.qualifiers.clone() };
            let plan = runtime.key.references.plan.as_ref().map(|plan| NativePlanIdentity {
                plan_id: plan.native.id.clone(),
                query: native.clone(),
                qualifiers: plan.native.qualifiers.clone(),
            });
            let definition = crate::collected_evidence::original_query(original, &runtime.key.references)?;
            let sql = definition
                .map(|definition| definition.sql.clone())
                .unwrap_or(DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected });
            Ok(Some(DerivedIncidentQuery {
                causal_role: None,
                identity: Some(DerivedQueryIdentity::QueryHistory { query: native, plan }),
                sql,
                caller: None,
                evidence: vec![reference],
            }))
        }
        _ => Ok(None),
    }
}

/// The family interpreter must first qualify this exact native request. Its labels remain DB-reported
/// context, and its current statement acquires no allocating, blocking or application-owner role here.
pub(super) fn from_qualified_request(
    request: &ActivityObservation,
    reference: DiagnosticEvidenceRef,
) -> DerivedIncidentQuery {
    DerivedIncidentQuery {
        causal_role: None,
        identity: Some(DerivedQueryIdentity::Activity {
            actor: request.key.clone(),
            statement: request.statement_identity.clone(),
        }),
        sql: request.statement.clone(),
        caller: qualified_caller(request, &reference),
        evidence: vec![reference],
    }
}

pub(super) fn qualified_caller(
    request: &ActivityObservation,
    evidence: &DiagnosticEvidenceRef,
) -> Option<DerivedCaller> {
    (request.client.is_some() || request.session_context.is_some()).then(|| DerivedCaller {
        client: request.client.clone(),
        session_context: request.session_context.clone(),
        evidence: evidence.clone(),
    })
}
