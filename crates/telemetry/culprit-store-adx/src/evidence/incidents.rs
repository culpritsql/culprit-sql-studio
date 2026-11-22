//! Candidate filters never establish original native identity or incident causality.
use super::*;
use crate::client::QueryRequest;
use culprit_core::telemetry::{DeliveryProblem, TelemetryStream};
use culprit_evidence::analytical::evidence::IncidentCandidatePosition;
use culprit_evidence::analytical::evidence::IncidentCandidateSelection;
use serde_json::json;
use std::collections::BTreeMap;

pub(super) async fn read(
    store: &AdxReadStore,
    contract: &Contract,
    request: &IncidentDiscoveryRequest,
    byte_limit: usize,
) -> Result<AnalyticalReadPage> {
    request.validate()?;
    let schema = store.catalog.get(TelemetryStream::DerivedIncidents)?;
    let mut parameters = BTreeMap::from([
        ("pContract".into(), json!(contract.id()?)),
        ("pSchema".into(), json!(contract.schema_digest())),
        ("pDatabase".into(), json!(request.selection.database().key())),
        ("pRecordedBefore".into(), json!(request.recorded_before)),
    ]);
    let mut declarations = vec!["pContract:string", "pSchema:string", "pDatabase:string", "pRecordedBefore:datetime"];
    let selected = match &request.selection {
        IncidentCandidateSelection::Around { from, until, .. } => {
            declarations.extend(["pFrom:datetime", "pUntil:datetime"]);
            parameters.extend([("pFrom".into(), json!(from)), ("pUntil".into(), json!(until))]);
            "((OriginalObservedFrom < pUntil and OriginalObservedUntil >= pFrom) or (RecordedAt >= pFrom and RecordedAt < pUntil) or (OriginalIntervalFrom < pUntil and OriginalIntervalUntil >= pFrom))"
        }
        IncidentCandidateSelection::Episode { episode_id, controller_version, .. } => {
            declarations.extend(["pEpisode:string", "pController:long"]);
            parameters
                .extend([("pEpisode".into(), json!(episode_id)), ("pController".into(), json!(controller_version))]);
            "EpisodeId == pEpisode and ControllerVersion == pController"
        }
    };
    let after = if let Some(cursor) = &request.after {
        declarations.extend([
            "pAfterEpisode:string",
            "pAfterController:long",
            "pAfterDigits:long",
            "pAfterRevision:string",
            "pAfterRange:string",
            "pAfterBatch:string",
            "pAfterOrdinal:long",
        ]);
        cursor_parameters(&mut parameters, &cursor.position);
        format!("| where {}\n", after_predicate())
    } else {
        String::new()
    };
    let columns = schema.columns().iter().map(|column| column.name.as_str()).collect::<Vec<_>>().join(", ");
    let text = format!(
        "declare query_parameters({});\n{}\n| where ContractId == pContract and SchemaDigest == pSchema and HasDatabaseSelector and DatabaseSelector == pDatabase\n| where RecordedAt <= pRecordedBefore and ({selected})\n{after}| project {}\n| distinct {columns}\n| extend RevisionDigits = strlen(Revision)\n| order by EpisodeId asc, ControllerVersion asc, RevisionDigits asc, Revision asc, RangeId asc, BatchId asc, RowOrdinal asc\n| take {}\n| project-away RevisionDigits",
        declarations.join(","),
        schema.table(),
        schema.verification_projection(),
        request.page_rows + 1,
    );
    let table = store
        .query
        .query(QueryRequest {
            text,
            parameters,
            columns: schema.verification_columns(),
            row_limit: request.page_rows as usize + 1,
            byte_limit,
        })
        .await?;
    if table.columns != schema.verification_columns() {
        return Err(DeliveryProblem::Integrity.error());
    }
    Ok(AnalyticalReadPage { rows: table.rows, response_bytes: table.response_bytes })
}
fn cursor_parameters(parameters: &mut BTreeMap<String, Value>, position: &IncidentCandidatePosition) {
    parameters.extend([
        ("pAfterEpisode".into(), json!(position.episode_id)),
        ("pAfterController".into(), json!(position.controller_version)),
        ("pAfterDigits".into(), json!(position.revision.len())),
        ("pAfterRevision".into(), json!(position.revision)),
        ("pAfterRange".into(), json!(position.range_id)),
        ("pAfterBatch".into(), json!(position.batch_id)),
        ("pAfterOrdinal".into(), json!(position.row_ordinal)),
    ]);
}
fn after_predicate() -> String {
    let components = [
        ("EpisodeId", "pAfterEpisode"),
        ("ControllerVersion", "pAfterController"),
        ("strlen(tostring(Revision))", "pAfterDigits"),
        ("tostring(Revision)", "pAfterRevision"),
        ("RangeId", "pAfterRange"),
        ("BatchId", "pAfterBatch"),
        ("RowOrdinal", "pAfterOrdinal"),
    ];
    components
        .iter()
        .enumerate()
        .map(|(index, (field, parameter))| {
            let mut equal = components[..index]
                .iter()
                .map(|(field, parameter)| format!("{field} == {parameter}"))
                .collect::<Vec<_>>();
            equal.push(format!("{field} > {parameter}"));
            format!("({})", equal.join(" and "))
        })
        .collect::<Vec<_>>()
        .join(" or ")
}
