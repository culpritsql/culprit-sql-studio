//! Bounded Kusto per-resource head discovery. Candidate hints are authenticated against committed manifests later.
use super::*;
use crate::client::QueryRequest;
use culprit_core::telemetry::DeliveryProblem;
use culprit_evidence::analytical::evidence::FleetResourceQuery;
use serde_json::json;
use std::collections::BTreeMap;

pub(super) async fn read(
    store: &AdxReadStore,
    contract: &Contract,
    request: &FleetResourceQuery,
    byte_limit: usize,
) -> Result<AnalyticalReadPage> {
    request.validate(contract)?;
    let schema = store.catalog.publication();
    let mut parameters = BTreeMap::from([
        ("pContract".into(), json!(contract.id()?)),
        ("pSchema".into(), json!(contract.schema_digest())),
        ("pRecovery".into(), json!(request.recovery_store_id)),
        ("pAsOf".into(), json!(request.selection.as_of)),
    ]);
    let mut declarations = vec!["pContract:string", "pSchema:string", "pRecovery:string", "pAsOf:datetime"];
    let window = if let Some(window) = &request.selection.window {
        declarations.extend(["pFrom:datetime", "pUntil:datetime"]);
        parameters.extend([("pFrom".into(), json!(window.from)), ("pUntil".into(), json!(window.until))]);
        "| where CollectedFrom < pUntil and CollectedUntil >= pFrom\n"
    } else {
        ""
    };
    let after = if let Some(cursor) = &request.selection.after {
        declarations.push("pAfterResource:string");
        parameters.insert("pAfterResource".into(), json!(cursor.resource_id));
        "| where ResourceId > pAfterResource\n"
    } else {
        ""
    };
    let text = format!(
        "declare query_parameters({});\n{}\n| where ContractId == pContract and SchemaDigest == pSchema and RecoveryStoreId == pRecovery and ResourceKind == 'database'\n| where CollectedUntil <= pAsOf\n{window}{after}| extend CulpritIsDerived = iff(tostring(SourceDescription.observation.kind) == 'derived-diagnosis', 1, 0)\n| order by ResourceId asc, CulpritIsDerived desc, CollectedUntil desc, SourceId desc, RangeId desc\n| extend CulpritFleetRow = row_number()\n| extend CulpritFleetStart = CulpritFleetRow == 1 or ResourceId != prev(ResourceId)\n| extend CulpritFleetOrd = row_number(1, CulpritFleetStart)\n| where CulpritFleetOrd == 1\n| project {}\n| take {}",
        declarations.join(","),
        schema.table(),
        schema.verification_projection(),
        request.selection.page_rows + 1,
    );
    let table = store
        .query
        .query(QueryRequest {
            text,
            parameters,
            columns: schema.verification_columns(),
            row_limit: request.selection.page_rows as usize + 1,
            byte_limit,
        })
        .await?;
    if table.columns != schema.verification_columns() {
        return Err(DeliveryProblem::Integrity.error());
    }
    Ok(AnalyticalReadPage { rows: table.rows, response_bytes: table.response_bytes })
}
