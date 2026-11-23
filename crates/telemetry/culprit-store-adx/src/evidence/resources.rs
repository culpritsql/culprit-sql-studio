//! Bounded Kusto search of existing publication rows. Labels come from authenticated original metadata later.
use super::*;
use crate::client::QueryRequest;
use culprit_core::telemetry::DeliveryProblem;
use culprit_evidence::analytical::evidence::PublishedResourceClock;
use serde_json::json;
use std::collections::BTreeMap;

pub(super) async fn read(
    store: &AdxReadStore,
    contract: &Contract,
    request: &PublishedResourceQuery,
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
    let database = if let Some(database) = &request.database {
        declarations.push("pDatabase:string");
        parameters.insert("pDatabase".into(), json!(database.key()));
        "| where DatabaseSelector == pDatabase\n"
    } else {
        ""
    };
    let content = if let Some(kind) = request.content.observation_kind() {
        declarations.push("pObservation:string");
        parameters.insert("pObservation".into(), json!(kind));
        "| where tostring(SourceDescription.observation.kind) == pObservation\n"
    } else {
        ""
    };
    let window = if let Some(window) = &request.selection.window {
        declarations.extend(["pFrom:datetime", "pUntil:datetime"]);
        parameters.extend([("pFrom".into(), json!(window.from)), ("pUntil".into(), json!(window.until))]);
        match request.clock {
            PublishedResourceClock::Acquisition => "| where CollectedFrom < pUntil and CollectedUntil >= pFrom\n",
            PublishedResourceClock::NativeWindowCandidates => {
                "| where isnull(IntervalFrom) or isnull(IntervalUntil) or (IntervalFrom <= pUntil and IntervalUntil >= pFrom)\n"
            }
        }
    } else {
        ""
    };
    let after = if let Some(cursor) = &request.selection.after {
        declarations.extend(["pAfterResource:string", "pAfterSource:string", "pAfterRange:string"]);
        parameters.extend([
            ("pAfterResource".into(), json!(cursor.position.resource_id)),
            ("pAfterSource".into(), json!(cursor.position.source_id)),
            ("pAfterRange".into(), json!(cursor.position.range_id)),
        ]);
        "| where ResourceId > pAfterResource or (ResourceId == pAfterResource and SourceId > pAfterSource) or (ResourceId == pAfterResource and SourceId == pAfterSource and RangeId > pAfterRange)\n"
    } else {
        ""
    };
    let columns = schema.columns().iter().map(|column| column.name.as_str()).collect::<Vec<_>>().join(", ");
    let text = format!(
        "declare query_parameters({});\n{}\n| where ContractId == pContract and SchemaDigest == pSchema and RecoveryStoreId == pRecovery and ResourceKind == 'database'\n| where CollectedUntil <= pAsOf\n{database}{content}{window}{after}| project {}\n| distinct {columns}\n| order by ResourceId asc, SourceId asc, RangeId asc\n| take {}",
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
