//! Existing publication schema and parameterized ClickHouse dialect; hints are not native authority.
use super::*;
use culprit_evidence::analytical::evidence::PublishedResourceClock;

pub(super) async fn read(
    store: &ClickHouseReadStore,
    contract: &Contract,
    request: &PublishedResourceQuery,
    byte_limit: usize,
) -> Result<AnalyticalReadPage> {
    request.validate(contract)?;
    let schema = store.catalog.publication();
    let mut parameters = BTreeMap::from([
        ("contract".into(), contract.id()?),
        ("schema".into(), contract.schema_digest().into()),
        ("recovery".into(), request.recovery_store_id.clone()),
        ("as_of".into(), nanos(&request.selection.as_of)?),
    ]);
    let database = if let Some(database) = &request.database {
        parameters.insert("database".into(), database.key().into());
        " AND DatabaseSelector={database:String}"
    } else {
        ""
    };
    let content = if let Some(kind) = request.content.observation_kind() {
        parameters.insert("observation".into(), kind.into());
        " AND JSONExtractString(SourceDescription,'observation','kind')={observation:String}"
    } else {
        ""
    };
    let window = if let Some(window) = &request.selection.window {
        parameters.extend([("from".into(), nanos(&window.from)?), ("until".into(), nanos(&window.until)?)]);
        match request.clock {
            PublishedResourceClock::Acquisition => {
                " AND CollectedFrom<fromUnixTimestamp64Nano({until:Int64}) AND CollectedUntil>=fromUnixTimestamp64Nano({from:Int64})"
            }
            PublishedResourceClock::NativeWindowCandidates => {
                " AND (isNull(IntervalFrom) OR isNull(IntervalUntil) OR (IntervalFrom<=fromUnixTimestamp64Nano({until:Int64}) AND IntervalUntil>=fromUnixTimestamp64Nano({from:Int64})))"
            }
        }
    } else {
        ""
    };
    let after = if let Some(cursor) = &request.selection.after {
        parameters.extend([
            ("after_resource".into(), cursor.position.resource_id.clone()),
            ("after_source".into(), cursor.position.source_id.clone()),
            ("after_range".into(), cursor.position.range_id.clone()),
        ]);
        " AND (ResourceId,SourceId,RangeId)>({after_resource:String},{after_source:String},{after_range:String})"
    } else {
        ""
    };
    let text = format!(
        "SELECT DISTINCT {} FROM {} WHERE ContractId={{contract:String}} AND SchemaDigest={{schema:String}} AND RecoveryStoreId={{recovery:String}} AND ResourceKind='database' AND CollectedUntil<=fromUnixTimestamp64Nano({{as_of:Int64}}){database}{content}{window}{after} ORDER BY ResourceId,SourceId,RangeId LIMIT {}",
        schema.verification_projection(),
        quote(schema.table()),
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
    Ok(AnalyticalReadPage { rows: table.rows, response_bytes: table.response_bytes })
}
fn nanos(value: &str) -> Result<String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|at| at.timestamp_nanos_opt())
        .map(|value| value.to_string())
        .ok_or_else(invalid)
}
