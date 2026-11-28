use crate::{
    client::{Client, invalid_response},
    identity::Identity,
    queries, sources,
};
use culprit_core::{ColumnInfo, Coverage, Result, Source, TableStat, message};
use serde::Deserialize;

const TABLE_LIMIT: usize = 1024;
const COLUMN_LIMIT: usize = 8192;
#[derive(Deserialize)]
struct Table {
    node: String,
    database: String,
    name: String,
    engine: String,
    total_rows: Option<u64>,
    total_bytes: Option<u64>,
    has_primary_key: u8,
}
#[derive(Deserialize)]
struct Column {
    node: String,
    database: String,
    table: String,
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    position: u32,
    default_kind: String,
}
impl Table {
    fn stat(self, owner: &Identity) -> Result<TableStat> {
        if self.node != owner.node || self.database != owner.database {
            return Err(invalid_response());
        }
        Ok(TableStat {
            schema: self.database,
            name: self.name,
            kind: Some(self.engine),
            rows: self.total_rows.map(|n| n as f64),
            total_bytes: self.total_bytes.map(|n| n as f64),
            has_primary_key: self.has_primary_key != 0,
            data_bytes: None,
            index_bytes: None,
            is_heap: None,
            seq_scans: None,
            seq_rows_read: None,
            index_scans: None,
            inserts: None,
            updates: None,
            deletes: None,
            dead_rows: None,
            modifications_since_analyze: None,
            last_vacuum: None,
            last_analyze: None,
            forwarded_records: None,
            xid_age: None,
            forwarded_fetches: None,
            storage: None,
            mssql_full_text: None,
            mssql_storage_configuration: None,
            mssql_tracked_by_cdc: None,
            mssql_temporal_configuration: None,
            mssql_heap_configuration: None,
            in_row_bytes: None,
            lob_bytes: None,
            overflow_bytes: None,
            partition_of: None,
            counters_since: None,
            definition: None,
        })
    }
}
impl Column {
    fn info(self, owner: &Identity) -> Result<ColumnInfo> {
        if self.node != owner.node || self.database != owner.database {
            return Err(invalid_response());
        }
        let nullable =
            self.data_type.starts_with("Nullable(") || self.data_type.starts_with("LowCardinality(Nullable(");
        Ok(ColumnInfo {
            schema: self.database,
            table: self.table,
            name: self.name,
            data_type: self.data_type,
            nullable,
            default: None,
            generated: matches!(self.default_kind.as_str(), "MATERIALIZED" | "ALIAS"),
            ordinal: self.position,
        })
    }
}
fn bounded<T>(coverage: &mut Coverage, source: &Source, result: Result<Vec<T>>, limit: usize) -> Vec<T> {
    let Some(mut rows) = coverage.read_rows(source, result) else {
        return vec![];
    };
    if rows.len() > limit {
        rows.truncate(limit);
        coverage.partial(source, message!("provider.clickhouse.truncated"));
    }
    for &capability in source.delivers {
        coverage.delivered(capability);
    }
    rows
}
pub async fn read(client: &Client, owner: &Identity, coverage: &mut Coverage) -> (Vec<TableStat>, Vec<ColumnInfo>) {
    let tables = client
        .rows::<Table>(queries::TABLES, vec![("row_limit", (TABLE_LIMIT + 1).to_string())])
        .await
        .and_then(|rows| rows.into_iter().map(|r| r.stat(owner)).collect());
    let tables = bounded(coverage, &sources::TABLES, tables, TABLE_LIMIT);
    let columns = client
        .rows::<Column>(queries::COLUMNS, vec![("row_limit", (COLUMN_LIMIT + 1).to_string())])
        .await
        .and_then(|rows| rows.into_iter().map(|r| r.info(owner)).collect());
    (tables, bounded(coverage, &sources::COLUMNS, columns, COLUMN_LIMIT))
}
