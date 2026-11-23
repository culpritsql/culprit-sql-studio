//! Tables, indexes, columns, foreign keys and configuration. No workload planning or scan orchestration.
use crate::read::QueryRead;
use crate::{
    probe::Probe,
    read::{query, query_one},
};
use culprit_core::{ColumnInfo, ForeignKey, IndexStat, Result, Setting, TableStat};

pub(crate) async fn tables(client: &mut impl QueryRead, version_num: u32) -> Result<Vec<TableStat>> {
    let sql = crate::native::relations::table_query;
    let rows = match query(client, &sql(version_num, crate::native::relations::SizeRead::Physical, None)).await {
        Ok(rows) => rows,
        Err(error) if error.code == culprit_core::ErrorCode::Timeout => {
            query(client, &sql(version_num, crate::native::relations::SizeRead::CatalogEstimate, None)).await?
        }
        Err(error) => return Err(error),
    };
    rows.iter()
        .map(|row| crate::native::relations::table(row, &row.string("database_oid")))
        .map(|result| result.map(|table| crate::native::relations::project(&table)))
        .collect()
}

pub(crate) async fn indexes(client: &mut impl QueryRead, probe: &Probe) -> Result<Vec<IndexStat>> {
    let sql = crate::native::indexes::query(probe.version_num, probe.timescale_version.is_some(), true, None);
    let rows = query(client, &sql).await?;
    let mut indexes = rows
        .into_iter()
        .map(|row| crate::native::indexes::decode(row, None))
        .map(|result| result.map(|reading| crate::native::indexes::project(&reading)))
        .collect::<Result<Vec<_>>>()?;

    // Exact sizes take a lock per index; fall back to the page-count estimate if any is held up.
    let sizes = query(
        client,
        "SELECT n.nspname AS schema_name, i.relname AS index_name, pg_relation_size(x.indexrelid) AS bytes
         FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_namespace n ON n.oid = i.relnamespace
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_toast%'",
    )
    .await;
    if let Ok(sizes) = sizes {
        for row in sizes {
            let (schema, name) = (row.string("schema_name"), row.string("index_name"));
            if let Some(index) = indexes.iter_mut().find(|i| i.schema == schema && i.name == name) {
                index.size_bytes = row.number("bytes").or(index.size_bytes);
            }
        }
    }
    Ok(indexes)
}

pub(crate) async fn columns(client: &mut impl QueryRead, version: u32) -> Result<Vec<ColumnInfo>> {
    let kind = crate::native::catalog::Kind::Columns;
    query(client, &crate::native::catalog::query(kind, version, true, 100_001))
        .await?
        .into_iter()
        .map(|row| {
            crate::native::catalog::decode(
                kind,
                row,
                None,
                culprit_core::telemetry::MAX_CATALOG_EXPRESSION_BYTES,
                culprit_core::Engine::Postgres,
            )
        })
        .map(|result| result.and_then(crate::native::catalog::column))
        .collect()
}
pub(crate) async fn foreign_keys(client: &mut impl QueryRead) -> Result<Vec<ForeignKey>> {
    let kind = crate::native::catalog::Kind::ForeignKeys;
    query(client, &crate::native::catalog::query(kind, 110_000, false, 100_001))
        .await?
        .into_iter()
        .map(|row| {
            crate::native::catalog::decode(
                kind,
                row,
                None,
                culprit_core::telemetry::MAX_CATALOG_EXPRESSION_BYTES,
                culprit_core::Engine::Postgres,
            )
        })
        .map(|result| result.and_then(crate::native::catalog::foreign_key))
        .collect()
}

pub(crate) async fn settings(
    client: &mut impl QueryRead,
    host: &str,
    port: u16,
) -> Result<crate::native::settings::CollectedSettings> {
    let mut collected = crate::native::settings::scan(client, host, port).await?;
    if collected.failure.is_some() {
        return Ok(collected);
    }
    match query_one(client, "SELECT age(datfrozenxid) AS age FROM pg_database WHERE datname = current_database()").await
    {
        Ok(Some(row)) => collected.values.push(Setting {
            name: "datfrozenxid_age".into(),
            value: row.string("age"),
            default_value: None,
            scope: "database".into(),
        }),
        Ok(None) => {}
        Err(error) => collected.failure = Some(error),
    }
    Ok(collected)
}
