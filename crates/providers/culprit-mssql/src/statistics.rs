//! The same native statistics rows feed app maintenance checks and background observations.
use crate::{
    read::{
        NativeRead, Parameter, ReadError, boolean, integer, invalid_data, optional_counter, optional_integer,
        optional_text, text,
    },
    value::Rec,
};
use culprit_core::{Result, StatisticsInfo, telemetry::*};

const APPLICATION_STATISTICS: usize = 10_000;
pub(crate) const CATALOG_CHANGED: &str = "culprit_statistics_catalog_changed";
const TABLE_METADATA: &str =
    "SELECT t.object_id,t.create_date,t.name AS table_name,s.name AS schema_name,t.is_memory_optimized
 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE t.is_ms_shipped=0";

pub(crate) fn select_sql() -> String {
    let display_updated = crate::sql_time::utc_local_iso("sp.last_updated");
    format!(
        r#"
SELECT * INTO #culprit_stat_tables FROM ({TABLE_METADATA}) visible OPTION(MAXDOP 1);
CREATE UNIQUE CLUSTERED INDEX object_id ON #culprit_stat_tables(object_id);

SELECT TOP (@P1) DB_ID() AS database_id,t.object_id,
 CONVERT(varchar(40),t.create_date,126) AS created_without_offset,
 t.schema_name,t.table_name,st.stats_id,st.name AS stats_name,
 st.auto_created,st.user_created,st.no_recompute,t.is_memory_optimized,
 CASE WHEN sp.stats_id IS NULL THEN 0 ELSE 1 END AS properties_available,
 CONVERT(varchar(40),sp.last_updated,126) AS last_updated_without_offset,
 {display_updated} AS display_updated_at,
 sp.rows,sp.rows_sampled,sp.steps,sp.unfiltered_rows,sp.modification_counter
FROM #culprit_stat_tables t JOIN sys.stats st ON st.object_id=t.object_id
OUTER APPLY sys.dm_db_stats_properties(st.object_id,st.stats_id) sp
ORDER BY t.object_id,st.stats_id OPTION(MAXDOP 1);

IF EXISTS(SELECT 1 FROM #culprit_stat_tables staged FULL JOIN ({TABLE_METADATA}) live ON live.object_id=staged.object_id
 WHERE live.object_id IS NULL OR staged.object_id IS NULL OR live.create_date<>staged.create_date
 OR CONVERT(varbinary(256),live.table_name)<>CONVERT(varbinary(256),staged.table_name)
 OR CONVERT(varbinary(256),live.schema_name)<>CONVERT(varbinary(256),staged.schema_name)
 OR live.is_memory_optimized<>staged.is_memory_optimized)
 THROW 51000,N'{CATALOG_CHANGED}',1;
DROP TABLE #culprit_stat_tables;
"#
    )
}
pub(crate) fn decode(row: &Rec) -> Result<MssqlStatisticsSnapshot> {
    let narrow = |field| integer(row, field)?.try_into().map_err(|_| invalid_data());
    let value = MssqlStatisticsSnapshot {
        table: MssqlObjectIdentity {
            database_id: narrow("database_id")?,
            object_id: narrow("object_id")?,
            created_without_offset: text(row, "created_without_offset")?,
        },
        schema: text(row, "schema_name")?,
        table_name: text(row, "table_name")?,
        statistics_id: narrow("stats_id")?,
        name: text(row, "stats_name")?,
        auto_created: boolean(row, "auto_created")?,
        user_created: boolean(row, "user_created")?,
        no_recompute: boolean(row, "no_recompute")?,
        memory_optimized: boolean(row, "is_memory_optimized")?,
        properties_available: boolean(row, "properties_available")?,
        last_updated_without_offset: optional_text(row, "last_updated_without_offset")?,
        rows: optional_counter(row, "rows")?,
        rows_sampled: optional_counter(row, "rows_sampled")?,
        histogram_steps: optional_integer(row, "steps")?
            .map(|value| value.try_into().map_err(|_| invalid_data()))
            .transpose()?,
        unfiltered_rows: optional_counter(row, "unfiltered_rows")?,
        modifications: optional_counter(row, "modification_counter")?,
        modifications_reset: culprit_core::ResetMarker::Unknown,
    };
    if !value.is_valid() {
        return Err(invalid_data());
    }
    Ok(value)
}
pub(crate) struct StatisticsRead {
    pub statistics: Vec<StatisticsInfo>,
    pub properties_complete: bool,
}
pub(crate) async fn application<R: NativeRead>(reader: &mut R) -> Result<StatisticsRead> {
    let rows = reader
        .query(&select_sql(), vec![Parameter::Integer((APPLICATION_STATISTICS + 1) as i64)], APPLICATION_STATISTICS + 1)
        .await
        .map_err(ReadError::into_error)?;
    if rows.len() > APPLICATION_STATISTICS {
        return Err(CollectionStop::RowLimit.error());
    }
    let mut statistics = Vec::with_capacity(rows.len());
    let mut properties_complete = true;
    for row in &rows {
        let native = decode(row)?;
        properties_complete &= native.properties_available;
        statistics.push(native.into_statistics_info(optional_text(row, "display_updated_at")?));
    }
    Ok(StatisticsRead { statistics, properties_complete })
}
#[cfg(test)]
pub(crate) mod tests;
