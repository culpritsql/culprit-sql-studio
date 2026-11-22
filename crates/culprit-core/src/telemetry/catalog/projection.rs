//! Pure conversion of already decoded native catalog facts. Scans and retained evidence share one mapping.
use super::*;
use crate::{ColumnInfo, IndexFilterFacts, IndexStat, MeasurementScope, Setting, StatisticsInfo, TableStat};

/// Display SQL and an acquired optional UTC label stay separate from native offset-free facts.
/// Retained callers supply only their known payload; no native local time is converted to UTC here.
pub struct MssqlIndexPresentation {
    pub filter: Option<String>,
    pub filter_facts: Option<IndexFilterFacts>,
    pub last_read: Option<String>,
}
fn display(value: Option<String>) -> Option<f64> {
    value.and_then(|value| value.parse::<u64>().ok()).map(|value| value as f64)
}

impl MssqlTableSnapshot {
    pub fn into_table_stat(self) -> Option<TableStat> {
        let value = self;
        let has_primary_key = value.has_primary_key?;
        Some(TableStat {
            schema: value.schema,
            name: value.name,
            kind: None,
            definition: None,
            rows: display(value.estimated_rows),
            total_bytes: display(value.reserved_bytes),
            data_bytes: display(value.data_bytes),
            index_bytes: display(value.index_bytes),
            in_row_bytes: display(value.in_row_bytes),
            lob_bytes: display(value.lob_bytes),
            overflow_bytes: display(value.row_overflow_bytes),
            has_primary_key,
            is_heap: value.is_heap,
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
            forwarded_fetches: None,
            xid_age: None,
            storage: None,
            mssql_full_text: Some(value.full_text),
            mssql_heap_configuration: value.heap_configuration,
            mssql_storage_configuration: Some(value.storage_configuration),
            mssql_tracked_by_cdc: value.tracked_by_cdc,
            mssql_temporal_configuration: Some(value.temporal_configuration),
            partition_of: None,
            counters_since: None,
        })
    }
}

impl MssqlIndexSnapshot {
    pub fn into_index_stat(self, presentation: MssqlIndexPresentation) -> Option<IndexStat> {
        let index = self;
        if index.parent_kind != MssqlIndexParentKind::Table {
            return None;
        }
        let counter = |field: fn(&MssqlIndexUsageCounters) -> &Option<String>| match &index.usage {
            MssqlIndexUsage::Observed(usage) if !usage.row_present => Some(0.0),
            MssqlIndexUsage::Observed(usage) => {
                field(usage).as_ref().and_then(|value| value.parse::<u64>().ok()).map(|value| value as f64)
            }
            _ => None,
        };
        let (seeks, scans, lookups, writes) = (
            counter(|v| &v.user_seeks),
            counter(|v| &v.user_scans),
            counter(|v| &v.user_lookups),
            counter(|v| &v.user_updates),
        );
        let mut keys = index.columns.iter().filter(|column| column.key_ordinal > 0).collect::<Vec<_>>();
        keys.sort_by_key(|column| column.key_ordinal);
        let columns = keys
            .into_iter()
            .map(|column| if column.descending { format!("{} DESC", column.name) } else { column.name.clone() })
            .collect();
        Some(IndexStat {
            mssql_columnstore: index.columnstore,
            mssql_configuration: index.configuration,
            docdb: None,
            filter_facts: presentation.filter_facts,
            schema: index.parent_schema,
            table: index.parent_name,
            name: index.name,
            kind: index.native_type.to_ascii_lowercase(),
            columns,
            include: index.columns.into_iter().filter(|column| column.included).map(|column| column.name).collect(),
            filter: presentation.filter,
            is_unique: index.unique,
            is_primary_key: index.primary_key,
            is_clustered: matches!(index.type_code, 1 | 5),
            is_disabled: index.disabled,
            state: None,
            maintained: None,
            has_statistics: None,
            size_bytes: index.reserved_bytes.and_then(|value| value.parse::<u64>().ok()).map(|value| value as f64),
            reads: seeks.zip(scans).zip(lookups).map(|((a, b), c)| a + b + c),
            writes,
            seeks,
            scans,
            lookups,
            last_read: presentation.last_read,
            definition: None,
            contention: None,
            created_at_latest: None,
            partition_of: None,
            opclasses: None,
            counters_since: None,
        })
    }
}

impl MssqlColumnDefinition {
    pub fn into_column_info(self, default: Option<String>) -> ColumnInfo {
        let column = self;
        ColumnInfo {
            data_type: declared_type(&column),
            generated: column.identity || column.computed.is_some(),
            schema: column.schema,
            table: column.table_name,
            name: column.name,
            nullable: column.nullable,
            default,
            ordinal: column.column_id,
        }
    }
}
fn declared_type(column: &MssqlColumnDefinition) -> String {
    let name = &column.type_name;
    let length = column.max_length;
    match name.to_ascii_lowercase().as_str() {
        "varchar" | "char" | "varbinary" | "binary" if length < 0 => format!("{name}(max)"),
        "varchar" | "char" | "varbinary" | "binary" => format!("{name}({length})"),
        "nvarchar" | "nchar" if length < 0 => format!("{name}(max)"),
        "nvarchar" | "nchar" => format!("{name}({})", length / 2),
        "decimal" | "numeric" => format!("{name}({},{})", column.precision, column.scale),
        "datetime2" | "datetimeoffset" | "time" => format!("{name}({})", column.scale),
        _ => name.clone(),
    }
}

impl MssqlStatisticsSnapshot {
    pub fn into_statistics_info(self, last_updated: Option<String>) -> StatisticsInfo {
        StatisticsInfo {
            schema: self.schema,
            table: self.table_name,
            name: self.name,
            auto_created: self.auto_created,
            last_updated,
            rows: display(self.rows),
            rows_sampled: display(self.rows_sampled),
            modifications: display(self.modifications),
        }
    }
}

impl SettingObservation {
    /// SQL Server native settings distinguish configured and effective server values.
    pub fn to_mssql_setting(&self) -> Option<Setting> {
        let value = self;
        let observed = if value.namespace == "sys.configurations" {
            value.value_in_use.as_ref()?
        } else {
            value.value.as_ref()?
        };
        Some(Setting {
            name: if value.namespace == "sys.database_query_store_options" {
                format!("query_store_options.{}", value.name)
            } else {
                value.name.clone()
            },
            value: observed.clone(),
            default_value: value.default_value.clone(),
            scope: match value.scope {
                MeasurementScope::Database => "database",
                MeasurementScope::Server => "server",
                _ => return None,
            }
            .into(),
        })
    }
}
