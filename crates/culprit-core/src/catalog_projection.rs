//! Shared pure projections from native catalog telemetry into `Snapshot` model types so live scans and archived
//! synthesis interpret the same schema, collation, operator-class, foreign-key, constraint, and sequence facts.

use crate::operations::SequenceCapacity;
use crate::telemetry::{
    CatalogExpression, DefinitionPayload, MeasurementValue, NativeCatalogCounters, NativeCatalogDefinition,
    NativeCounterSet,
};
use crate::{ColumnInfo, ConstraintIssue, ConstraintKind, ForeignKey, TableStat};
use std::collections::BTreeMap;

/// Counter value as `f64` when present and parseable.
pub fn counter_number(set: &NativeCounterSet, key: &str) -> Option<f64> {
    match &set.counters.iter().find(|sample| sample.key == key)?.value {
        MeasurementValue::Integer(raw) => raw.parse::<f64>().ok(),
        MeasurementValue::Real(value) => Some(*value),
    }
}

/// Appends ` COLLATE <collation>` to MySQL/MariaDB `ColumnInfo::data_type` when the schema uses multiple column
/// collations and a column's collation differs from the dominant collation.
pub fn apply_mysql_mixed_collations(columns: &mut [ColumnInfo], collations: &[Option<&str>]) {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for coll in collations.iter().copied().flatten().filter(|s| !s.is_empty()) {
        *counts.entry(coll.to_ascii_lowercase()).or_default() += 1;
    }
    if counts.len() <= 1 {
        return;
    }
    let dominant = counts.iter().max_by_key(|(_, count)| *count).map(|(coll, _)| coll.as_str());
    for (col, coll) in columns.iter_mut().zip(collations.iter().copied()) {
        if let Some(coll) = coll.filter(|s| !s.is_empty())
            && dominant != Some(coll.to_ascii_lowercase().as_str())
        {
            col.data_type.push_str(&format!(" COLLATE {coll}"));
        }
    }
}

/// Canonicalizes MySQL/MariaDB `REFERENTIAL_CONSTRAINTS.DELETE_RULE`, mapping default `NO ACTION` to `None`.
pub fn mysql_delete_rule(raw: Option<&str>) -> Option<String> {
    match raw?.trim().to_ascii_uppercase().as_str() {
        "CASCADE" => Some("CASCADE".into()),
        "SET NULL" => Some("SET NULL".into()),
        "SET DEFAULT" => Some("SET DEFAULT".into()),
        "RESTRICT" => Some("RESTRICT".into()),
        _ => None,
    }
}

/// Formats one MySQL/MariaDB index key part from `information_schema.STATISTICS`, preserving prefix lengths, descending
/// sort order, and functional key parts without failing catalog projection when `COLUMN_NAME` is `NULL`.
pub fn mysql_index_key_part(
    column_name: Option<&str>,
    expression: Option<&str>,
    sub_part: Option<u64>,
    collation: Option<&str>,
    uncollected_fallback: &str,
) -> String {
    let base = match (column_name.filter(|s| !s.is_empty()), expression.filter(|s| !s.is_empty())) {
        (Some(col), _) => col.to_owned(),
        (None, Some(expr)) => format!("({expr})"),
        (None, None) => uncollected_fallback.to_owned(),
    };
    let base = match sub_part {
        Some(prefix) => format!("{base}({prefix})"),
        None => base,
    };
    if collation == Some("D") { format!("{base} DESC") } else { base }
}

/// Exact integer bounds `(min, max)` for MySQL/MariaDB `AUTO_INCREMENT` integer column types.
pub fn mysql_integer_bounds(column_type: &str) -> Option<(&'static str, &'static str)> {
    let ty = column_type.to_ascii_lowercase();
    let unsigned = ty.contains("unsigned");
    let base = ty.split(['(', ' ']).next()?;
    Some(match (base, unsigned) {
        ("tinyint", false) => ("-128", "127"),
        ("tinyint", true) => ("0", "255"),
        ("smallint", false) => ("-32768", "32767"),
        ("smallint", true) => ("0", "65535"),
        ("mediumint", false) => ("-8388608", "8388607"),
        ("mediumint", true) => ("0", "16777215"),
        ("int" | "integer", false) => ("-2147483648", "2147483647"),
        ("int" | "integer", true) => ("0", "4294967295"),
        ("bigint", false) => ("-9223372036854775808", "9223372036854775807"),
        ("bigint", true) => ("0", "18446744073709551615"),
        _ => return None,
    })
}

/// Projects a MySQL/MariaDB `AUTO_INCREMENT` observation into a `SequenceCapacity`.
pub fn mysql_sequence_capacity(
    name: String,
    table: Option<String>,
    column_type: &str,
    current: String,
    increment: String,
) -> Option<SequenceCapacity> {
    let (minimum, maximum) = mysql_integer_bounds(column_type)?;
    Some(SequenceCapacity {
        name,
        table,
        current,
        increment,
        minimum: minimum.into(),
        maximum: maximum.into(),
        cycles: false,
        allocated: true,
    })
}

/// Projects a PostgreSQL/YSQL `NativeCatalogDefinition` (`column`) into `ColumnInfo`.
pub fn pg_catalog_column(def: &NativeCatalogDefinition, raw_default: Option<String>) -> Option<ColumnInfo> {
    let prop = |name: &str| def.properties.get(name).cloned().unwrap_or_default();
    let ordinal = prop("ordinal").parse::<u32>().ok()?;
    let default = raw_default.or_else(|| match &def.definition {
        Some(CatalogExpression { payload: DefinitionPayload::Available { text, .. }, .. }) => Some(text.clone()),
        _ => None,
    });
    Some(ColumnInfo {
        schema: prop("schema_name"),
        table: prop("table_name"),
        name: prop("column_name"),
        data_type: prop("data_type"),
        nullable: prop("nullable") == "true",
        generated: !prop("identity").is_empty()
            || !prop("generated").is_empty()
            || default.as_deref().is_some_and(|value| value.starts_with("nextval(")),
        default,
        ordinal,
    })
}

/// Projects a PostgreSQL/YSQL `NativeCatalogDefinition` (`foreign-key`) into `ForeignKey`.
pub fn pg_catalog_foreign_key(def: &NativeCatalogDefinition) -> Option<ForeignKey> {
    let prop = |name: &str| def.properties.get(name).cloned().unwrap_or_default();
    let columns: Vec<String> = serde_json::from_str(&prop("columns")).ok()?;
    let ref_columns: Vec<String> = serde_json::from_str(&prop("ref_columns")).ok()?;
    Some(ForeignKey {
        name: prop("name"),
        schema: prop("schema_name"),
        table: prop("table_name"),
        columns,
        ref_schema: prop("ref_schema"),
        ref_table: prop("ref_table"),
        ref_columns,
        delete_rule: match prop("delete_rule").as_str() {
            "c" => Some("CASCADE".into()),
            "n" => Some("SET NULL".into()),
            "d" => Some("SET DEFAULT".into()),
            "r" => Some("RESTRICT".into()),
            _ => None,
        },
    })
}

/// Projects an unvalidated PostgreSQL/YSQL constraint (`NOT VALID`) into `ConstraintIssue`.
pub fn pg_catalog_constraint_issue(
    def: &NativeCatalogDefinition,
    default_kind: ConstraintKind,
    allow_inheritance_child: bool,
) -> Option<ConstraintIssue> {
    let prop = |name: &str| def.properties.get(name).cloned().unwrap_or_default();
    if prop("validated") != "false" {
        return None;
    }
    let kind = match prop("constraint_type").as_str() {
        "f" => ConstraintKind::ForeignKey,
        "c" => ConstraintKind::Check,
        _ => default_kind,
    };
    Some(ConstraintIssue {
        schema: prop("schema_name"),
        table: prop("table_name"),
        name: prop("name"),
        kind,
        disabled: false,
        inheritance_child: allow_inheritance_child && prop("inheritance_child") == "true",
    })
}

/// Appends `issue` to `issues` without duplicating `(schema, table, name, kind)` when both `ForeignKeys` and
/// `CheckConstraints` (`UnvalidatedConstraints`) report the same `NOT VALID` foreign key.
pub fn push_unique_constraint_issue(issues: &mut Vec<ConstraintIssue>, issue: ConstraintIssue) {
    if let Some(existing) = issues.iter_mut().find(|existing| {
        existing.schema == issue.schema
            && existing.table == issue.table
            && existing.name == issue.name
            && existing.kind == issue.kind
    }) {
        existing.inheritance_child |= issue.inheritance_child;
        existing.disabled |= issue.disabled;
    } else {
        issues.push(issue);
    }
}

/// Projects a PostgreSQL table `NativeCounterSet` into `TableStat`, keeping negative/uncounted row estimates as `None`.
pub fn pg_table_stat(set: &NativeCounterSet) -> TableStat {
    let label = |key: &str| set.properties.get(key).cloned().unwrap_or_default();
    let optional_label = |key: &str| set.properties.get(key).cloned();
    let flag = |key: &str| set.properties.get(key).is_some_and(|value| matches!(value.as_str(), "t" | "true"));
    TableStat {
        schema: label("schema_name"),
        name: label("table_name"),
        kind: None,
        definition: None,
        rows: counter_number(set, "rows").filter(|v| v.is_finite() && *v >= 0.0),
        total_bytes: counter_number(set, "total_bytes"),
        data_bytes: counter_number(set, "data_bytes"),
        index_bytes: counter_number(set, "index_bytes"),
        has_primary_key: flag("has_pk"),
        is_heap: None,
        seq_scans: counter_number(set, "seq_scan"),
        seq_rows_read: counter_number(set, "seq_tup_read"),
        index_scans: counter_number(set, "idx_scan"),
        inserts: counter_number(set, "n_tup_ins"),
        updates: counter_number(set, "n_tup_upd"),
        deletes: counter_number(set, "n_tup_del"),
        dead_rows: counter_number(set, "n_dead_tup"),
        modifications_since_analyze: counter_number(set, "n_mod_since_analyze"),
        last_vacuum: optional_label("last_vacuum"),
        last_analyze: optional_label("last_analyze"),
        forwarded_records: None,
        forwarded_fetches: None,
        xid_age: counter_number(set, "xid_age"),
        storage: None,
        mssql_full_text: None,
        mssql_storage_configuration: None,
        mssql_tracked_by_cdc: None,
        mssql_heap_configuration: None,
        mssql_temporal_configuration: None,
        in_row_bytes: counter_number(set, "in_row_bytes"),
        lob_bytes: counter_number(set, "lob_bytes"),
        overflow_bytes: None,
        partition_of: optional_label("partition_of"),
        counters_since: None,
    }
}

/// Projects a PostgreSQL `NativeCatalogCounters` (`sequence`) into `SequenceCapacity`.
pub fn pg_sequence_capacity(reading: &NativeCatalogCounters) -> Option<SequenceCapacity> {
    let values = &reading.catalog.properties;
    Some(SequenceCapacity {
        name: values.get("name")?.clone(),
        table: values.get("table_name").cloned(),
        current: values.get("current_value")?.clone(),
        increment: values.get("step")?.clone(),
        minimum: values.get("min_value")?.clone(),
        maximum: values.get("max_value")?.clone(),
        cycles: values.get("cycle").is_some_and(|value| value == "t" || value == "true"),
        allocated: true,
    })
}

/// Parses a `chr(31)`-separated PostgreSQL/YSQL `key_opclasses` string, returning `None` when every key uses defaults.
pub fn pg_opclasses_from_list(list: Option<&str>) -> Option<Vec<String>> {
    let keys: Vec<String> = list?.split('\u{1f}').map(|k| k.trim().to_string()).collect();
    keys.iter().any(|k| !k.is_empty()).then_some(keys)
}

/// Reconstructs per-key operator-class/collation markers from archived PostgreSQL/YSQL index catalog properties.
/// Prefers human `key_opclasses` when present (`None` if all keys use defaults); falls back to raw `native_opclasses`
/// and `native_collations` OID vectors on older captures so distinct operator classes never compare equal.
pub fn pg_archive_opclasses(def: &NativeCatalogDefinition, key_count: usize) -> Option<Vec<String>> {
    if let Some(raw) = def.properties.get("key_opclasses") {
        return pg_opclasses_from_list(Some(raw));
    }
    let opc: Vec<&str> = def.properties.get("native_opclasses").map_or("", String::as_str).split_whitespace().collect();
    let coll: Vec<&str> =
        def.properties.get("native_collations").map_or("", String::as_str).split_whitespace().collect();
    if opc.is_empty() && coll.is_empty() {
        return None;
    }
    let n = key_count.max(opc.len()).max(coll.len());
    Some(
        (0..n)
            .map(|i| format!("oid:{}:{}", opc.get(i).copied().unwrap_or("0"), coll.get(i).copied().unwrap_or("0")))
            .collect(),
    )
}
