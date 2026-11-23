//! Resolve a recorded PostgreSQL entry by its captured native dimensions, never its opaque report ID.
use super::{CATALOG_PATH, elevated_read, not_its_own};
use crate::{read::QueryRead, rows::Row};
use culprit_core::{RecordedStatement, Result, telemetry::NativeQueryIdentity};

const NATIVE_KEYS: &[&str] = &["userid", "dbid", "postmaster_started_at", "toplevel", "stats_since"];
const ENTRY_FIELDS: &[&str] =
    &["queryid", "userid", "dbid", "postmaster_started_at", "toplevel", "stats_since", "query", "own_query"];
const CATALOG: &str = "SELECT n.nspname::text AS schema,
 EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attname='toplevel' AND NOT a.attisdropped) AS has_toplevel,
 EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attname='stats_since' AND NOT a.attisdropped) AS has_stats_since
 FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
 JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname='pg_stat_statements' AND c.relkind='v'
 WHERE e.extname='pg_stat_statements'";

struct ExpectedEntry {
    identity: NativeQueryIdentity,
}
impl ExpectedEntry {
    fn from_statement(statement: RecordedStatement<'_>) -> Result<Option<Self>> {
        let Some(source_id) = statement.source_id() else { return Ok(None) };
        let Some(identity) = statement.native_identity() else { return Err(not_its_own()) };
        if source_id.is_empty()
            || !identity.is_valid()
            || identity.query_id.parse::<i64>().is_err()
            || identity.qualifiers.keys().any(|key| !NATIVE_KEYS.contains(&key.as_str()))
        {
            return Err(not_its_own());
        }
        for key in ["userid", "dbid"] {
            if !identity
                .qualifiers
                .get(key)
                .is_some_and(|value| value.parse::<u32>().is_ok_and(|id| id > 0 && id.to_string() == *value))
            {
                return Err(not_its_own());
            }
        }
        let mut identity = identity.clone();
        let started = identity.qualifiers.get_mut("postmaster_started_at").ok_or_else(not_its_own)?;
        *started = culprit_core::telemetry::canonical_time(started).map_err(|_| not_its_own())?;
        if let Some(since) = identity.qualifiers.get_mut("stats_since") {
            *since = culprit_core::telemetry::canonical_time(since).map_err(|_| not_its_own())?;
        }
        if identity.qualifiers.get("toplevel").is_some_and(|value| !matches!(value.as_str(), "true" | "false")) {
            return Err(not_its_own());
        }
        Ok(Some(Self { identity }))
    }
    fn is_top_level(&self) -> bool {
        self.identity.qualifiers.get("toplevel").map(String::as_str) == Some("true")
    }
    fn parameters(&self) -> Vec<String> {
        let field = |key| self.identity.qualifiers.get(key).cloned().unwrap_or_default();
        vec![
            self.identity.query_id.clone(),
            field("userid"),
            field("dbid"),
            field("postmaster_started_at"),
            field("toplevel"),
            field("stats_since"),
        ]
    }
    fn text(&self, row: &Row, elevated: bool) -> Result<String> {
        let actual = crate::native::statements::scan_identity(row).map_err(|_| not_its_own())?;
        if actual != self.identity || elevated && (!self.is_top_level() || !native_flag(row, "own_query")?) {
            return Err(not_its_own());
        }
        row.text("query").filter(|text| !text.is_empty() && text != "<insufficient privilege>").ok_or_else(not_its_own)
    }
}

struct NativeColumns {
    schema: String,
    toplevel: bool,
    stats_since: bool,
}
impl NativeColumns {
    fn from_row(row: &Row, expected: &ExpectedEntry) -> Result<Self> {
        let columns = Self {
            schema: row.text("schema").filter(|schema| !schema.is_empty()).ok_or_else(not_its_own)?,
            toplevel: native_flag(row, "has_toplevel")?,
            stats_since: native_flag(row, "has_stats_since")?,
        };
        if columns.toplevel != expected.identity.qualifiers.contains_key("toplevel")
            || columns.stats_since != expected.identity.qualifiers.contains_key("stats_since")
        {
            return Err(not_its_own());
        }
        Ok(columns)
    }
    fn query(&self) -> String {
        let top = if self.toplevel { "s.toplevel::text" } else { "NULL::text" };
        let since = if self.stats_since { crate::native::iso("s.stats_since") } else { "NULL::text".into() };
        let top_predicate = if self.toplevel { " AND s.toplevel=(($1::text[])[5])::boolean" } else { "" };
        let since_predicate = if self.stats_since { " AND s.stats_since=(($1::text[])[6])::timestamptz" } else { "" };
        format!(
            "SELECT s.queryid::text AS queryid,s.userid::text AS userid,s.dbid::text AS dbid,
             {started} AS postmaster_started_at,{top} AS toplevel,{since} AS stats_since,s.query,
             CASE WHEN pg_catalog.pg_get_userbyid(s.userid)=session_user AND current_user=session_user THEN 't' ELSE 'f' END AS own_query
             FROM {schema}.pg_stat_statements s
             WHERE s.queryid=(($1::text[])[1])::bigint AND s.userid=(($1::text[])[2])::oid
              AND s.dbid=(($1::text[])[3])::oid
              AND s.dbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
              AND pg_catalog.pg_postmaster_start_time()=(($1::text[])[4])::timestamptz{top_predicate}{since_predicate}
             LIMIT 2",
            started = crate::native::iso("pg_catalog.pg_postmaster_start_time()"),
            schema = crate::rows::quote_ident(&self.schema),
        )
    }
}
fn native_flag(row: &Row, key: &str) -> Result<bool> {
    match row.text(key).as_deref() {
        Some("t" | "true") => Ok(true),
        Some("f" | "false") => Ok(false),
        _ => Err(not_its_own()),
    }
}
fn entry_row(values: Vec<Option<String>>) -> Result<Row> {
    if values.len() != ENTRY_FIELDS.len() {
        return Err(not_its_own());
    }
    // Omitted native dimensions stay absent; present-but-NULL dimensions fail the expected-identity check.
    Ok(Row::from_fields(ENTRY_FIELDS.iter().zip(values).filter_map(|(key, value)| {
        if value.is_none() && matches!(*key, "toplevel" | "stats_since") { None } else { Some(((*key).into(), value)) }
    })))
}

pub(crate) async fn resolve(reader: &mut impl QueryRead, statement: RecordedStatement<'_>) -> Result<String> {
    let expected = ExpectedEntry::from_statement(statement)?;
    let elevated = elevated_read(reader).await?;
    let Some(expected) = expected else {
        return if elevated { Err(not_its_own()) } else { Ok(statement.sql().into()) };
    };
    if elevated && !expected.is_top_level() {
        return Err(not_its_own());
    }
    reader.batch_execute(&format!("BEGIN READ ONLY; {CATALOG_PATH}")).await?;
    let result = lookup(reader, &expected, elevated).await;
    let cleanup = reader.batch_execute("ROLLBACK").await;
    match result {
        Ok(text) => cleanup.map(|_| text),
        Err(error) => Err(error),
    }
}
async fn lookup(reader: &mut impl QueryRead, expected: &ExpectedEntry, elevated: bool) -> Result<String> {
    let catalog = crate::read::query_one(reader, CATALOG).await?.ok_or_else(not_its_own)?;
    let columns = NativeColumns::from_row(&catalog, expected)?;
    let mut rows = reader.query_text_array(&columns.query(), expected.parameters(), 2).await?;
    if rows.len() != 1 {
        return Err(not_its_own());
    }
    let row = rows.pop().ok_or_else(not_its_own)?;
    let values = ENTRY_FIELDS
        .iter()
        .map(|name| row.try_get::<_, Option<String>>(*name))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(crate::connect::map_error)?;
    expected.text(&entry_row(values)?, elevated)
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
