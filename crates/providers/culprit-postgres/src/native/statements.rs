//! PostgreSQL statement identities, counters and distributions. Layout and units are native facts, not SQL Server defaults.
use super::metadata::{Metadata, StatementCatalog};
use super::*;
use culprit_core::{Engine, Error, ErrorCode, ResetMarker, StatementTemporaryWork, telemetry::*};
use std::collections::BTreeMap;

pub(crate) struct Layout {
    pub total: &'static str,
    pub mean: &'static str,
    pub max: &'static str,
    pub stddev: &'static str,
    pub min: &'static str,
    pub io: &'static str,
    pub since: Option<&'static str>,
    pub toplevel: bool,
    temp_read_time: bool,
    temp_write_time: bool,
}
impl Layout {
    pub fn for_version(version: u32) -> Self {
        if version < 130_000 {
            return Self::legacy();
        }
        Self {
            total: "total_exec_time",
            mean: "mean_exec_time",
            max: "max_exec_time",
            stddev: "stddev_exec_time",
            min: "min_exec_time",
            io: crate::pressure::statements_io_column(version),
            since: (version >= 170_000).then_some("stats_since"),
            toplevel: version >= 140_000,
            temp_read_time: version >= 150_000,
            temp_write_time: version >= 150_000,
        }
    }
    pub fn legacy() -> Self {
        Self {
            total: "total_time",
            mean: "mean_time",
            max: "max_time",
            stddev: "stddev_time",
            min: "min_time",
            io: "(blk_read_time + blk_write_time)",
            since: None,
            toplevel: false,
            temp_read_time: false,
            temp_write_time: false,
        }
    }
    pub fn detect(catalog: &StatementCatalog, version: u32) -> Result<Self> {
        let mut layout = if catalog.columns.contains("total_exec_time") {
            Self::for_version(version.max(130_000))
        } else {
            Self::legacy()
        };
        layout.since = catalog.columns.contains("stats_since").then_some("stats_since");
        layout.toplevel = catalog.columns.contains("toplevel");
        layout.temp_read_time = catalog.columns.contains(TEMP_READ_TIME.column);
        layout.temp_write_time = catalog.columns.contains(TEMP_WRITE_TIME.column);
        if [
            "userid",
            "dbid",
            "queryid",
            "query",
            "calls",
            "rows",
            layout.total,
            layout.mean,
            layout.max,
            layout.stddev,
            layout.min,
        ]
        .iter()
        .all(|column| catalog.columns.contains(*column))
        {
            Ok(layout)
        } else {
            Err(invalid())
        }
    }
}
struct MetricSpec {
    column: &'static str,
    key: RuntimeMetricKey,
    unit: RuntimeMetricUnit,
    integer: bool,
}
const TEMP_READ_BLOCKS: (&str, RuntimeMetricKey) = ("temp_blks_read", RuntimeMetricKey::TemporaryBlocksRead);
const TEMP_WRITTEN_BLOCKS: (&str, RuntimeMetricKey) = ("temp_blks_written", RuntimeMetricKey::TemporaryBlocksWritten);
const TEMP_READ_TIME: TemporaryTime =
    TemporaryTime { column: "temp_blk_read_time", key: RuntimeMetricKey::TemporaryReadTime };
const TEMP_WRITE_TIME: TemporaryTime =
    TemporaryTime { column: "temp_blk_write_time", key: RuntimeMetricKey::TemporaryWriteTime };
const TEMP_BLOCK_BYTES: &str = "temp_block_bytes";

struct TemporaryTime {
    column: &'static str,
    key: RuntimeMetricKey,
}

/// The same native columns feed exact exported counters and the scan's statement-resource projection.
/// Resource totals may include planning and execution; a zero timer does not establish historical timing coverage.
/// https://www.postgresql.org/docs/current/pgstatstatements.html
pub(crate) fn temporary_work(row: &Row) -> Result<Option<StatementTemporaryWork>> {
    let read_blocks = integer_value(row, TEMP_READ_BLOCKS.0)?;
    let written_blocks = integer_value(row, TEMP_WRITTEN_BLOCKS.0)?;
    let read_time_ms = number(row, TEMP_READ_TIME.column)?;
    let write_time_ms = number(row, TEMP_WRITE_TIME.column)?;
    if read_blocks.is_none() && written_blocks.is_none() && read_time_ms.is_none() && write_time_ms.is_none() {
        return Ok(None);
    }
    let bytes_per_block =
        row.text(TEMP_BLOCK_BYTES).map(|value| value.parse::<u32>().map_err(|_| invalid())).transpose()?;
    let work = StatementTemporaryWork::BlockIo {
        source: "pg_stat_statements".into(),
        bytes_per_block,
        read_blocks,
        written_blocks,
        read_time_ms,
        write_time_ms,
    };
    if work.is_valid() { Ok(Some(work)) } else { Err(invalid()) }
}

fn temporary_projection(layout: &Layout) -> String {
    let mut columns = [TEMP_READ_BLOCKS.0, TEMP_WRITTEN_BLOCKS.0]
        .into_iter()
        .map(|column| format!("sum({column})::text AS {column}"))
        .collect::<Vec<_>>();
    for (spec, available) in [(&TEMP_READ_TIME, layout.temp_read_time), (&TEMP_WRITE_TIME, layout.temp_write_time)] {
        let column = spec.column;
        columns.push(if available {
            format!("sum({column}) AS {column}")
        } else {
            format!("NULL::float8 AS {column}")
        });
    }
    columns.push(format!("pg_catalog.current_setting('block_size', true)::integer AS {TEMP_BLOCK_BYTES}"));
    columns.join(", ")
}

fn integer_value(row: &Row, name: &str) -> Result<Option<String>> {
    row.text(name).map(|value| value.parse::<u64>().map(|value| value.to_string()).map_err(|_| invalid())).transpose()
}

fn metrics(meta: &Metadata, layout: &Layout) -> Vec<MetricSpec> {
    use RuntimeMetricKey as Key;
    use RuntimeMetricUnit as Unit;
    let pages = Unit::Pages { bytes_per_page: meta.block_bytes };
    let mut specs = vec![
        MetricSpec { column: "calls", key: Key::Executions, unit: Unit::Count, integer: true },
        MetricSpec { column: "rows", key: Key::Rows, unit: Unit::Count, integer: true },
        MetricSpec { column: layout.total, key: Key::Duration, unit: Unit::Milliseconds, integer: false },
        MetricSpec { column: "wal_bytes", key: Key::LogBytes, unit: Unit::Bytes, integer: true },
    ];
    for (column, key, unit, integer) in [
        ("shared_blks_hit", "shared-block-hits", pages, true),
        ("shared_blks_read", "shared-block-reads", pages, true),
        ("shared_blks_dirtied", "shared-blocks-dirtied", pages, true),
        ("shared_blks_written", "shared-blocks-written", pages, true),
        ("local_blks_hit", "local-block-hits", pages, true),
        ("local_blks_read", "local-block-reads", pages, true),
        ("local_blks_dirtied", "local-blocks-dirtied", pages, true),
        ("local_blks_written", "local-blocks-written", pages, true),
        ("plans", "plans", Unit::Count, true),
        ("total_plan_time", "planning-duration", Unit::Milliseconds, false),
        ("wal_records", "wal-records", Unit::Count, true),
        ("wal_fpi", "wal-full-page-images", Unit::Count, true),
        ("blk_read_time", "io-read-time", Unit::Milliseconds, false),
        ("blk_write_time", "io-write-time", Unit::Milliseconds, false),
        ("shared_blk_read_time", "shared-io-read-time", Unit::Milliseconds, false),
        ("shared_blk_write_time", "shared-io-write-time", Unit::Milliseconds, false),
        ("local_blk_read_time", "local-io-read-time", Unit::Milliseconds, false),
        ("local_blk_write_time", "local-io-write-time", Unit::Milliseconds, false),
        ("jit_functions", "jit-functions", Unit::Count, true),
        ("jit_generation_time", "jit-generation-time", Unit::Milliseconds, false),
        ("jit_inlining_count", "jit-inlining-count", Unit::Count, true),
        ("jit_inlining_time", "jit-inlining-time", Unit::Milliseconds, false),
        ("jit_optimization_count", "jit-optimization-count", Unit::Count, true),
        ("jit_optimization_time", "jit-optimization-time", Unit::Milliseconds, false),
        ("jit_emission_count", "jit-emission-count", Unit::Count, true),
        ("jit_emission_time", "jit-emission-time", Unit::Milliseconds, false),
        ("jit_deform_count", "jit-deform-count", Unit::Count, true),
        ("jit_deform_time", "jit-deform-time", Unit::Milliseconds, false),
        ("parallel_workers_to_launch", "parallel-workers-requested", Unit::Count, true),
        ("parallel_workers_launched", "parallel-workers-launched", Unit::Count, true),
    ] {
        specs.push(MetricSpec { column, key: Key::Native(format!("postgres.{key}")), unit, integer });
    }
    for (column, key) in [TEMP_READ_BLOCKS, TEMP_WRITTEN_BLOCKS] {
        specs.push(MetricSpec { column, key, unit: pages, integer: true });
    }
    for spec in [TEMP_READ_TIME, TEMP_WRITE_TIME] {
        specs.push(MetricSpec { column: spec.column, key: spec.key, unit: Unit::Milliseconds, integer: false });
    }
    specs
}
fn catalog(meta: &Metadata) -> Result<&StatementCatalog> {
    if !meta.full_visibility {
        return Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
    }
    meta.statements
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")))
}
// PG18 extension 1.12's native function contract, including fields not interpreted by this decoder.
// Old/partial extension installs never inherit a native work bound from a server version or outer LIMIT.
// https://github.com/postgres/postgres/blob/REL_18_STABLE/contrib/pg_stat_statements/pg_stat_statements--1.11--1.12.sql
pub(crate) const GUARDED_COLUMNS: &[&str] = &[
    "userid",
    "dbid",
    "toplevel",
    "queryid",
    "query",
    "plans",
    "total_plan_time",
    "min_plan_time",
    "max_plan_time",
    "mean_plan_time",
    "stddev_plan_time",
    "calls",
    "total_exec_time",
    "min_exec_time",
    "max_exec_time",
    "mean_exec_time",
    "stddev_exec_time",
    "rows",
    "shared_blks_hit",
    "shared_blks_read",
    "shared_blks_dirtied",
    "shared_blks_written",
    "local_blks_hit",
    "local_blks_read",
    "local_blks_dirtied",
    "local_blks_written",
    "temp_blks_read",
    "temp_blks_written",
    "shared_blk_read_time",
    "shared_blk_write_time",
    "local_blk_read_time",
    "local_blk_write_time",
    "temp_blk_read_time",
    "temp_blk_write_time",
    "wal_records",
    "wal_fpi",
    "wal_bytes",
    "wal_buffers_full",
    "jit_functions",
    "jit_generation_time",
    "jit_inlining_count",
    "jit_inlining_time",
    "jit_optimization_count",
    "jit_optimization_time",
    "jit_emission_count",
    "jit_emission_time",
    "jit_deform_count",
    "jit_deform_time",
    "parallel_workers_to_launch",
    "parallel_workers_launched",
    "stats_since",
    "minmax_stats_since",
];

pub(crate) fn validate_guarded_counters(meta: &Metadata, max_entries: u32) -> Result<()> {
    let catalog = catalog(meta)?;
    if meta.version / 10_000 != 18
        || catalog.extension_version.as_deref() != Some("1.12")
        || !catalog.max_entries.is_some_and(|entries| (1..=max_entries).contains(&entries))
        || !GUARDED_COLUMNS.iter().all(|column| catalog.columns.contains(*column))
    {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    Layout::detect(catalog, meta.version)?;
    Ok(())
}

/// Guarded snapshots retain native entry/reset identity, not a fabricated execution interval. A saved entry
/// may precede this postmaster start; its own stats_since remains the only per-entry reset witness.
pub(crate) fn validate_guarded_sample(sample: &StatementCounterSample, meta: &Metadata) -> Result<()> {
    let identity = &sample.identity;
    let Some(since) = identity.qualifiers.get("stats_since") else { return Err(invalid()) };
    let since = canonical_time(since)?;
    let started_at = canonical_time(&meta.started_at)?;
    if sample.member.is_some()
        || sample.scope != culprit_core::MeasurementScope::Database
        || identity.qualifiers.get("dbid") != Some(&meta.database_oid)
        || identity.qualifiers.get("postmaster_started_at") != Some(&started_at)
        || !identity.qualifiers.get("userid").is_some_and(|user| user.parse::<u32>().is_ok_and(|user| user > 0))
        || !matches!(identity.qualifiers.get("toplevel").map(String::as_str), Some("true" | "false"))
        || sample.reset != ResetMarker::At(since.clone())
        || ObservationBounds::new(&since, &sample.observed_at).is_none()
        || sample.distributions.iter().any(|distribution| match &distribution.extrema_reset {
            ResetMarker::At(at) => ObservationBounds::new(at, &sample.observed_at).is_none(),
            ResetMarker::Unknown | ResetMarker::Generation(_) => true,
        })
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn query(meta: &Metadata, definitions: bool, limit: usize, max_sql_bytes: usize) -> Result<String> {
    let catalog = catalog(meta)?;
    let layout = Layout::detect(catalog, meta.version)?;
    let mut columns =
        vec!["s.userid::text AS userid".into(), "s.dbid::text AS dbid".into(), "s.queryid::text AS queryid".into()];
    columns.push(format!("{} AS observed_at", iso("pg_catalog.clock_timestamp()")));
    if layout.toplevel {
        columns.push("s.toplevel::text AS toplevel".into());
    }
    for name in ["stats_since", "minmax_stats_since"] {
        if catalog.columns.contains(name) {
            columns.push(format!("{} AS {name}", iso(&format!("s.{name}"))));
        }
    }
    if definitions {
        columns.push(format!(
            "CASE WHEN pg_catalog.octet_length(s.query)<={max_sql_bytes} THEN s.query ELSE NULL END AS query"
        ));
        columns.push("pg_catalog.octet_length(s.query)::text AS source_text_bytes".into());
    } else {
        for spec in metrics(meta, &layout) {
            if catalog.columns.contains(spec.column) {
                columns.push(format!("s.{0}::text AS {0}", spec.column));
            }
        }
        for name in [
            layout.mean,
            layout.min,
            layout.max,
            layout.stddev,
            "mean_plan_time",
            "min_plan_time",
            "max_plan_time",
            "stddev_plan_time",
        ] {
            if catalog.columns.contains(name) {
                columns.push(format!("s.{name}::text AS {name}"));
            }
        }
    }
    Ok(format!(
        "SELECT {} FROM {}.pg_stat_statements({definitions}) s WHERE s.dbid={} LIMIT {limit}",
        columns.join(","),
        crate::rows::quote_ident(&catalog.schema),
        meta.database_oid
    ))
}
fn identity(row: &Row, meta: &Metadata) -> Result<NativeQueryIdentity> {
    entry_identity(row, &meta.database_oid, &meta.started_at)
}

/// Scan and exported points keep the same native entry dimensions. This is content identity, not permission,
/// acquisition ownership or a claim that an older source without stats_since exposes a reset witness.
pub(crate) fn scan_identity(row: &Row) -> Result<NativeQueryIdentity> {
    let database_oid = integer(row, "dbid")?.to_string();
    entry_identity(row, &database_oid, &text(row, "postmaster_started_at")?)
}

fn entry_identity(row: &Row, database_oid: &str, started_at: &str) -> Result<NativeQueryIdentity> {
    let query_id = text(row, "queryid")?;
    query_id.parse::<i64>().map_err(|_| invalid())?;
    let dbid = integer(row, "dbid")?.to_string();
    if dbid != database_oid {
        return Err(invalid());
    }
    let mut qualifiers = BTreeMap::from([
        ("userid".into(), integer(row, "userid")?.to_string()),
        ("dbid".into(), dbid),
        ("postmaster_started_at".into(), canonical_time(started_at)?),
    ]);
    if row.value("toplevel").is_some() {
        qualifiers.insert("toplevel".into(), flag(row, "toplevel")?.to_string());
    }
    if let Some(value) = row.text("stats_since") {
        qualifiers.insert("stats_since".into(), canonical_time(&value)?);
    }
    Ok(NativeQueryIdentity { query_id, qualifiers })
}
fn reset_marker(row: &Row, name: &str) -> Result<ResetMarker> {
    row.text(name)
        .map(|value| canonical_time(&value).map(ResetMarker::At))
        .transpose()
        .map(|value| value.unwrap_or(ResetMarker::Unknown))
}
fn number(row: &Row, name: &str) -> Result<Option<f64>> {
    row.text(name)
        .map(|value| {
            value.parse::<f64>().ok().filter(|number| number.is_finite() && *number >= 0.0).ok_or_else(invalid)
        })
        .transpose()
}
pub(crate) fn counters(row: &Row, meta: &Metadata) -> Result<StatementCounterSample> {
    let layout = Layout::detect(catalog(meta)?, meta.version)?;
    let mut totals = vec![];
    for spec in metrics(meta, &layout) {
        if row.value(spec.column).flatten().is_none() {
            continue;
        }
        let total = if spec.integer {
            MeasurementValue::Integer(integer_value(row, spec.column)?.ok_or_else(invalid)?)
        } else {
            MeasurementValue::Real(number(row, spec.column)?.ok_or_else(invalid)?)
        };
        totals.push(StatementCounter { key: spec.key, unit: spec.unit, total });
    }
    let reset = reset_marker(row, "stats_since")?;
    let extrema_reset = reset_marker(row, "minmax_stats_since")?;
    let mut distributions = vec![];
    for (key, mean, min, max, stddev) in [
        (RuntimeMetricKey::Duration, layout.mean, layout.min, layout.max, layout.stddev),
        (
            RuntimeMetricKey::Native("postgres.planning-duration".into()),
            "mean_plan_time",
            "min_plan_time",
            "max_plan_time",
            "stddev_plan_time",
        ),
    ] {
        let count_column = if key == RuntimeMetricKey::Duration { "calls" } else { "plans" };
        if row.text(count_column).is_some_and(|value| value == "0") {
            continue;
        }
        if let Some(average) = number(row, mean)? {
            distributions.push(StatementDistribution {
                statistics: RuntimeMetric {
                    minimum: number(row, min)?.map(MeasurementValue::Real),
                    maximum: number(row, max)?.map(MeasurementValue::Real),
                    standard_deviation: number(row, stddev)?,
                    ..RuntimeMetric::average(key, RuntimeMetricUnit::Milliseconds, average)
                },
                reset: reset.clone(),
                extrema_reset: extrema_reset.clone(),
            });
        }
    }
    let sample = StatementCounterSample {
        member: None,
        scope: culprit_core::MeasurementScope::Database,
        identity: identity(row, meta)?,
        observed_at: text(row, "observed_at")?,
        reset,
        totals,
        distributions,
    };
    if !sample.is_valid() {
        return Err(invalid());
    }
    Ok(sample)
}
pub(crate) fn definition(
    row: &Row,
    meta: &Metadata,
    source: &CollectionDescription,
    max_sql_bytes: usize,
) -> Result<QueryDefinition> {
    let identity = identity(row, meta)?;
    let source_text_bytes = row.text("source_text_bytes");
    let text = row.text("query");
    let (origin, monitoring_template_id) = StatementOrigin::from_sql(text.as_deref(), Engine::Postgres);
    let sql = match &text {
        Some(text) => DefinitionPayload::sql(text, Engine::Postgres),
        None => DefinitionPayload::Unavailable {
            reason: if source_text_bytes
                .as_deref()
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|bytes| bytes > max_sql_bytes)
            {
                PayloadUnavailable::Oversized
            } else {
                PayloadUnavailable::Missing
            },
        },
    };
    Ok(QueryDefinition {
        variant_family: None,
        id: identity.definition_id(source)?,
        identity,
        origin,
        monitoring_template_id,
        context: Default::default(),
        context_available: false,
        source_text_bytes,
        sql,
    })
}

/// Every selected row remains one native userid/dbid/queryid/top-level entry and counter lifetime.
/// Query text is still the native representative for that entry, never another role's preferred text.
pub(crate) fn aggregated_base(relation: &str, c: &Layout) -> String {
    let toplevel = if c.toplevel { " AND toplevel" } else { "" };
    let extrema = |aggregate: &str, column: &str| {
        let value = format!("{aggregate}({column})");
        if let Some(since) = c.since {
            format!("CASE WHEN bool_and(minmax_stats_since = {since}) THEN {value} END")
        } else {
            value
        }
    };
    format!(
        "SELECT {identity}, (array_agg(query ORDER BY {total} DESC))[1] AS query, sum(calls) AS calls,
                sum({total}) AS total_ms, sum({total}) / nullif(sum(calls), 0)::float8 AS mean_ms,
                {maximum} AS max_ms, {minimum} AS min_ms,
                sqrt(greatest(sum(calls * ({stddev} ^ 2 + {mean} ^ 2)) / nullif(sum(calls), 0)::float8
                              - (sum({total}) / nullif(sum(calls), 0)::float8) ^ 2, 0)) AS stddev_ms,
                sum(rows) AS rows, sum(shared_blks_hit + shared_blks_read) AS reads, sum(shared_blks_read) AS phys,
                {temporary}, sum({io}) AS io_ms, count(*) AS entries,
                (array_agg(query ORDER BY {total} DESC)
                   FILTER (WHERE pg_get_userbyid(userid) = session_user AND current_user = session_user))[1] AS own_query
         FROM {relation}
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()){toplevel}
           AND query NOT LIKE '%{tag}'
         GROUP BY {group}",
        identity = scan_identity_projection(c),
        group = scan_identity_group(c),
        tag = crate::rows::TAG,
        total = c.total,
        mean = c.mean,
        maximum = extrema("max", c.max),
        minimum = extrema("min", c.min),
        stddev = c.stddev,
        io = c.io,
        temporary = temporary_projection(c),
    )
}

/// Shared by the workload and its later optional costs read, so neither can regroup another role/reset into it.
pub(crate) fn scan_identity_projection(layout: &Layout) -> String {
    // Identity clocks retain the same native microseconds as metadata and exported entry/reset points.
    // The display-oriented rows::iso truncates fractional seconds and must not merge distinct native lifetimes.
    let started = iso("pg_catalog.pg_postmaster_start_time()");
    let since = layout.since.map(iso).unwrap_or_else(|| "NULL::text".into());
    let top_key = if layout.toplevel { "toplevel::text" } else { "'unknown'" };
    let mut fields = vec![
        format!(
            "queryid::text || '/' || userid::text || '/' || dbid::text || '/' || {top_key} || '/' || coalesce({since}, 'unknown') || '/' || {started} AS id"
        ),
        "queryid::text AS queryid".into(),
        "userid::text AS userid".into(),
        "dbid::text AS dbid".into(),
        format!("{started} AS postmaster_started_at"),
        format!("{since} AS stats_since"),
    ];
    if layout.toplevel {
        fields.push("toplevel".into());
    }
    fields.join(", ")
}

pub(crate) fn scan_identity_group(layout: &Layout) -> String {
    let mut keys = vec!["queryid", "userid", "dbid"];
    if layout.toplevel {
        keys.push("toplevel");
    }
    if let Some(since) = layout.since {
        keys.push(since);
    }
    keys.join(", ")
}

pub(crate) fn statements_base(relation: &str, version_num: u32) -> String {
    aggregated_base(relation, &Layout::for_version(version_num))
}
pub(crate) fn statements_base_legacy(relation: &str) -> String {
    aggregated_base(relation, &Layout::legacy())
}

#[cfg(test)]
mod tests;
