//! One selected status snapshot shares the ordinary activity projection and decoder.
use super::*;
use culprit_core::telemetry::{PostgresSlruCounters, PostgresSlruKind, PostgresSubtransactionCache};
use serde::Deserialize;

pub(crate) fn classify(frame: &mut ActivityFrame) {
    for record in &mut frame.records {
        let Some(wait) = record.facts.as_mut().and_then(|actor| actor.wait.as_mut()) else { continue };
        wait.class = match wait.native_type.as_deref() {
            Some("LWLock: SubtransSLRU" | "LWLock: SubtransBuffer") => Some(ActivityWaitClass::SubtransactionMetadata),
            Some(
                "LWLock: MultiXactMemberSLRU"
                | "LWLock: MultiXactMemberBuffer"
                | "LWLock: MultiXactOffsetSLRU"
                | "LWLock: MultiXactOffsetBuffer"
                | "LWLock: MultiXactGen"
                | "LWLock: MultiXactTruncation",
            ) => Some(ActivityWaitClass::MultiTransactionMetadata),
            _ => wait.class,
        };
    }
}
fn slru_names(version: u32) -> [&'static str; 3] {
    if version < 170_000 {
        ["Subtrans", "MultiXactMember", "MultiXactOffset"]
    } else {
        ["subtransaction", "multixact_member", "multixact_offset"]
    }
}
/// Output/lookup bounds do not bound internal backend-status or SLRU snapshot work.
pub(crate) fn query(version: u32, focus_pid: u32, limit: usize) -> Result<String> {
    if version < 160_000 || focus_pid == 0 || !(1..=MAX_TRANSACTION_METADATA_BACKENDS + 1).contains(&limit) {
        return Err(invalid());
    }
    let names = slru_names(version).map(|name| format!("'{name}'")).join(",");
    let ctes = format!(
        r#"backend_cache AS MATERIALIZED (
        SELECT bid AS native_backend_id,pg_catalog.pg_stat_get_backend_pid(bid) AS native_pid,
        pg_catalog.pg_stat_get_backend_start(bid) AS native_backend_start,
        pg_catalog.pg_stat_get_backend_dbid(bid) AS native_database_oid,
        pg_catalog.pg_stat_get_backend_xact_start(bid) AS native_transaction_start,
        cache.subxact_count AS native_cached_subxids,cache.subxact_overflowed AS native_overflowed
        FROM pg_catalog.pg_stat_get_backend_idset() AS backend(bid)
        CROSS JOIN LATERAL pg_catalog.pg_stat_get_backend_subxact(bid) AS cache(subxact_count,subxact_overflowed)
        WHERE pg_catalog.pg_stat_get_backend_dbid(bid)=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
        AND pg_catalog.pg_stat_get_backend_pid(bid)<>pg_catalog.pg_backend_pid()
    ),slru_points AS MATERIALIZED (
        SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(s) ORDER BY s.native_name),'[]'::jsonb)::text AS native_slrus
        FROM (SELECT name AS native_name,{} AS observed_at,{} AS stats_reset,blks_zeroed::text AS blocks_zeroed,
            blks_hit::text AS blocks_hit,blks_read::text AS blocks_read,blks_written::text AS blocks_written,
            blks_exists::text AS blocks_exists,flushes::text AS flushes,truncates::text AS truncates
            FROM pg_catalog.pg_stat_slru WHERE name IN ({names})) s
    ),"#,
        iso("pg_catalog.clock_timestamp()"),
        iso("stats_reset")
    );
    let selection = format!(
        r#"SELECT a.*,c.native_backend_id,c.native_cached_subxids,c.native_overflowed
        FROM pg_catalog.pg_stat_activity a JOIN backend_cache c
        ON c.native_pid=a.pid AND c.native_backend_start=a.backend_start AND c.native_database_oid=a.datid
        AND c.native_transaction_start IS NOT DISTINCT FROM a.xact_start
        WHERE a.backend_type='client backend' AND (a.pid={focus_pid} OR c.native_overflowed IS TRUE)
        ORDER BY (a.pid={focus_pid}) DESC,a.pid"#
    );
    Ok(metadata_query(
        true,
        ",a.native_backend_id::text AS native_backend_id,a.native_cached_subxids::text AS native_cached_subxids,a.native_overflowed::text AS native_overflowed,(SELECT native_slrus FROM slru_points) AS native_slrus",
        &selection,
        limit,
        &ctes,
    ))
}
pub(crate) struct Reading {
    pub frame: ActivityFrame,
    pub caches: Vec<PostgresSubtransactionCache>,
    pub slrus: Vec<PostgresSlruCounters>,
    pub native_rows: u32,
    pub limited: bool,
}
pub(crate) async fn read<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus_pid: u32,
    limit: usize,
) -> Result<Reading> {
    if !(1..=MAX_TRANSACTION_METADATA_BACKENDS).contains(&limit) {
        return Err(invalid());
    }
    let rows = reader.query(&query(meta.version, focus_pid, limit + 1)?, limit + 1).await?;
    if rows.is_empty() || rows.len() > limit + 1 {
        return Err(invalid());
    }
    let at = canonical_time(&text(&rows[0], "observed_at")?)?;
    let raw_slrus = text(&rows[0], "native_slrus")?;
    let slrus = decode_slrus(&raw_slrus, meta.version)?;
    let native_rows = rows.iter().filter(|row| row.text("pid").is_some()).count() as u32;
    let limited = native_rows as usize > limit;
    if ObservationBounds::new(&meta.at, &at).is_none() {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut caches = vec![];
    let mut records = vec![];
    let mut backends = BTreeSet::new();
    let mut sessions = BTreeSet::new();
    for row in rows {
        if row.text("observed_at").as_deref().and_then(|at| canonical_time(at).ok()) != Some(at.clone())
            || text(&row, "native_slrus")? != raw_slrus
        {
            return Err(invalid());
        }
        if row.text("pid").is_none() {
            continue;
        }
        let record = decode_row(row, &meta.database_oid)?;
        let actor = record.facts.clone().ok_or_else(invalid)?;
        if actor.database.as_deref() != Some(meta.database.database.as_str())
            || record.native_text("database_oid").as_deref() != Some(meta.database_oid.as_str())
        {
            return Err(invalid());
        }
        let cache = PostgresSubtransactionCache {
            backend_id: integer(&record.row, "native_backend_id")?.try_into().map_err(|_| invalid())?,
            transaction_started_at: record.native_text("transaction_start"),
            cached_subxids: optional_u32(&record.row, "native_cached_subxids")?,
            overflowed: optional_flag(&record.row, "native_overflowed")?,
            actor,
        };
        if !cache.is_valid()
            || !backends.insert(cache.backend_id)
            || !sessions.insert(cache.actor.key.session.clone())
            || (cache.actor.key.session.id != focus_pid.to_string() && cache.overflowed != Some(true))
        {
            return Err(invalid());
        }
        if records.len() < limit {
            caches.push(cache);
            records.push(record);
        }
    }
    let mut frame = ActivityFrame { at, records, locks: vec![] };
    classify(&mut frame);
    synchronize(&frame, &mut caches)?;
    Ok(Reading { frame, caches, slrus, native_rows, limited })
}
pub(crate) fn synchronize(frame: &ActivityFrame, caches: &mut [PostgresSubtransactionCache]) -> Result<()> {
    if frame.records.len() != caches.len() {
        return Err(invalid());
    }
    for (record, cache) in frame.records.iter().zip(caches) {
        let actor = record.facts.as_ref().ok_or_else(invalid)?;
        if actor.key != cache.actor.key {
            return Err(invalid());
        }
        cache.actor = actor.clone();
    }
    Ok(())
}
fn optional_u32(row: &Row, name: &str) -> Result<Option<u32>> {
    row.unsigned_text(name)?
        .map(|raw| raw.parse::<u32>().ok().filter(|value| *value <= i32::MAX as u32).ok_or_else(invalid))
        .transpose()
}

fn optional_flag(row: &Row, name: &str) -> Result<Option<bool>> {
    match row.value(name) {
        Some(None) => Ok(None),
        Some(Some(_)) => flag(row, name).map(Some),
        None => Err(invalid()),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SlruRow {
    native_name: String,
    observed_at: String,
    #[serde(deserialize_with = "Option::deserialize")]
    stats_reset: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    blocks_zeroed: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    blocks_hit: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    blocks_read: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    blocks_written: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    blocks_exists: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    flushes: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    truncates: Option<String>,
}
fn decode_slrus(raw: &str, version: u32) -> Result<Vec<PostgresSlruCounters>> {
    let values: Vec<SlruRow> = serde_json::from_str(raw).map_err(|_| invalid())?;
    if values.len() > 3 {
        return Err(invalid());
    }
    let names = slru_names(version);
    let mut kinds = BTreeSet::new();
    values
        .into_iter()
        .map(|row| {
            let kind = match names.iter().position(|name| *name == row.native_name) {
                Some(0) => PostgresSlruKind::Subtransaction,
                Some(1) => PostgresSlruKind::MultiXactMember,
                Some(2) => PostgresSlruKind::MultiXactOffset,
                _ => return Err(invalid()),
            };
            let point = PostgresSlruCounters {
                kind,
                native_name: row.native_name,
                observed_at: canonical_time(&row.observed_at)?,
                stats_reset: row.stats_reset,
                blocks_zeroed: row.blocks_zeroed,
                blocks_hit: row.blocks_hit,
                blocks_read: row.blocks_read,
                blocks_written: row.blocks_written,
                blocks_exists: row.blocks_exists,
                flushes: row.flushes,
                truncates: row.truncates,
            };
            if !point.is_valid() || !kinds.insert(kind) {
                return Err(invalid());
            }
            Ok(point)
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;
