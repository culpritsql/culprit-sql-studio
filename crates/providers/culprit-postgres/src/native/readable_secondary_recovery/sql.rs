//! Bounded statistics and native startup points. PostgreSQL startup is not a pg_blocking_pids backend.
//! The existing lock projection can instead retain an actual queued relation lock and a granted reader lock.
use super::*;
pub(super) fn query(meta: &Metadata, focus: &ActivityRequestFocus) -> Result<String> {
    if meta.version / 10000 != 18 || !meta.standby || !focus.is_readable_secondary_recovery_focus() {
        return Err(invalid());
    }
    let database = meta
        .database_oid
        .parse::<u32>()
        .ok()
        .filter(|id| *id > 0 && id.to_string() == meta.database_oid)
        .ok_or_else(invalid)?;
    let replay = replication::selected_recovery(meta, "(SELECT at_text FROM stamp)")?;
    let process_cap = MAX_RECOVERY_PROCESSES + 1;
    let (lock_ctes, lock_columns) = activity::recovery_lock_projection(focus)?;
    // Both database views fetch the same PgStat_StatDBEntry in cache/snapshot mode. Its reset sets
    // the complete entry and stat_reset_timestamp together; unknown/none modes withhold deltas.
    Ok(format!(
        r#"WITH stamp AS MATERIALIZED(SELECT {at} AS at_text), startup AS MATERIALIZED(SELECT * FROM pg_catalog.pg_stat_activity WHERE backend_type='startup' AND pid<>pg_catalog.pg_backend_pid() LIMIT {process_cap}) {lock_ctes}
    SELECT 'pg_stat_database_conflicts'::text AS native_source,c.datid::text AS native_id,
        (pg_catalog.to_jsonb(c)||pg_catalog.jsonb_build_object('stats_reset',{reset},'observed_at',(SELECT at_text FROM stamp),
            'postmaster_started_at',{started},'stats_fetch_consistency',pg_catalog.current_setting('stats_fetch_consistency')))::text AS native_values
        FROM pg_catalog.pg_stat_database_conflicts c JOIN pg_catalog.pg_stat_database d ON d.datid=c.datid WHERE c.datid={database}
    UNION ALL {replay}
    UNION ALL SELECT 'pg_recovery_process'::text AS native_source,a.pid::text AS native_id,
        pg_catalog.jsonb_build_object('pid',a.pid::text,'started_at',{process_start},'database_oid',a.datid::text,
            'wait_type',a.wait_event_type,'wait_event',a.wait_event,{lock_columns})::text
        FROM startup a"#,
        at = iso("pg_catalog.clock_timestamp()"),
        reset = iso("d.stats_reset"),
        started = iso("pg_catalog.pg_postmaster_start_time()"),
        process_start = iso("a.backend_start")
    ))
}
