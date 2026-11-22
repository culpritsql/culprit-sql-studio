//! Filtered/capped retention is not a bound on pg_locks or pg_blocking_pids internal lock-manager work.
use super::*;
pub(super) fn query(frame: &ActivityFrame, by_pid: &BTreeMap<u32, usize>, visited: &BTreeSet<usize>) -> Result<String> {
    if by_pid.len() > MAX_BLOCKING_PARTICIPANTS as usize || visited.len() > MAX_BLOCKING_LOOKUPS as usize {
        return Err(CollectionStop::RowLimit.error());
    }
    let targets = by_pid
        .values()
        .map(|index| target(&frame.records[*index], visited.contains(index)))
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let limit = MAX_FOCUSED_LOCK_SNAPSHOT_ROWS;
    let lookahead = limit + 1;
    let owners = MAX_BLOCKING_LOOKUPS;
    let actor_limit = MAX_BLOCKING_PARTICIPANTS;
    let actor_lookahead = actor_limit + 1;
    let at = iso("stamp.at");
    Ok(format!("WITH targets(pid,backend_start,query_start,xact_start,active,selected) AS (VALUES {targets}),
relevant_activity AS MATERIALIZED (
 SELECT a.pid,a.leader_pid,a.backend_start,a.query_start,a.xact_start,a.state,a.wait_event_type
 FROM pg_catalog.pg_stat_activity a WHERE a.pid IN (SELECT pid FROM targets) OR a.leader_pid IN (SELECT pid FROM targets)
 LIMIT {actor_lookahead}),
actor_size AS (SELECT count(*) AS rows FROM relevant_activity),
current_actors AS MATERIALIZED (
 SELECT a.pid,a.wait_event_type,(a.leader_pid IS NULL AND actor_size.rows<={actor_limit}
 AND NOT EXISTS(SELECT 1 FROM relevant_activity worker WHERE worker.leader_pid=a.pid)) AS single_backend
 FROM targets t JOIN relevant_activity a ON a.pid=t.pid AND a.backend_start=t.backend_start
 AND a.query_start IS NOT DISTINCT FROM t.query_start AND a.xact_start IS NOT DISTINCT FROM t.xact_start
 AND (a.state IN ('active','fastpath function call'))=t.active CROSS JOIN actor_size),
current_waiters AS MATERIALIZED (SELECT a.* FROM current_actors a JOIN targets t USING(pid) WHERE t.selected AND a.wait_event_type='Lock'),
native_edges AS MATERIALIZED (SELECT pid,pg_catalog.pg_blocking_pids(pid) AS owners FROM current_waiters),
owner_ids AS MATERIALIZED (SELECT DISTINCT e.pid AS waiter,unnest(e.owners) AS owner FROM native_edges e),
lock_snapshot AS MATERIALIZED (
 SELECT l.pid,l.locktype,l.database,l.relation,l.mode,l.granted,l.waitstart FROM pg_catalog.pg_locks l
 WHERE l.pid IN (SELECT pid FROM current_actors) LIMIT {lookahead}),
snapshot_size AS (SELECT count(*) AS rows FROM lock_snapshot),
retained_locks AS MATERIALIZED (SELECT * FROM lock_snapshot LIMIT {limit}),
waits AS MATERIALIZED (SELECT l.* FROM retained_locks l WHERE NOT l.granted AND l.pid IN (SELECT pid FROM current_waiters)),
lock_modes AS MATERIALIZED (
 SELECT pid,database,relation,array_agg(DISTINCT mode ORDER BY mode) FILTER(WHERE granted) AS granted,
 array_agg(DISTINCT mode ORDER BY mode) FILTER(WHERE NOT granted) AS waiting
 FROM retained_locks WHERE locktype='relation' GROUP BY pid,database,relation),
stamp AS (SELECT pg_catalog.clock_timestamp() AS at)
SELECT t.pid::text AS pid,{at} AS observed_at,(a.pid IS NOT NULL)::text AS waiter_matches,
 snapshot_size.rows::text AS snapshot_rows,actor_size.rows::text AS actor_rows,COALESCE(a.single_backend,false)::text AS single_backend,((SELECT count(*) FROM owner_ids o WHERE o.waiter=t.pid)<={owners})::text AS blockers_complete,
 w.mode,w.locktype,w.database::text AS database_oid,w.relation::text AS relation_oid,
 CASE WHEN stamp.at>=w.waitstart THEN floor(extract(epoch FROM stamp.at-w.waitstart)*1000)::text END AS elapsed_ms,
 n.nspname||'.'||o.relname AS object,CASE WHEN c.relkind IN ('i','I') THEN c.relname END AS index_name,
 COALESCE((SELECT jsonb_agg(jsonb_build_object('pid',x.owner::text,'actor_matches',oa.pid IS NOT NULL,'single_backend',COALESCE(oa.single_backend,false),
 'granted',COALESCE(to_jsonb(m.granted),'[]'::jsonb),'waiting',COALESCE(to_jsonb(m.waiting),'[]'::jsonb)) ORDER BY x.owner)
 FROM (SELECT owner FROM owner_ids WHERE waiter=t.pid ORDER BY owner LIMIT {owners}) x
 LEFT JOIN current_actors oa ON oa.pid=x.owner
 LEFT JOIN lock_modes m ON m.pid=x.owner AND m.database=w.database AND m.relation=w.relation),'[]'::jsonb)::text AS owners
FROM targets t CROSS JOIN stamp CROSS JOIN snapshot_size CROSS JOIN actor_size
LEFT JOIN current_actors a ON a.pid=t.pid LEFT JOIN waits w ON w.pid=t.pid
LEFT JOIN pg_catalog.pg_class c ON c.oid=w.relation AND (w.database=0 OR w.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()))
LEFT JOIN pg_catalog.pg_index ix ON ix.indexrelid=c.oid LEFT JOIN pg_catalog.pg_class o ON o.oid=COALESCE(ix.indrelid,c.oid)
LEFT JOIN pg_catalog.pg_namespace n ON n.oid=o.relnamespace
WHERE t.selected ORDER BY t.pid"))
}
fn target(record: &ActivityRecord, selected: bool) -> Result<String> {
    let facts = record.facts.as_ref().ok_or_else(invalid)?;
    let pid = pid(&record.row)?;
    let started = canonical_time(&facts.key.session.generation)?;
    let clock = |name: &str| -> Result<String> {
        record
            .native_text(name)
            .map(|raw| canonical_time(&raw).map(|at| format!("'{at}'::timestamptz")))
            .transpose()
            .map(|at| at.unwrap_or_else(|| "NULL::timestamptz".into()))
    };
    let query = clock("query_start")?;
    let transaction = clock("transaction_start")?;
    let active = !facts.idle;
    Ok(format!("({pid},'{started}'::timestamptz,{query},{transaction},{active},{selected})"))
}
