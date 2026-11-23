//! A selected readonly request sees local recovery facts through existing native statistics/projection owners.
use super::metadata::Metadata;
use super::*;
use culprit_core::telemetry::*;
mod sql;

pub(crate) async fn read<R: crate::read::QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus: &ActivityRequestFocus,
) -> Result<Box<PostgresRecoveryEndpoint>> {
    let rows = reader.query(&sql::query(meta, focus)?, MAX_RECOVERY_PROCESSES + 3).await?;
    let mut counters = None;
    let mut replay = None;
    let mut processes = Vec::new();
    let mut at = None;
    let mut started = None;
    let mut mode = None;
    for row in rows {
        match text(&row, "native_source")?.as_str() {
            "pg_stat_database_conflicts" => {
                if counters.is_some() || text(&row, "native_id")? != meta.database_oid {
                    return Err(invalid());
                }
                let value: serde_json::Value =
                    serde_json::from_str(&text(&row, "native_values")?).map_err(|_| invalid())?;
                // Native oid values in to_jsonb(composite) are strings, not JSON numbers.
                if json_oid(&value, "datid")? != meta.database_oid.as_str() {
                    return Err(invalid());
                }
                let scalar =
                    |name| value.get(name).and_then(serde_json::Value::as_str).map(str::to_owned).ok_or_else(invalid);
                at = Some(canonical_time(&scalar("observed_at")?)?);
                started = Some(scalar("postmaster_started_at")?);
                mode = Some(scalar("stats_fetch_consistency")?);
                let record = operational::decode_values(
                    "pg_stat_database_conflicts".into(),
                    value,
                    operational::CounterReadContext {
                        database_oid: &meta.database_oid,
                        started_at: &meta.started_at,
                        block_bytes: meta.block_bytes,
                        version: meta.version,
                    },
                )?;
                let TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::CounterSet(set), ..
                }) = record
                else {
                    return Err(invalid());
                };
                counters = Some(*set);
            }
            "pg_recovery" => {
                if replay.is_some() {
                    return Err(invalid());
                }
                let observation = replication::decode(&row)?;
                let ReplicationFact::Native(point) = observation.fact else { return Err(invalid()) };
                replay = Some(point);
            }
            "pg_recovery_process" => {
                if text(&row, "native_id")?
                    != text(&row, "native_values")?
                        .parse::<serde_json::Value>()
                        .map_err(|_| invalid())?
                        .get("pid")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(invalid)?
                {
                    return Err(invalid());
                }
                processes.push(process(&row)?);
            }
            _ => return Err(invalid()),
        }
    }
    let limited = processes.len() > MAX_RECOVERY_PROCESSES;
    if processes.len() > MAX_RECOVERY_PROCESSES + 1 {
        return Err(invalid());
    }
    processes.truncate(MAX_RECOVERY_PROCESSES);
    let point = Box::new(PostgresRecoveryEndpoint {
        observed_at: at.ok_or_else(invalid)?,
        database_oid: meta.database_oid.clone(),
        postmaster_started_at: started.ok_or_else(invalid)?,
        stats_fetch_consistency: mode.ok_or_else(invalid)?,
        conflicts: counters.ok_or_else(invalid)?,
        replay: replay.ok_or_else(invalid)?,
        processes,
        processes_limited: limited,
    });
    if !point.is_valid()
        || point.native_epoch().as_deref() != Some(meta.activity_epoch().as_str())
        || ObservationBounds::new(&meta.at, &point.observed_at).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(point)
}
fn process(row: &Row) -> Result<PostgresRecoveryProcess> {
    let raw: Process = serde_json::from_str(&text(row, "native_values")?).map_err(|_| invalid())?;
    let point = PostgresRecoveryProcess {
        pid: raw.pid,
        started_at: raw.started_at.map(native_generation).transpose()?,
        database_oid: raw.database_oid,
        wait_type: raw.wait_type,
        wait_event: raw.wait_event,
        lock_snapshot_rows: raw.lock_snapshot_rows,
        edges_limited: raw.edges_limited,
        relation_edges: raw
            .relation_edges
            .into_iter()
            .map(|edge| -> Result<_> {
                Ok(PostgresRecoveryRelationEdge {
                    resource: culprit_core::blocking::PostgresRelationLockTag {
                        database_oid: edge.database_oid,
                        relation_oid: edge.relation_oid,
                    },
                    requested_mode: culprit_core::blocking::PostgresRelationLockMode::from_native(&edge.requested_mode),
                    reader: NativeActivityIdentity {
                        id: edge.reader_pid,
                        generation: native_generation(edge.reader_started_at)?,
                    },
                    reader_single_backend: edge.reader_single_backend,
                    granted_modes: edge
                        .granted_modes
                        .iter()
                        .map(|mode| culprit_core::blocking::PostgresRelationLockMode::from_native(mode))
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    if !point.is_valid() {
        return Err(invalid());
    }
    Ok(point)
}
// Activity's native generation strings are identities. Validate their UTC spelling, without rebinding
// an original six-digit generation to a newly formatted nine-digit identity.
fn native_generation(value: String) -> Result<String> {
    canonical_time(&value)?;
    Ok(value)
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Process {
    pid: String,
    #[serde(deserialize_with = "Option::deserialize")]
    started_at: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    database_oid: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    wait_type: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    wait_event: Option<String>,
    lock_snapshot_rows: u32,
    edges_limited: bool,
    relation_edges: Vec<Edge>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Edge {
    database_oid: String,
    relation_oid: String,
    requested_mode: String,
    reader_pid: String,
    reader_started_at: String,
    reader_single_backend: bool,
    granted_modes: Vec<String>,
}
#[cfg(test)]
pub(crate) mod tests;
