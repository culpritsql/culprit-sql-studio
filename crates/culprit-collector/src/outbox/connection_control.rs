//! One bounded collector observation per real baseline attempt; no driver, retry loop or native availability claim.
use super::*;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

mod writer;
pub(super) use writer::{archived, complete_output, preidentity_expiry};
#[cfg(test)]
mod tests;

const REQUEST_BYTES: usize = 4096;
const METADATA_BYTES: usize = MAX_CONNECTION_CONTROL_BYTES + REQUEST_BYTES;
const OUTPUT_ALLOWANCE: u64 = MAX_CONNECTION_CONTROL_BYTES as u64 + CAPTURE_ALLOWANCE + REQUEST_BYTES as u64;
const PENDING_ALLOWANCE: u64 = OUTPUT_ALLOWANCE + METADATA_BYTES as u64;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registration {
    target: ConfiguredConnectionIdentity,
    started_at: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    outcome: CollectorCaptureOutcome,
    elapsed_ms: f64,
}
impl Completion {
    pub(super) fn new(outcome: CollectorCaptureOutcome, elapsed_ms: f64) -> Result<Self> {
        if outcome == CollectorCaptureOutcome::Unknown || !elapsed_ms.is_finite() || elapsed_ms < 0.0 {
            return Err(integrity());
        }
        Ok(Self { outcome, elapsed_ms })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    registration: Registration,
    completion: Option<Completion>,
    snapshot: Option<CollectorConnectionObservation>,
    output: Option<CaptureIdentity>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    output: CaptureIdentity,
    snapshot_sha256: String,
    archive: Option<ArchiveReceipt>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveReceipt {
    range_id: String,
    manifest_sha256: String,
}

pub(super) fn registration(
    namespace: &str,
    revision: &str,
    name: &str,
    engine: culprit_core::Engine,
) -> Result<Registration> {
    Ok(Registration {
        target: ConfiguredConnectionIdentity::new(namespace, revision, name, engine)?,
        started_at: chrono::Utc::now().to_rfc3339(),
    })
}
pub(super) fn enrolled(registration: &Registration) -> Result<String> {
    encode(&Pending { registration: registration.clone(), completion: None, snapshot: None, output: None })
}
fn encode(value: &impl Serialize) -> Result<String> {
    let mut buffer = durable_fs::LimitedBuffer::new(METADATA_BYTES);
    serde_json::to_writer(&mut buffer, value)
        .map_err(|_| if buffer.exceeded() { problem(OutboxProblem::Capacity) } else { integrity() })?;
    String::from_utf8(buffer.into_bytes()).map_err(|_| integrity())
}
fn read_pending(conn: &Connection, id: &str) -> Result<Option<Pending>> {
    let encoded: Option<String> = conn
        .query_row("SELECT connection_pending_json FROM captures WHERE observation_id=?1", [id], |row| row.get(0))
        .map_err(storage)?;
    encoded
        .map(|value| {
            if value.len() > METADATA_BYTES {
                return Err(integrity());
            }
            let pending: Pending = journal::decode(&value)?;
            if !pending.registration.target.is_valid()
                || canonical_time(&pending.registration.started_at).is_err()
                || pending.completion.as_ref().is_some_and(|completion| {
                    !completion.elapsed_ms.is_finite()
                        || completion.elapsed_ms < 0.0
                        || completion.outcome == CollectorCaptureOutcome::Unknown
                })
                || pending
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.is_valid() || snapshot.original.capture.observation_id != id)
                || pending.output.as_ref().is_some_and(|output| !output.is_valid() || pending.snapshot.is_none())
            {
                return Err(integrity());
            }
            Ok(pending)
        })
        .transpose()
}
fn save_pending(conn: &Connection, id: &str, pending: &Pending) -> Result<()> {
    if conn.execute("UPDATE captures SET connection_pending_json=?1 WHERE observation_id=?2 AND connection_result_json IS NULL AND connection_pending_json IS NOT NULL", params![encode(pending)?, id]).map_err(storage)? != 1 {
        return Err(DeliveryProblem::Conflict.error());
    }
    Ok(())
}
pub(super) fn complete_attempt(conn: &Connection, capture: &journal::Capture, completion: &Completion) -> Result<()> {
    let Some(mut pending) = read_pending(conn, &capture.identity.observation_id)? else { return Ok(()) };
    if capture.state != OutboxCaptureState::Capturing || pending.snapshot.is_some() || pending.completion.is_some() {
        return Err(DeliveryProblem::Conflict.error());
    }
    pending.completion = Some(completion.clone());
    save_pending(conn, &capture.identity.observation_id, &pending)
}
/// Called by the SAME terminal transaction. Subsequent retries cannot regenerate its body or clocks.
pub(super) fn seal(
    tx: &Transaction<'_>,
    capture: &journal::Capture,
    state: OutboxCaptureState,
    original: Option<(&CollectionManifest, &str, bool)>,
) -> Result<()> {
    let Some(mut pending) = read_pending(tx, &capture.identity.observation_id)? else { return Ok(()) };
    if let Some(snapshot) = &pending.snapshot {
        return if snapshot.original.capture == capture.identity { Ok(()) } else { Err(integrity()) };
    }
    let original = original.filter(|(manifest, _, _)| manifest.finished_at.is_some());
    let (outcome, elapsed) = pending
        .completion
        .as_ref()
        .map(|completion| (completion.outcome, Some(completion.elapsed_ms)))
        .unwrap_or((CollectorCaptureOutcome::Unknown, None));
    let native = if state == OutboxCaptureState::Ready {
        original
            .and_then(|(manifest, _, _)| manifest.description.as_ref())
            .map(CollectorConnectionNativeLink::from_original)
            .transpose()?
            .flatten()
    } else {
        None
    };
    let snapshot = CollectorConnectionObservation {
        original: DiagnosticAttemptReceipt {
            capture: capture.identity.clone(),
            manifest_sha256: original.map(|(_, sha, _)| sha.into()),
        },
        original_state: state,
        original_status: original.map(|(manifest, _, _)| manifest.status.clone()),
        original_evidence: match original {
            None => CollectorConnectionOriginalEvidence::ManifestUnavailable,
            Some((_, _, true)) => CollectorConnectionOriginalEvidence::NoNativeEvidence,
            Some(_) => CollectorConnectionOriginalEvidence::MayContainNativeEvidence,
        },
        source_work: capture.source_work,
        collector_outcome: outcome,
        collector_started_at: pending.registration.started_at.clone(),
        collector_finished_at: chrono::Utc::now().to_rfc3339(),
        collector_elapsed_ms: elapsed,
        connections: original
            .and_then(|(manifest, _, _)| manifest.read_metrics.as_ref())
            .and_then(|metrics| metrics.connection_work.as_deref())
            .cloned(),
        original_source_queries: original.map(|(manifest, _, _)| manifest.source_queries),
        native,
    };
    if !snapshot.is_valid() {
        return Err(integrity());
    }
    if snapshot.original_evidence == CollectorConnectionOriginalEvidence::NoNativeEvidence
        && matches!(snapshot.source_work, SourceWorkState::NotStarted | SourceWorkState::Quiescent)
    {
        tx.execute(
            "UPDATE captures SET connection_empty_original=1 WHERE observation_id=?1",
            [&capture.identity.observation_id],
        )
        .map_err(storage)?;
    }
    pending.snapshot = Some(snapshot);
    save_pending(tx, &capture.identity.observation_id, &pending)
}

pub(super) fn reserve_allowance(
    conn: &Connection,
    request: &CollectionRequest,
    registration: Option<&Registration>,
    count: u32,
    used: u64,
    charge: u64,
    limits: &OutboxLimits,
) -> Result<()> {
    let mut pending: u32 = conn
        .query_row("SELECT count(*) FROM captures WHERE connection_pending_json IS NOT NULL", [], |row| row.get(0))
        .map_err(storage)?;
    if let CollectionSelection::CollectorConnection { attempt } = &request.selection {
        let control = read_pending(conn, &attempt.observation_id)?.ok_or_else(integrity)?;
        if control.snapshot.as_ref().is_none_or(|snapshot| snapshot.original.capture != *attempt)
            || *request != writer::request(&control)?
            || registration.is_some()
            || journal::encode(request)?.len() > REQUEST_BYTES
        {
            return Err(integrity());
        }
        pending = pending.checked_sub(1).ok_or_else(integrity)?;
    }
    if registration.is_some() {
        pending = pending.checked_add(1).ok_or_else(integrity)?;
    }
    let headroom = u64::from(pending).checked_mul(PENDING_ALLOWANCE).ok_or_else(integrity)?;
    if count.checked_add(1).and_then(|value| value.checked_add(pending)).is_none_or(|value| value > limits.max_captures)
        || used
            .checked_add(charge)
            .and_then(|bytes| bytes.checked_add(headroom))
            .is_none_or(|bytes| bytes > limits.max_bytes)
    {
        return Err(problem(OutboxProblem::Capacity));
    }
    Ok(())
}
pub(super) fn pin(tx: &Transaction<'_>, id: &str) -> Result<()> {
    tx.execute("INSERT INTO diagnostic_pins VALUES(?1,?2)", params![format!("connection-control:{id}"), id])
        .map_err(storage)?;
    Ok(())
}
pub(super) fn charged_bytes(conn: &Connection) -> Result<u64> {
    let bytes: i64 = conn.query_row("SELECT coalesce(sum(length(CAST(coalesce(connection_pending_json,'') AS BLOB))+length(CAST(coalesce(connection_result_json,'') AS BLOB))),0) FROM captures", [], |row| row.get(0)).map_err(storage)?;
    journal::bytes_from_sql(bytes)
}
fn integrity() -> culprit_core::Error {
    problem(OutboxProblem::Integrity)
}

/// A missing file/listing error never proves zero evidence. Only a terminal undescribed manifest,
/// known source settlement and its exact bounded empty inventory authorize control-backed expiry.
/// A charged acquisition attempt is not a dispatched statement; its complete original proof is required.
pub(super) fn no_native_evidence(
    directory: &Path,
    manifest: &CollectionManifest,
    source_work: SourceWorkState,
) -> bool {
    if manifest.finished_at.is_none()
        || manifest.description.is_some()
        || !matches!(source_work, SourceWorkState::NotStarted | SourceWorkState::Quiescent)
        || manifest.files.iter().any(|file| file.rows != 0 || file.bytes != 0 || file.pages != 0)
    {
        return false;
    }
    let no_charged_work = manifest.source_queries == 0
        && manifest.read_metrics.as_ref().is_none_or(|metrics| {
            metrics.is_valid()
                && metrics.active_tasks == 0
                && metrics.untracked_tasks == 0
                && !metrics.has_unconfirmed_work()
                && metrics.templates.is_empty()
                && metrics.native_session_work.is_none()
        });
    let failed_acquisition = manifest
        .read_metrics
        .as_ref()
        .is_some_and(|metrics| metrics.proves_pre_dispatch_connection_failure(source_work));
    if !no_charged_work && !failed_acquisition {
        return false;
    }
    let mut expected = manifest.files.iter().map(|file| file.file.as_str()).collect::<std::collections::BTreeSet<_>>();
    if expected.len() != manifest.files.len() {
        return false;
    }
    expected.insert("manifest.json");
    let Ok(entries) = fs::read_dir(directory) else { return false };
    let mut seen = 0;
    for entry in entries.take(expected.len() + 1) {
        let Ok(entry) = entry else { return false };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { return false };
        if !expected.contains(name)
            || !entry.file_type().is_ok_and(|kind| kind.is_file())
            || (name != "manifest.json" && !entry.metadata().is_ok_and(|metadata| metadata.len() == 0))
        {
            return false;
        }
        seen += 1;
    }
    seen == expected.len()
}

pub(super) fn allocated_output(
    tx: &Transaction<'_>,
    request: &CollectionRequest,
    output: &CaptureIdentity,
) -> Result<()> {
    let CollectionSelection::CollectorConnection { attempt } = &request.selection else { return Ok(()) };
    let mut pending = read_pending(tx, &attempt.observation_id)?.ok_or_else(integrity)?;
    if pending.output.is_some()
        || pending.snapshot.as_ref().is_none_or(|snapshot| snapshot.original.capture != *attempt)
    {
        return Err(integrity());
    }
    pending.output = Some(output.clone());
    save_pending(tx, &attempt.observation_id, &pending)
}

pub(super) fn is_observed(conn: &Connection, id: &str) -> Result<bool> {
    read_pending(conn, id).map(|pending| pending.is_some())
}
