//! Queue metadata shares the canonical Ready transaction and the existing original-pin owner.
use super::*;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub(in crate::outbox) const SCHEMA: &str = "
CREATE TABLE synthesis_configuration(id INTEGER PRIMARY KEY CHECK(id=1),policy_key TEXT NOT NULL,registration_json TEXT NOT NULL,backfill_epoch INTEGER NOT NULL,backfill_sequence INTEGER NOT NULL);
CREATE TABLE synthesis_jobs(primary_id TEXT PRIMARY KEY,policy_key TEXT NOT NULL,primary_json TEXT NOT NULL,plan_json TEXT,queued_at TEXT NOT NULL);
CREATE TABLE synthesis_failure_watermark(id INTEGER PRIMARY KEY CHECK(id=1),total INTEGER NOT NULL CHECK(total>0),latest_json TEXT NOT NULL);
CREATE TABLE synthesis_processed(primary_id TEXT PRIMARY KEY,policy_key TEXT NOT NULL,result_json TEXT NOT NULL);
CREATE TABLE synthesis_context_heads(head_key TEXT PRIMARY KEY,scope_key TEXT NOT NULL,family_key TEXT NOT NULL,observation_id TEXT NOT NULL,capture_json TEXT NOT NULL,source_json TEXT NOT NULL,collected_from TEXT NOT NULL,collected_until TEXT NOT NULL,epoch INTEGER NOT NULL,sequence INTEGER NOT NULL);
CREATE INDEX synthesis_head_scope ON synthesis_context_heads(scope_key,family_key,epoch DESC,sequence DESC);
";

#[derive(Clone)]
pub(super) struct Job {
    pub primary: DiagnosticCaptureRef,
    pub policy: String,
    pub plan: Option<ContextPlan>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "kebab-case", deny_unknown_fields)]
enum Terminal {
    Committed { output: DiagnosticCaptureRef },
    Failed { code: culprit_core::ErrorCode },
}

pub(super) fn registration(conn: &Connection) -> Result<Option<SynthesisRegistration>> {
    let encoded: Option<(String, String)> = conn
        .query_row("SELECT policy_key,registration_json FROM synthesis_configuration WHERE id=1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(storage)?;
    encoded
        .map(|(key, value)| {
            let value: SynthesisRegistration = capture_journal::decode(&value)?;
            if value.key()? != key {
                return Err(integrity());
            }
            Ok(value)
        })
        .transpose()
}

pub(super) fn install(conn: &mut Connection, registration: &SynthesisRegistration) -> Result<()> {
    registration.validate()?;
    let key = registration.key()?;
    let tx = conn.transaction().map_err(storage)?;
    if let Some(previous) = self::registration(&tx)? {
        if previous == *registration {
            tx.commit().map_err(storage)?;
            return Ok(());
        }
        if pending_count(&tx)? != 0 {
            return Err(DeliveryProblem::Conflict.error());
        }
    }
    tx.execute("INSERT INTO synthesis_configuration VALUES(1,?1,?2,0,0) ON CONFLICT(id) DO UPDATE SET policy_key=excluded.policy_key,registration_json=excluded.registration_json,backfill_epoch=0,backfill_sequence=0", params![key, capture_journal::encode(registration)?]).map_err(storage)?;
    enforce_metadata_bound(&tx)?;
    tx.commit().map_err(storage)
}

pub(super) fn pending_count(conn: &Connection) -> Result<u32> {
    conn.query_row("SELECT count(*) FROM synthesis_jobs", [], |row| row.get(0)).map_err(storage)
}

pub(super) fn eligible(collection: TelemetryCollection) -> bool {
    collection != TelemetryCollection::CollectorConnections
        && collection != TelemetryCollection::DerivedDiagnosis
        && collection.streams().iter().any(|stream| stream.delivery_required())
}

pub(in crate::outbox) fn reserve_allowance(
    conn: &Connection,
    request: &CollectionRequest,
    count: u32,
    used: u64,
    charge: u64,
    limits: &OutboxLimits,
) -> Result<()> {
    if registration(conn)?.is_none()
        || matches!(
            request.collection(),
            TelemetryCollection::DerivedDiagnosis | TelemetryCollection::CollectorConnections
        )
    {
        return Ok(());
    }
    // Every admitted input leaves one real output reservation and its canonical artifact allowance free.
    let headroom =
        MAX_DERIVED_OUTPUT_BYTES as u64 + super::super::CAPTURE_ALLOWANCE + super::super::MAX_REQUEST_BYTES as u64;
    if count.checked_add(2).is_none_or(|count| count > limits.max_captures)
        || used
            .checked_add(charge)
            .and_then(|bytes| bytes.checked_add(headroom))
            .is_none_or(|bytes| bytes > limits.max_bytes)
        || eligible(request.collection()) && pending_count(conn)? >= MAX_PENDING_SYNTHESIS_JOBS
        || charged_bytes(conn)?
            .checked_add(MAX_SYNTHESIS_PLAN_BYTES as u64)
            .is_none_or(|bytes| bytes > MAX_SYNTHESIS_METADATA_BYTES)
    {
        return Err(problem(OutboxProblem::Capacity));
    }
    Ok(())
}

fn pin_owner(id: &str) -> String {
    format!("synthesis:job:{id}")
}
pub(super) fn pin(tx: &Transaction<'_>, primary_id: &str, original_id: &str) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO diagnostic_pins(owner,observation_id) VALUES(?1,?2)",
        params![pin_owner(primary_id), original_id],
    )
    .map_err(storage)?;
    Ok(())
}
fn enroll(tx: &Transaction<'_>, policy: &str, original: &DiagnosticCaptureRef) -> Result<()> {
    if !original.is_valid() {
        return Err(integrity());
    }
    let id = &original.capture.observation_id;
    let terminal: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM synthesis_processed WHERE primary_id=?1 AND policy_key=?2)",
            params![id, policy],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if terminal {
        return Ok(());
    }
    let existing: Option<(String, String)> = tx
        .query_row("SELECT policy_key,primary_json FROM synthesis_jobs WHERE primary_id=?1", [id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(storage)?;
    if let Some((before_policy, before)) = existing {
        if before_policy != policy || capture_journal::decode::<DiagnosticCaptureRef>(&before)? != *original {
            return Err(integrity());
        }
        return Ok(());
    }
    if pending_count(tx)? >= MAX_PENDING_SYNTHESIS_JOBS {
        return Err(problem(OutboxProblem::Capacity));
    }
    tx.execute(
        "INSERT INTO synthesis_jobs VALUES(?1,?2,?3,NULL,?4)",
        params![id, policy, capture_journal::encode(original)?, canonical_time(&chrono::Utc::now().to_rfc3339())?],
    )
    .map_err(storage)?;
    pin(tx, id, id)
}

pub(in crate::outbox) fn committed(
    tx: &Transaction<'_>,
    capture: &capture_journal::Capture,
    validated: &super::super::artifacts::ValidatedCapture,
) -> Result<()> {
    let Some(registration) = registration(tx)? else { return Ok(()) };
    if !eligible(capture.request.collection()) {
        return Ok(());
    }
    let source = validated.manifest.description.as_ref().ok_or_else(integrity)?;
    let original = DiagnosticCaptureRef {
        capture: capture.identity.clone(),
        manifest_sha256: validated.manifest_sha.clone(),
        source_scope_id: source.captured_range()?.scope_id,
    };
    enroll(tx, &registration.key()?, &original)?;
    catalog::retain(
        tx,
        &original,
        source,
        &validated.manifest.started_at,
        validated.manifest.finished_at.as_deref().ok_or_else(integrity)?,
    )?;
    points::retain(
        tx,
        &original,
        source,
        &validated.manifest.started_at,
        validated.manifest.finished_at.as_deref().ok_or_else(integrity)?,
    )?;
    enforce_metadata_bound(tx)
}

/// Old retained originals are visited in bounded sequence pages, not an unbounded history scan.
/// New Ready transactions enroll independently, so advancing this cursor cannot hide newly committed work.
pub(super) fn backfill(
    conn: &mut Connection,
    expected: &SynthesisRegistration,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    check()?;
    if registration(conn)?.as_ref() != Some(expected) {
        return Err(DeliveryProblem::Conflict.error());
    }
    if pending_count(conn)? > 0
        && charged_bytes(conn)?
            .checked_add(MAX_SYNTHESIS_PLAN_BYTES as u64)
            .is_none_or(|bytes| bytes > MAX_SYNTHESIS_METADATA_BYTES)
    {
        return Ok(());
    }
    let tx = conn.transaction().map_err(storage)?;
    let (epoch, sequence): (i64, i64) = tx
        .query_row("SELECT backfill_epoch,backfill_sequence FROM synthesis_configuration WHERE id=1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(storage)?;
    let available = MAX_PENDING_SYNTHESIS_JOBS.saturating_sub(pending_count(&tx)?).min(SYNTHESIS_BACKFILL_PAGE);
    if available == 0 {
        return Ok(());
    }
    let entries = {
        let mut statement = tx.prepare("SELECT observation_id,epoch,sequence,collection,manifest_sha,range_json FROM captures WHERE state=?1 AND (epoch>?2 OR (epoch=?2 AND sequence>?3)) ORDER BY epoch,sequence LIMIT ?4").map_err(storage)?;
        statement
            .query_map(
                params![capture_journal::encode(&OutboxCaptureState::Ready)?, epoch, sequence, available],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .map_err(storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage)?
    };
    for (id, epoch, sequence, collection, manifest, range) in entries {
        check()?;
        if eligible(capture_journal::decode(&collection)?) {
            let capture = capture_identity(&tx, &id)?;
            let range: CapturedRange = capture_journal::decode(&range)?;
            enroll(
                &tx,
                &expected.key()?,
                &DiagnosticCaptureRef { capture, manifest_sha256: manifest, source_scope_id: range.scope_id },
            )?;
        }
        tx.execute(
            "UPDATE synthesis_configuration SET backfill_epoch=?1,backfill_sequence=?2 WHERE id=1",
            params![epoch, sequence],
        )
        .map_err(storage)?;
    }
    enforce_metadata_bound(&tx)?;
    tx.commit().map_err(storage)
}

pub(super) fn next(conn: &Connection, expected: &SynthesisRegistration) -> Result<Option<Job>> {
    let found: Option<(String,String,Option<String>)> = conn.query_row("SELECT j.policy_key,j.primary_json,j.plan_json FROM synthesis_jobs j LEFT JOIN captures c ON c.observation_id=j.primary_id ORDER BY c.epoch,c.sequence LIMIT 1", [], |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional().map_err(storage)?;
    found
        .map(|(policy, encoded, plan)| {
            if policy != expected.key()? {
                return Err(DeliveryProblem::Conflict.error());
            }
            let primary: DiagnosticCaptureRef = capture_journal::decode(&encoded)?;
            if !primary.is_valid() {
                return Err(integrity());
            }
            let plan = plan.map(|value| decode_plan(&value)).transpose()?;
            Ok(Job { primary, policy, plan })
        })
        .transpose()
}

pub(super) fn store_plan(conn: &mut Connection, job: &Job, plan: &ContextPlan) -> Result<()> {
    let encoded = encode_plan(plan)?;
    let tx = conn.transaction().map_err(storage)?;
    for capture in &plan.context {
        pin(&tx, &job.primary.capture.observation_id, &capture.capture.observation_id)?;
    }
    let changed = tx.execute("UPDATE synthesis_jobs SET plan_json=?1 WHERE primary_id=?2 AND policy_key=?3 AND primary_json=?4 AND plan_json IS NULL", params![encoded,job.primary.capture.observation_id,job.policy,capture_journal::encode(&job.primary)?]).map_err(storage)?;
    if changed != 1 {
        return Err(integrity());
    }
    enforce_metadata_bound(&tx)?;
    tx.commit().map_err(storage)
}

pub(super) fn complete(conn: &mut Connection, job: &Job, output: &DiagnosticCaptureRef) -> Result<()> {
    finish(conn, job, &Terminal::Committed { output: output.clone() })
}
pub(super) fn failed(conn: &mut Connection, job: &Job, code: culprit_core::ErrorCode) -> Result<()> {
    finish(conn, job, &Terminal::Failed { code })
}
fn finish(conn: &mut Connection, job: &Job, terminal: &Terminal) -> Result<()> {
    let tx = conn.transaction().map_err(storage)?;
    let changed = tx
        .execute(
            "DELETE FROM synthesis_jobs WHERE primary_id=?1 AND policy_key=?2 AND primary_json=?3",
            params![job.primary.capture.observation_id, job.policy, capture_journal::encode(&job.primary)?],
        )
        .map_err(storage)?;
    if changed != 1 {
        return Err(integrity());
    }
    tx.execute("INSERT INTO synthesis_processed VALUES(?1,?2,?3) ON CONFLICT(primary_id) DO UPDATE SET policy_key=excluded.policy_key,result_json=excluded.result_json", params![job.primary.capture.observation_id,job.policy,capture_journal::encode(terminal)?]).map_err(storage)?;
    if let Terminal::Failed { code } = terminal {
        record_failure(&tx, &job.primary, *code)?;
    }
    tx.execute("DELETE FROM diagnostic_pins WHERE owner=?1", [pin_owner(&job.primary.capture.observation_id)])
        .map_err(storage)?;
    enforce_metadata_bound(&tx)?;
    tx.commit().map_err(storage)
}

pub(in crate::outbox) fn before_remove(tx: &Transaction<'_>, id: &str) -> Result<()> {
    let pending: bool = tx
        .query_row("SELECT EXISTS(SELECT 1 FROM synthesis_jobs WHERE primary_id=?1)", [id], |row| row.get(0))
        .map_err(storage)?;
    if pending {
        return Err(integrity());
    }
    tx.execute("DELETE FROM synthesis_processed WHERE primary_id=?1", [id]).map_err(storage)?;
    // The primary can no longer be locally re-enrolled or replayed. Its finite output idempotence state may retire.
    tx.execute("DELETE FROM synthesized_outputs WHERE primary_observation_id=?1 AND NOT EXISTS(SELECT 1 FROM synthesis_jobs WHERE primary_id=?1) AND NOT EXISTS(SELECT 1 FROM captures c WHERE c.observation_id=synthesized_outputs.observation_id AND c.state=?2)",params![id,capture_journal::encode(&OutboxCaptureState::Capturing)?]).map_err(storage)?;
    heads::remove(tx, id)
}

pub(in crate::outbox) fn charged_bytes(conn: &Connection) -> Result<u64> {
    let bytes: i64 = conn.query_row("SELECT (SELECT coalesce(sum(length(CAST(policy_key AS BLOB))+length(CAST(registration_json AS BLOB))+64),0) FROM synthesis_configuration)+(SELECT coalesce(sum(length(CAST(latest_json AS BLOB))+72),0) FROM synthesis_failure_watermark)+(SELECT coalesce(sum(length(CAST(primary_id AS BLOB))+length(CAST(policy_key AS BLOB))+length(CAST(primary_json AS BLOB))+length(CAST(coalesce(plan_json,'') AS BLOB))+128),0) FROM synthesis_jobs)+(SELECT coalesce(sum(length(CAST(primary_id AS BLOB))+length(CAST(policy_key AS BLOB))+length(CAST(result_json AS BLOB))+96),0) FROM synthesis_processed)+(SELECT coalesce(sum(length(CAST(head_key AS BLOB))+length(CAST(scope_key AS BLOB))+length(CAST(family_key AS BLOB))+length(CAST(observation_id AS BLOB))+length(CAST(capture_json AS BLOB))+length(CAST(source_json AS BLOB))+length(CAST(collected_from AS BLOB))+length(CAST(collected_until AS BLOB))+128),0) FROM synthesis_context_heads)",[],|row|row.get(0)).map_err(storage)?;
    capture_journal::bytes_from_sql(bytes)
}
fn enforce_metadata_bound(conn: &Connection) -> Result<()> {
    if charged_bytes(conn)? > MAX_SYNTHESIS_METADATA_BYTES {
        return Err(problem(OutboxProblem::Capacity));
    }
    Ok(())
}

fn capture_identity(conn: &Connection, id: &str) -> Result<CaptureIdentity> {
    let (epoch, sequence): (i64, i64) = conn
        .query_row("SELECT epoch,sequence FROM captures WHERE observation_id=?1", [id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(storage)?;
    let outbox_id: String = conn.query_row("SELECT id FROM metadata", [], |row| row.get(0)).map_err(storage)?;
    Ok(CaptureIdentity {
        observation_id: id.into(),
        revision: Some(CaptureRevision { outbox_id, owner_epoch: epoch.to_string(), sequence: sequence.to_string() }),
    })
}

pub(super) fn retain_verified(conn: &mut Connection, capture: &SynthesisCapture) -> Result<()> {
    let tx = conn.transaction().map_err(storage)?;
    catalog::retain(&tx, capture.original(), capture.source(), capture.collected_from(), capture.collected_until())?;
    points::retain(&tx, capture.original(), capture.source(), capture.collected_from(), capture.collected_until())?;
    enforce_metadata_bound(&tx)?;
    tx.commit().map_err(storage)
}

pub(super) fn backlog(conn: &Connection, limit: u32, check: &dyn Fn() -> Result<()>) -> Result<SynthesisBacklog> {
    check()?;
    let pending = pending_count(conn)?;
    if pending > MAX_PENDING_SYNTHESIS_JOBS {
        return Err(integrity());
    }
    let oldest:Option<(String,String)>=conn.query_row("SELECT j.primary_json,j.queued_at FROM synthesis_jobs j LEFT JOIN captures c ON c.observation_id=j.primary_id ORDER BY c.epoch,c.sequence LIMIT 1",[],|row|Ok((row.get(0)?,row.get(1)?))).optional().map_err(storage)?;
    let oldest_pending = oldest
        .map(|(encoded, queued_at)| {
            let primary: DiagnosticCaptureRef = capture_journal::decode(&encoded)?;
            if !primary.is_valid() || canonical_time(&queued_at)? != queued_at {
                return Err(integrity());
            }
            Ok(SynthesisQueuedCapture { primary, queued_at })
        })
        .transpose()?;
    if (pending == 0) != oldest_pending.is_none() {
        return Err(integrity());
    }
    let mut query = conn.prepare("SELECT result_json FROM synthesis_processed LIMIT ?1").map_err(storage)?;
    let mut rows = query.query([i64::from(limit) + 1]).map_err(storage)?;
    let mut seen = 0_u32;
    let mut failed = 0_u32;
    while let Some(row) = rows.next().map_err(storage)? {
        check()?;
        seen = seen.checked_add(1).ok_or_else(integrity)?;
        if seen > limit {
            return Err(integrity());
        }
        let encoded: String = row.get(0).map_err(storage)?;
        let terminal: Terminal = capture_journal::decode(&encoded)?;
        match terminal {
            Terminal::Committed { output } if output.is_valid() => (),
            Terminal::Committed { .. } => return Err(integrity()),
            Terminal::Failed { .. } => failed += 1,
        }
    }
    let failure_watermark = failure_watermark(conn)?;
    if failure_watermark.total < u64::from(failed) {
        return Err(integrity());
    }
    Ok(SynthesisBacklog { pending, failed, oldest_pending, failure_watermark })
}

pub(in crate::outbox) fn archived(tx: &Transaction<'_>, id: &str) -> Result<()> {
    // A local output that was unfinished when its primary retired retains replay state until its own
    // authenticated archive acknowledgement. No known in-flight/committed writer receipt is erased.
    tx.execute("DELETE FROM synthesized_outputs WHERE observation_id=?1 AND NOT EXISTS(SELECT 1 FROM captures c WHERE c.observation_id=synthesized_outputs.primary_observation_id) AND NOT EXISTS(SELECT 1 FROM synthesis_jobs j WHERE j.primary_id=synthesized_outputs.primary_observation_id)",[id]).map_err(storage)?;
    Ok(())
}

fn failure_watermark(conn: &Connection) -> Result<SynthesisFailureWatermark> {
    let row: Option<(i64, String)> = conn
        .query_row("SELECT total,latest_json FROM synthesis_failure_watermark WHERE id=1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(storage)?;
    let Some((total, encoded)) = row else { return Ok(SynthesisFailureWatermark::default()) };
    let watermark = SynthesisFailureWatermark {
        total: u64::try_from(total).map_err(|_| integrity())?,
        latest: Some(capture_journal::decode(&encoded)?),
    };
    if !watermark.is_valid() {
        return Err(integrity());
    }
    Ok(watermark)
}
fn record_failure(tx: &Transaction<'_>, primary: &DiagnosticCaptureRef, code: culprit_core::ErrorCode) -> Result<()> {
    let previous = failure_watermark(tx)?;
    let total = previous
        .total
        .checked_add(1)
        .and_then(|total| i64::try_from(total).ok())
        .ok_or_else(|| problem(OutboxProblem::Capacity))?;
    let failure = SynthesisFailure {
        primary: primary.clone(),
        code,
        failed_at: canonical_time(&chrono::Utc::now().to_rfc3339())?,
    };
    tx.execute("INSERT INTO synthesis_failure_watermark VALUES(1,?1,?2) ON CONFLICT(id) DO UPDATE SET total=excluded.total,latest_json=excluded.latest_json",params![total,capture_journal::encode(&failure)?]).map_err(storage)?;
    Ok(())
}
