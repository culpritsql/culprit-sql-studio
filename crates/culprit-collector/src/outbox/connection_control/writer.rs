//! Projection runs on the existing Files worker/lease. The bounded source record was already frozen at settlement.
use super::*;
use crate::collection_files::ObservationFiles;

pub(super) fn request(pending: &Pending) -> Result<CollectionRequest> {
    let snapshot = pending.snapshot.as_ref().ok_or_else(integrity)?;
    Ok(CollectionRequest {
        subject: CollectionSubject::ConfiguredConnection { connection: pending.registration.target.clone() },
        selection: CollectionSelection::CollectorConnection { attempt: snapshot.original.capture.clone() },
        budget: CollectionBudget {
            page_rows: 1,
            max_snapshot_rows: 1,
            page_bytes: MAX_CONNECTION_CONTROL_BYTES as u32,
            total_bytes: MAX_CONNECTION_CONTROL_BYTES as u32,
            max_pages: 1,
            max_queries: 1,
            ..Default::default()
        },
    })
}
impl Files {
    /// One existing delivery turn may materialize one pending control. A native lease is never recovered here.
    pub(in crate::outbox) fn materialize_connection_control(&mut self) -> Result<Option<CaptureIdentity>> {
        if !self.local_output_mutation_ready()? {
            return Ok(None);
        }
        let id: Option<String> = self.journal.conn.query_row(
            "SELECT observation_id FROM captures WHERE connection_pending_json IS NOT NULL AND state<>?1 ORDER BY epoch,sequence LIMIT 1",
            [journal::encode(&OutboxCaptureState::Capturing)?], |row| row.get(0),
        ).optional().map_err(storage)?;
        let Some(id) = id else { return Ok(None) };
        let pending = read_pending(&self.journal.conn, &id)?.ok_or_else(integrity)?;
        let snapshot = pending.snapshot.as_ref().ok_or_else(integrity)?.clone();
        let request = request(&pending)?;
        let reservation = match pending.output.as_ref() {
            Some(output) => self.resume_connection_output(&request, output)?,
            None => {
                let reservation = self.reserve(&pending.registration.target.name, &request)?;
                // The same reserve transaction already retained the exact output association before files exist.
                reservation
            }
        };
        let identity = reservation.entry.identity.clone();
        let mut files = ObservationFiles::create(&reservation.directory, request, identity.clone(), None)?;
        files.describe(CollectionDescription {
            target: CollectionTarget::ConfiguredConnection(pending.registration.target),
            partition: SourcePartition::Target,
            source: COLLECTOR_CONNECTION_SOURCE.into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::CollectorConnection(CollectorConnectionDescription {
                attempt: snapshot.original.capture.clone(),
                snapshot_sha256: snapshot.snapshot_sha256()?,
                observed_from: snapshot.collector_started_at.clone(),
                observed_until: snapshot.collector_finished_at.clone(),
            }),
        })?;
        files.write_stream(0, &[TelemetryRecord::CollectorConnection(Box::new(snapshot))])?;
        files.confirm_source(SourceCompletion::Verified)?;
        if files.finish(0)?.status != CollectionStatus::Complete {
            return Err(integrity());
        }
        self.finish(reservation)?;
        Ok(Some(identity))
    }
    fn resume_connection_output(
        &mut self,
        request: &CollectionRequest,
        identity: &CaptureIdentity,
    ) -> Result<Reservation> {
        let capture = self.journal.capture(&identity.observation_id)?;
        if capture.identity != *identity
            || capture.request != *request
            || capture.source_work != SourceWorkState::Quiescent
            || !matches!(capture.state, OutboxCaptureState::Incomplete | OutboxCaptureState::Interrupted)
            || self.archive_range(&identity.observation_id)?.is_some()
        {
            return Err(integrity());
        }
        // This exact local-only allocation was never Ready/published. Reuse it rather than accumulate failed retry rows.
        self.remove_capture_files(&identity.observation_id)?;
        let charge = capture.reservation_charge()?;
        let tx = self.journal.conn.transaction().map_err(storage)?;
        let (count, used): (u32, i64) = tx
            .query_row("SELECT count(*),coalesce(sum(charged_bytes),0) FROM captures", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(storage)?;
        let metadata = journal::metadata_bytes(&tx)?;
        let used = journal::bytes_from_sql(used)?
            .checked_sub(capture.charged_bytes)
            .and_then(|bytes| bytes.checked_add(charge))
            .and_then(|bytes| bytes.checked_add(metadata))
            .ok_or_else(integrity)?;
        if count > self.journal.limits.max_captures || used > self.journal.limits.max_bytes {
            return Err(problem(OutboxProblem::Capacity));
        }
        if tx.execute("UPDATE captures SET state=?1,charged_bytes=?2,problem=NULL,manifest_sha=NULL WHERE observation_id=?3 AND state=?4 AND source_work=?5", params![journal::encode(&OutboxCaptureState::Capturing)?,journal::bytes_to_sql(charge)?,identity.observation_id,journal::encode(&capture.state)?,journal::encode(&SourceWorkState::Quiescent)?]).map_err(storage)? != 1 { return Err(DeliveryProblem::Conflict.error()); }
        tx.commit().map_err(storage)?;
        let entry = self.journal.capture(&identity.observation_id)?;
        Ok(Reservation { entry, directory: self.directory(&identity.observation_id) })
    }
}

pub(in crate::outbox) fn complete_output(
    tx: &Transaction<'_>,
    validated: &super::super::artifacts::ValidatedCapture,
) -> Result<()> {
    let source = validated.manifest.description.as_ref().ok_or_else(integrity)?;
    let SourceObservation::CollectorConnection(header) = &source.observation else { return Ok(()) };
    let id = &header.attempt.observation_id;
    let pending = read_pending(tx, id)?.ok_or_else(integrity)?;
    let snapshot = pending.snapshot.as_ref().ok_or_else(integrity)?;
    if pending.output.as_ref() != Some(&validated.manifest.capture)
        || source.target != CollectionTarget::ConfiguredConnection(pending.registration.target.clone())
        || !snapshot.fits_source(source)
        || snapshot.original.capture != header.attempt
        || validated.manifest.source_queries != 0
        || validated.manifest.read_metrics.is_some()
    {
        return Err(integrity());
    }
    let receipt = Receipt {
        output: validated.manifest.capture.clone(),
        snapshot_sha256: header.snapshot_sha256.clone(),
        archive: None,
    };
    if tx.execute("UPDATE captures SET connection_result_json=?1,connection_pending_json=NULL WHERE observation_id=?2 AND connection_pending_json IS NOT NULL AND connection_result_json IS NULL",params![encode(&receipt)?,id]).map_err(storage)? != 1 { return Err(DeliveryProblem::Conflict.error()); }
    tx.execute(
        "DELETE FROM diagnostic_pins WHERE owner=?1 AND observation_id=?2",
        params![format!("connection-control:{id}"), id],
    )
    .map_err(storage)?;
    Ok(())
}

/// Acknowledging a control output never authorizes deleting partial native evidence or clearing native debt.
pub(in crate::outbox) fn archived(
    tx: &Transaction<'_>,
    manifest: &culprit_evidence::delivery::evidence::RangeManifest,
    digest: &str,
    empty_candidate: Option<&str>,
) -> Result<Option<String>> {
    let SourceObservation::CollectorConnection(header) = &manifest.source.observation else { return Ok(None) };
    let id = &header.attempt.observation_id;
    let row: Option<(Option<String>, String, String)> = tx
        .query_row(
            "SELECT connection_result_json,state,source_work FROM captures WHERE observation_id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((Some(encoded), state, work)) = row else { return Ok(None) };
    let mut receipt: Receipt = journal::decode(&encoded)?;
    if receipt.output != manifest.capture || receipt.snapshot_sha256 != header.snapshot_sha256 {
        return Err(integrity());
    }
    receipt.archive = Some(ArchiveReceipt { range_id: manifest.range_id.clone(), manifest_sha256: digest.into() });
    tx.execute("UPDATE captures SET connection_result_json=?1 WHERE observation_id=?2", params![encode(&receipt)?, id])
        .map_err(storage)?;
    // Only an original positively proven empty at freeze can retire. No manifest/unknown/partial is never eligible.
    let eligible: bool = tx
        .query_row("SELECT connection_empty_original FROM captures WHERE observation_id=?1", [id], |row| row.get(0))
        .map_err(storage)?;
    let state: OutboxCaptureState = journal::decode(&state)?;
    let work: SourceWorkState = journal::decode(&work)?;
    if empty_candidate != Some(id.as_str())
        || !eligible
        || !matches!(state, OutboxCaptureState::Incomplete | OutboxCaptureState::Interrupted)
        || !matches!(work, SourceWorkState::NotStarted | SourceWorkState::Quiescent)
        || investigations::journal::is_pinned(tx, id)?
    {
        return Ok(None);
    }
    tx.execute(
        "UPDATE captures SET state=?1 WHERE observation_id=?2 AND state=?3",
        params![journal::encode(&OutboxCaptureState::Evicting)?, id, journal::encode(&state)?],
    )
    .map_err(storage)?;
    Ok(Some(id.clone()))
}

pub(in crate::outbox) fn preidentity_expiry(conn: &Connection, capture: &journal::Capture) -> Result<bool> {
    if capture.state != OutboxCaptureState::Evicting
        || !matches!(capture.source_work, SourceWorkState::NotStarted | SourceWorkState::Quiescent)
    {
        return Ok(false);
    }
    let row: (bool, Option<String>) = conn
        .query_row(
            "SELECT connection_empty_original,connection_result_json FROM captures WHERE observation_id=?1",
            [&capture.identity.observation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(storage)?;
    let Some(encoded) = row.1 else { return Ok(false) };
    let receipt: Receipt = journal::decode(&encoded)?;
    Ok(row.0
        && receipt.output.is_valid()
        && valid_digest(&receipt.snapshot_sha256)
        && receipt
            .archive
            .is_some_and(|archive| valid_digest(&archive.range_id) && valid_digest(&archive.manifest_sha256)))
}

impl Files {
    pub(in crate::outbox) fn connection_original_expiry_candidate(
        &self,
        manifest: &culprit_evidence::delivery::evidence::RangeManifest,
    ) -> Result<Option<String>> {
        let SourceObservation::CollectorConnection(header) = &manifest.source.observation else { return Ok(None) };
        let original = match self.journal.capture(&header.attempt.observation_id) {
            Ok(original) => original,
            Err(error) if error.detail == Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::MissingCapture)) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !matches!(original.state, OutboxCaptureState::Incomplete | OutboxCaptureState::Interrupted)
            || !matches!(original.source_work, SourceWorkState::NotStarted | SourceWorkState::Quiescent)
        {
            return Ok(None);
        }
        if !self.connection_original_is_empty(&original.identity.observation_id)? {
            return Ok(None);
        }
        Ok(Some(original.identity.observation_id))
    }
    pub(in crate::outbox) fn connection_original_is_empty(&self, id: &str) -> Result<bool> {
        let frozen: bool = self
            .journal
            .conn
            .query_row("SELECT connection_empty_original FROM captures WHERE observation_id=?1", [id], |row| row.get(0))
            .map_err(storage)?;
        if !frozen {
            return Ok(false);
        }
        let original = self.journal.capture(id)?;
        let recorded = attempts::read_manifest(&self.root, &self.journal, &original)?.ok_or_else(integrity)?;
        if !no_native_evidence(&self.directory(id), &recorded.manifest, original.source_work) {
            return Err(integrity());
        }
        Ok(true)
    }
}
