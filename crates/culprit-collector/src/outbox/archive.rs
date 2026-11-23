//! Remote archive acknowledgement is independent of publication and remains durable while a baseline stays local.
use super::*;
use culprit_evidence::delivery::recovery::CommittedRange;
use rusqlite::{OptionalExtension, params};
impl Files {
    pub(super) fn next_archive(&mut self) -> Result<Option<String>> {
        self.materialize_connection_control()?;
        self.journal.conn.query_row("SELECT observation_id FROM captures WHERE state=?1 AND archive_range IS NULL ORDER BY epoch,sequence LIMIT 1",
            [journal::encode(&OutboxCaptureState::Ready)?],|row|row.get(0)).optional().map_err(storage)
    }
    pub(super) fn archive_range(&self, id: &str) -> Result<Option<String>> {
        self.journal
            .conn
            .query_row("SELECT archive_range FROM captures WHERE observation_id=?1", [id], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .map(|row| row.flatten())
            .map_err(storage)
    }
    pub(super) fn mark_archived(&mut self, committed: &CommittedRange) -> Result<()> {
        if !matches!(&self.authority,RevisionAuthority::Recovery{store_id} if store_id==committed.store_id()) {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let manifest = committed.manifest();
        let capture = match self.journal.capture(&manifest.capture.observation_id) {
            Ok(capture) => capture,
            Err(error) if error.detail == Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::MissingCapture)) => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if capture.source_work != SourceWorkState::Quiescent {
            return Err(problem(OutboxProblem::SourceUnconfirmed));
        }
        let expected = self.prepare_delivery(
            &capture.identity.observation_id,
            manifest.contract.clone(),
            manifest.advances.clone(),
        )?;
        if expected.manifest() != manifest || manifest.digest()? != committed.manifest_digest() {
            return Err(problem(OutboxProblem::Integrity));
        }
        let empty_candidate = self.connection_original_expiry_candidate(manifest)?;
        let tx = self.journal.conn.transaction().map_err(storage)?;
        tx.execute(
            "UPDATE captures SET archive_range=?1,archive_digest=?2 WHERE observation_id=?3 AND state=?4",
            params![
                manifest.range_id,
                committed.manifest_digest(),
                capture.identity.observation_id,
                journal::encode(&OutboxCaptureState::Ready)?
            ],
        )
        .map_err(storage)?;
        if capture.request.collection() == TelemetryCollection::Diagnostics {
            tx.execute("DELETE FROM diagnostic_pins WHERE owner IN (SELECT 'head:' || controller_key FROM diagnostic_heads WHERE observation_id=?1)", [&capture.identity.observation_id]).map_err(storage)?;
        }
        tx.execute(
            "DELETE FROM diagnostic_pins WHERE owner=?1",
            [format!("derived:{}", capture.identity.observation_id)],
        )
        .map_err(storage)?;
        investigations::retirement::archived(&tx, &capture.identity.observation_id)?;
        super::automatic_synthesis::archived(&tx, &capture.identity.observation_id)?;
        let empty_original =
            connection_control::archived(&tx, manifest, committed.manifest_digest(), empty_candidate.as_deref())?;
        tx.commit().map_err(storage)?;
        if let Some(original) = empty_original {
            self.remove_capture(&original)?;
        }
        // No await or other Files owner can change the immutable capture between its canonical audit
        // above and this deletion decision. Carry that same prepared range; independent evict still audits.
        self.evict_prepared(committed, expected)?;
        Ok(())
    }
    pub(super) fn quarantine_archive(&mut self, id: &str) -> Result<()> {
        let capture = self.journal.capture(id)?;
        if capture.source_work != SourceWorkState::Quiescent {
            return Err(problem(OutboxProblem::SourceUnconfirmed));
        }
        self.journal.fail(
            &capture,
            OutboxCaptureState::Quarantined,
            Some(OutboxProblem::Integrity),
            capture.charged_bytes,
        )
    }
}
