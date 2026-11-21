//! Native-group hints and selected reconciliation reuse the current journal and immutable artifact reader.
use super::*;
use crate::delivery::diagnostic_recovery::original_revision;

const ORIGINAL_INDEX_CHARGE: u32 = 128;

impl Files {
    pub(in crate::outbox) fn original_history_witness(&self) -> Result<crate::outbox::OriginalHistoryWitness> {
        let (complete, generation): (bool, i64) = self
            .journal
            .conn
            .query_row("SELECT original_history_complete,original_history_generation FROM metadata", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(storage)?;
        if generation < 0 {
            return Err(integrity());
        }
        Ok(crate::outbox::OriginalHistoryWitness { complete, generation })
    }

    pub(in crate::outbox) fn original_group_references(
        &mut self,
        keys: &[String],
    ) -> Result<BTreeMap<String, Option<DiagnosticEvidenceRef>>> {
        Ok(self.original_history_selection(keys, None)?.references)
    }

    #[cfg(test)]
    pub(in crate::outbox) fn original_group_references_with_read_count(
        &mut self,
        keys: &[String],
    ) -> Result<(BTreeMap<String, Option<DiagnosticEvidenceRef>>, usize)> {
        super::input::count_reference_body_reads(|| self.original_group_references(keys))
    }

    pub(super) fn original_history_episodes(
        &mut self,
        source: &CollectionDescription,
        parent: &DiagnosticCaptureRef,
        rows: &[DiagnosticInputRow],
        reconciled: Option<&BTreeMap<String, Option<DiagnosticEvidenceRef>>>,
    ) -> Result<Vec<DiagnosticEpisode>> {
        let Some(reconciled) = reconciled else { return Ok(vec![]) };
        let keys = original_keys(source, parent, rows)?;
        Ok(self.original_history_selection(&keys, Some(reconciled))?.episodes)
    }

    fn original_history_selection(
        &mut self,
        keys: &[String],
        archived: Option<&BTreeMap<String, Option<DiagnosticEvidenceRef>>>,
    ) -> Result<OriginalHistorySelection> {
        crate::delivery::diagnostic_recovery::validate_original_group_keys(keys)?;
        if archived
            .is_some_and(|entries| entries.len() != keys.len() || keys.iter().any(|key| !entries.contains_key(key)))
        {
            return Err(integrity());
        }
        let mut captures = BTreeMap::<String, OriginalCaptureRequests>::new();
        for key in keys {
            let hint: Option<(String, u32)> = self
                .journal
                .conn
                .query_row(
                    "SELECT observation_id,row_ordinal FROM diagnostic_original_groups WHERE group_key=?1",
                    [key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(storage)?;
            if let Some((capture, ordinal)) = hint {
                captures.entry(capture).or_default().local.push((key.clone(), ordinal));
            }
            if let Some(reference) = archived.and_then(|entries| entries.get(key)).and_then(Option::as_ref) {
                captures
                    .entry(reference.capture.observation_id.clone())
                    .or_default()
                    .archived
                    .push((key.clone(), reference.clone()));
            }
        }
        let mut selected = BTreeMap::<String, (DiagnosticEvidenceRef, DiagnosticEpisode)>::new();
        for (capture, requested) in captures {
            // Native local hints still require the complete sealed local artifact, exactly as before. Archived
            // candidates use the existing authenticated restored-body path. Neither body survives this iteration.
            let original = if requested.local.is_empty() {
                self.diagnostic_reference_rows(&capture)?
            } else {
                self.canonical_diagnostic_reference_rows(&capture)?
            };
            if original.source.source != DIAGNOSTICS_SOURCE {
                return Err(integrity());
            }
            for (key, ordinal) in requested.local {
                let row = original
                    .rows
                    .iter()
                    .find(|row| {
                        row.reference.stream == TelemetryStream::OperationalSamples
                            && row.reference.row_ordinal == ordinal
                    })
                    .ok_or_else(integrity)?;
                select_original(&mut selected, &key, &row.reference, &row.row)?;
            }
            for (key, reference) in requested.archived {
                if original.parent != reference.capture_ref() {
                    return Err(integrity());
                }
                let row = original.rows.iter().find(|row| row.reference == reference).ok_or_else(integrity)?;
                select_original(&mut selected, &key, &reference, &row.row)?;
            }
        }
        let references =
            keys.iter().map(|key| (key.clone(), selected.get(key).map(|(reference, _)| reference.clone()))).collect();
        let episodes = selected.into_values().map(|(_, episode)| episode).collect();
        Ok(OriginalHistorySelection { references, episodes })
    }

    pub(in crate::outbox) fn complete_original_history(
        &mut self,
        observation: &str,
        mut reconciled: BTreeMap<String, Option<DiagnosticEvidenceRef>>,
        witness: crate::outbox::OriginalHistoryWitness,
    ) -> Result<DiagnosticInput> {
        // This whole method runs under the same serialized Files owner. No archive acknowledgement or
        // newly committed group can interleave between this check, refreshing hints and the transaction.
        if self.original_history_witness()? != witness {
            return Err(DeliveryProblem::Conflict.error());
        }
        let mut input = self.diagnostic_input_base(observation, DIAGNOSTIC_CONTROLLER_VERSION)?;
        let keys = original_keys(&input.source, &input.parent, &input.rows)?;
        let OriginalHistorySelection { references, episodes: historical } =
            self.original_history_selection(&keys, Some(&reconciled))?;
        reconciled = references;
        if historical.iter().any(|episode| {
            episode.retained_in_checkpoint()
                && !input
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| checkpoint.episodes.iter().any(|active| active.id == episode.id))
        }) {
            return Err(DeliveryProblem::NotReady.error());
        }
        input.original_history = Some(reconciled.clone());
        input.historical_episodes = historical
            .into_iter()
            .filter(|episode| {
                !input
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| checkpoint.episodes.iter().any(|active| active.id == episode.id))
            })
            .collect();
        let json = encode(&reconciled)?;
        if json.len() > MAX_RESTORED_INPUT_BYTES {
            return Err(problem(OutboxProblem::Capacity));
        }
        let previous: Option<String> = self
            .journal
            .conn
            .query_row(
                "SELECT original_history_json FROM diagnostic_inputs WHERE observation_id=?1",
                [observation],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if let Some(previous) = previous {
            if previous != json {
                return Err(DeliveryProblem::Conflict.error());
            }
            return Ok(input);
        }
        let charged_bytes = u32::try_from(json.len()).map_err(|_| problem(OutboxProblem::Capacity))?;
        let status = self.journal.status(1, None)?;
        if status
            .charged_bytes
            .checked_add(u64::from(charged_bytes))
            .is_none_or(|bytes| bytes > self.journal.limits.max_bytes)
        {
            return Err(problem(OutboxProblem::Capacity));
        }
        let tx = self.journal.conn.transaction().map_err(storage)?;
        tx.execute("UPDATE diagnostic_inputs SET original_history_json=?1,charged_bytes=charged_bytes+?2 WHERE observation_id=?3",
            params![json,charged_bytes,observation]).map_err(storage)?;
        for reference in reconciled.values().flatten() {
            super::journal::pin(&tx, &format!("original-history:{observation}"), &reference.capture.observation_id)?;
        }
        tx.commit().map_err(storage)?;
        Ok(input)
    }
}

/// This owner retains only the selected references/episodes, never a map of full original capture bodies.
struct OriginalHistorySelection {
    references: BTreeMap<String, Option<DiagnosticEvidenceRef>>,
    episodes: Vec<DiagnosticEpisode>,
}
#[derive(Default)]
struct OriginalCaptureRequests {
    local: Vec<(String, u32)>,
    archived: Vec<(String, DiagnosticEvidenceRef)>,
}
fn select_original(
    selected: &mut BTreeMap<String, (DiagnosticEvidenceRef, DiagnosticEpisode)>,
    key: &str,
    reference: &DiagnosticEvidenceRef,
    row: &ExportedRecord,
) -> Result<()> {
    let revision = original_revision(&row.record).ok_or_else(integrity)?;
    let episode = &revision.episode;
    if revision.controller_version != DIAGNOSTIC_CONTROLLER_VERSION
        || episode.original_group_key()?.as_deref() != Some(key)
    {
        return Err(integrity());
    }
    if let Some((_, before)) = selected.get(key) {
        if episode.id != before.id
            || episode.first != before.first
            || episode.signal != before.signal
            || episode.scope != before.scope
            || episode.first_seen_at != before.first_seen_at
        {
            return Err(integrity());
        }
        let now = revision_number(&episode.revision)?;
        let previous = revision_number(&before.revision)?;
        if now == previous && episode != before {
            return Err(integrity());
        }
        if now <= previous {
            return Ok(());
        }
    }
    selected.insert(key.to_owned(), (reference.clone(), episode.clone()));
    Ok(())
}

pub(crate) fn original_keys(
    source: &CollectionDescription,
    parent: &DiagnosticCaptureRef,
    rows: &[DiagnosticInputRow],
) -> Result<Vec<String>> {
    let rows = rows
        .iter()
        .map(|row| DiagnosticOriginalRow { reference: &row.reference, record: &row.row.record })
        .collect::<Vec<_>>();
    DiagnosticOriginalEvidence::new(source, parent, &rows)?.group_keys()
}

pub(super) fn record_original_groups(
    tx: &Transaction<'_>,
    capture: &CaptureIdentity,
    revisions: &[DiagnosticRevision],
) -> Result<()> {
    let mut changed = false;
    for (ordinal, revision) in revisions.iter().enumerate() {
        let Some(key) = revision.episode.original_group_key()? else { continue };
        tx.execute("DELETE FROM diagnostic_pins WHERE owner=?1", [format!("original-group:{key}")]).map_err(storage)?;
        tx.execute("INSERT INTO diagnostic_original_groups VALUES(?1,?2,?3,?4) ON CONFLICT(group_key) DO UPDATE SET observation_id=excluded.observation_id,row_ordinal=excluded.row_ordinal",
            params![key,capture.observation_id,ordinal as u32,ORIGINAL_INDEX_CHARGE]).map_err(storage)?;
        super::journal::pin(tx, &format!("original-group:{key}"), &capture.observation_id)?;
        changed = true;
    }
    if changed {
        crate::outbox::journal::advance_original_history_generation(tx)?;
    }
    Ok(())
}

fn revision_number(text: &str) -> Result<u64> {
    text.parse::<u64>().ok().filter(|number| *number > 0).ok_or_else(integrity)
}
