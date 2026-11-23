//! One authenticated result retains bounded original rows only for closed answer families.
use super::*;

impl Files {
    /// Exact saved-lineage lookup only. The caller still authenticates canonical result bytes and consumption
    /// before settling a lost Fleet acknowledgement; no current checkpoint/actor can replace this association.
    pub(in crate::outbox) fn consumed_source_read_result(
        &self,
        intent: &DiagnosticInvestigationIntent,
    ) -> Result<Option<DiagnosticCaptureRef>> {
        if !intent.is_valid() || !matches!(intent.focus, DiagnosticFocus::SourceRead { .. }) {
            return Err(integrity());
        }
        let lineage = DiagnosticInvestigationLineage::from_intent(intent)?;
        let encoded = encode_investigation(Some(&lineage))?.ok_or_else(integrity)?;
        let mut query = self
            .journal
            .conn
            .prepare(
                "SELECT parent_json FROM diagnostic_inputs WHERE controller_version=?1 AND acknowledged=1
             AND evidence_only=0 AND lineage_json=?2 LIMIT 2",
            )
            .map_err(storage)?;
        let rows = query
            .query_map(params![DIAGNOSTIC_CONTROLLER_VERSION, encoded], |row| row.get::<_, String>(0))
            .map_err(storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage)?;
        match rows.as_slice() {
            [] => Ok(None),
            [parent] => {
                let parent: DiagnosticCaptureRef = decode(parent)?;
                if !parent.is_valid() {
                    return Err(integrity());
                }
                Ok(Some(parent))
            }
            _ => Err(integrity()),
        }
    }

    pub(super) fn validate_source_read_capture(
        &self,
        event: &DiagnosticInvestigationEvent,
        input: &DiagnosticInput,
    ) -> Result<CollectionDescription> {
        let DiagnosticInvestigationOutcome::Captured { capture } = &event.outcome else { return Err(integrity()) };
        // The private input was constructed by the same canonical local verifier, or by authenticated
        // committed-range recovery. It attests a capture, not that history rows have been materialized here.
        if !event.is_valid()
            || input.parent != *capture
            || !source_read_retains_rows(&input.source, input.investigation.as_ref())? && !input.rows.is_empty()
            || !input.source.observation.is_source_read_result()
            || !event.intent.matches_result_source(&input.source)
            || input
                .investigation
                .as_ref()
                .is_none_or(|lineage| !lineage.matches_intent(&event.intent.focus, &event.intent))
        {
            return Err(integrity());
        }
        Ok(input.source.clone())
    }

    pub(super) fn validate_source_read_revision(
        &mut self,
        before: Option<&DiagnosticEpisode>,
        revision: &DiagnosticRevision,
        result_source: Option<&CollectionDescription>,
        input: &DiagnosticInput,
    ) -> Result<()> {
        for stage in revision.episode.source_reads.iter() {
            if before.and_then(|episode| episode.source_reads.get_focus(&stage.focus)) == Some(stage) {
                continue;
            }
            self.validate_source_stage(before, revision, stage, result_source, input)?;
        }
        Ok(())
    }

    fn validate_source_stage(
        &mut self,
        before: Option<&DiagnosticEpisode>,
        revision: &DiagnosticRevision,
        stage: &SourceReadStage,
        result_source: Option<&CollectionDescription>,
        input: &DiagnosticInput,
    ) -> Result<()> {
        match &stage.state {
            SourceReadStageState::Pending | SourceReadStageState::Deferred { .. } => {
                if !crate::diagnostics::continuation_due(&revision.episode, &stage.focus) {
                    return Err(integrity());
                }
                if revision
                    .investigation
                    .as_ref()
                    .is_some_and(|event| matches!(event.intent.focus, DiagnosticFocus::SourceRead { .. }))
                {
                    return Err(integrity());
                }
            }
            SourceReadStageState::Recovered => {
                if revision.episode.lifecycle != DiagnosticLifecycle::Resolved
                    || !stage.focus.question.requires_live_episode()
                {
                    return Err(integrity());
                }
            }
            SourceReadStageState::Collecting => {
                let event = stage_event(revision, stage)?;
                if !matches!(event.outcome, DiagnosticInvestigationOutcome::Admitted) {
                    return Err(integrity());
                }
                let expected = before
                    .ok_or_else(integrity)?
                    .transition_source_read(&stage.focus, SourceReadStageState::Collecting)?;
                if expected.source_reads != revision.episode.source_reads {
                    return Err(integrity());
                }
            }
            SourceReadStageState::Stopped { issue } => {
                let event = stage_event(revision, stage)?;
                if !matches!(&event.outcome, DiagnosticInvestigationOutcome::Stopped { issue: actual } if actual == issue)
                {
                    return Err(integrity());
                }
                let expected = before
                    .ok_or_else(integrity)?
                    .transition_source_read(&stage.focus, SourceReadStageState::Stopped { issue: issue.clone() })?;
                if expected.source_reads != revision.episode.source_reads {
                    return Err(integrity());
                }
            }
            SourceReadStageState::Answered { answer } => {
                let event = stage_event(revision, stage)?;
                let source = result_source.ok_or_else(integrity)?;
                let (basis_source, basis) = self.diagnostic_reference(&stage.focus.basis)?;
                let rows = input.rows.iter().map(|row| (&row.reference, &row.row)).collect::<Vec<_>>();
                let expected = before.ok_or_else(integrity)?.complete_source_read_from_originals(
                    &event.intent,
                    &basis_source,
                    &basis,
                    source,
                    answer.result(),
                    &rows,
                )?;
                if expected.source_reads != revision.episode.source_reads {
                    return Err(integrity());
                }
            }
            SourceReadStageState::Captured { capture } => {
                let event = stage_event(revision, stage)?;
                let source = result_source.ok_or_else(integrity)?;
                let expected = before.ok_or_else(integrity)?.complete_source_read(&event.intent, source, capture)?;
                if expected.source_reads != revision.episode.source_reads {
                    return Err(integrity());
                }
            }
        }
        Ok(())
    }
}

fn stage_event<'a>(
    revision: &'a DiagnosticRevision,
    stage: &SourceReadStage,
) -> Result<&'a DiagnosticInvestigationEvent> {
    let event = revision.investigation.as_ref().ok_or_else(integrity)?;
    match &event.intent.focus {
        DiagnosticFocus::SourceRead { focus } if **focus == stage.focus => Ok(event),
        _ => Err(integrity()),
    }
}
