//! A native recovery question preserves the affected original and a separately acquired local-secondary point.
use super::*;

impl ActivityRequestFocus {
    pub fn is_readable_secondary_recovery_focus(&self) -> bool {
        self.is_current_request_work_focus()
            && self.database_writable == Some(false)
            && matches!(self.partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) })
            && matches!(self.target.engine(), crate::Engine::Postgres | crate::Engine::Mssql)
            && self.original.key.request.is_some()
    }
}
impl SourceReadFocus {
    pub(super) fn from_readable_secondary(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.lifecycle == DiagnosticLifecycle::Resolved
            || !matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
            || reference != &episode.first
        {
            return Ok(None);
        }
        let TelemetryRecord::Activity(original) = &row.record else { return Ok(None) };
        reference.validate_canonical_record(source, &row.record)?;
        if row.capture != reference.capture
            || row.batch_id != reference.batch_id
            || row.payload_sha256()? != reference.payload_sha256
        {
            return Err(invalid());
        }
        let Some(request) = episode.original_request_focus(source, original, reference)? else { return Ok(None) };
        if !request.is_readable_secondary_recovery_focus() {
            return Ok(None);
        }
        let focus = Self {
            basis: Box::new(reference.clone()),
            target: source.target.clone(),
            question: SourceReadQuestion::ReadableSecondaryRecovery,
            prerequisite: SourceReadPrerequisite::ObservedReadableSecondaryReader { request: Box::new(request) },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(Some(focus))
    }
    pub fn readable_secondary_request(&self) -> Option<&ActivityRequestFocus> {
        match &self.prerequisite {
            SourceReadPrerequisite::ObservedReadableSecondaryReader { request }
                if self.question == SourceReadQuestion::ReadableSecondaryRecovery && self.is_valid() =>
            {
                Some(request)
            }
            _ => None,
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReadableSecondaryRecoveryAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub source_state: crate::SourceState,
    #[serde(deserialize_with = "Option::deserialize")]
    pub observation: Option<DiagnosticEvidenceRef>,
}
impl ReadableSecondaryRecoveryAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let request = focus.readable_secondary_request().ok_or_else(invalid)?;
        let SourceObservation::SourceRead { snapshot, focus_key, execution } = &source.observation else {
            return Err(invalid());
        };
        let authority = SourceReadExecutionScope::from_context(focus.question, execution)?;
        if !focus.matches_basis(original_source, original, &authority)
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || *focus_key != focus.key()?
            || !capture.is_valid()
            || capture.capture == focus.basis.capture
            || source.captured_range()?.scope_id != capture.source_scope_id
            || snapshot.records as usize != rows.len()
            || rows.len() > 1
        {
            return Err(invalid());
        }
        let observation = match rows {
            [] => None,
            [(reference, row)] => {
                reference.validate_canonical_record(source, &row.record)?;
                let TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::ReadableSecondaryRecovery(point),
                    ..
                }) = &row.record
                else {
                    return Err(invalid());
                };
                if row.capture != capture.capture
                    || row.batch_id != reference.batch_id
                    || row.payload_sha256()? != reference.payload_sha256
                    || reference.capture_ref() != *capture
                    || reference.row_ordinal != 0
                    || point.focus.as_ref() != request
                    || !point.fits_source(source)
                {
                    return Err(invalid());
                }
                Some((*reference).clone())
            }
            _ => return Err(invalid()),
        };
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            source_state: snapshot.coverage_state()?,
            observation,
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        let Ok(scope) = LocalResourceReadScope::from_context(&self.result_context) else { return false };
        self.basis.is_valid() && self.basis.stream == TelemetryStream::ActivityObservations
            && self.basis_context.database_writable == Some(false) && self.result_context.database_writable == Some(false)
            && scope.matches_original_context(&self.basis_context)
            && self.result.is_valid() && self.result.capture != self.basis.capture
            && self.result.source_scope_id != self.basis.source_scope_id
            && ObservationBounds::new(&self.basis_context.observed_until, &self.result_context.observed_from).is_some()
            && matches!(self.result_context.target.engine(), crate::Engine::Postgres | crate::Engine::Mssql)
            && self.observation.as_ref().is_none_or(|r| r.is_valid() && r.capture_ref() == self.result
                && r.stream == TelemetryStream::OperationalSamples && matches!(&r.native_key,
                    NativeRecordIdentity::Operational {source, ..} if source == crate::source_id::READABLE_SECONDARY_RECOVERY))
            && match self.source_state {
                crate::SourceState::Collected => self.observation.is_some(),
                crate::SourceState::Partial => true,
                _ => self.observation.is_none(),
            }
    }
    pub fn matches_point(&self, point: &ReadableSecondaryRecoveryPoint) -> bool {
        self.is_valid()
            && point.is_valid()
            && point.context == self.result_context
            && point.focus.parent == self.basis
            && self.observation.as_ref().is_some_and(|r| {
                point.key().is_ok_and(|key| {
                    r.native_key
                        == NativeRecordIdentity::Operational {
                            source: crate::source_id::READABLE_SECONDARY_RECOVERY.into(),
                            key,
                        }
                })
            })
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        std::iter::once(&self.basis).chain(self.observation.iter())
    }
}
impl DiagnosticEpisode {
    pub fn complete_readable_secondary_recovery(
        &self,
        intent: &DiagnosticInvestigationIntent,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { return Err(invalid()) };
        if !self.is_valid()
            || !self.needs_question(&intent.focus)
            || !intent.matches_episode(self)
            || !intent.matches_result_source(source)
        {
            return Err(invalid());
        }
        let answer =
            ReadableSecondaryRecoveryAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::ReadableSecondaryRecovery(Box::new(answer))),
            },
        )
    }
}
