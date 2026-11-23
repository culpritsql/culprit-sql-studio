//! A finite source answer carries exact receipts, not a copied native roster or a causal assertion.
use super::*;
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct TransactionMetadataAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub mechanism: TransactionMetadataWait,
    pub source_state: crate::SourceState,
    /// None preserves an empty/unavailable native result, never recovery or a healthy SLRU verdict.
    #[serde(deserialize_with = "Option::deserialize")]
    pub observation: Option<DiagnosticEvidenceRef>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub request_end: Option<BlockingRequestState>,
    /// Exact observed native overflow states; these are candidates, not statements that caused the wait.
    pub overflowed_backends: Vec<NativeActivityIdentity>,
}
impl TransactionMetadataAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let (key, _, mechanism) = focus.transaction_metadata_participant().ok_or_else(invalid)?;
        let SourceObservation::SourceRead { snapshot, focus_key, execution: result_context } = &source.observation
        else {
            return Err(invalid());
        };
        let execution = SourceReadExecutionScope::from_context(focus.question, result_context)?;
        if focus.question != SourceReadQuestion::TransactionMetadata
            || !focus.matches_basis(original_source, original, &execution)
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
        let mut observation = None;
        let mut request_end = None;
        let mut overflowed_backends = vec![];
        if let Some((reference, row)) = rows.first() {
            reference.validate_canonical_record(source, &row.record)?;
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::PostgresTransactionMetadata(point),
                ..
            }) = &row.record
            else {
                return Err(invalid());
            };
            if reference.capture_ref() != *capture
                || reference.capture != row.capture
                || reference.row_ordinal != 0
                || reference.batch_id != row.batch_id
                || reference.payload_sha256 != row.payload_sha256()?
                || !point.fits_source_scope(source)
                || point.focus != *key
            {
                return Err(invalid());
            }
            observation = Some((*reference).clone());
            request_end = Some(point.request_end);
            overflowed_backends = point
                .caches
                .iter()
                .filter(|cache| cache.overflowed == Some(true))
                .map(|cache| cache.actor.key.session.clone())
                .collect();
        }
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            result: capture.clone(),
            result_context: result_context.as_ref().clone(),
            mechanism,
            source_state: snapshot.coverage_state()?,
            observation,
            request_end,
            overflowed_backends,
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        let Ok(execution) = LocalResourceReadScope::from_context(&self.result_context) else { return false };
        let mut sessions = BTreeSet::new();
        self.basis.is_valid()
            && self.basis.stream == TelemetryStream::ActivityObservations
            && self.result.is_valid()
            && self.result.capture != self.basis.capture
            && self.result.source_scope_id != self.basis.source_scope_id
            && execution.matches_original_context(&self.basis_context)
            && self.result_context.target.engine() == crate::Engine::Postgres
            && ObservationBounds::new(&self.basis_context.observed_until, &self.result_context.observed_from).is_some()
            && self.observation.as_ref().is_none_or(|reference| {
                reference.is_valid()
                    && reference.stream == TelemetryStream::OperationalSamples
                    && reference.capture_ref() == self.result
            })
            && self.observation.is_some() == self.request_end.is_some()
            && self.request_end.is_none_or(|end| {
                matches!(
                    end,
                    BlockingRequestState::SameStatement | BlockingRequestState::Changed | BlockingRequestState::Missing
                )
            })
            && match self.source_state {
                crate::SourceState::Collected => self.observation.is_some(),
                crate::SourceState::Empty | crate::SourceState::Unavailable | crate::SourceState::NotApplicable => {
                    self.observation.is_none()
                }
                crate::SourceState::Partial => true,
            }
            && self.overflowed_backends.len() <= MAX_TRANSACTION_METADATA_BACKENDS
            && (self.observation.is_some() || self.overflowed_backends.is_empty())
            && self.overflowed_backends.iter().all(|actor| actor.is_valid() && sessions.insert(actor))
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        std::iter::once(&self.basis).chain(self.observation.iter())
    }
}
impl DiagnosticEpisode {
    pub fn complete_transaction_metadata_read(
        &self,
        intent: &DiagnosticInvestigationIntent,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else {
            return Err(invalid());
        };
        if !self.is_valid()
            || !self.needs_question(&intent.focus)
            || !intent.matches_episode(self)
            || !intent.matches_result_source(source)
        {
            return Err(invalid());
        }
        let answer =
            TransactionMetadataAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
    }
}
