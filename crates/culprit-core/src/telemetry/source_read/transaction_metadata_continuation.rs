//! One independently adverse later original can owe one second metadata point, never an unbounded sampler.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct TransactionMetadataContinuation {
    /// Exact first selected native point. Its body is authenticated separately before a new native admission.
    pub previous: DiagnosticEvidenceRef,
    pub request: Box<ActivityRequestFocus>,
    pub mechanism: TransactionMetadataWait,
}
impl TransactionMetadataContinuation {
    pub fn is_valid(&self) -> bool {
        self.previous.is_valid()
            && self.previous.stream == TelemetryStream::OperationalSamples
            && matches!(&self.previous.native_key, NativeRecordIdentity::Operational { source, .. }
                if source == POSTGRES_TRANSACTION_METADATA_SOURCE)
            && self.request.is_valid()
            && self.request.target.engine() == crate::Engine::Postgres
            && self.request.database_writable.is_some()
            && self.previous.capture != self.request.parent.capture
            && self.request.original.wait.as_ref().and_then(|wait| wait.class) == Some(self.mechanism.class())
    }
    fn request_context(&self) -> NativeReadContext {
        NativeReadContext {
            target: self.request.target.clone(),
            partition: self.request.partition.clone(),
            observed_from: self.request.original_observed_at.clone(),
            observed_until: self.request.original_observed_at.clone(),
            database_writable: self.request.database_writable,
        }
    }
    pub(super) fn matches_first(&self, first: &TransactionMetadataAnswer) -> bool {
        self.is_valid()
            && first.is_valid()
            && first.observation.as_ref() == Some(&self.previous)
            && first.mechanism == self.mechanism
            && first.request_end == Some(BlockingRequestState::SameStatement)
            && matches!(first.source_state, crate::SourceState::Collected | crate::SourceState::Partial)
            && LocalResourceReadScope::from_context(&first.result_context)
                .is_ok_and(|scope| scope.matches_context(&self.request_context()))
            && ObservationBounds::new(&first.result_context.observed_until, &self.request.original_observed_at)
                .is_some_and(|bounds| bounds.has_positive_duration())
    }
    pub(super) fn matches_episode(&self, episode: &DiagnosticEpisode) -> bool {
        self.is_valid()
            && episode.matches_recorded_request_origin(&self.request)
            && matches!(&episode.signal, DiagnosticSignal::TransactionMetadataWait { key, statement_identity: Some(statement), mechanism }
                if key == &self.request.original.key && statement == &self.request.statement_identity && *mechanism == self.mechanism)
            && episode.source_reads.get(SourceReadQuestion::TransactionMetadata).is_some_and(|stage| {
                matches!(&stage.state, SourceReadStageState::Answered { answer }
                    if matches!(answer.as_ref(), SourceReadAnswer::TransactionMetadata(first) if self.matches_first(first)))
            })
    }
    pub(super) fn matches_input<'a>(
        &self,
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = (&'a DiagnosticEvidenceRef, &'a OperationalObservation)>,
    ) -> bool {
        let TelemetryRecord::Activity(actor) = &original.record else {
            return false;
        };
        let mut rows = rows.into_iter();
        let Some((reference, row)) = rows.next() else {
            return false;
        };
        let OperationalFact::PostgresTransactionMetadata(point) = &row.fact else {
            return false;
        };
        let Ok(context) = SourceReadExecutionScope::context_from_source(source) else {
            return false;
        };
        self.is_valid()
            && rows.next().is_none()
            && self.request.parent == *focus.basis
            && self.request.matches_parent(original_source, actor, &focus.basis)
            && reference == &self.previous
            && reference.validate_canonical_record(source, &TelemetryRecord::Operational(row.clone())).is_ok()
            && point.fits_source_scope(source)
            && point.focus == self.request.original.key
            && point.request_end == BlockingRequestState::SameStatement
            && LocalResourceReadScope::from_context(&context)
                .is_ok_and(|scope| scope.matches_context(&self.request_context()))
            && ObservationBounds::new(&context.observed_until, &self.request.original_observed_at)
                .is_some_and(|bounds| bounds.has_positive_duration())
    }
}
impl SourceReadFocus {
    /// Both observations delegate to the same participant grammar and native reader.
    pub fn transaction_metadata_participant(&self) -> Option<(&ActivityKey, &NativeIdentity, TransactionMetadataWait)> {
        if self.question != SourceReadQuestion::TransactionMetadata {
            return None;
        }
        match &self.prerequisite {
            SourceReadPrerequisite::ObservedTransactionMetadataWait { key, statement_identity, mechanism } => {
                Some((key, statement_identity, *mechanism))
            }
            SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } => Some((
                &continuation.request.original.key,
                &continuation.request.statement_identity,
                continuation.mechanism,
            )),
            _ => None,
        }
    }
    pub(super) fn from_transaction_metadata_continuation(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.lifecycle != DiagnosticLifecycle::Active
            || episode
                .source_reads
                .iter()
                .filter(|stage| stage.focus.question == SourceReadQuestion::TransactionMetadata)
                .count()
                != 1
        {
            return Ok(None);
        }
        let Some(first) = episode.source_reads.get(SourceReadQuestion::TransactionMetadata) else {
            return Ok(None);
        };
        let SourceReadStageState::Answered { answer } = &first.state else {
            return Ok(None);
        };
        let SourceReadAnswer::TransactionMetadata(answer) = answer.as_ref() else {
            return Ok(None);
        };
        let Some(previous) = &answer.observation else {
            return Ok(None);
        };
        let TelemetryRecord::Activity(actor) = &row.record else {
            return Ok(None);
        };
        if reference != &episode.latest {
            return Ok(None);
        }
        let Some(request) = episode.original_request_focus(source, actor, reference)? else {
            return Ok(None);
        };
        let continuation = TransactionMetadataContinuation {
            previous: previous.clone(),
            request: Box::new(request),
            mechanism: answer.mechanism,
        };
        if !continuation.matches_first(answer) || !continuation.matches_episode(episode) {
            return Ok(None);
        }
        let focus = Self {
            basis: Box::new(reference.clone()),
            target: source.target.clone(),
            question: SourceReadQuestion::TransactionMetadata,
            prerequisite: SourceReadPrerequisite::ObservedTransactionMetadataContinuation {
                continuation: Box::new(continuation),
            },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(Some(focus))
    }
}
