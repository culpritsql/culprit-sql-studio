//! One selected metadata-ticket question preserves candidates and the native pending-edge limitation.
use super::*;
use crate::blocking::{MetadataQueueEvidence, MetadataQueueIssue, MetadataQueueStatus};
use std::collections::BTreeSet;

impl ActivityRequestFocus {
    pub fn is_metadata_lock_focus(&self) -> bool {
        self.is_blocking_focus() && self.target.engine()==crate::Engine::Mysql
            && self.source=="mysql-current-activity"
            && self.original.key.request.as_ref().is_some_and(|request|
                crate::engines::mysql_activity_statement_identity(&self.original.key.session,request).as_ref()==Some(&self.statement_identity))
            // Native foreground state, not SQL words or an elapsed-time inference.
            && self.original.wait.as_ref().and_then(|wait|wait.native_type.as_deref())
                .is_some_and(|native|native.eq_ignore_ascii_case("Waiting for table metadata lock"))
    }
}
impl SourceReadFocus {
    pub fn for_metadata_lock_request(request: ActivityRequestFocus) -> Result<Self> {
        let focus = Self {
            basis: Box::new(request.parent.clone()),
            target: request.target.clone(),
            question: SourceReadQuestion::MetadataLockContext,
            prerequisite: SourceReadPrerequisite::ObservedMetadataLockWait { request: Box::new(request) },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(focus)
    }
    pub fn metadata_lock_request(&self) -> Option<&ActivityRequestFocus> {
        if self.question != SourceReadQuestion::MetadataLockContext || !self.is_valid() {
            return None;
        }
        let SourceReadPrerequisite::ObservedMetadataLockWait { request } = &self.prerequisite else {
            return None;
        };
        (request.parent == *self.basis && request.target == self.target).then_some(request)
    }
    pub(super) fn from_metadata_lock_activity(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.source_reads.get(SourceReadQuestion::MetadataLockContext).is_some()
            || reference != &episode.first
            || !matches!(episode.signal, DiagnosticSignal::LockWait { .. })
        {
            return Ok(None);
        }
        let TelemetryRecord::Activity(original) = &row.record else {
            return Ok(None);
        };
        reference.validate_canonical_record(source, &row.record)?;
        if reference.capture != row.capture
            || reference.batch_id != row.batch_id
            || reference.payload_sha256 != row.payload_sha256()?
        {
            return Err(invalid());
        }
        let Some(request) = episode.original_request_focus(source, original, reference)? else {
            return Ok(None);
        };
        if !request.is_metadata_lock_focus() {
            return Ok(None);
        }
        Ok(Some(Self::for_metadata_lock_request(request)?))
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataLockContextAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub source_state: crate::SourceState,
    /// Whole authenticated selected Activity cohort, including zero rows for an ended original.
    pub observations: Vec<DiagnosticEvidenceRef>,
    pub evidence: MetadataQueueEvidence,
}
impl MetadataLockContextAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let request = focus.metadata_lock_request().ok_or_else(invalid)?;
        let SourceObservation::MetadataLockContext { snapshot, focus_key, execution, result } = &source.observation
        else {
            return Err(invalid());
        };
        let scope = SourceReadExecutionScope::from_context(focus.question, execution)?;
        if !focus.matches_basis(original_source, original, &scope)
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || *focus_key != focus.key()?
            || &result.request.focus != request
            || !capture.is_valid()
            || capture.capture == focus.basis.capture
            || source.captured_range()?.scope_id != capture.source_scope_id
            || rows.len() != snapshot.observations as usize
            || rows.len() > MAX_BLOCKING_PARTICIPANTS as usize
        {
            return Err(invalid());
        }
        let mut references = BTreeSet::new();
        let mut borrowed = Vec::new();
        for (ordinal, (reference, row)) in rows.iter().enumerate() {
            reference.validate_canonical_record(source, &row.record)?;
            if reference.row_ordinal != ordinal as u32
                || reference.capture_ref() != *capture
                || reference.capture != row.capture
                || reference.batch_id != row.batch_id
                || reference.payload_sha256 != row.payload_sha256()?
                || !references.insert(reference.key()?)
            {
                return Err(invalid());
            }
            let TelemetryRecord::Activity(observation) = &row.record else {
                return Err(invalid());
            };
            borrowed.push(ActivityFrameRow { observation, reference: Some(*reference) });
        }
        let frame = ActivityFrameEvidence::new(source, &borrowed, true)?;
        if !result.accepts_frame(&frame) {
            return Err(invalid());
        }
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            source_state: snapshot.coverage.state,
            observations: rows.iter().map(|(reference, _)| (*reference).clone()).collect(),
            evidence: crate::blocking::metadata_queue(&frame)?,
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        let Ok(scope) = LocalResourceReadScope::from_context(&self.result_context) else {
            return false;
        };
        let mut keys = BTreeSet::new();
        self.basis.is_valid()
            && self.basis.stream == TelemetryStream::ActivityObservations
            && self.result.is_valid()
            && self.result.capture != self.basis.capture
            && self.result.source_scope_id != self.basis.source_scope_id
            && scope.matches_original_context(&self.basis_context)
            && ObservationBounds::new(&self.basis_context.observed_until, &self.result_context.observed_from).is_some()
            && self.observations.len() <= MAX_BLOCKING_PARTICIPANTS as usize
            && self.observations.iter().all(|reference| {
                reference.is_valid()
                    && reference.stream == TelemetryStream::ActivityObservations
                    && reference.capture_ref() == self.result
                    && reference.key().is_ok_and(|key| keys.insert(key))
            })
            && self.evidence.status == MetadataQueueStatus::Unavailable
            && self.evidence.cascades.is_empty()
            && self.evidence.issues.contains(&MetadataQueueIssue::PendingBlockerNotExposed)
            && self.evidence.candidates.len() <= MAX_BLOCKING_PARTICIPANTS as usize
            && self.evidence.candidates.iter().all(|candidate| candidate.is_valid())
    }
    pub fn matches_prerequisite(&self, prerequisite: &SourceReadPrerequisite) -> bool {
        matches!(prerequisite,SourceReadPrerequisite::ObservedMetadataLockWait {request}
            if request.is_metadata_lock_focus() && request.parent==self.basis && request.target==self.basis_context.target
                && request.partition==self.basis_context.partition && request.database_writable==self.basis_context.database_writable
                && ObservationBounds::new(&self.basis_context.observed_from,&self.basis_context.observed_until)
                    .is_some_and(|bounds|bounds.contains(&request.original_observed_at)))
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        std::iter::once(&self.basis).chain(&self.observations)
    }
}
impl DiagnosticEpisode {
    pub fn complete_metadata_lock_read(
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
            MetadataLockContextAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::MetadataLockContext(Box::new(answer))),
            },
        )
    }
}
