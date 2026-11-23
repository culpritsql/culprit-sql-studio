//! A result-output request can ask about its reservation; the answer never identifies a slow client or network cause.
use super::*;
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OutputGrantRetentionAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub source_state: crate::SourceState,
    /// Exact matched native reservation, not a claim that an output wait was still present or caused a peer's wait.
    #[serde(deserialize_with = "Option::deserialize")]
    pub subject: Option<DiagnosticEvidenceRef>,
    pub peers: Vec<DiagnosticEvidenceRef>,
    /// Native current-wait duration at the original and later point. No start time or continuity is inferred.
    #[serde(deserialize_with = "Option::deserialize")]
    pub original_wait_ms: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub observed_output_wait_ms: Option<String>,
}
impl SourceReadFocus {
    pub fn output_request(&self) -> Option<&ActivityRequestFocus> {
        if self.question != SourceReadQuestion::OutputGrantRetention || !self.is_valid() {
            return None;
        }
        let SourceReadPrerequisite::ObservedClientOutputWait { request } = &self.prerequisite else {
            return None;
        };
        (request.parent == *self.basis && request.target == self.target).then_some(request)
    }
    /// The existing episode/request envelope owner validates the original native generations and row receipt.
    pub(super) fn from_output_activity(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.source_reads.get(SourceReadQuestion::OutputGrantRetention).is_some()
            || !matches!(&episode.signal, DiagnosticSignal::ClientResultOutput { statement_identity: Some(_), .. })
            || reference != &episode.first
        {
            return Ok(None);
        }
        let TelemetryRecord::Activity(original) = &row.record else { return Ok(None) };
        if reference.capture != row.capture
            || reference.batch_id != row.batch_id
            || reference.payload_sha256 != row.payload_sha256()?
        {
            return Err(invalid());
        }
        let Some(request) = episode.original_request_focus(source, original, reference)? else { return Ok(None) };
        if !request.is_output_memory_focus() {
            return Ok(None);
        }
        let focus = Self {
            basis: Box::new(reference.clone()),
            target: source.target.clone(),
            question: SourceReadQuestion::OutputGrantRetention,
            prerequisite: SourceReadPrerequisite::ObservedClientOutputWait { request: Box::new(request) },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(Some(focus))
    }
}
impl OutputGrantRetentionAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let request = focus.output_request().ok_or_else(invalid)?;
        let SourceObservation::SourceRead { snapshot, focus_key, execution } = &source.observation else {
            return Err(invalid());
        };
        let scope = SourceReadExecutionScope::from_context(focus.question, execution)?;
        if !focus.matches_basis(original_source, original, &scope)
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || *focus_key != focus.key()?
            || snapshot.dataset != SnapshotDataset::CurrentRequestMemory
            || !capture.is_valid()
            || capture.capture == focus.basis.capture
            || source.captured_range()?.scope_id != capture.source_scope_id
            || rows.len() != snapshot.records as usize
            || rows.len() > MAX_REQUEST_MEMORY_OBSERVATIONS
        {
            return Err(invalid());
        }
        let mut subjects = vec![];
        let mut grants = vec![];
        let mut references = BTreeSet::new();
        for (reference, row) in rows {
            reference.validate_canonical_record(source, &row.record)?;
            if reference.capture_ref() != *capture
                || reference.capture != row.capture
                || reference.batch_id != row.batch_id
                || reference.payload_sha256 != row.payload_sha256()?
                || !references.insert(reference.key()?)
            {
                return Err(invalid());
            }
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::ExecutionGrant(grant),
                ..
            }) = &row.record
            else {
                return Err(invalid());
            };
            if !grant.fits_source(source)
                || !grant.is_admission_participant(&request.target.database().ok_or_else(invalid)?.database)
                || !ObservationBounds::new(&execution.observed_from, &execution.observed_until)
                    .is_some_and(|bounds| bounds.contains(&grant.observed_at))
            {
                return Err(invalid());
            }
            if grant.request.as_ref().is_some_and(|actor| {
                actor.key == request.original.key
                    && actor.statement_identity.as_ref() == Some(&request.statement_identity)
            }) {
                subjects.push((*reference, grant.as_ref()));
            }
            grants.push((*reference, grant.as_ref()));
        }
        if subjects.len() > 1 {
            return Err(invalid());
        }
        let subject = subjects.first().copied();
        let peers = if let Some((_, subject)) = subject {
            grants
                .iter()
                .filter(|(_, grant)| grant.identity != subject.identity)
                .map(|(reference, grant)| {
                    if !subject
                        .shares_admission_resource(grant, &request.target.database().ok_or_else(invalid)?.database)
                    {
                        return Err(invalid());
                    }
                    Ok((*reference).clone())
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            if !grants.is_empty() {
                return Err(invalid());
            }
            vec![]
        };
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            source_state: snapshot.coverage_state()?,
            subject: subject.map(|(reference, _)| reference.clone()),
            peers,
            original_wait_ms: request.original.wait.as_ref().and_then(|wait| wait.elapsed_ms.clone()),
            observed_output_wait_ms: subject
                .and_then(|(_, grant)| output_wait_ms(grant, source.target.engine()))
                .map(str::to_owned),
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        let Ok(scope) = LocalResourceReadScope::from_context(&self.result_context) else { return false };
        let mut references = BTreeSet::new();
        self.basis.is_valid()
            && self.basis.stream == TelemetryStream::ActivityObservations
            && self.result.is_valid()
            && self.result.capture != self.basis.capture
            && self.result.source_scope_id != self.basis.source_scope_id
            && scope.matches_original_context(&self.basis_context)
            && ObservationBounds::new(&self.basis_context.observed_until, &self.result_context.observed_from).is_some()
            && self.peers.len() < MAX_REQUEST_MEMORY_OBSERVATIONS
            && (self.subject.is_some() || self.peers.is_empty() && self.observed_output_wait_ms.is_none())
            && self.subject.iter().chain(&self.peers).all(|reference| {
                reference.is_valid()
                    && reference.stream == TelemetryStream::OperationalSamples
                    && reference.capture_ref() == self.result
                    && reference.key().is_ok_and(|key| references.insert(key))
            })
            && [&self.original_wait_ms, &self.observed_output_wait_ms]
                .into_iter()
                .flatten()
                .all(|value| value.parse::<u64>().is_ok_and(|count| count.to_string() == *value))
            && match self.source_state {
                crate::SourceState::Collected => self.subject.is_some(),
                crate::SourceState::Partial => true,
                crate::SourceState::Empty | crate::SourceState::Unavailable | crate::SourceState::NotApplicable => {
                    self.subject.is_none()
                }
            }
    }
    pub fn matches_subject_wait(&self, grant: &ExecutionGrantObservation, engine: crate::Engine) -> bool {
        self.observed_output_wait_ms.as_deref() == output_wait_ms(grant, engine)
    }
    pub fn matches_prerequisite(&self, prerequisite: &SourceReadPrerequisite) -> bool {
        matches!(prerequisite,SourceReadPrerequisite::ObservedClientOutputWait {request}
            if request.is_output_memory_focus() && request.parent==self.basis
                && request.target == self.basis_context.target
                && request.partition == self.basis_context.partition
                && request.database_writable == self.basis_context.database_writable
                && ObservationBounds::new(&self.basis_context.observed_from, &self.basis_context.observed_until)
                    .is_some_and(|bounds| bounds.contains(&request.original_observed_at))
                && request.original.wait.as_ref().and_then(|wait|wait.elapsed_ms.as_ref())==self.original_wait_ms.as_ref())
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        std::iter::once(&self.basis).chain(self.subject.iter()).chain(&self.peers)
    }
}
fn output_wait_ms(grant: &ExecutionGrantObservation, engine: crate::Engine) -> Option<&str> {
    grant
        .request
        .as_ref()?
        .wait
        .as_ref()
        .filter(|wait| {
            wait.native_type.as_deref().is_some_and(|native| {
                engine.spec().native_wait_mechanism(native) == Some(WaitMechanism::ClientResultOutput)
            })
        })?
        .elapsed_ms
        .as_deref()
}
impl DiagnosticEpisode {
    pub fn complete_output_grant_read(
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
            OutputGrantRetentionAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::OutputGrantRetention(Box::new(answer))),
            },
        )
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum OutputGrantRetentionStatus {
    Observed,
    NotEstablished,
    ContextUnavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OutputGrantRetentionReport {
    pub status: OutputGrantRetentionStatus,
    pub subject_key: Option<String>,
    pub memory: crate::MemoryParticipantsReport,
    pub summary: String,
    pub investigation: String,
    pub verify: String,
    pub limitations: Vec<String>,
}
