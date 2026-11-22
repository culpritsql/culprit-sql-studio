//! An authenticated point answers one worker-resource question without declaring exclusive cause or recovery.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkerStarvationAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub point: DiagnosticEvidenceRef,
    pub worker_scope: SharedResourceIdentity,
    pub observed_at: String,
    pub source_state: crate::SourceState,
    pub evidence: WorkerStarvationEvidence,
}
impl WorkerStarvationAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let SourceObservation::SourceRead { snapshot, focus_key, execution } = &source.observation else {
            return Err(invalid());
        };
        let scope = SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, execution)?;
        if focus.question != SourceReadQuestion::WorkerStarvation
            || !focus.matches_basis(original_source, original, &scope)
            || focus.key()? != *focus_key
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || !capture.is_valid()
            || source.captured_range()?.scope_id != capture.source_scope_id
            || snapshot.records != 1
            || rows.len() != 1
        {
            return Err(invalid());
        }
        let (reference, row) = rows[0];
        if reference.capture_ref() != *capture || reference.validate_canonical_record(source, &row.record).is_err() {
            return Err(invalid());
        }
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::MssqlWorkerStarvation(point),
            ..
        }) = &row.record
        else {
            return Err(invalid());
        };
        if !point.fits_source(source) {
            return Err(invalid());
        }
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            point: reference.clone(),
            worker_scope: point.worker_scope.clone(),
            observed_at: point.observed_at.clone(),
            source_state: snapshot.coverage_state()?,
            evidence: WorkerStarvationEvidence::from_point(point)?,
        };
        answer.is_valid().then_some(answer).ok_or_else(invalid)
    }
    pub fn is_valid(&self) -> bool {
        let Ok(execution) =
            SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &self.result_context)
        else {
            return false;
        };
        self.basis.is_valid()
            && self.result.is_valid()
            && self.point.is_valid()
            && self.result.capture != self.basis.capture
            && self.point.capture_ref() == self.result
            && execution.matches_original_context(&self.basis_context)
            && ObservationBounds::new(&self.basis_context.observed_until, &self.result_context.observed_from).is_some()
            && self.worker_scope.is_valid()
            && self.worker_scope.engine == crate::Engine::Mssql
            && matches!(&self.result_context.partition,SourcePartition::InstanceLocal{member:Some(member),epoch:Some(_)} if member==&self.worker_scope)
            && ObservationBounds::new(&self.result_context.observed_from, &self.result_context.observed_until)
                .is_some_and(|bounds| bounds.contains(&self.observed_at))
            && self.point.stream == TelemetryStream::OperationalSamples
            && matches!(&self.point.native_key,NativeRecordIdentity::Operational{source,..} if source==crate::source_id::WORKER_QUEUE)
            && matches!(self.source_state, crate::SourceState::Collected | crate::SourceState::Partial)
            && self.evidence.is_valid()
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        [&self.basis, &self.point].into_iter()
    }
}
impl DiagnosticEpisode {
    pub fn complete_worker_starvation_read(
        &self,
        intent: &DiagnosticInvestigationIntent,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        if !intent.matches_episode(self) || !intent.matches_result_source(source) {
            return Err(invalid());
        }
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { return Err(invalid()) };
        let answer = WorkerStarvationAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::WorkerStarvation(Box::new(answer))) },
        )
    }
}
