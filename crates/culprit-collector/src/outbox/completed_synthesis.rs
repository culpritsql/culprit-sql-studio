//! Synthesis selects authenticated originals through the existing cumulative canonical worker.
use super::{
    Files, OutboxDelivery,
    completed_capture::{CaptureWork, CompletedCapture, CompletedCaptureRead},
};
use crate::{
    outbox::artifacts::VerificationObserver,
    synthesis::{SynthesisInput, SynthesisRequest, Synthesizer},
};
use culprit_core::{Result, limits::OperationHandle, telemetry::*};
use std::sync::Arc;

/// Immutable native evidence. Only the serialized Outbox verifier creates this owner.
pub struct SynthesisCapture {
    completed: CompletedCapture,
}

impl SynthesisCapture {
    pub fn connection(&self) -> &str {
        &self.completed.connection
    }
    pub fn source(&self) -> &CollectionDescription {
        &self.completed.source
    }
    pub fn original(&self) -> &DiagnosticCaptureRef {
        &self.completed.capture
    }
    pub fn records(&self) -> &[ExportedRecord] {
        &self.completed.records
    }
    pub fn row_ordinals(&self) -> &[u32] {
        &self.completed.row_ordinals
    }
    pub fn captured_rows(&self, stream: TelemetryStream) -> Option<u32> {
        self.completed.streams.iter().find(|entry| entry.stream == stream).map(|entry| entry.rows)
    }
    pub fn reference_coverage(&self) -> &ReferenceCoverage {
        &self.completed.references
    }
    pub fn collected_from(&self) -> &str {
        &self.completed.collected_from
    }
    pub fn collected_until(&self) -> &str {
        &self.completed.collected_until
    }
    /// Canonical EOF verification includes every row. Only rows with a resolved semantic ID can supply
    /// a row-level reference; unresolved rows remain available as capture-wide evidence.
    pub fn row_references(&self) -> impl Iterator<Item = Result<DiagnosticEvidenceRef>> + '_ {
        self.records()
            .iter()
            .enumerate()
            .filter(|(_, row)| row.record_id.is_some())
            .map(|(position, _)| self.reference(position))
    }
    pub fn reference(&self, position: usize) -> Result<DiagnosticEvidenceRef> {
        let row = self.completed.records.get(position).ok_or_else(integrity)?;
        let ordinal = self.completed.row_ordinals.get(position).ok_or_else(integrity)?;
        DiagnosticEvidenceRef::from_export(
            &self.completed.source,
            &self.completed.capture.manifest_sha256,
            row,
            *ordinal,
        )
    }
}

impl OutboxDelivery {
    /// Every requested capture shares one row/byte allowance, deadline and serialized verification owner.
    /// The projection receives owned originals only after all selected artifact proofs have completed.
    pub async fn with_synthesis_inputs<T: Send + 'static>(
        &self,
        request: SynthesisRequest,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
        project: impl FnOnce(SynthesisInput) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.with_synthesis_worker(request, operation, worker_guard, move |_, _, input| {
            project(input).map(CaptureWork::Read)
        })
        .await
    }

    /// The injected analyzer receives no database/provider handles. Publication follows the normal sealed path.
    pub async fn synthesize(
        &self,
        request: SynthesisRequest,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
        synthesizer: Arc<dyn Synthesizer>,
    ) -> Result<DiagnosticCaptureRef> {
        self.with_synthesis_worker(request, operation, worker_guard, move |files, read, input| {
            files.synthesize_input(read, input, synthesizer.as_ref(), None).map(CaptureWork::Committed)
        })
        .await
    }

    pub(super) async fn with_synthesis_worker<T: Send + 'static>(
        &self,
        request: SynthesisRequest,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
        project: impl FnOnce(&mut Files, &mut CompletedCaptureRead, SynthesisInput) -> Result<CaptureWork<T>>
        + Send
        + 'static,
    ) -> Result<T> {
        request.validate()?;
        let read = CompletedCaptureRead::synthesis_reference(request.primary, request.budget, operation)?;
        self.with_capture_operation(read, worker_guard, move |files, read| {
            let primary = files.synthesis_capture(read)?;
            let mut context = Vec::with_capacity(request.context.len());
            for reference in request.context {
                read.select_synthesis_reference(reference)?;
                context.push(files.synthesis_capture(read)?);
            }
            let input = SynthesisInput::new(primary, context, chrono::Utc::now().to_rfc3339())?;
            project(files, read, input)
        })
        .await
    }
}

impl Files {
    pub(super) fn synthesis_capture(&mut self, read: &mut CompletedCaptureRead) -> Result<SynthesisCapture> {
        Ok(SynthesisCapture { completed: self.completed_capture(read)? })
    }
    pub(super) fn synthesize_input(
        &mut self,
        read: &mut CompletedCaptureRead,
        input: SynthesisInput,
        synthesizer: &dyn Synthesizer,
        registration: Option<&crate::synthesis::SynthesisRegistration>,
    ) -> Result<DiagnosticCaptureRef> {
        if registration.is_some_and(|identity| !identity.matches(synthesizer)) {
            return Err(DeliveryProblem::Conflict.error());
        }
        let analyzer = synthesizer.analyzer();
        let rules_version = synthesizer.rules_version().to_owned();
        let diagnosis = culprit_core::i18n::scope("en", || synthesizer.synthesize(&input))?;
        if diagnosis.description.rules_version != rules_version
            || synthesizer.analyzer() != analyzer
            || synthesizer.rules_version() != rules_version
            || registration.is_some_and(|identity| !identity.matches(synthesizer))
        {
            return Err(DeliveryProblem::Conflict.error());
        }
        read.check()?;
        self.commit_synthesized_output(&input, diagnosis, &analyzer, &|| read.check())
    }
}

fn integrity() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
