//! One closed, nonrecursive reference walk for local synthesis and warehouse incident assembly.
//! Naming an original is not authentication; each caller retains its existing verified read/session owner.
use crate::{Result, telemetry::*};
use serde::Serialize;
use std::collections::BTreeSet;
use ts_rs::TS;

pub const MAX_DIAGNOSTIC_ANCESTRY_LINKS: usize = 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, TS)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DiagnosticAncestryRole {
    Input,
    SignalEvidence,
    ReportedEvidence,
    AnswerEvidence,
    FirstObservation,
    LatestObservation,
    PeakObservation,
    SourceBasis,
    SourceResult,
    Recovery,
    Investigation,
    SharedExecutionOwner,
    AnalysisObservation,
    AnalysisCatalog,
    AnalysisRevision,
    AnalysisRelatedEvidence,
}
#[derive(Clone, Copy, Debug)]
pub enum DiagnosticAncestryTarget<'a> {
    Record(&'a DiagnosticEvidenceRef),
    Capture(&'a DiagnosticCaptureRef),
}
#[derive(Clone, Copy, Debug)]
pub struct DiagnosticAncestryReference<'a> {
    pub role: DiagnosticAncestryRole,
    pub target: DiagnosticAncestryTarget<'a>,
}
impl DiagnosticRevision {
    pub fn referenced_evidence(&self) -> Result<Vec<DiagnosticAncestryReference<'_>>> {
        if !self.is_valid() {
            return Err(DeliveryProblem::Integrity.error());
        }
        let mut walk = References::default();
        for reference in &self.inputs {
            walk.record(DiagnosticAncestryRole::Input, reference)?;
        }
        let episode = &self.episode;
        walk.record(DiagnosticAncestryRole::FirstObservation, &episode.first)?;
        walk.record(DiagnosticAncestryRole::LatestObservation, &episode.latest)?;
        if let Some(peak) = &episode.peak {
            walk.record(DiagnosticAncestryRole::PeakObservation, peak)?;
        }
        for reference in episode.signal.references() {
            walk.record(DiagnosticAncestryRole::SignalEvidence, reference)?;
        }
        if let Some(report) = &episode.report {
            for reference in report.references() {
                walk.record(DiagnosticAncestryRole::ReportedEvidence, reference)?;
            }
        }
        for stage in episode.source_reads.iter() {
            for reference in stage.focus.references() {
                walk.record(DiagnosticAncestryRole::SourceBasis, reference)?;
            }
            if let Some(result) = stage.result() {
                walk.capture(DiagnosticAncestryRole::SourceResult, result)?;
            }
            if let SourceReadStageState::Answered { answer } = &stage.state {
                for reference in answer.references() {
                    walk.record(DiagnosticAncestryRole::AnswerEvidence, reference)?;
                }
            }
        }
        if let Some(proof) = &episode.recovery {
            walk.capture(DiagnosticAncestryRole::Recovery, proof.capture())?;
            if let DiagnosticRecoveryProof::FocusedParticipantGone { parent, .. } = proof {
                walk.record(DiagnosticAncestryRole::Recovery, parent)?;
            }
        }
        if let Some(event) = &self.investigation {
            for reference in event.intent.focus.references() {
                walk.record(DiagnosticAncestryRole::Investigation, reference)?;
            }
            if let DiagnosticExecutionBinding::Shared { owner } = &event.intent.execution {
                walk.record(DiagnosticAncestryRole::SharedExecutionOwner, owner)?;
            }
            if let DiagnosticInvestigationOutcome::Captured { capture } = &event.outcome {
                walk.capture(DiagnosticAncestryRole::Investigation, capture)?;
            }
        }
        Ok(walk.values)
    }
}
#[derive(Default)]
struct References<'a> {
    values: Vec<DiagnosticAncestryReference<'a>>,
    keys: BTreeSet<(DiagnosticAncestryRole, bool, String)>,
}
impl<'a> References<'a> {
    fn record(&mut self, role: DiagnosticAncestryRole, reference: &'a DiagnosticEvidenceRef) -> Result<()> {
        self.insert(role, true, reference.key()?, DiagnosticAncestryTarget::Record(reference))
    }
    fn capture(&mut self, role: DiagnosticAncestryRole, capture: &'a DiagnosticCaptureRef) -> Result<()> {
        self.insert(role, false, capture.key()?, DiagnosticAncestryTarget::Capture(capture))
    }
    fn insert(
        &mut self,
        role: DiagnosticAncestryRole,
        is_record: bool,
        key: String,
        target: DiagnosticAncestryTarget<'a>,
    ) -> Result<()> {
        if self.keys.insert((role, is_record, key)) {
            if self.values.len() >= MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                return Err(DeliveryProblem::Capacity.error());
            }
            self.values.push(DiagnosticAncestryReference { role, target });
        }
        Ok(())
    }
}
