//! Pure diagnosis consumes authenticated, retained observations after native collection has settled.
//! The host supplies its existing analysis engine; this boundary exposes no provider or credentials.
use crate::outbox::CompletedCaptureBudget;
pub use crate::outbox::SynthesisCapture;
use culprit_core::{ComponentVersion, Result, telemetry::*};
mod automatic;
mod context;
mod validation;
pub use automatic::*;
pub use context::*;

/// Implementations reuse the host's pure checks and plan analysis. They cannot acquire a source through this API.
pub trait Synthesizer: Send + Sync {
    fn analyzer(&self) -> ComponentVersion;
    fn rules_version(&self) -> &str;
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis>;
}

/// Context keeps each original's own scope and clock. Its presence alone establishes no causal relationship.
pub struct SynthesisInput {
    primary: SynthesisCapture,
    context: Vec<SynthesisCapture>,
    recorded_at: String,
    display: DerivedDisplayContext,
    context_issues: Vec<SynthesisContextIssue>,
}

impl SynthesisInput {
    pub(crate) fn new(primary: SynthesisCapture, context: Vec<SynthesisCapture>, recorded_at: String) -> Result<Self> {
        Self::new_with_context_issues(primary, context, recorded_at, vec![])
    }
    pub(crate) fn new_with_context_issues(
        primary: SynthesisCapture,
        context: Vec<SynthesisCapture>,
        recorded_at: String,
        context_issues: Vec<SynthesisContextIssue>,
    ) -> Result<Self> {
        if context_issues.len() > MAX_SYNTHESIS_CONTEXT_ISSUES {
            return Err(DeliveryProblem::Capacity.error());
        }
        let display = DerivedDisplayContext {
            connection_alias: primary.connection().into(),
            environment: None,
            database_name: None,
        };
        let input = Self { primary, context, recorded_at: canonical_time(&recorded_at)?, display, context_issues };
        validate_capture_references(&input.captures().map(SynthesisCapture::original).collect::<Vec<_>>())?;
        Ok(input)
    }
    pub fn context_issues(&self) -> &[SynthesisContextIssue] {
        &self.context_issues
    }
    pub fn derived_context_issues(&self) -> Vec<culprit_core::DerivedContextIssue> {
        let mut issues = Vec::new();
        for issue in self.context_issues.iter().map(SynthesisContextIssue::derived_issue) {
            if !issues.contains(&issue) {
                issues.push(issue);
            }
        }
        issues
    }
    pub fn primary(&self) -> &SynthesisCapture {
        &self.primary
    }
    pub fn context(&self) -> &[SynthesisCapture] {
        &self.context
    }
    pub fn recorded_at(&self) -> &str {
        &self.recorded_at
    }
    pub fn display(&self) -> &DerivedDisplayContext {
        &self.display
    }
    pub fn captures(&self) -> impl Iterator<Item = &SynthesisCapture> {
        std::iter::once(&self.primary).chain(&self.context)
    }
}

/// One cumulative local allowance covers original verification and analysis; it never widens a native budget.
pub struct SynthesisRequest {
    pub primary: DiagnosticCaptureRef,
    pub context: Vec<DiagnosticCaptureRef>,
    pub budget: CompletedCaptureBudget,
}

impl SynthesisRequest {
    pub fn validate(&self) -> Result<()> {
        self.budget.validate()?;
        validate_capture_references(&std::iter::once(&self.primary).chain(&self.context).collect::<Vec<_>>())
    }
}

fn validate_capture_references(captures: &[&DiagnosticCaptureRef]) -> Result<()> {
    if captures.is_empty() || captures.len() > MAX_DERIVED_CAPTURES {
        return Err(DeliveryProblem::Capacity.error());
    }
    for (position, capture) in captures.iter().enumerate() {
        if !capture.is_valid() {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        if captures[..position].iter().any(|before| before.capture == capture.capture) {
            return Err(DeliveryProblem::Conflict.error());
        }
    }
    Ok(())
}

pub(crate) fn synthesis_metadata_key(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_vec(value).map(|bytes| digest(&bytes)).map_err(|_| DeliveryProblem::InvalidContract.error())
}
