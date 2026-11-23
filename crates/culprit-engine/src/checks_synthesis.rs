//! One pure host adapter reuses the existing diagnosis pipeline and original-evidence interpreters.
//! Native acquisition, durable scheduling, authentication and publication stay with their existing owners.
#[cfg(feature = "collector")]
use culprit_collector::synthesis::{SynthesisInput, Synthesizer};
use culprit_core::{telemetry::*, *};
use culprit_evidence::analytical::evidence::EvidenceCapture;
use culprit_evidence::original_projection::OriginalProjection;
use std::sync::Arc;
mod causal_actors;
mod changes;
mod evaluation;
mod explanations;
mod finding_rows;
mod job_executions;
mod output_budget;
mod settings;
mod statement_counters;
use output_budget::OutputBudget;
mod incident_queries;
mod incidents;
mod queries;

// Advance this policy when retained interpretation changes, including builds sharing a package version.
pub(crate) const BUILTIN_RULES_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "/retained-diagnosis-6");

pub struct ChecksSynthesizer {
    pipeline: Arc<culprit_checks::Pipeline>,
    rules_version: String,
}
impl ChecksSynthesizer {
    /// The host retains its installed checks/stages and supplies their explicit versioned rules policy.
    pub fn new(pipeline: Arc<culprit_checks::Pipeline>, rules_version: impl Into<String>) -> Result<Self> {
        let rules_version = rules_version.into();
        if rules_version.is_empty() || rules_version.len() > 1024 || rules_version.chars().any(char::is_control) {
            return Err(invalid());
        }
        Ok(Self { pipeline, rules_version })
    }
    /// Analyze explicitly selected authenticated warehouse originals through the same retained-input pipeline.
    /// `primary` selects the output family; context keeps its own identity and clocks. This method performs no
    /// warehouse, recovery or source reads: callers retain their existing cumulative read budget and cancellation.
    /// Saved Gold wording is English; `recorded_at`, display metadata and the constructor's rules policy are explicit.
    pub fn synthesize_captures(
        &self,
        primary: &EvidenceCapture,
        context: &[&EvidenceCapture],
        recorded_at: &str,
        display: &DerivedDisplayContext,
    ) -> Result<DerivedDiagnosis> {
        if context.len() >= MAX_DERIVED_CAPTURES {
            return Err(capacity());
        }
        let originals = std::iter::once(primary)
            .chain(context.iter().copied())
            .map(EvidenceCapture::verified_original)
            .collect::<Result<Vec<_>>>()?;
        let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
        self.analyze_originals(&borrowed, recorded_at, display, vec![])
    }
    fn analyzer(&self) -> ComponentVersion {
        Self::builtin_analyzer()
    }
    pub(crate) fn builtin_analyzer() -> ComponentVersion {
        ComponentVersion::package(
            "culprit-checks",
            env!("CARGO_PKG_VERSION"),
            option_env!("CULPRIT_BUILD_REVISION").or(option_env!("GITHUB_SHA")),
        )
    }
    #[cfg(feature = "collector")]
    fn project(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let originals = input.captures().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
        let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
        self.analyze_originals(&borrowed, input.recorded_at(), input.display(), input.derived_context_issues())
    }
    fn analyze_originals(
        &self,
        originals: &[&dyn OriginalProjection],
        recorded_at: &str,
        display: &DerivedDisplayContext,
        context_issues: Vec<DerivedContextIssue>,
    ) -> Result<DerivedDiagnosis> {
        // One wording owner serves both authenticated intake adapters and restores the caller's locale.
        culprit_core::i18n::scope("en", || self.project_originals(originals, recorded_at, display, context_issues))
    }
    fn project_originals(
        &self,
        originals: &[&dyn OriginalProjection],
        recorded_at: &str,
        display: &DerivedDisplayContext,
        context_issues: Vec<DerivedContextIssue>,
    ) -> Result<DerivedDiagnosis> {
        let primary = *originals.first().ok_or_else(invalid)?;
        if !display.is_valid()
            || originals.len() > MAX_DERIVED_CAPTURES
            || originals.iter().enumerate().any(|(position, original)| {
                !original.original().is_valid()
                    || originals[..position].iter().any(|before| before.capture() == original.capture())
            })
            || originals
                .iter()
                .any(|original| matches!(original.source().observation, SourceObservation::DerivedDiagnosis(_)))
        {
            return Err(invalid());
        }
        let mut description = description(originals, recorded_at, self.analyzer(), &self.rules_version, display)?;
        description.context_issues = context_issues;
        let catalogs = crate::collected_evidence::catalogs::CatalogContext::from_originals(primary, originals)?;
        let changes = changes::ComparisonPlan::select(primary, originals)?;
        let job_executions = job_executions::JobExecutionPlan::select(primary)?;
        for &issue in catalogs.issues().iter().chain(&changes.issues) {
            if !description.context_issues.contains(&issue) {
                description.context_issues.push(issue);
            }
        }
        if let Some(issue) = settings::role_limitation(primary)
            && !description.context_issues.contains(&issue)
        {
            description.context_issues.push(issue);
        }
        if let Some(evaluation) = changes.evaluation() {
            description.change_evaluations.push(evaluation);
            description.output_coverage.changes = DerivedOutputCoverage::Evaluated;
        }
        if let Some(plan) = &job_executions {
            description.change_evaluations.push(plan.evaluation());
            description.output_coverage.changes = DerivedOutputCoverage::Evaluated;
        }
        description.analysis_id = description.expected_analysis_id()?;
        let mut output =
            DerivedDiagnosis { description: description.clone(), incidents: vec![], findings: vec![], changes: vec![] };
        let mut budget = OutputBudget::new(&output)?;
        if matches!(primary.source().observation, SourceObservation::Diagnostics(_)) {
            output.incidents = incidents::project(originals, &description, &mut budget)?;
            description.output_coverage.incidents = DerivedOutputCoverage::Evaluated;
        }
        if settings::supports(primary) {
            let projected = settings::findings(primary, &description, &self.pipeline, &mut budget)?;
            output.findings = projected.findings;
            description.finding_evaluations = projected.evaluations;
            description.output_coverage.findings = DerivedOutputCoverage::Evaluated;
        }
        if statement_counters::supports(primary) {
            description.finding_evaluations = statement_counters::evaluations(primary)?;
            // The input point is retained and assessed, but missing native application definitions still prevent
            // running query checks. Keep the existing visible AnalysisSourceUnavailable output limitation.
        }
        if primary.source().query_history().is_some() {
            let projected = queries::findings(primary, &description, &self.pipeline, &catalogs, &mut budget)?;
            output.findings = projected.findings;
            description.finding_evaluations = projected.evaluations;
            description.output_coverage.findings = DerivedOutputCoverage::Evaluated;
        }
        output.changes = changes.project(&description, &self.pipeline, &mut budget)?;
        if let Some(plan) = job_executions {
            output.changes.extend(plan.project(&description, &mut budget)?);
        }
        description.counts = DerivedDiagnosisCounts {
            incidents: output.incidents.len().try_into().map_err(|_| capacity())?,
            findings: output.findings.len().try_into().map_err(|_| capacity())?,
            changes: output.changes.len().try_into().map_err(|_| capacity())?,
        };
        budget.admit_header_growth(&output.description, &description)?;
        output.description = description;
        output.validate()?;
        Ok(output)
    }
}
#[cfg(feature = "collector")]
impl Synthesizer for ChecksSynthesizer {
    fn analyzer(&self) -> ComponentVersion {
        ChecksSynthesizer::analyzer(self)
    }
    fn rules_version(&self) -> &str {
        &self.rules_version
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        self.project(input)
    }
}
fn description(
    originals: &[&dyn OriginalProjection],
    recorded_at: &str,
    analyzer: ComponentVersion,
    rules_version: &str,
    display: &DerivedDisplayContext,
) -> Result<DerivedDiagnosisDescription> {
    let mut from = None;
    let mut until = None;
    let mut inputs = Vec::with_capacity(originals.len());
    for original in originals {
        let start = chrono::DateTime::parse_from_rfc3339(original.collected_from()).map_err(|_| invalid())?;
        let end = chrono::DateTime::parse_from_rfc3339(original.collected_until()).map_err(|_| invalid())?;
        if start > end {
            return Err(invalid());
        }
        if from.as_ref().is_none_or(|current| start < *current) {
            from = Some(start);
        }
        if until.as_ref().is_none_or(|current| end > *current) {
            until = Some(end);
        }
        let catalog = original
            .source()
            .observation
            .snapshot()
            .filter(|snapshot| {
                matches!(
                    snapshot.dataset,
                    SnapshotDataset::Tables
                        | SnapshotDataset::Indexes
                        | SnapshotDataset::Columns
                        | SnapshotDataset::Statistics
                        | SnapshotDataset::Settings
                        | SnapshotDataset::ForeignKeys
                        | SnapshotDataset::CheckConstraints
                        | SnapshotDataset::Objects
                        | SnapshotDataset::Triggers
                        | SnapshotDataset::Partitions
                )
            })
            .map(|snapshot| -> Result<_> {
                Ok(DerivedCatalogProvenance {
                    observed_from: original.collected_from().into(),
                    observed_until: original.collected_until().into(),
                    native_time: Some(DerivedCatalogTime::Utc { at: canonical_time(&snapshot.observed_until)? }),
                })
            })
            .transpose()?;
        let role = if matches!(original.source().observation, SourceObservation::Diagnostics(_)) {
            DerivedInputRole::Revision
        } else if catalog.is_some() {
            DerivedInputRole::Catalog
        } else {
            DerivedInputRole::Observation
        };
        inputs.push(DerivedInputReference { capture: original.original().clone(), role, catalog });
    }
    let unavailable = |reason| DerivedOutputCoverage::ContextUnavailable { reason };
    let mut description = DerivedDiagnosisDescription {
        analysis_id: String::new(),
        analyzer,
        rules_version: rules_version.into(),
        language: "en".into(),
        display: display.clone(),
        recorded_at: canonical_time(recorded_at)?,
        observed_from: canonical_time(&from.ok_or_else(invalid)?.to_rfc3339())?,
        observed_until: canonical_time(&until.ok_or_else(invalid)?.to_rfc3339())?,
        inputs,
        origin: AnalysisOrigin::RetainedInput,
        input_read_complete: originals.iter().all(|original| original.is_read_complete()),
        finding_evaluations: Default::default(),
        change_evaluations: vec![],
        context_issues: vec![],
        counts: DerivedDiagnosisCounts::default(),
        output_coverage: DerivedOutputCoverageSet {
            incidents: unavailable(DerivedOutputGap::NoDiagnosticRevisions),
            findings: unavailable(DerivedOutputGap::AnalysisSourceUnavailable),
            changes: unavailable(DerivedOutputGap::CatalogComparisonUnavailable),
        },
    };
    description.analysis_id = description.expected_analysis_id()?;
    Ok(description)
}
fn invalid() -> Error {
    DeliveryProblem::Integrity.error()
}
fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}

#[cfg(all(test, feature = "collector"))]
mod tests;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod warehouse_tests;

#[cfg(test)]
mod causal_tests;

#[cfg(test)]
mod catalog_tests;

#[cfg(test)]
mod job_execution_tests;

#[cfg(test)]
mod metadata_queue_tests;

#[cfg(test)]
mod settings_tests;

#[cfg(test)]
mod statement_counter_tests;
