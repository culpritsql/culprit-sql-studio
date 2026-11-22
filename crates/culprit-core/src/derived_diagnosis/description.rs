use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedDiagnosisCounts {
    pub incidents: u32,
    pub findings: u32,
    pub changes: u32,
}
impl DerivedDiagnosisCounts {
    pub fn total(self) -> Option<u32> {
        self.incidents.checked_add(self.findings)?.checked_add(self.changes)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedDiagnosisDescription {
    pub analysis_id: String,
    pub analyzer: ComponentVersion,
    pub rules_version: String,
    pub language: String,
    /// Explicitly approved labels remain available even when evaluation emits no rows.
    pub display: DerivedDisplayContext,
    pub recorded_at: String,
    pub observed_from: String,
    pub observed_until: String,
    pub inputs: Vec<DerivedInputReference>,
    pub origin: AnalysisOrigin,
    /// Completeness of the explicitly selected original inputs, never a healthy native population verdict.
    pub input_read_complete: bool,
    pub counts: DerivedDiagnosisCounts,
    pub output_coverage: DerivedOutputCoverageSet,
    pub finding_evaluations: DerivedFindingEvaluations,
    pub change_evaluations: Vec<DerivedChangeEvaluation>,
    pub context_issues: Vec<DerivedContextIssue>,
}
impl DerivedDiagnosisDescription {
    pub fn expected_analysis_id(&self) -> Result<String> {
        let encoded = serde_json::to_vec(&(
            "culprit-derived-analysis-v2",
            &self.analyzer,
            &self.rules_version,
            &self.display,
            &self.inputs,
            &self.context_issues,
            &self.change_evaluations,
            self.origin,
        ))
        .map_err(|_| invalid())?;
        Ok(digest(&encoded))
    }
    pub fn is_valid(&self) -> bool {
        let mut refs = BTreeSet::new();
        self.language == "en"
            && self.display.is_valid()
            && label(&self.analyzer.component)
            && self.analyzer.version.as_deref().is_some_and(label)
            && self.analyzer.revision.as_deref().is_none_or(label)
            && label(&self.rules_version)
            && utc(&self.recorded_at)
            && utc(&self.observed_from)
            && utc(&self.observed_until)
            && ObservationBounds::new(&self.observed_from, &self.observed_until).is_some()
            && !self.inputs.is_empty()
            && self.inputs.len() <= MAX_DERIVED_CAPTURES
            && self.inputs.iter().all(|input| {
                input.is_valid() && serde_json::to_vec(&input.capture.capture).is_ok_and(|key| refs.insert(key))
            })
            && self.output_coverage.matches_counts(self.counts)
            && self.finding_evaluations.is_valid_for(&self.inputs)
            && self.change_evaluations.len() <= MAX_DERIVED_CAPTURES
            && self.change_evaluations.iter().all(|evaluation| evaluation.is_valid_for(&self.inputs))
            && (self.change_evaluations.is_empty() || self.output_coverage.changes == DerivedOutputCoverage::Evaluated)
            && self.change_evaluations.iter().map(DerivedChangeEvaluation::scope_key).collect::<BTreeSet<_>>().len()
                == self.change_evaluations.len()
            && self.context_issues.len() <= MAX_DERIVED_CAPTURES
            && self.context_issues.iter().map(|issue| *issue as u8).collect::<BTreeSet<_>>().len()
                == self.context_issues.len()
            && self.counts.total().is_some_and(|count| count as usize <= MAX_DERIVED_ROWS)
            && self.expected_analysis_id().is_ok_and(|id| id == self.analysis_id)
    }
    pub fn limitations(&self) -> Vec<String> {
        let mut reasons = self.output_coverage.limitations();
        reasons.extend(self.context_issues.iter().map(|issue| issue.explanation()));
        for evaluation in &self.change_evaluations {
            let reason = evaluation.limitation();
            if !reasons.contains(&reason) {
                reasons.push(reason);
            }
        }
        if self.finding_evaluations.omitted_populations > 0 {
            reasons.push(crate::t!(
                "view.synthesis.context.omitted_evaluations",
                count = self.finding_evaluations.omitted_populations
            ));
        }
        if self.finding_evaluations.checks.iter().any(|check| {
            matches!(check.status, crate::CheckStatus::Unavailable | crate::CheckStatus::InsufficientEvidence)
        }) {
            reasons.push(crate::t!("view.synthesis.context.limited_checks"));
        }
        reasons
    }
    /// Structural capture membership only. The verified input/resolver owns exact row ordinal/digest authentication.
    pub fn contains_capture_for(&self, reference: &DiagnosticEvidenceRef) -> bool {
        self.inputs.iter().any(|input| input.capture == reference.capture_ref())
    }
}
