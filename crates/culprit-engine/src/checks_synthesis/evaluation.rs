//! One compact inventory retains assessment/lock coverage even when the pipeline emits no Finding row.
use super::*;
use culprit_core::collected_analysis::CollectedQueryAnalysisResult;
use culprit_core::serialization::json_size;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Evaluations {
    output: DerivedFindingEvaluations,
}
impl Evaluations {
    pub fn new() -> Self {
        Self { output: Default::default() }
    }
    pub fn add(
        &mut self,
        analysis: &CollectedQueryAnalysisResult,
        input: u16,
        catalog_inputs: &[u16],
        engine: Engine,
    ) -> Result<()> {
        self.append(
            DerivedFindingAssessment {
                input,
                scope: DerivedFindingScope::QueryHistory { population: analysis.population.clone() },
                checks: vec![],
                limitations: limitation_reasons(&analysis.limitations),
                catalog_inputs: catalog_inputs.to_vec(),
            },
            &analysis.assessments,
            &analysis.locked,
            engine,
        )
    }
    pub fn add_settings(
        &mut self,
        analysis: &crate::collected_evidence::RetainedSettingsAnalysis,
        input: u16,
        engine: Engine,
    ) -> Result<()> {
        self.append(
            DerivedFindingAssessment {
                input,
                scope: DerivedFindingScope::Settings,
                checks: vec![],
                limitations: limitation_reasons(&analysis.limitations),
                catalog_inputs: vec![],
            },
            &analysis.report.assessments,
            &analysis.report.locked,
            engine,
        )
    }
    pub fn add_statement_counters(
        &mut self,
        limitations: Vec<culprit_core::collected_analysis::CollectedAnalysisLimit>,
        assessments: &[CheckAssessment],
        locked: &[LockedCheck],
        catalog_inputs: &[u16],
        engine: Engine,
    ) -> Result<()> {
        self.append(
            DerivedFindingAssessment {
                input: 0,
                scope: DerivedFindingScope::StatementCounters,
                checks: vec![],
                limitations,
                catalog_inputs: catalog_inputs.to_vec(),
            },
            assessments,
            locked,
            engine,
        )
    }
    pub fn add_operational(
        &mut self,
        analysis: &crate::collected_evidence::RetainedOperationalAnalysis,
        input: u16,
        catalog_inputs: &[u16],
        engine: Engine,
    ) -> Result<()> {
        self.append(
            DerivedFindingAssessment {
                input,
                scope: DerivedFindingScope::Operational,
                checks: vec![],
                limitations: limitation_reasons(&analysis.limitations),
                catalog_inputs: catalog_inputs.to_vec(),
            },
            &analysis.report.assessments,
            &analysis.report.locked,
            engine,
        )
    }
    fn append(
        &mut self,
        mut assessment: DerivedFindingAssessment,
        assessments: &[CheckAssessment],
        locked: &[LockedCheck],
        engine: Engine,
    ) -> Result<()> {
        self.output.assessment_count = self.output.assessment_count.checked_add(1).ok_or_else(capacity)?;
        if assessments.len() > MAX_DERIVED_CHECK_STATES || locked.len() > MAX_DERIVED_CHECK_STATES {
            return self.omit();
        }
        let checkpoint = self.output.checks.len();
        for state in check_states(assessments, locked, engine)? {
            assessment.checks.push(self.index(state)?);
        }
        self.output.assessments.push(assessment);
        if self.output.checks.len() > MAX_DERIVED_CHECK_STATES || self.serialized_bound_exceeded() {
            self.output.assessments.pop();
            self.output.checks.truncate(checkpoint);
            self.omit()?;
        }
        Ok(())
    }
    fn omit(&mut self) -> Result<()> {
        self.output.omitted_assessments = self.output.omitted_assessments.checked_add(1).ok_or_else(capacity)?;
        Ok(())
    }
    fn serialized_bound_exceeded(&self) -> bool {
        #[derive(serde::Serialize)]
        struct ReservedCounts<'a> {
            checks: &'a [DerivedCheckEvaluation],
            assessments: &'a [DerivedFindingAssessment],
            assessment_count: u32,
            omitted_assessments: u32,
        }
        json_size(
            &ReservedCounts {
                checks: &self.output.checks,
                assessments: &self.output.assessments,
                assessment_count: u32::MAX,
                omitted_assessments: u32::MAX,
            },
            MAX_DERIVED_EVALUATION_BYTES,
        )
        .is_err()
    }
    fn index(&mut self, state: DerivedCheckEvaluation) -> Result<u16> {
        if let Some(index) = self.output.checks.iter().position(|existing| *existing == state) {
            return index.try_into().map_err(|_| capacity());
        }
        let index = self.output.checks.len().try_into().map_err(|_| capacity())?;
        self.output.checks.push(state);
        Ok(index)
    }
    pub fn finish(self) -> DerivedFindingEvaluations {
        self.output
    }
}
fn limitation_reasons(
    limitations: &[culprit_core::collected_analysis::CollectedAnalysisLimitation],
) -> Vec<culprit_core::collected_analysis::CollectedAnalysisLimit> {
    let mut reasons = Vec::new();
    for limitation in limitations {
        if !reasons.contains(&limitation.reason) {
            reasons.push(limitation.reason);
        }
    }
    reasons
}

/// Assessment wording and missing-capability locks are two facets of the same pipeline check.
/// Preserve their one coherent Unavailable state; contradictory or repeated facets still refuse.
fn check_states(
    assessments: &[CheckAssessment],
    locked: &[LockedCheck],
    engine: Engine,
) -> Result<Vec<DerivedCheckEvaluation>> {
    let mut states = Vec::new();
    let mut positions = BTreeMap::new();
    for assessment in assessments {
        if positions.insert(assessment.check.as_str(), states.len()).is_some() {
            return Err(invalid());
        }
        states.push(DerivedCheckEvaluation {
            check: assessment.check.clone(),
            status: assessment.status,
            reason: assessment.reason.as_ref().map(|reason| culprit_core::redact::redact_prose(reason, engine)),
            missing: vec![],
            assessed_queries: assessment.assessed_queries.len().try_into().map_err(|_| capacity())?,
        });
    }
    let mut locks = BTreeSet::new();
    for lock in locked {
        if !locks.insert(lock.check.id.as_str()) {
            return Err(invalid());
        }
        if let Some(position) = positions.get(lock.check.id.as_str()) {
            let state = states.get_mut(*position).ok_or_else(invalid)?;
            if state.status != CheckStatus::Unavailable || state.assessed_queries != 0 {
                return Err(invalid());
            }
            state.missing = lock.missing.clone();
        } else {
            positions.insert(lock.check.id.as_str(), states.len());
            states.push(DerivedCheckEvaluation {
                check: lock.check.id.clone(),
                status: CheckStatus::Unavailable,
                reason: None,
                missing: lock.missing.clone(),
                assessed_queries: 0,
            });
        }
    }
    Ok(states)
}
