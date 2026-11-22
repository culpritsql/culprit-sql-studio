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
        self.output.population_count = self.output.population_count.checked_add(1).ok_or_else(capacity)?;
        if analysis.assessments.len() > MAX_DERIVED_CHECK_STATES || analysis.locked.len() > MAX_DERIVED_CHECK_STATES {
            self.output.omitted_populations = self.output.omitted_populations.checked_add(1).ok_or_else(capacity)?;
            return Ok(());
        }
        let checkpoint = self.output.checks.len();
        let mut checks = Vec::new();
        for state in check_states(analysis, engine)? {
            checks.push(self.index(state)?);
        }
        let mut limitations = Vec::new();
        for limitation in &analysis.limitations {
            if !limitations.contains(&limitation.reason) {
                limitations.push(limitation.reason);
            }
        }
        self.output.populations.push(DerivedPopulationEvaluation {
            input,
            population: analysis.population.clone(),
            checks,
            limitations,
            catalog_inputs: catalog_inputs.to_vec(),
        });
        if self.output.checks.len() > MAX_DERIVED_CHECK_STATES || self.serialized_bound_exceeded() {
            self.output.populations.pop();
            self.output.checks.truncate(checkpoint);
            self.output.omitted_populations = self.output.omitted_populations.checked_add(1).ok_or_else(capacity)?;
        }
        Ok(())
    }
    fn serialized_bound_exceeded(&self) -> bool {
        #[derive(serde::Serialize)]
        struct ReservedCounts<'a> {
            checks: &'a [DerivedCheckEvaluation],
            populations: &'a [DerivedPopulationEvaluation],
            population_count: u32,
            omitted_populations: u32,
        }
        json_size(
            &ReservedCounts {
                checks: &self.output.checks,
                populations: &self.output.populations,
                population_count: u32::MAX,
                omitted_populations: u32::MAX,
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

/// Assessment wording and missing-capability locks are two facets of the same pipeline check.
/// Preserve their one coherent Unavailable state; contradictory or repeated facets still refuse.
fn check_states(analysis: &CollectedQueryAnalysisResult, engine: Engine) -> Result<Vec<DerivedCheckEvaluation>> {
    let mut states = Vec::new();
    let mut positions = BTreeMap::new();
    for assessment in &analysis.assessments {
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
    for locked in &analysis.locked {
        if !locks.insert(locked.check.id.as_str()) {
            return Err(invalid());
        }
        if let Some(position) = positions.get(locked.check.id.as_str()) {
            let state = states.get_mut(*position).ok_or_else(invalid)?;
            if state.status != CheckStatus::Unavailable || state.assessed_queries != 0 {
                return Err(invalid());
            }
            state.missing = locked.missing.clone();
        } else {
            positions.insert(locked.check.id.as_str(), states.len());
            states.push(DerivedCheckEvaluation {
                check: locked.check.id.clone(),
                status: CheckStatus::Unavailable,
                reason: None,
                missing: locked.missing.clone(),
                assessed_queries: 0,
            });
        }
    }
    Ok(states)
}
