//! Compact evaluation survives zero findings without repeating all check wording for every native population.
use super::*;
use crate::{
    Capability, CheckStatus,
    collected_analysis::{CollectedAnalysisLimit, CollectedQueryPopulation},
};

pub const MAX_DERIVED_EVALUATION_BYTES: usize = 32 * 1024;
pub const MAX_DERIVED_CHECK_STATES: usize = 512;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedCheckEvaluation {
    pub check: String,
    pub status: CheckStatus,
    pub reason: Option<String>,
    pub missing: Vec<Capability>,
    /// Assessed query identities, never executions, incidents or a complete native workload count.
    pub assessed_queries: u32,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedPopulationEvaluation {
    /// Position in the description's authenticated capture inputs.
    pub input: u16,
    pub population: CollectedQueryPopulation,
    /// Positions in the shared check-state inventory. A locked state is explicitly Unavailable.
    pub checks: Vec<u16>,
    pub limitations: Vec<CollectedAnalysisLimit>,
    /// Selected catalog captures only; each input keeps its independent acquisition clock.
    pub catalog_inputs: Vec<u16>,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedFindingEvaluations {
    pub checks: Vec<DerivedCheckEvaluation>,
    pub populations: Vec<DerivedPopulationEvaluation>,
    /// Entire evaluated population count, including explicitly omitted detail below.
    pub population_count: u32,
    pub omitted_populations: u32,
}
impl DerivedFindingEvaluations {
    pub fn is_valid_for(&self, inputs: &[DerivedInputReference]) -> bool {
        if self.population_count != (self.populations.len() as u32).saturating_add(self.omitted_populations)
            || self.checks.len() > MAX_DERIVED_CHECK_STATES
            || self.populations.len() > MAX_DERIVED_ROWS
            || self.checks.iter().any(|check| {
                !label(&check.check)
                    || check.reason.as_ref().is_some_and(|reason| reason.len() > MAX_DERIVED_RECORD_BYTES)
            })
        {
            return false;
        }
        let mut populations = BTreeSet::new();
        self.populations.iter().all(|population| {
            usize::from(population.input) < inputs.len()
                && population.checks.iter().all(|index| usize::from(*index) < self.checks.len())
                && population.catalog_inputs.iter().all(|index| {
                    inputs.get(usize::from(*index)).is_some_and(|input| input.role == DerivedInputRole::Catalog)
                })
                && population.checks.iter().copied().collect::<BTreeSet<_>>().len() == population.checks.len()
                && population
                    .checks
                    .iter()
                    .filter_map(|index| self.checks.get(usize::from(*index)))
                    .map(|check| check.check.as_str())
                    .collect::<BTreeSet<_>>()
                    .len()
                    == population.checks.len()
                && serde_json::to_vec(&(population.input, &population.population))
                    .is_ok_and(|key| populations.insert(key))
        }) && crate::serialization::json_size(self, MAX_DERIVED_EVALUATION_BYTES).is_ok()
    }
}

/// Selection and interpretation gaps are separate from EOF-completeness of available selected bodies.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedContextIssue {
    MissingOriginal,
    UnavailableOriginal,
    ContextLimit,
    UnqualifiedCatalogScope,
    UnknownCatalogClock,
    CatalogNotRetainedAsOf,
    PartialCatalog,
    AmbiguousCatalog,
    ChangedCatalogSubject,
    AcquisitionCatalogOnly,
    UnqualifiedPointScope,
    UnknownPointClock,
    PointNotRetainedNearObservation,
    PointOutsideObservationWindow,
    IndependentPointRole,
    AmbiguousPointContext,
}
impl DerivedContextIssue {
    pub fn explanation(self) -> String {
        match self {
            Self::MissingOriginal => crate::t!("view.synthesis.context.missing_original"),
            Self::UnavailableOriginal => crate::t!("view.synthesis.context.unavailable_original"),
            Self::ContextLimit => crate::t!("view.synthesis.context.limit"),
            Self::UnqualifiedCatalogScope => crate::t!("view.synthesis.context.scope"),
            Self::UnknownCatalogClock => crate::t!("view.synthesis.context.clock"),
            Self::CatalogNotRetainedAsOf => crate::t!("view.synthesis.context.as_of"),
            Self::PartialCatalog => crate::t!("view.synthesis.context.partial_catalog"),
            Self::AmbiguousCatalog => crate::t!("view.synthesis.context.ambiguous_catalog"),
            Self::ChangedCatalogSubject => crate::t!("view.synthesis.context.changed_subject"),
            Self::AcquisitionCatalogOnly => crate::t!("view.synthesis.context.acquisition_only"),
            Self::UnqualifiedPointScope => crate::t!("view.synthesis.context.point_scope"),
            Self::UnknownPointClock => crate::t!("view.synthesis.context.point_clock"),
            Self::PointNotRetainedNearObservation => crate::t!("view.synthesis.context.point_missing"),
            Self::PointOutsideObservationWindow => crate::t!("view.synthesis.context.point_stale"),
            Self::IndependentPointRole => crate::t!("view.synthesis.context.point_role"),
            Self::AmbiguousPointContext => crate::t!("view.synthesis.context.point_ambiguous"),
        }
    }
}
