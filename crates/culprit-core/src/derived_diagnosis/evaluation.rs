//! Compact evaluation survives zero findings for native query populations and ordinary configuration inputs.
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

/// The captured input assessed by the installed checks; settings never impersonate query history.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DerivedFindingScope {
    QueryHistory {
        population: CollectedQueryPopulation,
    },
    Settings,
    /// Native cumulative points were assessed without pretending that their missing definitions are query history.
    StatementCounters,
    /// Database-scoped operational, cleanup, or plan-cache point observations assessed without query history.
    Operational,
    /// Standalone database catalog, index, constraint, or statistics snapshots assessed without query history.
    Catalog,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedFindingAssessment {
    /// Position in the description's authenticated capture inputs.
    pub input: u16,
    pub scope: DerivedFindingScope,
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
    pub assessments: Vec<DerivedFindingAssessment>,
    /// Entire assessed input-scope count, including explicitly omitted detail below.
    pub assessment_count: u32,
    pub omitted_assessments: u32,
}
impl DerivedFindingEvaluations {
    pub fn is_valid_for(&self, inputs: &[DerivedInputReference]) -> bool {
        if self.assessment_count != (self.assessments.len() as u32).saturating_add(self.omitted_assessments)
            || self.checks.len() > MAX_DERIVED_CHECK_STATES
            || self.assessments.len() > MAX_DERIVED_ROWS
            || self.checks.iter().any(|check| {
                !label(&check.check)
                    || check.reason.as_ref().is_some_and(|reason| reason.len() > MAX_DERIVED_RECORD_BYTES)
            })
        {
            return false;
        }
        let mut scopes = BTreeSet::new();
        self.assessments.iter().all(|assessment| {
            usize::from(assessment.input) < inputs.len()
                && match &assessment.scope {
                    DerivedFindingScope::QueryHistory {
                        population: CollectedQueryPopulation::Population { identity },
                    } => identity.is_valid(),
                    DerivedFindingScope::QueryHistory { .. } => true,
                    DerivedFindingScope::StatementCounters | DerivedFindingScope::Operational => inputs
                        .get(usize::from(assessment.input))
                        .is_some_and(|input| input.role == DerivedInputRole::Observation),
                    DerivedFindingScope::Settings => {
                        inputs
                            .get(usize::from(assessment.input))
                            .is_some_and(|input| input.role == DerivedInputRole::Catalog)
                            && assessment.catalog_inputs.is_empty()
                    }
                    DerivedFindingScope::Catalog => inputs
                        .get(usize::from(assessment.input))
                        .is_some_and(|input| input.role == DerivedInputRole::Catalog),
                }
                && assessment.checks.iter().all(|index| usize::from(*index) < self.checks.len())
                && assessment.catalog_inputs.iter().all(|index| {
                    inputs.get(usize::from(*index)).is_some_and(|input| input.role == DerivedInputRole::Catalog)
                })
                && assessment.checks.iter().copied().collect::<BTreeSet<_>>().len() == assessment.checks.len()
                && assessment
                    .checks
                    .iter()
                    .filter_map(|index| self.checks.get(usize::from(*index)))
                    .map(|check| check.check.as_str())
                    .collect::<BTreeSet<_>>()
                    .len()
                    == assessment.checks.len()
                && serde_json::to_vec(&(assessment.input, &assessment.scope)).is_ok_and(|key| scopes.insert(key))
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
    MaintenanceInactiveInRecovery,
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
            Self::MaintenanceInactiveInRecovery => crate::t!("engine.collected_analysis.recovery_settings"),
            Self::UnqualifiedPointScope => crate::t!("view.synthesis.context.point_scope"),
            Self::UnknownPointClock => crate::t!("view.synthesis.context.point_clock"),
            Self::PointNotRetainedNearObservation => crate::t!("view.synthesis.context.point_missing"),
            Self::PointOutsideObservationWindow => crate::t!("view.synthesis.context.point_stale"),
            Self::IndependentPointRole => crate::t!("view.synthesis.context.point_role"),
            Self::AmbiguousPointContext => crate::t!("view.synthesis.context.point_ambiguous"),
        }
    }
}
