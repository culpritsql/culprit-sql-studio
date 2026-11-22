//! Evaluation describes only the selected original inputs. Even an evaluated empty output is not native health.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedOutputGap {
    NoDiagnosticRevisions,
    AnalysisSourceUnavailable,
    CatalogComparisonUnavailable,
}
impl DerivedOutputGap {
    pub fn explanation(self) -> String {
        match self {
            Self::NoDiagnosticRevisions => crate::t!("view.synthesis.limit.no_incident_revisions"),
            Self::AnalysisSourceUnavailable => crate::t!("view.synthesis.limit.analysis_source"),
            Self::CatalogComparisonUnavailable => crate::t!("view.synthesis.limit.catalog_comparison"),
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DerivedOutputCoverage {
    Evaluated,
    ContextUnavailable { reason: DerivedOutputGap },
}
impl DerivedOutputCoverage {
    fn accepts_count(self, count: u32) -> bool {
        matches!(self, Self::Evaluated) || count == 0
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedOutputCoverageSet {
    pub incidents: DerivedOutputCoverage,
    pub findings: DerivedOutputCoverage,
    pub changes: DerivedOutputCoverage,
}
impl DerivedOutputCoverageSet {
    pub fn matches_counts(self, counts: DerivedDiagnosisCounts) -> bool {
        self.incidents.accepts_count(counts.incidents)
            && self.findings.accepts_count(counts.findings)
            && self.changes.accepts_count(counts.changes)
    }
    /// Catalog wording is projected once in the same declared language as the derived output.
    pub fn limitations(self) -> Vec<String> {
        [self.incidents, self.findings, self.changes]
            .into_iter()
            .filter_map(|coverage| match coverage {
                DerivedOutputCoverage::Evaluated => None,
                DerivedOutputCoverage::ContextUnavailable { reason } => Some(reason.explanation()),
            })
            .collect()
    }
}
