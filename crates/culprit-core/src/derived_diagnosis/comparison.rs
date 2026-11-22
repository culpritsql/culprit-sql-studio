//! Each assessment names its exact original inputs and the native scope that was actually evaluated.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "scope", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DerivedChangeEvaluation {
    /// Known configuration values only; not schema, runtime state, resets or workload absence.
    Settings { previous_input: u16, current_input: u16, observed_after: String, observed_until: String },
    /// One selected native job-record observation, including unplaced records and independent source coverage.
    JobExecutions { input: u16, assessment: crate::JobExecutionAssessment },
}
impl DerivedChangeEvaluation {
    pub fn is_valid_for(&self, inputs: &[DerivedInputReference]) -> bool {
        match self {
            Self::Settings { previous_input, current_input, observed_after, observed_until } => {
                let catalog = |index: u16| {
                    inputs
                        .get(usize::from(index))
                        .is_some_and(|input| input.role == DerivedInputRole::Catalog && input.catalog.is_some())
                };
                previous_input != current_input
                    && catalog(*previous_input)
                    && catalog(*current_input)
                    && utc(observed_after)
                    && utc(observed_until)
                    && ObservationBounds::new(observed_after, observed_until).is_some()
            }
            Self::JobExecutions { input, assessment } => {
                inputs.get(usize::from(*input)).is_some_and(|input| input.role == DerivedInputRole::Observation)
                    && assessment.is_valid()
            }
        }
    }
    pub fn scope_key(&self) -> (u8, u16, Option<u16>) {
        match self {
            Self::Settings { previous_input, current_input, .. } => (0, *previous_input, Some(*current_input)),
            Self::JobExecutions { input, .. } => (1, *input, None),
        }
    }
    pub fn limitation(&self) -> String {
        match self {
            Self::Settings { .. } => crate::t!("view.synthesis.context.settings_comparison"),
            Self::JobExecutions { .. } => crate::t!("view.synthesis.context.job_executions"),
        }
    }
}
