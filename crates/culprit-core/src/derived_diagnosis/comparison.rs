//! A comparison keeps its actual scope even when no change rows are produced.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedChangeScope {
    /// Known configuration values only; not schema, runtime state, resets or workload absence.
    Settings,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedChangeEvaluation {
    pub scope: DerivedChangeScope,
    pub previous_input: u16,
    pub current_input: u16,
    /// Conservative collector acquisition bounds, not native DDL/event timestamps.
    pub observed_after: String,
    pub observed_until: String,
}
impl DerivedChangeEvaluation {
    pub fn is_valid_for(&self, inputs: &[DerivedInputReference]) -> bool {
        let catalog = |index: u16| {
            inputs
                .get(usize::from(index))
                .is_some_and(|input| input.role == DerivedInputRole::Catalog && input.catalog.is_some())
        };
        self.previous_input != self.current_input
            && catalog(self.previous_input)
            && catalog(self.current_input)
            && utc(&self.observed_after)
            && utc(&self.observed_until)
            && ObservationBounds::new(&self.observed_after, &self.observed_until).is_some()
    }
    pub fn limitation(&self) -> String {
        match self.scope {
            DerivedChangeScope::Settings => crate::t!("view.synthesis.context.settings_comparison"),
        }
    }
}
