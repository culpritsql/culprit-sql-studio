//! Native job records enter Gold through the existing pure jobs interpreter and canonical original row references.
use super::*;
use culprit_evidence::original_projection::OriginalProjection;
pub(super) struct JobExecutionPlan<'a> {
    original: &'a dyn OriginalProjection,
    projection: JobExecutionProjection,
}
impl<'a> JobExecutionPlan<'a> {
    pub fn select(primary: &'a dyn OriginalProjection) -> Result<Option<Self>> {
        if !matches!(
            primary.source().observation,
            SourceObservation::Jobs { .. }
                | SourceObservation::MssqlAgentJobs { .. }
                | SourceObservation::MysqlEvents { .. }
        ) {
            return Ok(None);
        }
        let read = crate::collected_resources::jobs::from_original(primary)?;
        let projection = culprit_checks::jobs::execution_projection(&read)?;
        Ok(Some(Self { original: primary, projection }))
    }
    pub fn evaluation(&self) -> DerivedChangeEvaluation {
        DerivedChangeEvaluation::JobExecutions { input: 0, assessment: self.projection.assessment.clone() }
    }
    pub fn project(
        &self,
        description: &DerivedDiagnosisDescription,
        budget: &mut OutputBudget,
    ) -> Result<Vec<DerivedChange>> {
        let database =
            self.original.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
        let mut positions = std::collections::BTreeMap::new();
        for (position, row) in self.original.records().iter().enumerate() {
            let TelemetryRecord::Operational(native) = &row.row().record else { return Err(invalid()) };
            if positions.insert((native.source.as_str(), native.key.as_str()), position).is_some() {
                return Err(invalid());
            }
        }
        let mut changes = Vec::new();
        for event in &self.projection.events {
            let reference = &event.job_execution.as_ref().ok_or_else(invalid)?.reference;
            let position = positions
                .get(&(reference.source.as_str(), reference.observation_key.as_str()))
                .copied()
                .ok_or_else(invalid)?;
            let mut row = DerivedChange {
                analysis_id: description.analysis_id.clone(),
                database: database.clone(),
                display: description.display.clone(),
                change: event.clone(),
                inputs: vec![self.original.reference(position)?],
            };
            row.redact(self.original.source().target.engine());
            budget.admit(&row)?;
            changes.push(row);
        }
        Ok(changes)
    }
}
