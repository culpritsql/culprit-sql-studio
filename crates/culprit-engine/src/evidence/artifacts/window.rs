//! CLI selection is presentation over the same window result, then one continuation of its paid session.
use super::*;
use crate::evidence::NamedWindowQuery;
/// Borrowed presentation locator only; the ordinary original reader authenticates all selected rows again.
pub struct WindowArtifactChoice<'a> {
    pub title: &'a str,
    pub references: Vec<&'a DiagnosticEvidenceRef>,
    pub sql_sha256: Option<&'a str>,
}
impl WindowArtifactChoice<'_> {
    fn query(&self, resource: &PublishedResourceSelector) -> Result<WarehouseArtifactQuery> {
        let references = self
            .references
            .iter()
            .map(|reference| reference.key().map(|key| (key, (**reference).clone())))
            .collect::<Result<BTreeMap<_, _>>>()?
            .into_values()
            .collect();
        let query = WarehouseArtifactQuery {
            resource: resource.clone(),
            references,
            full_payloads: true,
            text: WarehouseArtifactTextRead::Complete,
            expected_sql_sha256: self.sql_sha256.map(str::to_owned),
        };
        query.validate()?;
        Ok(query)
    }
}
pub fn window_artifact_choices(report: &WarehouseWindowReport) -> Vec<WindowArtifactChoice<'_>> {
    let mut choices = vec![];
    for incident in &report.incidents.incidents {
        for query in
            incident.affected_queries.iter().chain(&incident.culprit_queries).chain(&incident.candidate_queries)
        {
            if !query.evidence.is_empty() {
                choices.push(WindowArtifactChoice {
                    title: &incident.entry.title,
                    references: query.evidence.iter().collect(),
                    sql_sha256: payload_sha(&query.sql),
                });
            }
        }
    }
    for analysis in &report.published {
        for row in &analysis.findings {
            let finding = &row.finding;
            if let Some(query) = &finding.query {
                choices.push(WindowArtifactChoice {
                    title: &finding.finding.title,
                    references: vec![&query.reference],
                    sql_sha256: payload_sha(&query.sql),
                });
            } else if !finding.inputs.is_empty() {
                choices.push(WindowArtifactChoice {
                    title: &finding.finding.title,
                    references: finding.inputs.iter().collect(),
                    sql_sha256: None,
                });
            }
        }
        for row in &analysis.changes {
            if !row.change.inputs.is_empty() {
                choices.push(WindowArtifactChoice {
                    title: row.change.change.title.text(),
                    references: row.change.inputs.iter().collect(),
                    sql_sha256: None,
                });
            }
        }
    }
    choices
}
fn payload_sha(payload: &DefinitionPayload) -> Option<&str> {
    match payload {
        DefinitionPayload::Available { sha256, .. } => Some(sha256),
        _ => None,
    }
}
impl EvidenceService {
    /// Deliberate complete detail action. Resource-name discovery, window selection, ancestry and detail all share
    /// one action budget/deadline. No hidden read after the CLI's numbered selection receives a fresh allowance.
    pub async fn window_artifact_named(
        &self,
        request: &NamedWindowQuery<'_>,
        detail: usize,
        budget: EvidenceReadBudget,
        token: &CollectionCancellation,
    ) -> Result<WarehouseArtifactReport> {
        if detail == 0 || budget.recovery_payloads.is_none() {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        let mut session = self.session(budget, token)?;
        let query = super::super::incidents::window::resolve_named(&mut session, request).await?;
        let (report, mut session) =
            Box::pin(super::super::incidents::window::read_retaining_session(self, &query, budget, session)).await?;
        let choices = window_artifact_choices(&report);
        let selected =
            choices.get(detail - 1).ok_or_else(|| DeliveryProblem::InvalidContract.error())?.query(&query.resource)?;
        drop(choices);
        drop(report);
        Box::pin(super::read(&mut session, &selected)).await
    }
}
