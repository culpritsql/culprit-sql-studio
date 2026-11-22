//! Friendly archive selection is a complete bounded traversal, never a saved connection or native-name join.
use super::*;

const MAX_RESOURCE_NAME_BYTES: usize = 1024;
impl EvidenceService {
    /// Resolve an exact observed database/display name, then consume the SAME action for incident ancestry.
    /// Identical route selectors can recur across original sources; different selectors never collapse by name.
    pub async fn incident_named(
        &self,
        resource_name: &str,
        around: &str,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<IncidentReadReport> {
        if resource_name.trim().is_empty()
            || resource_name.len() > MAX_RESOURCE_NAME_BYTES
            || resource_name.chars().any(char::is_control)
        {
            return Err(Error::invalid(t!("incident.cli.resource_name_invalid")));
        }
        around_clock(around)?;
        let mut selection = PublishedResourceRequest {
            as_of: chrono::Utc::now().to_rfc3339(),
            window: None,
            after: None,
            page_rows: MAX_PUBLISHED_RESOURCE_PAGE_ROWS,
        };
        let mut session = self.session(budget, cancellation)?;
        let selector = resolve(&mut session, resource_name, &mut selection).await?;
        let mut request = IncidentRequest::around_selector(selector.database.clone(), around, budget)?;
        // Discovery and the existing incident assembler share one fixed processing cutoff, not a new native clock.
        request.recorded_before = selection.as_of;
        complete_resource(session, &self.contract, &selector, request).await
    }
}
async fn resolve(
    session: &mut EvidenceSession<'_>,
    name: &str,
    selection: &mut PublishedResourceRequest,
) -> Result<PublishedResourceSelector> {
    let mut selected = None;
    loop {
        let page = session.resources(selection).await.map_err(|error| {
            if error.detail == Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)) {
                error.with_hint(t!("incident.cli.resource_incomplete"))
            } else {
                error
            }
        })?;
        if page.gaps.iter().any(|gap| *gap != PublishedResourceGap::MoreCandidates) {
            return Err(Error::invalid(t!("incident.cli.resource_incomplete")));
        }
        for resource in page.resources {
            if resource.database_name != name && resource.connection_alias.as_deref() != Some(name) {
                continue;
            }
            match &selected {
                Some(before) if *before != resource.selector => {
                    return Err(Error::invalid(t!("incident.cli.resource_ambiguous")));
                }
                Some(_) => (),
                None => selected = Some(resource.selector),
            }
        }
        let Some(next) = page.next else {
            if !page.discovery_complete {
                return Err(Error::invalid(t!("incident.cli.resource_incomplete")));
            }
            return selected.ok_or_else(|| Error::invalid(t!("incident.cli.resource_missing")));
        };
        if selection.after.as_ref().is_some_and(|before| next.position <= before.position) {
            return Err(DeliveryProblem::Conflict.error());
        }
        selection.after = Some(next);
    }
}
