//! Bounded per-resource fleet candidate discovery. Every selected candidate is authenticated against immutable
//! recovery manifests and projection indexes before entering a fleet report.
use super::{
    EvidenceReadBudget, EvidenceReadMetrics, PublishedResourceClock, PublishedResourceContent, PublishedResourceHint,
    PublishedResourceQuery,
};
use crate::delivery::contract::Contract;
use culprit_core::{
    PublishedResourceRequest, Result, WarehouseFleetCursor, WarehouseFleetRequest,
    telemetry::{DeliveryProblem, digest, valid_digest},
};
use serde_json::{Map, Value};

#[derive(Clone, Debug)]
pub struct FleetResourceQuery {
    pub selection: WarehouseFleetRequest,
    pub recovery_store_id: String,
}

impl FleetResourceQuery {
    pub fn key(&self, contract: &Contract) -> Result<String> {
        Ok(digest(
            &serde_json::to_vec(&(
                "warehouse-fleet-selection-v1",
                contract.id()?,
                &self.recovery_store_id,
                &self.selection.as_of,
                &self.selection.window,
                self.selection.max_age_seconds,
                &self.selection.filter,
                self.selection.sort,
            ))
            .map_err(|_| invalid())?,
        ))
    }

    pub fn validate(&self, contract: &Contract) -> Result<()> {
        contract.validate()?;
        self.selection.validate()?;
        if !valid_digest(&self.recovery_store_id)
            || self
                .selection
                .after
                .as_ref()
                .is_some_and(|cursor| self.key(contract).ok().as_ref() != Some(&cursor.selection_key))
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }

    pub fn within_budget(&self, contract: &Contract, budget: EvidenceReadBudget) -> Result<Self> {
        self.validate(contract)?;
        budget.validate()?;
        let mut request = self.clone();
        request.selection.page_rows =
            request.selection.page_rows.min(budget.page_rows).min(budget.max_rows.saturating_sub(1));
        if request.selection.page_rows == 0 {
            return Err(DeliveryProblem::Capacity.error());
        }
        Ok(request)
    }

    pub(super) fn hint_from_values(
        &self,
        contract: &Contract,
        values: &Map<String, Value>,
    ) -> Result<PublishedResourceHint> {
        let base = PublishedResourceQuery {
            selection: PublishedResourceRequest {
                as_of: self.selection.as_of.clone(),
                window: self.selection.window.clone(),
                after: None,
                page_rows: self.selection.page_rows.min(culprit_core::MAX_PUBLISHED_RESOURCE_PAGE_ROWS),
            },
            recovery_store_id: self.recovery_store_id.clone(),
            database: None,
            clock: PublishedResourceClock::Acquisition,
            content: PublishedResourceContent::All,
        };
        PublishedResourceHint::from_values(contract, &base, values)
    }
}

pub struct FleetDiscoveryPage {
    pub candidates: Vec<PublishedResourceHint>,
    pub next: Option<WarehouseFleetCursor>,
    pub charged_candidate_rows: u32,
    pub metrics: EvidenceReadMetrics,
}

pub(super) fn page(
    contract: &Contract,
    request: &FleetResourceQuery,
    mut candidates: Vec<PublishedResourceHint>,
    metrics: EvidenceReadMetrics,
) -> Result<FleetDiscoveryPage> {
    request.validate(contract)?;
    if candidates.len() > request.selection.page_rows as usize + 1 {
        return Err(invalid());
    }
    let charged_candidate_rows = candidates.len() as u32;
    let mut previous = request.selection.after.as_ref().map(|cursor| cursor.resource_id.as_str());
    for candidate in &candidates {
        let current = candidate.position.resource_id.as_str();
        if previous.is_some_and(|prior| prior >= current) {
            return Err(invalid());
        }
        previous = Some(current);
    }
    let next = if candidates.len() > request.selection.page_rows as usize {
        candidates.pop();
        Some(WarehouseFleetCursor {
            selection_key: request.key(contract)?,
            resource_id: candidates.last().ok_or_else(invalid)?.position.resource_id.clone(),
        })
    } else {
        None
    };
    Ok(FleetDiscoveryPage { candidates, next, charged_candidate_rows, metrics })
}

fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
