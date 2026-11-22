//! Deterministic completion and full content verification, including successful-empty captures.
use super::sink::AnalyticalStore;
use crate::delivery::sink::DeliveryAttempt;
use crate::delivery::sink::Publication;
use crate::delivery::sink::VerifiedRange;
use culprit_core::{Result, telemetry::DeliveryProblem};
use culprit_evidence::analytical::{
    ProjectionCatalog, projection, publication::input, read_store::MAX_READ_RESPONSE_BYTES,
};
use culprit_evidence::delivery::recovery::CommittedRange;
use serde_json::Value;
use std::sync::Arc;
pub struct ControlPublisher {
    catalog: Arc<dyn ProjectionCatalog>,
    store: Arc<dyn AnalyticalStore>,
}
impl ControlPublisher {
    pub fn new(catalog: Arc<dyn ProjectionCatalog>, store: Arc<dyn AnalyticalStore>) -> Self {
        Self { catalog, store }
    }
    pub async fn publish(&self, verified: &VerifiedRange, attempt: &DeliveryAttempt) -> Result<Publication> {
        let range = verified.committed();
        self.validate(range)?;
        if verified.receipts_digest() != range.manifest().receipts_digest()? {
            return Err(invalid());
        }
        let input = self.input(range)?;
        let schema = self.catalog.publication();
        let values = schema.project(&input)?;
        let bytes = schema.encode(&values)?;
        let id = input["publication_id"].as_str().ok_or_else(invalid)?;
        self.store.publish(range, attempt, id, bytes).await?;
        Ok(Publication { range_id: range.manifest().range_id.clone(), manifest_digest: range.manifest_digest().into() })
    }
    pub async fn visible(&self, publication: &Publication, range: &CommittedRange) -> Result<bool> {
        self.validate(range)?;
        if publication.range_id != range.manifest().range_id || publication.manifest_digest != range.manifest_digest() {
            return Err(invalid());
        }
        let schema = self.catalog.publication();
        let wanted = projection::digest(&schema.project(&self.input(range)?)?)?;
        let rows = self.store.publication_rows(range, MAX_READ_RESPONSE_BYTES).await?.rows;
        if rows.len() > 1 {
            return Err(invalid());
        }
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        if schema.returned_digest(row)? != wanted {
            return Err(invalid());
        }
        Ok(true)
    }
    fn validate(&self, range: &CommittedRange) -> Result<()> {
        let contract = &range.manifest().contract;
        if contract.destination() != self.store.destination() {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        if contract.schema_digest() != self.catalog.digest() {
            return Err(DeliveryProblem::UnsupportedSchema.error());
        }
        Ok(())
    }
    pub fn input(&self, range: &CommittedRange) -> Result<Value> {
        input(self.catalog.as_ref(), range)
    }
}
/// The expected completion row is the same for publishing and read-only evidence validation.
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
