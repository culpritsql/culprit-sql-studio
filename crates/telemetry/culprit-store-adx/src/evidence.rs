//! Read-only Kusto access to explicitly selected, independently verified capture evidence.
use crate::{catalog::SchemaCatalog, client::QueryTransport, receipts::AdxReadback};
use async_trait::async_trait;
use culprit_core::{Result, telemetry::TelemetryStream};
use culprit_evidence::analytical::ProjectionCatalog;
use culprit_evidence::analytical::evidence::EvidenceReader;
use culprit_evidence::analytical::evidence::{FleetResourceQuery, IncidentDiscoveryRequest, PublishedResourceQuery};
use culprit_evidence::analytical::indexes::ProjectionIndexes;
use culprit_evidence::analytical::read_store::AnalyticalReadPage;
use culprit_evidence::analytical::read_store::AnalyticalReadStore;
use culprit_evidence::delivery::contract::Contract;
use culprit_evidence::delivery::recovery::CommittedRange;
use serde_json::{Map, Value};
use std::sync::Arc;

pub struct AdxReadStore {
    query: Arc<dyn QueryTransport>,
    catalog: Arc<SchemaCatalog>,
    readback: AdxReadback,
}
impl AdxReadStore {
    pub fn new(query: Arc<dyn QueryTransport>) -> Result<Self> {
        let catalog = Arc::new(SchemaCatalog::new()?);
        Ok(Self { readback: AdxReadback::new(query.clone(), catalog.clone()), query, catalog })
    }
    #[cfg(feature = "write")]
    pub(crate) fn schema_catalog(&self) -> Arc<SchemaCatalog> {
        self.catalog.clone()
    }
}

pub fn new(query: Arc<dyn QueryTransport>, indexes: Arc<ProjectionIndexes>) -> Result<EvidenceReader> {
    Ok(EvidenceReader::new(Arc::new(AdxReadStore::new(query)?), indexes))
}

#[async_trait]
impl AnalyticalReadStore for AdxReadStore {
    fn destination(&self) -> &str {
        self.query.destination()
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.catalog.clone()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.catalog.get(stream)?.logical_values(row)
    }
    async fn resource_candidates(
        &self,
        contract: &Contract,
        request: &PublishedResourceQuery,
        byte_limit: usize,
    ) -> Result<AnalyticalReadPage> {
        resources::read(self, contract, request, byte_limit).await
    }
    async fn incident_candidates(
        &self,
        contract: &Contract,
        request: &IncidentDiscoveryRequest,
        byte_limit: usize,
    ) -> Result<AnalyticalReadPage> {
        incidents::read(self, contract, request, byte_limit).await
    }
    async fn fleet_candidates(
        &self,
        contract: &Contract,
        request: &FleetResourceQuery,
        byte_limit: usize,
    ) -> Result<AnalyticalReadPage> {
        fleet::read(self, contract, request, byte_limit).await
    }
    async fn audit_membership(&self, range: &CommittedRange, byte_limit: usize) -> Result<usize> {
        self.readback.audit_membership(range, byte_limit).await
    }
    async fn rows(
        &self,
        range: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        byte_limit: usize,
    ) -> Result<AnalyticalReadPage> {
        self.readback.rows(range, batch, after, until, byte_limit).await
    }
    async fn publication_rows(&self, range: &CommittedRange, byte_limit: usize) -> Result<AnalyticalReadPage> {
        crate::control::read_rows(self.catalog.as_ref(), self.query.as_ref(), range, byte_limit).await
    }
}

mod fleet;
mod incidents;
mod resources;

#[cfg(all(test, feature = "write"))]
mod tests;
