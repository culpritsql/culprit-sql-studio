//! Read existing committed captures without creating history, acquiring write ownership or opening a source database.
mod config;
mod incidents;
pub use incidents::IncidentRequest;
mod recovery;
mod resources;
mod session;
pub use recovery::EvidenceRecoveryProvider;
pub(super) mod stores;
pub use config::{EvidenceConfig, EvidenceStoreConfig};
pub use session::EvidenceSession;
pub use stores::EvidenceStoreProvider;

use culprit_core::{
    Result,
    telemetry::{DeliveryProblem, valid_digest},
};
use culprit_evidence::analytical::evidence::EvidenceCapture;
use culprit_evidence::analytical::evidence::EvidenceFamily;
use culprit_evidence::analytical::evidence::EvidenceReadBudget;
use culprit_evidence::analytical::evidence::EvidenceReader;
use culprit_evidence::analytical::indexes::ProjectionIndexes;
use culprit_evidence::delivery::contract::Contract;
use culprit_evidence::delivery::object_store::transport::ObjectReadTransport;
use culprit_read::cancellation::CollectionCancellation;
use std::sync::Arc;

pub struct EvidenceRequest {
    pub range_id: String,
    pub families: Vec<EvidenceFamily>,
    pub budget: EvidenceReadBudget,
}
impl EvidenceRequest {
    pub fn validate(&self) -> Result<()> {
        self.budget.validate()?;
        if !valid_digest(&self.range_id)
            || self.families.is_empty()
            || self.families.len() > EvidenceFamily::ALL.len()
            || self.families.iter().enumerate().any(|(index, family)| self.families[..index].contains(family))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub struct EvidenceService {
    recovery: Arc<dyn ObjectReadTransport>,
    contract: Contract,
    reader: EvidenceReader,
}
/// Source-free warehouse/recovery composition. It has no source inventory, Outbox, scheduler or write owner.
pub struct EvidenceRuntime {
    recovery: recovery::RecoveryRegistry,
    evidence_stores: stores::StoreRegistry,
    credentials: Option<Arc<dyn culprit_auth::CredentialResolver>>,
}
impl Default for EvidenceRuntime {
    fn default() -> Self {
        Self {
            recovery: recovery::RecoveryRegistry::builtins(),
            evidence_stores: stores::StoreRegistry::builtins(),
            credentials: None,
        }
    }
}
impl EvidenceRuntime {
    pub fn empty() -> Self {
        Self { recovery: Default::default(), evidence_stores: Default::default(), credentials: None }
    }
    pub fn with_credentials(mut self, credentials: Arc<dyn culprit_auth::CredentialResolver>) -> Self {
        self.credentials = Some(credentials);
        self
    }
    pub fn with_recovery_provider(mut self, provider: Arc<dyn EvidenceRecoveryProvider>) -> Result<Self> {
        self.recovery.register(provider)?;
        Ok(self)
    }
}
impl EvidenceRuntime {
    pub fn with_evidence_store_provider(mut self, provider: Arc<dyn EvidenceStoreProvider>) -> Result<Self> {
        self.evidence_stores.register(provider)?;
        Ok(self)
    }

    /// Configures only warehouse and recovery identities. No source, runtime directory or remote history is opened.
    pub fn evidence(&self, config: &EvidenceConfig) -> Result<EvidenceService> {
        let credentials: Arc<dyn culprit_auth::CredentialResolver> = match &self.credentials {
            Some(credentials) => credentials.clone(),
            None => Arc::new(config.credentials()?),
        };
        let recovery = self.recovery.configure(&config.recovery, credentials.as_ref())?;
        let store = self.evidence_stores.configure(&config.warehouse, credentials.as_ref())?;
        let contract = Contract::new(
            config.pipeline.clone(),
            config.shard.clone(),
            store.destination().into(),
            store.catalog().digest().into(),
        )?;
        let reader = EvidenceReader::new(store, Arc::new(ProjectionIndexes::new(recovery.clone())));
        Ok(EvidenceService { recovery, contract, reader })
    }
}
impl EvidenceService {
    pub async fn read(
        &self,
        request: &EvidenceRequest,
        cancellation: &CollectionCancellation,
    ) -> Result<EvidenceCapture> {
        request.validate()?;
        self.session(request.budget, cancellation)?.read(&request.range_id, &request.families).await
    }

    /// Related captures share one allowance and deadline. Failure closes the session; retries require a new action.
    pub fn session<'a>(
        &'a self,
        budget: EvidenceReadBudget,
        cancellation: &'a CollectionCancellation,
    ) -> Result<EvidenceSession<'a>> {
        EvidenceSession::new(self, budget, cancellation)
    }
}

fn invalid() -> culprit_core::Error {
    DeliveryProblem::InvalidContract.error()
}

#[cfg(all(test, feature = "store-adx", feature = "recovery-azure"))]
mod tests;

#[cfg(test)]
mod boundary_tests;
