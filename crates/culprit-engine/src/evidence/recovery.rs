//! Read capabilities configure the same native transports without mutation or collector registration.
use super::invalid;
use crate::recovery_config::{RecoveryConfig, valid_provider_name};
use culprit_auth::CredentialResolver;
use culprit_core::Result;
use culprit_evidence::delivery::object_store::transport::ObjectReadTransport;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
pub trait EvidenceRecoveryProvider: Send + Sync {
    fn name(&self) -> &str;
    fn configure(
        &self,
        options: &Value,
        credential: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>>;
}
#[derive(Default)]
pub(super) struct RecoveryRegistry(BTreeMap<String, Arc<dyn EvidenceRecoveryProvider>>);
impl RecoveryRegistry {
    pub fn builtins() -> Self {
        let providers: Vec<Arc<dyn EvidenceRecoveryProvider>> = vec![
            #[cfg(feature = "evidence-azure")]
            Arc::new(Azure),
            #[cfg(feature = "evidence-s3")]
            Arc::new(S3),
        ];
        Self(providers.into_iter().map(|p| (p.name().to_owned(), p)).collect())
    }
    pub fn register(&mut self, provider: Arc<dyn EvidenceRecoveryProvider>) -> Result<()> {
        let name = provider.name();
        if !valid_provider_name(name) || self.0.contains_key(name) {
            return Err(invalid());
        }
        self.0.insert(name.to_owned(), provider);
        Ok(())
    }
    pub fn configure(
        &self,
        config: &RecoveryConfig,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        self.0.get(&config.provider).ok_or_else(invalid)?.configure(&config.options, &config.credential, credentials)
    }
}
#[cfg(feature = "evidence-azure")]
struct Azure;
#[cfg(feature = "evidence-azure")]
impl EvidenceRecoveryProvider for Azure {
    fn name(&self) -> &str {
        "azure-blob"
    }
    fn configure(
        &self,
        options: &Value,
        credential: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        Ok(culprit_blob_azure::configure_transport(options, credential, credentials)?.transport)
    }
}
#[cfg(feature = "evidence-s3")]
struct S3;
#[cfg(feature = "evidence-s3")]
impl EvidenceRecoveryProvider for S3 {
    fn name(&self) -> &str {
        "s3"
    }
    fn configure(
        &self,
        options: &Value,
        credential: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        Ok(culprit_blob_s3::configure_transport(options, credential, credentials)?.transport)
    }
}
