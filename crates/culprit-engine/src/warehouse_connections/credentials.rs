//! Exact independently named read credentials. No database token/profile fallback or identity discovery.
use super::*;
use crate::{
    credentials::{ConfiguredCredentials, CredentialProfile},
    evidence::{EvidenceConfig, EvidenceStoreConfig},
    recovery_config::RecoveryConfig,
};
use culprit_auth::{
    AccessKeyProvider, BearerTokenProvider, CredentialResolver, DatabaseTokenProvider, PasswordCredential,
};
use serde_json::json;
use zeroize::Zeroizing;

#[derive(Clone)]
enum BearerScope {
    Kusto(String),
    Storage,
}
struct ScopedAzure {
    scope: BearerScope,
    tenant: Option<String>,
}
#[async_trait::async_trait]
impl BearerTokenProvider for ScopedAzure {
    async fn bearer_token(&self, resource: &str) -> Result<Zeroizing<String>> {
        match &self.scope {
            BearerScope::Kusto(endpoint) => validate_kusto_audience(endpoint, resource)?,
            BearerScope::Storage if resource == "https://storage.azure.com/" => {}
            _ => return Err(invalid()),
        }
        azure_token(resource, self.tenant.clone()).await
    }
}
#[cfg(feature = "auth-azure")]
async fn azure_token(resource: &str, tenant: Option<String>) -> Result<Zeroizing<String>> {
    let credential = culprit_auth_azure::cli::AzureCliCredential::for_audience(
        crate::credentials::azure_commands(),
        resource.into(),
        tenant,
    )?;
    credential.bearer_token(resource).await
}
#[cfg(not(feature = "auth-azure"))]
async fn azure_token(_: &str, _: Option<String>) -> Result<Zeroizing<String>> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "warehouse-adx")]
fn validate_kusto_audience(endpoint: &str, resource: &str) -> Result<()> {
    culprit_store_adx::authentication::Audience::kusto(endpoint, resource).map(|_| ())
}
#[cfg(not(feature = "warehouse-adx"))]
fn validate_kusto_audience(_: &str, _: &str) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
struct PasswordReference {
    profile: String,
    username: String,
    account: String,
}
struct Credentials {
    bearer: BTreeMap<String, Arc<dyn BearerTokenProvider>>,
    cloud: ConfiguredCredentials,
    password: Option<PasswordReference>,
    secrets: Arc<dyn SecretStore>,
}
impl CredentialResolver for Credentials {
    fn bearer(&self, profile: &str) -> Result<Arc<dyn BearerTokenProvider>> {
        self.bearer.get(profile).cloned().ok_or_else(invalid)
    }
    fn access_keys(&self, profile: &str) -> Result<Arc<dyn AccessKeyProvider>> {
        self.cloud.access_keys(profile)
    }
    fn database(&self, _: &str) -> Result<Arc<dyn DatabaseTokenProvider>> {
        Err(invalid())
    }
    fn validate_password(&self, profile: &str) -> Result<()> {
        if self.password.as_ref().is_none_or(|reference| reference.profile != profile) {
            return Err(invalid());
        }
        Ok(())
    }
    fn password(&self, profile: &str) -> Result<PasswordCredential> {
        self.validate_password(profile)?;
        let reference = self.password.as_ref().ok_or_else(invalid)?;
        let password = self.secrets.get(&reference.account)?.ok_or_else(|| {
            Error::new(ErrorCode::AuthFailed, culprit_core::t!("warehouse.connection.password_unavailable"))
        })?;
        Ok(PasswordCredential { username: reference.username.clone(), password: Zeroizing::new(password) })
    }
}
pub(super) fn validate_settings(settings: &WarehouseConnectionSettings) -> Result<()> {
    for label in [&settings.pipeline, &settings.shard] {
        if label.len() > 128 || !culprit_evidence::delivery::contract::bounded_id(label) {
            return Err(invalid());
        }
    }
    let warehouse = match &settings.warehouse {
        WarehouseEndpoint::Adx { endpoint, database, authentication } => {
            azure(authentication)?;
            validate_adx(endpoint, database)?;
            &authentication.name
        }
        WarehouseEndpoint::Clickhouse { endpoint, database, username, credential_name } => {
            profile_name(credential_name)?;
            if username.is_empty() || username.len() > 256 || username.chars().any(char::is_control) {
                return Err(invalid());
            }
            validate_clickhouse(endpoint, database)?;
            credential_name
        }
    };
    let recovery = match &settings.recovery {
        WarehouseRecovery::AzureBlob { account, container, prefix, authentication } => {
            azure(authentication)?;
            validate_blob(account, container, prefix)?;
            &authentication.name
        }
        WarehouseRecovery::S3 { bucket, region, prefix, expected_owner, authentication } => {
            profile_name(&authentication.name)?;
            if authentication.profile.as_ref().is_some_and(|profile| {
                profile.is_empty() || profile.len() > 128 || profile.chars().any(char::is_control)
            }) {
                return Err(invalid());
            }
            validate_s3(bucket, region, prefix, expected_owner)?;
            &authentication.name
        }
    };
    if warehouse == recovery {
        return Err(invalid());
    }
    Ok(())
}
fn azure(profile: &WarehouseAzureProfile) -> Result<()> {
    profile_name(&profile.name)?;
    validate_tenant(profile.tenant.as_deref())
}
#[cfg(feature = "auth-azure")]
fn validate_tenant(tenant: Option<&str>) -> Result<()> {
    if tenant.is_some_and(|tenant| !culprit_auth_azure::is_guid(tenant)) { Err(invalid()) } else { Ok(()) }
}
#[cfg(not(feature = "auth-azure"))]
fn validate_tenant(_: Option<&str>) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "warehouse-adx")]
fn validate_adx(endpoint: &str, database: &str) -> Result<()> {
    culprit_store_adx::client::AdxConfig { endpoint: endpoint.into(), database: database.into() }
        .destination_id()
        .map(|_| ())
}
#[cfg(not(feature = "warehouse-adx"))]
fn validate_adx(_: &str, _: &str) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "warehouse-clickhouse")]
fn validate_clickhouse(endpoint: &str, database: &str) -> Result<()> {
    culprit_store_clickhouse::client::ClickHouseConfig { endpoint: endpoint.into(), database: database.into() }
        .destination_id()
        .map(|_| ())
}
#[cfg(not(feature = "warehouse-clickhouse"))]
fn validate_clickhouse(_: &str, _: &str) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "evidence-azure")]
fn validate_blob(account: &str, container: &str, prefix: &str) -> Result<()> {
    culprit_blob_azure::AzureBlobConfig { account: account.into(), container: container.into(), prefix: prefix.into() }
        .access_scope()
        .map(|_| ())
}
#[cfg(not(feature = "evidence-azure"))]
fn validate_blob(_: &str, _: &str, _: &str) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "evidence-s3")]
fn validate_s3(bucket: &str, region: &str, prefix: &str, owner: &str) -> Result<()> {
    culprit_blob_s3::S3Config {
        bucket: bucket.into(),
        region: region.into(),
        prefix: prefix.into(),
        expected_owner: Some(owner.into()),
        endpoint: None,
    }
    .access_scope()
    .map(|_| ())
}
#[cfg(not(feature = "evidence-s3"))]
fn validate_s3(_: &str, _: &str, _: &str, _: &str) -> Result<()> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
#[cfg(feature = "auth-aws")]
fn aws_profile(profile: Option<String>) -> Result<CredentialProfile> {
    Ok(CredentialProfile::Aws { profile })
}
#[cfg(not(feature = "auth-aws"))]
fn aws_profile(_: Option<String>) -> Result<CredentialProfile> {
    Err(DeliveryProblem::UnsupportedSchema.error())
}
fn profile_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn compose(
    saved: &storage::Stored,
    secrets: Arc<dyn SecretStore>,
) -> Result<(EvidenceConfig, Arc<dyn CredentialResolver>)> {
    validate_settings(&saved.settings)?;
    let settings = &saved.settings;
    let mut bearer = BTreeMap::<String, Arc<dyn BearerTokenProvider>>::new();
    let mut cloud = BTreeMap::new();
    let mut password = None;
    let warehouse = match &settings.warehouse {
        WarehouseEndpoint::Adx { endpoint, database, authentication } => {
            bearer.insert(
                authentication.name.clone(),
                Arc::new(ScopedAzure {
                    scope: BearerScope::Kusto(endpoint.clone()),
                    tenant: authentication.tenant.clone(),
                }),
            );
            EvidenceStoreConfig {
                provider: "adx".into(),
                options: json!({"endpoint":endpoint,"database":database}),
                credential: authentication.name.clone(),
            }
        }
        WarehouseEndpoint::Clickhouse { endpoint, database, username, credential_name } => {
            password = Some(PasswordReference {
                profile: credential_name.clone(),
                username: username.clone(),
                account: saved.password_account.clone().ok_or_else(invalid)?,
            });
            EvidenceStoreConfig {
                provider: "clickhouse".into(),
                options: json!({"endpoint":endpoint,"database":database}),
                credential: credential_name.clone(),
            }
        }
    };
    let recovery = match &settings.recovery {
        WarehouseRecovery::AzureBlob { account, container, prefix, authentication } => {
            bearer.insert(
                authentication.name.clone(),
                Arc::new(ScopedAzure { scope: BearerScope::Storage, tenant: authentication.tenant.clone() }),
            );
            RecoveryConfig {
                provider: "azure-blob".into(),
                options: json!({"account":account,"container":container,"prefix":prefix}),
                credential: authentication.name.clone(),
            }
        }
        WarehouseRecovery::S3 { bucket, region, prefix, expected_owner, authentication } => {
            cloud.insert(authentication.name.clone(), aws_profile(authentication.profile.clone())?);
            RecoveryConfig {
                provider: "s3".into(),
                options: json!({"bucket":bucket,"region":region,"prefix":prefix,"expected_owner":expected_owner}),
                credential: authentication.name.clone(),
            }
        }
    };
    let credentials = Arc::new(Credentials { bearer, cloud: ConfiguredCredentials::new(&cloud)?, password, secrets });
    Ok((
        EvidenceConfig {
            pipeline: settings.pipeline.clone(),
            shard: settings.shard.clone(),
            credentials: BTreeMap::new(),
            warehouse,
            recovery,
        },
        credentials,
    ))
}

#[cfg(all(test, feature = "warehouse-adx", feature = "auth-azure"))]
mod tests;
