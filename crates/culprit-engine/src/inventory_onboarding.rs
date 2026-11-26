//! Pluggable inventory onboarding: converts cloud discovery results into validated static collector shards
//! and renders GitOps-ready Helm values or collector service configurations with zero inline secrets.

use crate::{
    discover::{Cloud, DiscoveredDatabase, DiscoveryResult},
    private_fs,
    store::StoredConnection,
};
use culprit_collector::fleet::onboarding::{
    OnboardedConnection, OnboardingCandidate, OnboardingOptions, OnboardingPlan, PlannedShard, plan_inventory,
};
use culprit_core::{
    AuthKind, Engine, Error, Result, aws_iam_host_allowed, entra_host_allowed, t, telemetry::OutboxLimits,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum OnboardingTargetSpec {
    AzureAdx {
        pipeline: String,
        storage_account: String,
        storage_container: String,
        adx_endpoint: String,
        adx_database: String,
        tenant_id: Option<String>,
        client_id: Option<String>,
    },
    AwsClickHouse {
        pipeline: String,
        s3_bucket: String,
        s3_region: String,
        s3_expected_owner: String,
        clickhouse_endpoint: String,
        clickhouse_database: String,
        aws_profile: Option<String>,
        clickhouse_user: String,
        clickhouse_password_env: String,
    },
}

impl OnboardingTargetSpec {
    pub fn default_azure(pipeline: impl Into<String>) -> Self {
        Self::AzureAdx {
            pipeline: pipeline.into(),
            storage_account: "replacewithstorageacct".into(),
            storage_container: "culprit-native".into(),
            adx_endpoint: "https://replace-cluster.westeurope.kusto.windows.net".into(),
            adx_database: "culprit".into(),
            tenant_id: None,
            client_id: None,
        }
    }

    pub fn default_aws(pipeline: impl Into<String>) -> Self {
        Self::AwsClickHouse {
            pipeline: pipeline.into(),
            s3_bucket: "replace-culprit-archive".into(),
            s3_region: "us-east-1".into(),
            s3_expected_owner: "000000000000".into(),
            clickhouse_endpoint: "https://clickhouse.internal:8443".into(),
            clickhouse_database: "culprit".into(),
            aws_profile: None,
            clickhouse_user: "culprit_writer".into(),
            clickhouse_password_env: "CULPRIT_CLICKHOUSE_PASSWORD".into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if let Self::AwsClickHouse { s3_expected_owner, .. } = self
            && (s3_expected_owner.len() != 12 || !s3_expected_owner.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(Error::invalid(t!("onboarding.invalid_s3_owner")));
        }
        Ok(())
    }

    pub fn pipeline(&self) -> &str {
        match self {
            Self::AzureAdx { pipeline, .. } | Self::AwsClickHouse { pipeline, .. } => pipeline,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedArtifact {
    pub relative_path: PathBuf,
    pub contents: String,
}

/// Pluggable GitOps / configuration renderer so operators can target Helm, raw CollectorConfig JSON,
/// Kustomize, or custom internal deployment manifests without altering shard planning.
pub trait InventoryRenderer: Send + Sync {
    fn format_name(&self) -> &'static str;
    fn render(&self, plan: &OnboardingPlan, target: &OnboardingTargetSpec) -> Result<Vec<RenderedArtifact>>;
}

/// Convert a bounded cloud `DiscoveryResult` into `OnboardingCandidate` entries, refusing incomplete pages
/// or cloud warnings so partial discovery never silently omits databases from static shard assignments.
pub fn candidates_from_discovery(result: &DiscoveryResult) -> Result<Vec<OnboardingCandidate>> {
    if result.has_more || !result.warnings.is_empty() {
        return Err(Error::invalid(t!("onboarding.discovery_incomplete")));
    }
    let mut out = Vec::with_capacity(result.items.len());
    for item in &result.items {
        out.push(candidate_from_discovered(item)?);
    }
    Ok(out)
}

pub fn candidate_from_discovered(d: &DiscoveredDatabase) -> Result<OnboardingCandidate> {
    if d.cloud == Cloud::Azure && d.engine == Engine::Yugabyte {
        return Err(Error::invalid(t!("engine.connection.yugabyte_not_azure")));
    }
    let mut options = BTreeMap::new();
    let (auth, port) = match d.cloud {
        Cloud::Azure => match d.engine {
            Engine::Mssql => {
                options.insert("encrypt".into(), "True".into());
                (AuthKind::Entra, d.port.or(Some(1433)))
            }
            Engine::Mysql => {
                options.insert("ssl-mode".into(), "VERIFY_IDENTITY".into());
                let auth = if d.auth == AuthKind::Entra && entra_host_allowed(d.engine, &d.host) {
                    AuthKind::Entra
                } else {
                    AuthKind::Password
                };
                (auth, d.port.or(Some(3306)))
            }
            _ => {
                options.insert("sslmode".into(), "verify-full".into());
                let auth = if d.auth == AuthKind::Entra && entra_host_allowed(d.engine, &d.host) {
                    AuthKind::Entra
                } else {
                    AuthKind::Password
                };
                (auth, d.port.or(Some(5432)))
            }
        },
        Cloud::Aws => {
            if let Some(region) = d.region.as_deref().filter(|r| !r.is_empty()) {
                options.insert("region".into(), region.to_string());
            }
            let auth = if d.auth == AuthKind::AwsIam && d.engine != Engine::Mssql && aws_iam_host_allowed(&d.host) {
                AuthKind::AwsIam
            } else {
                AuthKind::Password
            };
            let default_port = match d.engine {
                Engine::Mssql => {
                    options.insert("encrypt".into(), "True".into());
                    1433
                }
                Engine::Mysql => {
                    options.insert("ssl-mode".into(), "VERIFY_IDENTITY".into());
                    3306
                }
                _ => {
                    options.insert("sslmode".into(), "verify-full".into());
                    5432
                }
            };
            (auth, d.port.or(Some(default_port)))
        }
    };

    Ok(OnboardingCandidate {
        alias: d.alias.clone(),
        engine: d.engine,
        environment: d.environment,
        server_group: d.server.clone(),
        host: d.host.clone(),
        port,
        database: d.database.clone(),
        user: d.user.clone(),
        auth,
        options,
    })
}

pub fn plan_discovered_inventory(result: &DiscoveryResult, options: &OnboardingOptions) -> Result<OnboardingPlan> {
    let candidates = candidates_from_discovery(result)?;
    plan_inventory(&candidates, options)
}

pub fn write_rendered_artifacts(output_dir: &Path, artifacts: &[RenderedArtifact]) -> Result<Vec<PathBuf>> {
    private_fs::create_dir(output_dir).map_err(|e| Error::internal(format!("{}: {e}", output_dir.display())))?;
    let mut written = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        if artifact.relative_path.is_absolute()
            || artifact.relative_path.components().any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(Error::invalid(t!("onboarding.invalid_options")));
        }
        let full = output_dir.join(&artifact.relative_path);
        if let Some(parent) = full.parent() {
            private_fs::create_dir(parent).map_err(|e| Error::internal(format!("{}: {e}", parent.display())))?;
        }
        private_fs::write_atomic(&full, artifact.contents.as_bytes())
            .map_err(|e| Error::internal(format!("{}: {e}", full.display())))?;
        written.push(full);
    }
    Ok(written)
}

fn password_env_name(connection_name: &str) -> String {
    let suffix: String =
        connection_name.chars().map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' }).collect();
    format!("CULPRIT_DB_PASSWORD_{suffix}")
}

fn stored_connection_from_onboarded(conn: &OnboardedConnection) -> StoredConnection {
    let password_env = (conn.auth == AuthKind::Password).then(|| password_env_name(&conn.name));
    StoredConnection {
        id: String::new(),
        engine: conn.engine,
        environment: conn.environment,
        host: conn.host.clone(),
        port: conn.port,
        database: conn.database.clone(),
        user: conn.user.clone(),
        auth: conn.auth,
        password_env,
        password_saved: Some(false),
        options: conn.options.clone(),
        ssh: None,
        agent: None,
        query_log: None,
        no_query_log: false,
    }
}

fn build_credentials_and_bindings(
    shard: &PlannedShard,
    target: &OnboardingTargetSpec,
    token_file: &Path,
) -> (Map<String, Value>, BTreeMap<String, String>) {
    let mut credentials = Map::new();
    let mut source_credentials = BTreeMap::new();
    let needs_entra = shard.connections.values().any(|c| c.auth == AuthKind::Entra);
    let needs_aws_iam = shard.connections.values().any(|c| c.auth == AuthKind::AwsIam);

    let azure_profile = match target {
        OnboardingTargetSpec::AzureAdx { tenant_id: Some(tid), client_id: Some(cid), .. } => json!({
            "kind": "azure",
            "identity": {
                "kind": "workload-identity",
                "tenant_id": tid,
                "client_id": cid,
                "token_file": token_file
            }
        }),
        _ => json!({
            "kind": "azure",
            "identity": {
                "kind": "managed-identity",
                "client_id": Value::Null
            }
        }),
    };
    let aws_profile_val = match target {
        OnboardingTargetSpec::AwsClickHouse { aws_profile, .. } => json!({
            "kind": "aws",
            "profile": aws_profile
        }),
        _ => json!({
            "kind": "aws",
            "profile": Value::Null
        }),
    };

    match target {
        OnboardingTargetSpec::AzureAdx { .. } => {
            credentials.insert("source".into(), azure_profile.clone());
            credentials.insert("recovery".into(), azure_profile.clone());
            credentials.insert("analytics".into(), azure_profile.clone());
            credentials.insert("ingestion".into(), azure_profile);
            if needs_aws_iam {
                credentials.insert("aws-source".into(), aws_profile_val);
            }
            for conn in shard.connections.values() {
                match conn.auth {
                    AuthKind::Entra => {
                        source_credentials.insert(conn.name.clone(), "source".into());
                    }
                    AuthKind::AwsIam => {
                        source_credentials.insert(conn.name.clone(), "aws-source".into());
                    }
                    AuthKind::Password | AuthKind::None => {}
                }
            }
        }
        OnboardingTargetSpec::AwsClickHouse { clickhouse_user, clickhouse_password_env, .. } => {
            credentials.insert("source".into(), aws_profile_val.clone());
            credentials.insert("recovery".into(), aws_profile_val);
            credentials.insert(
                "analytics".into(),
                json!({
                    "kind": "password-environment",
                    "username": clickhouse_user,
                    "variable": clickhouse_password_env
                }),
            );
            if needs_entra {
                credentials.insert("azure-source".into(), azure_profile);
            }
            for conn in shard.connections.values() {
                match conn.auth {
                    AuthKind::AwsIam => {
                        source_credentials.insert(conn.name.clone(), "source".into());
                    }
                    AuthKind::Entra => {
                        source_credentials.insert(conn.name.clone(), "azure-source".into());
                    }
                    AuthKind::Password | AuthKind::None => {}
                }
            }
        }
    }
    (credentials, source_credentials)
}

fn recovery_and_target_json(shard_name: &str, target: &OnboardingTargetSpec) -> (Value, Value) {
    match target {
        OnboardingTargetSpec::AzureAdx {
            pipeline,
            storage_account,
            storage_container,
            adx_endpoint,
            adx_database,
            ..
        } => (
            json!({
                "provider": "azure-blob",
                "options": {
                    "account": storage_account,
                    "container": storage_container,
                    "prefix": format!("{pipeline}/{shard_name}")
                },
                "credential": "recovery"
            }),
            json!({
                "kind": "adx",
                "query": {
                    "endpoint": adx_endpoint,
                    "database": adx_database
                },
                "credential": "analytics",
                "delivery": {
                    "credential": "ingestion"
                }
            }),
        ),
        OnboardingTargetSpec::AwsClickHouse {
            pipeline,
            s3_bucket,
            s3_region,
            s3_expected_owner,
            clickhouse_endpoint,
            clickhouse_database,
            ..
        } => (
            json!({
                "provider": "s3",
                "options": {
                    "bucket": s3_bucket,
                    "region": s3_region,
                    "prefix": format!("{pipeline}/{shard_name}"),
                    "expected_owner": s3_expected_owner
                },
                "credential": "recovery"
            }),
            json!({
                "kind": "clickhouse",
                "query": {
                    "endpoint": clickhouse_endpoint,
                    "database": clickhouse_database
                },
                "credential": "analytics"
            }),
        ),
    }
}

/// Renders one Helm `values-<shard>.yaml` file per static shard for `deploy/helm/culprit-collector`.
/// Defaults to `replicaCount: 0` and `activation.sourceQuiescent: false` with zero inline secrets.
#[derive(Clone, Copy, Debug, Default)]
pub struct HelmValuesRenderer;

impl InventoryRenderer for HelmValuesRenderer {
    fn format_name(&self) -> &'static str {
        "helm"
    }

    fn render(&self, plan: &OnboardingPlan, target: &OnboardingTargetSpec) -> Result<Vec<RenderedArtifact>> {
        target.validate()?;
        let mut artifacts = Vec::with_capacity(plan.shards.len());
        let limits = OutboxLimits::default();

        for shard in &plan.shards {
            let (credentials, source_credentials) =
                build_credentials_and_bindings(shard, target, Path::new("/var/run/secrets/culprit/token"));
            let (recovery, target_cfg) = recovery_and_target_json(&shard.shard, target);

            let mut helm_connections = Map::new();
            for (name, conn) in &shard.connections {
                let stored = stored_connection_from_onboarded(conn);
                let mut entry = Map::new();
                entry.insert("engine".into(), serde_json::to_value(stored.engine).map_err(internal_err)?);
                entry.insert("environment".into(), serde_json::to_value(stored.environment).map_err(internal_err)?);
                entry.insert("host".into(), Value::String(stored.host));
                if let Some(port) = stored.port {
                    entry.insert("port".into(), Value::Number(port.into()));
                }
                if let Some(db) = stored.database {
                    entry.insert("database".into(), Value::String(db));
                }
                if let Some(user) = stored.user {
                    entry.insert("user".into(), Value::String(user));
                }
                entry.insert("auth".into(), serde_json::to_value(stored.auth).map_err(internal_err)?);
                if let Some(pw_env) = stored.password_env {
                    entry.insert("password_env".into(), Value::String(pw_env));
                }
                if !stored.options.is_empty() {
                    entry.insert("options".into(), serde_json::to_value(stored.options).map_err(internal_err)?);
                }
                helm_connections.insert(name.clone(), Value::Object(entry));
            }

            let wi_block = match target {
                OnboardingTargetSpec::AzureAdx { tenant_id: Some(tid), client_id: Some(cid), .. } => json!({
                    "enabled": true,
                    "tenantId": tid,
                    "clientId": cid,
                    "tokenExpirationSeconds": 3600
                }),
                _ => json!({
                    "enabled": false,
                    "tenantId": "",
                    "clientId": "",
                    "tokenExpirationSeconds": 3600
                }),
            };

            let root = json!({
                "replicaCount": 0,
                "activation": {
                    "sourceQuiescent": false,
                    "deliveryOnly": false,
                    "initNamespaceJob": { "enabled": false }
                },
                "azureWorkloadIdentity": wi_block,
                "connections": helm_connections,
                "collector": {
                    "pipeline": target.pipeline(),
                    "shard": shard.shard,
                    "healthListen": "0.0.0.0:9098",
                    "limits": limits,
                    "monitoring": shard.monitoring,
                    "inventory": shard.inventory,
                    "credentials": credentials,
                    "sourceCredentials": source_credentials,
                    "recovery": recovery,
                    "target": target_cfg
                }
            });

            let mut yaml = String::from(
                "# Generated by `culprit collect sync-inventory`.\n\
                 # Keep replicaCount: 0 until namespace init completes and sourceQuiescent: true is verified.\n",
            );
            emit_yaml_value(&root, 0, &mut yaml);
            yaml.push('\n');

            artifacts.push(RenderedArtifact {
                relative_path: PathBuf::from(format!("values-{}.yaml", shard.shard)),
                contents: yaml,
            });
        }
        Ok(artifacts)
    }
}

/// Renders standalone `<shard>/workspace/connections.toml` and `<shard>/collector.json` files
/// ready for `culprit collect service validate --config <shard>/collector.json`.
#[derive(Clone, Debug)]
pub struct JsonConfigRenderer {
    pub runtime_root: PathBuf,
}

impl Default for JsonConfigRenderer {
    fn default() -> Self {
        Self { runtime_root: PathBuf::from("/var/lib/culprit") }
    }
}

#[derive(Serialize)]
struct RenderedConnectionsToml<'a> {
    connections: BTreeMap<&'a str, StoredConnection>,
}

impl InventoryRenderer for JsonConfigRenderer {
    fn format_name(&self) -> &'static str {
        "collector-json"
    }

    fn render(&self, plan: &OnboardingPlan, target: &OnboardingTargetSpec) -> Result<Vec<RenderedArtifact>> {
        target.validate()?;
        if !self.runtime_root.is_absolute() {
            return Err(Error::invalid(t!("onboarding.invalid_options")));
        }
        let mut artifacts = Vec::with_capacity(plan.shards.len() * 2);
        let limits = OutboxLimits::default();

        for shard in &plan.shards {
            let mut stored_map = BTreeMap::new();
            for (name, conn) in &shard.connections {
                stored_map.insert(name.as_str(), stored_connection_from_onboarded(conn));
            }
            let connections_toml =
                toml::to_string_pretty(&RenderedConnectionsToml { connections: stored_map }).map_err(internal_err)?;

            let shard_root = self.runtime_root.join(&shard.shard);
            let workspace_dir = shard_root.join("workspace");
            let state_dir = shard_root.join("state");
            let default_token = Path::new("/var/run/secrets/culprit/token");
            let token_file = if self.runtime_root == Path::new("/var/lib/culprit") && default_token.is_absolute() {
                default_token.to_path_buf()
            } else {
                shard_root.join("secrets").join("token")
            };
            let (credentials, source_credentials) = build_credentials_and_bindings(shard, target, &token_file);
            let (recovery, target_cfg) = recovery_and_target_json(&shard.shard, target);

            let collector_json = serde_json::to_string_pretty(&json!({
                "pipeline": target.pipeline(),
                "shard": shard.shard,
                "workspace": workspace_dir,
                "state": state_dir,
                "inventory": shard.inventory,
                "monitoring": shard.monitoring,
                "credentials": credentials,
                "source_credentials": source_credentials,
                "recovery": recovery,
                "target": target_cfg,
                "limits": limits,
                "health_listen": "0.0.0.0:9098"
            }))
            .map_err(internal_err)?;

            artifacts.push(RenderedArtifact {
                relative_path: PathBuf::from(&shard.shard).join("workspace").join("connections.toml"),
                contents: connections_toml,
            });
            artifacts.push(RenderedArtifact {
                relative_path: PathBuf::from(&shard.shard).join("collector.json"),
                contents: format!("{collector_json}\n"),
            });
        }
        Ok(artifacts)
    }
}

fn internal_err(err: impl std::fmt::Display) -> Error {
    Error::internal(err.to_string())
}

fn emit_yaml_value(value: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => {
            out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into()));
        }
        Value::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push('\n');
                    out.push_str(&pad);
                }
                match item {
                    Value::Object(_) | Value::Array(_) => {
                        out.push_str("-\n");
                        out.push_str(&" ".repeat(indent + 2));
                        emit_yaml_value(item, indent + 2, out);
                    }
                    _ => {
                        out.push_str("- ");
                        emit_yaml_value(item, indent + 2, out);
                    }
                }
            }
        }
        Value::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            for (idx, (k, v)) in map.iter().enumerate() {
                if idx > 0 {
                    out.push('\n');
                    out.push_str(&pad);
                }
                if k.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')) && !k.is_empty() {
                    out.push_str(k);
                } else {
                    out.push_str(&serde_json::to_string(k).unwrap_or_else(|_| "\"\"".into()));
                }
                match v {
                    Value::Object(inner) if !inner.is_empty() => {
                        out.push_str(":\n");
                        out.push_str(&" ".repeat(indent + 2));
                        emit_yaml_value(v, indent + 2, out);
                    }
                    Value::Array(items) if !items.is_empty() => {
                        out.push_str(":\n");
                        out.push_str(&" ".repeat(indent + 2));
                        emit_yaml_value(v, indent + 2, out);
                    }
                    _ => {
                        out.push_str(": ");
                        emit_yaml_value(v, indent + 2, out);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_core::Environment;

    fn sample_discovery() -> DiscoveryResult {
        DiscoveryResult {
            items: vec![
                DiscoveredDatabase {
                    alias: "orders-prod".into(),
                    cloud: Cloud::Azure,
                    engine: Engine::Mssql,
                    environment: Environment::Production,
                    server: "sql-pool-main".into(),
                    host: "sql-pool-main.database.windows.net".into(),
                    database: "orders".into(),
                    location: "westeurope".into(),
                    subscription: "sub-1".into(),
                    resource_group: "rg-prod".into(),
                    port: Some(1433),
                    user: None,
                    region: None,
                    auth: AuthKind::Entra,
                    saved_as: None,
                },
                DiscoveredDatabase {
                    alias: "payments-prod".into(),
                    cloud: Cloud::Azure,
                    engine: Engine::Mssql,
                    environment: Environment::Production,
                    server: "sql-pool-main".into(),
                    host: "sql-pool-main.database.windows.net".into(),
                    database: "payments".into(),
                    location: "westeurope".into(),
                    subscription: "sub-1".into(),
                    resource_group: "rg-prod".into(),
                    port: Some(1433),
                    user: None,
                    region: None,
                    auth: AuthKind::Entra,
                    saved_as: None,
                },
                DiscoveredDatabase {
                    alias: "users-flex".into(),
                    cloud: Cloud::Azure,
                    engine: Engine::Postgres,
                    environment: Environment::Staging,
                    server: "pg-flex-users".into(),
                    host: "pg-flex-users.postgres.database.azure.com".into(),
                    database: "users".into(),
                    location: "westeurope".into(),
                    subscription: "sub-1".into(),
                    resource_group: "rg-stg".into(),
                    port: Some(5432),
                    user: Some("pgadmin".into()),
                    region: None,
                    auth: AuthKind::Password,
                    saved_as: None,
                },
            ],
            has_more: false,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn incomplete_discovery_is_refused_before_planning() {
        let mut truncated = sample_discovery();
        truncated.has_more = true;
        assert!(plan_discovered_inventory(&truncated, &OnboardingOptions::default()).is_err());

        let mut warned = sample_discovery();
        warned.warnings.push("subscription throttled".into());
        assert!(plan_discovered_inventory(&warned, &OnboardingOptions::default()).is_err());
    }

    #[test]
    fn helm_renderer_emits_zero_replica_quiescent_false_and_no_inline_secrets() {
        let plan = plan_discovered_inventory(
            &sample_discovery(),
            &OnboardingOptions { shard_count: 4, shard_prefix: "estate".into(), ..Default::default() },
        )
        .unwrap();
        let spec = OnboardingTargetSpec::AzureAdx {
            pipeline: "database-telemetry".into(),
            storage_account: "culpritprodstore".into(),
            storage_container: "culprit-native".into(),
            adx_endpoint: "https://culprit-obs.westeurope.kusto.windows.net".into(),
            adx_database: "culprit".into(),
            tenant_id: Some("d8353d2a-b153-4d17-8827-902c51f72357".into()),
            client_id: Some("2a82e7db-d5f3-4f59-8dd5-3da21f70bf5c".into()),
        };
        let rendered = HelmValuesRenderer.render(&plan, &spec).unwrap();
        assert!(!rendered.is_empty());
        for file in &rendered {
            assert!(file.contents.contains("replicaCount: 0"));
            assert!(file.contents.contains("sourceQuiescent: false"));
            assert!(!file.contents.contains("\n    password:"));
            assert!(!file.contents.contains("\n    token:"));
            assert!(!file.contents.contains("\n    secret:"));
        }
    }

    #[cfg(all(feature = "store-adx", feature = "recovery-azure", feature = "auth-azure"))]
    #[test]
    fn json_config_renderer_passes_collector_service_validate_end_to_end() {
        let dir = std::env::temp_dir().join(format!("culprit-onboarding-validate-az-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let plan = plan_discovered_inventory(&sample_discovery(), &OnboardingOptions::default()).unwrap();
        let renderer = JsonConfigRenderer { runtime_root: dir.clone() };
        let spec = OnboardingTargetSpec::AzureAdx {
            pipeline: "database-telemetry".into(),
            storage_account: "culpritprodstore".into(),
            storage_container: "culprit-native".into(),
            adx_endpoint: "https://culprit-obs.westeurope.kusto.windows.net".into(),
            adx_database: "culprit".into(),
            tenant_id: Some("d8353d2a-b153-4d17-8827-902c51f72357".into()),
            client_id: Some("2a82e7db-d5f3-4f59-8dd5-3da21f70bf5c".into()),
        };
        let artifacts = renderer.render(&plan, &spec).unwrap();
        let paths = write_rendered_artifacts(&dir, &artifacts).unwrap();
        let cfg_path = paths.iter().find(|p| p.ends_with("collector.json")).unwrap();
        let cfg = crate::collector_service::config::CollectorConfig::from_file(cfg_path).unwrap();
        crate::collector_service::validate(&cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(all(feature = "store-clickhouse", feature = "recovery-s3", feature = "auth-aws"))]
    #[test]
    fn aws_clickhouse_renderers_emit_s3_expected_owner_and_pass_collector_service_validate() {
        let dir = std::env::temp_dir().join(format!("culprit-onboarding-validate-aws-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let aws_discovery = DiscoveryResult {
            items: vec![DiscoveredDatabase {
                alias: "orders-rds".into(),
                cloud: Cloud::Aws,
                engine: Engine::Postgres,
                environment: Environment::Production,
                server: "orders-rds".into(),
                host: "orders-rds.abc123.us-east-1.rds.amazonaws.com".into(),
                database: "orders".into(),
                location: "us-east-1".into(),
                subscription: "123456789012".into(),
                resource_group: String::new(),
                port: Some(5432),
                user: Some("culprit_reader".into()),
                region: Some("us-east-1".into()),
                auth: AuthKind::AwsIam,
                saved_as: None,
            }],
            has_more: false,
            warnings: Vec::new(),
        };
        let plan = plan_discovered_inventory(&aws_discovery, &OnboardingOptions::default()).unwrap();
        let mut bad_spec = OnboardingTargetSpec::default_aws("database-telemetry");
        if let OnboardingTargetSpec::AwsClickHouse { s3_expected_owner, .. } = &mut bad_spec {
            *s3_expected_owner = "not-12-digits".into();
        }
        assert!(HelmValuesRenderer.render(&plan, &bad_spec).is_err());
        assert!(JsonConfigRenderer { runtime_root: dir.clone() }.render(&plan, &bad_spec).is_err());

        let spec = OnboardingTargetSpec::AwsClickHouse {
            pipeline: "database-telemetry".into(),
            s3_bucket: "culprit-prod-archive".into(),
            s3_region: "us-east-1".into(),
            s3_expected_owner: "123456789012".into(),
            clickhouse_endpoint: "https://clickhouse.internal:8443".into(),
            clickhouse_database: "culprit".into(),
            aws_profile: None,
            clickhouse_user: "culprit_writer".into(),
            clickhouse_password_env: "CULPRIT_CLICKHOUSE_PASSWORD".into(),
        };
        let helm_files = HelmValuesRenderer.render(&plan, &spec).unwrap();
        assert_eq!(helm_files.len(), 1);
        assert!(helm_files[0].contents.contains("expected_owner: \"123456789012\""));

        let renderer = JsonConfigRenderer { runtime_root: dir.clone() };
        let artifacts = renderer.render(&plan, &spec).unwrap();
        let paths = write_rendered_artifacts(&dir, &artifacts).unwrap();
        let cfg_path = paths.iter().find(|p| p.ends_with("collector.json")).unwrap();
        let cfg = crate::collector_service::config::CollectorConfig::from_file(cfg_path).unwrap();
        crate::collector_service::validate(&cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    struct CustomKustomizeRenderer;
    impl InventoryRenderer for CustomKustomizeRenderer {
        fn format_name(&self) -> &'static str {
            "kustomize"
        }
        fn render(&self, plan: &OnboardingPlan, _target: &OnboardingTargetSpec) -> Result<Vec<RenderedArtifact>> {
            let names: Vec<_> = plan.shards.iter().map(|s| format!("  - {}.yaml", s.shard)).collect();
            Ok(vec![RenderedArtifact {
                relative_path: PathBuf::from("kustomization.yaml"),
                contents: format!("resources:\n{}\n", names.join("\n")),
            }])
        }
    }

    #[test]
    fn custom_inventory_renderer_plugs_in_without_touching_planner() {
        let plan = plan_discovered_inventory(&sample_discovery(), &OnboardingOptions::default()).unwrap();
        let renderer: &dyn InventoryRenderer = &CustomKustomizeRenderer;
        let out = renderer.render(&plan, &OnboardingTargetSpec::default_azure("db-telemetry")).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].contents.contains("shard-00.yaml"));
    }
}
