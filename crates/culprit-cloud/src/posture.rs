//! Read-only cloud control-plane security and backup posture adapters (Azure Resource Graph & AWS RDS).
//!
//! Missing or permission-restricted fields remain `None` (never `false` or `0`) and append a non-secret
//! coverage note so downstream checks report `Insufficient` rather than a false clean bill of health.

use crate::{AwsCloud, AzureCloud, CloudConnection, discovery as discover, metrics};
use culprit_core::{Engine, Error, ErrorCode, PostureSnapshot, Result, t};
use serde::Deserialize;
use serde_json::Value;

pub(crate) async fn read_azure_posture(client: &AzureCloud, connection: &CloudConnection) -> Result<PostureSnapshot> {
    match metrics::target(connection) {
        Some(metrics::Target::AzureSql { server, database }) => azure_sql_posture(client, &server, &database).await,
        Some(metrics::Target::AzureFlexible { engine, server }) => {
            azure_flexible_posture(client, engine, &server).await
        }
        _ => Err(Error::new(ErrorCode::Unsupported, t!("engine.cloud.not_cloud", name = &connection.name))),
    }
}

async fn azure_sql_posture(client: &AzureCloud, server: &str, database: &str) -> Result<PostureSnapshot> {
    #[derive(Deserialize)]
    struct ServerRow {
        public_network: Option<String>,
        min_tls: Option<String>,
        entra_only: Option<String>,
        private_endpoints: Option<u64>,
    }
    let query = format!(
        "resources | where type =~ 'microsoft.sql/servers' and name =~ '{server}' \
         | project public_network = tostring(properties.publicNetworkAccess), \
           min_tls = tostring(properties.minimalTlsVersion), \
           entra_only = tostring(properties.administrators.azureADOnlyAuthentication), \
           private_endpoints = tolong(array_length(properties.privateEndpointConnections))"
    );
    let row =
        discover::resource_graph::<ServerRow>(client, &query).await?.into_iter().next().ok_or_else(|| {
            Error::not_found(t!("engine.cloud.azure.no_database", database = database, server = server))
        })?;

    let public_network_access = match row.public_network.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("enabled") => Some(true),
        Some("disabled") => Some(false),
        _ => None,
    };
    let private_endpoint_only = match (public_network_access, row.private_endpoints) {
        (Some(false), Some(count)) if count > 0 => Some(true),
        (Some(true), _) => Some(false),
        _ => None,
    };
    let min_tls_version = row.min_tls.filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("none"));
    let tls_enforced = match min_tls_version.as_deref() {
        Some("1.2" | "1.3") => Some(true),
        Some(_) => Some(false),
        None => None,
    };
    let password_auth_enabled = match row.entra_only.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("true") => Some(false),
        Some("false") => Some(true),
        _ => None,
    };

    Ok(PostureSnapshot {
        public_network_access,
        private_endpoint_only,
        tls_enforced,
        min_tls_version,
        unencrypted_sessions: None,
        password_auth_enabled,
        auditing_enabled: None,
        exported_log_types: None,
        backup_retention_days: None,
        last_full_backup_days: None,
        last_integrity_check_days: None,
        coverage_notes: Vec::new(),
    })
}

async fn azure_flexible_posture(client: &AzureCloud, engine: Engine, server: &str) -> Result<PostureSnapshot> {
    #[derive(Deserialize)]
    struct FlexRow {
        public_network: Option<String>,
        password_auth: Option<String>,
        retention_days: Option<u32>,
    }
    let kind = if engine == Engine::Postgres { "dbforpostgresql" } else { "dbformysql" };
    let query = format!(
        "resources | where type =~ 'microsoft.{kind}/flexibleservers' and name =~ '{server}' \
         | project public_network = tostring(properties.network.publicNetworkAccess), \
           password_auth = tostring(properties.authConfig.passwordAuth), \
           retention_days = toint(properties.backup.backupRetentionDays)"
    );
    let row = discover::resource_graph::<FlexRow>(client, &query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::not_found(t!("engine.cloud.azure.no_flexible_server", server = server)))?;

    let public_network_access = match row.public_network.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("enabled") => Some(true),
        Some("disabled") => Some(false),
        _ => None,
    };
    let password_auth_enabled = match row.password_auth.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("enabled") => Some(true),
        Some("disabled") => Some(false),
        _ => None,
    };

    Ok(PostureSnapshot {
        public_network_access,
        private_endpoint_only: public_network_access.map(|pub_net| !pub_net),
        tls_enforced: None,
        min_tls_version: None,
        unencrypted_sessions: None,
        password_auth_enabled,
        auditing_enabled: None,
        exported_log_types: None,
        backup_retention_days: row.retention_days,
        last_full_backup_days: None,
        last_integrity_check_days: None,
        coverage_notes: Vec::new(),
    })
}

pub(crate) async fn read_aws_posture(client: &AwsCloud, connection: &CloudConnection) -> Result<PostureSnapshot> {
    let Some(metrics::Target::Rds { region, id, cluster, reader }) = metrics::target(connection) else {
        return Err(Error::new(ErrorCode::Unsupported, t!("engine.cloud.not_cloud", name = &connection.name)));
    };
    let resource = metrics::aws::resolve_rds(client, &region, &id, cluster, reader).await?;
    let instance = resource.instance.as_deref().unwrap_or(&id);
    let bytes = client
        .commands
        .run(&[
            "rds",
            "describe-db-instances",
            "--db-instance-identifier",
            instance,
            "--region",
            &region,
            "--output",
            "json",
        ])
        .await?;
    let json: Value = serde_json::from_slice(&bytes)
        .map_err(|e| Error::internal(t!("engine.cloud.rds.unexpected_output", error = e.to_string())))?;
    let inst =
        json.get("DBInstances").and_then(Value::as_array).and_then(|a| a.first()).ok_or_else(|| {
            Error::not_found(t!("engine.cloud.rds.no_instance", instance = instance, region = &region))
        })?;

    let public_network_access = inst.get("PubliclyAccessible").and_then(Value::as_bool);
    let backup_retention_days =
        inst.get("BackupRetentionPeriod").and_then(Value::as_u64).and_then(|d| u32::try_from(d).ok());
    let iam_enabled = inst.get("IAMDatabaseAuthenticationEnabled").and_then(Value::as_bool);
    // When IAM database authentication is disabled on RDS, password authentication is the sole mechanism.
    let password_auth_enabled = match iam_enabled {
        Some(false) => Some(true),
        Some(true) | None => None,
    };
    let exported_log_types = inst
        .get("EnabledCloudwatchLogsExports")
        .and_then(Value::as_array)
        .map(|exports| exports.iter().filter_map(Value::as_str).map(str::to_string).collect::<Vec<_>>());
    let mut coverage_notes = Vec::new();
    if exported_log_types.is_some() {
        coverage_notes.push(t!("engine.cloud.rds.note.log_exports_not_audit"));
    }

    Ok(PostureSnapshot {
        public_network_access,
        private_endpoint_only: public_network_access.map(|p| !p),
        tls_enforced: None,
        min_tls_version: None,
        unencrypted_sessions: None,
        password_auth_enabled,
        auditing_enabled: None,
        exported_log_types,
        backup_retention_days,
        last_full_backup_days: None,
        last_integrity_check_days: None,
        coverage_notes,
    })
}

#[cfg(test)]
mod tests {
    use crate::{CloudConnection, CloudHttp, CloudServices, JsonReply, PostureProvider, providers::*};
    use async_trait::async_trait;
    use culprit_core::{Engine, PostureSnapshot, Result};
    use serde_json::json;
    use std::sync::Arc;

    struct MockAzureTokens;
    #[async_trait]
    impl culprit_auth::BearerTokenProvider for MockAzureTokens {
        async fn bearer_token(&self, _: &str) -> Result<zeroize::Zeroizing<String>> {
            Ok(zeroize::Zeroizing::new("fake-arm-token".into()))
        }
    }

    struct MockAzureCommands;
    #[async_trait]
    impl culprit_auth::CommandRunner for MockAzureCommands {
        async fn output(&self, _: &[&str], _: &[(&str, &str)]) -> Result<std::process::Output> {
            unreachable!("Resource Graph posture uses CloudHttp")
        }
    }

    struct MockAzureHttp;
    #[async_trait]
    impl CloudHttp for MockAzureHttp {
        async fn get_bearer(&self, _: &str, _: &str, _: u64) -> Result<JsonReply> {
            unreachable!()
        }
    }

    struct CustomOnPremPosture;
    #[async_trait]
    impl PostureProvider for CustomOnPremPosture {
        fn name(&self) -> &str {
            "custom-vault-posture"
        }
        fn recognizes(&self, conn: &CloudConnection) -> bool {
            conn.host.ends_with(".corp.internal")
        }
        async fn read_posture(&self, _: &CloudConnection) -> Result<PostureSnapshot> {
            Ok(PostureSnapshot {
                public_network_access: Some(false),
                tls_enforced: Some(true),
                backup_retention_days: Some(35),
                ..Default::default()
            })
        }
    }

    struct MockAwsCli;
    #[async_trait]
    impl culprit_auth::CommandClient for MockAwsCli {
        async fn run(&self, args: &[&str]) -> Result<Vec<u8>> {
            assert_eq!(args[0], "rds");
            assert_eq!(args[1], "describe-db-instances");
            Ok(serde_json::to_vec(&json!({
                "DBInstances": [{
                    "DBInstanceIdentifier": "orders-db",
                    "Engine": "postgres",
                    "PubliclyAccessible": true,
                    "IAMDatabaseAuthenticationEnabled": false,
                    "BackupRetentionPeriod": 3,
                    "EnabledCloudwatchLogsExports": []
                }]
            }))
            .unwrap())
        }
    }

    #[tokio::test]
    async fn aws_rds_posture_extracts_public_access_backup_retention_and_audit_exports() {
        let aws = Arc::new(AwsCloud::new(Arc::new(MockAwsCli)));
        let services =
            CloudServices::default().with_posture_provider(aws).with_posture_provider(Arc::new(CustomOnPremPosture));

        let rds_conn = CloudConnection {
            name: "orders".into(),
            engine: Engine::Postgres,
            host: "orders-db.abc123.eu-west-1.rds.amazonaws.com".into(),
            database: Some("orders".into()),
        };
        let p = services.posture(&rds_conn).await.unwrap();
        assert_eq!(p.public_network_access, Some(true));
        assert_eq!(p.password_auth_enabled, Some(true));
        assert_eq!(p.backup_retention_days, Some(3));
        assert_eq!(p.exported_log_types, Some(Vec::<String>::new()));
        assert_eq!(
            p.auditing_enabled, None,
            "CloudWatch log export list must never claim in-engine auditing is disabled or enabled"
        );
        assert!(!p.coverage_notes.is_empty());
        assert_eq!(p.tls_enforced, None, "unobserved RDS parameter-group setting must stay None");

        let corp_conn = CloudConnection {
            name: "corp".into(),
            engine: Engine::Mssql,
            host: "sql01.corp.internal".into(),
            database: Some("erp".into()),
        };
        let corp = services.posture(&corp_conn).await.unwrap();
        assert_eq!(corp.backup_retention_days, Some(35));

        let azure = AzureCloud::new(Arc::new(MockAzureCommands), Arc::new(MockAzureTokens), Arc::new(MockAzureHttp));
        assert!(azure.recognizes(&CloudConnection {
            name: "az".into(),
            engine: Engine::Mssql,
            host: "myserver.database.windows.net".into(),
            database: Some("app".into()),
        }));
    }
}
