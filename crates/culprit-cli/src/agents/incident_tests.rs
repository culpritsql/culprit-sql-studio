//! Actual CLI startup installs an evidence-only host grant without saved native routes or source credentials.
use super::*;
use clap::Parser;
use culprit_core::Engine;
use culprit_engine::{ProviderConstructionPolicy, ProviderRegistration};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn warehouse_startup_is_explicit_and_rejects_native_connection_selection_instead_of_ignoring_it() {
    let cli = crate::Cli::try_parse_from(["culprit", "mcp", "--warehouse-evidence", "warehouse.json"]).unwrap();
    let crate::Command::Mcp(args) = cli.command else { panic!() };
    assert_eq!(args.warehouse_evidence, Some(PathBuf::from("warehouse.json")));
    assert!(args.connections.is_empty());
    assert!(
        crate::Cli::try_parse_from([
            "culprit",
            "mcp",
            "--warehouse-evidence",
            "warehouse.json",
            "--connection",
            "saved-route"
        ])
        .is_err()
    );
    let cli = crate::Cli::try_parse_from(["culprit", "mcp"]).unwrap();
    let crate::Command::Mcp(args) = cli.command else { panic!() };
    assert!(args.warehouse_evidence.is_none());
    assert!(
        crate::Cli::try_parse_from([
            "culprit",
            "mcp",
            "--warehouse-evidence",
            "one.json",
            "--warehouse-evidence",
            "two.json"
        ])
        .is_err()
    );
}
struct NoSourceSecrets(AtomicUsize);
impl culprit_engine::secrets::SecretStore for NoSourceSecrets {
    fn get(&self, _: &str) -> Result<Option<String>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Error::internal("source credentials forbidden"))
    }
    fn set(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn real_mcp_warehouse_startup_needs_no_native_connection_and_refuses_forged_native_calls_before_admission() {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "culprit-cli-warehouse-startup-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let secrets = Arc::new(NoSourceSecrets(AtomicUsize::new(0)));
    let workspace = Workspace::open_in(directory.join("workspace")).with_secrets(secrets.clone()).with_provider(
        Engine::Mssql,
        ProviderRegistration::new(
            Arc::new(move |_, _| {
                called.fetch_add(1, Ordering::SeqCst);
                Err(Error::internal("no native source may run"))
            }),
            ProviderConstructionPolicy::DeferredNativeWork,
            Vec::new(),
        )
        .unwrap(),
    );
    assert!(workspace.connections().unwrap().is_empty());
    let path = directory.join("evidence.json");
    let config = json!({"pipeline":"pipeline", "shard":"shard", "credentials":{
        "archive-role":{"kind":"azure", "identity":{"kind":"managed-identity", "client_id":null}},
        "warehouse-role":{"kind":"azure", "identity":{"kind":"managed-identity", "client_id":null}},
        "unused-source-role":{"kind":"password-environment", "username":"", "variable":"MUST_NOT_RESOLVE_SOURCE_CREDENTIAL"}},
        "recovery":{"provider":"azure-blob", "options":{"account":"fixtureaccount", "container":"recovery", "prefix":"private"}, "credential":"archive-role"},
        "warehouse":{"provider":"adx", "options":{"endpoint":"https://fixture.westeurope.kusto.windows.net", "database":"warehouse"}, "credential":"warehouse-role"}});
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let cli = crate::Cli::try_parse_from(["culprit", "mcp", "--warehouse-evidence", path.to_str().unwrap()]).unwrap();
    let crate::Command::Mcp(args) = cli.command else { panic!() };
    let server = mcp_server(workspace, args).unwrap();
    let result = server
        .handle(
            "startup-control",
            "incident",
            json!({"resource":{
        "namespace_id":culprit_core::telemetry::digest(b"not-used-before-bad-time"),
        "database":culprit_core::telemetry::digest(b"not-used-before-bad-time")}, "around":"bad-time"})
            .as_object()
            .unwrap()
            .clone(),
        )
        .await;
    assert_eq!(result.is_error, Some(true));
    let body: serde_json::Value = serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(body["error"]["message"], culprit_core::t!("mcp.incident.around_invalid"));
    for tool in ["scan", "run_query", "list_connections"] {
        let result = server
            .handle(
                "startup-control",
                tool,
                json!({"connection":"not-required", "sql":"SELECT private input"}).as_object().unwrap().clone(),
            )
            .await;
        let body: serde_json::Value = serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
        assert_eq!(body["error"]["code"], serde_json::to_value(culprit_core::ErrorCode::PermissionDenied).unwrap());
        assert!(body.get("provider_acquisition").is_none());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(secrets.0.load(Ordering::SeqCst), 0);
    drop(server);
    std::fs::remove_dir_all(directory).unwrap();
}
