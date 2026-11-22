//! Actual startup constructs the read capability lazily; neither source nor warehouse work runs in these controls.
use super::*;
use clap::Parser;
use culprit_core::{AuthKind, Engine};
use culprit_engine::{NewConnection, ProviderConstructionPolicy, ProviderRegistration};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn one_startup_evidence_configuration_enables_lookup_without_per_tool_hash_or_credentials() {
    let cli = crate::Cli::try_parse_from([
        "culprit",
        "mcp",
        "--warehouse-evidence",
        "warehouse.json",
        "--connection",
        "saved-route",
    ])
    .unwrap();
    let crate::Command::Mcp(args) = cli.command else { panic!() };
    assert_eq!(args.warehouse_evidence, Some(PathBuf::from("warehouse.json")));
    assert_eq!(args.connections, ["saved-route"]);
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
#[tokio::test]
async fn real_mcp_startup_uses_evidence_runtime_and_refuses_bad_time_without_a_source_factory_or_token_request() {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "culprit-cli-incident-startup-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let workspace = Workspace::open_in(directory.join("workspace")).with_provider(
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
    workspace
        .add_connection(NewConnection {
            name: "saved-route".into(),
            input: "Server=fixture.invalid,14333;Database=ExactDB".into(),
            engine: Some(Engine::Mssql),
            auth: Some(AuthKind::None),
            ..Default::default()
        })
        .unwrap();
    workspace.set_agent_access("saved-route", AgentAccess::Stats).unwrap();
    let path = directory.join("evidence.json");
    let config = json!({"pipeline":"pipeline", "shard":"shard", "credentials":{
        "archive-role":{"kind":"azure", "identity":{"kind":"managed-identity", "client_id":null}},
        "warehouse-role":{"kind":"azure", "identity":{"kind":"managed-identity", "client_id":null}},
        "unused-source-role":{"kind":"password-environment", "username":"", "variable":"MUST_NOT_RESOLVE_SOURCE_CREDENTIAL"}},
        "recovery":{"provider":"azure-blob", "options":{"account":"fixtureaccount", "container":"recovery", "prefix":"private"}, "credential":"archive-role"},
        "warehouse":{"provider":"adx", "options":{"endpoint":"https://fixture.westeurope.kusto.windows.net", "database":"warehouse"}, "credential":"warehouse-role"}});
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let cli = crate::Cli::try_parse_from([
        "culprit",
        "mcp",
        "--warehouse-evidence",
        path.to_str().unwrap(),
        "--connection",
        "saved-route",
    ])
    .unwrap();
    let crate::Command::Mcp(args) = cli.command else { panic!() };
    let server = mcp_server(workspace, args).unwrap();
    let result = server
        .handle(
            "startup-control",
            "incident",
            json!({"connection":"saved-route", "around":"bad-time"}).as_object().unwrap().clone(),
        )
        .await;
    assert_eq!(result.is_error, Some(true));
    let body: serde_json::Value = serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(body["error"]["message"], culprit_core::t!("mcp.incident.around_invalid"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(server);
    std::fs::remove_dir_all(directory).unwrap();
}
