use super::*;
use clap::Parser;
#[test]
fn query_asset_generation_is_namespace_configured_and_accepts_no_manual_hash_or_native_connection() {
    let base = [
        "culprit",
        "collect",
        "queries",
        "--config",
        "archive.json",
        "--name",
        "Pilot",
        "--history-from",
        "2026-11-01T00:00:00Z",
        "--rules-version",
        "rules-2",
    ];
    let parsed = crate::Cli::try_parse_from(base).unwrap();
    let crate::Command::Collect(crate::collect_cmd::CollectCommand::Queries(args)) = parsed.command else { panic!() };
    assert_eq!(args.policy().unwrap().name, "Pilot");
    for extra in [["--range", "a"], ["--contract", "a"], ["--connection", "production"]] {
        assert!(crate::Cli::try_parse_from(base.into_iter().chain(extra)).is_err());
    }
    let mut policy = args.policy().unwrap();
    policy.name = "unsafe;query".into();
    assert!(policy.validate().is_err());
}

#[test]
fn text_output_moves_the_store_owned_script_verbatim_and_bounds_both_formats() {
    use culprit_evidence::analytical::query_helpers::QueryHelperBinding;
    use culprit_evidence::delivery::contract::Contract;
    let assets = || {
        let policy = QueryHelperPolicy {
            name: "Pilot".into(),
            analyzer: ComponentVersion::package("checks", "1", None),
            rules_version: "rules-2".into(),
            history_from: "2026-11-01T00:00:00Z".into(),
        };
        let contract = Contract::new("pipeline".into(), "shard".into(), "warehouse".into(), "a".repeat(64)).unwrap();
        QueryHelperAssets {
            binding: QueryHelperBinding::new(policy, &contract, &"b".repeat(64)).unwrap(),
            commands: vec!["raw command requiring store-specific framing".into()],
            script: "owned-script\n".into(),
            examples: vec![],
            operator: culprit_evidence::analytical::query_helpers::operators::OperatorDeploymentAssets {
                dashboard: serde_json::json!({"panels": []}),
                alerts: serde_json::json!({"apiVersion": 1, "groups": []}),
            },
        }
    };
    let exact = assets().script.len();
    assert_eq!(encoded(assets(), Format::Text, exact).unwrap(), b"owned-script\n");
    assert!(encoded(assets(), Format::Text, exact - 1).is_err());
    let json = encoded(assets(), Format::Json, bounded_output::DEFAULT_JSON_BYTES).unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&json).unwrap()["script"], "owned-script\n");
    assert!(encoded(assets(), Format::Json, 1).is_err());
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert!(value["operator"]["dashboard"]["panels"].is_array());
    assert_eq!(value["operator"]["alerts"]["apiVersion"], 1);
}
