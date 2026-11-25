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

#[test]
fn adx_and_clickhouse_operator_packs_share_gold_views_freshness_stages_and_paused_alerts_without_rule_duplication() {
    use culprit_evidence::analytical::query_helpers::QueryHelperBinding;
    use culprit_evidence::delivery::contract::Contract;
    use std::collections::BTreeSet;

    let policy = QueryHelperPolicy {
        name: "Pilot".into(),
        analyzer: ComponentVersion::package("culprit-checks", "0.5.6", Some("6809956f")),
        rules_version: "/retained-diagnosis-13".into(),
        history_from: "2026-11-01T00:00:00Z".into(),
    };
    let adx_schema = culprit_store_adx::catalog::SchemaCatalog::new().unwrap();
    let ch_schema = culprit_store_clickhouse::catalog::SchemaCatalog::new().unwrap();
    let adx_contract =
        Contract::new("pilot".into(), "shard-0".into(), "adx:db".into(), adx_schema.digest().into()).unwrap();
    let ch_contract =
        Contract::new("pilot".into(), "shard-0".into(), "clickhouse:db".into(), ch_schema.digest().into()).unwrap();
    let store_id = "b".repeat(64);

    let adx = culprit_store_adx::query_assets::configured(
        QueryHelperBinding::new(policy.clone(), &adx_contract, &store_id).unwrap(),
    )
    .unwrap();
    let ch = culprit_store_clickhouse::query_assets::configured(
        QueryHelperBinding::new(policy, &ch_contract, &store_id).unwrap(),
    )
    .unwrap();

    let adx_names: BTreeSet<&str> =
        adx.examples.iter().map(|ex| ex.name).filter(|&n| n != "opt-in-adx-queued-batching").collect();
    let ch_names: BTreeSet<&str> = ch.examples.iter().map(|ex| ex.name).collect();
    assert_eq!(adx_names, ch_names, "both stores must expose identical logical query/alert examples");
    assert!(adx.examples.iter().any(|ex| ex.name == "opt-in-adx-queued-batching"));

    // Verify all three Gold tables and independent freshness/availability wrappers appear in both helper scripts,
    // with PublicationTimeKnown=false honesty and zero inline CPU/latency threshold detector duplication.
    for script in [&adx.script, &ch.script] {
        for gold in ["CulpritIncidentsV1", "CulpritFindingsV1", "CulpritChangesV1", "CulpritPublishedRangesV1"] {
            assert!(script.contains(gold), "helper script must consume {gold}");
        }
        assert!(script.contains("PublicationTimeKnown"));
        assert!(!script.contains("avg_cpu_percent >"), "warehouse helpers must not duplicate threshold detectors");
    }

    for (store, pack) in [("adx", &adx.operator), ("clickhouse", &ch.operator)] {
        let panels = pack.dashboard["panels"].as_array().unwrap();
        assert_eq!(panels.len(), 8, "{store} dashboard must ship all 8 bounded operator panels");
        let groups = pack.alerts["groups"].as_array().unwrap();
        let rules: Vec<&serde_json::Value> =
            groups.iter().flat_map(|g| g["rules"].as_array().unwrap().iter()).collect();
        assert_eq!(rules.len(), 3, "{store} alert pack must ship 3 paused rules (incident, deadlock, availability)");
        assert!(
            rules.iter().all(|rule| rule["isPaused"] == serde_json::json!(true)),
            "{store} provisioned alert rules must start paused"
        );
        let serialized = serde_json::to_string(&pack.alerts).unwrap();
        for freshness_kind in
            ["diagnosis", "collection", "diagnosis-unavailable", "namespace-unavailable", "acquisition-unavailable"]
        {
            assert!(serialized.contains(freshness_kind), "{store} availability alert missing {freshness_kind}");
        }
    }
}
