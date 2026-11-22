use super::*;
fn policy() -> QueryHelperPolicy {
    QueryHelperPolicy {
        name: "Pilot".into(),
        analyzer: ComponentVersion::package("culprit-checks", "1", None),
        rules_version: "rules-2".into(),
        history_from: "2026-11-01T00:00:00Z".into(),
    }
}
#[test]
fn configured_names_and_clocks_cannot_become_query_text_or_an_invented_version_order() {
    for name in ["", "1Pilot", "Pilot; DROP TABLE data", "x.y", "x\nquery"] {
        let mut p = policy();
        p.name = name.into();
        assert!(p.validate().is_err());
    }
    for time in ["2026-11-01T00:00:00", "2026-11-01T00:00:00+02:00", "not UTC"] {
        let mut p = policy();
        p.history_from = time.into();
        assert!(p.validate().is_err());
    }
    let mut p = policy();
    p.analyzer.version = None;
    assert!(p.validate().is_err());
    let mut p = policy();
    p.rules_version.clear();
    assert!(p.validate().is_err());
    let mut p = policy();
    p.rules_version = "package/v10\"'\\r".into();
    assert!(p.validate().is_ok(), "version is an exact label, not a lexical order or query fragment");
}
#[test]
fn bindings_require_the_selected_contract_and_actual_store_identity() {
    let contract = Contract::new("pipeline".into(), "shard".into(), "warehouse".into(), "a".repeat(64)).unwrap();
    assert!(QueryHelperBinding::new(policy(), &contract, "a-label-is-not-a-root-id").is_err());
    let bound = QueryHelperBinding::new(policy(), &contract, &"b".repeat(64)).unwrap();
    assert_eq!(bound.contract_id, contract.id().unwrap());
    assert_eq!(bound.schema_digest, contract.schema_digest());
    assert_eq!(bound.recovery_store_id, "b".repeat(64));
    let mut foreign = bound;
    foreign.contract_id = "foreign".into();
    assert!(foreign.validate().is_err());
}

#[test]
fn policy_text_containing_another_asset_marker_is_substituted_once_and_remains_literal_data() {
    let result = render_friendly_query(
        "$NStatus $RULES $ANALYZER",
        &[("$N", "Pilot".into()), ("$RULES", "\"policy-$ANALYZER\"".into()), ("$ANALYZER", "\"checks-$RULES\"".into())],
    )
    .unwrap();
    assert_eq!(result, "PilotStatus \"policy-$ANALYZER\" \"checks-$RULES\"");
    assert!(render_friendly_query("$UNKNOWN", &[]).is_err());
}
