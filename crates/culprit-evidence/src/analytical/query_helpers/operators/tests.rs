use super::*;
fn binding() -> QueryHelperBinding {
    let contract = Contract::new("pipeline".into(), "shard".into(), "warehouse".into(), "a".repeat(64)).unwrap();
    QueryHelperBinding::new(
        QueryHelperPolicy {
            name: "Pilot".into(),
            analyzer: ComponentVersion::package("checks", "1", None),
            rules_version: "rules-1".into(),
            history_from: "2026-11-01T00:00:00Z".into(),
        },
        &contract,
        &"b".repeat(64),
    )
    .unwrap()
}
fn assets(binding: &QueryHelperBinding) -> OperatorDeploymentAssets {
    configured(binding, "existing-plugin", |_| json!({"refId": "A"}), |_| json!({"refId": "A"})).unwrap()
}
#[test]
fn interpretation_changes_do_not_rename_alert_instances_but_foreign_namespaces_do() {
    let original = binding();
    let first = assets(&original);
    let mut replay = original.clone();
    replay.policy.rules_version = "new-rules".into();
    replay.policy.analyzer.version = Some("new-analyzer".into());
    replay.policy.history_from = "2026-11-02T00:00:00Z".into();
    let next = assets(&replay);
    assert_eq!(first.dashboard["uid"], next.dashboard["uid"]);
    let ids = |value: &Value| {
        value["groups"][0]["rules"].as_array().unwrap().iter().map(|rule| rule["uid"].clone()).collect::<Vec<_>>()
    };
    assert_eq!(ids(&first.alerts), ids(&next.alerts));
    replay.recovery_store_id = "c".repeat(64);
    let foreign = assets(&replay);
    assert_ne!(first.dashboard["uid"], foreign.dashboard["uid"]);
    assert_ne!(ids(&first.alerts), ids(&foreign.alerts));
    replay.contract_id.clear();
    assert!(configured(&replay, "existing-plugin", |_| json!({}), |_| json!({})).is_err());
}
#[test]
fn imported_rules_are_paused_numeric_table_conditions_without_contact_points_or_silent_errors() {
    let result = assets(&binding());
    let rules = result.alerts["groups"][0]["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 3);
    assert_eq!(result.alerts["apiVersion"], 1);
    for rule in rules {
        assert_eq!(rule["isPaused"], true);
        assert_eq!(rule["execErrState"], "Error");
        assert_eq!(rule["data"][1]["model"]["type"], "threshold");
        assert_eq!(rule["data"][1]["model"]["expression"], "A");
        assert!(rule.get("notification_settings").is_none());
        assert!(rule["uid"].as_str().unwrap().len() <= 40);
        assert_eq!(rule["labels"].as_object().unwrap().len(), 2);
    }
    assert_eq!(rules[0]["noDataState"], "OK");
    assert_eq!(rules[2]["noDataState"], "Alerting");
    assert!(configured(&binding(), "existing-plugin", |_| json!({}), |_| Value::Null).is_err());
}
#[test]
fn scoped_translations_change_presentation_without_changing_namespace_or_rule_identity() {
    let binding = binding();
    let english = culprit_core::i18n::scope("en", || assets(&binding));
    let spanish = culprit_core::i18n::scope("es", || assets(&binding));
    assert_ne!(english.dashboard["panels"][3]["title"], spanish.dashboard["panels"][3]["title"]);
    assert_eq!(english.dashboard["uid"], spanish.dashboard["uid"]);
    assert_eq!(english.alerts["groups"][0]["rules"][0]["uid"], spanish.alerts["groups"][0]["rules"][0]["uid"]);
}
