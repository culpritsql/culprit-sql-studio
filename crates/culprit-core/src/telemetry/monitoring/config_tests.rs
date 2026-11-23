//! Config shorthand changes no source authority or immutable native budget grammar.
use super::*;
use serde_json::json;

fn inventory() -> serde_json::Value {
    json!({"targets":[{"id":"database","connection":"saved-route","subject":{"kind":"database"},
        "shard":"pilot","admission_group":"physical-resource"}]})
}

#[test]
fn compact_standard_uses_the_existing_defaults_and_preserves_its_serialized_budget() {
    let compact: MonitoringPolicy = serde_json::from_value(json!({"kind":"standard"})).unwrap();
    let expected = MonitoringPolicy::default();
    assert_eq!(serde_json::to_value(&compact).unwrap(), serde_json::to_value(&expected).unwrap());
    let MonitoringPolicy::Standard { budget } = compact else { panic!("standard policy") };
    assert_eq!(budget, CollectionBudget::default());
    let inventory: FleetInventory = serde_json::from_value(inventory()).unwrap();
    assert!(inventory.targets[0].schedules.is_empty());
    expected.validate(&inventory, "pilot").unwrap();
    assert!(MonitoringPolicy::Explicit.validate(&inventory, "pilot").is_err());
    assert!(inventory.validate("pilot").is_err(), "an unenrolled assignment is not a runnable fleet");
}

#[test]
fn explicit_missing_or_empty_schedules_never_selects_a_default_native_source() {
    for schedules in [None, Some(json!([]))] {
        let mut value = inventory();
        if let Some(schedules) = schedules {
            value["targets"][0]["schedules"] = schedules;
        }
        let inventory: FleetInventory = serde_json::from_value(value).unwrap();
        assert!(MonitoringPolicy::Explicit.validate(&inventory, "pilot").is_err());
    }
}

#[test]
fn a_shared_standard_override_remains_exact_and_zero_cannot_heal_to_defaults() {
    let caller = CollectionBudget {
        max_queries: 2,
        max_snapshot_rows: 100,
        page_rows: 100,
        statement_timeout_ms: 900,
        total_timeout_ms: 4_000,
        ..CollectionBudget::default()
    };
    let policy: MonitoringPolicy = serde_json::from_value(json!({"kind":"standard","budget":caller})).unwrap();
    let MonitoringPolicy::Standard { budget } = &policy else { panic!("standard policy") };
    assert_eq!(budget, &caller);
    let assignment: FleetInventory = serde_json::from_value(inventory()).unwrap();
    policy.validate(&assignment, "pilot").unwrap();
    let mut invalid = serde_json::to_value(policy).unwrap();
    invalid["budget"]["max_queries"] = json!(0);
    let invalid: MonitoringPolicy = serde_json::from_value(invalid).unwrap();
    assert!(invalid.validate(&assignment, "pilot").is_err());
}

#[test]
fn shorthand_does_not_relax_budget_or_unknown_configuration_grammar() {
    for value in [
        json!({"kind":"standard","budget":{"max_queries":2}}),
        json!({"kind":"standard","budget":null}),
        json!({"kind":"standard","budegt":{}}),
        json!({"kind":"unknown"}),
    ] {
        assert!(serde_json::from_value::<MonitoringPolicy>(value).is_err());
    }
    assert!(
        serde_json::from_value::<CollectionBudget>(json!({"max_queries":2})).is_err(),
        "persisted request/proof budgets remain complete, independent of monitoring config defaults"
    );
    let mut assignment = inventory();
    assignment["targets"][0]["schedule"] = json!([]);
    assert!(serde_json::from_value::<FleetInventory>(assignment).is_err());
}
