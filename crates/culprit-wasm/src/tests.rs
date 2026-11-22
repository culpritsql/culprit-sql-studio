use super::*;
use serde_json::{Value, json};

const POSTGRES: &str = r#"[{"Plan":{"Node Type":"Seq Scan","Relation Name":"orders","Schema":"public","Startup Cost":0.0,"Total Cost":12.5,"Plan Rows":100,"Plan Width":8}}]"#;
const DEADLOCK: &str = include_str!("../tests/data/postgres-deadlock.log");
fn response(value: std::result::Result<String, String>) -> Value {
    serde_json::from_str(&value.unwrap()).unwrap()
}
fn plan() -> Plan {
    culprit_plans::parse_plan(POSTGRES).unwrap()
}

#[test]
fn json_abi_preserves_the_existing_plan_and_rejects_a_conflicting_engine_hint() {
    let expected = serde_json::to_value(plan()).unwrap();
    assert_eq!(response(parse_plan(POSTGRES, "null")), json!({"Ok":expected.clone()}));
    assert_eq!(response(parse_plan(POSTGRES, "\"postgres\""))["Ok"], expected);
    let rejected = response(parse_plan(POSTGRES, "\"mysql\""));
    assert_eq!(rejected["Err"]["code"], "invalid_input");
    assert!(rejected.get("Ok").is_none());
    assert_eq!(response(parse_plan("", "null"))["Err"]["code"], "invalid_input");
}

#[test]
fn diff_abi_keeps_names_explicit_statistics_and_core_unknown_cost_semantics() {
    let a = plan();
    let mut b = a.clone();
    b.total_cost = 0.0;
    b.facts_mut().estimated_cost_unavailable = true;
    b.root.facts_mut().estimated_rows_unavailable = true;
    let context = CompareContext::default();
    let expected = culprit_plans::compare_plans(&a, "earlier", &b, "later", &context);
    let actual = response(diff_plans(
        &serde_json::to_string(&a).unwrap(),
        "earlier",
        &serde_json::to_string(&b).unwrap(),
        "later",
        &serde_json::to_string(&context).unwrap(),
    ));
    assert_eq!(actual["Ok"], serde_json::to_value(expected).unwrap());
    assert!(actual["Ok"]["cost_b"].is_null());
    assert!(actual["Ok"]["cost_ratio"].is_null());
    assert_eq!(response(diff_plans("{}", "a", "{}", "b", "{}"))["Err"]["code"], "invalid_input");
}

#[test]
fn deadlock_abi_is_the_same_core_observation_vector_with_native_clock_and_error_hint() {
    let expected = culprit_plans::parse_deadlocks(DEADLOCK).unwrap();
    let actual = response(parse_deadlock(DEADLOCK));
    assert_eq!(actual["Ok"], serde_json::to_value(expected).unwrap());
    assert_eq!(actual["Ok"].as_array().unwrap().len(), 1);
    assert_eq!(actual["Ok"][0]["at"], "2026-10-11T13:48:28Z");
    assert_eq!(actual["Ok"][0]["source"], "pasted");
    let missing = response(parse_deadlock("no native deadlock here"));
    assert_eq!(missing["Err"]["code"], "invalid_input");
    assert!(missing["Err"]["hint"].is_string());
}

#[test]
fn export_abi_keeps_existing_redaction_html_escaping_and_native_text_refusal() {
    let mut plan = plan();
    plan.statement = Some("SELECT * FROM orders WHERE token='wasm-private-literal'".into());
    plan.raw = None;
    let encoded = serde_json::to_string(&plan).unwrap();
    for format in ["markdown", "html"] {
        let output = response(export_plan(
            &encoded,
            "portable <script>",
            &serde_json::to_string(format).unwrap(),
            "{\"redact\":true,\"notes\":[]}",
            "2026-11-22 13:00 UTC",
        ));
        let file = &output["Ok"];
        assert_eq!(file["format"], format);
        let contents = file["contents"].as_str().unwrap();
        assert!(!contents.contains("wasm-private-literal"));
        if format == "html" {
            assert!(!contents.contains("<script>"));
        }
    }
    let missing = response(export_plan(
        &encoded,
        "portable",
        "\"native\"",
        "{\"redact\":true,\"notes\":[]}",
        "2026-11-22 13:00 UTC",
    ));
    assert_eq!(missing["Err"]["code"], "invalid_input");
    assert!(missing["Err"]["hint"].is_string());
    assert_eq!(
        response(export_plan(&encoded, "portable", "\"unknown-format\"", "{}", "2026-11-22 13:00 UTC"))["Err"]["code"],
        "invalid_input"
    );
}

#[test]
fn json_model_and_context_decode_admit_the_existing_byte_bound_and_refuse_one_over_before_deserialization() {
    let limit = culprit_plans::MAX_PLAN_BYTES;
    let mut at_limit = " ".repeat(limit - 2);
    at_limit.push_str("{}");
    let context: CompareContext = decode(&at_limit).unwrap();
    assert!(context.statistics.is_empty() && context.taken_at.is_none());
    at_limit.push(' ');
    // Invalid syntax at the first byte still yields the byte-limit error, proving refusal precedes serde.
    at_limit.replace_range(..1, "!");
    let expected = culprit_core::t!("engine.plans.too_large", size = ((limit + 1) / (1024 * 1024)).to_string());
    let context = decode::<CompareContext>(&at_limit).unwrap_err();
    assert_eq!(context.code, culprit_core::ErrorCode::InvalidInput);
    assert_eq!(context.message, expected);
    let plan = decode::<Plan>(&at_limit).unwrap_err();
    assert_eq!(plan.code, culprit_core::ErrorCode::InvalidInput);
    assert_eq!(plan.message, expected);
}
