use super::workspace::home;
use serde_json::{Value, json};
use std::process::{Command, Output};

fn native(code: &str, line: u64) -> Value {
    json!({
        "timestamp":"2020-01-01 01:02:03.123 UTC",
        "state_code":code,"error_severity":"ERROR","dbname":"context-only-database",
        "pid":539,"session_id":"5e0bf100.21b","session_start":"2020-01-01 01:00:00 UTC",
        "line_num":line,"query_id":-9007199254740993_i64,
        "message":"private-native-message-canary","remote_host":"private-client-canary",
        "statement":"SELECT 'private-value-canary' FROM items WHERE id=42"
    })
}

fn run(tag: &str, input: &str, arguments: &[&str]) -> Output {
    let directory = home(tag);
    let path = directory.0.join("a-file-name-is-not-a-database-identity.jsonl");
    std::fs::write(&path, input).unwrap();
    let invalid_home = directory.0.join("not-a-workspace-directory");
    std::fs::write(&invalid_home, "an actual file blocks workspace/configuration access").unwrap();
    Command::new(env!("CARGO_BIN_EXE_culprit"))
        .arg("logs")
        .arg(path)
        .args(["--input-format", "json", "--json"])
        .args(arguments)
        .env("CULPRIT_HOME", invalid_home)
        .env_remove("CULPRIT_LANG")
        .env("NO_COLOR", "1")
        .output()
        .expect("bounded local-log CLI")
}

#[test]
fn supplied_failures_work_without_a_workspace_and_export_no_private_values_or_database_owner() {
    let result = run("native-logs-no-workspace", &native("53200", 1).to_string(), &[]);
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    let result: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(result["input"]["complete"], true);
    assert_eq!(result["source"]["state"], "collected");
    assert!(result["evidence"]["resource"].is_null());
    assert_eq!(result["report"]["entries"].as_array().unwrap().len(), 1);
    let native = &result["evidence"]["observations"][0]["observation"];
    assert_eq!(native["query_id"], "-9007199254740993");
    assert_eq!(native["database"], "context-only-database");
    let text = result.to_string();
    for private in [
        "private-native-message-canary",
        "private-client-canary",
        "private-value-canary",
        "a-file-name-is-not-a-database-identity",
    ] {
        assert!(!text.contains(private), "{private}");
    }
}

#[test]
fn a_malformed_record_preserves_dated_conditions_and_exits_partial_instead_of_healthy_empty() {
    let bytes = format!("{}\n{{malformed\n{}", native("53200", 1), native("57014", 2));
    let result = run("native-logs-partial", &bytes, &[]);
    assert_eq!(result.status.code(), Some(2), "{}", String::from_utf8_lossy(&result.stderr));
    let result: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(result["source"]["state"], "partial");
    assert_eq!(result["issues"].as_array().unwrap().len(), 1);
    let observations = result["evidence"]["observations"].as_array().unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0]["observation"]["position"], 0);
    assert_eq!(observations[1]["observation"]["position"], 2);
    assert_eq!(observations[1]["observation"]["sql_state"], "57014");
}

#[test]
fn an_output_cap_produces_no_truncated_json_or_sensitive_error_payload() {
    let result = run("native-logs-output-cap", &native("53200", 1).to_string(), &["--max-output-bytes", "1024"]);
    assert_eq!(result.status.code(), Some(1));
    assert!(result.stdout.is_empty());
    let error: Value = serde_json::from_slice(&result.stderr).unwrap();
    assert!(error["error"].is_object());
    assert!(!error.to_string().contains("private-value-canary"));
}

#[test]
fn explicit_extended_event_xml_works_without_a_workspace_and_is_never_binary_xel_or_timeout_authority() {
    let directory = home("native-xe-xml-no-workspace");
    let path = directory.0.join("selected.xml");
    std::fs::write(&path, "<event name='attention' package='sqlserver' timestamp='2020-01-01T12:00:00Z'><action name='sql_text' package='sqlserver'><value>SELECT 'private-xe-canary'</value></action></event>").unwrap();
    let invalid_home = directory.0.join("not-a-workspace");
    std::fs::write(&invalid_home, "not a directory").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_culprit"))
        .arg("logs")
        .arg(&path)
        .args(["--input-format", "mssql-xe-xml", "--json"])
        .env("CULPRIT_HOME", &invalid_home)
        .env_remove("CULPRIT_LANG")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let output: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output["input"]["format"], "mssql-extended-events-xml");
    assert_eq!(output["native_events"]["timeline"][0]["source"], "imported-artifact");
    assert!(output["evidence"]["resource"].is_null());
    assert!(!output.to_string().contains("private-xe-canary"));
    std::fs::write(&path, [0x1f, 0x8b, 0x08]).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_culprit"))
        .arg("logs")
        .arg(&path)
        .args(["--input-format", "mssql-xe-xml", "--json"])
        .env("CULPRIT_HOME", &invalid_home)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let output: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output["source"]["state"], "partial");
}

#[test]
fn explicit_extended_event_xel_decodes_offline_without_workspace_or_provider_and_refuses_xml_input() {
    const SAMPLE_XEL: &[u8] = include_bytes!("../../culprit-plans/src/mssql/test_support/mssql-events.sample.xel");
    let directory = home("native-xe-xel-no-workspace");
    let path = directory.0.join("production-sql.xel");
    std::fs::write(&path, SAMPLE_XEL).unwrap();
    let invalid_home = directory.0.join("blocked-workspace");
    std::fs::write(&invalid_home, "regular file blocks workspace and credential initialization").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_culprit"))
        .arg("logs")
        .arg(&path)
        .args(["--input-format", "mssql-xe-xel", "--json"])
        .env("CULPRIT_HOME", &invalid_home)
        .env_remove("CULPRIT_LANG")
        .output()
        .unwrap();
    // Sample contains 2 unsupported sql_batch_completed events alongside 4 supported events -> partial exit code 2.
    assert_eq!(output.status.code(), Some(2), "{}", String::from_utf8_lossy(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(parsed["input"]["format"], "mssql-extended-events-xel");
    assert_eq!(parsed["input"]["complete"], true);
    assert!(parsed["evidence"]["resource"].is_null());
    assert_eq!(parsed["evidence"]["observations"].as_array().unwrap().len(), 4);
    assert_eq!(parsed["native_events"]["deadlocks"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["native_events"]["locations"][0]["buffer_offset"], 0x5000);
    assert!(!parsed.to_string().contains("private-xel-error-canary"));
    assert!(!parsed.to_string().contains("production-sql.xel"));
}
