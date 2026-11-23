//! Compact diagnostic metadata from an already bounded synthetic reply; never a result/proof decoder.
use serde_json::{Value, json};
const MAX_RESOURCE_SAMPLES: usize = 2;
const MAX_FAILURES: usize = 4;
const MAX_TEXT_CHARS: usize = 96;

pub(super) fn report(request_id: &str, bytes: &[u8], parse_ok: bool) {
    eprintln!("Owned emulator response metrics: {}", summary(request_id, bytes, parse_ok));
}
fn summary(request_id: &str, bytes: &[u8], parse_ok: bool) -> Value {
    let Ok(Value::Array(frames)) = serde_json::from_slice::<Value>(bytes) else {
        return json!({"request_id":request_id,"parse_ok":parse_ok,"completion_metadata":"unavailable"});
    };
    let mut resources = Vec::new();
    let mut failures = Vec::new();
    for frame in frames.iter().filter(|frame| frame["TableKind"] == "QueryCompletionInformation") {
        let Some(columns) = frame["Columns"].as_array() else {
            continue;
        };
        let position = |name| columns.iter().position(|column| column["ColumnName"] == name);
        let (Some(event), Some(payload)) = (position("EventTypeName"), position("Payload")) else {
            continue;
        };
        let level = position("Level");
        let status = position("StatusCodeName");
        let Some(rows) = frame["Rows"].as_array() else {
            continue;
        };
        for row in rows {
            if row.get(event).and_then(Value::as_str) == Some("QueryResourceConsumption")
                && resources.len() < MAX_RESOURCE_SAMPLES
            {
                if let Some(payload) =
                    row.get(payload).and_then(Value::as_str).and_then(|text| serde_json::from_str::<Value>(text).ok())
                {
                    resources.push(resource(&payload));
                }
            } else if failures.len() < MAX_FAILURES
                && level.and_then(|index| row.get(index)).and_then(Value::as_i64).is_some_and(|level| level <= 2)
            {
                failures.push(
                    json!({"event":short(row.get(event)),"status":short(status.and_then(|index|row.get(index)))}),
                );
            }
        }
    }
    json!({"request_id":request_id,"parse_ok":parse_ok,"resources":resources,"failures":failures})
}
fn short(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(|value| value.chars().take(MAX_TEXT_CHARS).collect())
}
fn number(value: &Value) -> Option<&Value> {
    value.is_number().then_some(value)
}
fn resource(payload: &Value) -> Value {
    let cpu = &payload["resource_usage"]["cpu"];
    let input = &payload["input_dataset_statistics"];
    json!({
        "query_hash":short(payload.get("QueryHash")),"elapsed_seconds":number(&payload["ExecutionTime"]),
        "total_cpu":short(cpu.get("total cpu")),"planning_cpu":short(cpu["breakdown"].get("query planning")),
        "execution_cpu":short(cpu["breakdown"].get("query execution")),
        "peak_memory_bytes":number(&payload["resource_usage"]["memory"]["peak_per_node"]),
        "scanned_extents":number(&input["extents"]["scanned"]),"scanned_rows":number(&input["rows"]["scanned"]),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn frame(level: i32, event: &str, payload: Value) -> Value {
        json!({"TableKind":"QueryCompletionInformation","Columns":[
            {"ColumnName":"Level"},{"ColumnName":"EventTypeName"},{"ColumnName":"StatusCodeName"},{"ColumnName":"Payload"}],
            "Rows":[[level,event,"fixture-status",payload.to_string()]]})
    }
    #[test]
    fn full_bounded_reply_keeps_late_stats_separate_from_partial_failure_and_private_primary_content() {
        let private = "synthetic-primary-canary".repeat(300);
        let bytes=serde_json::to_vec(&json!([
            {"TableKind":"PrimaryResult","Rows":[[private]]},
            frame(2,"QueryRuntimeError",json!({"Text":"fixture partial failure"})),
            frame(6,"QueryResourceConsumption",json!({"QueryHash":"hash","ExecutionTime":6.4,
                "resource_usage":{"cpu":{"total cpu":"00:00:04.95","breakdown":{"query planning":"00:00:04.94","query execution":"00:00:00.01"}},"memory":{"peak_per_node":68281168}},
                "input_dataset_statistics":{"extents":{"scanned":322},"rows":{"scanned":322}}}))
        ])).unwrap();
        assert!(bytes.len() > 4096, "metadata intentionally follows the existing raw diagnostic prefix");
        let value = summary("fixture-request", &bytes, false);
        assert_eq!(value["parse_ok"], false);
        assert_eq!(value["resources"][0]["planning_cpu"], "00:00:04.94");
        assert_eq!(value["resources"][0]["execution_cpu"], "00:00:00.01");
        assert_eq!(value["resources"][0]["scanned_extents"], 322);
        assert_eq!(value["failures"][0]["event"], "QueryRuntimeError");
        assert!(!value.to_string().contains("synthetic-primary-canary"));
        assert_eq!(summary("fixture-request", &bytes, true)["parse_ok"], true);
    }
    #[test]
    fn malformed_or_omitted_metadata_is_not_zero_and_diagnostic_fields_are_bounded() {
        assert_eq!(summary("fixture", b"not-json", false)["completion_metadata"], "unavailable");
        let bytes = serde_json::to_vec(&vec![frame(2, &"x".repeat(200), json!({"Text":"not copied"})); 12]).unwrap();
        let value = summary("fixture", &bytes, false);
        assert_eq!(value["resources"], json!([]));
        assert_eq!(value["failures"].as_array().unwrap().len(), MAX_FAILURES);
        assert_eq!(value["failures"][0]["event"].as_str().unwrap().len(), MAX_TEXT_CHARS);
        assert!(!value.to_string().contains("not copied"));
    }
}
