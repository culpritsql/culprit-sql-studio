use super::super::incidents::tests::{Host, body};
use super::*;
use rmcp::ServerHandler;

fn xml(at: &str) -> String {
    format!(
        "<event name=\"attention\" package=\"sqlserver\" timestamp=\"{at}\"><action name=\"client_app_name\" package=\"sqlserver\"><value>&lt;script&gt;data&lt;/script&gt;</value></action><action name=\"sql_text\" package=\"sqlserver\"><value>SELECT 'private-event-literal'</value></action></event>"
    )
}
fn input(xml: String) -> serde_json::Map<String, Value> {
    json!({"xml":xml}).as_object().unwrap().clone()
}

#[test]
fn source_free_tool_accepts_only_supplied_xml_and_is_available_in_evidence_only_mode() {
    let host = Host::new();
    let tool = host.server.get_tool(NATIVE_EVENTS_TOOL).unwrap();
    let properties = tool.input_schema["properties"].as_object().unwrap();
    assert_eq!(properties.len(), 1);
    assert!(properties.contains_key("xml"));
    assert_eq!(tool.input_schema["additionalProperties"], false);
    assert_eq!(tool.annotations.as_ref().unwrap().read_only_hint, Some(true));
    assert_eq!(tool.annotations.as_ref().unwrap().open_world_hint, Some(false));
    host.assert_source_free();
}

#[tokio::test]
async fn evidence_only_intake_uses_the_engine_report_artifact_clocks_and_single_audit_without_database_lookup() {
    let host = Host::new();
    let text = format!("<events>{}{}</events>", xml("2020-01-01T12:00:00Z"), xml("2020-01-01T13:00:00"));
    let expected = culprit_engine::native_logs::analyze_bytes(
        text.as_bytes(),
        NativeLogFormat::MssqlExtendedEventsXml,
        Default::default(),
    )
    .unwrap();
    let result = host.server.handle("xml-test", NATIVE_EVENTS_TOOL, input(text)).await;
    assert_ne!(result.is_error, Some(true));
    assert!(serde_json::to_vec(&result).unwrap().len() <= super::super::response::MAX_OUTPUT_BYTES);
    let value = body(&result);
    assert_eq!(value["input"], json!(expected.input));
    assert_eq!(value["report"], json!(expected.report));
    assert_eq!(value["native_events"], json!(expected.native_events));
    assert!(value["evidence"]["resource"].is_null());
    assert!(!value.to_string().contains("private-event-literal"));
    let audit = host.server.ws.audit_log(2).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].tool, NATIVE_EVENTS_TOOL);
    assert!(audit[0].connection.is_none() && audit[0].sql.is_none());
    host.assert_source_free();
}

#[tokio::test]
async fn paths_extra_configuration_oversize_and_excess_result_envelopes_never_publish_a_success() {
    let host = Host::new();
    for args in [
        json!({"path":"/private/not-opened.xml"}),
        json!({"xml":xml("2020-01-01T12:00:00Z"),"connection":"must-not-look-up"}),
        json!({"xml":"x".repeat(NativeLogLimits::default().input_bytes+1)}),
    ] {
        let result = host.server.handle("xml-test", NATIVE_EVENTS_TOOL, args.as_object().unwrap().clone()).await;
        assert_eq!(result.is_error, Some(true));
        assert!(body(&result).get("input").is_none());
    }
    let large = format!("<events>{}</events>", xml("2020-01-01T12:00:00Z").repeat(1024));
    assert!(large.len() < NativeLogLimits::default().input_bytes);
    let result = host.server.handle("xml-test", NATIVE_EVENTS_TOOL, input(large)).await;
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("input").is_none());
    assert!(serde_json::to_vec(&result).unwrap().len() <= super::super::response::MAX_OUTPUT_BYTES);
    host.assert_source_free();
}

#[tokio::test]
async fn cancellation_before_dispatch_settles_the_owned_operation_and_malformed_input_stays_partial() {
    let host = Host::new();
    let result = host
        .server
        .handle_cancellable("xml-test", NATIVE_EVENTS_TOOL, input(xml("2020-01-01T12:00:00Z")), async {})
        .await;
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("input").is_none());
    let result = host.server.handle("xml-test", NATIVE_EVENTS_TOOL, input("<events><event".into())).await;
    assert_ne!(result.is_error, Some(true));
    let value = body(&result);
    assert_eq!(value["source"]["state"], "partial");
    assert!(value["stopped"].is_object());
    assert_eq!(value["evidence"]["observations"], json!([]));
    host.assert_source_free();
}
