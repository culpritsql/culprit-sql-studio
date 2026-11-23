//! Shared saved-evidence response admission. This bounds presentation, independently of original-read limits.
use culprit_core::{
    Result,
    serialization::{JsonSizeError, json_size},
    telemetry::DeliveryProblem,
};
use serde::Serialize;
use serde_json::Value;
pub(super) const MAX_OUTPUT_BYTES: usize = 64 * 1024;
pub(super) fn admits(value: &impl Serialize) -> Result<bool> {
    match json_size(value, MAX_OUTPUT_BYTES) {
        Ok(_) => Ok(true),
        Err(JsonSizeError::LimitExceeded) => Ok(false),
        Err(JsonSizeError::Serialization) => Err(DeliveryProblem::Integrity.error()),
    }
}
pub(super) fn bounded(value: Value) -> Result<Value> {
    if admits(&value)? { Ok(value) } else { Err(DeliveryProblem::Capacity.error()) }
}

/// Admit the actual tool-result envelope, including the JSON string's second escaping and metadata.
pub(super) fn bounded_tool_result(value: Value) -> Result<Value> {
    use rmcp::model::{CallToolResult, ContentBlock};
    let result = CallToolResult::success(vec![ContentBlock::text(value.to_string())]);
    if admits(&result)? { Ok(value) } else { Err(DeliveryProblem::Capacity.error()) }
}
