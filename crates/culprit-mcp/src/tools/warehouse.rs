//! Common source-free argument/privacy and cancellation boundaries; the evidence owner validates all selectors.
use super::*;
use crate::WarehouseDelegation;
use culprit_core::telemetry::CollectionStop;
use culprit_read::cancellation::CollectionCancellation;

impl CulpritMcp {
    pub(super) fn warehouse_delegation(&self, tool: &str) -> Result<&WarehouseDelegation> {
        let delegation = self.warehouse.as_deref().ok_or_else(|| unknown_tool(tool))?;
        delegation.check()?;
        Ok(delegation)
    }
}
pub(super) fn arguments<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    // Invalid input can contain arbitrary private text. Do not echo serde's raw offending value into errors/audit.
    serde_json::from_value(value).map_err(|_| Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")))
}
pub(super) fn check(cancellation: &CollectionCancellation) -> Result<()> {
    if cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
}
// These are input schemas only; deserialization/validation uses the one canonical Core selector/cursor/window.
pub(super) fn resource_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"type":"object","additionalProperties":false,
        "required":["namespace_id","database"],"properties":{
            "namespace_id":{"type":"string","pattern":"^[a-f0-9]{64}$"},
            "database":{"type":"string","pattern":"^[a-f0-9]{64}$"}}})
}
pub(super) fn optional_window_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"anyOf":[{"type":"null"},{"type":"object","additionalProperties":false,
        "required":["from","until"],"properties":{
            "from":{"type":"string","format":"date-time"},
            "until":{"type":"string","format":"date-time"}}}]})
}
pub(super) fn optional_cursor_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"anyOf":[{"type":"null"},{"type":"object","additionalProperties":false,
        "required":["selection_key","position"],"properties":{
            "selection_key":{"type":"string","pattern":"^[a-f0-9]{64}$"},
            "position":{"type":"object","additionalProperties":false,
                "required":["resource_id","source_id","range_id"],"properties":{
                    "resource_id":{"type":"string","pattern":"^[a-f0-9]{64}$"},
                    "source_id":{"type":"string","pattern":"^[a-f0-9]{64}$"},
                    "range_id":{"type":"string","pattern":"^[a-f0-9]{64}$"}}}}}]})
}
