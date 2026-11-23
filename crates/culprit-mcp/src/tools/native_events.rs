//! Supplied XML is data: no file path, saved database, source read or warehouse authority is acquired.
use super::*;
use culprit_core::{NativeLogFormat, NativeLogLimits, limits::OperationHandle, telemetry::DeliveryProblem};

pub(crate) const NATIVE_EVENTS_TOOL: &str = "analyze_event_xml";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EventXmlArgs {
    // XML exported from SQL Server Extended Events, supplied as text rather than a host filesystem path.
    xml: String,
}

pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            NATIVE_EVENTS_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("native_events.mcp.description")),
            schema_for_type::<EventXmlArgs>(),
        )
        .with_title(culprit_core::t!("native_events.mcp.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}

impl CulpritMcp {
    pub(super) async fn analyze_event_xml(&self, value: Value, operation: Option<OperationHandle>) -> Result<Value> {
        let input: EventXmlArgs = args(value)?;
        let limits = NativeLogLimits::default();
        if input.xml.len() > limits.input_bytes {
            return Err(DeliveryProblem::Capacity.error());
        }
        let operation = operation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check_local_operation(&operation)?;
        let after = operation.clone();
        let language = culprit_core::i18n::current();
        let analysis = culprit_engine::run_blocking(move || {
            culprit_core::i18n::scope(&language.code, || {
                culprit_engine::native_logs::analyze_bytes_with(
                    input.xml.as_bytes(),
                    NativeLogFormat::MssqlExtendedEventsXml,
                    limits,
                    &operation,
                )
            })
        })
        .await?;
        check_local_operation(&after)?;
        if !super::response::admits(&analysis)? {
            return Err(DeliveryProblem::Capacity.error());
        }
        let value = serde_json::to_value(analysis).map_err(|_| DeliveryProblem::Integrity.error())?;
        let value = super::response::bounded_tool_result(value)?;
        check_local_operation(&after)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests;
