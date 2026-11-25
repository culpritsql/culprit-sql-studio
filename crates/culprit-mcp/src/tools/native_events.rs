//! Supplied XML and base64 XEL payloads are data: no file path, saved database, source read or warehouse authority is acquired.
use super::*;
use culprit_core::{NativeLogFormat, NativeLogLimits, limits::OperationHandle, telemetry::DeliveryProblem};

pub(crate) const NATIVE_EVENTS_TOOL: &str = "analyze_event_xml";
pub(crate) const NATIVE_EVENTS_XEL_TOOL: &str = "analyze_event_xel";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EventXmlArgs {
    // XML exported from SQL Server Extended Events, supplied as text rather than a host filesystem path.
    xml: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EventXelArgs {
    // Base64-encoded binary SQL Server Extended Events (.xel) payload, supplied as data rather than a host path.
    xel_base64: String,
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

pub(super) fn xel_definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            NATIVE_EVENTS_XEL_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("native_events.mcp_xel.description")),
            schema_for_type::<EventXelArgs>(),
        )
        .with_title(culprit_core::t!("native_events.mcp_xel.title"))
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
        self.run_event_analysis(input.xml.into_bytes(), NativeLogFormat::MssqlExtendedEventsXml, limits, operation)
            .await
    }

    pub(super) async fn analyze_event_xel(&self, value: Value, operation: Option<OperationHandle>) -> Result<Value> {
        let input: EventXelArgs = args(value)?;
        let limits = NativeLogLimits::default();
        let max_encoded = (limits.input_bytes / 3 + 2) * 4;
        if input.xel_base64.len() > max_encoded {
            return Err(DeliveryProblem::Capacity.error());
        }
        let decoded = decode_base64(input.xel_base64.trim())?;
        if decoded.len() > limits.input_bytes {
            return Err(DeliveryProblem::Capacity.error());
        }
        self.run_event_analysis(decoded, NativeLogFormat::MssqlExtendedEventsXel, limits, operation).await
    }

    async fn run_event_analysis(
        &self,
        bytes: Vec<u8>,
        format: NativeLogFormat,
        limits: NativeLogLimits,
        operation: Option<OperationHandle>,
    ) -> Result<Value> {
        let operation = operation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check_local_operation(&operation)?;
        let after = operation.clone();
        let language = culprit_core::i18n::current();
        let analysis = culprit_engine::run_blocking(move || {
            culprit_core::i18n::scope(&language.code, || {
                culprit_engine::native_logs::analyze_bytes_with(&bytes, format, limits, &operation)
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

fn decode_base64(text: &str) -> Result<Vec<u8>> {
    let filtered: Vec<u8> = text.bytes().filter(|byte| !byte.is_ascii_whitespace()).collect();
    if filtered.is_empty() || !filtered.len().is_multiple_of(4) {
        return Err(Error::invalid("Invalid arguments: malformed base64 payload"));
    }
    let val = |byte: u8| -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut out = Vec::with_capacity(filtered.len() / 4 * 3);
    let (chunks, _) = filtered.as_chunks::<4>();
    let last_index = chunks.len() - 1;
    for (idx, chunk) in chunks.iter().enumerate() {
        let pad = chunk.iter().rev().take_while(|&&b| b == b'=').count();
        if pad > 2 || (pad > 0 && idx != last_index) || chunk[..4 - pad].contains(&b'=') {
            return Err(Error::invalid("Invalid arguments: malformed base64 padding"));
        }
        let a = val(chunk[0]).ok_or_else(|| Error::invalid("Invalid arguments: invalid base64 character"))?;
        let b = val(chunk[1]).ok_or_else(|| Error::invalid("Invalid arguments: invalid base64 character"))?;
        out.push((a << 2) | (b >> 4));
        if pad < 2 {
            let c = val(chunk[2]).ok_or_else(|| Error::invalid("Invalid arguments: invalid base64 character"))?;
            out.push((b << 4) | (c >> 2));
            if pad == 0 {
                let d = val(chunk[3]).ok_or_else(|| Error::invalid("Invalid arguments: invalid base64 character"))?;
                out.push((c << 6) | d);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
