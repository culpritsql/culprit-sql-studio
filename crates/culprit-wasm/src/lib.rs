//! Pure operations over the existing models. JSON is the transport, never a second parser or DTO.
use culprit_core::{CompareContext, Engine, Error, NativeLogLimits, Plan, Result};
use serde::{Serialize, de::DeserializeOwned};
use wasm_bindgen::prelude::*;

/// The optional supplied engine selects the existing strict native parser; it is not source authority.
#[wasm_bindgen(js_name = parsePlanJson)]
pub fn parse_plan(input: &str, engine_json: &str) -> std::result::Result<String, String> {
    let result = match decode::<Option<Engine>>(engine_json) {
        Ok(Some(engine)) => culprit_plans::parse_native_plan(input, engine),
        Ok(None) => culprit_plans::parse_plan(input),
        Err(error) => Err(error),
    };
    encode(result)
}

#[wasm_bindgen(js_name = diffPlansJson)]
pub fn diff_plans(
    a_json: &str,
    name_a: &str,
    b_json: &str,
    name_b: &str,
    context_json: &str,
) -> std::result::Result<String, String> {
    let compare = || {
        let a: Plan = decode(a_json)?;
        let b: Plan = decode(b_json)?;
        let context: CompareContext = decode(context_json)?;
        Ok(culprit_plans::compare_plans(&a, name_a, &b, name_b, &context))
    };
    encode(compare())
}

#[wasm_bindgen(js_name = parseDeadlockJson)]
pub fn parse_deadlock(input: &str) -> std::result::Result<String, String> {
    encode(culprit_plans::parse_deadlocks(input))
}

/// Decode a binary SQL Server Extended Events (.xel) buffer within explicit or default bounds.
/// XML text is refused on this route so binary session-clock framing stays distinct from XML exports.
#[wasm_bindgen(js_name = parseXelJson)]
pub fn parse_xel(input: &[u8], limits_json: &str) -> std::result::Result<String, String> {
    let run = || {
        let limits = decode::<Option<NativeLogLimits>>(limits_json)?.unwrap_or_else(NativeLogLimits::portable_events);
        culprit_plans::parse_xel_events(input, limits)
    };
    encode(run())
}

/// Decode exported SQL Server Extended Events XML (`<events>`, `<RingBufferTarget>`, or `<event>`) within bounds.
/// Binary .xel files are refused on this route.
#[wasm_bindgen(js_name = parseExtendedEventsXmlJson)]
pub fn parse_extended_events_xml(input: &[u8], limits_json: &str) -> std::result::Result<String, String> {
    let run = || {
        let limits = decode::<Option<NativeLogLimits>>(limits_json)?.unwrap_or_else(NativeLogLimits::portable_events);
        culprit_plans::parse_extended_events_xml(input, limits)
    };
    encode(run())
}

#[wasm_bindgen(js_name = exportPlanJson)]
pub fn export_plan(
    plan_json: &str,
    title: &str,
    format_json: &str,
    options_json: &str,
    exported_at: &str,
) -> std::result::Result<String, String> {
    let export = || {
        let plan: Plan = decode(plan_json)?;
        let format = decode(format_json)?;
        let options = decode(options_json)?;
        culprit_plans::export::export_plan(&plan, title, format, &options, exported_at)
    };
    encode(export())
}

fn decode<T: DeserializeOwned>(input: &str) -> Result<T> {
    if input.len() > culprit_plans::MAX_PLAN_BYTES {
        return Err(Error::invalid(culprit_core::t!(
            "engine.plans.too_large",
            size = (input.len() / (1024 * 1024)).to_string()
        )));
    }
    serde_json::from_str(input).map_err(|error| Error::invalid(error.to_string()))
}

// Serde's existing Result representation preserves the complete Core Error, including code/hint/detail.
// A genuine transport-serialization failure is a bindgen exception, never a fabricated successful result.
fn encode<T: Serialize>(result: Result<T>) -> std::result::Result<String, String> {
    serde_json::to_string(&result).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
