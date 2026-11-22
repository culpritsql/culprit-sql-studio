use super::contract::*;
use culprit_core::{Result, telemetry::DeliveryProblem};
pub fn coverage_key(range: &culprit_core::telemetry::CapturedRange) -> Result<Option<String>> {
    if !valid_digest(&range.scope_id) {
        return Err(DeliveryProblem::Integrity.error());
    }
    if matches!(range.range, culprit_core::telemetry::TelemetryRange::Current) {
        return Ok(None);
    }
    let canonical = range.range.canonical()?;
    let bytes = serde_json::to_vec(&(&range.scope_id, canonical)).map_err(|_| DeliveryProblem::Integrity.error())?;
    if bytes.len() > culprit_core::telemetry::MAX_CURSOR_BYTES {
        return Err(DeliveryProblem::Capacity.error());
    }
    Ok(Some(culprit_core::telemetry::digest(&bytes)))
}
