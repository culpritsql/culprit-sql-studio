//! Public worker facts retain opaque locators, not a second native source authority. The existing canonical
//! recovery reader restores and authenticates whole original rows; public keys never reconstruct a context.
use culprit_core::{Result, telemetry::*};
use serde_json::{Value, json};
pub(super) fn project(details: &mut Value, point: &MssqlWorkerStarvationPoint) -> Result<()> {
    if !point.is_valid() {
        return Err(invalid());
    }
    let execution = SourceReadExecutionScope::from_context(SourceReadQuestion::WorkerStarvation, &point.context)?;
    *details.get_mut("context").ok_or_else(invalid)? = json!({"key":execution.key()?,"observed_from":point.context.observed_from,"observed_until":point.context.observed_until});
    *details.get_mut("worker_scope").ok_or_else(invalid)? = json!({"key":point.worker_scope.telemetry_id()?});
    Ok(())
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_worker_details_retain_values_and_clocks_without_native_target_member_or_authority() {
        for shared in [false, true] {
            let (source, records) = crate::analytical::test_support::worker_starvation_evidence(shared);
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlWorkerStarvation(point),
                ..
            }) = &records[0]
            else {
                panic!()
            };
            let original = point.as_ref().clone();
            let mut prepared = json!({"record":records[0]});
            crate::analytical::prepared::prepare(&mut prepared, &records[0], &source.target).unwrap();
            let projected = prepared["_projection"]["details"].clone();
            assert!(projected["context"].get("target").is_none());
            assert!(projected["context"].get("partition").is_none());
            assert_eq!(projected["worker_scope"].as_object().unwrap().len(), 1);
            assert_eq!(projected["threadpool_waits"][0]["elapsed_ms"], "9007199254740993");
            assert_eq!(projected["context"]["observed_from"], original.context.observed_from);
            assert!(
                projected["requests"][0]["observation"]["statement"].get("text").is_none(),
                "privacy curation preserves existing payload preview preparation"
            );
            assert!(
                serde_json::from_value::<MssqlWorkerStarvationPoint>(projected).is_err(),
                "public locators are not native authority"
            );
            assert_eq!(point.as_ref(), &original);
            assert!(point.fits_source(&source));
        }
    }
}
