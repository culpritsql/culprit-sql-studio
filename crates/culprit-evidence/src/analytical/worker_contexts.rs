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
pub(super) fn project_execution_profile(input: &mut Value, point: &MssqlCurrentExecutionProfilePoint) -> Result<()> {
    if !point.is_valid() {
        return Err(invalid());
    }
    project_request(input, SourceReadQuestion::CurrentExecutionProfile, &point.context, &point.focus)
}
pub(super) fn project_recovery(input: &mut Value, point: &ReadableSecondaryRecoveryPoint) -> Result<()> {
    if !point.is_valid() {
        return Err(invalid());
    }
    project_request(input, SourceReadQuestion::ReadableSecondaryRecovery, &point.context, &point.focus)
}
fn project_request(
    input: &mut Value,
    question: SourceReadQuestion,
    context: &NativeReadContext,
    focus: &ActivityRequestFocus,
) -> Result<()> {
    let execution = SourceReadExecutionScope::from_context(question, context)?;
    withhold_focus_payloads(input)?;
    let details = input.pointer_mut("/record/data/fact/data").ok_or_else(invalid)?;
    *details.get_mut("context").ok_or_else(invalid)? =
        json!({"key":execution.key()?,"observed_from":context.observed_from,"observed_until":context.observed_until});
    // Public pointers retain one recovery association, not another private request/source authority.
    *details.get_mut("focus").ok_or_else(invalid)? =
        json!({"key":focus.key()?,"parent":focus.parent,"original_observed_at":focus.original_observed_at});
    Ok(())
}

/// Removing private request authority also removes its public payload locators. The original record and
/// its complete payload inventory remain intact for authenticated recovery; dangling locators are invalid.
fn withhold_focus_payloads(input: &mut Value) -> Result<()> {
    const PRIVATE_FOCUS: &str = "/record/data/fact/data/focus";
    let payloads = input.pointer_mut("/_projection/payloads").and_then(Value::as_array_mut).ok_or_else(invalid)?;
    let mut public = Vec::with_capacity(payloads.len());
    for payload in std::mem::take(payloads) {
        let pointer = payload.get("pointer").and_then(Value::as_str).ok_or_else(invalid)?;
        if pointer != PRIVATE_FOCUS
            && !pointer.strip_prefix(PRIVATE_FOCUS).is_some_and(|suffix| suffix.starts_with('/'))
        {
            public.push(payload);
        }
    }
    *payloads = public;
    Ok(())
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_recovery_context_keeps_values_without_copying_private_request_or_source_authority() {
        use crate::analytical::test_support::{ReadableSecondaryCase as Case, readable_secondary_evidence};
        for case in [Case::PostgresRelation, Case::MssqlRedo] {
            let (source, records) = readable_secondary_evidence(case);
            let original = serde_json::to_value(&records[0]).unwrap();
            let mut prepared = json!({"record":records[0]});
            crate::analytical::prepared::prepare(&mut prepared, &records[0], &source.target).unwrap();
            let details = &prepared["_projection"]["details"];
            assert!(details["context"].get("target").is_none());
            assert!(details["context"].get("partition").is_none());
            assert!(details["focus"].get("original").is_none());
            assert_eq!(details["focus"].as_object().unwrap().len(), 3);
            assert!(
                prepared["_projection"]["payloads"].as_array().unwrap().is_empty(),
                "the withheld private focus cannot leave a dangling public SQL locator"
            );
            assert!(serde_json::from_value::<ReadableSecondaryRecoveryPoint>(details.clone()).is_err());
            assert_eq!(
                serde_json::to_value(&records[0]).unwrap(),
                original,
                "curation does not mutate native receipts"
            );
        }
    }
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
