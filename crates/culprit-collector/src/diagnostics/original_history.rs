//! Reconcile historical native groups before a new original observation can enroll a follow-up.
use super::*;
use crate::delivery::diagnostic_recovery::{MAX_ORIGINAL_HISTORY_LOOKUP_GROUPS, authenticate_original_capture};

pub(super) async fn reconcile(
    outbox: &mut Outbox,
    input: Box<DiagnosticInput>,
    recovery: Option<&dyn RecoveryStore>,
    cancellation: &CollectionCancellation,
) -> Result<Option<Box<DiagnosticInput>>> {
    if diagnostic_original_source_kind(&input.source).is_none() || input.original_history.is_some() {
        return Ok(Some(input));
    }
    let operation = Box::pin(reconcile_originals(outbox, &input, recovery));
    read_recovery(operation, cancellation).await.map_err(deferred)
}

async fn reconcile_originals(
    outbox: &mut Outbox,
    input: &DiagnosticInput,
    recovery: Option<&dyn RecoveryStore>,
) -> Result<Box<DiagnosticInput>> {
    let rows = input
        .rows
        .iter()
        .map(|row| DiagnosticOriginalRow { reference: &row.reference, record: &row.row.record })
        .collect::<Vec<_>>();
    let keys = DiagnosticOriginalEvidence::new(&input.source, &input.parent, &rows)?.group_keys()?;
    if keys.len() > MAX_ORIGINAL_HISTORY_LOOKUP_GROUPS {
        return Err(DeliveryProblem::Capacity.error());
    }
    let witness = outbox.original_history_witness().await?;
    let mut reconciled = outbox.local_original_groups(&keys).await?;
    let missing =
        keys.iter().filter(|key| reconciled.get(*key).is_some_and(Option::is_none)).cloned().collect::<Vec<_>>();
    if !witness.is_complete() && !missing.is_empty() {
        let store = recovery.ok_or_else(|| DeliveryProblem::NotReady.error())?;
        if outbox.recovery_store_id() != Some(store.id()) {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let lookup = store.diagnostic_original_groups(&missing).await?;
        lookup.validate_for(&missing)?;
        for pointers in lookup.captures()? {
            let evidence = Box::pin(authenticate_original_capture(store, &pointers)).await?;
            // Every pointer in this capture was checked against the same complete immutable body. Retain its
            // original once; cache admission does not add or acknowledge a diagnostic input obligation.
            for pointer in pointers {
                reconciled.insert(pointer.group_key.clone(), Some(pointer.reference.clone()));
            }
            outbox.cache_diagnostic_evidence(&input.connection, *evidence).await?;
        }
    }
    outbox.complete_original_history(&input.parent.capture.observation_id, reconciled, witness).await
}

fn deferred(error: Error) -> Error {
    let stop = CollectionStop::from_error(&error);
    if matches!(stop, CollectionStop::Cancelled | CollectionStop::TimedOut) {
        return error;
    }
    Error::new(error.code, culprit_core::t!("diagnostic.history.reconciliation_deferred"))
        .with_detail(error.detail.unwrap_or(ErrorDetail::CollectionStopped(CollectionStop::SourceUnavailable)))
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use std::{future::Future, mem::size_of};

    const MAX_RECONCILIATION_FUTURE_BYTES: usize = 48 * 1024;
    const MAX_CALLER_FUTURE_BYTES: usize = 4096;
    const MAX_OUTPUT_BYTES: usize = 512;
    type Args = (&'static mut Outbox, &'static DiagnosticInput, Option<&'static dyn RecoveryStore>);
    type CallerArgs = (
        &'static mut Outbox,
        Box<DiagnosticInput>,
        Option<&'static dyn RecoveryStore>,
        &'static CollectionCancellation,
    );

    fn storage<Args, F, Fut>(_: F) -> (usize, usize)
    where
        F: FnOnce(Args) -> Fut,
        Fut: Future,
    {
        (size_of::<Fut>(), size_of::<Fut::Output>())
    }

    #[test]
    fn actual_reconciliation_future_and_output_types_bound_the_unpolled_construction_storage() {
        // These are the actual concrete future types and outputs, not Pin<Box<_>> or dyn handles.
        // No reference is fabricated and neither constructor closure is invoked or future polled.
        let inner = storage(|args: Args| reconcile_originals(args.0, args.1, args.2));
        let caller = storage(|args: CallerArgs| reconcile(args.0, args.1, args.2, args.3));
        eprintln!(
            "Original history storage: inner={}B caller={}B outputs={}B/{}B body={}B",
            inner.0,
            caller.0,
            inner.1,
            caller.1,
            size_of::<DiagnosticInput>()
        );
        assert!(inner.0 <= MAX_RECONCILIATION_FUTURE_BYTES, "actual reconciliation construction: {}", inner.0);
        assert!(caller.0 <= MAX_CALLER_FUTURE_BYTES, "actual owned caller construction: {}", caller.0);
        assert_eq!(inner.1, size_of::<Result<Box<DiagnosticInput>>>());
        assert_eq!(caller.1, size_of::<Result<Option<Box<DiagnosticInput>>>>());
        assert!(inner.1 <= MAX_OUTPUT_BYTES);
        assert!(caller.1 <= MAX_OUTPUT_BYTES);
        assert!(size_of::<DiagnosticInput>() > MAX_OUTPUT_BYTES, "the guard must cover the actual large body");
    }
}
