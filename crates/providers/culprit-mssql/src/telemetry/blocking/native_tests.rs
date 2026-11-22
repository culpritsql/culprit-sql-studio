//! Correctness prerequisite only. The official driver's missing cross-message ACK still blocks auto admission.
use super::*;
use crate::{
    activity::page_targets::native_tests::{
        fixture::{self, Fixture},
        workload::WRITER_APPLICATIONS,
    },
    test_support::{
        owned_captures::{Captured, OwnedCaptures},
        owned_case, receipt_directory, save_cost_receipt,
    },
};
use culprit_core::{Error, Result};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

// Existing nonce fixture: admin+seed SET2, native ownership1, DB setup1, seed4, sixteen actors SET/command32,
// cleanup1, gate SET/start/release3. These actor/setup dispatches never enter a production source allowance.
const FIXTURE_DISPATCH_CAP: u32 = 44;
const SOURCE_CAPTURE_CAP: u32 = 3;
const SOURCE_TOTAL_ALLOWANCE: u32 = 8 + MAX_BLOCKING_QUERIES * 2;
const CASE_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "requires explicit nonce-owned SQL16 edition3 loopback fixture and fresh private receipt directory; manual source correctness, no built-in admission or native-cost claim"]
async fn the_same_owned_activity_original_drives_a_native_focused_lock_closure_and_survives_actor_retirement() {
    let cancellation = CollectionCancellation::default();
    let body_cancellation = cancellation.clone();
    let body = Box::pin(fixture::run(move |fixture| {
        let cancellation = body_cancellation.clone();
        Box::pin(async move { Box::pin(prove(fixture, &cancellation)).await })
    }));
    let (expired, outcome) = owned_case::finish_case(
        tokio::time::Instant::now() + CASE_DEADLINE,
        &cancellation,
        AssertUnwindSafe(body).catch_unwind(),
    )
    .await;
    let saved = save_case_outcome(expired, outcome.is_ok(), cancellation.is_cancelled());
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    assert!(!expired, "late settled work cannot satisfy the native prerequisite");
    saved.expect("private native prerequisite outcome receipt");
}

async fn prove(fixture: &mut Fixture, cancellation: &CollectionCancellation) -> Result<()> {
    owned_case::require_active(cancellation)?;
    let mut captures = OwnedCaptures::from_pool(fixture.pool.as_ref().ok_or_else(invalid)?.clone(), fixture.port)?;
    fixture.start_holder_gate().await?;
    owned_case::require_active(cancellation)?;
    let work = fixture.start_holder_workload().await?;
    owned_case::require_active(cancellation)?;
    let original = Box::new(captures.capture_with_cancellation(CollectionSelection::Activity, cancellation).await?);
    let focus = select_original(&original)?;
    captures.verify_retained(&original).await?;
    let current = Box::new(
        captures
            .capture_with_cancellation(
                CollectionSelection::FocusedBlocking { focus: Box::new(focus.clone()) },
                cancellation,
            )
            .await?,
    );
    let SourceObservation::FocusedBlocking { result, .. } = &current.source()?.observation else {
        return Err(invalid());
    };
    if result.binding != FocusedRequestBinding::Matched
        || result.membership != FocusedRequestMembership::Observed
        || result.request_end != BlockingRequestState::SameStatement
        || result.closure.status != BlockingClosureStatus::Observed
    {
        return Err(invalid());
    }
    let report = current.with_activity_frame(|frame| {
        if !result.has_complete_owner_context(frame) {
            return Err(invalid());
        }
        let rows = frame.rows().collect::<Vec<_>>();
        culprit_checks::focused_blocking::explain(current.source()?, &rows, true)
    })?;
    if report.relationships.is_empty() || report.relationships.iter().any(|edge| !edge.owner_observed) {
        return Err(invalid());
    }
    // Same retained fixture owner releases its seed lock and joins the original actors; no replacement workload.
    fixture.release_holder_gate().await?;
    let completed = fixture.finish_workload().await?;
    if completed.len() != 16 || completed.iter().any(|worker| worker.inserts == 0 || worker.inserts > 2048) {
        return Err(invalid());
    }
    owned_case::require_active(cancellation)?;
    let ended = Box::new(
        captures
            .capture_with_cancellation(
                CollectionSelection::FocusedBlocking { focus: Box::new(focus.clone()) },
                cancellation,
            )
            .await?,
    );
    let SourceObservation::FocusedBlocking { result: last, .. } = &ended.source()?.observation else {
        return Err(invalid());
    };
    if !matches!(last.binding, FocusedRequestBinding::ParticipantMissing | FocusedRequestBinding::ParticipantChanged)
        || last.membership != FocusedRequestMembership::NotObserved
        || !ended.records.is_empty()
    {
        return Err(invalid());
    }
    let mut reader_queries = 0;
    for captured in [&original, &current, &ended] {
        captures.verify_retained(captured).await?;
        if captured.manifest.status != CollectionStatus::Complete {
            return Err(invalid());
        }
        reader_queries += captured.manifest.source_queries;
        let metrics = captured.manifest.read_metrics.as_ref().ok_or_else(invalid)?;
        if metrics.has_unconfirmed_work() || metrics.active_tasks != 0 || metrics.untracked_tasks != 0 {
            return Err(invalid());
        }
    }
    if original.manifest.source_queries > 7
        || current.manifest.source_queries > 9
        || ended.manifest.source_queries > 7
        || reader_queries > SOURCE_TOTAL_ALLOWANCE
    {
        return Err(invalid());
    }
    save_prerequisite_receipt(&original, &current, &ended, &focus, &report, reader_queries, work.elapsed())
}

fn save_prerequisite_receipt(
    original: &Captured,
    current: &Captured,
    ended: &Captured,
    focus: &ActivityRequestFocus,
    report: &culprit_core::BlockingReport,
    reader_queries: u32,
    elapsed: Duration,
) -> Result<()> {
    save_cost_receipt(
        &receipt_directory(),
        "mssql-manual-focused-blocking-prerequisite",
        &serde_json::json!({
            "original_manifest":original.manifest,"original_focus":focus,"focused_manifest":current.manifest,
            "ended_manifest":ended.manifest,"report":report,"originals_reverified":true,
            "source_capture_limit":SOURCE_CAPTURE_CAP,"reader_queries":reader_queries,
            "source_total_allowance_including_possible_SET":SOURCE_TOTAL_ALLOWANCE,
            "fixture_dispatch_cap_including_all_setup_and_cleanup":FIXTURE_DISPATCH_CAP,
            "combined_dispatch_cap":SOURCE_TOTAL_ALLOWANCE+FIXTURE_DISPATCH_CAP,
            "fixture_work_client_ms":elapsed.as_secs_f64()*1000.0,
            "fixture_actor_bound":"same existing16actors,4s work window,5s lock timeout,2048 inserts each maximum",
            "native_profile":"boxed SQL16 edition3 writable non-replica original local member",
            "admission":"unqualified","native_cpu_io_cost_qualified":false,"automatic_enrollment_proved":false,
            "active_cancellation_proved":false,"incident_recovery_proved":false,
            "limitations":"current native lock-owner session context; no exact owning request, lifetime continuity or other-version/Azure/MI/replica acceptance",
        }),
    )
}

fn save_case_outcome(expired: bool, settled: bool, cancelled: bool) -> Result<()> {
    save_cost_receipt(
        &receipt_directory(),
        "mssql-manual-focused-blocking-case-outcome",
        &serde_json::json!({
            "case_deadline_exceeded":expired,"body_and_existing_fixture_cleanup_completed":settled,
            "cancellation_requested":cancelled,"automatic_admission":false,"measured_native_cost":false,
            "failed_cleanup":"existing fixture logs retain its independent outcome; a panic never establishes settled cleanup",
        }),
    )
}
fn select_original(capture: &Captured) -> Result<ActivityRequestFocus> {
    let source = capture.source()?;
    let native = source.observation.activity().ok_or_else(invalid)?;
    let context = capture.activity_context()?;
    if !native.coverage.complete_scope
        || !context.has_qualified_local_owner()
        || context.database_writable != Some(true)
    {
        return Err(invalid());
    }
    for (ordinal, row) in capture.records.iter().enumerate() {
        let TelemetryRecord::Activity(original) = &row.record else { return Err(invalid()) };
        if original.waiting_on_lock != Some(true)
            || original.scope != ActivityScopeBasis::RequestDatabase
            || !original
                .client
                .as_ref()
                .and_then(|client| client.application.as_deref())
                .is_some_and(|name| WRITER_APPLICATIONS.contains(&name))
        {
            continue;
        }
        let focus = ActivityRequestFocus {
            parent: capture.reference(ordinal)?,
            target: source.target.clone(),
            partition: source.partition.clone(),
            database_writable: context.database_writable,
            source: source.source.clone(),
            epoch: native.epoch.clone().ok_or_else(invalid)?,
            original_observed_at: native.at.clone(),
            statement_identity: original.statement_identity.clone().ok_or_else(invalid)?,
            original: original.as_ref().clone(),
        };
        if focus.is_blocking_focus() {
            return Ok(focus);
        }
    }
    Err(invalid())
}
fn invalid() -> Error {
    CollectionStop::SourceUnavailable.error()
}

#[test]
fn actual_owned_blocking_prerequisite_body_has_bounded_inline_storage() {
    fn storage<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let bytes = storage(|(fixture, cancellation): (&'static mut Fixture, &'static CollectionCancellation)| {
        prove(fixture, cancellation)
    });
    assert!(bytes <= 64 * 1024, "actual owned native prerequisite body storage: {bytes}");
}
