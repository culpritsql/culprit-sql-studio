//! A real refused loopback acquisition keeps its admission charge and uses the ordinary control/archive owners.
use crate::{
    conn::Pool,
    read::invalid_data,
    test_support::{OwnedSourceFixture, owned_case::finish_case, receipt_directory, save_cost_receipt},
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpSocket, time::Instant};

mod archive;
mod capture;

const CASE_TIMEOUT: Duration = Duration::from_secs(30);
const ATTEMPTS: usize = 2;
const MAX_CASE_FUTURE_BYTES: usize = 32 * 1024;
const UNUSED_CREDENTIAL: &str = "owned-refused-before-login-credential-canary";

#[test]
fn refused_connection_concrete_case_future_is_bounded_before_any_native_or_filesystem_work() {
    let cancellation = CollectionCancellation::default();
    let future = run(Path::new("unopened-refused-loopback-fixture"), &cancellation);
    assert!(
        std::mem::size_of_val(&future) <= MAX_CASE_FUTURE_BYTES,
        "concrete refused-connection future is {} bytes",
        std::mem::size_of_val(&future)
    );
}

#[test]
fn refused_connection_concrete_capture_and_archive_futures_keep_their_actual_storage_bounded() {
    use culprit_collector::outbox::Outbox;
    type CaptureArguments = (&'static mut Outbox, &'static Path, Arc<Pool>, u16, &'static CollectionCancellation);
    type ArchiveArguments =
        (&'static mut archive::Archive, &'static capture::OriginalAttempt, &'static CollectionCancellation);
    // These constructors are never invoked; only the concrete, unerased return-future types are measured.
    let capture_bytes = concrete_future_bytes(|(outbox, root, pool, port, token): CaptureArguments| {
        capture::attempt(outbox, root, pool, port, token)
    });
    let archive_bytes = concrete_future_bytes(|(archive, original, token): ArchiveArguments| {
        archive.commit_and_verify(original, token)
    });
    assert!(capture_bytes <= MAX_CASE_FUTURE_BYTES, "concrete capture future is {capture_bytes} bytes");
    assert!(archive_bytes <= MAX_CASE_FUTURE_BYTES, "concrete archive future is {archive_bytes} bytes");
}

fn concrete_future_bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[tokio::test]
#[ignore = "explicit nonce-owned disposable MSSQL supervisor only; exclusively bound non-listening loopback socket, two actual failed source acquisitions, canonical local archive; no cost qualification or native-debt clearing"]
async fn owned_refused_loopback_preserves_real_admission_and_retires_only_the_sealed_empty_original() -> Result<()> {
    let directory = receipt_directory();
    let cancellation = CollectionCancellation::default();
    let started = Instant::now();
    let (expired, outcome) = finish_case(
        started + CASE_TIMEOUT,
        &cancellation,
        AssertUnwindSafe(Box::pin(run(&directory, &cancellation))).catch_unwind(),
    )
    .await;
    let saved = save_outcome(&directory, expired, &outcome, started.elapsed());
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    saved?;
    if expired {
        return Err(CollectionStop::TimedOut.error());
    }
    Ok(())
}

async fn run(directory: &Path, cancellation: &CollectionCancellation) -> Result<()> {
    // The existing native admission verifies this supervisor's complete nonce/database/member identity.
    // Its connection is independent of the refused endpoint and is never substituted for source authority.
    let fixture = boundary(
        "owned-fixture-admission",
        Box::pin(OwnedSourceFixture::open("culprit-refused-control-admission")).await,
    )?;
    let refused = TcpSocket::new_v4().map_err(|_| invalid_data())?;
    refused.bind("127.0.0.1:0".parse().map_err(|_| invalid_data())?).map_err(|_| invalid_data())?;
    let address = refused.local_addr().map_err(|_| invalid_data())?;
    if !address.ip().is_loopback() || address.port() == 0 || address.port() == fixture.port {
        return Err(invalid_data());
    }
    // Retain the bound socket without listen(): nobody else can claim this test's destination port.
    // A host that silently drops closed-port traffic cannot supply the positive refusal proof.
    let pool = Arc::new(Pool::new(
        crate::test_support::config(address.port(), &fixture.database, UNUSED_CREDENTIAL, "culprit-refused-control"),
        None,
    ));
    let root = directory.join("refused-loopback-control");
    std::fs::create_dir(&root).map_err(|_| invalid_data())?;
    let mut archive = boundary("archive-initialization", Box::pin(archive::Archive::create(&root)).await)?;
    let mut receipts = Vec::with_capacity(ATTEMPTS);
    for _ in 0..ATTEMPTS {
        crate::test_support::owned_case::require_active(cancellation)?;
        let original = boundary(
            "source-terminal-facts",
            Box::pin(capture::attempt(&mut archive.outbox, &root, pool.clone(), address.port(), cancellation)).await,
        )?;
        let archived = boundary(
            "archive-and-canonical-readback",
            Box::pin(archive.commit_and_verify(&original, cancellation)).await,
        )?;
        receipts.push(archived);
        if pool.source_shutdown() != SourceShutdown::Quiescent {
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        let status = archive.outbox.status(2, None).await?;
        if status.captures != 0 || status.source_debts != 0 || status.connection_controls_pending != 0 {
            return Err(invalid_data());
        }
    }
    if receipts[0].original.capture == receipts[1].original.capture || refused.local_addr().ok() != Some(address) {
        return Err(invalid_data());
    }
    save_originals(directory, &receipts, fixture.admission_elapsed_ms)?;
    Ok(())
}

fn boundary<T>(stage: &str, result: Result<T>) -> Result<T> {
    result.inspect_err(|error| {
        eprintln!(
            "owned refused-connection boundary: {}",
            serde_json::json!({"stage":stage,"code":error.code,"stop":CollectionStop::from_error(error)})
        );
    })
}

fn save_originals(directory: &Path, receipts: &[archive::ArchivedAttempt], admission_elapsed_ms: f64) -> Result<()> {
    save_cost_receipt(
        directory,
        "refused-loopback-control-native",
        &serde_json::json!({
            "attempts":receipts,"maximum_outbox_captures":2,"attempt_count":ATTEMPTS,
            "native_fixture_admission_elapsed_ms":admission_elapsed_ms,
            "endpoint":"exclusively bound non-listening IPv4 loopback socket",
            "source_admission_charges_per_attempt":1,"executed_source_templates_per_attempt":0,
            "canonical_eof_verified":true,"safe_empty_originals_retired_after_archive":true,
            "native_shutdown":"quiescent","source_cpu":null,"cost_qualified":false,
            "failed_native_login_is_covered_separately_and_remains_unconfirmed":true
        }),
    )
}

fn save_outcome(
    directory: &Path,
    expired: bool,
    outcome: &std::thread::Result<Result<()>>,
    elapsed: Duration,
) -> Result<()> {
    save_cost_receipt(
        directory,
        "refused-loopback-control-outcome",
        &serde_json::json!({
            "completed":!expired&&matches!(outcome,Ok(Ok(()))),"expired":expired,
            "fixture_elapsed_ms":elapsed.as_secs_f64()*1000.0,
            "cost_qualified":false,"production_budgets_and_native_fences_unchanged":true
        }),
    )
}
