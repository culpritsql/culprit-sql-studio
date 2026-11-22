//! The host installs its pure analyzer once and keeps local analysis alive independently of source access.
use super::*;
use culprit_collector::{
    outbox::{CompletedCaptureBudget, OutboxDelivery},
    synthesis::{AutomaticSynthesis, SynthesisTurn},
};
use culprit_core::limits::OperationGuard;
use std::time::Duration;

// These are cumulative local file/CPU allowances. They never authorize or widen a database read.
const ANALYSIS_BUDGET: CompletedCaptureBudget =
    CompletedCaptureBudget { max_rows: 100_000, max_bytes: 64 * 1024 * 1024, timeout: Duration::from_secs(30) };
const IDLE_INTERVAL: Duration = Duration::from_secs(1);

pub(super) fn configured(runtime: &CollectorRuntime, workspace: &Workspace) -> Result<AutomaticSynthesis> {
    AutomaticSynthesis::new(runtime.synthesizer(workspace)?, ANALYSIS_BUDGET)
}

pub(super) async fn install(
    outbox: &mut Outbox,
    automatic: &AutomaticSynthesis,
    cancellation: &CollectionCancellation,
) -> Result<()> {
    let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout)?;
    let registration = outbox.register_automatic_synthesis(automatic, guard.handle(), ());
    match settle(cancellation, guard, registration).await {
        Err(error) if cancelled(&error, cancellation) => Ok(()),
        result => result,
    }
}

pub(super) async fn run(
    delivery: OutboxDelivery,
    automatic: &AutomaticSynthesis,
    cancellation: &CollectionCancellation,
    health: &CollectorHealth,
    reporter: &dyn ServiceReporter,
) -> Result<()> {
    while !cancellation.is_cancelled() {
        let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout)?;
        let work = delivery.synthesize_pending(automatic, guard.handle(), ());
        let turn = match settle(cancellation, guard, work).await {
            Err(error) if cancelled(&error, cancellation) => return Ok(()),
            Err(error) => budget_deferral(error)?,
            Ok(turn) => turn,
        };
        health.synthesis(&turn)?;
        reporter.synthesis(&turn)?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout)?;
        let backlog = delivery.synthesis_backlog(guard.handle(), ());
        let backlog = match settle(cancellation, guard, backlog).await {
            Err(error) if cancelled(&error, cancellation) => return Ok(()),
            Err(error) => {
                let deferred = budget_deferral(error)?;
                health.synthesis(&deferred)?;
                reporter.synthesis(&deferred)?;
                pause(cancellation).await;
                continue;
            }
            Ok(backlog) => backlog,
        };
        health.synthesis_backlog(&backlog)?;
        if matches!(turn, SynthesisTurn::Committed { queue_retired: true, .. }) {
            tokio::task::yield_now().await;
        } else {
            pause(cancellation).await;
        }
    }
    Ok(())
}

fn budget_deferral(error: culprit_core::Error) -> Result<SynthesisTurn> {
    if matches!(
        CollectionStop::from_error(&error),
        CollectionStop::TimedOut | CollectionStop::ByteLimit | CollectionStop::RowLimit
    ) {
        return Ok(SynthesisTurn::Deferred { reason: culprit_collector::synthesis::SynthesisDeferral::LocalBudget });
    }
    Err(error)
}
async fn pause(cancellation: &CollectionCancellation) {
    tokio::select! {
        biased;
        _ = cancellation.cancelled()=>{},
        _ = tokio::time::sleep(IDLE_INTERVAL)=>{},
    }
}

/// Cancellation signals the existing operation owner and then awaits it. A known commit remains a result.
async fn settle<T>(
    cancellation: &CollectionCancellation,
    guard: OperationGuard,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if cancellation.is_cancelled() {
        return Err(CollectionStop::Cancelled.error());
    }
    tokio::pin!(work);
    tokio::select! {
        biased;
        result = &mut work => result,
        _ = cancellation.cancelled() => {
            drop(guard);
            work.await
        },
    }
}

fn cancelled(error: &culprit_core::Error, cancellation: &CollectionCancellation) -> bool {
    cancellation.is_cancelled() && CollectionStop::from_error(error) == CollectionStop::Cancelled
}

#[cfg(test)]
mod tests;
