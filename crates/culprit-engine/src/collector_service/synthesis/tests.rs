use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

#[tokio::test]
async fn cancellation_before_local_admission_never_polls_the_worker() {
    let cancellation = CollectionCancellation::default();
    cancellation.cancel();
    let polled = AtomicBool::new(false);
    let work = async {
        polled.store(true, Ordering::Release);
        Ok(())
    };
    let error = settle(&cancellation, OperationGuard::new(ANALYSIS_BUDGET.timeout).unwrap(), work).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    assert!(!polled.load(Ordering::Acquire));
}

#[tokio::test]
async fn cancellation_awaits_owned_local_work_and_preserves_its_known_commit() {
    let cancellation = CollectionCancellation::default();
    let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout).unwrap();
    let operation = guard.handle();
    let entered = Notify::new();
    let settled = AtomicBool::new(false);
    let work = async {
        entered.notify_one();
        cancellation.cancelled().await;
        tokio::task::yield_now().await;
        assert!(operation.is_cancelled());
        settled.store(true, Ordering::Release);
        Ok("already committed")
    };
    let stop = async {
        entered.notified().await;
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(settle(&cancellation, guard, work), stop);
    assert_eq!(result.unwrap(), "already committed");
    assert!(settled.load(Ordering::Acquire));
}

#[tokio::test]
async fn cancelled_reads_finish_retiring_before_the_analysis_lifetime_returns() {
    let cancellation = CollectionCancellation::default();
    let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout).unwrap();
    let operation = guard.handle();
    let entered = Notify::new();
    let retired = AtomicBool::new(false);
    let work = async {
        entered.notify_one();
        cancellation.cancelled().await;
        tokio::task::yield_now().await;
        assert!(operation.is_cancelled());
        retired.store(true, Ordering::Release);
        Err::<(), _>(CollectionStop::Cancelled.error())
    };
    let stop = async {
        entered.notified().await;
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(settle(&cancellation, guard, work), stop);
    assert!(cancelled(&result.unwrap_err(), &cancellation));
    assert!(retired.load(Ordering::Acquire));
}

#[tokio::test]
async fn normal_host_installs_pure_analysis_without_source_routes_or_credentials_and_retains_its_policy() {
    let directory = tempfile::tempdir().unwrap();
    let workspace_path = directory.path().join("no-workspace-files");
    let runtime = CollectorRuntime::empty();
    let workspace = runtime.workspace(&workspace_path);
    let automatic = configured(&runtime, &workspace).unwrap();
    let path = directory.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    outbox.recover().await.unwrap();
    install(&mut outbox, &automatic, &CollectionCancellation::default()).await.unwrap();
    drop(outbox);
    let mut reopened = Outbox::open(&path).unwrap();
    reopened.recover().await.unwrap();
    install(&mut reopened, &automatic, &CollectionCancellation::default()).await.unwrap();
    let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout).unwrap();
    assert!(matches!(
        reopened.delivery().synthesize_pending(&automatic, guard.handle(), ()).await.unwrap(),
        SynthesisTurn::Idle
    ));
    assert!(!workspace_path.exists(), "pure installation must not discover credentials or saved source routes");
}

#[tokio::test]
async fn bounded_local_failure_defers_analysis_without_cancelling_the_running_service_owner() {
    let cancellation = CollectionCancellation::default();
    let deferred = Notify::new();
    let source = async {
        deferred.notified().await;
        assert!(!cancellation.is_cancelled());
        cancellation.cancel();
        Ok(17)
    };
    let analysis = async {
        let guard = OperationGuard::new(ANALYSIS_BUDGET.timeout).unwrap();
        let error =
            settle(&cancellation, guard, async { Err::<(), _>(CollectionStop::ByteLimit.error()) }).await.unwrap_err();
        assert!(matches!(
            budget_deferral(error).unwrap(),
            SynthesisTurn::Deferred { reason: culprit_collector::synthesis::SynthesisDeferral::LocalBudget }
        ));
        deferred.notify_one();
        cancellation.cancelled().await;
        Ok(())
    };
    assert_eq!(with_background(&cancellation, source, analysis).await.unwrap(), 17);
}

#[test]
fn local_budget_deferral_does_not_hide_storage_fencing_or_cancellation() {
    for reason in [CollectionStop::TimedOut, CollectionStop::ByteLimit, CollectionStop::RowLimit] {
        assert!(matches!(budget_deferral(reason.error()).unwrap(), SynthesisTurn::Deferred { .. }));
    }
    for error in [DeliveryProblem::Fenced.error(), DeliveryProblem::Storage.error(), CollectionStop::Cancelled.error()]
    {
        let retained = budget_deferral(error.clone()).unwrap_err();
        assert_eq!(retained.code, error.code);
        assert_eq!(retained.detail, error.detail);
        assert_eq!(retained.message, error.message);
        assert_eq!(retained.hint, error.hint);
    }
}
