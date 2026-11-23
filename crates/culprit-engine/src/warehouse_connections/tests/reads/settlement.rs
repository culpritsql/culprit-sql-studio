//! The existing saved-route wrapper and actual session CPU owner share the caller's one real read permit.
use super::*;
use crate::evidence::EvidenceReadPermit;
use culprit_evidence::analytical::evidence::EvidenceReadBudget;

#[tokio::test]
async fn cancelled_saved_archive_read_settles_admitted_cpu_before_a_replacement_can_enter() {
    let dir = tempfile::tempdir().unwrap();
    let (ws, saved, store, configured) = configured(dir.path()).await;
    let ws = Arc::new(ws);
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = EvidenceReadPermit::from(gate.clone().acquire_owned().await.unwrap());
    let cancellation = CollectionCancellation::default();
    let first_cancellation = cancellation.clone();
    let reading_cancellation = cancellation.clone();
    let first = ws.clone();
    let selected = saved.reference.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let settled = Arc::new(AtomicBool::new(false));
    let worker_settled = settled.clone();
    let original = tokio::spawn(async move {
        first
            .with_warehouse_read(
                &selected,
                &first_cancellation,
                EvidenceReadBudget::default(),
                move |service, budget| async move {
                    let mut session = service.session(budget, &reading_cancellation)?.retaining(Some(permit));
                    session
                        .analyze(move |_| {
                            entered.send(()).unwrap();
                            released.recv_timeout(Duration::from_secs(2)).unwrap();
                            worker_settled.store(true, Ordering::SeqCst);
                            Ok(())
                        })
                        .await
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), started).await.unwrap().unwrap();
    cancellation.cancel();
    tokio::task::yield_now().await;
    assert!(!original.is_finished(), "cancellation must not detach the already-admitted service/CPU awaiter");
    assert!(!settled.load(Ordering::SeqCst));
    assert_eq!(gate.available_permits(), 0);
    let queried = Arc::new(AtomicBool::new(false));
    let observed = queried.clone();
    *store.after_query.lock().unwrap() = Some(Box::new(move || {
        observed.store(true, Ordering::SeqCst);
    }));
    let next_ws = ws.clone();
    let next_gate = gate.clone();
    let next_reference = saved.reference.clone();
    let replacement = tokio::spawn(async move {
        let _same_slot = next_gate.acquire_owned().await.unwrap();
        next_ws
            .warehouse_resources(
                &next_reference,
                &PublishedResourceRequest::default(),
                &CollectionCancellation::default(),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!queried.load(Ordering::SeqCst), "no replacement warehouse query overlaps the unsettled admitted CPU");
    assert!(!replacement.is_finished());
    release.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), original).await.unwrap().unwrap().unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    let resources = tokio::time::timeout(Duration::from_secs(2), replacement).await.unwrap().unwrap().unwrap();
    assert_eq!(resources.resources.len(), 1);
    assert!(settled.load(Ordering::SeqCst));
    assert!(queried.load(Ordering::SeqCst));
    assert_eq!(configured.load(Ordering::SeqCst), 1, "replacement reuses the real saved revision/client owner");
    assert!(ws.connections().unwrap().is_empty());
}

#[tokio::test]
async fn a_removed_saved_revision_is_rechecked_after_started_cpu_settles() {
    let dir = tempfile::tempdir().unwrap();
    let (ws, saved, _, configured) = configured(dir.path()).await;
    let ws = Arc::new(ws);
    let first = ws.clone();
    let selected = saved.reference.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let read = tokio::spawn(async move {
        let cancellation = CollectionCancellation::default();
        first
            .with_warehouse_read(
                &selected,
                &cancellation,
                EvidenceReadBudget::default(),
                move |service, budget| async move {
                    let token = CollectionCancellation::default();
                    let mut session = service.session(budget, &token)?;
                    session
                        .analyze(move |_| {
                            entered.send(()).unwrap();
                            released.recv_timeout(Duration::from_secs(2)).unwrap();
                            Ok(42)
                        })
                        .await
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), started).await.unwrap().unwrap();
    ws.remove_warehouse_connection(&saved.reference).unwrap();
    release.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), read).await.unwrap().unwrap().unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::DestinationChanged)));
    assert_eq!(configured.load(Ordering::SeqCst), 1);
    assert!(ws.warehouse_connections().unwrap().is_empty());
}

#[test]
fn remaining_action_time_is_floored_and_a_sub_millisecond_remainder_cannot_start_a_new_phase() {
    let budget = EvidenceReadBudget::default();
    let error = remaining(budget, Instant::now() + Duration::from_micros(500)).unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::TimedOut);
    let bounded = EvidenceReadBudget { timeout_ms: 100, ..budget };
    assert_eq!(remaining(bounded, Instant::now() + Duration::from_secs(1)).unwrap().timeout_ms, 100);
}
