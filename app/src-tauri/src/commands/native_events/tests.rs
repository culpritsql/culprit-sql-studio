use super::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, oneshot};

#[tokio::test]
async fn cancellation_waits_for_the_same_admitted_cpu_owner_before_reusing_its_permit() {
    let gate = Arc::new(Semaphore::new(1));
    let permit = gate.clone().acquire_owned().await.unwrap();
    let cancellation = CollectionCancellation::default();
    let token = cancellation.clone();
    let (started, ready) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        settle_import(Some(permit), &token, move |operation| {
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(2)).unwrap();
            culprit_engine::native_logs::analyze_bytes_with(
                b"<events/>",
                NativeLogFormat::MssqlExtendedEventsXml,
                Default::default(),
                operation,
            )
        })
        .await
    });
    ready.await.unwrap();
    cancellation.cancel();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    assert_eq!(gate.available_permits(), 0);
    release.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(gate.available_permits(), 1);
}

#[tokio::test]
async fn caller_drop_cannot_release_a_slot_while_the_actual_blocking_worker_is_still_active() {
    let gate = Arc::new(Semaphore::new(1));
    let permit = gate.clone().acquire_owned().await.unwrap();
    let (started, ready) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        settle_import(Some(permit), &CollectionCancellation::default(), move |operation| {
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(2)).unwrap();
            culprit_engine::native_logs::analyze_bytes_with(
                b"<events/>",
                NativeLogFormat::MssqlExtendedEventsXml,
                Default::default(),
                operation,
            )
        })
        .await
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(gate.available_permits(), 0);
    release.send(()).unwrap();
    let held = tokio::time::timeout(Duration::from_secs(2), gate.acquire()).await.unwrap().unwrap();
    drop(held);
}

#[tokio::test]
async fn selected_path_uses_the_canonical_engine_reader_and_bounds_without_returning_the_path() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "culprit-event-xml-{}-{}.xml",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let xml = b"<event name=\"attention\" package=\"sqlserver\" timestamp=\"2020-01-01T12:00:00Z\"/>";
    std::fs::write(&path, xml).unwrap();
    let selected = path.clone();
    let result = settle_import(None, &CollectionCancellation::default(), move |operation| {
        culprit_engine::native_logs::analyze_file_with(
            &selected,
            NativeLogFormat::MssqlExtendedEventsXml,
            Default::default(),
            operation,
        )
    })
    .await;
    std::fs::remove_file(&path).unwrap();
    let result = result.unwrap();
    let expected =
        culprit_engine::native_logs::analyze_bytes(xml, NativeLogFormat::MssqlExtendedEventsXml, Default::default())
            .unwrap();
    assert_eq!(result.input, expected.input);
    assert_eq!(result.report, expected.report);
    assert_eq!(result.native_events, expected.native_events);
    assert!(result.evidence.resource.is_none());
    assert!(!serde_json::to_string(&result).unwrap().contains(&path.display().to_string()));
    let cancellation = CollectionCancellation::default();
    cancellation.cancel();
    let result = settle_import(None, &cancellation, move |operation| {
        culprit_engine::native_logs::analyze_file_with(
            &path,
            NativeLogFormat::MssqlExtendedEventsXml,
            Default::default(),
            operation,
        )
    })
    .await
    .unwrap_err();
    assert_eq!(result.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
}
