use super::*;

#[tokio::test]
async fn replaced_archive_request_cancels_then_settles_before_the_same_read_slot_is_reused() {
    let requests = Arc::new(WindowRequests::serialized());
    requests.begin("main", "old".into()).unwrap();
    let cancellation = CollectionCancellation::default();
    let reading = cancellation.clone();
    let previous = requests.clone();
    let (entered, started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let old = tokio::spawn(async move {
        previous
            .run_settled("main", "old", &reading, |permit| async move {
                let _permit = permit.expect("this is the existing serialized gate");
                entered.send(()).unwrap();
                released.await.unwrap();
                Ok("stale result")
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), started).await.unwrap().unwrap();
    requests.begin("main", "new".into()).unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let next_requests = requests.clone();
    let next = tokio::spawn(async move {
        next_requests
            .run_settled("main", "new", &CollectionCancellation::default(), |permit| async move {
                let _permit = permit.unwrap();
                observed.store(true, Ordering::SeqCst);
                Ok("new result")
            })
            .await
    });
    while !cancellation.is_cancelled() {
        tokio::task::yield_now().await;
    }
    assert!(!old.is_finished(), "the real admitted callback is awaited after replacement");
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(requests.gate.as_ref().unwrap().available_permits(), 0);
    release.send(()).unwrap();
    assert!(old.await.unwrap().is_err(), "settlement cannot publish the stale success");
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), next).await.unwrap().unwrap().unwrap(), "new result");
    assert!(polled.load(Ordering::SeqCst));
    assert!(requests.active.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelling_a_queued_archive_request_starts_no_callback_and_does_not_release_another_owner() {
    let requests = Arc::new(WindowRequests::serialized());
    let held = requests.gate.as_ref().unwrap().clone().acquire_owned().await.unwrap();
    requests.begin("main", "queued".into()).unwrap();
    let cancellation = CollectionCancellation::default();
    let observing = cancellation.clone();
    let next_requests = requests.clone();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let queued = tokio::spawn(async move {
        next_requests
            .run_settled("main", "queued", &observing, |_| async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(2), queued).await.unwrap().unwrap().is_err());
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(requests.gate.as_ref().unwrap().available_permits(), 0);
    assert!(requests.active.lock().unwrap().is_empty());
    drop(held);
    assert_eq!(requests.gate.as_ref().unwrap().available_permits(), 1);
}

#[tokio::test]
async fn a_ready_archive_callback_cannot_publish_after_a_newer_registration_replaces_it() {
    let requests = WindowRequests::serialized();
    requests.begin("main", "old".into()).unwrap();
    let cancellation = CollectionCancellation::default();
    let registration = &requests;
    let result = requests
        .run_settled("main", "old", &cancellation, |permit| async move {
            let _permit = permit.unwrap();
            registration.begin("main", "new".into()).unwrap();
            Ok("late success")
        })
        .await;
    assert!(result.is_err());
    assert!(cancellation.is_cancelled());
    assert_eq!(requests.active.lock().unwrap()["main"].id, "new");
    assert_eq!(
        requests.run_settled("main", "new", &CollectionCancellation::default(), |_| async { Ok(42) }).await.unwrap(),
        42
    );
    assert!(requests.active.lock().unwrap().is_empty());
}
