use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[tokio::test]
async fn replacing_or_closing_an_archive_request_cancels_its_existing_read_token_without_new_work() {
    for close in [false, true] {
        let requests = Arc::new(WarehouseRequests::default());
        requests.0.begin("main", "old".into()).unwrap();
        let cancellation = CollectionCancellation::default();
        let observed = cancellation.clone();
        let active = requests.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let previous = tokio::spawn(async move {
            active
                .0
                .run("main", "old", async move {
                    let _owner = ReadCancellation(cancellation);
                    let _ = ready.send(());
                    std::future::pending::<Result<()>>().await
                })
                .await
        });
        started.await.unwrap();
        if close {
            requests.close("main");
        } else {
            requests.0.begin("main", "new".into()).unwrap();
        }
        assert!(previous.await.unwrap().is_err());
        assert!(observed.is_cancelled());
        if !close {
            assert_eq!(requests.0.run("main", "new", async { Ok(1) }).await.unwrap(), 1);
        }
    }
}
#[tokio::test]
async fn obsolete_registration_and_early_cancellation_never_poll_an_archive_composition() {
    let requests = WarehouseRequests::default();
    requests.0.begin("main", "old".into()).unwrap();
    requests.0.begin("main", "new".into()).unwrap();
    let opened = AtomicBool::new(false);
    assert!(
        requests
            .0
            .run("main", "old", async {
                opened.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .is_err()
    );
    requests.0.cancel("main", "new");
    assert!(
        requests
            .0
            .run("main", "new", async {
                opened.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .is_err()
    );
    assert!(!opened.load(Ordering::SeqCst));
}
