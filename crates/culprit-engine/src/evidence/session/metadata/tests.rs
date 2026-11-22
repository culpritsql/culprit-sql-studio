use super::*;
use culprit_evidence::delivery::object_store::{read_bytes, transport::ObjectRead};
use std::sync::atomic::{AtomicUsize, Ordering};
fn fence() -> ReadFence {
    ReadFence {
        cancellation: culprit_read::cancellation::CollectionCancellation::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    }
}
struct Objects {
    gets: AtomicUsize,
}
#[async_trait::async_trait]
impl ObjectReadTransport for Objects {
    async fn get(&self, key: &str) -> Result<Option<ObjectRead>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok((key != "missing.json").then(|| ObjectRead {
            etag: "stable".into(),
            bytes: 3,
            body: Box::pin(std::io::Cursor::new(vec![1, 2, 3])),
        }))
    }
}
#[tokio::test]
async fn missing_metadata_reads_are_bounded_and_payload_bytes_do_not_consume_the_proof_allowance_twice() {
    let transport = Arc::new(Objects { gets: AtomicUsize::new(0) });
    let accounting = MetadataAccounting::new(transport.clone(), 1, 3, fence());
    let payload = accounting.get("payloads/owned.ndjson").await.unwrap().unwrap();
    assert_eq!(read_bytes(payload, 3).await.unwrap(), vec![1, 2, 3]);
    assert_eq!(accounting.work().unwrap().reads, 0);
    assert_eq!(accounting.work().unwrap().bytes, 0);
    let object = accounting.get("recovery.json").await.unwrap().unwrap();
    assert_eq!(read_bytes(object, 3).await.unwrap(), vec![1, 2, 3]);
    for _ in 0..3 {
        assert!(accounting.get("missing.json").await.unwrap().is_none());
    }
    assert!(accounting.get("missing.json").await.is_err());
    assert_eq!(transport.gets.load(Ordering::SeqCst), 5);
    assert_eq!(accounting.work().unwrap().bytes, 3);
}
#[tokio::test]
async fn metadata_body_length_is_admitted_before_the_existing_decoder_allocates_or_authenticates_it() {
    let transport = Arc::new(Objects { gets: AtomicUsize::new(0) });
    let accounting = MetadataAccounting::new(transport.clone(), 1, 2, fence());
    assert!(accounting.get("manifests/exact.json").await.is_err());
    assert_eq!(transport.gets.load(Ordering::SeqCst), 1);
}
