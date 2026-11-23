//! Actual canonical capture/receipt/EOF, over the shared scripted native owner.
use super::*;
use culprit_collector::{
    capture::{self, SourceFactory},
    outbox::Outbox,
};
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::sync::Mutex;
struct Factory(Mutex<Option<ResourceSource<FakeRead>>>);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        let source = self.0.lock().map_err(|_| decode::malformed())?.take().ok_or_else(decode::malformed)?;
        if &source.request != request {
            return Err(decode::malformed());
        }
        Ok(Box::new(source))
    }
}
#[tokio::test]
async fn readonly_history_roundtrips_the_real_canonical_writer_and_eof_without_primary_alias_or_native_recollection() {
    let directory = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::create(&directory.path().join("outbox"), OutboxLimits::default()).unwrap();
    let request = request();
    let factory = Factory(Mutex::new(Some(scripted_source(replies(1)))));
    let cancellation = CollectionCancellation::default();
    let mut lease = outbox.reserve("readonly-member", &request).await.unwrap();
    let identity = lease.identity().clone();
    let output = lease.directory().to_owned();
    let manifest =
        capture::collect(&SourceGate::default(), &factory, request, &output, identity, &cancellation, Some(&mut lease))
            .await
            .unwrap();
    lease.finish().await.unwrap();
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert_eq!(manifest.source_queries, 9);
    let description = manifest.description.as_ref().unwrap();
    let SourceObservation::MssqlReadableResources(history) = &description.observation else { panic!() };
    assert_eq!(history.observed_role, Some(MssqlResourceReplicaRole::HighAvailabilitySecondary));
    assert_eq!(description.observation.native_coverage_state().unwrap(), Some(culprit_core::SourceState::Partial));
    let attempt = outbox.attempt_receipt(&manifest.capture, None).await.unwrap();
    assert!(attempt.manifest_sha256.is_some());
    let verified = outbox.verify(&manifest.capture.observation_id).await.unwrap();
    assert_eq!(verified.status, CollectionStatus::Complete);
    let raw = std::fs::read_to_string(output.join(TelemetryStream::ResourceSamples.file_name())).unwrap();
    let mut rows = 0;
    for line in raw.lines() {
        let row: ExportedRecord = serde_json::from_str(line).unwrap();
        assert_eq!(row, ExportedRecord::new(description, &manifest.capture, row.record.clone()).unwrap());
        assert!(matches!(row.record,TelemetryRecord::ResourceSample(sample) if sample.replica_id.is_none()));
        rows += 1;
    }
    assert_eq!(rows, 2);
    assert!(factory.0.lock().unwrap().is_none());
    let mut altered = raw.lines().map(|line| serde_json::from_str::<ExportedRecord>(line).unwrap()).collect::<Vec<_>>();
    let TelemetryRecord::ResourceSample(sample) = &mut altered[0].record else { panic!() };
    sample.metrics[0].value = Some(42.0);
    let altered = altered.iter().map(|row| serde_json::to_string(row).unwrap() + "\n").collect::<String>();
    std::fs::write(output.join(TelemetryStream::ResourceSamples.file_name()), altered).unwrap();
    assert!(outbox.verify(&manifest.capture.observation_id).await.is_err());
}
