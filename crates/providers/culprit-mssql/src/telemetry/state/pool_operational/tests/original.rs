//! The real canonical writer/reopen path, over the same scripted NativeRead. This is not native cost qualification.
use super::*;
use culprit_collector::{
    capture::{self, SourceFactory},
    outbox::{CompletedCaptureBudget, Outbox},
};
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::sync::Mutex;
struct Factory(Mutex<Option<StateSource<FakeRead>>>);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        let source = self.0.lock().map_err(|_| invalid_data())?.take().ok_or_else(invalid_data)?;
        if source.request != *request {
            return Err(invalid_data());
        }
        Ok(Box::new(source))
    }
}
#[tokio::test]
async fn one_pool_original_reopens_to_eof_and_feeds_existing_scan_views_without_native_recollection() {
    let (_directory, outbox, manifest, factory) = capture_source(scripted_source(replies(), 22)).await;
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert_eq!(manifest.source_queries, 21);
    let attempt = outbox.attempt_receipt(&manifest.capture, None).await.unwrap();
    let captured = outbox
        .delivery()
        .completed_snapshot_for_connection(
            "pool-anchor",
            &attempt,
            SnapshotDataset::Operational,
            CompletedCaptureBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    let original = captured.into_operational_read().unwrap();
    assert!(original.is_valid() && original.read_complete);
    assert!(original.captured_references().unwrap().len() == original.observations.len());
    let clerks = culprit_checks::memory_clerks::explain_captured(&original).unwrap();
    assert_eq!(clerks.entries.len(), 1);
    let mut foreign_coverage = original.memory_clerk_source().unwrap().clone();
    foreign_coverage.complete_scope = !foreign_coverage.complete_scope;
    let clerk_rows =
        original.observations.iter().filter(|row| row.source == foreign_coverage.source).collect::<Vec<_>>();
    assert!(
        culprit_checks::memory_clerks::explain_source(&original.description, &foreign_coverage, &clerk_rows, true)
            .is_err()
    );
    let mut scan = empty_scan();
    scan.operations.memory_clerks = Some(Box::new(original.clone()));
    let scan_report = culprit_checks::memory_clerks::explain_snapshot(&scan).unwrap().unwrap();
    assert_eq!(scan_report.entries.len(), clerks.entries.len());
    let coverage = original.source_coverage(culprit_core::source_id::PERFORMANCE_COUNTERS).unwrap();
    let rows = original.observations.iter().filter(|row| row.source == coverage.source).collect::<Vec<_>>();
    let counters =
        culprit_checks::performance_counters::explain_source(&original.description, coverage, &rows, true).unwrap();
    assert_eq!(counters.population, culprit_core::PerformanceCounterPopulation::SharedPool);
    assert!(!counters.entries.is_empty());
    let schedulers = culprit_checks::instance_resources::explain_captured(&original).unwrap();
    assert_eq!(schedulers.schedulers.unwrap().entries.len(), 1);
    let storage = culprit_checks::storage::explain_captured(&original).unwrap();
    assert_eq!(storage.target, original.description.target);
    assert_eq!(storage.native_rows, 1);
    assert_eq!(storage.observed_rows, 1);
    assert_eq!(storage.source_state, SourceState::Collected);
    assert_eq!(storage.entries.len(), 1);
    assert!(storage.entries[0].fields.iter().any(|field| field.key == "user_bytes" && field.value.is_some()));
    assert!(storage.entries[0].fields.iter().any(|field| field.key == "internal_bytes" && field.value.is_some()));
    assert!(storage.entries[0].fields.iter().any(|field| field.key == "version_bytes" && field.value.is_some()));
    let public = culprit_core::redact::redact_snapshot(&scan);
    assert!(public.operations.memory_clerks.is_none());
    assert!(!serde_json::to_string(&public).unwrap().contains(PHYSICAL));
    assert!(factory.0.lock().unwrap().is_none()); // no reopened native source for any consumer.
    let mut tampered = original.clone();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut tampered.description.observation else { panic!() };
    authority.native.as_mut().unwrap().after = "f".repeat(64);
    assert!(!tampered.is_valid());
    assert!(culprit_checks::memory_clerks::explain_captured(&tampered).is_err());
    assert!(culprit_checks::storage::explain_captured(&tampered).is_err());
}

#[tokio::test]
async fn caller_twenty_or_twenty_one_never_commits_a_complete_pool_original_without_all_final_native_checks() {
    for total in [20, 21] {
        let (_directory, outbox, manifest, _) = capture_source(scripted_source(replies(), total)).await;
        assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::QueryLimit));
        assert_eq!(manifest.source_queries, total - 1);
        let attempt = outbox.attempt_receipt(&manifest.capture, None).await.unwrap();
        assert!(
            outbox
                .delivery()
                .completed_snapshot_for_connection(
                    "pool-anchor",
                    &attempt,
                    SnapshotDataset::Operational,
                    CompletedCaptureBudget::default(),
                    &CollectionCancellation::default(),
                )
                .await
                .is_err()
        );
    }
}

async fn capture_source(source: StateSource<FakeRead>) -> (tempfile::TempDir, Outbox, CollectionManifest, Factory) {
    let request = source.request.clone();
    let factory = Factory(Mutex::new(Some(source)));
    let directory = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::create(&directory.path().join("outbox"), OutboxLimits::default()).unwrap();
    let cancellation = CollectionCancellation::default();
    let mut lease = outbox.reserve("pool-anchor", &request).await.unwrap();
    let identity = lease.identity().clone();
    let output = lease.directory().to_owned();
    let result =
        capture::collect(&SourceGate::default(), &factory, request, &output, identity, &cancellation, Some(&mut lease))
            .await;
    lease.finish().await.unwrap();
    (directory, outbox, result.unwrap(), factory)
}
#[tokio::test]
async fn unconfirmed_native_shutdown_keeps_failed_bytes_sealed_and_never_publishes_a_pool_original() {
    let mut source = scripted_source(replies(), 22);
    source.io.shutdown = SourceShutdown::Unconfirmed;
    let (_directory, outbox, manifest, _) = capture_source(source).await;
    assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::CancellationUnconfirmed));
    let attempt = outbox.attempt_receipt(&manifest.capture, None).await.unwrap();
    assert!(attempt.manifest_sha256.is_some());
    let error = outbox
        .delivery()
        .completed_snapshot_for_connection(
            "pool-anchor",
            &attempt,
            SnapshotDataset::Operational,
            CompletedCaptureBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::SourceUnconfirmed)));
}

fn empty_scan() -> culprit_core::Snapshot {
    use culprit_core::{Environment, SNAPSHOT_FORMAT, ServerInfo, Snapshot};
    Snapshot {
        operations: Default::default(),
        format: SNAPSHOT_FORMAT,
        id: "scripted-current-scan".into(),
        connection: "pool-anchor".into(),
        environment: Environment::Local,
        taken_at: "2020-01-01T07:00:00Z".into(),
        duration_ms: 0.0,
        server: ServerInfo {
            engine: Engine::Mssql,
            product: "scripted".into(),
            version: "16".into(),
            edition: None,
            cloud: None,
            platform: None,
            database: "independent-current-database".into(),
            database_size_bytes: None,
            started_at: None,
            read_only_session: false,
            capabilities: vec![],
            missing: vec![],
        },
        runtime_populations: None,
        workload: None,
        tables: vec![],
        indexes: vec![],
        columns: vec![],
        foreign_keys: vec![],
        missing_indexes: vec![],
        statistics: vec![],
        waits: vec![],
        resources: vec![],
        settings: vec![],
        notes: vec![],
        pressure: None,
        options: None,
        query_families: vec![],
        plan_cache: None,
        tuning: None,
        deadlocks: None,
        constraints: vec![],
        histograms: None,
        escalations_before: None,
        compilations_before: None,
        contention_before: None,
        evictions_before: None,
        sizes_before: None,
        sources: vec![],
        identity: None,
    }
}
