use super::*;
use crate::{
    MssqlProvider,
    deadlocks::system_health::tests::owned_fixture,
    test_support::{config, save_cost_receipt},
};
use culprit_collector::capture::{self, SourceFactory};
use culprit_core::{
    Environment, Provider, SnapshotOptions,
    telemetry::{CollectionStatus, MssqlExtendedEventKind, SharedResourceIdentity},
};
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use serde_json::json;

fn provider(port: u16, database: &str, password: &str) -> MssqlProvider {
    MssqlProvider {
        endpoint_host: "127.0.0.1".into(),
        endpoint_port: port,
        environment: Environment::Development,
        pool: Arc::new(Pool::new(config(port, database, password, "culprit"), None)),
    }
}

struct ProviderFactory {
    provider: MssqlProvider,
}

#[async_trait::async_trait]
impl SourceFactory for ProviderFactory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.provider.telemetry_source(request)
    }
}

#[tokio::test]
#[ignore = "requires nonce-owned loopback SQL Server and private native receipts; non-flushing system_health read only"]
async fn owned_system_health_native_failures_roundtrip_scan_and_outbox_without_target_flushing() {
    // 1. Relative configured filename on stock system_health is refused cleanly without target-data fallback.
    owned_fixture::run("native-failures-relative-path", false, |fixture| {
        Box::pin(async move {
            let db_provider = provider(fixture.admitted.port, &fixture.admitted.database, fixture.admitted.password());
            let scan = db_provider
                .snapshot(
                    "native-failures-relative",
                    &SnapshotOptions { collect_native_failures: true, ..Default::default() },
                )
                .await?;
            assert!(scan.operations.native_failures.is_none());
            let outcome = scan.sources.iter().find(|entry| entry.source == sources::NATIVE_FAILURES.name).unwrap();
            assert_eq!(outcome.state, culprit_core::SourceState::Unavailable);
            assert_eq!(db_provider.source_shutdown(), SourceShutdown::Quiescent);
            Ok(())
        })
    })
    .await
    .unwrap();

    // 2. Owned absolute system_health .xel file with a real planted deadlock decodes identically across scan and collector.
    owned_fixture::run("native-failures-system-health", true, |fixture| {
        Box::pin(async move {
            let db_provider = provider(fixture.admitted.port, &fixture.admitted.database, fixture.admitted.password());
            let discovered = db_provider
                .telemetry_resources(&culprit_core::telemetry::ResourceDiscoveryRequest {
                    kind: SharedResourceKind::Instance,
                    budget: CollectionBudget::default(),
                })
                .await?;
            assert_eq!(discovered.len(), 1);
            let instance: SharedResourceIdentity = discovered[0].identity.clone();
            assert!(
                discovered[0]
                    .collections
                    .contains(&culprit_core::telemetry::TelemetryCollection::Snapshot(SnapshotDataset::NativeFailures))
            );

            let scan = db_provider
                .snapshot(
                    "native-failures-fixture",
                    &SnapshotOptions { collect_native_failures: true, ..Default::default() },
                )
                .await?;
            let scan_failures =
                scan.operations.native_failures.as_ref().expect("system_health native failures snapshot");
            assert_eq!(scan_failures.resource.as_ref(), Some(&instance));
            assert!(
                scan_failures.observations.iter().any(|record| matches!(
                    record,
                    NativeFailureRecord::MssqlExtendedEvent(event)
                        if matches!(event.kind, MssqlExtendedEventKind::DeadlockReport { graphs } if graphs >= 1)
                )),
                "real planted deadlock must decode from SQL Server 2022 system_health .xel file"
            );
            let outcome = scan.sources.iter().find(|entry| entry.source == sources::NATIVE_FAILURES.name).unwrap();
            assert_eq!(
                outcome.state,
                culprit_core::SourceState::Partial,
                "default system_health predicates error_reported and must stay Partial rather than complete history"
            );

            let dir = tempfile::tempdir().unwrap();
            let request = CollectionRequest {
                subject: CollectionSubject::Shared { resource: instance },
                selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::NativeFailures },
                budget: CollectionBudget::default(),
            };
            let factory = ProviderFactory {
                provider: provider(fixture.admitted.port, &fixture.admitted.database, fixture.admitted.password()),
            };
            let cancellation = CollectionCancellation::default();
            let gate = SourceGate::default();
            let manifest =
                capture::collect_once(&gate, &factory, request, &dir.path().join("capture"), &cancellation).await?;
            assert_eq!(manifest.status, CollectionStatus::Complete);
            assert!(manifest.directory_synced);
            let description = manifest.description.as_ref().expect("completed source description");
            assert!(description.observation.snapshot().expect("snapshot description").records > 0);
            assert_eq!(description.source, MSSQL_SYSTEM_HEALTH_EVENTS_SOURCE);
            assert_eq!(
                culprit_core::telemetry::diagnostic_original_input_qualification(Some(description)),
                culprit_core::telemetry::DiagnosticInputQualification::Qualified {
                    source: culprit_core::telemetry::DiagnosticSourceKind::NativeFailures,
                }
            );
            assert_eq!(db_provider.source_shutdown(), SourceShutdown::Quiescent);
            assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);

            let receipt_dir = crate::test_support::receipt_directory();
            save_cost_receipt(
                &receipt_dir,
                "system-health-native-failures",
                &json!({
                    "completed": true,
                    "source": MSSQL_SYSTEM_HEALTH_EVENTS_SOURCE,
                    "scan_records": scan_failures.observations.len(),
                    "scan_state": outcome.state,
                    "capture_queries": manifest.source_queries,
                    "target_flushing_used": false,
                    "production_admission": "unqualified",
                }),
            )?;
            Ok(())
        })
    })
    .await
    .unwrap();
}
