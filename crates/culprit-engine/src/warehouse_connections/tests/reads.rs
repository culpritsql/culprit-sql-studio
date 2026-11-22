use super::*;
use culprit_auth::CredentialResolver;
use culprit_evidence::{
    analytical::{
        ProjectionCatalog,
        evidence::{IncidentDiscoveryRequest, PublishedResourceQuery},
        publication,
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
        test_support::{self, ReadOnlyFixture},
    },
    delivery::{
        contract::Contract,
        object_store::{ObjectRecoveryReader, transport::ObjectReadTransport},
        recovery::{CommittedRange, reopen},
    },
};
use serde_json::{Map, Value};
struct Store {
    inner: Arc<dyn AnalyticalReadStore>,
    publication: Vec<Value>,
    after_query: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    after_configure: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}
#[async_trait::async_trait]
impl AnalyticalReadStore for Store {
    fn destination(&self) -> &str {
        self.inner.destination()
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.inner.catalog()
    }
    fn logical_values(
        &self,
        stream: culprit_core::telemetry::TelemetryStream,
        row: &[Value],
    ) -> Result<Map<String, Value>> {
        self.inner.logical_values(stream, row)
    }
    async fn audit_membership(&self, range: &CommittedRange, limit: usize) -> Result<usize> {
        self.inner.audit_membership(range, limit).await
    }
    async fn rows(
        &self,
        range: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        limit: usize,
    ) -> Result<AnalyticalReadPage> {
        self.inner.rows(range, batch, after, until, limit).await
    }
    async fn publication_rows(&self, range: &CommittedRange, limit: usize) -> Result<AnalyticalReadPage> {
        self.inner.publication_rows(range, limit).await
    }
    async fn resource_candidates(
        &self,
        _: &Contract,
        _: &PublishedResourceQuery,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        if let Some(action) = self.after_query.lock().unwrap().take() {
            action();
        }
        let rows = vec![self.publication.clone()];
        Ok(AnalyticalReadPage { response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows })
    }
    async fn incident_candidates(
        &self,
        _: &Contract,
        _: &IncidentDiscoveryRequest,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        Ok(AnalyticalReadPage { rows: vec![], response_bytes: 2 })
    }
}
struct WarehouseProvider {
    store: Arc<Store>,
    configured: Arc<AtomicUsize>,
}
impl crate::evidence::EvidenceStoreProvider for WarehouseProvider {
    fn name(&self) -> &str {
        "clickhouse"
    }
    fn configure(
        &self,
        _: &Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn AnalyticalReadStore>> {
        assert_eq!(profile, "warehouse-read");
        assert!(credentials.validate_password("recovery-read").is_err());
        let password = credentials.password(profile)?;
        assert_eq!(password.username, "archive_reader");
        assert_eq!(password.password.as_str(), "private-warehouse-password");
        self.configured.fetch_add(1, Ordering::SeqCst);
        if let Some(action) = self.store.after_configure.lock().unwrap().take() {
            action();
        }
        Ok(self.store.clone())
    }
}
struct RecoveryProvider(Arc<dyn ObjectReadTransport>);
impl crate::evidence::EvidenceRecoveryProvider for RecoveryProvider {
    fn name(&self) -> &str {
        "azure-blob"
    }
    fn configure(
        &self,
        _: &Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        assert_eq!(profile, "recovery-read");
        credentials.bearer(profile)?;
        assert!(credentials.bearer("warehouse-read").is_err());
        Ok(self.0.clone())
    }
}
async fn configured(dir: &std::path::Path) -> (Workspace, WarehouseConnectionInfo, Arc<Store>, Arc<AtomicUsize>) {
    configured_with_secrets(dir, Arc::new(Memory::default())).await
}
async fn configured_with_secrets(
    dir: &std::path::Path,
    secrets: Arc<dyn SecretStore>,
) -> (Workspace, WarehouseConnectionInfo, Arc<Store>, Arc<AtomicUsize>) {
    let (source, rows) = test_support::informational_query_evidence();
    let fixture = ReadOnlyFixture::capture(source, rows).await;
    let recovery = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let catalog = fixture.store.catalog();
    let schema = catalog.publication();
    let values = schema.project(&publication::input(catalog.as_ref(), &range).unwrap()).unwrap();
    let encoded: Map<String, Value> = serde_json::from_slice(&schema.encode(&values).unwrap()).unwrap();
    let publication = schema.columns().iter().map(|column| encoded[&column.name].clone()).collect();
    let store = Arc::new(Store {
        inner: fixture.store.clone(),
        publication,
        after_query: Mutex::new(None),
        after_configure: Mutex::new(None),
    });
    let configured = Arc::new(AtomicUsize::new(0));
    let runtime = EvidenceRuntime::empty()
        .with_evidence_store_provider(Arc::new(WarehouseProvider {
            store: store.clone(),
            configured: configured.clone(),
        }))
        .unwrap()
        .with_recovery_provider(Arc::new(RecoveryProvider(fixture.transport())))
        .unwrap();
    let ws = Workspace::open_in(dir).with_secrets(secrets).with_warehouse_runtime(Arc::new(runtime));
    let saved = ws.add_warehouse_connection(input("archive")).unwrap();
    (ws, saved, store, configured)
}
#[tokio::test]
async fn saved_archive_resources_then_around_incident_use_one_real_read_handle_and_canonical_proof_without_native_connection()
 {
    let dir = tempfile::tempdir().unwrap();
    let (ws, saved, _, configured) = configured(dir.path()).await;
    let cancellation = CollectionCancellation::default();
    let request = PublishedResourceRequest { as_of: "2026-11-22T12:00:00Z".into(), ..Default::default() };
    let resources = ws.warehouse_resources(&saved.reference, &request, &cancellation).await.unwrap();
    assert_eq!(resources.resources.len(), 1);
    assert!(resources.discovery_complete);
    let resource = &resources.resources[0];
    let report = ws
        .warehouse_incident(&saved.reference, &resource.selector, &resource.collected_until, &cancellation)
        .await
        .unwrap();
    assert_eq!(report.database, resource.selector.database);
    assert!(ws.connections().unwrap().is_empty());
    assert_eq!(
        configured.load(Ordering::SeqCst),
        1,
        "cancellation/replacement must not replace real native transport gates for the same revision"
    );
}
#[tokio::test]
async fn removed_archive_and_cancelled_late_response_cannot_publish_the_originals_as_another_saved_route() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (ws, saved, store, _) = configured(dir.path()).await;
        let token = CollectionCancellation::default();
        if cancel {
            let token = token.clone();
            *store.after_query.lock().unwrap() = Some(Box::new(move || token.cancel()));
        } else {
            let file = dir.path().join("warehouse-connections.toml");
            *store.after_query.lock().unwrap() = Some(Box::new(move || {
                std::fs::remove_file(&file).unwrap();
            }));
        }
        let request = PublishedResourceRequest { as_of: "2026-11-22T12:00:00Z".into(), ..Default::default() };
        let error = ws.warehouse_resources(&saved.reference, &request, &token).await.unwrap_err();
        assert_eq!(
            error.detail,
            Some(if cancel {
                ErrorDetail::CollectionStopped(CollectionStop::Cancelled)
            } else {
                ErrorDetail::Delivery(DeliveryProblem::DestinationChanged)
            })
        );
    }
}

#[tokio::test]
async fn cancelled_or_expired_preparation_waiting_for_the_existing_handle_lock_cannot_read_credentials_or_cache_a_client()
 {
    for cancel in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let secrets = Arc::new(Secrets::default());
        let (ws, saved, _, configured) = configured_with_secrets(dir.path(), secrets.clone()).await;
        {
            let held = ws.warehouse.handles.lock().unwrap();
            let manager = ws.warehouse.clone();
            let cancellation = CollectionCancellation::default();
            let preparing = cancellation.clone();
            let deadline = Instant::now() + if cancel { Duration::from_secs(5) } else { Duration::from_millis(10) };
            let (entered, submitted) = std::sync::mpsc::channel();
            let worker_secrets = secrets.clone();
            // This is the real preparation entry and real existing mutex, not a mirrored gate or fake credential path.
            let worker = std::thread::spawn(move || {
                entered.send(()).unwrap();
                manager.prepare(&saved.reference, worker_secrets, &preparing, deadline)
            });
            submitted.recv_timeout(Duration::from_secs(2)).unwrap();
            if cancel {
                cancellation.cancel();
            } else {
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            }
            drop(held);
            let error = worker.join().unwrap().err().unwrap();
            assert_eq!(
                CollectionStop::from_error(&error),
                if cancel { CollectionStop::Cancelled } else { CollectionStop::TimedOut }
            );
            assert_eq!(secrets.reads.load(Ordering::SeqCst), 0);
            assert_eq!(configured.load(Ordering::SeqCst), 0);
            assert!(ws.warehouse.handles.lock().unwrap().is_empty());
            assert!(ws.connections().unwrap().is_empty());
        }
    }
}
#[tokio::test]
async fn cancelled_successful_client_preparation_does_not_cache_a_handle_or_start_an_evidence_query() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let (ws, saved, store, configured) = configured_with_secrets(dir.path(), secrets.clone()).await;
    let cancellation = CollectionCancellation::default();
    let cancelling = cancellation.clone();
    *store.after_configure.lock().unwrap() = Some(Box::new(move || cancelling.cancel()));
    let queried = Arc::new(AtomicBool::new(false));
    let observed = queried.clone();
    *store.after_query.lock().unwrap() = Some(Box::new(move || {
        observed.store(true, Ordering::SeqCst);
    }));
    let error = ws
        .warehouse_resources(&saved.reference, &PublishedResourceRequest::default(), &cancellation)
        .await
        .unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    // Taking the same lock waits for the already-started synchronous constructor to return. It never
    // asserts that caller cancellation settles an already-started HTTP request or resets native gates.
    assert!(ws.warehouse.handles.lock().unwrap().is_empty());
    assert_eq!(secrets.reads.load(Ordering::SeqCst), 1);
    assert_eq!(configured.load(Ordering::SeqCst), 1);
    assert!(!queried.load(Ordering::SeqCst));
}
