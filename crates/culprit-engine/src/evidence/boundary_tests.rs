use super::*;
use crate::recovery_config::RecoveryConfig;
use culprit_auth::{BearerTokenProvider, CredentialResolver, DatabaseTokenProvider, PasswordCredential};
use culprit_core::telemetry::CollectionStop;
use culprit_evidence::{
    analytical::{
        ProjectionCatalog,
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
        test_support::{self, InstanceResourceCase, ReadOnlyFixture},
    },
    delivery::{object_store::transport::ObjectReadTransport, recovery::CommittedRange},
};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use zeroize::Zeroizing;
struct NoToken;
#[async_trait::async_trait]
impl BearerTokenProvider for NoToken {
    async fn bearer_token(&self, _: &str) -> Result<Zeroizing<String>> {
        panic!("the fake read stores never acquire a token")
    }
}
#[derive(Default)]
struct Credentials(Mutex<Vec<String>>);
impl CredentialResolver for Credentials {
    fn bearer(&self, profile: &str) -> Result<Arc<dyn BearerTokenProvider>> {
        self.0.lock().unwrap().push(profile.into());
        Ok(Arc::new(NoToken))
    }
    fn database(&self, _: &str) -> Result<Arc<dyn DatabaseTokenProvider>> {
        panic!("archive reads cannot resolve database credentials")
    }
    fn validate_password(&self, _: &str) -> Result<()> {
        panic!("unexpected credential role")
    }
    fn password(&self, _: &str) -> Result<PasswordCredential> {
        panic!("unexpected credential role")
    }
}
struct Recovery(Arc<dyn ObjectReadTransport>);
impl EvidenceRecoveryProvider for Recovery {
    fn name(&self) -> &str {
        "read-recovery"
    }
    fn configure(
        &self,
        _: &serde_json::Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        credentials.bearer(profile)?;
        Ok(self.0.clone())
    }
}
struct Store(Arc<dyn AnalyticalReadStore>);
impl EvidenceStoreProvider for Store {
    fn name(&self) -> &str {
        "read-store"
    }
    fn configure(
        &self,
        _: &serde_json::Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn AnalyticalReadStore>> {
        credentials.bearer(profile)?;
        Ok(self.0.clone())
    }
}
fn configured_read_service(
    fixture: &ReadOnlyFixture,
    store: Arc<dyn AnalyticalReadStore>,
) -> (EvidenceService, Arc<Credentials>) {
    let credentials = Arc::new(Credentials::default());
    let runtime = EvidenceRuntime::empty()
        .with_credentials(credentials.clone())
        .with_recovery_provider(Arc::new(Recovery(fixture.transport())))
        .unwrap()
        .with_evidence_store_provider(Arc::new(Store(store)))
        .unwrap();
    let config = EvidenceConfig {
        pipeline: "pipeline".into(),
        shard: "shard-0".into(),
        credentials: BTreeMap::new(),
        recovery: RecoveryConfig {
            provider: "read-recovery".into(),
            options: serde_json::json!({}),
            credential: "archive-identity".into(),
        },
        warehouse: EvidenceStoreConfig {
            provider: "read-store".into(),
            options: serde_json::json!({}),
            credential: "warehouse-identity".into(),
        },
    };
    let service = runtime.evidence(&config).unwrap();
    (service, credentials)
}

#[tokio::test]
async fn independent_read_runtime_uses_named_credentials_without_a_collector_or_source_factory() {
    let (source, rows) = test_support::instance_resource_evidence(InstanceResourceCase::Exact);
    let fixture = ReadOnlyFixture::capture(source.clone(), rows).await;
    let (service, credentials) = configured_read_service(&fixture, fixture.store.clone());
    assert_eq!(*credentials.0.lock().unwrap(), ["archive-identity", "warehouse-identity"]);
    let request = EvidenceRequest {
        range_id: fixture.manifest.range_id.clone(),
        families: vec![EvidenceFamily::OperationalSamples],
        budget: EvidenceReadBudget::default(),
    };
    let capture = service.read(&request, &CollectionCancellation::default()).await.unwrap();
    assert_eq!(capture.source(), &source);
    assert_eq!(capture.capture(), &fixture.manifest.capture);
    assert!(capture.is_read_complete());
    let cancelled = CollectionCancellation::default();
    cancelled.cancel();
    assert!(service.read(&request, &cancelled).await.is_err());
}

#[test]
fn concrete_read_service_future_and_authenticated_output_have_bounded_storage() {
    use std::{future::Future, mem::size_of};
    fn future_bytes<A, F, T>(_: F) -> usize
    where
        F: FnOnce(A) -> T,
        T: Future,
    {
        size_of::<T>()
    }
    // The closure is never invoked: this measures the actual read specialization, not an erased Box/dyn handle.
    let bytes = future_bytes(
        |(service, request, cancellation): (
            &'static EvidenceService,
            &'static EvidenceRequest,
            &'static CollectionCancellation,
        )| service.read(request, cancellation),
    );
    assert!(bytes <= 64 * 1024, "concrete EvidenceService::read future: {bytes} bytes");
    assert!(size_of::<Result<EvidenceCapture>>() <= 16 * 1024);
}

// Delegate all proof/data reads to the existing fixture; only signal timing changes after the final response.
struct CancelFinalRows {
    inner: Arc<dyn AnalyticalReadStore>,
    cancellation: CollectionCancellation,
    final_batch: String,
    final_row: usize,
    fail_after_response: bool,
    queries: AtomicUsize,
    row_responses: AtomicUsize,
    active_rows: AtomicUsize,
}
struct ActiveRow<'a>(&'a AtomicUsize);
impl Drop for ActiveRow<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl AnalyticalReadStore for CancelFinalRows {
    fn destination(&self) -> &str {
        self.inner.destination()
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.inner.catalog()
    }
    fn logical_values(
        &self,
        stream: culprit_core::telemetry::TelemetryStream,
        row: &[serde_json::Value],
    ) -> Result<serde_json::Map<String, serde_json::Value>> {
        self.inner.logical_values(stream, row)
    }
    async fn audit_membership(&self, range: &CommittedRange, byte_limit: usize) -> Result<usize> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.inner.audit_membership(range, byte_limit).await
    }
    async fn rows(
        &self,
        range: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        byte_limit: usize,
    ) -> Result<AnalyticalReadPage> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.active_rows.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveRow(&self.active_rows);
        let page = self.inner.rows(range, batch, after, until, byte_limit).await?;
        self.row_responses.fetch_add(1, Ordering::SeqCst);
        if batch == self.final_batch && until == self.final_row {
            self.cancellation.cancel();
            if self.fail_after_response {
                return Err(DeliveryProblem::Integrity.error());
            }
        }
        Ok(page)
    }
    async fn publication_rows(&self, range: &CommittedRange, byte_limit: usize) -> Result<AnalyticalReadPage> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.inner.publication_rows(range, byte_limit).await
    }
}
async fn cancel_completed_rows(fail_after_response: bool) -> culprit_core::Error {
    let (source, rows) = test_support::instance_resource_evidence(InstanceResourceCase::Exact);
    let fixture = ReadOnlyFixture::capture(source, rows).await;
    let batch = fixture
        .manifest
        .batches
        .iter()
        .find(|batch| batch.stream == EvidenceFamily::OperationalSamples.stream())
        .unwrap();
    assert!(!batch.rows.is_empty());
    let cancellation = CollectionCancellation::default();
    let store = Arc::new(CancelFinalRows {
        inner: fixture.store.clone(),
        cancellation: cancellation.clone(),
        final_batch: batch.batch_id.clone(),
        final_row: batch.rows.len(),
        fail_after_response,
        queries: AtomicUsize::new(0),
        row_responses: AtomicUsize::new(0),
        active_rows: AtomicUsize::new(0),
    });
    let (service, credentials) = configured_read_service(&fixture, store.clone());
    let mut session = service.session(EvidenceReadBudget::default(), &cancellation).unwrap();
    let error = session.read(&fixture.manifest.range_id, &[EvidenceFamily::OperationalSamples]).await.unwrap_err();
    assert!(cancellation.is_cancelled());
    assert!(store.row_responses.load(Ordering::SeqCst) > 0, "the original final data response really completed");
    assert_eq!(store.active_rows.load(Ordering::SeqCst), 0, "the same started row owner settled");
    let queries = store.queries.load(Ordering::SeqCst);
    assert!(queries >= 3, "publication, membership and native rows all use the canonical reader");
    let refused = session.read(&fixture.manifest.range_id, &[EvidenceFamily::OperationalSamples]).await.unwrap_err();
    assert_eq!(
        refused.detail,
        Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Capacity)),
        "the consumed action did not regain its read allowance"
    );
    assert_eq!(store.queries.load(Ordering::SeqCst), queries, "a failed action cannot retry or issue another query");
    assert_eq!(*credentials.0.lock().unwrap(), ["archive-identity", "warehouse-identity"]);
    error
}
#[tokio::test]
async fn cancellation_from_the_completed_final_canonical_row_refuses_success_and_does_not_renew_the_session() {
    let error = cancel_completed_rows(false).await;
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
}
#[tokio::test]
async fn a_real_final_row_failure_precedes_simultaneous_cancellation_and_keeps_the_session_consumed() {
    let error = cancel_completed_rows(true).await;
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)));
}
