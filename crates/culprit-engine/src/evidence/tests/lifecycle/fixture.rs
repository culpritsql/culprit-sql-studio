use super::*;
use crate::evidence::EvidenceRecoveryProvider;
use async_trait::async_trait;
use culprit_auth::CredentialResolver;
use culprit_collector::analytical::encoding::BatchEncoder;
use culprit_collector::delivery::object_store::ObjectRecoveryStore;
use culprit_collector::delivery::recovery::RecoveryStore;
use culprit_evidence::analytical::catalog::PhysicalCatalog;
use culprit_evidence::analytical::publication;
use culprit_evidence::analytical::read_store::AnalyticalReadPage;
use culprit_evidence::analytical::read_store::AnalyticalReadStore;
use culprit_evidence::analytical::test_support::JsonProjection;
use culprit_evidence::analytical::test_support::Recovery;
use culprit_evidence::analytical::{ProjectionCatalog, RowProjection};
use culprit_evidence::delivery::object_store::transport::ObjectReadTransport;
use culprit_evidence::delivery::object_store::transport::*;
use culprit_evidence::delivery::recovery::CommittedRange;
use culprit_evidence::delivery::recovery::PayloadObject;
use culprit_evidence::delivery::recovery::reopen;
use culprit_evidence::delivery::{mutations::RecoveryWrite as _, recovery::RecoveryRead as _};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::Notify,
};

#[derive(Default)]
pub(in crate::evidence::tests) struct Objects {
    entries: Mutex<BTreeMap<String, (Vec<u8>, String)>>,
    revision: AtomicUsize,
    readonly: AtomicBool,
    pub(in crate::evidence::tests) reads: Mutex<Vec<String>>,
    pub(in crate::evidence::tests) stall: AtomicBool,
    pub(in crate::evidence::tests) allow_payloads: AtomicBool,
    pub(in crate::evidence::tests) stall_payload_body: AtomicBool,
    pub(in crate::evidence::tests) payload_started: Arc<Notify>,
    pub(in crate::evidence::tests) active: Arc<AtomicUsize>,
}
impl Objects {
    pub(in crate::evidence::tests) fn snapshot(&self) -> BTreeMap<String, (Vec<u8>, String)> {
        self.entries.lock().unwrap().clone()
    }
    pub(in crate::evidence::tests) fn remove_proofs(&self) {
        self.entries.lock().unwrap().retain(|key, _| !key.starts_with("validation/"));
    }
    pub(in crate::evidence::tests) fn remove_history(&self) {
        self.entries.lock().unwrap().clear();
    }
    pub(in crate::evidence::tests) fn corrupt_manifest(&self) {
        let mut entries = self.entries.lock().unwrap();
        let (_, (bytes, _)) = entries.iter_mut().find(|(key, _)| key.starts_with("manifests/")).unwrap();
        bytes.push(b' ');
    }
}
struct PendingRead<'a>(&'a AtomicUsize);
impl Drop for PendingRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct StalledPayload {
    active: Arc<AtomicUsize>,
    started: Arc<Notify>,
    reading: bool,
}
impl AsyncRead for StalledPayload {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if !self.reading {
            self.reading = true;
            self.active.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
        }
        Poll::Pending
    }
}
impl Drop for StalledPayload {
    fn drop(&mut self) {
        if self.reading {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
#[async_trait]
impl ObjectTransport for Objects {
    async fn put(&self, key: &str, bytes: Vec<u8>, condition: WriteCondition<'_>) -> Result<ObjectWrite> {
        assert!(!self.readonly.load(Ordering::SeqCst), "archive investigation attempted to mutate recovery history");
        let mut entries = self.entries.lock().unwrap();
        let matches = match condition {
            WriteCondition::Absent => !entries.contains_key(key),
            WriteCondition::Matches(expected) => entries.get(key).is_some_and(|(_, etag)| etag == expected),
        };
        if !matches {
            return Ok(ObjectWrite::ConditionNotMet);
        }
        let etag = self.revision.fetch_add(1, Ordering::SeqCst).to_string();
        entries.insert(key.into(), (bytes, etag));
        Ok(ObjectWrite::Written)
    }
    async fn put_file(&self, key: &str, path: &Path, bytes: u64) -> Result<ObjectWrite> {
        let content = std::fs::read(path).unwrap();
        assert_eq!(content.len() as u64, bytes);
        self.put(key, content, WriteCondition::Absent).await
    }
}
#[async_trait]
impl culprit_evidence::delivery::object_store::transport::ObjectReadTransport for Objects {
    async fn get(&self, key: &str) -> Result<Option<ObjectRead>> {
        self.reads.lock().unwrap().push(key.into());
        if self.readonly.load(Ordering::SeqCst) {
            assert!(
                !key.starts_with("payloads/") || self.allow_payloads.load(Ordering::SeqCst),
                "investigation must not reread raw native payloads without opt-in"
            );
        }
        if self.stall.load(Ordering::SeqCst) {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _pending = PendingRead(&self.active);
            std::future::pending::<()>().await;
        }
        Ok(self.entries.lock().unwrap().get(key).map(|(bytes, etag)| ObjectRead {
            etag: etag.clone(),
            bytes: bytes.len() as u64,
            body: if key.starts_with("payloads/") && self.stall_payload_body.load(Ordering::SeqCst) {
                Box::pin(StalledPayload {
                    active: self.active.clone(),
                    started: self.payload_started.clone(),
                    reading: false,
                })
            } else {
                Box::pin(std::io::Cursor::new(bytes.clone()))
            },
        }))
    }
}

pub(in crate::evidence::tests) struct Store {
    catalog: Arc<PhysicalCatalog<JsonProjection>>,
    rows: Mutex<BTreeMap<String, Vec<Vec<Value>>>>,
    additional_publications: Mutex<BTreeMap<String, Option<Vec<Value>>>>,
    candidate_pages: Mutex<VecDeque<Vec<Vec<Value>>>>,
    pub(in crate::evidence::tests) candidate_requests:
        Mutex<Vec<culprit_evidence::analytical::evidence::IncidentDiscoveryRequest>>,
    pub(in crate::evidence::tests) publication: Mutex<Option<Vec<Value>>>,
    pub(in crate::evidence::tests) requests: AtomicUsize,
    pub(in crate::evidence::tests) denied: AtomicBool,
}
#[async_trait]
impl AnalyticalReadStore for Store {
    fn destination(&self) -> &str {
        "fixture:archive"
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.catalog.clone()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.catalog.get(stream)?.logical_values(row)
    }
    async fn incident_candidates(
        &self,
        _: &culprit_evidence::delivery::contract::Contract,
        request: &culprit_evidence::analytical::evidence::IncidentDiscoveryRequest,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.candidate_requests.lock().unwrap().push(request.clone());
        let rows = self.candidate_pages.lock().unwrap().pop_front().expect("unexpected discovery query");
        Ok(AnalyticalReadPage { response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows })
    }
    async fn audit_membership(&self, _: &CommittedRange, _: usize) -> Result<usize> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(3)
    }
    async fn rows(
        &self,
        range: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let stream = range.manifest().batches.iter().find(|candidate| candidate.batch_id == batch).unwrap().stream;
        let column = self.catalog.get(stream)?.columns().iter().position(|column| column.name == "RowOrdinal").unwrap();
        let rows = self.rows.lock().unwrap()[batch]
            .iter()
            .filter(|row| row[column].as_u64().is_some_and(|ordinal| ordinal > after as u64 && ordinal <= until as u64))
            .cloned()
            .collect::<Vec<_>>();
        Ok(AnalyticalReadPage { response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows })
    }
    async fn publication_rows(&self, range: &CommittedRange, _: usize) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if self.denied.load(Ordering::SeqCst) {
            return Err(culprit_core::Error::new(culprit_core::ErrorCode::PermissionDenied, "owned fixture denied"));
        }
        let rows = self
            .additional_publications
            .lock()
            .unwrap()
            .get(&range.manifest().range_id)
            .cloned()
            .unwrap_or_else(|| self.publication.lock().unwrap().clone())
            .into_iter()
            .collect::<Vec<_>>();
        Ok(AnalyticalReadPage { response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows })
    }
}
impl Store {
    pub(in crate::evidence::tests) fn replace_publication(&self, field: &str, value: Value) {
        let schema = self.catalog.publication();
        let index = schema.columns().iter().position(|column| column.name == field).unwrap();
        self.publication.lock().unwrap().as_mut().unwrap()[index] = value;
    }
    pub(in crate::evidence::tests) fn omit_first_row(&self) {
        self.rows.lock().unwrap().values_mut().next().unwrap().remove(0);
    }
    pub(in crate::evidence::tests) fn corrupt_row(&self) {
        let schema = self.catalog.get(TelemetryStream::OperationalSamples).unwrap();
        let column = schema.columns().iter().position(|column| column.name == "FactKind").unwrap();
        self.rows.lock().unwrap().values_mut().next().unwrap()[0][column] = serde_json::json!("unproven-fact");
    }
}
struct Archive(Arc<Objects>);
impl EvidenceRecoveryProvider for Archive {
    fn name(&self) -> &str {
        "fixture-recovery"
    }
    fn configure(
        &self,
        _: &Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn ObjectReadTransport>> {
        credentials.bearer(profile)?;
        Ok(self.0.clone())
    }
}
struct Warehouse(Arc<Store>);
impl EvidenceStoreProvider for Warehouse {
    fn name(&self) -> &str {
        "fixture-warehouse"
    }
    fn configure(
        &self,
        _: &Value,
        profile: &str,
        credentials: &dyn CredentialResolver,
    ) -> Result<Arc<dyn AnalyticalReadStore>> {
        credentials.bearer(profile)?;
        Ok(self.0.clone())
    }
}

pub(in crate::evidence::tests) struct Fixture {
    pub(in crate::evidence::tests) objects: Arc<Objects>,
    pub(in crate::evidence::tests) store: Arc<Store>,
    pub(in crate::evidence::tests) service: EvidenceService,
    pub(in crate::evidence::tests) request: EvidenceRequest,
    pub(in crate::evidence::tests) config: EvidenceConfig,
    pub(in crate::evidence::tests) runtime: EvidenceRuntime,
    pub(in crate::evidence::tests) profiles: Arc<super::super::profiles::Profiles>,
    /// Authenticated conservative row-planning bound, separate from bytes actually returned by the transport.
    pub(in crate::evidence::tests) largest_row_response_bytes: u32,
}
impl Fixture {
    pub(in crate::evidence::tests) async fn new() -> Self {
        let (source, records) = culprit_evidence::analytical::test_support::memory_failure_evidence();
        Self::capture(source, records).await
    }
    pub(in crate::evidence::tests) async fn capture(
        source: CollectionDescription,
        records: Vec<TelemetryRecord>,
    ) -> Self {
        Self::capture_with_read_metrics(source, records, 0, None).await
    }
    pub(in crate::evidence::tests) async fn capture_with_read_metrics(
        source: CollectionDescription,
        records: Vec<TelemetryRecord>,
        source_queries: u32,
        read_metrics: Option<ReadWorkMetrics>,
    ) -> Self {
        let objects = Arc::new(Objects::default());
        let catalog = Arc::new(PhysicalCatalog::<JsonProjection>::new().unwrap());
        let mut native = Recovery::capture("fixture:archive", catalog.digest(), source, records, 100_000);
        native.manifest.source_queries = source_queries;
        native.manifest.read_metrics = read_metrics;
        let archive =
            Arc::new(ObjectRecoveryStore::create(objects.clone(), native.manifest.contract.clone()).await.unwrap());
        let material = archive_capture(objects.clone(), catalog.clone(), archive, native).await;
        let manifest = material.range.manifest().clone();
        let largest_row_response_bytes = material.largest_row_response_bytes;
        let store = Arc::new(Store {
            catalog,
            rows: Mutex::new(material.rows),
            publication: Mutex::new(Some(material.publication)),
            additional_publications: Mutex::new(BTreeMap::new()),
            candidate_pages: Mutex::new(VecDeque::new()),
            candidate_requests: Mutex::new(vec![]),
            requests: AtomicUsize::new(0),
            denied: AtomicBool::new(false),
        });
        let profiles = Arc::new(super::super::profiles::Profiles::default());
        let runtime = EvidenceRuntime::empty()
            .with_credentials(profiles.clone())
            .with_recovery_provider(Arc::new(Archive(objects.clone())))
            .unwrap()
            .with_evidence_store_provider(Arc::new(Warehouse(store.clone())))
            .unwrap();
        let config = EvidenceConfig {
            pipeline: "pipeline".into(),
            shard: "shard-0".into(),
            credentials: BTreeMap::new(),
            recovery: crate::recovery_config::RecoveryConfig {
                provider: "fixture-recovery".into(),
                options: serde_json::json!({}),
                credential: "archive-reader".into(),
            },
            warehouse: EvidenceStoreConfig {
                provider: "fixture-warehouse".into(),
                options: serde_json::json!({}),
                credential: "warehouse-reader".into(),
            },
        };
        let service = runtime.evidence(&config).unwrap();
        let request = EvidenceRequest {
            range_id: manifest.range_id,
            families: vec![EvidenceFamily::OperationalSamples],
            budget: EvidenceReadBudget { page_rows: 1, ..Default::default() },
        };
        objects.readonly.store(true, Ordering::SeqCst);
        objects.reads.lock().unwrap().clear();
        Self { objects, store, service, request, config, runtime, profiles, largest_row_response_bytes }
    }
    /// The same genuine stage/commit/encode owner adds a distinct immutable capture to this test archive.
    pub(in crate::evidence::tests) async fn append_capture(
        &self,
        source: CollectionDescription,
        rows: Vec<TelemetryRecord>,
        sequence: u64,
    ) -> CommittedRange {
        self.objects.readonly.store(false, Ordering::SeqCst);
        let native = Recovery::try_capture("fixture:archive", self.store.catalog.digest(), source, rows, sequence)
            .unwrap_or_else(|error| panic!("archive capture {sequence}: {error:?}"));
        let archive =
            Arc::new(ObjectRecoveryStore::open(self.objects.clone(), native.manifest.contract.clone()).await.unwrap());
        let material = archive_capture(self.objects.clone(), self.store.catalog.clone(), archive, native).await;
        let range = material.range;
        let mut existing = self.store.rows.lock().unwrap();
        for (batch, rows) in material.rows {
            assert!(existing.insert(batch, rows).is_none());
        }
        self.store
            .additional_publications
            .lock()
            .unwrap()
            .insert(range.manifest().range_id.clone(), Some(material.publication));
        self.objects.readonly.store(true, Ordering::SeqCst);
        range
    }
    pub(in crate::evidence::tests) fn rows_for(
        &self,
        range: &CommittedRange,
        stream: TelemetryStream,
    ) -> Vec<Vec<Value>> {
        let batch = range.manifest().batches.iter().find(|batch| batch.stream == stream).unwrap();
        self.store.rows.lock().unwrap()[&batch.batch_id].clone()
    }
    pub(in crate::evidence::tests) fn unpublish(&self, range: &CommittedRange) {
        self.store.additional_publications.lock().unwrap().insert(range.manifest().range_id.clone(), None);
    }
    pub(in crate::evidence::tests) fn omit_row(
        &self,
        range: &CommittedRange,
        stream: TelemetryStream,
        position: usize,
    ) {
        let batch = range.manifest().batches.iter().find(|batch| batch.stream == stream).unwrap();
        self.store.rows.lock().unwrap().get_mut(&batch.batch_id).unwrap().remove(position);
    }
    pub(in crate::evidence::tests) fn candidate_pages(&self, pages: Vec<Vec<Vec<Value>>>) {
        self.store.candidate_pages.lock().unwrap().extend(pages);
    }
}
struct ArchiveMaterial {
    range: CommittedRange,
    rows: BTreeMap<String, Vec<Vec<Value>>>,
    publication: Vec<Value>,
    largest_row_response_bytes: u32,
}
async fn archive_capture(
    objects: Arc<Objects>,
    catalog: Arc<PhysicalCatalog<JsonProjection>>,
    archive: Arc<ObjectRecoveryStore>,
    native: Recovery,
) -> ArchiveMaterial {
    let manifest = native.manifest.clone();
    let fence = archive.acquire().await.unwrap();
    let expected = archive.checkpoint_head().await.unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut staged = Vec::new();
    for batch in &manifest.batches {
        let payload = PayloadObject::from(batch);
        let mut input = native.open_object(&payload).await.unwrap();
        let mut bytes = Vec::new();
        input.read_to_end(&mut bytes).await.unwrap();
        let path = scratch.path().join(&batch.batch_id);
        std::fs::write(&path, &bytes).unwrap();
        staged.push(archive.stage(&payload, &path).await.unwrap());
    }
    archive.commit(&fence, expected.as_deref(), &manifest, &staged).await.unwrap();
    let range = reopen(archive.as_ref(), &manifest.range_id).await.unwrap();
    let indexes = culprit_evidence::analytical::indexes::ProjectionIndexWriter::new(objects);
    let encoder = BatchEncoder::new(catalog.clone(), archive, scratch.path()).unwrap();
    let mut rows = BTreeMap::new();
    let mut largest_row_response_bytes = 0;
    for batch in manifest.batches.iter().filter(|batch| batch.delivery_required) {
        let encoded = encoder.encode(&range, &batch.batch_id).await.unwrap();
        largest_row_response_bytes =
            largest_row_response_bytes.max(encoded.digests.iter().map(|row| row.response_bytes).max().unwrap_or(0));
        indexes.put(&encoded.index_key, &encoded.digests).await.unwrap();
        rows.insert(
            batch.batch_id.clone(),
            std::fs::read_to_string(encoded.path())
                .unwrap()
                .lines()
                .map(|line| physical_row(catalog.get(batch.stream).unwrap(), line.as_bytes()))
                .collect(),
        );
    }
    let schema = catalog.publication();
    let values = schema.project(&publication::input(catalog.as_ref(), &range).unwrap()).unwrap();
    let publication = physical_row(schema, &schema.encode(&values).unwrap());
    ArchiveMaterial { range, rows, publication, largest_row_response_bytes }
}

fn physical_row(schema: &JsonProjection, bytes: &[u8]) -> Vec<Value> {
    let values: Map<String, Value> = serde_json::from_slice(bytes).unwrap();
    schema.columns().iter().map(|column| values[&column.name].clone()).collect()
}
