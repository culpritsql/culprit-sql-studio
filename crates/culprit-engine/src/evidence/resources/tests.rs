use super::*;
use culprit_evidence::{
    analytical::{
        ProjectionCatalog,
        indexes::ProjectionIndexes,
        publication,
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
        test_support::{self, Blobs, ReadOnlyFixture},
    },
    delivery::{
        contract::Contract,
        object_store::{
            state::{CommitIndex, Root},
            transport::{ObjectRead, ObjectReadTransport},
        },
        recovery::CommittedRange,
    },
};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    NoPublication,
    ForeignNamespace,
    TamperedHint,
    Empty,
    CancelFinal,
    LateFinal,
    RoundedClock,
}
struct Publications {
    catalog: Arc<dyn ProjectionCatalog>,
    rows: BTreeMap<String, Vec<Value>>,
    requests: AtomicUsize,
    fault: Mutex<Fault>,
    cancellation: CollectionCancellation,
}
impl Publications {
    fn values(&self, row: &[Value]) -> Map<String, Value> {
        self.catalog.publication().logical_values(row).unwrap()
    }
    fn position(&self, row: &[Value]) -> PublishedResourcePosition {
        let values = self.values(row);
        PublishedResourcePosition {
            resource_id: values["ResourceId"].as_str().unwrap().into(),
            source_id: values["SourceId"].as_str().unwrap().into(),
            range_id: values["RangeId"].as_str().unwrap().into(),
        }
    }
    fn page(rows: Vec<Vec<Value>>) -> AnalyticalReadPage {
        AnalyticalReadPage { response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows }
    }
}
#[async_trait::async_trait]
impl AnalyticalReadStore for Publications {
    fn destination(&self) -> &str {
        "boundary:warehouse"
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.catalog.clone()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.catalog.schema(stream)?.logical_values(row)
    }
    async fn audit_membership(&self, _: &CommittedRange, _: usize) -> Result<usize> {
        panic!("discovery cannot read native bodies")
    }
    async fn rows(&self, _: &CommittedRange, _: &str, _: usize, _: usize, _: usize) -> Result<AnalyticalReadPage> {
        panic!("discovery cannot read native bodies")
    }
    async fn resource_candidates(
        &self,
        contract: &Contract,
        request: &PublishedResourceQuery,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        request.validate(contract)?;
        let fault = *self.fault.lock().unwrap();
        if matches!(fault, Fault::Empty) {
            return Ok(Self::page(vec![]));
        }
        let mut rows = self
            .rows
            .values()
            .filter(|row| {
                let values = self.values(row);
                values["RecoveryStoreId"].as_str() == Some(request.recovery_store_id.as_str())
                    && (matches!(fault, Fault::RoundedClock)
                        || request.selection.accepts_acquisition(
                            values["CollectedFrom"].as_str().unwrap(),
                            values["CollectedUntil"].as_str().unwrap(),
                        ))
                    && request.selection.after.as_ref().is_none_or(|cursor| self.position(row) > cursor.position)
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| self.position(row));
        rows.truncate(request.selection.page_rows as usize + 1);
        if matches!(fault, Fault::RoundedClock) {
            let column =
                self.catalog.publication().columns().iter().position(|column| column.name == "CollectedUntil").unwrap();
            for row in &mut rows {
                row[column] = json!(request.selection.as_of);
            }
        }
        if matches!(fault, Fault::ForeignNamespace | Fault::TamperedHint) {
            let field = if matches!(fault, Fault::ForeignNamespace) { "RecoveryStoreId" } else { "ManifestSha256" };
            let column = self.catalog.publication().columns().iter().position(|column| column.name == field).unwrap();
            for row in &mut rows {
                row[column] = json!(digest(b"foreign or modified candidate"));
            }
        }
        Ok(Self::page(rows))
    }
    async fn incident_candidates(
        &self,
        _: &Contract,
        _: &culprit_evidence::analytical::evidence::IncidentDiscoveryRequest,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(Self::page(vec![]))
    }
    async fn publication_rows(&self, range: &CommittedRange, _: usize) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let fault = *self.fault.lock().unwrap();
        if matches!(fault, Fault::LateFinal) {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        if matches!(fault, Fault::CancelFinal) {
            self.cancellation.cancel();
        }
        Ok(Self::page(if matches!(fault, Fault::NoPublication) {
            vec![]
        } else {
            vec![self.rows[&range.manifest().range_id].clone()]
        }))
    }
}
struct Archive {
    service: EvidenceService,
    store: Arc<Publications>,
    objects: Arc<Blobs>,
    manifests: Vec<culprit_evidence::delivery::evidence::RangeManifest>,
}
impl Archive {
    /// All originals are canonical fixture captures. Only committed root/index ownership is assembled here,
    /// using the existing exact object reader and publication projector; no substitute decoder or authority.
    async fn new(cases: Vec<(CollectionDescription, Vec<TelemetryRecord>)>) -> Self {
        Self::new_at(cases, "2026-11-22T10:00:00.123456789Z", "2026-11-22T10:00:01.123456789Z").await
    }
    async fn new_at(
        cases: Vec<(CollectionDescription, Vec<TelemetryRecord>)>,
        collected_from: &str,
        collected_until: &str,
    ) -> Self {
        let mut fixtures = Vec::new();
        for (i, (source, rows)) in cases.into_iter().enumerate() {
            fixtures.push(
                ReadOnlyFixture::capture_at(source, rows, 800_001 + i as u64, collected_from, collected_until).await,
            );
        }
        let contract = fixtures[0].contract.clone();
        let catalog = fixtures[0].store.catalog();
        let objects = Arc::new(Blobs::default());
        let mut root = Root::new(contract.clone()).unwrap();
        root.epoch = 1;
        root.token = Some("owned-resource-fixture".into());
        let mut manifests = Vec::new();
        let mut previous = None;
        {
            let mut stored = objects.objects.lock().unwrap();
            for (i, fixture) in fixtures.into_iter().enumerate() {
                let manifest = fixture.manifest;
                let sequence = manifest.capture.revision.as_ref().unwrap().sequence.parse().unwrap();
                let index = CommitIndex {
                    store_id: root.store_id.clone(),
                    order: i as u64 + 1,
                    range_id: manifest.range_id.clone(),
                    manifest_digest: manifest.digest().unwrap(),
                    high_sequence: sequence,
                    previous: previous.clone(),
                };
                index.validate(&root.store_id).unwrap();
                previous = Some(index.range_id.clone());
                // Body/projection objects stay byte-identical, although metadata discovery never opens them.
                stored.extend(
                    fixture
                        .objects
                        .objects
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(key, _)| key.starts_with("payloads/"))
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
                stored.insert(index.manifest_key(), serde_json::to_vec(&manifest).unwrap());
                stored.insert(index.range_key(), serde_json::to_vec(&index).unwrap());
                stored.insert(index.order_key(), serde_json::to_vec(&index).unwrap());
                root.high_sequence = sequence;
                root.indexed_order = index.order;
                root.head = Some(index);
                manifests.push(manifest);
            }
            root.validate(&contract).unwrap();
            stored.insert("recovery.json".into(), serde_json::to_vec(&root).unwrap());
        }
        let recovery = ObjectRecoveryReader::open(objects.clone(), contract.clone()).await.unwrap();
        let mut rows = BTreeMap::new();
        for manifest in &manifests {
            let range = reopen(&recovery, &manifest.range_id).await.unwrap();
            let schema = catalog.publication();
            let values = schema.project(&publication::input(catalog.as_ref(), &range).unwrap()).unwrap();
            let encoded: Map<String, Value> = serde_json::from_slice(&schema.encode(&values).unwrap()).unwrap();
            rows.insert(
                manifest.range_id.clone(),
                schema.columns().iter().map(|column| encoded[&column.name].clone()).collect(),
            );
        }
        let store = Arc::new(Publications {
            catalog,
            rows,
            requests: AtomicUsize::new(0),
            fault: Mutex::new(Fault::None),
            cancellation: CollectionCancellation::default(),
        });
        let service = EvidenceService {
            recovery: objects.clone(),
            contract,
            reader: culprit_evidence::analytical::evidence::EvidenceReader::new(
                store.clone(),
                Arc::new(ProjectionIndexes::new(objects.clone())),
            ),
        };
        Self { service, store, objects, manifests }
    }
    async fn single() -> Self {
        Self::new(vec![test_support::informational_query_evidence()]).await
    }
    async fn read(&self) -> Result<PublishedResources> {
        self.service.resources(&request(), EvidenceReadBudget::default(), &self.store.cancellation).await
    }
}
fn request() -> PublishedResourceRequest {
    PublishedResourceRequest { as_of: "2026-11-22T12:00:00Z".into(), window: None, after: None, page_rows: 2 }
}

#[tokio::test]
async fn resources_paginate_real_originals_without_merging_native_members_epochs_or_incarnations() {
    let mut cases = test_support::replica_activity_evidence_cases();
    cases.truncate(3);
    let mut new_incarnation = cases[0].clone();
    let CollectionTarget::Database(database) = &mut new_incarnation.0.target else { panic!() };
    database.incarnation = Some("private-native-database-incarnation".into());
    cases.push(new_incarnation);
    let archive = Archive::new(cases).await;
    let cancel = CollectionCancellation::default();
    let mut session = archive.service.session(EvidenceReadBudget::default(), &cancel).unwrap();
    let mut selected = request();
    let first = session.resources(&selected).await.unwrap();
    assert_eq!(first.resources.len(), 2);
    assert_eq!(first.candidate_rows, 3);
    assert!(!first.discovery_complete);
    assert_eq!(first.recovery_metadata_reads, 7);
    assert_eq!(first.archive_work.queries, 3);
    assert!(first.archive_work.proof_bytes > 0);
    assert_eq!(first.archive_work.recovery_objects, 0);
    selected.after = first.next.clone();
    let second = session.resources(&selected).await.unwrap();
    assert_eq!(second.resources.len(), 2);
    assert!(second.next.is_none() && second.discovery_complete);
    let all = first.resources.into_iter().chain(second.resources).collect::<Vec<_>>();
    let source_ids = all.iter().map(|resource| &resource.position.source_id).collect::<std::collections::BTreeSet<_>>();
    let resource_ids =
        all.iter().map(|resource| &resource.position.resource_id).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(source_ids.len(), 4);
    assert_eq!(resource_ids.len(), 2);
    assert!(all.iter().all(|resource| resource.connection_alias.is_none()));
    let exposed = serde_json::to_string(&all).unwrap();
    for private in [
        "private-compute-a",
        "private-compute-b",
        "start-a",
        "private-native-database-incarnation",
        "private-compute-namespace",
    ] {
        assert!(!exposed.contains(private), "{private}");
    }
    assert!(all.iter().all(|resource| matches!(resource.coverage, PublishedResourceCoverage::Activity { .. })));
}
#[tokio::test]
async fn absent_publication_or_committed_original_does_not_expose_identity_or_mean_no_database() {
    let archive = Archive::single().await;
    *archive.store.fault.lock().unwrap() = Fault::NoPublication;
    let result = archive.read().await.unwrap();
    assert!(result.resources.is_empty());
    assert!(!result.discovery_complete);
    assert_eq!(result.gaps, vec![PublishedResourceGap::NotPublished]);
    // Remove both authoritative lookup alternatives. The candidate remains a hint, not proof of existence/absence.
    let mut root: Root = serde_json::from_slice(&archive.objects.objects.lock().unwrap()["recovery.json"]).unwrap();
    root.head = None;
    root.high_sequence = 0;
    root.indexed_order = 0;
    {
        let mut stored = archive.objects.objects.lock().unwrap();
        stored.remove(&format!("ranges/{}.json", archive.manifests[0].range_id));
        stored.insert("recovery.json".into(), serde_json::to_vec(&root).unwrap());
    }
    *archive.store.fault.lock().unwrap() = Fault::None;
    let result = archive.read().await.unwrap();
    assert!(result.resources.is_empty());
    assert_eq!(result.gaps, vec![PublishedResourceGap::OriginalUnavailable]);
    *archive.store.fault.lock().unwrap() = Fault::Empty;
    let empty = archive.read().await.unwrap();
    assert!(empty.resources.is_empty() && empty.discovery_complete);
    assert_eq!(empty.candidate_rows, 0);
    assert_eq!(empty.recovery_metadata_reads, 1);
    archive.objects.objects.lock().unwrap().remove("recovery.json");
    archive.store.requests.store(0, Ordering::SeqCst);
    assert_eq!(archive.read().await.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::NotReady)));
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0, "an unavailable namespace is not an empty inventory");
}
#[tokio::test]
async fn foreign_namespace_modified_pointer_and_original_manifest_tampering_refuse_instead_of_returning_labels() {
    for fault in [Fault::ForeignNamespace, Fault::TamperedHint] {
        let archive = Archive::single().await;
        *archive.store.fault.lock().unwrap() = fault;
        let error = archive.read().await.unwrap_err();
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
        assert_eq!(archive.store.requests.load(Ordering::SeqCst), 1, "no publication proof follows a forged candidate");
    }
    let archive = Archive::single().await;
    let manifest = &archive.manifests[0];
    let key = format!("manifests/{}.json", manifest.digest().unwrap());
    archive.objects.objects.lock().unwrap().get_mut(&key).unwrap().push(b' ');
    assert_eq!(archive.read().await.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
}
#[tokio::test]
async fn native_unavailable_partial_and_unknown_clock_are_independent_from_complete_archive_read() {
    for case in [test_support::ReplicationCase::Partial, test_support::ReplicationCase::Unavailable] {
        let (source, rows) = test_support::replication_evidence(case);
        let archive = Archive::new(vec![(source, rows)]).await;
        let result = archive.read().await.unwrap();
        assert!(result.discovery_complete);
        assert_eq!(result.resources.len(), 1);
        let PublishedResourceCoverage::Snapshot { sources } = &result.resources[0].coverage else { panic!() };
        assert!(sources.iter().any(|source| !source.complete_scope));
    }
    let (mut source, rows) = test_support::replication_evidence(test_support::ReplicationCase::Unavailable);
    let SourceObservation::Snapshot(snapshot) = &mut source.observation else { panic!() };
    snapshot.observed_from = "2026-11-22T12:00:00+02:00".into();
    snapshot.observed_until = "2026-11-22T12:00:01+02:00".into();
    let archive = Archive::new(vec![(source, rows)]).await;
    assert!(matches!(archive.read().await.unwrap().resources[0].native_time, PublishedResourceTime::Unknown));
}
#[tokio::test]
async fn whole_action_query_and_metadata_budgets_are_not_renewed_between_pages_or_payload_allowances() {
    fn future_bytes<F: std::future::Future>(
        _: impl FnOnce((&'static EvidenceService, &'static PublishedResourceRequest, &'static CollectionCancellation)) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    let bytes = future_bytes(|(service, request, cancellation)| {
        service.resources(request, EvidenceReadBudget::default(), cancellation)
    });
    assert!(bytes <= 64 * 1024, "actual resource-discovery future storage: {bytes}");
    let archive = Archive::single().await;
    let cancel = CollectionCancellation::default();
    let mut session =
        archive.service.session(EvidenceReadBudget { max_queries: 2, ..Default::default() }, &cancel).unwrap();
    assert_eq!(session.resources(&request()).await.unwrap().resources.len(), 1);
    assert!(session.resources(&request()).await.is_err());
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 2);
    let archive = Archive::single().await;
    let tiny = EvidenceReadBudget {
        max_proof_bytes: 1,
        recovery_payloads: Some(culprit_evidence::analytical::evidence::RecoveryPayloadBudget::default()),
        ..Default::default()
    };
    assert!(archive.service.resources(&request(), tiny, &cancel).await.is_err());
    assert_eq!(
        archive.store.requests.load(Ordering::SeqCst),
        0,
        "native-payload allowance cannot pay metadata proof work"
    );
}
#[tokio::test]
async fn cancelled_and_late_final_publication_responses_cannot_escape_the_shared_session() {
    for fault in [Fault::CancelFinal, Fault::LateFinal] {
        let archive = Archive::single().await;
        *archive.store.fault.lock().unwrap() = fault;
        let budget = EvidenceReadBudget {
            timeout_ms: if matches!(fault, Fault::LateFinal) { 5 } else { 1000 },
            ..Default::default()
        };
        let mut session = archive.service.session(budget, &archive.store.cancellation).unwrap();
        let error = session.resources(&request()).await.unwrap_err();
        assert_eq!(
            error.detail,
            Some(ErrorDetail::CollectionStopped(if matches!(fault, Fault::LateFinal) {
                CollectionStop::TimedOut
            } else {
                CollectionStop::Cancelled
            }))
        );
        assert!(session.resources(&request()).await.is_err(), "failed action cannot renew work");
    }
    let archive = Archive::single().await;
    archive.store.cancellation.cancel();
    assert!(archive.read().await.is_err());
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn published_selector_rechecks_actual_namespace_before_incident_candidates_on_the_same_action() {
    let archive = Archive::single().await;
    let resource = archive.read().await.unwrap().resources.remove(0);
    archive.store.requests.store(0, Ordering::SeqCst);
    let mut foreign = resource.selector.clone();
    foreign.namespace_id = digest(b"another actual warehouse namespace");
    let error = archive
        .service
        .resource_incident(&foreign, "2026-11-22T10:00:00Z", EvidenceReadBudget::default(), &archive.store.cancellation)
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::DestinationChanged)));
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
    let result = archive
        .service
        .resource_incident(
            &resource.selector,
            "2026-11-22T10:00:00Z",
            EvidenceReadBudget::default(),
            &archive.store.cancellation,
        )
        .await
        .unwrap();
    assert_eq!(result.database, resource.selector.database);
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 1);
    let root_bytes = archive.objects.objects.lock().unwrap()["recovery.json"].len() as u64;
    let exhausted = EvidenceReadBudget { max_proof_bytes: root_bytes, ..Default::default() };
    archive.store.requests.store(0, Ordering::SeqCst);
    assert!(
        archive
            .service
            .resource_incident(&resource.selector, "2026-11-22T10:00:00Z", exhausted, &archive.store.cancellation)
            .await
            .is_err()
    );
    assert_eq!(
        archive.store.requests.load(Ordering::SeqCst),
        0,
        "namespace admission cannot renew proof allowance for incident discovery"
    );
}
#[tokio::test]
async fn all_existing_session_read_routes_charge_metadata_before_publication_or_payload_proofs() {
    use culprit_evidence::analytical::evidence::EvidenceFamily;
    let archive = Archive::single().await;
    let manifest = &archive.manifests[0];
    let reference = DiagnosticCaptureRef {
        capture: manifest.capture.clone(),
        manifest_sha256: manifest.capture_manifest_sha256.clone(),
        source_scope_id: manifest.source.captured_range().unwrap().scope_id,
    };
    let cancellation = CollectionCancellation::default();
    for mode in 0..4 {
        let mut session = archive
            .service
            .session(EvidenceReadBudget { max_proof_bytes: 1, ..Default::default() }, &cancellation)
            .unwrap();
        let error = match mode {
            0 => session.read_metadata(&manifest.range_id).await.unwrap_err(),
            1 => session.lookup_metadata_reference(&reference).await.unwrap_err(),
            2 => session.read(&manifest.range_id, &[EvidenceFamily::QueryRuntime]).await.unwrap_err(),
            _ => session.read_reference(&reference, &[EvidenceFamily::QueryRuntime]).await.unwrap_err(),
        };
        assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
        assert!(session.read_metadata(&manifest.range_id).await.is_err());
    }
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
    let read_bytes = {
        let stored = archive.objects.objects.lock().unwrap();
        2 * stored["recovery.json"].len() as u64
            + stored[&format!("ranges/{}.json", manifest.range_id)].len() as u64
            + stored[&format!("manifests/{}.json", manifest.digest().unwrap())].len() as u64
    };
    let mut session = archive
        .service
        .session(EvidenceReadBudget { max_proof_bytes: read_bytes * 2 - 1, ..Default::default() }, &cancellation)
        .unwrap();
    assert!(session.read_metadata(&manifest.range_id).await.unwrap().is_read_complete());
    assert!(session.read_metadata(&manifest.range_id).await.is_err());
    assert_eq!(
        archive.store.requests.load(Ordering::SeqCst),
        1,
        "second metadata read cannot spend the first read's proof bytes again"
    );
}

#[tokio::test]
async fn rounded_search_clock_cannot_admit_an_outside_original_or_return_its_database_label() {
    let archive = Archive::single().await;
    *archive.store.fault.lock().unwrap() = Fault::RoundedClock;
    let mut selected = request();
    selected.as_of = "2026-11-22T10:00:01.123456700Z".into();
    let result =
        archive.service.resources(&selected, EvidenceReadBudget::default(), &archive.store.cancellation).await.unwrap();
    assert!(result.resources.is_empty());
    assert_eq!(result.gaps, vec![PublishedResourceGap::AcquisitionClockBoundary]);
    assert!(!result.discovery_complete);
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 1);
}
struct ChangedNamespace {
    objects: Arc<Blobs>,
    roots: AtomicUsize,
}
#[async_trait::async_trait]
impl ObjectReadTransport for ChangedNamespace {
    async fn get(&self, key: &str) -> Result<Option<ObjectRead>> {
        if key == "recovery.json" && self.roots.fetch_add(1, Ordering::SeqCst) == 1 {
            let mut stored = self.objects.objects.lock().unwrap();
            let mut root: Root = serde_json::from_slice(&stored["recovery.json"]).unwrap();
            root.store_id = digest(b"different namespace after discovery");
            if let Some(head) = &mut root.head {
                head.store_id = root.store_id.clone();
            }
            root.validate(&root.contract).unwrap();
            stored.insert("recovery.json".into(), serde_json::to_vec(&root).unwrap());
        }
        self.objects.get(key).await
    }
}
#[tokio::test]
async fn namespace_changed_between_candidate_discovery_and_reopening_original_is_refused() {
    let mut archive = Archive::single().await;
    archive.service.recovery =
        Arc::new(ChangedNamespace { objects: archive.objects.clone(), roots: AtomicUsize::new(0) });
    let error = archive.read().await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    assert_eq!(
        archive.store.requests.load(Ordering::SeqCst),
        1,
        "raced namespace cannot reach public source projection"
    );
}

async fn named_archive(cases: Vec<(CollectionDescription, Vec<TelemetryRecord>)>) -> Archive {
    Archive::new_at(cases, "2020-01-01T12:00:00Z", "2020-01-01T12:00:01Z").await
}
#[tokio::test]
async fn friendly_name_pages_all_same_selector_originals_and_keeps_one_budget_through_incident_assembly() {
    let original = test_support::informational_query_evidence();
    let name = original.0.target.database().unwrap().database.clone();
    // More than one real 32-row page. These are independent originals of the same exact route, not 34 databases.
    let cases = vec![original; 34];
    let archive = named_archive(cases.clone()).await;
    let report = archive
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            super::super::IncidentRequest::default_budget(),
            &archive.store.cancellation,
        )
        .await
        .unwrap();
    let queries = archive.store.requests.load(Ordering::SeqCst) as u32;
    assert!(queries > 34, "resource pages, publication verification and incident discovery all dispatch");
    assert_eq!(report.archive_work.queries, queries);
    assert!(report.archive_work.proof_bytes > 0);
    assert_eq!(report.archive_work.recovery_objects, 0);
    assert!(
        report.incidents.is_empty(),
        "this fixture publishes originals but no Gold incidents; no native-health claim"
    );
    let limited = named_archive(cases).await;
    let result = limited
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            EvidenceReadBudget { max_queries: queries - 1, ..super::super::IncidentRequest::default_budget() },
            &limited.store.cancellation,
        )
        .await;
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert!(
        (limited.store.requests.load(Ordering::SeqCst) as u32) < queries,
        "name resolution cannot renew its consumed allowance before incident discovery"
    );
}
#[tokio::test]
async fn friendly_name_refuses_distinct_routes_incomplete_originals_and_complete_missing_names() {
    let (source, _) = test_support::informational_query_evidence();
    let name = source.target.database().unwrap().database.clone();
    let mut foreign = source.clone();
    let CollectionTarget::Database(database) = &mut foreign.target else { panic!() };
    let culprit_core::DatabaseLocation::Server { host, .. } = &mut database.location else { panic!() };
    *host = "different-original-endpoint.invalid".into();
    let ambiguous = named_archive(vec![(source.clone(), vec![]), (foreign, vec![])]).await;
    let result = ambiguous
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            super::super::IncidentRequest::default_budget(),
            &ambiguous.store.cancellation,
        )
        .await;
    assert_eq!(result.unwrap_err().message, t!("incident.cli.resource_ambiguous"));
    let missing = named_archive(vec![(source, vec![])]).await;
    let result = missing
        .service
        .incident_named(
            "not-an-observed-name",
            "2020-01-01T12:00:00Z",
            super::super::IncidentRequest::default_budget(),
            &missing.store.cancellation,
        )
        .await;
    assert_eq!(result.unwrap_err().message, t!("incident.cli.resource_missing"));
    *missing.store.fault.lock().unwrap() = Fault::NoPublication;
    let result = missing
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            super::super::IncidentRequest::default_budget(),
            &missing.store.cancellation,
        )
        .await;
    assert_eq!(result.unwrap_err().message, t!("incident.cli.resource_incomplete"));
}
#[tokio::test]
async fn friendly_name_invalid_time_cancelled_admission_and_proof_limit_never_fall_back_to_a_source() {
    let (source, rows) = test_support::informational_query_evidence();
    let name = source.target.database().unwrap().database.clone();
    let archive = named_archive(vec![(source, rows)]).await;
    assert!(
        archive
            .service
            .incident_named(
                &name,
                "invalid-clock",
                super::super::IncidentRequest::default_budget(),
                &archive.store.cancellation
            )
            .await
            .is_err()
    );
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
    archive.store.cancellation.cancel();
    let result = archive
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            super::super::IncidentRequest::default_budget(),
            &archive.store.cancellation,
        )
        .await;
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
    let fresh = CollectionCancellation::default();
    let result = archive
        .service
        .incident_named(
            &name,
            "2020-01-01T12:00:00Z",
            EvidenceReadBudget { max_proof_bytes: 1, ..super::super::IncidentRequest::default_budget() },
            &fresh,
        )
        .await;
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(archive.store.requests.load(Ordering::SeqCst), 0);
}

#[test]
fn concrete_named_lookup_future_and_report_fit_the_existing_read_action_storage_ceiling() {
    use std::{future::Future, mem::size_of};
    fn future_bytes<A, F, T>(_: F) -> usize
    where
        F: FnOnce(A) -> T,
        T: Future,
    {
        size_of::<T>()
    }
    let bytes = future_bytes(
        |(service, name, around, cancellation): (
            &'static EvidenceService,
            &'static str,
            &'static str,
            &'static CollectionCancellation,
        )| service.incident_named(name, around, super::super::IncidentRequest::default_budget(), cancellation),
    );
    assert!(bytes <= 64 * 1024, "actual named lookup future: {bytes} bytes");
    assert!(size_of::<Result<IncidentReadReport>>() <= 16 * 1024);
}
