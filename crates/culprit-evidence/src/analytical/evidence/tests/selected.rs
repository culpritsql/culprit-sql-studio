//! Canonical manifest/publication/index/decoder/EOF controls; no Collector or native source dependency.
use super::*;
use crate::{
    analytical::{
        ProjectionCatalog,
        indexes::{IndexKey, ProjectionIndexes},
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
        test_support::ReadOnlyFixture,
    },
    delivery::{
        object_store::ObjectRecoveryReader,
        recovery::{CommittedRange, reopen},
    },
};
use culprit_core::ErrorDetail;
use culprit_core::Result;
use serde_json::{Map, Value};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Missing,
    Tampered,
}
struct Store {
    inner: Arc<dyn AnalyticalReadStore>,
    rows: Mutex<Vec<(usize, usize)>>,
    queries: AtomicUsize,
    fault: Mutex<Fault>,
    stall: AtomicBool,
    active: AtomicUsize,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl AnalyticalReadStore for Store {
    fn destination(&self) -> &str {
        self.inner.destination()
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.inner.catalog()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.inner.logical_values(stream, row)
    }
    async fn publication_rows(&self, range: &CommittedRange, limit: usize) -> Result<AnalyticalReadPage> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.inner.publication_rows(range, limit).await
    }
    async fn audit_membership(&self, range: &CommittedRange, limit: usize) -> Result<usize> {
        self.queries.fetch_add(1, Ordering::SeqCst);
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
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.rows.lock().unwrap().push((after, until));
        if self.stall.load(Ordering::SeqCst) {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _owner = Active(&self.active);
            std::future::pending::<()>().await;
        }
        let mut page = self.inner.rows(range, batch, after, until, limit).await?;
        match *self.fault.lock().unwrap() {
            Fault::None => (),
            Fault::Missing => page.rows.clear(),
            Fault::Tampered => {
                let stream = range.manifest().batches.iter().find(|known| known.batch_id == batch).unwrap().stream;
                let catalog = self.catalog();
                let schema = catalog.schema(stream)?;
                let ordinal = schema.columns().iter().position(|column| column.name == "RowOrdinal").unwrap();
                page.rows[0][ordinal] = serde_json::json!(0);
            }
        }
        page.response_bytes = serde_json::to_vec(&page.rows).unwrap().len();
        Ok(page)
    }
}
struct Fixture {
    original: ReadOnlyFixture,
    recovery: ObjectRecoveryReader,
    range: CommittedRange,
    reader: EvidenceReader,
    store: Arc<Store>,
}
impl Fixture {
    async fn new(source: CollectionDescription, rows: Vec<TelemetryRecord>) -> Self {
        let original = ReadOnlyFixture::capture(source, rows).await;
        let recovery = ObjectRecoveryReader::open(original.transport(), original.contract.clone()).await.unwrap();
        let range = reopen(&recovery, &original.manifest.range_id).await.unwrap();
        let store = Arc::new(Store {
            inner: original.store.clone(),
            rows: Mutex::default(),
            queries: AtomicUsize::new(0),
            fault: Mutex::default(),
            stall: AtomicBool::new(false),
            active: AtomicUsize::new(0),
        });
        let reader = EvidenceReader::new(store.clone(), Arc::new(ProjectionIndexes::new(original.transport())));
        Self { original, recovery, range, reader, store }
    }
    fn reference(&self, stream: TelemetryStream, ordinal: u32) -> DiagnosticEvidenceRef {
        let batch = self.range.manifest().batches.iter().find(|batch| batch.stream == stream).unwrap();
        let objects = self.original.objects.objects.lock().unwrap();
        let bytes = &objects[&batch.object];
        let line =
            bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()).nth(ordinal as usize - 1).unwrap();
        let row: ExportedRecord = serde_json::from_slice(line).unwrap();
        DiagnosticEvidenceRef::from_export(
            &self.range.manifest().source,
            &self.range.manifest().capture_manifest_sha256,
            &row,
            ordinal - 1,
        )
        .unwrap()
    }
    async fn read(
        &self,
        references: &[DiagnosticEvidenceRef],
        budget: EvidenceReadBudget,
    ) -> Result<SelectedEvidenceRecords> {
        self.reader.read_records(&self.range, references, budget, Some(&self.recovery)).await
    }
}
fn many_queries(count: u32) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (source, rows) = test_support::query_cohort_evidence(test_support::QueryCohortCase::PlanVariation);
    let template = rows
        .into_iter()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(query) => Some(query),
            _ => None,
        })
        .unwrap();
    let rows = (0..count)
        .map(|number| {
            let mut query = template.clone();
            query.identity.query_id = (1000 + number).to_string();
            query.id = query.identity.definition_id(&source).unwrap();
            TelemetryRecord::QueryDefinition(query)
        })
        .collect();
    (source, rows)
}

#[tokio::test]
async fn sparse_original_positions_do_not_select_a_large_family_and_pay_the_whole_authenticated_index() {
    let (source, rows) = many_queries(1000);
    let fixture = Fixture::new(source, rows).await;
    let references = [
        fixture.reference(TelemetryStream::QueryDefinitions, 1),
        fixture.reference(TelemetryStream::QueryDefinitions, 1000),
    ];
    let budget = EvidenceReadBudget { max_rows: 2, ..Default::default() };
    let result = fixture.read(&references, budget).await.unwrap();
    assert!(result.is_selection_complete());
    assert_eq!(result.records().len(), 2);
    assert_eq!(result.metrics().selected_rows, 2);
    assert_eq!(result.metrics().returned_rows, 2);
    assert_eq!(*fixture.store.rows.lock().unwrap(), [(0, 1), (999, 1000)]);
    let catalog = fixture.store.catalog();
    let batch = fixture
        .range
        .manifest()
        .batches
        .iter()
        .find(|batch| batch.stream == TelemetryStream::QueryDefinitions)
        .unwrap();
    let key =
        IndexKey::for_batch(&fixture.range, &batch.batch_id, catalog.schema(batch.stream).unwrap().digest()).unwrap();
    assert_eq!(
        result.metrics().proof_bytes,
        key.encoded_bytes() as u64,
        "all1000 proof entries are authenticated and charged"
    );
    for reference in &references {
        assert!(result.selected(reference).unwrap().is_some());
    }
    let unselected = fixture.reference(TelemetryStream::QueryDefinitions, 500);
    assert!(result.selected(&unselected).is_err(), "a selected-row result cannot silently claim an unrequested row");
}

#[tokio::test]
async fn exact_rows_fit_the_existing_row_budget_while_full_family_and_full_proof_exhaustion_refuse_before_dispatch() {
    let (source, rows) = many_queries(1000);
    let fixture = Fixture::new(source, rows).await;
    let reference = fixture.reference(TelemetryStream::QueryDefinitions, 1000);
    let budget = EvidenceReadBudget { max_rows: 1, ..Default::default() };
    let error = fixture
        .reader
        .read_capture(&fixture.range, &[EvidenceFamily::QueryDefinitions], budget, None)
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(fixture.store.queries.load(Ordering::SeqCst), 0);
    assert!(fixture.read(std::slice::from_ref(&reference), budget).await.unwrap().is_selection_complete());
    let catalog = fixture.store.catalog();
    let batch = fixture
        .range
        .manifest()
        .batches
        .iter()
        .find(|batch| batch.stream == TelemetryStream::QueryDefinitions)
        .unwrap();
    let key =
        IndexKey::for_batch(&fixture.range, &batch.batch_id, catalog.schema(batch.stream).unwrap().digest()).unwrap();
    let before = fixture.store.rows.lock().unwrap().len();
    let error = fixture
        .read(&[reference], EvidenceReadBudget { max_proof_bytes: key.encoded_bytes() as u64 - 1, ..budget })
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(fixture.store.rows.lock().unwrap().len(), before);
}

#[tokio::test]
async fn changed_manifest_payload_position_and_native_identity_are_refused_before_any_query() {
    for field in ["manifest", "payload", "position", "identity"] {
        let (source, rows) = many_queries(2);
        let fixture = Fixture::new(source, rows).await;
        let mut reference = fixture.reference(TelemetryStream::QueryDefinitions, 1);
        match field {
            "manifest" => reference.manifest_sha256 = digest(b"another native original"),
            "payload" => reference.payload_sha256 = digest(b"another row body"),
            "position" => reference.row_ordinal = 1000,
            "identity" => reference.native_key = fixture.reference(TelemetryStream::QueryDefinitions, 2).native_key,
            _ => unreachable!(),
        }
        assert!(reference.is_valid());
        assert_eq!(
            fixture.read(&[reference], EvidenceReadBudget::default()).await.unwrap_err().detail,
            Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)),
            "{field}"
        );
        assert_eq!(fixture.store.queries.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_missing_selected_row_is_unavailable_and_an_outside_or_tampered_row_is_not_position_healed() {
    for tampered in [false, true] {
        let (source, rows) = many_queries(2);
        let fixture = Fixture::new(source, rows).await;
        let reference = fixture.reference(TelemetryStream::QueryDefinitions, 2);
        *fixture.store.fault.lock().unwrap() = if tampered { Fault::Tampered } else { Fault::Missing };
        let result = fixture.read(std::slice::from_ref(&reference), EvidenceReadBudget::default()).await;
        if tampered {
            assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
        } else {
            let result = result.unwrap();
            assert!(!result.is_selection_complete());
            assert!(result.selected(&reference).unwrap().is_none());
            assert_eq!(result.gaps()[0].reason, EvidenceGapReason::MissingRows);
            assert_eq!(result.metrics().selected_rows, 1);
        }
    }
}

#[tokio::test]
async fn a_late_whole_index_checksum_tamper_is_refused_even_for_one_selected_position() {
    let (source, rows) = many_queries(1000);
    let fixture = Fixture::new(source, rows).await;
    let reference = fixture.reference(TelemetryStream::QueryDefinitions, 1);
    {
        let mut objects = fixture.original.objects.objects.lock().unwrap();
        let (_, bytes) = objects.iter_mut().find(|(key, _)| key.starts_with("validation/")).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
    }
    assert_eq!(
        fixture.read(&[reference], EvidenceReadBudget::default()).await.unwrap_err().detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::Integrity))
    );
    assert!(fixture.store.rows.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_selected_row_timeout_drops_the_same_pending_read_owner_without_returning_partial_success() {
    let (source, rows) = many_queries(1);
    let fixture = Fixture::new(source, rows).await;
    let reference = fixture.reference(TelemetryStream::QueryDefinitions, 1);
    fixture.store.stall.store(true, Ordering::SeqCst);
    let error =
        fixture.read(&[reference], EvidenceReadBudget { timeout_ms: 100, ..Default::default() }).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::CollectionStopped(CollectionStop::TimedOut)));
    assert_eq!(fixture.store.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.store.rows.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn only_explicit_recovery_restores_a_selected_native_body_and_still_authenticates_the_complete_object_to_eof() {
    let (source, rows) = test_support::object_definition_recovery_only_evidence();
    let fixture = Fixture::new(source, rows).await;
    let reference = fixture.reference(TelemetryStream::ObjectDefinitions, 1);
    let public = fixture.read(std::slice::from_ref(&reference), EvidenceReadBudget::default()).await.unwrap();
    assert!(!public.is_selection_complete());
    assert!(public.selected(&reference).unwrap().is_none());
    assert_eq!(public.metrics().recovery_objects, 0);
    let budget = EvidenceReadBudget { recovery_payloads: Some(RecoveryPayloadBudget::default()), ..Default::default() };
    let private = fixture.read(std::slice::from_ref(&reference), budget).await.unwrap();
    assert!(private.is_selection_complete());
    assert_eq!(private.metrics().recovery_objects, 1);
    assert!(private.metrics().recovery_bytes > 1024);
    assert!(private.selected(&reference).unwrap().is_some());
    {
        let batch = fixture.range.manifest().batches.iter().find(|batch| batch.stream == reference.stream).unwrap();
        fixture.original.objects.objects.lock().unwrap().get_mut(&batch.object).unwrap().push(b' ');
    }
    assert!(
        fixture.read(&[reference], budget).await.is_err(),
        "late EOF failure cannot install an authenticated prefix"
    );
}
