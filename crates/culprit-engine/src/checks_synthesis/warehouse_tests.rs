//! Real canonical warehouse originals enter the same pure analyzer with the Collector feature disabled.
use super::test_support::CaptureFinding;
use super::*;
use culprit_evidence::{
    analytical::{
        ProjectionCatalog,
        evidence::{EvidenceFamily, EvidenceReadBudget, EvidenceReader},
        indexes::ProjectionIndexes,
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
        test_support::{self, ReadOnlyFixture},
    },
    delivery::{
        object_store::{
            ObjectRecoveryReader,
            transport::{ObjectRead, ObjectReadTransport},
        },
        recovery::{CommittedRange, reopen},
    },
};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

const RECORDED_AT: &str = "2020-01-01T12:02:00Z";
const QUERY_FAMILIES: &[EvidenceFamily] = &[
    EvidenceFamily::QueryDefinitions,
    EvidenceFamily::PlanDefinitions,
    EvidenceFamily::QueryRuntime,
    EvidenceFamily::QueryWaits,
];
fn display() -> DerivedDisplayContext {
    DerivedDisplayContext {
        connection_alias: "Explicit warehouse alias".into(),
        environment: None,
        database_name: None,
    }
}
fn policy(many: bool) -> ChecksSynthesizer {
    let pipeline = culprit_checks::Pipeline::builtin().with_check(Arc::new(CaptureFinding { many }));
    ChecksSynthesizer::new(Arc::new(pipeline), "warehouse-controlled-rules-1").unwrap()
}

// Delegation changes only observation/omission timing. Encoding, namespace, proofs, decoder and EOF stay real.
struct ReadStore {
    inner: Arc<dyn AnalyticalReadStore>,
    requests: AtomicUsize,
    omitted_batch: Option<String>,
}
#[async_trait::async_trait]
impl AnalyticalReadStore for ReadStore {
    fn destination(&self) -> &str {
        self.inner.destination()
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.inner.catalog()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.inner.logical_values(stream, row)
    }
    async fn audit_membership(&self, range: &CommittedRange, bytes: usize) -> Result<usize> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.audit_membership(range, bytes).await
    }
    async fn publication_rows(&self, range: &CommittedRange, bytes: usize) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.publication_rows(range, bytes).await
    }
    async fn rows(
        &self,
        range: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        bytes: usize,
    ) -> Result<AnalyticalReadPage> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let mut page = self.inner.rows(range, batch, after, until, bytes).await?;
        if self.omitted_batch.as_deref() == Some(batch) && after == 0 && !page.rows.is_empty() {
            page.rows.remove(0);
            page.response_bytes = serde_json::to_vec(&page.rows).unwrap().len();
        }
        Ok(page)
    }
}
struct ReadObjects {
    inner: Arc<dyn ObjectReadTransport>,
    requests: AtomicUsize,
    native_payloads: AtomicUsize,
}
#[async_trait::async_trait]
impl ObjectReadTransport for ReadObjects {
    async fn get(&self, key: &str) -> Result<Option<ObjectRead>> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if key.starts_with("payloads/") {
            self.native_payloads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key).await
    }
}
struct Warehouse {
    range: CommittedRange,
    reader: EvidenceReader,
    store: Arc<ReadStore>,
    objects: Arc<ReadObjects>,
}
impl Warehouse {
    async fn open(fixture: ReadOnlyFixture, omit_runtime: bool) -> Self {
        assert_eq!(fixture.manifest.source_queries, 0);
        let omitted_batch = omit_runtime.then(|| {
            fixture
                .manifest
                .batches
                .iter()
                .find(|batch| batch.stream == TelemetryStream::QueryRuntime)
                .unwrap()
                .batch_id
                .clone()
        });
        let objects = Arc::new(ReadObjects {
            inner: fixture.transport(),
            requests: AtomicUsize::new(0),
            native_payloads: AtomicUsize::new(0),
        });
        let recovery = ObjectRecoveryReader::open(objects.clone(), fixture.contract).await.unwrap();
        let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
        let store = Arc::new(ReadStore { inner: fixture.store, requests: AtomicUsize::new(0), omitted_batch });
        let reader = EvidenceReader::new(store.clone(), Arc::new(ProjectionIndexes::new(objects.clone())));
        Self { range, reader, store, objects }
    }
    async fn read(&self, families: &[EvidenceFamily]) -> EvidenceCapture {
        self.reader.read_capture(&self.range, families, EvidenceReadBudget::default(), None).await.unwrap()
    }
    fn work(&self) -> (usize, usize, usize) {
        (
            self.store.requests.load(Ordering::SeqCst),
            self.objects.requests.load(Ordering::SeqCst),
            self.objects.native_payloads.load(Ordering::SeqCst),
        )
    }
}

#[tokio::test]
async fn authenticated_query_original_uses_same_gold_rules_english_and_display_without_any_read_work() {
    let (source, rows) = test_support::query_evidence();
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source.clone(), rows).await, false).await;
    let capture = warehouse.read(QUERY_FAMILIES).await;
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    assert_eq!((capture.metrics().recovery_objects, capture.metrics().recovery_bytes), (0, 0));
    let before = warehouse.work();
    let metrics = serde_json::to_value(capture.metrics()).unwrap();
    let policy = policy(false);
    let output = culprit_core::i18n::scope("ro", || {
        let before_language = culprit_core::i18n::current().code.clone();
        let output = policy.synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        assert_eq!(culprit_core::i18n::current().code, before_language);
        output
    });
    // Equality with the existing private pure entry keeps adapter conversion, output cap and catalog policy singular.
    let original = capture.verified_original().unwrap();
    let expected = policy.analyze_originals(&[&original], RECORDED_AT, &display(), vec![]).unwrap();
    assert_eq!(serde_json::to_value(&output).unwrap(), serde_json::to_value(expected).unwrap());
    assert_eq!(output.description.inputs[0].capture, *original.original());
    assert_eq!(output.description.recorded_at, canonical_time(RECORDED_AT).unwrap());
    assert_eq!(output.description.language, "en");
    assert_eq!(output.description.rules_version, "warehouse-controlled-rules-1");
    let finding = output.findings.iter().find(|row| row.finding.check == "controlled-capture").unwrap();
    assert_eq!(finding.display, display());
    assert!(!output.description.finding_evaluations.checks.is_empty());
    assert_eq!(warehouse.work(), before, "pure synthesis has no store/recovery/source owner");
    assert_eq!(serde_json::to_value(capture.metrics()).unwrap(), metrics);
    assert_eq!(capture.source(), &source);
    assert!(!serde_json::to_string(&output).unwrap().contains("fixture-private-literal"));
    output.validate().unwrap();
}

#[tokio::test]
async fn missing_verified_rows_remain_partial_and_do_not_read_native_recovery_as_a_fallback() {
    let (source, rows) = test_support::query_evidence();
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source, rows).await, true).await;
    let capture = warehouse.read(QUERY_FAMILIES).await;
    assert!(!capture.is_read_complete());
    assert!(
        capture.gaps().iter().any(|gap| gap.reason == culprit_core::collected_analysis::EvidenceGapReason::MissingRows)
    );
    let before = warehouse.work();
    let output = policy(false).synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    assert!(!output.description.input_read_complete);
    assert!(!output.description.limitations().is_empty());
    assert!(!output.description.finding_evaluations.populations.is_empty());
    assert_eq!(warehouse.work(), before);
    assert_eq!(before.2, 0);
    output.validate().unwrap();
}

#[tokio::test]
async fn caller_row_and_query_limits_refuse_before_an_authenticated_analysis_handle_exists() {
    let (source, rows) = test_support::query_evidence();
    let row_count = rows.len() as u32;
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source, rows).await, false).await;
    let error = warehouse
        .reader
        .read_capture(
            &warehouse.range,
            QUERY_FAMILIES,
            EvidenceReadBudget { max_rows: row_count - 1, ..Default::default() },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(warehouse.store.requests.load(Ordering::SeqCst), 0);
    let error = warehouse
        .reader
        .read_capture(
            &warehouse.range,
            QUERY_FAMILIES,
            EvidenceReadBudget { max_queries: 2, max_rows: row_count, ..Default::default() },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(warehouse.store.requests.load(Ordering::SeqCst), 2);
    assert_eq!(warehouse.objects.native_payloads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_policy_rejects_duplicate_originals_bad_metadata_and_output_overflow_without_rebinding() {
    let (source, rows) = test_support::query_evidence();
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source, rows).await, false).await;
    let capture = warehouse.read(QUERY_FAMILIES).await;
    let before = warehouse.work();
    assert!(ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "\n").is_err());
    let policy = policy(false);
    assert!(policy.synthesize_captures(&capture, &[&capture], RECORDED_AT, &display()).is_err());
    assert!(policy.synthesize_captures(&capture, &[], "unknown-time", &display()).is_err());
    let mut invalid_display = display();
    invalid_display.connection_alias.clear();
    assert!(policy.synthesize_captures(&capture, &[], RECORDED_AT, &invalid_display).is_err());
    let oversized = vec![&capture; MAX_DERIVED_CAPTURES];
    let error = policy.synthesize_captures(&capture, &oversized, RECORDED_AT, &display()).unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    let error = self::policy(true).synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(warehouse.work(), before);
}

#[tokio::test]
async fn retained_revision_projects_incident_history_without_an_outbox_or_current_source() {
    let (source, rows) = test_support::diagnostic_evidence_cases().remove(0);
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source, rows).await, false).await;
    let capture = warehouse.read(&[EvidenceFamily::OperationalSamples]).await;
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    let before = warehouse.work();
    let output = policy(false).synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    assert_eq!(output.incidents.len(), 1);
    assert_eq!(output.description.output_coverage.incidents, DerivedOutputCoverage::Evaluated);
    assert_eq!(output.incidents[0].entry.original.as_ref(), Some(&output.incidents[0].revision_reference));
    assert!(output.incidents[0].affected_queries.is_empty());
    assert!(output.incidents[0].culprit_queries.is_empty());
    assert_eq!(output.incidents[0].attribution_status, DerivedAttributionStatus::Unestablished);
    assert_eq!(warehouse.work(), before);
    output.validate().unwrap();
}

fn settings_source(
    primary: &CollectionDescription,
    at: &str,
    value: &str,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mut source = primary.clone();
    source.source = "controlled-retained-settings".into();
    source.partition = SourcePartition::Target;
    source.observation = SourceObservation::Snapshot(StateSnapshotDescription {
        dataset: SnapshotDataset::Settings,
        observed_from: at.into(),
        observed_until: at.into(),
        records: 1,
        sources: vec![StateSourceCoverage::observed(source_id::SETTINGS.into(), 1, true)],
    });
    let rows = vec![TelemetryRecord::Setting(SettingObservation {
        source: source_id::SETTINGS.into(),
        namespace: "sys.database_scoped_configurations".into(),
        name: "MAXDOP".into(),
        scope: MeasurementScope::Database,
        nature: SettingNature::Configuration,
        value: Some(value.into()),
        value_in_use: None,
        value_for_secondary: None,
        unit: None,
        native_context: None,
        native_source: None,
        pending_restart: None,
        native_is_default: None,
        default_value: None,
        default_source: SettingDefaultSource::Unknown,
    })];
    (source, rows)
}
#[tokio::test]
async fn independent_catalog_originals_keep_same_scope_clocks_and_changes_while_foreign_context_is_withheld() {
    let original = test_support::query_compilation_followup_original();
    let primary = Warehouse::open(
        ReadOnlyFixture::capture_at(
            original.source.clone(),
            original.records,
            1,
            "2020-01-01T12:01:00Z",
            "2020-01-01T12:01:01Z",
        )
        .await,
        false,
    )
    .await;
    let primary_capture = primary.read(QUERY_FAMILIES).await;
    let (source, rows) = settings_source(&original.source, "2020-01-01T10:00:00Z", "0");
    let previous = Warehouse::open(
        ReadOnlyFixture::capture_at(source, rows, 2, "2020-01-01T10:00:00Z", "2020-01-01T10:00:01Z").await,
        false,
    )
    .await;
    let previous_capture = previous.read(&[EvidenceFamily::SettingObservations]).await;
    for foreign in [false, true] {
        let (mut source, rows) = settings_source(&original.source, "2020-01-01T11:00:00Z", "8");
        if foreign {
            let CollectionTarget::Database(database) = &mut source.target else { panic!() };
            database.incarnation = Some("foreign-database-incarnation".into());
        }
        let current = Warehouse::open(
            ReadOnlyFixture::capture_at(source, rows, 3, "2020-01-01T11:00:00Z", "2020-01-01T11:00:01Z").await,
            false,
        )
        .await;
        let current_capture = current.read(&[EvidenceFamily::SettingObservations]).await;
        assert!(
            primary_capture.is_read_complete()
                && previous_capture.is_read_complete()
                && current_capture.is_read_complete()
        );
        let before = (primary.work(), previous.work(), current.work());
        let output = policy(false)
            .synthesize_captures(&primary_capture, &[&previous_capture, &current_capture], RECORDED_AT, &display())
            .unwrap();
        assert_eq!(output.description.inputs.len(), 3);
        assert_eq!(output.changes.len(), usize::from(!foreign));
        if foreign {
            assert!(output.description.context_issues.contains(&DerivedContextIssue::UnqualifiedCatalogScope));
            assert!(matches!(
                output.description.output_coverage.changes,
                DerivedOutputCoverage::ContextUnavailable { .. }
            ));
        } else {
            assert_eq!(output.description.output_coverage.changes, DerivedOutputCoverage::Evaluated);
            assert_eq!(
                output.description.change_evaluations[0],
                DerivedChangeEvaluation::Settings {
                    previous_input: 1,
                    current_input: 2,
                    observed_after: canonical_time(previous_capture.collected_from()).unwrap(),
                    observed_until: canonical_time(current_capture.collected_until()).unwrap(),
                }
            );
            assert_eq!(output.changes[0].change.source, EventSource::Scans);
            assert_eq!(output.changes[0].change.object.as_deref(), Some("MAXDOP"));
        }
        assert_eq!((primary.work(), previous.work(), current.work()), before);
        output.validate().unwrap();
    }
}

#[tokio::test]
async fn materialized_gold_is_not_promoted_to_an_original_native_analysis_input() {
    let (source, rows) = test_support::derived_diagnosis_evidence_cases().remove(0);
    let warehouse = Warehouse::open(ReadOnlyFixture::capture(source, rows).await, false).await;
    let capture = warehouse.read(&[EvidenceFamily::DerivedIncidents]).await;
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    let before = warehouse.work();
    let error = policy(false).synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    assert_eq!(warehouse.work(), before);
}
