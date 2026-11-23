//! One real capture owner for local-original consumer conformance. It does not contact a database.
use crate::capture;
use crate::capture::SourceFactory;
use crate::outbox::Outbox;
use culprit_core::telemetry::*;
use culprit_evidence::analytical::test_support::*;
use culprit_read::cancellation::CollectionCancellation;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The source closes before this fixture exposes a durable result; retaining the fixture retains its temporary files.
pub struct LocalCaptureFixture {
    pub outbox: Option<Outbox>,
    pub directory: PathBuf,
    pub capture_directory: PathBuf,
    pub manifest: CollectionManifest,
    _temporary: tempfile::TempDir,
}

/// Selector names are conformance inputs, not provider selection or native schema interpretation.
pub struct LocalSnapshotCase {
    pub analysis: &'static str,
    pub source: CollectionDescription,
    pub records: Vec<TelemetryRecord>,
}
pub fn local_snapshot_evidence_cases() -> Vec<LocalSnapshotCase> {
    let case = |analysis, (source, records)| LocalSnapshotCase { analysis, source, records };
    vec![
        case(
            "performance_counters",
            culprit_evidence::analytical::test_support::performance_counter_evidence(
                culprit_evidence::analytical::test_support::PerformanceCounterCase::Exact,
            ),
        ),
        case(
            "tempdb_metadata_memory",
            culprit_evidence::analytical::test_support::tempdb_metadata_memory_evidence(
                culprit_evidence::analytical::test_support::TempdbMetadataMemoryCase::Reserved,
            ),
        ),
        case(
            "table_memory",
            culprit_evidence::analytical::test_support::table_memory_evidence(
                culprit_evidence::analytical::test_support::TableMemoryCase::Exact,
            ),
        ),
        case(
            "full_text_populations",
            culprit_evidence::analytical::test_support::full_text_population_evidence(
                culprit_evidence::analytical::test_support::FullTextPopulationCase::Processing,
            ),
        ),
        case(
            "instance_resources",
            culprit_evidence::analytical::test_support::instance_resource_scheduler_evidence(
                culprit_evidence::analytical::test_support::SchedulerCase::Exact,
            ),
        ),
        case(
            "database_properties",
            culprit_evidence::analytical::test_support::database_properties_evidence(
                culprit_evidence::analytical::test_support::DatabasePropertiesCase::Exact,
            ),
        ),
        case(
            "table_full_text",
            culprit_evidence::analytical::test_support::table_full_text_evidence(
                culprit_evidence::analytical::test_support::TableFullTextCase::Enabled,
            ),
        ),
        case(
            "table_temporal",
            culprit_evidence::analytical::test_support::table_temporal_evidence(
                culprit_evidence::analytical::test_support::TableTemporalCase::SystemVersioned,
            ),
        ),
        case(
            "index_configuration",
            culprit_evidence::analytical::test_support::index_configuration_evidence(
                culprit_evidence::analytical::test_support::IndexConfigurationCase::Mixed,
            ),
        ),
        case(
            "indexed_views",
            culprit_evidence::analytical::test_support::indexed_views_evidence(
                culprit_evidence::analytical::test_support::IndexedViewsCase::Present,
            ),
        ),
        case("compilation", culprit_evidence::analytical::test_support::query_compilation_evidence()),
        case(
            "configured_log_rates",
            culprit_evidence::analytical::test_support::configured_log_rate_evidence_cases().remove(0),
        ),
        case(
            "connection_outcomes",
            culprit_evidence::analytical::test_support::connection_outcome_evidence_cases().remove(0),
        ),
        case(
            "jobs",
            culprit_evidence::analytical::test_support::jobs_evidence(
                culprit_evidence::analytical::test_support::JobsCase::Failed,
            ),
        ),
        case(
            "cdc",
            culprit_evidence::analytical::test_support::cdc_evidence(
                culprit_evidence::analytical::test_support::CdcCase::ErrorAndProgress,
            ),
        ),
        case(
            "change_feed",
            culprit_evidence::analytical::test_support::change_feed_evidence(
                culprit_evidence::analytical::test_support::ChangeFeedCase::ProgressAndError,
            ),
        ),
        case(
            "columnstore_rowgroups",
            culprit_evidence::analytical::test_support::columnstore_rowgroup_evidence(
                culprit_evidence::analytical::test_support::ColumnstoreRowgroupCase::States,
            ),
        ),
        case(
            "suspect_pages",
            culprit_evidence::analytical::test_support::suspect_page_history_evidence(
                culprit_evidence::analytical::test_support::SuspectPageHistoryCase::RepairedOnly,
            ),
        ),
        case(
            "backup_history",
            culprit_evidence::analytical::test_support::backup_history_evidence(
                culprit_evidence::analytical::test_support::BackupHistoryCase::MultipleRecords,
            ),
        ),
        case("request_memory", culprit_evidence::analytical::test_support::request_memory_evidence_cases().remove(0)),
        case("task_space", culprit_evidence::analytical::test_support::task_space_evidence_cases().remove(0)),
        case(
            "current_request_work",
            culprit_evidence::analytical::test_support::current_request_work_evidence_cases().remove(0),
        ),
        case("statement_work", culprit_evidence::analytical::test_support::statement_work_evidence_cases().remove(0)),
        case(
            "maintenance_progress",
            culprit_evidence::analytical::test_support::maintenance_progress_evidence_cases().remove(0),
        ),
        case(
            "completed_spills",
            culprit_evidence::analytical::test_support::completed_spill_evidence_cases()
                .into_iter()
                .find(|case| case.name == "recorded_spills")
                .unwrap()
                .runtime,
        ),
        case(
            "storage",
            culprit_evidence::analytical::test_support::storage_evidence(
                culprit_evidence::analytical::test_support::StorageCase::Exact,
            ),
        ),
        case(
            "pool_settings",
            culprit_evidence::analytical::test_support::pool_settings_evidence(
                culprit_evidence::analytical::test_support::PoolSettingsCase::Exact,
            ),
        ),
        case(
            "wait_counters",
            culprit_evidence::analytical::test_support::pool_wait_counter_evidence(
                culprit_evidence::analytical::test_support::PoolWaitCounterCase::GeneralPurpose,
            ),
        ),
        case(
            "replication",
            culprit_evidence::analytical::test_support::replication_evidence(
                culprit_evidence::analytical::test_support::ReplicationCase::MssqlHyperscale,
            ),
        ),
        case(
            "group_replication",
            culprit_evidence::analytical::test_support::group_replication_evidence(
                culprit_evidence::analytical::test_support::GroupReplicationCase::Queued,
            ),
        ),
        case(
            "galera",
            culprit_evidence::analytical::test_support::galera_evidence(
                culprit_evidence::analytical::test_support::GaleraCase::Queued,
            ),
        ),
    ]
}
/// These modes exercise real local manifest settlement, never native database behavior or cost.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SourceAcquisitionCase {
    Complete,
    Failed,
    Unconfirmed,
}
impl LocalCaptureFixture {
    pub async fn source_acquisition(case: SourceAcquisitionCase) -> Self {
        Self::source_acquisition_for("local-evidence", case).await
    }
    pub async fn source_acquisition_for(connection: &str, case: SourceAcquisitionCase) -> Self {
        let source = culprit_evidence::analytical::test_support::page_server_read_evidence_cases().remove(0).source;
        let records = vec![];
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("outbox");
        let mut outbox = Outbox::create(&directory, OutboxLimits::default()).unwrap();
        let selection = selection(&source);
        // This metadata-only fixture uses the same capture/lease/close owner as every ordinary local corpus.
        let (capture_directory, manifest) = collect_original_mode(
            &mut outbox,
            source,
            records,
            selection,
            if case == SourceAcquisitionCase::Unconfirmed {
                SourceShutdown::Unconfirmed
            } else {
                SourceShutdown::Quiescent
            },
            Some(case),
            connection,
        )
        .await;
        Self { outbox: Some(outbox), directory, capture_directory, manifest, _temporary: temporary }
    }

    pub async fn query_history(source: CollectionDescription, records: Vec<TelemetryRecord>) -> Self {
        assert!(source.query_history().is_some());
        Self::with_shutdown(source, records, SourceShutdown::Quiescent).await
    }

    pub async fn snapshot(source: CollectionDescription, records: Vec<TelemetryRecord>) -> Self {
        assert!(source.observation.snapshot().is_some());
        Self::with_shutdown(source, records, SourceShutdown::Quiescent).await
    }

    pub async fn activity(source: CollectionDescription, records: Vec<TelemetryRecord>) -> Self {
        assert!(source.observation.activity().is_some());
        Self::with_shutdown(source, records, SourceShutdown::Quiescent).await
    }

    pub async fn with_shutdown(
        source: CollectionDescription,
        records: Vec<TelemetryRecord>,
        shutdown: SourceShutdown,
    ) -> Self {
        Self::capture_with_synthesis(source, records, shutdown, None).await
    }

    /// Install the normal pure analyzer before any native fixture reservation. No source behavior changes.
    pub async fn with_automatic_synthesis(
        source: CollectionDescription,
        records: Vec<TelemetryRecord>,
        automatic: &crate::synthesis::AutomaticSynthesis,
    ) -> Self {
        Self::capture_with_synthesis(source, records, SourceShutdown::Quiescent, Some(automatic)).await
    }

    async fn capture_with_synthesis(
        mut source: CollectionDescription,
        records: Vec<TelemetryRecord>,
        shutdown: SourceShutdown,
        automatic: Option<&crate::synthesis::AutomaticSynthesis>,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("outbox");
        let mut outbox = Outbox::create(&directory, OutboxLimits::default()).unwrap();
        if let Some(automatic) = automatic {
            let operation =
                culprit_core::limits::OperationGuard::new(crate::outbox::CompletedCaptureBudget::default().timeout)
                    .unwrap();
            outbox.register_automatic_synthesis(automatic, operation.handle(), ()).await.unwrap();
        }
        retain_focused_parent(&mut outbox, &mut source).await;
        let selection = selection(&source);
        let (capture_directory, manifest) = collect_original(&mut outbox, source, records, selection, shutdown).await;
        Self { outbox: Some(outbox), directory, capture_directory, manifest, _temporary: temporary }
    }

    pub fn original_reference(&self) -> DiagnosticCaptureRef {
        DiagnosticCaptureRef {
            capture: self.manifest.capture.clone(),
            manifest_sha256: digest(&std::fs::read(self.capture_directory.join("manifest.json")).unwrap()),
            source_scope_id: self.manifest.description.as_ref().unwrap().captured_range().unwrap().scope_id,
        }
    }
    /// Add another genuine sealed fixture original through the same capture/close owner and journal.
    pub async fn append_original(
        &mut self,
        mut source: CollectionDescription,
        records: Vec<TelemetryRecord>,
    ) -> DiagnosticCaptureRef {
        let outbox = self.outbox.as_mut().unwrap();
        retain_focused_parent(outbox, &mut source).await;
        let selected = selection(&source);
        let collection = source.observation.collection();
        let (directory, manifest) =
            Box::pin(collect_original(outbox, source, records, selected, SourceShutdown::Quiescent)).await;
        assert_eq!(
            manifest.status,
            CollectionStatus::Complete,
            "appended {collection:?}: integrity={}, queries={:?}, plans={:?}, unavailable_payloads={}",
            manifest.references.integrity_complete,
            manifest.references.queries,
            manifest.references.plans,
            manifest.references.unavailable_payloads
        );
        self.capture_directory = directory;
        self.manifest = manifest;
        self.original_reference()
    }

    pub fn original_bytes(&self) -> u64 {
        std::fs::metadata(self.capture_directory.join("manifest.json")).unwrap().len()
            + self.manifest.files.iter().map(|file| u64::from(file.bytes)).sum::<u64>()
    }
}

fn selection(source: &CollectionDescription) -> CollectionSelection {
    match &source.observation {
        SourceObservation::MssqlManagedInstanceResources(history) => {
            CollectionSelection::ResourceHistory { from: history.from.clone(), until: history.until.clone() }
        }
        SourceObservation::MssqlPoolSettings { .. } => CollectionSelection::ResourcePoolSettings,
        SourceObservation::Activity(_) => CollectionSelection::Activity,
        SourceObservation::TableMemory { target, .. } => CollectionSelection::TableMemory { target: target.clone() },
        SourceObservation::FullTextPopulations { target, .. } => {
            CollectionSelection::FullTextPopulations { target: target.clone() }
        }
        SourceObservation::ColumnstoreRowgroups { target, .. } => {
            CollectionSelection::ColumnstoreRowgroups { target: target.clone() }
        }
        SourceObservation::FocusedBlocking { result, .. } => {
            CollectionSelection::FocusedBlocking { focus: Box::new(result.focus.clone()) }
        }
        SourceObservation::MetadataLockContext { result, execution, .. } => CollectionSelection::SourceRead {
            focus: Box::new(SourceReadFocus::for_metadata_lock_request(result.request.focus.clone()).unwrap()),
            execution: Box::new(
                SourceReadExecutionScope::from_context(SourceReadQuestion::MetadataLockContext, execution).unwrap(),
            ),
        },
        SourceObservation::FocusedRequestMemory { result, .. } => {
            CollectionSelection::FocusedRequestMemory { focus: Box::new(result.focus.clone()) }
        }
        SourceObservation::QueryHistory(history) => CollectionSelection::QueryHistory {
            from: history.interval.from.clone(),
            until: history.interval.until.clone(),
        },
        SourceObservation::MaintenanceProgress { result, .. } => CollectionSelection::MaintenanceProgress {
            focus: result.focus.clone().expect("focused native maintenance fixture"),
        },
        SourceObservation::CurrentRequestWork { result, .. } => CollectionSelection::CurrentRequestWork {
            focus: result.focus.clone().expect("focused native current-work fixture"),
        },
        observation if observation.snapshot().is_some() => {
            CollectionSelection::Snapshot { dataset: observation.snapshot().unwrap().dataset }
        }
        _ => panic!("local fixture requires an original history, Activity or snapshot read"),
    }
}
async fn collect_original(
    outbox: &mut Outbox,
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    selection: CollectionSelection,
    shutdown: SourceShutdown,
) -> (PathBuf, CollectionManifest) {
    collect_original_mode(outbox, source, records, selection, shutdown, None, "local-evidence").await
}
async fn collect_original_mode(
    outbox: &mut Outbox,
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    selection: CollectionSelection,
    shutdown: SourceShutdown,
    acquisition: Option<SourceAcquisitionCase>,
    connection: &str,
) -> (PathBuf, CollectionManifest) {
    let request = CollectionRequest {
        subject: match &source.target {
            CollectionTarget::ConfiguredConnection(_) => panic!("native fixture cannot represent collector control"),
            CollectionTarget::Database(_) => CollectionSubject::Database,
            CollectionTarget::Shared(resource) => CollectionSubject::Shared { resource: resource.clone() },
        },
        selection,
        budget: CollectionBudget::default(),
    };
    let closed = Arc::new(AtomicBool::new(false));
    let factory = OriginalSource { source, records, shutdown, closed: closed.clone(), acquisition };
    let mut lease = outbox.reserve(connection, &request).await.unwrap();
    let capture_directory = lease.directory().to_owned();
    let result = capture::collect(
        &culprit_read::admission::SourceGate::default(),
        &factory,
        request,
        &capture_directory,
        lease.identity().clone(),
        &CollectionCancellation::default(),
        Some(&mut lease),
    )
    .await;
    assert!(closed.load(Ordering::SeqCst));
    if acquisition == Some(SourceAcquisitionCase::Failed) {
        assert_eq!(result.unwrap_err().code, culprit_core::ErrorCode::PermissionDenied);
    } else {
        result.unwrap();
    }
    lease.finish().await.unwrap();
    let manifest = Outbox::inspect_manifest(
        outbox_path(&capture_directory),
        capture_directory.file_name().unwrap().to_str().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    (capture_directory, manifest)
}

/// Controlled focused rows acquire their real prerequisite before the selected capture is sealed. This is a
/// local ownership fixture, not native execution/cost qualification or a replacement source of actor identity.
async fn retain_focused_parent(outbox: &mut Outbox, source: &mut CollectionDescription) {
    let focus = match &mut source.observation {
        SourceObservation::FocusedBlocking { result, .. } => &mut result.focus,
        SourceObservation::MetadataLockContext { result, .. } => &mut result.request.focus,
        SourceObservation::FocusedRequestMemory { result, .. } => &mut result.focus,
        SourceObservation::MaintenanceProgress { result, .. }
        | SourceObservation::CurrentRequestWork { result, .. } => {
            result.focus.as_mut().expect("focused fixture retains its original actor")
        }
        _ => return,
    };
    let original = CollectionDescription {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        source: focus.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some(focus.epoch.clone()),
            database_writable: focus.database_writable,
            observed_from: focus.original_observed_at.clone(),
            observed_until: focus.original_observed_at.clone(),
            at: focus.original_observed_at.clone(),
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::observed(focus.source.clone(), 1, false),
        }),
    };
    let (directory, manifest) = collect_original(
        outbox,
        original,
        vec![TelemetryRecord::from(focus.original.clone())],
        CollectionSelection::Activity,
        SourceShutdown::Quiescent,
    )
    .await;
    assert_eq!(manifest.status, CollectionStatus::Complete);
    let file = manifest.files.iter().find(|file| file.stream == TelemetryStream::ActivityObservations).unwrap();
    let rows = std::fs::read_to_string(directory.join(&file.file)).unwrap();
    let row: ExportedRecord = serde_json::from_str(rows.lines().next().unwrap()).unwrap();
    let parent_source = manifest.description.as_ref().unwrap();
    let reference = DiagnosticEvidenceRef::from_export(
        parent_source,
        &digest(&std::fs::read(directory.join("manifest.json")).unwrap()),
        &row,
        0,
    )
    .unwrap();
    let TelemetryRecord::Activity(original) = row.record else { panic!() };
    focus.parent = reference;
    focus.original = *original;
    focus.statement_identity = focus.original.statement_identity.clone().unwrap();
    assert!(focus.matches_parent(parent_source, &focus.original, &focus.parent));
    if let SourceObservation::MetadataLockContext { result, focus_key, .. } = &mut source.observation {
        *focus_key = SourceReadFocus::for_metadata_lock_request(result.request.focus.clone()).unwrap().key().unwrap();
    }
}

fn outbox_path(directory: &std::path::Path) -> &std::path::Path {
    directory.parent().unwrap().parent().unwrap()
}
#[derive(Clone)]
struct OriginalSource {
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    shutdown: SourceShutdown,
    closed: Arc<AtomicBool>,
    acquisition: Option<SourceAcquisitionCase>,
}
#[async_trait::async_trait]
impl SourceFactory for OriginalSource {
    async fn open(&self, request: &CollectionRequest) -> culprit_core::Result<Box<dyn TelemetrySource>> {
        assert!(self.source.matches_request(request));
        Ok(Box::new(self.clone()))
    }
}
#[async_trait::async_trait]
impl TelemetrySource for OriginalSource {
    async fn describe(&mut self) -> culprit_core::Result<CollectionDescription> {
        if self.acquisition.is_some_and(|case| case != SourceAcquisitionCase::Complete) {
            return Err(culprit_core::Error::new(
                culprit_core::ErrorCode::PermissionDenied,
                "controlled metadata refusal",
            ));
        }
        // The acquisition-only fixture returns a genuine complete zero-row header, not a fabricated workload.
        let mut source = self.source.clone();
        if self.acquisition.is_some() {
            let SourceObservation::QueryHistory(history) = &mut source.observation else { panic!() };
            history.population_catalog = None;
            history.population_binding = QueryHistoryPopulationBinding::Unknown;
        }
        Ok(source)
    }
    fn query_count(&self) -> u32 {
        if self.acquisition.is_some() { 100 } else { 0 }
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.acquisition.map(|case| {
            let mut metrics = culprit_evidence::analytical::test_support::native_source_overhead_read_metrics();
            if case == SourceAcquisitionCase::Unconfirmed {
                metrics.active_tasks = 1;
            }
            metrics
        })
    }
    async fn read_page(
        &mut self,
        stream: TelemetryStream,
        cursor: Option<&str>,
    ) -> culprit_core::Result<CollectionRead> {
        assert!(cursor.is_none());
        Ok(CollectionRead::Page {
            records: self.records.iter().filter(|row| row.stream() == stream).cloned().collect(),
            next_cursor: None,
        })
    }
    async fn finish(&mut self) -> culprit_core::Result<SourceCompletion> {
        Ok(match &self.source.observation {
            // History with a retained acquisition owner requires the typed final confirmation, even
            // when this controlled source's original owner and final observed bounds are unchanged.
            SourceObservation::QueryHistory(history) if history.acquisition_owner.is_some() => {
                SourceCompletion::QueryHistory(Box::new(history.clone()))
            }
            _ => SourceCompletion::Verified,
        })
    }
    async fn close(&mut self) -> SourceShutdown {
        self.closed.store(true, Ordering::SeqCst);
        self.shutdown
    }
}

mod diagnostics;
