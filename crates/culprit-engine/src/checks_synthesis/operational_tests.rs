//! Warehouse synthesis for database-scoped Operational, PlanCacheSummary, Cleanup, and enriched FocusedBlocking causes.
use super::*;
use crate::collected_evidence::RetainedEvidenceFixture as Fixture;
use culprit_evidence::{
    analytical::{
        evidence::{EvidenceFamily, EvidenceReadBudget, EvidenceReader},
        indexes::ProjectionIndexes,
        test_support::{self, BlockingCase, CleanupCase, ReadOnlyFixture},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::reopen},
};
use std::sync::atomic::{AtomicU64, Ordering};

const RECORDED_AT: &str = "2020-01-01T12:02:00Z";

fn display() -> DerivedDisplayContext {
    DerivedDisplayContext {
        connection_alias: "Approved operational alias".into(),
        environment: None,
        database_name: Some("AppDb".into()),
    }
}

fn policy() -> ChecksSynthesizer {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "controlled-operational-rules-1").unwrap()
}

async fn seal_operational(source: CollectionDescription, records: Vec<TelemetryRecord>) -> EvidenceCapture {
    static NEXT: AtomicU64 = AtomicU64::new(8_600_000);
    let fixture = ReadOnlyFixture::capture_at(
        source,
        records,
        NEXT.fetch_add(1, Ordering::SeqCst),
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let transport = fixture.transport();
    let recovery = ObjectRecoveryReader::open(transport.clone(), fixture.contract).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store, Arc::new(ProjectionIndexes::new(transport)));
    reader
        .read_capture(&range, &[EvidenceFamily::OperationalSamples], EvidenceReadBudget::default(), None)
        .await
        .unwrap()
}

fn mssql_database_base(
    dataset: SnapshotDataset,
    sources: Vec<StateSourceCoverage>,
    records: u32,
) -> CollectionDescription {
    let (mut base, _) = test_support::cleanup_evidence(CleanupCase::LocalLoggedGenerator);
    let snapshot = base.observation.snapshot().unwrap().clone();
    base.observation = SourceObservation::Snapshot(StateSnapshotDescription {
        dataset,
        observed_from: snapshot.observed_from,
        observed_until: snapshot.observed_until,
        records,
        sources,
    });
    assert!(base.partition_is_consistent());
    base
}

#[tokio::test]
async fn operational_log_space_capture_synthesizes_log_reuse_blocked_and_storage_capacity_in_gold() {
    let source = mssql_database_base(
        SnapshotDataset::Operational,
        vec![StateSourceCoverage::collected(source_id::LOG_CAPACITY.into(), 1)],
        1,
    );
    let db_name = source.target.database().unwrap().database.clone();
    let observed_at = source.observation.snapshot().unwrap().observed_until.clone();
    let space = OperationalSpace {
        kind: FileKind::Log,
        population: StoragePopulation::DatabaseTransactionLog,
        observed_at,
        database: Some(db_name),
        scope: FileScope::Database,
        native_name: None,
        total_bytes: Some("1000000000".into()),
        used_bytes: Some("920000000".into()),
        limit_bytes: Some("1000000000".into()),
        growth_enabled: Some(false),
        reuse_wait: Some("ACTIVE_TRANSACTION".into()),
        parts: vec![],
    };
    let records = vec![TelemetryRecord::Operational(OperationalObservation {
        source: source_id::LOG_CAPACITY.into(),
        key: "database-transaction-log".into(),
        fact: OperationalFact::Space(space),
    })];
    let capture = seal_operational(source, records).await;
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    output.validate().unwrap();
    assert_eq!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
    assert_eq!(output.description.finding_evaluations.assessments[0].scope, DerivedFindingScope::Operational);

    let log_reuse = output
        .findings
        .iter()
        .find(|row| row.finding.check == "log-reuse-blocked")
        .expect("log-reuse-blocked synthesized from Operational capture");
    assert_eq!(log_reuse.finding.severity, Severity::High);
    assert!(log_reuse.finding.title.contains("ACTIVE_TRANSACTION"));
    assert_eq!(log_reuse.inputs, vec![capture.verified_original().unwrap().reference(0).unwrap()]);
    assert!(output.findings.iter().any(|row| row.finding.check == "storage-capacity"));
}

#[tokio::test]
async fn plan_cache_summary_capture_synthesizes_single_use_plan_cache_bloat_without_workload_text() {
    let entries = [
        ("plan-cache-total-entries", "total_plans", 15_000_u64, CounterUnit::Count),
        ("plan-cache-total-bytes", "total_bytes", 5_368_709_120_u64, CounterUnit::Bytes),
        ("plan-cache-adhoc-lookup-one-entries", "adhoc_lookup_one_entries", 12_000_u64, CounterUnit::Count),
        ("plan-cache-adhoc-lookup-one-bytes", "adhoc_lookup_one_bytes", 3_221_225_472_u64, CounterUnit::Bytes),
    ];
    let source = mssql_database_base(
        SnapshotDataset::PlanCacheSummary,
        vec![StateSourceCoverage::collected(source_id::PLAN_CACHE_USE.into(), entries.len() as u32)],
        entries.len() as u32,
    );
    let records = entries
        .into_iter()
        .map(|(key, name, value, unit)| {
            TelemetryRecord::Operational(OperationalObservation {
                source: source_id::PLAN_CACHE_USE.into(),
                key: key.into(),
                fact: OperationalFact::Counter(NativeCounterSample::gauge(
                    key,
                    value,
                    unit,
                    MeasurementScope::Database,
                    "sys.dm_exec_cached_plans",
                    name,
                )),
            })
        })
        .collect();
    let capture = seal_operational(source, records).await;
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    output.validate().unwrap();
    let bloat = output
        .findings
        .iter()
        .find(|row| row.finding.id == "query-family:plan-cache")
        .expect("single-use plan-cache bloat synthesized without query text");
    assert_eq!(bloat.finding.severity, Severity::Medium);
    assert_eq!(bloat.inputs.len(), 4);
    let qf_eval =
        output.description.finding_evaluations.checks.iter().find(|eval| eval.check == "query-family").unwrap();
    assert_eq!(qf_eval.status, CheckStatus::Findings);

    let healthy_entries = [
        ("plan-cache-total-entries", "total_plans", 500_u64, CounterUnit::Count),
        ("plan-cache-total-bytes", "total_bytes", 52_428_800_u64, CounterUnit::Bytes),
        ("plan-cache-adhoc-lookup-one-entries", "adhoc_lookup_one_entries", 10_u64, CounterUnit::Count),
        ("plan-cache-adhoc-lookup-one-bytes", "adhoc_lookup_one_bytes", 104_857_u64, CounterUnit::Bytes),
    ];
    let healthy_source = mssql_database_base(
        SnapshotDataset::PlanCacheSummary,
        vec![StateSourceCoverage::collected(source_id::PLAN_CACHE_USE.into(), healthy_entries.len() as u32)],
        healthy_entries.len() as u32,
    );
    let healthy_records = healthy_entries
        .into_iter()
        .map(|(key, name, value, unit)| {
            TelemetryRecord::Operational(OperationalObservation {
                source: source_id::PLAN_CACHE_USE.into(),
                key: key.into(),
                fact: OperationalFact::Counter(NativeCounterSample::gauge(
                    key,
                    value,
                    unit,
                    MeasurementScope::Database,
                    "sys.dm_exec_cached_plans",
                    name,
                )),
            })
        })
        .collect();
    let healthy_capture = seal_operational(healthy_source, healthy_records).await;
    let healthy_output = policy().synthesize_captures(&healthy_capture, &[], RECORDED_AT, &display()).unwrap();
    let healthy_qf =
        healthy_output.description.finding_evaluations.checks.iter().find(|eval| eval.check == "query-family").unwrap();
    assert_eq!(
        healthy_qf.status,
        CheckStatus::InsufficientEvidence,
        "without query history, absence of plan-cache bloat never falsely marks SQL query-family grouping clean"
    );
}

#[tokio::test]
async fn cleanup_capture_synthesizes_held_cleanup_for_confirmed_database_horizon() {
    let (source, records) = test_support::cleanup_evidence(CleanupCase::LocalLoggedGenerator);
    let capture = seal_operational(source, records).await;
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    output.validate().unwrap();
    let held = output
        .findings
        .iter()
        .find(|row| row.finding.check == "held-cleanup")
        .expect("held-cleanup synthesized from Cleanup capture");
    assert!(!held.inputs.is_empty());
    assert_eq!(held.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
}

#[tokio::test]
async fn idle_open_transaction_and_mssql_object_lock_escalation_enrich_verified_blocking_explanations() {
    let (idle_source, idle_records) = test_support::blocking_evidence(BlockingCase::IdleOwner);
    let idle_fixture = Fixture::new(idle_source, idle_records).await;
    let idle_capture = idle_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let idle_original = idle_capture.verified_original().unwrap();
    let idle_exp = explanations::blocking(&idle_original).unwrap();
    assert!(
        idle_exp.victim_summary.as_deref().is_some_and(|text| text.contains("43") && text.contains("3 min")),
        "{:?}",
        idle_exp.victim_summary
    );
    assert!(idle_exp.mechanism.as_deref().is_some_and(|text| text.contains("idle")));
    assert!(idle_exp.action.as_deref().is_some_and(|text| text.contains("COMMIT")));

    let (unverified_source, unverified_records) = test_support::blocking_evidence(BlockingCase::UnverifiedOriginal);
    let unverified_fixture = Fixture::new(unverified_source, unverified_records).await;
    let unverified_capture = unverified_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let unverified_original = unverified_capture.verified_original().unwrap();
    let unverified_exp = explanations::blocking(&unverified_original).unwrap();
    assert!(unverified_exp.victim_summary.is_none());

    let (mut mssql_source, mut mssql_records) =
        test_support::mssql_metadata_queue_evidence(test_support::MssqlMetadataQueueCase::RunningHolder);
    let holder = mssql_records.remove(2);
    mssql_records.truncate(1);
    {
        let TelemetryRecord::Activity(victim) = &mut mssql_records[0] else { panic!() };
        let TelemetryRecord::Activity(holder_obs) = &holder else { panic!() };
        let holder_owner = ActivityWaitOwner::Session {
            id: holder_obs.key.session.id.clone(),
            generation: Some(holder_obs.key.session.generation.clone()),
        };
        let wait = victim.wait.as_mut().unwrap();
        wait.native_type = Some("LCK_M_X".into());
        wait.owners = vec![holder_owner];
        wait.resources = Some(vec![LockWait {
            session: victim.key.session.id.clone(),
            mode: Some("X".into()),
            resource: Some("OBJECT".into()),
            object: Some("dbo.Orders".into()),
            index: None,
            native: None,
        }]);
    }
    mssql_records.push(holder);
    let mut parent_source = mssql_source.clone();
    {
        let SourceObservation::FocusedBlocking { snapshot, result } = &mut mssql_source.observation else {
            panic!();
        };
        snapshot.native_rows = 2;
        snapshot.observations = 2;
        snapshot.coverage.records = 2;
        let TelemetryRecord::Activity(victim_after) = &mssql_records[0] else { panic!() };
        result.focus.original = (**victim_after).clone();
        let parent_capture = result.focus.parent.capture.clone();
        parent_source.source = source_id::SESSIONS.into();
        parent_source.observation = SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: snapshot.epoch.clone(),
            database_writable: snapshot.database_writable,
            observed_from: "2020-01-01T12:00:00Z".into(),
            at: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:00Z".into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(source_id::SESSIONS.into(), 1),
        });
        let mut parent_victim = mssql_records[0].clone();
        if let TelemetryRecord::Activity(obs) = &mut parent_victim {
            obs.wait.as_mut().unwrap().resources = None;
        }
        let exported = ExportedRecord::new(&parent_source, &parent_capture, parent_victim).unwrap();
        result.focus.parent =
            DiagnosticEvidenceRef::from_export(&parent_source, &digest(b"mssql-object-lock-parent"), &exported, 0)
                .unwrap();
    }
    let mssql_fixture = Fixture::new(mssql_source, mssql_records).await;
    let mssql_capture = mssql_fixture.read_families(&[EvidenceFamily::ActivityObservations]).await;
    let mssql_original = mssql_capture.verified_original().unwrap();
    let mssql_exp = explanations::blocking(&mssql_original).unwrap();
    assert!(
        mssql_exp
            .mechanism
            .as_deref()
            .is_some_and(|text| text.contains("dbo.Orders") && text.contains("OBJECT") && text.contains("5,000")),
        "{:?}",
        mssql_exp.mechanism
    );
    assert!(mssql_exp.action.as_deref().is_some_and(|text| text.contains("TOP (1000)")));
}

struct DeadlockCaptureBuffer {
    source: CollectionDescription,
    original: DiagnosticCaptureRef,
    exported: Vec<ExportedRecord>,
    coverage: ReferenceCoverage,
}

impl DeadlockCaptureBuffer {
    fn new(source: CollectionDescription, observation_id: &str, records: Vec<TelemetryRecord>) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(9_100_000);
        let capture = CaptureIdentity {
            observation_id: observation_id.into(),
            revision: Some(CaptureRevision {
                outbox_id: "deadlock-test-outbox".into(),
                owner_epoch: "1".into(),
                sequence: SEQ.fetch_add(1, Ordering::SeqCst).to_string(),
            }),
        };
        let manifest_sha256 = digest(observation_id.as_bytes());
        let original = DiagnosticCaptureRef {
            capture: capture.clone(),
            manifest_sha256,
            source_scope_id: source.captured_range().unwrap().scope_id,
        };
        let exported = records
            .into_iter()
            .map(|record| ExportedRecord::new(&source, &capture, record).unwrap())
            .collect::<Vec<_>>();
        Self {
            source,
            original,
            exported,
            coverage: ReferenceCoverage { integrity_complete: true, complete: true, ..Default::default() },
        }
    }

    fn projection(&self) -> RetainedDeadlockProjection<'_> {
        let mut ordinals = std::collections::BTreeMap::<&'static str, u32>::new();
        let rows = self
            .exported
            .iter()
            .map(|row| {
                let ord = ordinals.entry(row.record.stream().file_name()).or_insert(0);
                let current = *ord;
                *ord += 1;
                culprit_evidence::original_projection::OriginalProjectionRecord::new(
                    row,
                    current,
                    row.payload_sha256().unwrap(),
                )
            })
            .collect();
        RetainedDeadlockProjection { buffer: self, rows }
    }
}

struct RetainedDeadlockProjection<'a> {
    buffer: &'a DeadlockCaptureBuffer,
    rows: Vec<culprit_evidence::original_projection::OriginalProjectionRecord<'a>>,
}

impl OriginalProjection for RetainedDeadlockProjection<'_> {
    fn source(&self) -> &CollectionDescription {
        &self.buffer.source
    }
    fn original(&self) -> &DiagnosticCaptureRef {
        &self.buffer.original
    }
    fn records(&self) -> &[culprit_evidence::original_projection::OriginalProjectionRecord<'_>] {
        &self.rows
    }
    fn reference_coverage(&self) -> &ReferenceCoverage {
        &self.buffer.coverage
    }
    fn captured_rows(&self, stream: TelemetryStream) -> Option<u32> {
        self.buffer
            .source
            .observation
            .collection()
            .streams()
            .contains(&stream)
            .then(|| self.rows.iter().filter(|row| row.row().record.stream() == stream).count() as u32)
    }
    fn selected_streams(&self) -> Box<dyn Iterator<Item = TelemetryStream> + '_> {
        Box::new(self.buffer.source.observation.collection().streams().iter().copied())
    }
    fn collected_from(&self) -> &str {
        "2020-01-01T12:00:00Z"
    }
    fn collected_until(&self) -> &str {
        "2020-01-01T12:00:01Z"
    }
    fn gaps(&self) -> &[culprit_core::collected_analysis::EvidenceGap] {
        &[]
    }
}

fn sample_deadlock_record() -> TelemetryRecord {
    let mut event = DeadlockEvent {
        position: digest(b"deadlock-occurrence-1"),
        file_id: digest(b"system_health_0_1.xel"),
        file_offset: "4096".into(),
        graph_ordinal: 0,
        at: "2020-01-01T11:55:00.000000000Z".into(),
        processes: vec![
            culprit_core::DeadlockProcess {
                id: "process1".into(),
                session: Some("52".into()),
                victim: true,
                statement: Some("SELECT OrderTotal FROM dbo.Orders WHERE CustomerId = 42".into()),
                wait_mode: Some("S".into()),
                ..Default::default()
            },
            culprit_core::DeadlockProcess {
                id: "process2".into(),
                session: Some("67".into()),
                victim: false,
                statement: Some("UPDATE dbo.Orders SET Status = 'PAID' WHERE OrderId = 99".into()),
                wait_mode: Some("X".into()),
                ..Default::default()
            },
        ],
        resources: vec![
            culprit_core::DeadlockResource {
                id: "keylock1".into(),
                kind: "keylock".into(),
                object: Some("dbo.Orders".into()),
                index: Some("PK_Orders".into()),
                holders: vec![culprit_core::LockHolder { process: "process2".into(), mode: Some("X".into()) }],
                waiters: vec![culprit_core::LockHolder { process: "process1".into(), mode: Some("S".into()) }],
            },
            culprit_core::DeadlockResource {
                id: "keylock2".into(),
                kind: "keylock".into(),
                object: Some("dbo.Orders".into()),
                index: Some("IX_Orders_CustomerId".into()),
                holders: vec![culprit_core::LockHolder { process: "process1".into(), mode: Some("S".into()) }],
                waiters: vec![culprit_core::LockHolder { process: "process2".into(), mode: Some("X".into()) }],
            },
        ],
        pattern: culprit_core::DeadlockPattern::KeyLookup,
    };
    event.redact(Engine::Mssql);
    TelemetryRecord::Deadlock(event)
}

#[test]
fn shared_and_database_deadlock_captures_project_both_gold_incidents_and_database_findings() {
    let instance = SharedResourceIdentity {
        engine: Engine::Mssql,
        kind: SharedResourceKind::Instance,
        namespace: NativeIdentity::new("sql-host").qualified("kind", "sql-server-network-machine"),
        native: NativeIdentity::new("sql-host\\PROD").qualified("kind", "sql-server-instance"),
    };
    let shared_source = CollectionDescription {
        target: CollectionTarget::Shared(instance.clone()),
        partition: SourcePartition::InstanceLocal {
            member: Some(instance),
            epoch: Some(digest(b"system-health-epoch-v3")),
        },
        source: source_id::DEADLOCKS.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::RetainedEvents(EventHistoryDescription {
            from: "2020-01-01T11:00:00Z".into(),
            until: "2020-01-01T12:00:00Z".into(),
            observed_at: "2020-01-01T12:00:01Z".into(),
            retained_from: None,
            history_complete: false,
        }),
    };
    assert_eq!(
        diagnostic_original_input_qualification(Some(&shared_source)),
        DiagnosticInputQualification::Qualified { source: DiagnosticSourceKind::Deadlocks }
    );
    let shared_buf =
        DeadlockCaptureBuffer::new(shared_source, "shared-deadlock-capture-1", vec![sample_deadlock_record()]);
    let shared_basis = shared_buf.projection();
    let shared_ref = shared_basis.reference(0).unwrap();
    let update = observe_diagnostic_original(
        shared_basis.source(),
        shared_basis.original(),
        &[DiagnosticOriginalRow { reference: &shared_ref, record: &shared_basis.records()[0].row().record }],
        None,
        &[],
        "2020-01-01T12:00:02Z",
    )
    .unwrap();
    assert_eq!(update.revisions.len(), 1);
    let settled = update.revisions[0].episode.finish_original_observation_planning().unwrap();
    let revision = DiagnosticRevision::between(None, settled.clone(), "2020-01-01T12:00:03Z").unwrap().unwrap();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: settled.scope.target.clone(),
        input: shared_basis.original().clone(),
        part: 0,
        parts: 1,
        episodes: vec![],
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let diag_source = CollectionDescription {
        target: settled.scope.target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: shared_basis.original().clone(),
            step_id: digest(b"deadlock-gold-step"),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(settled.scope.key().unwrap(), settled.scope.clone())].into(),
        }),
    };
    let diag_buf = DeadlockCaptureBuffer::new(
        diag_source,
        "shared-deadlock-diag-1",
        vec![
            TelemetryRecord::Operational(OperationalObservation {
                source: DIAGNOSTICS_SOURCE.into(),
                key: revision.id.clone(),
                fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
            }),
            TelemetryRecord::DiagnosticCheckpoint(checkpoint),
        ],
    );
    let diag_projection = diag_buf.projection();
    let shared_display = DerivedDisplayContext {
        connection_alias: "Approved instance alias".into(),
        environment: None,
        database_name: None,
    };
    let incident_gold = policy()
        .analyze_originals(
            &[&diag_projection as &dyn OriginalProjection, &shared_basis as &dyn OriginalProjection],
            RECORDED_AT,
            &shared_display,
            vec![],
        )
        .unwrap();
    incident_gold.validate().unwrap();
    assert_eq!(incident_gold.incidents.len(), 1);
    let incident = &incident_gold.incidents[0];
    assert!(incident.victim_summary.contains("dbo.Orders"), "{}", incident.victim_summary);
    assert!(incident.mechanism.as_deref().is_some_and(|m| m.contains("IX_Orders_CustomerId")));
    assert_eq!(incident.affected_queries.len(), 1);
    assert_eq!(incident.candidate_queries.len(), 1);
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);

    let (base_db, _) = test_support::cleanup_evidence(CleanupCase::LocalLoggedGenerator);
    let db_source = CollectionDescription {
        target: base_db.target,
        partition: SourcePartition::Target,
        source: source_id::DEADLOCKS.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::RetainedEvents(EventHistoryDescription {
            from: "2020-01-01T11:00:00Z".into(),
            until: "2020-01-01T12:00:00Z".into(),
            observed_at: "2020-01-01T12:00:01Z".into(),
            retained_from: None,
            history_complete: false,
        }),
    };
    assert_eq!(
        diagnostic_original_input_qualification(Some(&db_source)),
        DiagnosticInputQualification::Qualified { source: DiagnosticSourceKind::Deadlocks }
    );
    let db_buf = DeadlockCaptureBuffer::new(db_source, "db-deadlock-capture-1", vec![sample_deadlock_record()]);
    let db_basis = db_buf.projection();
    let finding_gold =
        policy().analyze_originals(&[&db_basis as &dyn OriginalProjection], RECORDED_AT, &display(), vec![]).unwrap();
    finding_gold.validate().unwrap();
    let deadlock_finding =
        finding_gold.findings.iter().find(|row| row.finding.check == "deadlocks").expect("deadlocks finding");
    assert_eq!(deadlock_finding.inputs, vec![db_basis.reference(0).unwrap()]);
}

#[tokio::test]
async fn database_wait_counters_capture_synthesizes_sync_stats_refresh_in_gold() {
    let source = mssql_database_base(
        SnapshotDataset::WaitCounters,
        vec![StateSourceCoverage::collected(source_id::WAITS.into(), 2)],
        2,
    );
    let records = vec![
        TelemetryRecord::WaitCounter(WaitCounterObservation {
            mechanism: None,
            source: source_id::WAITS.into(),
            wait_type: "WAIT_ON_SYNC_STATISTICS_REFRESH".into(),
            scope: MeasurementScope::Database,
            reset: ResetMarker::Unknown,
            waiting_tasks_count: "18".into(),
            wait_time_ms: "12000".into(),
            signal_wait_time_ms: "120".into(),
            max_wait_time_ms: "2400".into(),
        }),
        TelemetryRecord::WaitCounter(WaitCounterObservation {
            mechanism: None,
            source: source_id::WAITS.into(),
            wait_type: "SLEEP_TASK".into(),
            scope: MeasurementScope::Database,
            reset: ResetMarker::Unknown,
            waiting_tasks_count: "500".into(),
            wait_time_ms: "900000".into(),
            signal_wait_time_ms: "10".into(),
            max_wait_time_ms: "5000".into(),
        }),
    ];
    let fixture = Fixture::new(source, records).await;
    let capture = fixture.read_families(&[EvidenceFamily::WaitCounterSamples]).await;
    let output = policy().synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    output.validate().unwrap();
    assert_eq!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
    let finding = output
        .findings
        .iter()
        .find(|row| row.finding.check == "sync-stats-refresh")
        .expect("sync-stats-refresh synthesized from database WaitCounters capture");
    assert_eq!(finding.inputs, vec![capture.verified_original().unwrap().reference(0).unwrap()]);
    assert_eq!(finding.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
}
