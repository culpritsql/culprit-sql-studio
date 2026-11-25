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
