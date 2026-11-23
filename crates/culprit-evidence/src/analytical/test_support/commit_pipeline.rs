//! Shared controlled canonical facts. No source/native workload or cost qualification is manufactured.
use super::*;
mod intervals;
use culprit_core::{Engine, FileKind, MeasurementScope, ResetMarker};
pub use intervals::{CommitPipelineIntervalCase, commit_pipeline_interval_fixture};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitPipelineCase {
    Exact,
    SingleLifetime,
    Partial,
    Denied,
    ForeignMember,
    Restart,
    ReusedTransaction,
    DecreasedBytes,
    NativeClockBackwards,
    UnknownRole,
    ChangedRole,
    UnknownFileBasis,
    LogCapacityDenied,
    FileReplaced,
    DuplicateFile,
    MissingActor,
    Postgres17,
    Postgres18,
    MysqlTotals,
}
pub struct CommitPipelineOriginalFixture {
    pub source: CollectionDescription,
    pub records: Vec<TelemetryRecord>,
}
pub struct CommitPipelineFixture {
    pub originals: Vec<CommitPipelineOriginalFixture>,
}

pub fn commit_pipeline_fixture(case: CommitPipelineCase) -> CommitPipelineFixture {
    if matches!(case, CommitPipelineCase::Postgres17 | CommitPipelineCase::Postgres18 | CommitPipelineCase::MysqlTotals)
    {
        return other_provider(case);
    }
    let fixture = request_memory_investigation_fixture();
    let mut source = fixture.baseline_source;
    let (files_source, _) = storage_evidence(StorageCase::FileCensus);
    source.target = files_source.target;
    // A same-route catalog fixture alone has no native database generation. Declare the controlled
    // original generation before deriving every endpoint, body identity and immutable row receipt.
    let CollectionTarget::Database(target) = &mut source.target else { unreachable!() };
    target.incarnation = Some("controlled-original-commit-database-generation".into());
    let database = source.target.database().unwrap().database.clone();
    let SourceObservation::Activity(header) = &mut source.observation else { unreachable!() };
    header.at = "2020-01-01T12:00:01Z".into();
    header.observed_from = header.at.clone();
    header.observed_until = header.at.clone();
    header.observations = 2;
    header.native_rows = 2;
    header.coverage = StateSourceCoverage::collected(source.source.clone(), 2);
    let TelemetryRecord::Activity(mut victim) = fixture.baseline_record.record else { unreachable!() };
    victim.database = Some(database.clone());
    victim.elapsed_ms = Some("100000".into());
    victim.blocking_status = ActivityBlockingStatus::Known;
    victim.waiting_on_lock = Some(false);
    victim.wait = Some(ActivityWait {
        class: None,
        native_type: Some("WRITELOG".into()),
        elapsed_ms: Some("37".into()),
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    victim.client = Some(ActivityClient {
        application: Some("native-commit-victim".into()),
        user: Some("native-victim-user".into()),
        host: None,
    });
    let mut producer = victim.as_ref().clone();
    producer.key.session.id = "77".into();
    producer.key.session.generation = "2020-01-01T10:00:00".into();
    producer.key.request = None;
    producer.key.transaction = Some("900".into());
    producer.idle = true;
    producer.state = "sleeping".into();
    producer.scope = ActivityScopeBasis::TransactionDatabase;
    producer.open_transactions = Some(1);
    producer.elapsed_ms = None;
    producer.request_counters = None;
    producer.statement_identity = None;
    producer.wait = None;
    producer.client = Some(ActivityClient {
        application: Some("native-log-producer".into()),
        user: Some("native-producer-user".into()),
        host: None,
    });
    // Deliberately readable prior text is not proof of a log-generating/acquiring statement.
    producer.statement =
        DefinitionPayload::sql("UPDATE previous_operation SET value='commit-private-canary'", Engine::Mssql);
    let transaction = MssqlCleanupTransaction {
        session: producer.key.session.clone(),
        transaction: NativeActivityIdentity { id: "900".into(), generation: "2020-01-01T13:59:00".into() },
        observed_time_without_offset: "2020-01-01T14:00:00".into(),
        age_seconds: Some("60".into()),
        program: Some("native-log-producer".into()),
        native_status: Some("sleeping".into()),
        log_record_count: Some("1".into()),
        log_bytes_used: Some("100".into()),
        begin_lsn: None,
        row_version_participant: false,
        snapshot_isolation: None,
        transaction_sequence_number: None,
        versioning_age_seconds: None,
    };
    if case == CommitPipelineCase::MissingActor {
        producer.key.transaction = Some("901".into());
    }
    let primary = CommitPipelineOriginalFixture {
        source: source.clone(),
        records: vec![TelemetryRecord::Activity(victim), producer.into()],
    };
    let before = transaction_point(&source, &transaction, "2020-01-01T12:00:00Z");
    let mut changed = transaction;
    changed.observed_time_without_offset = "2020-01-01T14:00:02".into();
    changed.log_bytes_used = Some("116".into());
    changed.log_record_count = Some("2".into());
    changed.age_seconds = Some("62".into());
    if case == CommitPipelineCase::ReusedTransaction {
        changed.transaction.generation = "2020-01-01T14:00:01".into();
        changed.age_seconds = Some("1".into());
    }
    if case == CommitPipelineCase::DecreasedBytes {
        changed.log_bytes_used = Some("17".into());
    }
    if case == CommitPipelineCase::NativeClockBackwards {
        changed.observed_time_without_offset = "2020-01-01T13:59:59".into();
        changed.age_seconds = Some("59".into());
    }
    let mut after = transaction_point(&source, &changed, "2020-01-01T12:00:02Z");
    mutate_after(&mut after, case);
    let storage_case = match case {
        CommitPipelineCase::UnknownFileBasis => StorageCase::FileCensusUnknownBasis,
        CommitPipelineCase::LogCapacityDenied => StorageCase::FileCensusLogDenied,
        _ => StorageCase::FileCensus,
    };
    let files_before = files_point(&source, storage_case, false);
    let mut files_after = files_point(&source, storage_case, true);
    mutate_after(&mut files_after, case);
    if matches!(case, CommitPipelineCase::FileReplaced | CommitPipelineCase::DuplicateFile) {
        for row in &mut files_after.records {
            if let TelemetryRecord::Operational(point) = row {
                if let OperationalFact::MssqlDatabaseFile(file) = &mut point.fact {
                    if file.kind == FileKind::Log {
                        if case == CommitPipelineCase::FileReplaced {
                            file.file_id = "2001".into();
                            file.file_guid = Some("new-native-file-generation".into());
                        } else {
                            file.file_id = "1".into();
                        }
                        point.key = file.key().unwrap();
                    }
                }
            }
        }
    }
    let mut originals = vec![primary, before];
    if case != CommitPipelineCase::SingleLifetime {
        originals.push(after);
    }
    originals.push(files_before);
    if case != CommitPipelineCase::SingleLifetime {
        originals.push(files_after);
    }
    CommitPipelineFixture { originals }
}
fn transaction_point(
    primary: &CollectionDescription,
    point: &MssqlCleanupTransaction,
    at: &str,
) -> CommitPipelineOriginalFixture {
    let row = OperationalObservation {
        source: culprit_core::source_id::OPEN_TRANSACTIONS.into(),
        key: "oldest-cleanup-transaction".into(),
        fact: OperationalFact::MssqlCleanupHorizon(MssqlCleanupHorizon {
            oldest_confirmed: Some(Box::new(point.clone())),
            oldest_unattributed_versioning: None,
        }),
    };
    let source = CollectionDescription {
        target: primary.target.clone(),
        partition: primary.partition.clone(),
        source: "commit-cleanup-original".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::LocalResource {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::Cleanup,
                observed_from: at.into(),
                observed_until: at.into(),
                records: 1,
                sources: vec![StateSourceCoverage::collected(row.source.clone(), 1)],
            },
            database_writable: true,
        },
    };
    CommitPipelineOriginalFixture { source, records: vec![TelemetryRecord::Operational(row)] }
}
fn files_point(primary: &CollectionDescription, case: StorageCase, after: bool) -> CommitPipelineOriginalFixture {
    let (mut source, mut records) = storage_evidence(case);
    source.target = primary.target.clone();
    source.partition = primary.partition.clone();
    let SourceObservation::Snapshot(mut header) = source.observation else { unreachable!() };
    header.observed_from = if after { "2020-01-01T12:00:02Z" } else { "2020-01-01T12:00:00Z" }.into();
    header.observed_until = if after { "2020-01-01T12:00:02.9Z" } else { "2020-01-01T12:00:00.9Z" }.into();
    for record in &mut records {
        let TelemetryRecord::Operational(row) = record else { unreachable!() };
        match &mut row.fact {
            OperationalFact::MssqlDatabaseFile(file) => {
                file.observed_at = if after { "2020-01-01T12:00:02.5Z" } else { "2020-01-01T12:00:00.5Z" }.into();
                if after && file.kind == FileKind::Log {
                    file.size_pages = "48".into();
                }
            }
            OperationalFact::Space(space) => {
                // Filegroup aggregates and file rows share their original catalog batch clock.
                // The independent DMV log-space point keeps its separate later native timestamp.
                space.observed_at = match space.population {
                    StoragePopulation::WritableOnlineDataFilegroups => {
                        if after {
                            "2020-01-01T12:00:02.5Z"
                        } else {
                            "2020-01-01T12:00:00.5Z"
                        }
                    }
                    StoragePopulation::DatabaseTransactionLog => {
                        if after {
                            "2020-01-01T12:00:02.75Z"
                        } else {
                            "2020-01-01T12:00:00.75Z"
                        }
                    }
                    _ => unreachable!("the existing file-census fixture has only catalog and DMV-log populations"),
                }
                .into();
            }
            _ => unreachable!(),
        }
    }
    source.observation = SourceObservation::Snapshot(header);
    CommitPipelineOriginalFixture { source, records }
}
fn mutate_after(original: &mut CommitPipelineOriginalFixture, case: CommitPipelineCase) {
    match case {
        CommitPipelineCase::ForeignMember => {
            if let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut original.source.partition {
                member.native.id = "foreign-native-member".into();
            }
        }
        CommitPipelineCase::Restart => {
            if let SourcePartition::InstanceLocal { epoch, .. } = &mut original.source.partition {
                *epoch = Some("another-native-process".into());
            }
        }
        CommitPipelineCase::UnknownRole => {
            if let SourceObservation::LocalResource { snapshot, .. } = original.source.observation.clone() {
                original.source.observation = SourceObservation::Snapshot(snapshot);
            }
        }
        CommitPipelineCase::ChangedRole => {
            if let SourceObservation::LocalResource { database_writable, .. } = &mut original.source.observation {
                *database_writable = false;
            }
        }
        CommitPipelineCase::Partial | CommitPipelineCase::Denied => {
            let header = match &mut original.source.observation {
                SourceObservation::LocalResource { snapshot, .. } | SourceObservation::Snapshot(snapshot) => snapshot,
                _ => unreachable!(),
            };
            if case == CommitPipelineCase::Partial {
                for coverage in &mut header.sources {
                    coverage.state = culprit_core::SourceState::Partial;
                    coverage.complete_scope = false;
                }
            } else {
                for coverage in &mut header.sources {
                    *coverage = StateSourceCoverage::unavailable(
                        coverage.source.clone(),
                        culprit_core::ErrorCode::PermissionDenied,
                        None,
                    );
                }
                header.records = 0;
                original.records.clear();
            }
        }
        _ => {}
    }
}

fn other_provider(case: CommitPipelineCase) -> CommitPipelineFixture {
    let (captured, records) = blocking_evidence(BlockingCase::Matched);
    let SourceObservation::FocusedBlocking { result, .. } = captured.observation else { unreachable!() };
    let mut source = CollectionDescription {
        target: result.focus.target.clone(),
        partition: result.focus.partition.clone(),
        source: result.focus.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some(result.focus.epoch.clone()),
            database_writable: Some(true),
            at: "2020-01-01T12:00:01Z".into(),
            observed_from: "2020-01-01T12:00:01Z".into(),
            observed_until: "2020-01-01T12:00:01Z".into(),
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            scope: MeasurementScope::Database,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(result.focus.source.clone(), 1),
        }),
    };
    let TelemetryRecord::Activity(mut actor) = records[0].clone() else { unreachable!() };
    actor.waiting_on_lock = Some(false);
    actor.wait.as_mut().unwrap().owners.clear();
    actor.wait.as_mut().unwrap().native_type =
        Some(if case == CommitPipelineCase::Postgres17 { "IO: WALSync" } else { "IO: WalSync" }.into());
    let native_source = if case == CommitPipelineCase::Postgres17 { "pg_stat_wal" } else { "pg_stat_io" };
    let key = if case == CommitPipelineCase::Postgres17 { "wal_sync_time" } else { "fsync_time" };
    if case == CommitPipelineCase::MysqlTotals {
        let database = source.target.database().unwrap();
        source.target = culprit_core::DatabaseIdentity::server(
            Engine::Mysql,
            &database.database,
            "fixture.invalid",
            3306,
            Some("mysql-original-database".into()),
        )
        .into();
        if let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut source.partition {
            member.engine = Engine::Mysql;
        }
        actor.wait.as_mut().unwrap().native_type = Some("Waiting for handler commit".into());
        actor.session_context = None;
    }
    let primary =
        CommitPipelineOriginalFixture { source: source.clone(), records: vec![TelemetryRecord::Activity(actor)] };
    let native_source = if case == CommitPipelineCase::MysqlTotals { "global-status" } else { native_source };
    let key = if case == CommitPipelineCase::MysqlTotals { "Innodb_os_log_written" } else { key };
    let unit = if case == CommitPipelineCase::MysqlTotals { CounterUnit::Bytes } else { CounterUnit::Milliseconds };
    let mut sample = NativeCounterSample::gauge(key, 0, unit, MeasurementScope::Server, native_source, key);
    sample.nature = CounterNature::Cumulative;
    sample.reset = ResetMarker::Unknown;
    let mut set = NativeCounterSet {
        identity: NativeIdentity::new(native_source),
        scope: MeasurementScope::Server,
        counters: vec![sample],
        ratios: vec![],
        ratio_gaps: vec![],
        properties: Default::default(),
    };
    if case == CommitPipelineCase::Postgres18 {
        set.properties.insert("object".into(), "wal".into());
        set.properties.insert("backend_type".into(), "client backend".into());
    }
    let row = OperationalObservation {
        source: native_source.into(),
        key: set.key().unwrap(),
        fact: OperationalFact::CounterSet(Box::new(set)),
    };
    let SourcePartition::InstanceLocal { member: Some(owner), .. } = &source.partition else { unreachable!() };
    let context = CommitPipelineOriginalFixture {
        source: CollectionDescription {
            target: CollectionTarget::Shared(owner.clone()),
            partition: source.partition.clone(),
            source: "commit-instance-original".into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::Snapshot(StateSnapshotDescription {
                dataset: SnapshotDataset::Operational,
                observed_from: "2020-01-01T12:00:00Z".into(),
                observed_until: "2020-01-01T12:00:02Z".into(),
                records: 1,
                sources: vec![StateSourceCoverage::collected(row.source.clone(), 1)],
            }),
        },
        records: vec![TelemetryRecord::Operational(row)],
    };
    CommitPipelineFixture { originals: vec![primary, context] }
}
