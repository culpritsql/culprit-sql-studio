//! Controlled streaming reuses the existing scripted native reader; it supplies no native cost approval.
use super::*;
use crate::{progress::test_support, read::QueryRead};
use culprit_evidence::analytical::test_support::baseline_profile_expectation;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const FROM: &str = "2026-11-15T10:00:00Z";
const INITIAL_MEMBER: &str = "2026-11-15T10:00:00.010Z";
const FINAL_METADATA: &str = "2026-11-15T10:00:00.020Z";
const UNTIL: &str = "2026-11-15T10:00:00.030Z";
const START: &str = "2026-11-14T00:00:00Z";
struct Read {
    original: test_support::Reader,
    profile: culprit_read::SourceProfileGuard,
    active: Arc<AtomicBool>,
    retire_after: Option<usize>,
    change_after: Option<usize>,
    generation: u64,
    setup_charged: bool,
    batches: VecDeque<Result<ReadBatch<Row>>>,
    page_rows: usize,
    late_failure: bool,
}
#[async_trait::async_trait]
impl QueryRead for Read {
    fn connection_generation(&self) -> Option<u64> {
        Some(self.generation)
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        self.profile.check_connection(Some(self.generation))?;
        // The fake models the existing reader's one SET charge, never its native cost or SQL implementation.
        self.setup_charged = true;
        let result = self.original.query(sql, limit).await;
        if self.retire_after == Some(self.original.calls.len()) {
            self.active.store(false, Ordering::Release);
        }
        if self.change_after == Some(self.original.calls.len()) {
            self.generation += 1;
        }
        result
    }
}
#[async_trait::async_trait]
impl NativeRead for Read {
    fn queries(&self) -> u32 {
        self.original.queries() + u32::from(self.setup_charged)
    }
    fn metrics(&self) -> Option<ReadWorkMetrics> {
        None
    }
    async fn close(&mut self) -> SourceShutdown {
        self.original.close().await
    }
}
#[async_trait::async_trait]
impl SnapshotIo for Read {
    async fn begin_stream(&mut self, sql: &str, limit: usize) -> Result<()> {
        let rows = self.query(sql, limit).await?;
        if rows.is_empty() {
            self.batches.push_back(Ok(ReadBatch { rows: vec![], complete: !self.late_failure }));
        } else {
            let chunks = rows.chunks(self.page_rows).collect::<Vec<_>>();
            let count = chunks.len();
            for (index, rows) in chunks.into_iter().enumerate() {
                self.batches.push_back(Ok(ReadBatch {
                    rows: rows.to_vec(),
                    complete: index + 1 == count && !self.late_failure,
                }));
            }
        }
        if self.late_failure {
            self.batches.push_back(Err(CollectionStop::SourceUnavailable.error()));
        }
        Ok(())
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Row>> {
        self.batches.pop_front().expect("every bounded native batch is declared")
    }
}
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
        budget: bounded_budget(&CollectionBudget { page_rows: 1, ..Default::default() }),
    }
}
fn settings() -> Vec<Row> {
    vec![
        test_support::row(&[
            ("name", Some("autovacuum")),
            ("setting", Some("on")),
            ("unit", None),
            ("boot_val", Some("on")),
            ("source", Some("configuration file")),
            ("context", Some("sighup")),
            ("pending_restart", Some("f")),
        ]),
        test_support::row(&[
            ("name", Some("work_mem")),
            ("setting", None),
            ("unit", Some("kB")),
            ("boot_val", None),
            ("source", Some("session")),
            ("context", Some("user")),
            ("pending_restart", Some("f")),
        ]),
    ]
}
fn replies(writable: bool, rows: Vec<Row>) -> VecDeque<Result<Vec<Row>>> {
    let role = if writable { "f" } else { "t" };
    VecDeque::from([
        Ok(vec![test_support::metadata_at(180_000, role, START, false, FROM)]),
        Ok(vec![test_support::member_at("127.0.0.1", INITIAL_MEMBER)]),
        Ok(rows),
        Ok(vec![test_support::metadata_at(180_000, role, START, false, FINAL_METADATA)]),
        Ok(vec![test_support::member_at("127.0.0.1", UNTIL)]),
    ])
}
async fn expected(writable: bool) -> NativeReadContext {
    let mut original = test_support::Reader { replies: replies(writable, vec![]), ..Default::default() };
    let metadata = metadata::read(&mut original, "fixture.invalid", 5432).await.unwrap();
    native::local_context::Context::observe(&mut original, &metadata).await.unwrap().unwrap().read_context().unwrap()
}
async fn reader(request: &CollectionRequest, writable: bool) -> (PostgresSnapshotReader<Read>, Arc<AtomicBool>) {
    reader_for(request, writable, expected(writable).await)
}
fn reader_for(
    request: &CollectionRequest,
    writable: bool,
    expected: NativeReadContext,
) -> (PostgresSnapshotReader<Read>, Arc<AtomicBool>) {
    let active = Arc::new(AtomicBool::new(true));
    let flag = active.clone();
    let admission = baseline_profile_expectation(request, &expected, move || flag.load(Ordering::Acquire));
    let profile = culprit_read::SourceProfileGuard::new(request, &admission).unwrap();
    let original = test_support::Reader {
        replies: replies(writable, settings()),
        query_limit: Some(request.budget.max_queries.saturating_sub(1) as usize),
        ..Default::default()
    };
    (
        PostgresSnapshotReader {
            io: Read {
                original,
                profile: profile.clone(),
                active: active.clone(),
                retire_after: None,
                change_after: None,
                generation: 1,
                setup_charged: false,
                batches: VecDeque::new(),
                page_rows: request.budget.page_rows as usize,
                late_failure: false,
            },
            budget: request.budget.clone(),
            recipe: Recipe::Settings,
            host: "fixture.invalid".into(),
            port: 5432,
            metadata: None,
            instance: None,
            memory_access: native::memory::Access::Unsupported,
            acquisition: None,
            generation: None,
            profile: Some(profile),
        },
        active,
    )
}
async fn read_all(source: &mut impl TelemetrySource) -> Result<Vec<TelemetryRecord>> {
    read_stream(source, TelemetryStream::SettingObservations).await
}
async fn read_stream(source: &mut impl TelemetrySource, stream: TelemetryStream) -> Result<Vec<TelemetryRecord>> {
    let mut records = vec![];
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records: page, next_cursor } = source.read_page(stream, cursor.as_deref()).await?
        else {
            panic!()
        };
        records.extend(page);
        cursor = next_cursor;
        if cursor.is_none() {
            return Ok(records);
        }
    }
}
#[tokio::test]
async fn one_guarded_settings_read_uses_six_charges_and_paging_never_dispatches_again() {
    for writable in [true, false] {
        let request = request();
        let (reader, _) = reader(&request, writable).await;
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let initial = source.describe().await.unwrap();
        assert!(initial.target.database().unwrap().incarnation.is_none());
        assert_eq!(initial.partition, SourcePartition::Target);
        let records = read_all(&mut source).await.unwrap();
        assert_eq!(records.len(), 2);
        let final_source = initial.completed(source.finish().await.unwrap()).unwrap().unwrap();
        let execution = final_source.observation.snapshot_acquisition_context().unwrap();
        assert!(execution.has_qualified_local_owner());
        assert_eq!(execution.database_writable, Some(writable));
        assert_eq!(execution.observed_from, canonical_time(FROM).unwrap());
        assert_eq!(execution.observed_until, canonical_time(UNTIL).unwrap());
        assert_eq!(source.query_count(), 6, "one modeled owned SET plus five original native queries");
        let calls = &source.reader().io.original.calls;
        assert_eq!(calls.len(), 5);
        assert_eq!(calls.iter().filter(|sql| sql.contains("FROM pg_catalog.pg_settings")).count(), 1);
        assert_eq!(calls[2], native::settings::capture_query(request.budget.max_snapshot_rows as usize + 1));
        let TelemetryRecord::Setting(nullable) = &records[1] else { panic!() };
        assert_eq!(nullable.value_in_use, None);
        assert_eq!(nullable.scope, culprit_core::MeasurementScope::Session);
        assert!(final_source.observation.snapshot().unwrap().sources[0].complete_scope);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert!(source.reader().io.original.closed);
    }
}
#[tokio::test]
async fn caller_five_cannot_complete_the_mandatory_final_member_and_cannot_refill() {
    let mut request = request();
    request.budget.max_queries = 5;
    let (reader, _) = reader(&request, true).await;
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    read_all(&mut source).await.unwrap();
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 5);
    assert_eq!(source.reader().io.original.calls.len(), 4);
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn empty_completed_read_keeps_the_actual_native_context_without_fabricated_settings() {
    let request = request();
    let (mut reader, _) = reader(&request, true).await;
    reader.io.original.replies[2] = Ok(vec![]);
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    let initial = source.describe().await.unwrap();
    assert!(read_all(&mut source).await.unwrap().is_empty());
    let final_source = initial.completed(source.finish().await.unwrap()).unwrap().unwrap();
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(final_source.observation.snapshot().unwrap().records, 0);
    assert!(final_source.observation.snapshot_acquisition_context().is_some());
}
#[tokio::test]
async fn wrong_expected_database_member_epoch_role_and_missing_initial_owner_admit_no_population() {
    for case in ["database", "member", "epoch", "role", "missing"] {
        let request = request();
        let mut context = expected(true).await;
        match case {
            "database" => {
                let CollectionTarget::Database(database) = &mut context.target else { panic!() };
                database.database = "foreign".into();
            }
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut context.partition else {
                    panic!()
                };
                member.native.id = "127.0.0.2".into();
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut context.partition else { panic!() };
                *epoch = Some("other-process".into());
            }
            "role" => context.database_writable = Some(false),
            _ => (),
        }
        let (mut reader, _) = reader_for(&request, true, context);
        if case == "missing" {
            reader.io.original.replies[1] =
                Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.unsupported")));
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        assert_eq!(
            CollectionStop::from_error(&source.describe().await.unwrap_err()),
            CollectionStop::SourceChanged,
            "{case}"
        );
        assert_eq!(source.reader().io.original.calls.len(), 2, "{case}");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn each_original_dispatch_and_final_verification_retain_permanent_retirement_and_transport_binding() {
    for after in 1..=5 {
        let request = request();
        let (mut reader, active) = reader(&request, true).await;
        reader.io.retire_after = Some(after);
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let result = async {
            source.describe().await?;
            read_all(&mut source).await?;
            source.finish().await
        }
        .await;
        assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::SourceChanged, "{after}");
        assert_eq!(source.reader().io.original.calls.len(), after);
        active.store(true, Ordering::Release);
        assert!(source.reader().profile.as_ref().unwrap().ensure_active().is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    for after in 1..=5 {
        let request = request();
        let (mut reader, _) = reader(&request, true).await;
        reader.io.change_after = Some(after);
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let result = async {
            source.describe().await?;
            read_all(&mut source).await?;
            source.finish().await
        }
        .await;
        assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::SourceChanged, "{after}");
        assert_eq!(source.reader().io.original.calls.len(), after);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn changed_final_role_database_process_or_member_cannot_publish_provisional_settings() {
    for case in ["role", "database", "process", "member", "clock", "permission"] {
        let request = request();
        let (mut reader, _) = reader(&request, true).await;
        match case {
            "role" => {
                reader.io.original.replies[3] =
                    Ok(vec![test_support::metadata_at(180_000, "t", START, false, FINAL_METADATA)])
            }
            "process" => {
                reader.io.original.replies[3] =
                    Ok(vec![test_support::metadata_at(180_000, "f", "2026-11-14T01:00:00Z", false, FINAL_METADATA)])
            }
            "database" => {
                let original = test_support::metadata_at(180_000, "f", START, false, FINAL_METADATA);
                let names = [
                    "build",
                    "database_oid",
                    "database",
                    "version",
                    "block_bytes",
                    "standby",
                    "started_at",
                    "observed_at",
                    "full_visibility",
                    "replay_pause_requested_access",
                    "replay_pause_state_access",
                    "pgss_schema",
                    "pgss_columns",
                    "pgss_info",
                ];
                let row = Row::from_fields(names.into_iter().map(|name| {
                    let value =
                        if name == "database" { Some("other-native-database".into()) } else { original.text(name) };
                    (name.into(), value)
                }));
                reader.io.original.replies[3] = Ok(vec![row]);
            }
            "member" => reader.io.original.replies[4] = Ok(vec![test_support::member_at("127.0.0.2", UNTIL)]),
            "clock" => {
                reader.io.original.replies[4] = Ok(vec![test_support::member_at("127.0.0.1", "2026-11-15T09:59:59Z")])
            }
            "permission" => {
                reader.io.original.replies[4] =
                    Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.unsupported")))
            }
            _ => unreachable!(),
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        source.describe().await.unwrap();
        read_all(&mut source).await.unwrap();
        assert!(source.finish().await.is_err(), "{case}");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn late_native_error_and_unconfirmed_shutdown_never_become_a_completed_header() {
    let request = request();
    let (mut failing, _) = reader(&request, true).await;
    failing.io.late_failure = true;
    let mut source = SnapshotSource::new(failing, &request).unwrap();
    source.describe().await.unwrap();
    assert_eq!(
        CollectionStop::from_error(&read_all(&mut source).await.unwrap_err()),
        CollectionStop::SourceUnavailable
    );
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let (mut unsettled, _) = reader(&request, true).await;
    unsettled.io.original.unconfirmed = true;
    let mut source = SnapshotSource::new(unsettled, &request).unwrap();
    source.describe().await.unwrap();
    read_all(&mut source).await.unwrap();
    source.finish().await.unwrap();
    assert_eq!(
        source.close().await,
        SourceShutdown::Unconfirmed,
        "source settlement stays independent of a valid read header"
    );
}
#[tokio::test]
async fn an_actual_retired_profile_refuses_before_authentication_or_the_owned_set_batch() {
    use culprit_core::{ConnectionSpec, Engine, Provider};
    let request = request();
    let active = Arc::new(AtomicBool::new(true));
    let flag = active.clone();
    let admission = baseline_profile_expectation(&request, &expected(true).await, move || flag.load(Ordering::Acquire));
    let provider = crate::PostgresProvider::new(&ConnectionSpec {
        engine: Some(Engine::Postgres),
        host: "no-settings-auth.invalid".into(),
        database: Some("shop".into()),
        user: Some("monitor".into()),
        ..Default::default()
    })
    .unwrap();
    let mut source = provider.telemetry_source_for_profile(&request, &admission).unwrap();
    active.store(false, Ordering::Release);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
}
#[test]
fn declared_caps_preserve_every_stricter_caller_dimension_and_never_approve_native_cost() {
    let upper = CollectionBudget::upper_limits();
    let bounded = bounded_budget(&upper);
    assert_eq!(bounded.max_queries, 6);
    assert_eq!(bounded.max_snapshot_rows, native::settings::capture_rows());
    for budget in [
        CollectionBudget { max_queries: 1, ..upper.clone() },
        CollectionBudget { max_snapshot_rows: 1, page_rows: 1, ..upper.clone() },
        CollectionBudget { total_timeout_ms: 1, statement_timeout_ms: 1, ..upper.clone() },
        CollectionBudget {
            total_bytes: 1024,
            page_bytes: 1024,
            max_sql_bytes: 1024,
            max_plan_bytes: 1024,
            ..upper.clone()
        },
    ] {
        budget.validate().unwrap();
        let actual = bounded_budget(&budget);
        actual.validate().unwrap();
        assert_eq!(actual, actual.intersection(&budget));
    }
    let recipe = declarations().into_iter().find(|recipe| recipe.id == "postgres.settings").unwrap();
    assert_eq!(recipe.version, 2);
    assert_eq!(recipe.budget, bounded);
    assert!(!matches!(recipe.admission, RecipeAdmission::Measured(_)));
    assert_eq!(
        crate::telemetry_profile_selectors(),
        vec![
            RecipeSelector::Collection(TelemetryCollection::Activity),
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::StatementCounters)),
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Settings))
        ]
    );
}

#[tokio::test]
async fn a_tighter_row_cap_cannot_turn_a_native_prefix_into_a_completed_settings_capture() {
    let mut request = request();
    request.budget.page_rows = 1;
    request.budget.max_snapshot_rows = 1;
    let (reader, _) = reader(&request, true).await;
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    assert_eq!(CollectionStop::from_error(&read_all(&mut source).await.unwrap_err()), CollectionStop::RowLimit);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

mod statement_counters;
