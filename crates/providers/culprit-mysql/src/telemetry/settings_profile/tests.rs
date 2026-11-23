//! Scripted source controls reuse the existing phase-counting native reader, never a second connection.
use super::*;
use crate::{
    read::{OwnedRead, QueryRead, test_support::Read},
    rows::Rec,
};
use culprit_evidence::analytical::test_support::baseline_profile_expectation;
use mysql_async::Value;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
const FROM: &str = "2020-01-01T06:00:00Z";
const UNTIL: &str = "2020-01-01T06:00:00.200000Z";
struct Stream {
    native: Read,
    profile: culprit_read::SourceProfileGuard,
    active: Arc<AtomicBool>,
    retire_after: Option<usize>,
    batches: VecDeque<Result<ReadBatch<Rec>>>,
    late_failure: bool,
    stream_generation: Option<u64>,
    stream_active: bool,
    stream_transition: Option<Option<u64>>,
    native_batches: usize,
    page_rows: usize,
}
#[async_trait::async_trait]
impl QueryRead for Stream {
    fn connection_generation(&self) -> Option<u64> {
        if self.stream_active { None } else { self.native.connection_generation() }
    }
    async fn read(&mut self, sql: &str, parameters: Vec<Value>, limit: usize) -> Result<Vec<Rec>> {
        self.profile.check_connection(self.native.connection_generation())?;
        let rows = self.native.read(sql, parameters, limit).await;
        if self.retire_after == Some(self.native.queries.len()) {
            self.active.store(false, Ordering::Release);
        }
        rows
    }
    async fn explain(&mut self, _: &str) -> Result<Option<Rec>> {
        panic!("Settings never explains application SQL")
    }
}
#[async_trait::async_trait]
impl OwnedRead for Stream {
    fn query_count(&self) -> u32 {
        self.native.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.native.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.native.close().await
    }
}
#[async_trait::async_trait]
impl snapshot_io::SnapshotIo for Stream {
    fn snapshot_generation(&self) -> Option<u64> {
        if self.stream_active {
            self.stream_generation.filter(|generation| Some(*generation) == self.native.connection_generation())
        } else {
            self.connection_generation()
        }
    }
    async fn begin_stream(&mut self, sql: &str, parameters: Vec<Value>, limit: usize) -> Result<()> {
        let generation = self.connection_generation();
        let rows = self.read(sql, parameters, limit).await?;
        self.stream_generation = generation;
        self.stream_active = true;
        if rows.is_empty() {
            self.batches.push_back(Ok(ReadBatch { rows, complete: !self.late_failure }));
        } else {
            let count = rows.len().div_ceil(self.page_rows);
            let mut rows = rows.into_iter();
            for n in 0..count {
                self.batches.push_back(Ok(ReadBatch {
                    rows: rows.by_ref().take(self.page_rows).collect(),
                    complete: n + 1 == count && !self.late_failure,
                }));
            }
        }
        if self.late_failure {
            self.batches.push_back(Err(CollectionStop::SourceUnavailable.error()));
        }
        Ok(())
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Rec>> {
        let mut batch = self.batches.pop_front().expect("declared native page");
        if self.native_batches == 1
            && let Some(generation) = self.stream_transition
        {
            self.native.generation = generation;
            if let Ok(batch) = &mut batch {
                batch.complete = false;
            } // the lost/replaced task has not supplied native EOF
        }
        self.native_batches += 1;
        if batch.as_ref().is_err() || batch.as_ref().is_ok_and(|batch| batch.complete) {
            self.stream_active = false;
            self.stream_generation = None;
        }
        batch
    }
}
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
        budget: bounded_budget(&CollectionBudget { page_rows: 1, ..Default::default() }),
    }
}
fn context(at: &str, maria: bool, instrumented: bool, writable: bool) -> Result<Vec<Rec>> {
    let row = super::super::activity::tests::context(at, 1)?.remove(0).with_fields(&[
        ("version", if maria { "11.8.9-MariaDB" } else { "8.4.6" }.into()),
        ("performance_schema", u64::from(instrumented).into()),
        ("read_only", u64::from(!writable).into()),
    ]);
    Ok(vec![row])
}
fn visibility(maria: bool) -> Result<Vec<Rec>> {
    Ok(vec![super::super::activity::tests::visibility()?.remove(0).with_fields(&[(
        "native_member",
        if maria { "q83hHHfky00d9QAi5bwmy+JVlfE=" } else { "f54a461c-0001-4000-8000-000000000001" }.into(),
    )])])
}
fn rows() -> Vec<Rec> {
    vec![
        Rec::from_values(vec![("variable_name", "slow_query_log".into()), ("value", "OFF".into())]),
        Rec::from_values(vec![("variable_name", "sort_buffer_size".into()), ("value", Value::NULL)]),
    ]
}
fn replies(maria: bool, instrumented: bool, writable: bool) -> Vec<Result<Vec<Rec>>> {
    vec![
        context(FROM, maria, instrumented, writable),
        super::super::activity::tests::epoch(),
        visibility(maria),
        Ok(rows()),
        context(UNTIL, maria, instrumented, writable),
        super::super::activity::tests::epoch(),
        visibility(maria),
    ]
}
async fn reader(
    request: &CollectionRequest,
    maria: bool,
    instrumented: bool,
    writable: bool,
) -> (MysqlSnapshotReader<Stream>, Arc<AtomicBool>) {
    let original = super::super::activity::tests::connector();
    let mut expected = Read::with(replies(maria, instrumented, writable));
    let expected = metadata::settings(&mut expected, &original).await.unwrap();
    let expected = expected.activity_context(&expected).unwrap();
    assert!(expected.has_qualified_local_owner());
    let active = Arc::new(AtomicBool::new(true));
    let flag = active.clone();
    let admission = baseline_profile_expectation(request, &expected, move || flag.load(Ordering::Acquire));
    let profile = culprit_read::SourceProfileGuard::new(request, &admission).unwrap();
    let connector = Arc::new(original.with_profile(profile.clone()));
    (
        MysqlSnapshotReader {
            io: Stream {
                native: Read::with(replies(maria, instrumented, writable))
                    .with_native_phases()
                    .with_query_budget(request.budget.max_queries, 7),
                profile,
                active: active.clone(),
                retire_after: None,
                late_failure: false,
                stream_generation: None,
                stream_active: false,
                stream_transition: None,
                native_batches: 0,
                batches: VecDeque::new(),
                page_rows: request.budget.page_rows as usize,
            },
            connector,
            declared: vec![Recipe::Settings],
            pending: VecDeque::new(),
            current: None,
            reading: false,
            unavailable: BTreeMap::new(),
            not_applicable: BTreeSet::new(),
            metadata: None,
            layout: None,
            budget: request.budget.clone(),
            generation: None,
        },
        active,
    )
}
async fn drain(source: &mut impl TelemetrySource) -> Result<Vec<TelemetryRecord>> {
    let mut records = vec![];
    let mut cursor = None;
    loop {
        let CollectionRead::Page { records: page, next_cursor } =
            source.read_page(TelemetryStream::SettingObservations, cursor.as_deref()).await?
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
async fn settings_uses_eighteen_actual_phases_with_native_member_and_no_instrumentation_dependency_or_repage_sql() {
    for maria in [false, true] {
        for instrumented in [false, true] {
            for writable in [false, true] {
                let request = request();
                let (reader, _) = reader(&request, maria, instrumented, writable).await;
                let mut source = SnapshotSource::new(reader, &request).unwrap();
                let before = source.describe().await.unwrap();
                assert_eq!(source.query_count(), 12);
                assert_eq!(before.partition, SourcePartition::Target);
                let rows = drain(&mut source).await.unwrap();
                assert_eq!(source.query_count(), 13);
                assert_eq!(rows.len(), 2);
                let complete = before.completed(source.finish().await.unwrap()).unwrap().unwrap();
                assert_eq!(source.query_count(), 18);
                let context = complete.observation.snapshot_acquisition_context().unwrap();
                assert!(context.has_qualified_local_owner());
                assert_eq!(context.database_writable, Some(writable));
                assert!(complete.target.database().unwrap().incarnation.is_none());
                let native = complete.observation.snapshot_settings_context().unwrap();
                assert_eq!(
                    native.format,
                    if maria {
                        NativeSettingsFormat::MariaDbGlobalVariables
                    } else {
                        NativeSettingsFormat::MysqlGlobalVariables
                    }
                );
                let TelemetryRecord::Setting(missing) = &rows[1] else { panic!() };
                assert!(missing.value_in_use.is_none());
                assert!(native.project(missing).is_none());
                assert_eq!(source.reader().io.native.queries.len(), 7);
                assert!(
                    source
                        .reader()
                        .io
                        .native
                        .queries
                        .iter()
                        .all(|(sql, _, _)| !sql.contains("events_statements") && !sql.contains("setup_consumers"))
                );
                assert_eq!(source.close().await, SourceShutdown::Quiescent);
                assert_eq!(source.reader().io.native.closed, 1);
            }
        }
    }
}
#[tokio::test]
async fn caller_seventeen_never_replaces_mandatory_final_execute_with_a_completed_header() {
    let mut request = request();
    request.budget.max_queries = 17;
    let (reader, _) = reader(&request, false, true, true).await;
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    drain(&mut source).await.unwrap();
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 17);
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn native_member_role_process_version_and_transport_changes_never_seal_the_provisional_settings() {
    for case in ["member", "role", "process", "version", "transport"] {
        let request = request();
        let (mut reader, _) = reader(&request, false, true, true).await;
        match case {
            "member" => {
                reader.io.native.replies[6] = Ok(vec![
                    visibility(false)
                        .unwrap()
                        .remove(0)
                        .with_fields(&[("native_member", "f54a461c-0001-4000-8000-000000000002".into())]),
                ])
            }
            "role" => reader.io.native.replies[4] = context(UNTIL, false, true, false),
            "process" => {
                reader.io.native.replies[5] =
                    Ok(vec![Rec::from_values(vec![("observed_second", 10000u64.into()), ("uptime", 99u64.into())])])
            }
            "version" => {
                reader.io.native.replies[4] = Ok(vec![
                    context(UNTIL, false, true, true).unwrap().remove(0).with_fields(&[("version", "8.4.7".into())]),
                ])
            }
            "transport" => reader.io.native.change_generation_after = Some(4),
            _ => unreachable!(),
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        source.describe().await.unwrap();
        let rows = drain(&mut source).await;
        if case == "transport" {
            assert!(rows.is_err());
        } else {
            rows.unwrap();
            let result = source.finish().await;
            assert!(matches!(result, Err(_) | Ok(SourceCompletion::Changed)), "{case}");
        }
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn missing_or_cancelled_native_facts_and_retirement_keep_exact_failure_and_await_same_close() {
    for boundary in 0..7 {
        let request = request();
        let (mut reader, _) = reader(&request, true, false, true).await;
        reader.io.native.replies[boundary] = Err(CollectionStop::Cancelled.error());
        reader.io.native.shutdown = Some(SourceShutdown::Unconfirmed);
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let result = source.describe().await;
        if result.is_ok() {
            let rows = drain(&mut source).await;
            if rows.is_ok() {
                assert!(source.finish().await.is_err());
            }
        }
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
        assert_eq!(source.reader().io.native.closed, 1);
    }
    let request = request();
    let (mut reader, active) = reader(&request, false, true, true).await;
    reader.io.retire_after = Some(4);
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    assert!(drain(&mut source).await.is_err());
    active.store(true, Ordering::Release);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 13);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[test]
fn profile_caps_are_strict_and_do_not_qualify_cost_or_enable_statements_or_definitions() {
    let cap = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(cap.max_queries, 18);
    assert_eq!(cap.max_snapshot_rows, 23);
    for format in [NativeSettingsFormat::MysqlGlobalVariables, NativeSettingsFormat::MariaDbGlobalVariables] {
        assert!(format.global_variable_names().unwrap().len() <= cap.max_snapshot_rows as usize);
    }
    for mut budget in [
        CollectionBudget::upper_limits(),
        CollectionBudget { max_queries: 1, ..Default::default() },
        CollectionBudget { page_rows: 1, max_snapshot_rows: 1, ..Default::default() },
        CollectionBudget { page_bytes: 1024, total_bytes: 1024, ..Default::default() },
    ] {
        budget.max_sql_bytes = budget.max_sql_bytes.min(budget.total_bytes);
        budget.max_plan_bytes = budget.max_plan_bytes.min(budget.total_bytes);
        budget.validate().unwrap();
        let limited = bounded_budget(&budget);
        limited.validate().unwrap();
        assert_eq!(limited, limited.intersection(&budget));
    }
    let recipes = crate::telemetry_recipes();
    assert!(
        recipes
            .iter()
            .filter(|recipe| recipe.id == "mysql.settings")
            .all(|recipe| matches!(&recipe.admission,RecipeAdmission::BuiltinSafety(policy) if crate::verify_builtin_safety(policy).unwrap()))
    );
    assert_eq!(
        crate::telemetry_profile_selectors(),
        vec![
            RecipeSelector::Collection(TelemetryCollection::Activity),
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Settings))
        ]
    );
}

#[tokio::test]
async fn unsupported_versions_missing_native_member_and_denied_metadata_never_open_the_settings_body() {
    for case in ["old-mysql", "old-maria", "member", "visibility", "clock", "null-role", "foreign-database"] {
        let request = request();
        let (mut reader, _) = reader(&request, false, false, true).await;
        match case {
            "old-mysql" => {
                reader.io.native.replies[0] = Ok(vec![
                    context(FROM, false, false, true).unwrap().remove(0).with_fields(&[("version", "8.0.42".into())]),
                ])
            }
            "old-maria" => {
                reader.io.native.replies[0] = Ok(vec![
                    context(FROM, true, false, true)
                        .unwrap()
                        .remove(0)
                        .with_fields(&[("version", "11.4.9-MariaDB".into())]),
                ]);
                reader.io.native.replies[2] = visibility(true);
            }
            "member" => {
                reader.io.native.replies[2] =
                    Ok(vec![visibility(false).unwrap().remove(0).with_fields(&[("native_member", Value::NULL)])])
            }
            "visibility" => {
                reader.io.native.replies[2] =
                    Err(culprit_core::Error::new(ErrorCode::PermissionDenied, "controlled denied metadata"))
            }
            "clock" => reader.io.native.replies[1] = Ok(vec![]),
            "null-role" => {
                reader.io.native.replies[0] = Ok(vec![
                    context(FROM, false, false, true).unwrap().remove(0).with_fields(&[("read_only", Value::NULL)]),
                ])
            }
            "foreign-database" => {
                reader.io.native.replies[0] = Ok(vec![
                    context(FROM, false, false, true)
                        .unwrap()
                        .remove(0)
                        .with_fields(&[("database_name", "foreign".into())]),
                ])
            }
            _ => unreachable!(),
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        assert!(source.describe().await.is_err(), "{case}");
        assert!(source.reader().io.native.queries.iter().all(|(sql, _, _)| !sql.starts_with("SHOW GLOBAL")), "{case}");
        assert!(source.finish().await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn empty_and_late_native_responses_preserve_distinct_eof_and_no_final_recheck_after_error() {
    for late in [false, true] {
        let request = request();
        let (mut reader, _) = reader(&request, false, false, true).await;
        if late {
            reader.io.late_failure = true;
        } else {
            reader.io.native.replies[3] = Ok(vec![]);
        }
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let before = source.describe().await.unwrap();
        if late {
            let CollectionRead::Page { next_cursor: Some(cursor), .. } =
                source.read_page(TelemetryStream::SettingObservations, None).await.unwrap()
            else {
                panic!()
            };
            let CollectionRead::Page { next_cursor: Some(next), .. } =
                source.read_page(TelemetryStream::SettingObservations, Some(&cursor)).await.unwrap()
            else {
                panic!()
            };
            assert!(source.read_page(TelemetryStream::SettingObservations, Some(&next)).await.is_err());
            assert!(source.finish().await.is_err());
            assert_eq!(source.query_count(), 13);
        } else {
            assert!(drain(&mut source).await.unwrap().is_empty());
            let complete = before.completed(source.finish().await.unwrap()).unwrap().unwrap();
            let snapshot = complete.observation.snapshot().unwrap();
            assert_eq!(snapshot.records, 0);
            assert_eq!(snapshot.sources[0].state, culprit_core::SourceState::Empty);
            assert!(complete.observation.snapshot_acquisition_context().is_some());
            assert_eq!(source.query_count(), 18);
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn oversized_settings_record_cannot_finish_as_complete_or_spend_a_final_context_replacement() {
    let mut request = request();
    request.budget.page_bytes = 2048;
    request.budget.total_bytes = 4096;
    request.budget.max_sql_bytes = 2048;
    request.budget.max_plan_bytes = 2048;
    let (mut reader, _) = reader(&request, false, true, true).await;
    reader.io.native.replies[3] = Ok(vec![rows().remove(0).with_fields(&[("value", "x".repeat(8192).into())])]);
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    source.describe().await.unwrap();
    let error = drain(&mut source).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::ByteLimit);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 13);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn multi_native_batches_keep_only_the_original_task_generation_until_real_eof_restores_the_idle_reader() {
    let request = request();
    let (reader, _) = reader(&request, false, true, true).await;
    let mut source = SnapshotSource::new(reader, &request).unwrap();
    let before = source.describe().await.unwrap();
    assert_eq!(source.reader().io.connection_generation(), Some(1));
    let CollectionRead::Page { records: first, next_cursor: Some(cursor) } =
        source.read_page(TelemetryStream::SettingObservations, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(first.len(), 1);
    assert_eq!(source.query_count(), 13);
    assert_eq!(
        source.reader().io.connection_generation(),
        None,
        "real active ReadSession has moved its client to the owned task"
    );
    assert_eq!(snapshot_io::SnapshotIo::snapshot_generation(&source.reader().io), Some(1));
    let CollectionRead::Page { records: last, next_cursor: None } =
        source.read_page(TelemetryStream::SettingObservations, Some(&cursor)).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(last.len(), 1);
    assert_eq!(
        source.reader().io.connection_generation(),
        Some(1),
        "only native EOF settlement restores the same idle client"
    );
    assert_eq!(source.query_count(), 13, "native paging does not dispatch another SHOW or replace its client");
    let complete = before.completed(source.finish().await.unwrap()).unwrap().unwrap();
    assert!(complete.observation.snapshot_acquisition_context().is_some());
    assert_eq!(source.query_count(), 18);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn unknown_or_replaced_generation_during_an_active_native_stream_never_borrows_the_old_acquisition() {
    for lost in [false, true] {
        let request = request();
        let (mut reader, _) = reader(&request, false, true, true).await;
        reader.io.stream_transition = Some(if lost { None } else { Some(2) });
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        source.describe().await.unwrap();
        let CollectionRead::Page { next_cursor: Some(cursor), .. } =
            source.read_page(TelemetryStream::SettingObservations, None).await.unwrap()
        else {
            panic!()
        };
        // The real accounting owner changes only on an actual acquisition. The fake models that exact refusal,
        // including a lost owner; neither case is native acceptance or permission to construct a replacement.
        assert!(source.read_page(TelemetryStream::SettingObservations, Some(&cursor)).await.is_err());
        assert_eq!(source.reader().io.connection_generation(), None);
        assert_eq!(snapshot_io::SnapshotIo::snapshot_generation(&source.reader().io), None);
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), 13);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
