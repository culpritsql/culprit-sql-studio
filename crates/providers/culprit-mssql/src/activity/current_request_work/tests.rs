use super::*;
use crate::{
    instance::test_support::{epoch, metadata},
    read::{NativeRead, ReadResult},
    resources::test_support::{FakeRead, replace},
    value::Val,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

pub(crate) const SQL: &str = "SELECT id FROM dbo.items WHERE secret='private-current-request-secret'";
pub(crate) const XML: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT id FROM dbo.items WHERE secret='private-current-request-secret'" /></Statements></Batch></BatchSequence></ShowPlanXML>"#;
pub(crate) fn authority(at: &str, role: Option<&str>) -> Rec {
    replace(
        &crate::activity::database_context::tests::row(3),
        &[
            ("observed_at", Val::Text(at.into())),
            ("updateability", role.map(|v| Val::Text(v.into())).unwrap_or(Val::Null)),
        ],
    )
}
#[derive(Default)]
pub(crate) struct Trace {
    pub calls: Mutex<Vec<String>>,
    pub closes: AtomicU32,
    pub retired_after_error: AtomicBool,
    pub reads_requiring_replacement: AtomicU32,
}
pub(crate) struct Io {
    pub inner: FakeRead,
    pub trace: Arc<Trace>,
    pub generation: Option<u64>,
    pub change_at: Option<u32>,
}
#[async_trait::async_trait]
impl NativeRead for Io {
    async fn query(&mut self, sql: &str, parameters: Vec<Parameter>, limit: usize) -> ReadResult<Vec<Rec>> {
        self.trace.calls.lock().unwrap().push(sql.into());
        // ReadSession retires its held client on a Source error. Record any later invocation that
        // would require replacement acquisition; this fake never opens a connection or invents SET work.
        if self.trace.retired_after_error.load(Ordering::SeqCst) {
            self.trace.reads_requiring_replacement.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.query(sql, parameters, limit).await;
        if matches!(&result, Err(ReadError::Source(_))) {
            self.trace.retired_after_error.store(true, Ordering::SeqCst);
        }
        result
    }
    fn queries(&self) -> u32 {
        self.inner.queries()
    }
    fn connection_generation(&self) -> Option<u64> {
        self.generation.map(|n| n + u64::from(self.change_at.is_some_and(|at| self.queries() >= at)))
    }
    async fn close(&mut self) -> SourceShutdown {
        self.trace.closes.fetch_add(1, Ordering::SeqCst);
        self.inner.close().await
    }
}
pub(crate) fn io(replies: Vec<ReadResult<Vec<Rec>>>) -> Io {
    Io { inner: FakeRead::new(replies), trace: Arc::default(), generation: Some(1), change_at: None }
}
pub(crate) async fn focus() -> ActivityRequestFocus {
    focus_for_role(true).await
}
pub(crate) async fn focus_for_role(writable: bool) -> ActivityRequestFocus {
    focus_for_original(writable, None).await
}
pub(crate) async fn focus_for_statement(
    writable: bool,
    statement: &NativeIdentity,
    text: &str,
) -> ActivityRequestFocus {
    focus_for_original(writable, Some((statement, text))).await
}
async fn focus_for_original(writable: bool, statement: Option<(&NativeIdentity, &str)>) -> ActivityRequestFocus {
    let role = if writable { "READ_WRITE" } else { "READ_ONLY" };
    let mut reader =
        io(vec![Ok(vec![metadata()]), Ok(vec![epoch(100)]), Ok(vec![authority("2020-01-01T06:00:00Z", Some(role))])]);
    let owner = Context::read(&mut reader, "fixture.invalid", 1433).await.unwrap();
    let (source, _) = culprit_evidence::analytical::test_support::mssql_current_work_evidence(
        culprit_evidence::analytical::test_support::MssqlWorkCase::Observed,
    );
    let SourceObservation::CurrentRequestWork { result, .. } = source.observation else { panic!() };
    let mut focus = *result.focus.unwrap();
    focus.database_writable = owner.activity.writable;
    focus.target = owner.activity.database.clone().into();
    focus.partition = owner.source_partition().unwrap();
    focus.epoch = focus.partition.native_epoch().unwrap().into();
    focus.original.key.session.generation = "2020-01-01T04:00:00.0030000".into();
    focus.original.key.request.as_mut().unwrap().generation = "2020-01-01T05:59:00.0030000".into();
    focus.source = culprit_core::source_id::SESSIONS.into();
    focus.original.database = Some(owner.activity.database.database.clone());
    focus.original_observed_at = "2020-01-01T05:59:59Z".into();
    if let Some((identity, text)) = statement {
        focus.statement_identity = identity.clone();
        focus.original.statement_identity = Some(identity.clone());
        focus.original.statement = DefinitionPayload::sql(text, Engine::Mssql);
    }
    let parent = CollectionDescription {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        source: focus.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            at: focus.original_observed_at.clone(),
            observed_from: focus.original_observed_at.clone(),
            observed_until: focus.original_observed_at.clone(),
            epoch: Some(focus.epoch.clone()),
            database_writable: focus.database_writable,
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::DefaultDatabaseConnections,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(focus.source.clone(), 1),
        }),
    };
    let capture = CaptureIdentity {
        observation_id: "controlled-native-current-request".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-native-current-outbox".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let row =
        ExportedRecord::new(&parent, &capture, TelemetryRecord::Activity(Box::new(focus.original.clone()))).unwrap();
    focus.parent =
        DiagnosticEvidenceRef::from_export(&parent, &digest(b"controlled-native-current-manifest"), &row, 0).unwrap();
    assert!(focus.matches_parent(&parent, &focus.original, &focus.parent));
    focus
}
pub(crate) fn point(at: &str) -> Rec {
    Rec::of(&[
        ("ordinal", Val::Int(0)),
        ("observed_at", Val::Text(at.into())),
        ("updateability", Val::Text("READ_WRITE".into())),
        ("checked_session_id", Val::Int(42)),
        ("checked_session_generation", Val::Text("2020-01-01T04:00:00.0030000".into())),
        ("checked_request_id", Val::Int(0)),
        ("checked_request_generation", Val::Text("2020-01-01T05:59:00.0030000".into())),
        ("checked_sql_handle", Val::Text("AABB".into())),
        ("checked_begin", Val::Int(0)),
        ("checked_end", Val::Int(-1)),
        ("checked_database_id", Val::Int(7)),
        ("checked_plan_handle", Val::Text("CCDD".into())),
        ("checked_query_hash", Val::Text("0123456789ABCDEF".into())),
        ("checked_plan_hash", Val::Text("0123456789ABCDEF".into())),
        ("checked_wait_type", Val::Null),
        ("checked_wait_resource", Val::Null),
        ("work_database_id", Val::Int(7)),
        ("work_elapsed_ms", Val::Int(60000)),
        ("work_cpu_ms", Val::Int(3200)),
        ("work_reads", Val::Int(9007199254740993)),
        ("work_writes", Val::Int(0)),
        ("work_logical_reads", Val::Null),
        ("work_rows_returned", Val::Int(42)),
    ])
}
pub(crate) fn cache(at: &str, generation: i64) -> Rec {
    Rec::of(&[
        ("ordinal", Val::Int(0)),
        ("observed_at", Val::Text(at.into())),
        ("updateability", Val::Text("READ_WRITE".into())),
        ("plan_handle", Val::Text("0xCCDD".into())),
        ("sql_handle", Val::Text("0xAABB".into())),
        ("statement_start_offset", Val::Int(0)),
        ("statement_end_offset", Val::Int(-1)),
        ("creation_time", Val::Text("2020-01-01T05:59:00.003".into())),
        ("plan_generation_num", Val::Int(generation)),
        ("database_id", Val::Int(7)),
        ("query_hash", Val::Text("0x0123456789ABCDEF".into())),
        ("source_text_bytes", Val::Int((SQL.encode_utf16().count() * 2) as i64)),
        ("text_complete", Val::Bool(true)),
        ("text_prefix", Val::Text(SQL.into())),
        ("engine_internal", Val::Int(0)),
        ("cloud_internal", Val::Int(0)),
        ("statement_bytes", Val::Int((SQL.encode_utf16().count() * 2) as i64)),
        ("statement", Val::Text(SQL.into())),
        ("plan_bytes", Val::Int((XML.encode_utf16().count() * 2) as i64)),
        ("plan", Val::Text(XML.into())),
        ("encrypted", Val::Bool(false)),
    ])
}
pub(crate) fn replies() -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![authority("2020-01-01T06:00:00Z", Some("READ_WRITE"))]),
        Ok(vec![point("2020-01-01T06:00:00.1Z")]),
        Ok(vec![cache("2020-01-01T06:00:00.2Z", 4)]),
        Ok(vec![cache("2020-01-01T06:00:00.3Z", 4)]),
        Ok(vec![point("2020-01-01T06:00:00.4Z")]),
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![authority("2020-01-01T06:00:01Z", Some("READ_WRITE"))]),
    ]
}
async fn capture(
    replies: Vec<ReadResult<Vec<Rec>>>,
    focus: &ActivityRequestFocus,
) -> (Result<CurrentRequestWorkSnapshot>, ActivityReader<Io>) {
    let mut reader = ActivityReader::new(
        io(replies),
        ReadOptions { budget: CollectionBudget::default(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    let result = reader.capture_current_request_work(focus).await;
    (result, reader)
}
fn work(snapshot: &CurrentRequestWorkSnapshot) -> &MssqlCurrentRequestWork {
    let CurrentRequestWorkNative::Mssql { work } = &snapshot.observations[0].native else { panic!() };
    work
}

#[tokio::test]
async fn held_current_request_counts_and_cache_generation_keep_exact_native_owners_and_independent_clocks() {
    let focus = focus().await;
    let (result, mut reader) = capture(replies(), &focus).await;
    let snapshot = result.unwrap();
    assert!(snapshot.is_valid());
    assert_eq!(reader.query_count(), 10);
    let native = work(&snapshot);
    assert_eq!(native.reads.as_deref(), Some("9007199254740993"));
    assert_eq!(native.logical_reads, None);
    let MssqlCurrentRequestPlan::Observed { context, observed_from, observed_until, plan } = &native.current_plan
    else {
        panic!()
    };
    assert_eq!(context.identity.plan_generation, "4");
    assert_ne!(observed_from, observed_until);
    assert!(plan.is_available());
    assert!(!serde_json::to_string(&snapshot).unwrap().contains("private-current-request-secret"));
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    assert_eq!(reader.io.trace.closes.load(Ordering::SeqCst), 1);
    let sql = reader.io.trace.calls.lock().unwrap();
    assert_eq!(sql.iter().filter(|sql| sql.contains("FROM sys.dm_exec_query_stats stats")).count(), 2);
    assert!(sql.iter().all(|sql| !sql.contains("total_worker_time")
        && !sql.contains("execution_count")
        && !sql.contains("sys.dm_exec_query_profiles")));
}
#[tokio::test]
async fn missing_or_reused_current_actor_never_proves_outcome_or_attributes_work_to_its_predecessor() {
    let focus = focus().await;
    for defect in ["missing", "login", "request", "statement"] {
        let mut rows = replies();
        let changes = match defect {
            "missing" => vec![
                ("checked_session_id", Val::Null),
                ("checked_request_id", Val::Null),
                ("work_database_id", Val::Null),
            ],
            "login" => vec![("checked_session_generation", Val::Text("2020-01-01T04:00:01.0030000".into()))],
            "request" => vec![("checked_request_generation", Val::Text("2020-01-01T05:59:01.0030000".into()))],
            _ => vec![("checked_sql_handle", Val::Text("EEFF".into()))],
        };
        rows[3] = Ok(vec![replace(&point("2020-01-01T06:00:00.1Z"), &changes)]);
        rows.drain(4..7);
        let (result, mut reader) = capture(rows, &focus).await;
        let result = result.unwrap();
        assert!(result.observations.is_empty());
        assert_eq!(result.result.membership, FocusedRequestMembership::NotObserved);
        assert_eq!(result.coverage.state, culprit_core::SourceState::Partial);
        if defect == "missing" {
            assert_eq!(result.result.binding, FocusedRequestBinding::Matched);
        } else {
            assert_eq!(result.result.binding, FocusedRequestBinding::ParticipantChanged);
        }
        assert_eq!(reader.query_count(), 7);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
    let mut rows = replies();
    rows[6] = Ok(vec![replace(
        &point("2020-01-01T06:00:00.4Z"),
        &[("checked_request_id", Val::Null), ("work_database_id", Val::Null)],
    )]);
    let (result, mut reader) = capture(rows, &focus).await;
    assert!(result.unwrap().observations.is_empty());
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn independent_cache_change_or_successful_unavailability_preserves_request_counters() {
    let focus = focus().await;
    for defect in
        ["generation", "plan", "missing", "foreign-compilation-db", "no-plan-token", "ambiguous", "payload-unavailable"]
    {
        let mut rows = replies();
        match defect {
            "generation" => rows[5] = Ok(vec![cache("2020-01-01T06:00:00.3Z", 5)]),
            "plan" => {
                rows[6] = Ok(vec![replace(
                    &point("2020-01-01T06:00:00.4Z"),
                    &[("checked_plan_handle", Val::Text("EEEE".into()))],
                )])
            }
            "missing" => {
                for index in [4, 5] {
                    let Ok(native) = &mut rows[index] else { panic!() };
                    native[0] = replace(&native[0], &[("plan_generation_num", Val::Null), ("database_id", Val::Null)]);
                }
            }
            "no-plan-token" => {
                rows[3] = Ok(vec![replace(&point("2020-01-01T06:00:00.1Z"), &[("checked_plan_handle", Val::Null)])]);
                rows[6] = Ok(vec![replace(&point("2020-01-01T06:00:00.4Z"), &[("checked_plan_handle", Val::Null)])]);
                rows.drain(4..6);
            }
            "foreign-compilation-db" => {
                for index in [4, 5] {
                    let Ok(native) = &mut rows[index] else { panic!() };
                    native[0] = replace(&native[0], &[("database_id", Val::Int(8))]);
                }
            }
            "ambiguous" => {
                rows[4] = Ok(vec![cache("2020-01-01T06:00:00.2Z", 4), cache("2020-01-01T06:00:00.2Z", 5)]);
                rows.remove(5).unwrap();
            }
            _ => {
                for index in [4, 5] {
                    let Ok(native) = &mut rows[index] else { panic!() };
                    native[0] = replace(
                        &native[0],
                        &[
                            ("statement_bytes", Val::Null),
                            ("statement", Val::Null),
                            ("plan_bytes", Val::Null),
                            ("plan", Val::Null),
                        ],
                    );
                }
            }
        }
        let (result, mut reader) = capture(rows, &focus).await;
        let snapshot = result.unwrap();
        assert_eq!(work(&snapshot).reads.as_deref(), Some("9007199254740993"));
        if defect == "payload-unavailable" {
            assert!(!snapshot.observations[0].statement.is_available());
            assert!(matches!(
                &work(&snapshot).current_plan,
                MssqlCurrentRequestPlan::Observed { plan, .. } if !plan.is_available()
            ));
        } else {
            assert!(matches!(work(&snapshot).current_plan, MssqlCurrentRequestPlan::Unavailable { .. }), "{defect}");
        }
        assert_eq!(snapshot.result.binding, FocusedRequestBinding::Matched);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        assert_eq!(
            reader.query_count(),
            if defect == "no-plan-token" {
                8
            } else if defect == "ambiguous" {
                9
            } else {
                10
            }
        );
    }
}
#[tokio::test]
async fn role_member_process_database_or_clock_conflicts_never_publish_borrowed_context() {
    let focus = focus().await;
    for defect in ["frame-role", "payload-role", "final-role", "member", "process", "database", "frame-db", "clock"] {
        let mut rows = replies();
        match defect {
            "frame-role" => {
                rows[3] = Ok(vec![replace(
                    &point("2020-01-01T06:00:00.1Z"),
                    &[("updateability", Val::Text("READ_ONLY".into()))],
                )])
            }
            "payload-role" => {
                rows[4] = Ok(vec![replace(
                    &cache("2020-01-01T06:00:00.2Z", 4),
                    &[("updateability", Val::Text("READ_ONLY".into()))],
                )])
            }
            "final-role" => rows[9] = Ok(vec![authority("2020-01-01T06:00:01Z", Some("READ_ONLY"))]),
            "member" => {
                rows[7] = Ok(vec![replace(
                    &metadata(),
                    &[
                        ("machine_name", Val::Text("other-machine".into())),
                        ("server_name", Val::Text("other-machine\\NATIVE".into())),
                    ],
                )])
            }
            "process" => rows[8] = Ok(vec![epoch(101)]),
            "frame-db" => {
                rows[3] = Ok(vec![replace(&point("2020-01-01T06:00:00.1Z"), &[("work_database_id", Val::Int(8))])])
            }
            "database" => {
                rows[9] = Ok(vec![replace(
                    &authority("2020-01-01T06:00:01Z", Some("READ_WRITE")),
                    &[("database_guid", Val::Text("foreign".into()))],
                )])
            }
            _ => rows[5] = Ok(vec![cache("2020-01-01T05:59:59Z", 4)]),
        }
        let (result, mut reader) = capture(rows, &focus).await;
        assert!(result.is_err(), "{defect}");
        let count = reader.query_count();
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        assert_eq!(reader.query_count(), count);
    }
    let mut reader = ActivityReader::new(
        io(replies()),
        ReadOptions { budget: CollectionBudget::default(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    reader.io.change_at = Some(7);
    let result = reader.capture_current_request_work(&focus).await;
    assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::SourceChanged);
    let count = reader.query_count();
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    assert_eq!(reader.query_count(), count);
}

const MAX_REQUEST_CAPTURE_FUTURE_BYTES: usize = 4 * 1024;

#[tokio::test]
async fn current_request_capture_future_retains_large_native_contexts_on_their_heap_owner() {
    let focus = focus().await;
    let mut reader = ActivityReader::new(
        io(replies()),
        ReadOptions { budget: CollectionBudget::default(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    let future = reader.capture_current_request_work(&focus);
    assert!(std::mem::size_of_val(&future) <= MAX_REQUEST_CAPTURE_FUTURE_BYTES);
    drop(future);
    assert_eq!(reader.query_count(), 0);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}

#[test]
fn actual_native_request_future_and_output_are_bounded_without_constructing_or_polling_a_client() {
    fn future_bytes<Arguments, Function, Future>(_: Function) -> usize
    where
        Function: FnOnce(Arguments) -> Future,
    {
        std::mem::size_of::<Future>()
    }
    let bytes = future_bytes(
        |(reader, focus): (&'static mut ActivityReader<crate::read::ReadSession>, &'static ActivityRequestFocus)| {
            reader.capture_current_request_work(focus)
        },
    );
    assert!(bytes <= MAX_REQUEST_CAPTURE_FUTURE_BYTES, "actual NativeRead request future: {bytes} bytes");
    const MAX_REQUEST_CAPTURE_OUTPUT_BYTES: usize = 4 * 1024;
    let output_bytes = std::mem::size_of::<Result<CurrentRequestWorkSnapshot>>();
    assert!(output_bytes <= MAX_REQUEST_CAPTURE_OUTPUT_BYTES, "actual request output: {output_bytes} bytes");
}
