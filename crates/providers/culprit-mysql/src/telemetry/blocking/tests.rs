use super::super::activity::tests::{connector, context, epoch, original_actor_on, visibility};
use super::*;
use crate::{native::activity::test_support as actor, read::test_support::Read};

pub(super) fn waiter(at: &str, payload: bool) -> Rec {
    let row = actor::row_with(
        1,
        "Query",
        true,
        &[("state", "Waiting for table metadata lock".into()), ("seconds", 40u64.into()), ("observed_at", at.into())],
    );
    if payload {
        row.with_fields(&[
            ("info", Value::NULL),
            ("sql_text", "SELECT id FROM shop.items WHERE id=42".into()),
            ("sql_text_bytes", Value::UInt(37)),
        ])
    } else {
        row
    }
}
pub(super) fn holder(at: &str, thread: u64) -> Rec {
    actor::row_with(2, "Sleep", false, &[("thread_id", thread.into()), ("observed_at", at.into())])
}
pub(in crate::telemetry) async fn request() -> (CollectionRequest, DiagnosticScope) {
    request_on("8.4.6").await
}
pub(super) fn member(version: &str) -> &'static str {
    if version.contains("MariaDB") { "q83hHHfky00d9QAi5bwmy+JVlfE=" } else { "f54a461c-0001-4000-8000-000000000001" }
}
pub(super) async fn request_on(version: &str) -> (CollectionRequest, DiagnosticScope) {
    let (source, original) =
        original_actor_on(waiter("2020-01-01T06:00:00.100000Z", false), version, member(version)).await;
    let scope = DiagnosticScope::from_source(&source).unwrap();
    let native = source.observation.activity().unwrap();
    let capture = CaptureIdentity {
        observation_id: "native-mysql-wait-original".into(),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"mysql original outbox"),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let exported = ExportedRecord::new(&source, &capture, TelemetryRecord::from(original.clone())).unwrap();
    let parent =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"mysql original manifest"), &exported, 0).unwrap();
    let focus = ActivityRequestFocus {
        parent,
        target: source.target.clone(),
        partition: source.partition.clone(),
        database_writable: native.database_writable,
        source: source.source.clone(),
        epoch: native.epoch.clone().unwrap(),
        original_observed_at: native.at.clone(),
        statement_identity: original.statement_identity.clone().unwrap(),
        original,
    };
    assert!(focus.is_blocking_focus());
    (
        CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::FocusedBlocking { focus: Box::new(focus) },
            budget: bounded_budget(&CollectionBudget::default()),
        },
        scope,
    )
}
fn metadata_edge() -> Rec {
    Rec::from_values(vec![
        ("session", 1u64.into()),
        ("blocking", 2u64.into()),
        ("mode", "EXCLUSIVE".into()),
        ("resource", "METADATA".into()),
        ("object", "shop.items".into()),
        ("index_name", Value::NULL),
        ("native_lock_id", Value::NULL),
        ("native_engine", Value::NULL),
        ("requesting_native_thread", 101u64.into()),
        ("blocking_native_thread", 102u64.into()),
    ])
}
pub(super) fn reader() -> Reader<Read> {
    Reader {
        detail: Detail::Blocking,
        io: Read::with(vec![
            context("2020-01-01T06:00:01Z", 1),
            epoch(),
            visibility(),
            Ok(actor::consumers(true)),
            Ok(vec![Rec::from_values(vec![("enabled", "YES".into())])]),
            Ok(vec![waiter("2020-01-01T06:00:01.100000Z", true)]),
            Ok(vec![]),
            Ok(vec![metadata_edge()]),
            Ok(vec![waiter("2020-01-01T06:00:01.200000Z", false), holder("2020-01-01T06:00:01.200000Z", 102)]),
            Ok(vec![]),
            Ok(vec![metadata_edge()]),
            Ok(vec![waiter("2020-01-01T06:00:01.300000Z", false), holder("2020-01-01T06:00:01.300000Z", 102)]),
            context("2020-01-01T06:00:01.400000Z", 1),
            epoch(),
            visibility(),
        ])
        .with_native_phases()
        .with_query_budget(33, 7),
        connector: connector(),
        verified: None,
        generation: None,
    }
}
fn evidence<'a>(
    source: &'a CollectionDescription,
    records: &'a [TelemetryRecord],
) -> (Vec<ActivityFrameRow<'a>>, &'a ActivityBlockingFocusResult) {
    let rows = records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(actor) = row else { panic!() };
            ActivityFrameRow { observation: actor.as_ref(), reference: None }
        })
        .collect();
    let SourceObservation::FocusedBlocking { result, .. } = &source.observation else { panic!() };
    (rows, result)
}
#[tokio::test]
async fn selected_native_metadata_owner_survives_both_points_eof_and_all_thirty_three_owned_phases() {
    for version in ["8.4.6", "11.8.9-MariaDB-ubu2404"] {
        let (request, _) = request_on(version).await;
        let mut reader = reader();
        for index in [0, 12] {
            let row = reader.io.replies[index].as_mut().unwrap().remove(0);
            reader.io.replies[index] = Ok(vec![row.with_fields(&[("version", version.into())])]);
        }
        for index in [2, 14] {
            let row = reader.io.replies[index].as_mut().unwrap().remove(0);
            reader.io.replies[index] = Ok(vec![row.with_fields(&[("native_member", member(version).into())])]);
        }
        let mut source = FrozenSource::new(reader, &request).unwrap();
        let description = source.describe().await.unwrap();
        assert_eq!(source.query_count(), 33);
        let CollectionRead::Page { records, next_cursor: None } =
            source.read_page(TelemetryStream::ActivityObservations, None).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(records.len(), 2);
        let (rows, result) = evidence(&description, &records);
        let frame = ActivityFrameEvidence::new(&description, &rows, true).unwrap();
        assert!(result.has_complete_owner_context(&frame));
        assert_eq!(result.closure.max_depth, 1);
        let holder = rows.iter().find(|row| row.observation.key.session.id == "2").unwrap().observation;
        assert!(holder.idle);
        assert!(!holder.statement.is_available());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 33, "paging/verification cannot dispatch another native phase");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn caller_thirty_two_cannot_publish_a_frame_without_the_mandatory_final_native_guard() {
    let (mut request, _) = request().await;
    request.budget.max_queries = 32;
    let mut reader = reader();
    reader.io = reader.io.with_query_budget(32, 7);
    let error = reader.observe(&request).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    assert!(reader.verified.is_none());
    assert_eq!(reader.query_count(), 32);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn ended_reused_or_changed_requests_cannot_donate_later_lock_edges_or_sql_to_the_original() {
    for defect in ["root-ended", "root-reused", "holder-reused", "edge-foreign", "edges-empty"] {
        let (request, _) = request().await;
        let mut reader = reader();
        match defect {
            "root-ended" => reader.io.replies[11] = Ok(vec![holder("2020-01-01T06:00:01.300000Z", 102)]),
            "root-reused" => {
                reader.io.replies[11] = Ok(vec![
                    waiter("2020-01-01T06:00:01.300000Z", false).with_fields(&[("event_id", 4u64.into())]),
                    holder("2020-01-01T06:00:01.300000Z", 102),
                ])
            }
            "holder-reused" => {
                reader.io.replies[11] =
                    Ok(vec![waiter("2020-01-01T06:00:01.300000Z", false), holder("2020-01-01T06:00:01.300000Z", 999)])
            }
            "edge-foreign" => {
                reader.io.replies[10] =
                    Ok(vec![metadata_edge().with_fields(&[("blocking_native_thread", 999u64.into())])])
            }
            _ => {
                reader.io.replies[7] = Ok(vec![]);
                reader.io.replies[10] = Ok(vec![]);
                reader.io.replies[8] = Ok(vec![waiter("2020-01-01T06:00:01.200000Z", false)]);
                reader.io.replies[11] = Ok(vec![waiter("2020-01-01T06:00:01.300000Z", false)]);
            }
        }
        let observed = reader.observe(&request).await.unwrap();
        let (rows, result) = evidence(&observed.description, &observed.records);
        let frame = ActivityFrameEvidence::new(&observed.description, &rows, true).unwrap();
        assert!(!result.has_complete_owner_context(&frame), "{defect}");
        assert!(result.closure.is_limited());
        assert_eq!(observed.records.len(), 1, "{defect} must not name a different current holder/query");
        assert_eq!(reader.query_count(), 33);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn original_member_epoch_role_and_held_transport_remain_mandatory_before_publication() {
    for defect in ["member", "role", "server", "transport", "unknown-transport"] {
        let (request, _) = request().await;
        let mut reader = reader();
        match defect {
            "member" => {
                reader.io.replies[14] = Ok(vec![
                    visibility()
                        .unwrap()
                        .remove(0)
                        .with_fields(&[("native_member", "f54a461c-0001-4000-8000-000000000002".into())]),
                ])
            }
            "role" => {
                reader.io.replies[12] = Ok(vec![
                    context("2020-01-01T06:00:01.400000Z", 1)
                        .unwrap()
                        .remove(0)
                        .with_fields(&[("read_only", 1u64.into())]),
                ])
            }
            "server" => reader.io.replies[12] = context("2020-01-01T06:00:01.400000Z", 2),
            "transport" => reader.io.change_generation_after = Some(8),
            _ => reader.io.generation = None,
        }
        assert!(reader.observe(&request).await.is_err(), "{defect}");
        assert!(reader.verified.is_none());
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn native_errors_at_every_selected_phase_stop_once_preserve_first_cause_and_close_the_same_owner() {
    for index in 0..15 {
        for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
            let (request, _) = request().await;
            let mut reader = reader();
            reader.io.replies[index] = Err(stop.error());
            reader.io.change_generation_after = Some(index + 1);
            reader.io.shutdown = Some(SourceShutdown::Unconfirmed);
            let error = reader.observe(&request).await.err().unwrap();
            assert_eq!(CollectionStop::from_error(&error), stop, "phase{index}");
            assert_eq!(reader.io.queries.len(), index + 1);
            assert!(reader.verified.is_none());
            let paid = reader.query_count();
            assert_eq!(reader.close().await, SourceShutdown::Unconfirmed);
            assert_eq!(reader.io.closed, 1);
            assert_eq!(reader.query_count(), paid);
        }
    }
}
#[tokio::test]
async fn construction_and_close_do_no_native_work_and_stricter_declared_caps_stay_stricter() {
    let (request, _) = request().await;
    let mut source = source(connector(), &request).unwrap();
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(source.query_count(), 0);
    let tight = CollectionBudget {
        max_queries: 12,
        max_snapshot_rows: 1,
        page_rows: 1,
        statement_timeout_ms: 50,
        total_timeout_ms: 100,
        total_bytes: 8192,
        page_bytes: 2048,
        max_sql_bytes: 512,
        ..CollectionBudget::default()
    };
    assert_eq!(bounded_budget(&tight), tight);
}

#[tokio::test]
async fn source_owned_builtin_admission_keeps_native_scope_and_refuses_forged_foreign_or_retired_permission() {
    use culprit_core::Provider;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (request, scope) = request().await;
    let recipe = crate::telemetry::recipes().into_iter().find(|recipe| recipe.id == "mysql.blocking").unwrap();
    let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else { panic!() };
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"original mysql native route"),
        policy.as_ref().clone(),
        Some(RecipeCostScope::Activity(scope.clone())),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    assert!(admission.qualification().is_none());
    let mut native = reader();
    native.connector = Arc::new(native.connector.with_profile(SourceProfileGuard::new(&request, &admission).unwrap()));
    let observed = native.observe(&request).await.unwrap();
    assert_eq!(observed.records.len(), 2);
    assert!(native.verify(&observed.description).await.unwrap());
    assert_eq!(native.query_count(), 33);
    assert_eq!(native.close().await, SourceShutdown::Quiescent);
    let mut forged = policy.as_ref().clone();
    forged.templates_sha256 = digest(b"foreign native mysql templates");
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"original mysql native route"),
        forged,
        Some(RecipeCostScope::Activity(scope.clone())),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    let provider = crate::MysqlProvider::new(&super::super::activity::tests::specification()).unwrap();
    assert!(provider.telemetry_source_for_safety(&request, &admission).is_err());
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
    let mut foreign = scope.clone();
    foreign.native_epoch = Some("foreign-native-process".into());
    assert!(!foreign.is_valid(), "the independent native epoch cannot contradict its partition");
    assert!(
        SourceProfileAdmission::for_builtin(
            digest(b"original mysql native route"),
            policy.as_ref().clone(),
            Some(RecipeCostScope::Activity(foreign.clone())),
            request.budget.clone(),
            || true,
        )
        .is_err(),
        "malformed native authority refuses before source construction"
    );
    let SourcePartition::InstanceLocal { epoch, .. } = &mut foreign.partition else {
        panic!("genuine original local native scope")
    };
    *epoch = foreign.native_epoch.clone();
    assert!(foreign.is_valid(), "a well-formed other process still cannot bind this original native source");
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"original mysql native route"),
        policy.as_ref().clone(),
        Some(RecipeCostScope::Activity(foreign)),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    let mut wrong = reader();
    wrong.connector = Arc::new(wrong.connector.with_profile(SourceProfileGuard::new(&request, &admission).unwrap()));
    assert_eq!(
        CollectionStop::from_error(&wrong.observe(&request).await.err().unwrap()),
        CollectionStop::SourceChanged
    );
    assert_eq!(wrong.query_count(), 12, "scope must bind after setup/native metadata and before a population query");
    assert!(wrong.verified.is_none());
    assert_eq!(wrong.close().await, SourceShutdown::Quiescent);
    let active = Arc::new(AtomicBool::new(true));
    let owner = active.clone();
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"original mysql native route"),
        policy.as_ref().clone(),
        Some(RecipeCostScope::Activity(scope)),
        request.budget.clone(),
        move || owner.load(Ordering::Acquire),
    )
    .unwrap();
    let guard = SourceProfileGuard::new(&request, &admission).unwrap();
    let mut source = provider.telemetry_source_for_safety(&request, &admission).unwrap();
    active.store(false, Ordering::Release);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    active.store(true, Ordering::Release);
    assert!(guard.ensure_active().is_err(), "retirement is monotonic across clones");
}

#[tokio::test]
async fn actual_selected_reader_binds_native_index_keys_and_withholds_owner_sql_bodies() {
    for version in ["8.4.6", "11.8.9-MariaDB-ubu2404"] {
        let (request, _) = request_on(version).await;
        let mut native = reader();
        for index in [0, 12] {
            let row = native.io.replies[index].as_mut().unwrap().remove(0);
            native.io.replies[index] = Ok(vec![row.with_fields(&[("version", version.into())])]);
        }
        for index in [2, 14] {
            let row = native.io.replies[index].as_mut().unwrap().remove(0);
            native.io.replies[index] = Ok(vec![row.with_fields(&[("native_member", member(version).into())])]);
        }
        let result = native.observe(&request).await.unwrap();
        assert_eq!(result.records.len(), 2);
        assert_eq!(native.io.queries.len(), 15);
        let selected = if version.contains("MariaDB") { Value::UInt(1) } else { Value::UInt(101) };
        for index in [6, 9] {
            assert_eq!(native.io.queries[index].1.as_slice(), std::slice::from_ref(&selected));
        }
        for index in [7, 10] {
            assert_eq!(native.io.queries[index].1, [Value::UInt(101)]);
        }
        for index in [8, 11] {
            assert_eq!(native.io.queries[index].1, [Value::UInt(1), Value::UInt(2)]);
            assert!(!native.io.queries[index].0.contains("AS sql_text"));
            assert!(!native.io.queries[index].0.contains("OCTET_LENGTH"));
        }
        if !version.contains("MariaDB") {
            assert!(native.io.queries[6].0.contains("WHERE w.REQUESTING_THREAD_ID=?"));
        }
        assert!(
            !native.io.queries.iter().any(|(sql, _, _)| sql.contains("sys.schema_table_lock_waits")),
            "selected work cannot materialize an unrelated global TEMPTABLE view"
        );
        assert_eq!(native.query_count(), 33);
        assert_eq!(native.close().await, SourceShutdown::Quiescent);
    }
}

pub(in crate::telemetry) async fn metadata_request_on(version: &str) -> CollectionRequest {
    let (mut request, _) = request_on(version).await;
    let CollectionSelection::FocusedBlocking { focus } = request.selection else { panic!() };
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        database_writable: focus.database_writable,
        observed_from: focus.original_observed_at.clone(),
        observed_until: focus.original_observed_at.clone(),
    };
    let focus = SourceReadFocus::for_metadata_lock_request(*focus).unwrap();
    let execution = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    request.selection = CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) };
    request.budget = metadata_budget(&CollectionBudget::default());
    request.validate().unwrap();
    request
}
