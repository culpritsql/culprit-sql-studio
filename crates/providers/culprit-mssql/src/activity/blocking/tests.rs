use super::*;
use crate::{
    activity::{
        IdentityDetail, ReadOptions,
        database_context::tests::row as authority,
        tests::{details, metadata},
    },
    instance::test_support::{epoch, metadata as platform},
    read::{ReadError, ReadResult},
    resources::test_support::{FakeRead, replace},
    value::{Rec, Val},
};

pub(crate) fn waiter() -> Rec {
    replace(&metadata(42), &[("edition", Val::Int(3)), ("replica_id", Val::Null)])
}
pub(crate) fn owner() -> Rec {
    replace(
        &metadata(55),
        &[
            ("edition", Val::Int(3)),
            ("replica_id", Val::Null),
            ("request_id", Val::Null),
            ("request_generation", Val::Null),
            ("state", Val::Text("sleeping".into())),
            ("no_current_request", Val::Int(1)),
            ("wait_type", Val::Null),
            ("wait_ms", Val::Null),
            ("blocking_session_id", Val::Int(0)),
            ("blocker_generation", Val::Null),
        ],
    )
}
pub(crate) fn bound() -> Vec<ReadResult<Vec<Rec>>> {
    vec![Ok(vec![platform()]), Ok(vec![epoch(100)]), Ok(vec![replace(&authority(3), &[("replica_id", Val::Null)])])]
}
pub(crate) fn replies(rows: Vec<Rec>, observed: bool, enrich: bool) -> Vec<ReadResult<Vec<Rec>>> {
    let mut result = bound();
    result.push(Ok(if rows.is_empty() { vec![replace(&authority(3), &[("replica_id", Val::Null)])] } else { rows }));
    if enrich {
        result.push(Ok(vec![details(42, "SELECT * FROM dbo.orders WHERE id=42")]));
    }
    if observed {
        result.push(Ok(vec![crate::activity::page_targets::tests::participant(0, &waiter())]));
    }
    result.extend(bound());
    result
}
pub(crate) fn reader(replies: Vec<ReadResult<Vec<Rec>>>, budget: CollectionBudget) -> ActivityReader<FakeRead> {
    ActivityReader::new(
        FakeRead::new(replies),
        ReadOptions { budget, identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    )
}
pub(crate) fn unconfirmed_reader(
    replies: Vec<ReadResult<Vec<Rec>>>,
    budget: CollectionBudget,
) -> ActivityReader<FakeRead> {
    let mut owner = reader(replies, budget);
    owner.io.shutdown = SourceShutdown::Unconfirmed;
    owner
}
pub(crate) async fn focus() -> ActivityRequestFocus {
    let mut values = bound();
    values.push(Ok(vec![waiter(), owner()]));
    values.extend(bound());
    let mut original = reader(values, CollectionBudget::default());
    let (source, rows) = crate::telemetry::activity::capture(&mut original).await.unwrap();
    assert_eq!(original.close().await, SourceShutdown::Quiescent);
    let native = source.observation.activity().unwrap();
    let current = rows.iter().find(|row| row.key.session.id == "42").unwrap().clone();
    let capture = CaptureIdentity {
        observation_id: "original-native-lock-wait".into(),
        revision: Some(CaptureRevision {
            outbox_id: "sealed-blocking-fixture".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let row = ExportedRecord::new(&source, &capture, TelemetryRecord::from(current.clone())).unwrap();
    let parent =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"original native lock manifest"), &row, 0).unwrap();
    ActivityRequestFocus {
        parent,
        target: source.target.clone(),
        partition: source.partition.clone(),
        database_writable: native.database_writable,
        source: source.source.clone(),
        epoch: native.epoch.clone().unwrap(),
        original_observed_at: native.at.clone(),
        statement_identity: current.statement_identity.clone().unwrap(),
        original: current,
    }
}
fn facts(
    observation: &culprit_read::FrozenObservation,
) -> (&ActivitySnapshotDescription, &ActivityBlockingFocusResult) {
    let SourceObservation::FocusedBlocking { snapshot, result } = &observation.description.observation else {
        panic!()
    };
    (snapshot, result)
}

#[tokio::test]
async fn the_exact_original_and_owner_use_one_frame_guarded_current_sql_and_final_participant() {
    let original = focus().await;
    let mut current = reader(replies(vec![waiter(), owner()], true, true), CollectionBudget::default());
    let observation = current.capture_focused_blocking(&original).await.unwrap();
    let (snapshot, result) = facts(&observation);
    assert_eq!(result.focus, original);
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(result.request_end, BlockingRequestState::SameStatement);
    assert_eq!(result.membership, FocusedRequestMembership::Observed);
    assert_eq!(result.closure.status, BlockingClosureStatus::Observed);
    assert!(!snapshot.coverage.complete_scope);
    assert_eq!(snapshot.observations, 2);
    assert_eq!(current.query_count(), 9, "nine reader dispatches plus the real session SET make the declared ten");
    let [crate::read::Parameter::Text(selectors), ..] = current.io.calls[4].parameters.as_slice() else { panic!() };
    assert!(selectors.contains("session=\"42\""));
    assert!(!selectors.contains("session=\"55\""), "idle-owner previous batches are uncollected");
    assert_eq!(current.io.calls[4].limit, 1);
    assert_eq!(current.io.calls[5].limit, 1);
    let observations = observation
        .records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(row) = row else { panic!() };
            row
        })
        .collect::<Vec<_>>();
    let rows =
        observations.iter().map(|row| ActivityFrameRow { observation: row, reference: None }).collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(&observation.description, &rows, true).unwrap();
    assert_eq!(report.relationships.len(), 1);
    assert!(report.relationships[0].owner_observed);
    assert!(observations.iter().find(|row| row.key.session.id == "42").unwrap().statement.is_available());
    assert!(!observations.iter().find(|row| row.key.session.id == "55").unwrap().statement.is_available());
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
    assert_eq!(current.io.closed, 1);
}

#[tokio::test]
async fn missing_reused_and_changed_statement_participants_never_acquire_replacement_sql_or_recover_the_incident() {
    let original = focus().await;
    for changed in [
        None,
        Some(vec![("session_generation", Val::Text("2020-01-01T07:00:01".into()))]),
        Some(vec![("request_generation", Val::Text("2020-01-01T07:58:01".into()))]),
        Some(vec![("statement_start", Val::Int(2))]),
    ] {
        let values = changed.as_ref().map(|changes| vec![replace(&waiter(), changes)]).unwrap_or_default();
        let mut current = reader(replies(values, false, false), CollectionBudget::default());
        let observation = current.capture_focused_blocking(&original).await.unwrap();
        let (_, result) = facts(&observation);
        assert_eq!(
            result.binding,
            if changed.is_none() {
                FocusedRequestBinding::ParticipantMissing
            } else {
                FocusedRequestBinding::ParticipantChanged
            }
        );
        assert_eq!(result.membership, FocusedRequestMembership::NotObserved);
        assert_eq!(result.request_end, BlockingRequestState::NotRead);
        assert_eq!(result.closure.status, BlockingClosureStatus::NotRead);
        assert!(observation.records.is_empty());
        assert_eq!(current.query_count(), 7);
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn an_incomplete_census_cannot_become_original_absence_or_an_owner_context_claim() {
    let original = focus().await;
    let mut current = reader(
        replies(vec![waiter(), owner()], false, false),
        CollectionBudget { max_snapshot_rows: 1, ..Default::default() },
    );
    let error = current.capture_focused_blocking(&original).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::RowLimit);
    assert_eq!(current.query_count(), 4);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
    let hidden = replace(&waiter(), &[("database_wide", Val::Int(0))]);
    let mut current = reader(replies(vec![hidden], false, false), CollectionBudget::default());
    assert!(current.capture_focused_blocking(&original).await.is_err());
    assert_eq!(current.query_count(), 4);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn unknown_original_role_foreign_scope_and_unverified_native_profiles_are_not_promoted() {
    let original = focus().await;
    for role in [None, Some(false)] {
        let mut wrong = original.clone();
        wrong.database_writable = role;
        let mut current = reader(vec![], CollectionBudget::default());
        assert!(current.capture_focused_blocking(&wrong).await.is_err());
        assert_eq!(current.query_count(), 0);
    }
    for fault in ["major", "edition", "role", "replica", "epoch"] {
        let mut values = replies(vec![waiter()], false, false);
        let change = match fault {
            "major" => vec![("major", Val::Int(17))],
            "edition" => vec![("edition", Val::Int(2))],
            "role" => vec![("updateability", Val::Text("READ_ONLY".into()))],
            "replica" => vec![("replica_id", Val::Text("91487244-730d-4b83-8e49-d482241c7288".into()))],
            _ => vec![("database_guid", Val::Null)],
        };
        let point = replace(&authority(3), &change);
        values[2] = Ok(vec![replace(
            &point,
            &[(
                "replica_id",
                if fault == "replica" { Val::Text("91487244-730d-4b83-8e49-d482241c7288".into()) } else { Val::Null },
            )],
        )]);
        if fault == "major" {
            values[0] = Ok(vec![replace(&platform(), &[("major", Val::Text("17".into()))])]);
        }
        if fault == "edition" {
            values[0] = Ok(vec![replace(&platform(), &[("edition", Val::Int(2))])]);
        }
        let mut current = reader(values, CollectionBudget::default());
        assert!(current.capture_focused_blocking(&original).await.is_err(), "{fault}");
        assert!(current.query_count() <= 4, "{fault}");
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
    let mut wrong = original;
    wrong.target = culprit_core::DatabaseIdentity::server(Engine::Mssql, "shop", "foreign.invalid", 1433, None).into();
    let mut current = reader(replies(vec![waiter()], false, false), CollectionBudget::default());
    assert!(current.capture_focused_blocking(&wrong).await.is_err());
    assert!(current.query_count() <= 4);
}

#[tokio::test]
async fn changed_final_source_or_unconfirmed_work_is_not_repaired_by_equal_cached_context_or_close() {
    let original = focus().await;
    let mut values = replies(vec![waiter(), owner()], true, true);
    let last = values.last_mut().unwrap();
    *last = Ok(vec![replace(
        &authority(3),
        &[("replica_id", Val::Null), ("recovery_fork_guid", Val::Text("another-fork".into()))],
    )]);
    let mut current = reader(values, CollectionBudget::default());
    assert!(current.capture_focused_blocking(&original).await.is_err());
    assert_eq!(current.query_count(), 9);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
    for position in 0..9 {
        let mut values = replies(vec![waiter(), owner()], true, true);
        values[position] = Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed));
        let mut current = reader(values, CollectionBudget::default());
        current.io.shutdown = SourceShutdown::Unconfirmed;
        let error = current.capture_focused_blocking(&original).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::CancellationUnconfirmed);
        assert_eq!(current.query_count(), position as u32 + 1);
        assert_eq!(current.close().await, SourceShutdown::Unconfirmed);
    }
}

#[tokio::test]
async fn a_final_participant_clock_must_follow_the_frame_and_precede_the_final_native_bound() {
    let original = focus().await;
    for at in ["2020-01-01T05:59:59Z", "2020-01-01T06:00:01Z"] {
        let mut values = replies(vec![waiter(), owner()], true, true);
        values[5] = Ok(vec![replace(
            &crate::activity::page_targets::tests::participant(0, &waiter()),
            &[("observed_at", Val::Text(at.into()))],
        )]);
        let mut current = reader(values, CollectionBudget::default());
        let error = current.capture_focused_blocking(&original).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::SourceChanged);
        assert_eq!(current.query_count(), 9);
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn missing_owner_generation_does_not_join_a_visible_reused_session_number() {
    let original = focus().await;
    let unknown = replace(&waiter(), &[("blocker_generation", Val::Null)]);
    let mut current = reader(replies(vec![unknown, owner()], true, true), CollectionBudget::default());
    let observation = current.capture_focused_blocking(&original).await.unwrap();
    let (_, result) = facts(&observation);
    assert_eq!(observation.records.len(), 1);
    let observations = observation
        .records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(row) = row else { panic!() };
            row
        })
        .collect::<Vec<_>>();
    let rows =
        observations.iter().map(|row| ActivityFrameRow { observation: row, reference: None }).collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&observation.description, &rows, true).unwrap();
    assert!(!result.has_complete_owner_context(&frame));
    let report = culprit_checks::focused_blocking::explain(&observation.description, &rows, true).unwrap();
    assert_eq!(report.relationships.len(), 1);
    assert!(!report.relationships[0].owner_observed);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn the_final_request_guard_preserves_changed_missing_and_unknown_as_distinct_outcomes() {
    let original = focus().await;
    for (change, expected) in [
        (("checked_request_generation", Val::Text("2020-01-01T07:58:02".into())), Some(BlockingRequestState::Changed)),
        (("checked_request_id", Val::Null), Some(BlockingRequestState::Missing)),
        (("checked_sql_handle", Val::Null), None),
    ] {
        let mut values = replies(vec![waiter(), owner()], true, true);
        values[5] = Ok(vec![replace(&crate::activity::page_targets::tests::participant(0, &waiter()), &[change])]);
        let mut current = reader(values, CollectionBudget::default());
        let observed = current.capture_focused_blocking(&original).await;
        if let Some(expected) = expected {
            assert_eq!(facts(&observed.unwrap()).1.request_end, expected);
        } else {
            assert_eq!(CollectionStop::from_error(&observed.err().unwrap()), CollectionStop::SourceUnavailable);
        }
        assert_eq!(current.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn a_recorded_prelogin_or_pool_reset_withdraws_original_generation_authority_before_any_read() {
    let mut original = focus().await;
    original.original.session_context = Some(Box::new(ActivitySessionContext {
        lifecycle: SessionLifecycle::PoolResetPrelogin,
        login_age_ms: None,
        last_request_end_age_ms: None,
        client_library: None,
        client_process_id: None,
        native: None,
    }));
    let mut current = reader(vec![], CollectionBudget::default());
    assert!(current.capture_focused_blocking(&original).await.is_err());
    assert_eq!(current.query_count(), 0);
    assert_eq!(current.close().await, SourceShutdown::Quiescent);
}

#[test]
fn actual_native_focused_capture_future_has_bounded_inline_storage() {
    fn storage<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let bytes = storage(
        |(reader, focus): (&'static mut ActivityReader<crate::read::ReadSession>, &'static ActivityRequestFocus)| {
            reader.capture_focused_blocking(focus)
        },
    );
    assert!(bytes <= 64 * 1024, "actual NativeRead-specialized focused capture storage: {bytes}");
}
