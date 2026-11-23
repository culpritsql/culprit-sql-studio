use super::*;
use crate::activity::current_request_work::tests::{self as fixture, Io};
use crate::instance::test_support::{epoch, metadata};
use crate::read::{ReadError, ReadResult};
use crate::resources::test_support::replace;
use crate::value::{Rec, Val};
use std::sync::atomic::Ordering;

pub(crate) async fn focus_and_scope() -> (SourceReadFocus, SourceReadExecutionScope) {
    let original = fixture::focus_for_role(false).await;
    let context = NativeReadContext {
        target: original.target.clone(),
        partition: original.partition.clone(),
        database_writable: Some(false),
        observed_from: original.original_observed_at.clone(),
        observed_until: original.original_observed_at.clone(),
    };
    let focus = SourceReadFocus {
        basis: Box::new(original.parent.clone()),
        target: original.target.clone(),
        question: SourceReadQuestion::ReadableSecondaryRecovery,
        prerequisite: SourceReadPrerequisite::ObservedReadableSecondaryReader { request: Box::new(original) },
    };
    let scope = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    (focus, scope)
}
fn ag(at: &str) -> Rec {
    let original = crate::replication::tests::availability();
    let original = replace(&original, &[("database_id", Val::Int(7)), ("is_local", Val::Int(1))]);
    let mut fields = original.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    fields.push(("recovery_observed_at", Val::Text(at.into())));
    Rec::of(&fields)
}
fn selected(at: &str) -> Rec {
    replace(&fixture::point(at), &[("updateability", Val::Text("READ_ONLY".into()))])
}
pub(crate) fn redo_row(at: &str) -> Rec {
    Rec::of(&[
        ("observed_at", Val::Text(at.into())),
        ("selected_database_id", Val::Int(7)),
        ("updateability", Val::Text("READ_ONLY".into())),
        ("redo_session_id", Val::Int(19)),
        ("redo_request_id", Val::Int(0)),
        ("redo_generation", Val::Text("2020-01-01T04:00:00.0030000".into())),
        ("redo_started", Val::Text("2020-01-01T05:58:00.0030000".into())),
        ("redo_command", Val::Text("DB STARTUP".into())),
        ("redo_wait_type", Val::Text("LCK_M_SCH_M".into())),
        ("redo_wait_ms", Val::Int(3)),
        ("redo_wait_resource", Val::Text("OBJECT: 7:42000:0".into())),
        ("redo_blocker", Val::Int(42)),
        ("reader_session_id", Val::Int(42)),
        ("reader_generation", Val::Text("2020-01-01T04:00:00.0030000".into())),
    ])
}
pub(crate) fn replies() -> Vec<ReadResult<Vec<Rec>>> {
    let layout = || crate::replication::tests::layout(Val::Int(1), 1);
    vec![
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![fixture::authority("2020-01-01T06:00:00Z", Some("READ_ONLY"))]),
        Ok(vec![selected("2020-01-01T06:00:00.1Z")]),
        Ok(vec![layout()]),
        Ok(vec![ag("2020-01-01T06:00:00.2Z")]),
        Ok(vec![redo_row("2020-01-01T06:00:00.3Z")]),
        Ok(vec![layout()]),
        Ok(vec![ag("2020-01-01T06:00:00.4Z")]),
        Ok(vec![redo_row("2020-01-01T06:00:00.5Z")]),
        Ok(vec![selected("2020-01-01T06:00:00.6Z")]),
        Ok(vec![metadata()]),
        Ok(vec![epoch(100)]),
        Ok(vec![fixture::authority("2020-01-01T06:00:01Z", Some("READ_ONLY"))]),
    ]
}
async fn capture(replies: Vec<ReadResult<Vec<Rec>>>) -> (Result<ReadableSecondaryRecoveryPoint>, ActivityReader<Io>) {
    let (focus, scope) = focus_and_scope().await;
    let mut reader = ActivityReader::new(
        fixture::io(replies),
        ReadOptions { budget: CollectionBudget::default(), identity: IdentityDetail::Endpoint },
        "fixture.invalid".into(),
        1433,
    );
    let result = reader.capture_readable_secondary_recovery(&focus, &scope).await;
    (result, reader)
}
#[tokio::test]
async fn actual_local_database_recovery_epoch_and_readonly_ag_redo_bind_the_reader_without_global_incarnation() {
    let (result, mut reader) = capture(replies()).await;
    let point = result.unwrap();
    assert!(point.context.target.database().unwrap().incarnation.is_none());
    assert_eq!(point.native_blocking_readers().len(), 1);
    assert_eq!(point.native_blocking_readers()[0].session, point.focus.original.key.session);
    assert_eq!(reader.query_count(), 14);
    let calls = reader.io.trace.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|sql| sql.contains("drs.is_local=1")).count(), 2);
    assert_eq!(calls.iter().filter(|sql| sql.contains("native.command IN")).count(), 2);
    assert!(
        calls
            .iter()
            .filter(|sql| sql.contains("native.command IN"))
            .all(|sql| sql.contains("native.database_id=DB_ID()") && sql.contains("s.is_user_process=0"))
    );
    drop(calls);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn native_error_or_cancel_at_each_phase_never_reacquires_or_finishes_an_uncertain_original() {
    for index in 0..14 {
        let mut scripted = replies();
        scripted[index] = Err(ReadError::Stopped(CollectionStop::Cancelled));
        let (result, mut reader) = capture(scripted).await;
        assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::Cancelled, "{index}");
        assert_eq!(reader.query_count(), index as u32 + 1);
        assert_eq!(reader.io.trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn foreign_ag_member_reader_generation_and_final_role_do_not_become_native_blockers() {
    for defect in ["reader-reuse", "redo-reuse", "different-wait", "ag-member", "final-role"] {
        let mut scripted = replies();
        let index = if defect == "ag-member" {
            8
        } else if defect == "final-role" {
            13
        } else {
            9
        };
        let rows = scripted[index].as_mut().unwrap();
        let (name, value) = match defect {
            "reader-reuse" => ("reader_generation", Val::Text("2020-01-01T05:00:00.0030000".into())),
            "redo-reuse" => ("redo_generation", Val::Text("2020-01-01T05:00:00.0030000".into())),
            "different-wait" => ("redo_wait_type", Val::Text("PAGEIOLATCH_SH".into())),
            "ag-member" => ("replica_id", Val::Text("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE".into())),
            _ => ("updateability", Val::Text("READ_WRITE".into())),
        };
        rows[0] = replace(&rows[0], &[(name, value)]);
        let (result, mut reader) = capture(scripted).await;
        if matches!(defect, "ag-member" | "final-role") {
            assert!(result.is_err(), "{defect}");
        } else {
            assert!(result.unwrap().native_blocking_readers().is_empty(), "{defect}");
        }
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
