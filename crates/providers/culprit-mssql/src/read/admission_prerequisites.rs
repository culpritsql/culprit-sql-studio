//! Explicit strict admission prerequisites, excluded from the ordinary native_tests:: smoke roster.
//! Official0.13.0 can miss a separate-message DONE_ATTN; these tests must not accept observed quiet as an ACK.
use super::native_tests::{
    CPU_CANCELLATION_SQL, MAX_CPU_OBSERVER_QUERIES, MAX_QUIET_OBSERVER_QUERIES, assert_quiet, fixture,
    observe_cpu_start,
};
use super::*;
use crate::test_support::{owned_case::retire, receipt_directory, save_cost_receipt, verify_owned_source};
use culprit_core::telemetry::SourceShutdown;
use futures_util::FutureExt;
use serde_json::json;
use std::panic::AssertUnwindSafe;

#[tokio::test]
#[ignore = "strict builtin-admission prerequisite on explicit nonce-owned loopback only; expected blocked by official tiberius0.13 separate-EOM ACK defect, never interpreted as a passing native smoke"]
async fn an_active_cpu_deadline_requires_confirmed_shutdown_and_same_pool_debt_remains_sticky_on_failure() {
    prove(false).await;
}
#[tokio::test]
#[ignore = "strict builtin-admission prerequisite on explicit nonce-owned loopback only; original query borrow is dropped then same owner closes, no socket-close/timer substitute for Attention ACK"]
async fn dropping_an_active_cpu_borrow_requires_confirmed_shutdown_from_the_same_native_owner() {
    prove(true).await;
}

async fn prove(drop_borrow: bool) {
    let directory = receipt_directory();
    let (mut reader, mut observer, session) = fixture().await;
    verify_owned_source(&mut observer).await.expect("original held observer verifies the nonce-owned instance");
    let queries_before = reader.queries();
    let outcome = AssertUnwindSafe(async {
        let work = reader.query(CPU_CANCELLATION_SQL, vec![], 1);
        tokio::pin!(work);
        let observed = observe_cpu_start(work.as_mut(), &mut observer, &session).await;
        let result = if drop_borrow { None } else { Some(work.await) };
        (observed, result)
    })
    .catch_unwind()
    .await;
    // Even failed/panicked readiness retains this reader until the same active native task has settled.
    let shutdown = reader.close().await;
    // Readiness may have abandoned an observer borrow on panic. Retire that same observer; never submit
    // another observation on an uncertain client merely to improve this diagnostic.
    let quiet = if outcome.is_ok() {
        Some(AssertUnwindSafe(assert_quiet(&mut observer, &session)).catch_unwind().await)
    } else {
        None
    };
    // Independent observation above does not repair the native driver's failed acknowledgement.
    let mut replacement_refused = None;
    if shutdown == SourceShutdown::Unconfirmed {
        let count = reader.queries();
        let replacement = reader.query("SELECT /* culprit */ 1 AS n", vec![], 1).await;
        replacement_refused =
            Some(matches!(replacement, Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))));
        assert_eq!(reader.queries(), count, "unconfirmed work cannot issue replacement SQL");
        let mut sibling = ReadSession::new(reader.pool.clone(), CollectionBudget::default());
        assert!(matches!(sibling.admit(), Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))));
        assert_eq!(sibling.close().await, SourceShutdown::Unconfirmed);
    }
    let retired = retire(observer).await;
    let saved = save_cost_receipt(
        &directory,
        if drop_borrow { "mssql-active-borrow-strict-admission" } else { "mssql-active-timeout-strict-admission" },
        &json!({
            "official_tiberius_version":"0.13.0",
            "official_archive_sha256":"e07324791de2bdaed058af4aa433a0b5ec5c0beac407a9797419c19805df2415",
            "native_work_issued":reader.queries()-queries_before,
            "source_identity_and_cpu_query_limit":2,"possible_source_SET_limit":1,
            "observer_readiness_queries":outcome.as_ref().ok().map(|(count,_)|count),"observer_readiness_query_limit":MAX_CPU_OBSERVER_QUERIES,
            "fixture_nonce_verification_queries":1,"quiet_observer_queries":quiet.as_ref().and_then(|value|value.as_ref().ok()),
            "quiet_observer_query_limit":MAX_QUIET_OBSERVER_QUERIES,
            "fixture_dispatch_cap_including_both_session_SETs":2+2+1+MAX_CPU_OBSERVER_QUERIES+MAX_QUIET_OBSERVER_QUERIES,
            "independent_quiet_observation":"existing bounded3s native observer; not Attention acknowledgement",
            "shutdown_quiescent":shutdown==SourceShutdown::Quiescent,"replacement_refused_without_dispatch":replacement_refused,
            "result_stop":outcome.as_ref().ok().and_then(|(_,result)|result.as_ref()).map(|result|match result {
                Ok(_) => "unexpected-output",
                Err(ReadError::Stopped(CollectionStop::TimedOut)) => "timed-out",
                Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)) => "cancellation-unconfirmed",
                Err(_) => "other-error",
            }),
            "source_phase_panicked":outcome.is_err(),"independent_quiet_observation_completed":quiet.as_ref().is_some_and(|value|value.is_ok()),
            "observer_native_work_unconfirmed":outcome.is_err() || quiet.as_ref().is_some_and(|value|value.is_err()),
            "same_observer_retired":retired.is_ok(),"observer_retirement_error":retired.as_ref().err().map(|error|error.code),
            "original_pool_debt_preserved":shutdown==SourceShutdown::Unconfirmed,
            "confirmed_shutdown_prerequisite_pass":shutdown==SourceShutdown::Quiescent,
            "attention_branch_separately_traced":false,"exact_template_active_cancellation_proved":false,
            "source_read_metrics":reader.read_metrics(),
            "admission":"unqualified","measured_native_cost":false,
            "unexercised":"exact Activity/blocking templates, routing/setup interrupts and every other native edition/version/role remain independent prerequisites",
        }),
    );
    let (_, result) = match outcome {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    if !drop_borrow {
        assert!(
            matches!(result, Some(Err(ReadError::Stopped(CollectionStop::TimedOut)))),
            "a shutdown prerequisite cannot be passed by source failure, accepted rows or missing ACK: {result:?}"
        );
    }
    assert_eq!(
        shutdown,
        SourceShutdown::Quiescent,
        "a quiet observed server or closed socket cannot authorize a missing Attention acknowledgement"
    );
    if let Some(Err(panic)) = quiet {
        std::panic::resume_unwind(panic);
    }
    saved.expect("private strict prerequisite receipt");
    retired.expect("same native observer retired before any assertion");
}
