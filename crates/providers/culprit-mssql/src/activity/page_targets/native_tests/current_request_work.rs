//! Reuse the nonce page fixture's one gate, actor slots and durable capture owner. No holder claim is made.
use super::{
    durable_capture::{Captured, OwnedCaptures, require_readable_capture},
    fixture::{self, Fixture, actors, invalid},
    workload::CompletedWorker,
};
use crate::{
    conn::MsClient,
    read::text,
    test_support::{owned_case, receipt_directory, save_cost_receipt},
};
use culprit_core::{CurrentRequestWorkReport, Result, SourceState, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;
use std::time::Duration;

const APPLICATION: &str = "current-request-work-owned-actor";
const WORK: &str = "SET LOCK_TIMEOUT 15000;SELECT COUNT_BIG(*) AS completed FROM dbo.page_fixture_select WITH(READCOMMITTEDLOCK) OPTION(MAXDOP 1)";
// Existing nonce DB setup/admin+seed:8; two gate SET/control/release:6; two actor SET/identity/warm/work:8; DROP:1.
const FIXTURE_DISPATCH_CAP: u32 = 23;
const CASE_TIMEOUT: Duration = Duration::from_secs(35);

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn owned_current_request_factory_preserves_native_work_after_actors_end_or_same_label_returns() {
    Box::pin(fixture::run(|fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let (expired, result) = owned_case::finish_case(
                tokio::time::Instant::now() + CASE_TIMEOUT,
                &cancellation,
                Box::pin(prove(fixture, &cancellation)),
            )
            .await;
            result?;
            if expired {
                return Err(CollectionStop::TimedOut.error());
            }
            Ok(())
        })
    }))
    .await;
}

async fn prove(fixture: &mut Fixture, cancellation: &CollectionCancellation) -> Result<()> {
    let mut captures = OwnedCaptures::new(fixture)?;
    let first_actor = Box::pin(prepare_actor(fixture, cancellation)).await?;
    let first_session = first_actor.1.clone();
    Box::pin(start_actor(fixture, first_actor, cancellation)).await?;
    let original = capture(&mut captures, CollectionSelection::Activity, cancellation).await?;
    let focus = request_focus(&original, &first_session)?;
    let current = capture(&mut captures, selected(&focus), cancellation).await?;
    require_native_positive(&current, &focus)?;
    Box::pin(finish_actor(fixture, cancellation)).await?;

    // A new physical login uses exactly the same SQL and label. Neither label nor SQL can heal the old focus.
    let next_actor = Box::pin(prepare_actor(fixture, cancellation)).await?;
    let next_session = next_actor.1.clone();
    if first_session == next_session {
        return Err(invalid("fresh actor login did not establish its own native generation"));
    }
    Box::pin(start_actor(fixture, next_actor, cancellation)).await?;
    let next_original = capture(&mut captures, CollectionSelection::Activity, cancellation).await?;
    let next_focus = request_focus(&next_original, &next_session)?;
    let old_lookup = capture(&mut captures, selected(&focus), cancellation).await?;
    require_no_predecessor_work(&old_lookup, &focus)?;
    let next_current = capture(&mut captures, selected(&next_focus), cancellation).await?;
    require_native_positive(&next_current, &next_focus)?;
    Box::pin(finish_actor(fixture, cancellation)).await?;

    for original in [&original, &current, &next_original, &old_lookup, &next_current] {
        Box::pin(captures.verify_retained(original)).await?;
    }
    save_native_receipt(
        [original.as_ref(), current.as_ref(), next_original.as_ref(), old_lookup.as_ref(), next_current.as_ref()],
        first_session.id == next_session.id,
    )
}

/// Serialization and offline presentation run after every native source and actor has settled. Their temporary
/// stack storage never remains live while another native login/query is being polled.
fn save_native_receipt(captures: [&Captured; 5], session_number_reused: bool) -> Result<()> {
    let [original, current, next_original, old_lookup, next_current] = captures;
    let current_report = report(current)?;
    let old_report = report(old_lookup)?;
    assert_eq!(current_report.entries.len(), 1);
    assert!(old_report.entries.is_empty());
    let source_reader_queries = [original, current, next_original, old_lookup, next_current]
        .into_iter()
        .map(|read| read.manifest.source_queries)
        .sum::<u32>();
    let source_total_allowance = [original, current, next_original, old_lookup, next_current]
        .into_iter()
        .map(|read| read.manifest.request.budget.max_queries)
        .sum::<u32>();
    save_cost_receipt(
        &receipt_directory(),
        "current-request-work-original-native",
        &json!({
            "original": {"manifest":original.manifest,"records":original.records},
            "current": {"manifest":current.manifest,"records":current.records,"report":current_report},
            "next_original": {"manifest":next_original.manifest,"records":next_original.records},
            "old_lookup": {"manifest":old_lookup.manifest,"records":old_lookup.records,"report":old_report},
            "next_current": {"manifest":next_current.manifest,"records":next_current.records},
            "source_reader_queries":source_reader_queries,
            "source_total_allowance_including_session_setup":source_total_allowance,
            "fixture_dispatch_cap_including_setup_identity_warm_commands_gates_cleanup":FIXTURE_DISPATCH_CAP,
            "combined_dispatch_cap":source_total_allowance+FIXTURE_DISPATCH_CAP,
            "fixture_rows_per_count":100,"fixture_table_seed_rows":102,"fixture_database_data_limit_mb":64,
            "workload_rows_written":0,"actor_lock_timeout_ms":15000,"case_timeout_ms":35000,
            "same_label_is_native_identity":false,"session_number_reused":session_number_reused,
            "old_request_ended_inferred_from_missing_row":false,"earlier_executor_plan_attested":false,
            "cache_totals_are_current_request_work":false,"azure_native_factory_verified":false,
            "native_cost_qualified":false,"originals_reverified_after_actors_retired":true,
        }),
    )?;
    Ok(())
}

async fn prepare_actor(
    fixture: &Fixture,
    cancellation: &CollectionCancellation,
) -> Result<(MsClient, NativeActivityIdentity)> {
    owned_case::require_active(cancellation)?;
    let mut client = fixture.client(APPLICATION).await?;
    let result = async {
        owned_case::require_active(cancellation)?;
        let rows = crate::conn::query(&mut client, "SELECT /* culprit fixture */ CONVERT(varchar(16),s.session_id) AS session_id,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS login_time FROM sys.dm_exec_sessions s WHERE s.session_id=@@SPID").await.map_err(crate::conn::map_error)?;
        let [row] = rows.as_slice() else { return Err(invalid("owned actor identity unavailable")) };
        let session = NativeActivityIdentity {
            id:text(row,"session_id")?,
            generation:crate::sql_time::native_time(&text(row,"login_time")?)?,
        };
        if !session.is_valid() { return Err(invalid("owned actor identity malformed")); }
        owned_case::require_active(cancellation)?;
        client.simple_query(WORK).await.map_err(crate::conn::map_error)?.into_results().await.map_err(crate::conn::map_error)?;
        Ok(session)
    }.await;
    match result {
        Ok(session) => Ok((client, session)),
        Err(original) => {
            let _ = owned_case::retire(client).await;
            Err(original)
        }
    }
}
async fn start_actor(
    fixture: &mut Fixture,
    actor: (MsClient, NativeActivityIdentity),
    cancellation: &CollectionCancellation,
) -> Result<()> {
    let (client, _) = actor;
    let admitted = async {
        owned_case::require_active(cancellation)?;
        fixture.start_holder_gate().await?;
        owned_case::require_active(cancellation)
    }
    .await;
    if let Err(original) = admitted {
        let _ = owned_case::retire(client).await;
        return Err(original);
    }
    actors::start(&mut fixture.workers, vec![(client, (), WORK.into())], |(), row| {
        Ok(CompletedWorker { pattern: 0, inserts: actors::completion_count(row, "completed")? })
    })
    .await?;
    Ok(())
}

async fn finish_actor(fixture: &mut Fixture, cancellation: &CollectionCancellation) -> Result<()> {
    owned_case::require_active(cancellation)?;
    fixture.release_holder_gate().await?;
    let completed = fixture.finish_workload().await?;
    if completed.len() != 1 || completed[0].inserts != 100 {
        return Err(invalid("owned current request did not drain its one exact COUNT result"));
    }
    Ok(())
}
fn selected(focus: &ActivityRequestFocus) -> CollectionSelection {
    CollectionSelection::CurrentRequestWork { focus: Box::new(focus.clone()) }
}
async fn capture(
    captures: &mut OwnedCaptures,
    selection: CollectionSelection,
    cancellation: &CollectionCancellation,
) -> Result<Box<Captured>> {
    let captured = Box::new(Box::pin(captures.capture_with_cancellation(selection, cancellation)).await?);
    require_readable_capture(&captured)?;
    Ok(captured)
}
fn request_focus(captured: &Captured, session: &NativeActivityIdentity) -> Result<ActivityRequestFocus> {
    let source = captured.source()?;
    let native = source.observation.activity().ok_or_else(|| invalid("native Activity source absent"))?;
    let context = captured.activity_context()?;
    let matches = captured
        .records
        .iter()
        .enumerate()
        .filter_map(|(ordinal, row)| match &row.record {
            TelemetryRecord::Activity(row)
                if row.key.session == *session && row.scope == ActivityScopeBasis::RequestDatabase =>
            {
                Some((ordinal, row.as_ref()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(ordinal, row)] = matches.as_slice() else {
        return Err(invalid("one genuine owned current request required"));
    };
    let focus = ActivityRequestFocus {
        partition: context.partition,
        target: context.target,
        source: source.source.clone(),
        database_writable: context.database_writable,
        parent: captured.reference(*ordinal)?,
        epoch: native.epoch.clone().ok_or_else(|| invalid("native Activity epoch absent"))?,
        original_observed_at: native.at.clone(),
        statement_identity: row.statement_identity.clone().ok_or_else(|| invalid("native statement token absent"))?,
        original: (**row).clone(),
    };
    if !CurrentRequestWorkProfile::MssqlRequest.accepts_focus(&focus) {
        return Err(invalid("native focus is not a supported request identity"));
    }
    Ok(focus)
}
fn require_native_positive(captured: &Captured, focus: &ActivityRequestFocus) -> Result<()> {
    let SourceObservation::CurrentRequestWork { profile, snapshot, result } = &captured.source()?.observation else {
        return Err(invalid("current-work header absent"));
    };
    if *profile != CurrentRequestWorkProfile::MssqlRequest
        || snapshot.records != 1
        || captured.records.len() != 1
        || result.focus.as_deref() != Some(focus)
        || result.binding != FocusedRequestBinding::Matched
        || result.membership != FocusedRequestMembership::Observed
    {
        return Err(invalid("current-work source did not retain its exact original native focus"));
    }
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::CurrentRequestWork(row), .. }) =
        &captured.records[0].record
    else {
        return Err(invalid("current-work row absent"));
    };
    let CurrentRequestWorkNative::Mssql { work } = &row.native else {
        return Err(invalid("wrong native request profile"));
    };
    if row.actor != focus.original.key
        || work.elapsed_ms.is_none()
        || work.cpu_ms.is_none()
        || [&work.reads, &work.writes, &work.logical_reads, &work.rows_returned]
            .into_iter()
            .any(|count| count.is_none())
        || !matches!(&work.current_plan, MssqlCurrentRequestPlan::Observed { .. })
        || captured.manifest.source_queries != 10
    {
        return Err(invalid("native request/cache fields were not genuinely observed"));
    }
    Ok(())
}
fn require_no_predecessor_work(captured: &Captured, focus: &ActivityRequestFocus) -> Result<()> {
    let SourceObservation::CurrentRequestWork { snapshot, result, .. } = &captured.source()?.observation else {
        return Err(invalid("current-work header absent"));
    };
    if result.focus.as_deref() != Some(focus)
        || result.membership != FocusedRequestMembership::NotObserved
        || snapshot.records != 0
        || !captured.records.is_empty()
        || captured.manifest.source_queries != 7
        || snapshot.sources.iter().any(|source| source.state != SourceState::Partial || source.complete_scope)
    {
        return Err(invalid("old native request gained replacement work or a complete absence claim"));
    }
    Ok(())
}
fn report(captured: &Captured) -> Result<CurrentRequestWorkReport> {
    let source = captured.source()?;
    let SourceObservation::CurrentRequestWork { profile, snapshot, result } = &source.observation else {
        return Err(invalid("current-work header absent"));
    };
    let observations = captured
        .records
        .iter()
        .map(|row| match &row.record {
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::CurrentRequestWork(row),
                ..
            }) => Ok(row.as_ref().clone()),
            _ => Err(invalid("foreign current-work row")),
        })
        .collect::<Result<Vec<_>>>()?;
    culprit_checks::current_request_work::explain(
        &CurrentRequestWorkSnapshot {
            profile: *profile,
            source: source.source.clone(),
            context: result.read_context(source, snapshot),
            coverage: snapshot.sources[0].clone(),
            result: result.as_ref().clone(),
            observations,
        },
        true,
    )
}

#[test]
fn native_case_and_capture_measure_concrete_future_storage_and_heap_owned_output_without_polling() {
    fn future_bytes<Arguments, Function, Future>(_: Function) -> usize
    where
        Function: FnOnce(Arguments) -> Future,
    {
        std::mem::size_of::<Future>()
    }
    // This measures the actual prove/capture state, not a Pin<Box<dyn Future>> pointer. It complements the
    // default-stack owned native execution: state size alone cannot bound synchronous poll temporaries.
    const MAX_CASE_FUTURE_BYTES: usize = 32 * 1024;
    const MAX_CAPTURE_FUTURE_BYTES: usize = 4 * 1024;
    const MAX_CAPTURE_OUTPUT_BYTES: usize = 128;
    let case = future_bytes(|(fixture, cancellation): (&'static mut Fixture, &'static CollectionCancellation)| {
        prove(fixture, cancellation)
    });
    let selected = future_bytes(
        |(captures, selection, cancellation): (
            &'static mut OwnedCaptures,
            CollectionSelection,
            &'static CollectionCancellation,
        )| capture(captures, selection, cancellation),
    );
    let output = std::mem::size_of::<Result<Box<Captured>>>();
    assert!(case <= MAX_CASE_FUTURE_BYTES, "concrete native case future: {case} bytes");
    assert!(selected <= MAX_CAPTURE_FUTURE_BYTES, "concrete owned capture future: {selected} bytes");
    assert!(output <= MAX_CAPTURE_OUTPUT_BYTES, "heap-owned native capture result: {output} bytes");
}
