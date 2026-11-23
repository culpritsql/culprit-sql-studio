//! One explicitly selected boxed instance capture through the existing reader and canonical native-fixture owner.
use super::idle_connections::{IdleIdentity, setup::IdleClients};
use super::*;
use crate::{
    read::ReadSession,
    test_support::owned_captures::{Captured, OwnedCaptures},
};
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, path::Path};
use tokio::time::Instant;

const INSTANCE_DISCOVERY_QUERIES: u32 = 2 + ReadSession::SESSION_SETUP_QUERIES;
const MAX_NATIVE_CASE_FUTURE_BYTES: usize = 32 * 1024;

#[test]
fn owned_instance_census_concrete_orchestration_future_stays_bounded_before_any_fixture_or_native_work() {
    let cancellation = CollectionCancellation::default();
    let future = run(Path::new("unopened-owned-instance-fixture"), &cancellation);
    assert!(
        std::mem::size_of_val(&future) <= MAX_NATIVE_CASE_FUTURE_BYTES,
        "concrete native case future is {} bytes",
        std::mem::size_of_val(&future)
    );
}

#[tokio::test]
#[ignore = "explicit nonce-owned loopback SQL Server only;3 owned idle clients,one3-query discovery,one8-query instance source and one stricter6-query refusal;unchanged production limits,60s fixture bound;not native cost or active-cancellation qualification"]
async fn owned_boxed_instance_census_preserves_native_member_idle_clients_recent_view_and_canonical_original()
-> Result<()> {
    let directory = receipt_directory();
    let cancellation = CollectionCancellation::default();
    let started = Instant::now();
    let (expired, outcome) = finish_case(
        started + CASE_TIMEOUT,
        &cancellation,
        AssertUnwindSafe(Box::pin(run(&directory, &cancellation))).catch_unwind(),
    )
    .await;
    let saved = save_cost_receipt(
        &directory,
        "instance-census-native-outcome",
        &json!({
            "completed":!expired&&matches!(&outcome,Ok(Ok(()))),"expired":expired,
            "fixture_elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            "fixture_actor_cap":OWNED_IDLE_SESSIONS,"fixture_actor_identity_queries":OWNED_IDLE_SESSIONS,
            "instance_discovery_including_possible_setup":INSTANCE_DISCOVERY_QUERIES,
            "production_source_max_queries":8,"production_source_max_rows":500,
            "native_cpu_io_cost_qualified":false,"production_admission":"unqualified",
            "limitations":"only actual observed native edition/version accepted;no Azure/MI or active exact-template cancellation qualification"
        }),
    );
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    saved?;
    if expired {
        return Err(CollectionStop::TimedOut.error());
    }
    Ok(())
}

async fn run(directory: &Path, cancellation: &CollectionCancellation) -> Result<()> {
    let fixture = Box::pin(OwnedSourceFixture::open("culprit-instance-census")).await?;
    let mut clients = IdleClients::new();
    let mut captures = Box::new(OwnedCaptures::from_pool(fixture.pool.clone(), fixture.port)?);
    let work = AssertUnwindSafe(Box::pin(async {
        Box::pin(clients.extend_to(OWNED_IDLE_SESSIONS, &fixture, cancellation)).await?;
        let native = Box::pin(discover(&fixture, directory, cancellation)).await?;
        let subject = CollectionSubject::Shared { resource: native.identity.clone() };
        Box::pin(refuse_stricter_caller(&fixture, &subject, directory, cancellation)).await?;
        let captured =
            Box::pin(captures.capture_on_subject(CollectionSelection::Activity, subject, cancellation)).await?;
        validate_original(&captured, &native, clients.identities())?;
        Box::pin(captures.verify_retained(&captured)).await?;
        save_original(directory, &captured, &native)?;
        Ok::<(), culprit_core::Error>(())
    }))
    .catch_unwind()
    .await;
    clients.fence_unconfirmed(&fixture.pool);
    let retirement = AssertUnwindSafe(Box::pin(clients.retire())).catch_unwind().await;
    let shutdown = fixture.pool.source_shutdown();
    let saved = save_cost_receipt(
        directory,
        "instance-census-native-retirement",
        &json!({
            "clients":clients.accounting(),"pool_shutdown":shutdown_state(shutdown),
            "supervisor_cleanup_required":shutdown!=SourceShutdown::Quiescent||!matches!(&retirement,Ok(Ok(()))),
            "source_cost_qualified":false,
        }),
    );
    // Preserve the first source/identity failure or panic after awaiting the same actors' retirement.
    match work {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    match retirement {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    saved?;
    if shutdown != SourceShutdown::Quiescent {
        return Err(CollectionStop::CancellationUnconfirmed.error());
    }
    Ok(())
}

async fn discover(
    fixture: &OwnedSourceFixture,
    directory: &Path,
    cancellation: &CollectionCancellation,
) -> Result<Box<crate::instance::Context>> {
    let mut budget = crate::telemetry::activity::bounded_budget(&CollectionBudget::default());
    budget.max_queries = INSTANCE_DISCOVERY_QUERIES;
    let mut reader = ReadSession::collection(fixture.pool.clone(), budget)?;
    let outcome = AssertUnwindSafe(until_cancelled(cancellation, Box::pin(crate::instance::read(&mut reader))))
        .catch_unwind()
        .await;
    let shutdown = reader.close().await;
    let saved = save_cost_receipt(
        directory,
        "instance-census-native-discovery",
        &json!({
            "queries":reader.queries(),"max_queries_including_setup":INSTANCE_DISCOVERY_QUERIES,
            "shutdown":shutdown_state(shutdown),"outside_instance_source_measurement":true,
            "source_cost_qualified":false,
        }),
    );
    let (native, _) = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    saved?;
    if shutdown != SourceShutdown::Quiescent {
        return Err(CollectionStop::CancellationUnconfirmed.error());
    }
    if reader.queries() > INSTANCE_DISCOVERY_QUERIES || !matches!(native.engine_edition(), 2..=4) {
        return Err(invalid_data());
    }
    Ok(Box::new(native))
}

async fn refuse_stricter_caller(
    fixture: &OwnedSourceFixture,
    subject: &CollectionSubject,
    directory: &Path,
    cancellation: &CollectionCancellation,
) -> Result<()> {
    let mut budget = crate::telemetry::activity::bounded_budget(&CollectionBudget::default());
    budget.max_queries = SOURCE_QUERY_LIMIT - 1;
    let request = CollectionRequest { subject: subject.clone(), selection: CollectionSelection::Activity, budget };
    let mut source = crate::telemetry::source(fixture.pool.clone(), &request, "127.0.0.1".into(), fixture.port)?;
    let outcome = AssertUnwindSafe(until_cancelled(cancellation, Box::pin(source.describe()))).catch_unwind().await;
    let queries = source.query_count();
    let completion = source.finish().await;
    let final_queries = source.query_count();
    let shutdown = source.close().await;
    let saved = save_cost_receipt(
        directory,
        "instance-census-native-strict-caller",
        &json!({
            "max_queries":request.budget.max_queries,"queries":queries,"final_queries":final_queries,
            "completion_refused":completion.is_err(),"shutdown":shutdown_state(shutdown),"source_cost_qualified":false,
        }),
    );
    let error = match outcome {
        Ok(Err(error)) => error,
        Ok(Ok(_)) => return Err(invalid_data()),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    if CollectionStop::from_error(&error) != CollectionStop::QueryLimit {
        return Err(error);
    }
    saved?;
    if completion.is_ok()
        || queries != final_queries
        || queries > request.budget.max_queries
        || shutdown != SourceShutdown::Quiescent
    {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_original(
    captured: &Captured,
    native: &crate::instance::Context,
    identities: &[IdleIdentity],
) -> Result<()> {
    if !(7..=8).contains(&captured.manifest.source_queries) {
        return Err(invalid_data());
    }
    captured.with_activity_frame(|frame| {
        let context = frame.context();
        let epoch = native.capture_epoch()?;
        if !frame.source().has_qualified_instance_activity()
            || context.target != CollectionTarget::Shared(native.identity.clone())
            || context.partition.native_epoch() != Some(epoch.as_str())
            || context.database_writable.is_some()
            || !frame.native().coverage.complete_scope
        {
            return Err(invalid_data());
        }
        for identity in identities {
            let mut rows = frame.session_rows(&identity.session);
            let row = rows.next().ok_or_else(invalid_data)?;
            let actor = row.observation;
            let Some(SessionNativeContext::Mssql(context)) =
                actor.session_context.as_deref().and_then(|context| context.native.as_deref())
            else {
                return Err(invalid_data());
            };
            let Some(MssqlIdleSessionConnection::Observed { connection_id, connected_without_offset }) =
                &context.idle_connection
            else {
                return Err(invalid_data());
            };
            if rows.next().is_some()
                || !actor.idle
                || actor.key.request.is_some()
                || actor.scope != ActivityScopeBasis::InstanceMember
                || connection_id != &identity.connection_id
                || connected_without_offset != &identity.connected_without_offset
                || row.reference.is_none()
            {
                return Err(invalid_data());
            }
        }
        let report = culprit_checks::session_census::explain_frame(frame)?;
        if report.recent_clients.coverage != culprit_core::RecentClientPopulationCoverage::CompleteOriginalCensus
            || report.recent_clients.groups.grouped_sessions < identities.len() as u32
        {
            return Err(invalid_data());
        }
        Ok(())
    })
}
fn save_original(directory: &Path, captured: &Captured, native: &crate::instance::Context) -> Result<()> {
    save_cost_receipt(
        directory,
        "instance-census-native-original",
        &json!({
            "source":captured.source()?,"manifest":captured.manifest,"records":captured.records,
            "observed_native_edition":native.engine_edition(),"observed_native_major":native.major,
            "native_cpu_io_cost_qualified":false,"production_admission":"unqualified",
            "same_original_verified_after_native_source_close":true,
        }),
    )
}
