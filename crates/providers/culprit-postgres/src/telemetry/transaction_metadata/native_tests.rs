//! Genuine owned direct-API correctness, not a manufactured SLRU wait, controller admission or cost receipt.
use super::*;
use crate::native_test_support::{self as owned, MaintenanceFixture, TransactionMetadataRole as Role, case};
use culprit_read::cancellation::CollectionCancellation;
use std::future::Future;

const SOURCE_PHASES: u32 = 4;
const SOURCE_QUERIES_PER_PHASE: u32 = 8;
const MAX_SOURCE_QUERIES: u32 = SOURCE_PHASES * SOURCE_QUERIES_PER_PHASE;
const MAX_RECEIPT_BYTES: usize = 1024 * 1024;

/// Only fixture cancellation admission is added; the borrowed production reader still owns all wire work.
struct Admitted<'a, R> {
    reader: &'a mut R,
    cancellation: &'a CollectionCancellation,
}
#[async_trait::async_trait]
impl<R: QueryRead> QueryRead for Admitted<'_, R> {
    fn connection_generation(&self) -> Option<u64> {
        self.reader.connection_generation()
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        case::admitted(self.cancellation, self.reader.query(sql, limit)).await
    }
}
struct Phase {
    name: &'static str,
    focus: NativeActivityIdentity,
    partial_focus: NativeActivityIdentity,
}
struct Observed {
    context: NativeReadContext,
    full: native_activity::transaction_metadata::Reading,
    partial: native_activity::transaction_metadata::Reading,
    retainers: native::transaction_metadata::Retainers,
}
struct Captured {
    observed: Box<Observed>,
    queries: u32,
    receipt: serde_json::Value,
}
/// Native leaves, the original SameConnection wrapper and original completion witness are reused unchanged.
async fn observe_phase(
    reader: &mut ReadSession,
    specification: &culprit_core::ConnectionSpec,
    phase: &Phase,
    cancellation: &CollectionCancellation,
) -> Result<Box<Observed>> {
    let mut admitted = Admitted { reader, cancellation };
    let before =
        Box::new(Box::pin(metadata::activity(&mut admitted, &specification.host, specification.port.unwrap())).await?);
    if before.version < 160_000 || !before.full_visibility {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    let generation = admitted.connection_generation().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    let mut held = SameConnection { reader: &mut admitted, generation };
    let owner = Box::new(
        Box::pin(Context::observe(&mut held, &before))
            .await?
            .ok_or_else(|| CollectionStop::SourceUnavailable.error())?,
    );
    let full = Box::pin(native_activity::transaction_metadata::read(
        &mut held,
        &before,
        phase.focus.id.parse().map_err(|_| native::invalid())?,
        MAX_TRANSACTION_METADATA_BACKENDS,
    ))
    .await?;
    let partial = Box::pin(native_activity::transaction_metadata::read(
        &mut held,
        &before,
        phase.partial_focus.id.parse().map_err(|_| native::invalid())?,
        1,
    ))
    .await?;
    let retainers = Box::pin(native::transaction_metadata::read_retainers(
        &mut held,
        before.version,
        before.standby,
        MAX_TRANSACTION_METADATA_RETAINERS,
    ))
    .await?;
    let completed = Box::pin(local_context::complete(
        &mut held,
        &specification.host,
        specification.port.unwrap(),
        &before,
        Some(&owner),
    ))
    .await?;
    if !completed.full_visibility
        || !completed.context.has_qualified_local_owner()
        || completed.context.database_writable.is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(Box::new(Observed { context: completed.context, full, partial, retainers }))
}
async fn capture_phase(fixture: &mut MaintenanceFixture, phase: &Phase) -> Box<Captured> {
    fixture.begin_transaction_probe();
    let budget = bounded_budget(&CollectionBudget::upper_limits());
    let mut reader = Box::new(ReadSession::new(Connector::new(&fixture.specification).unwrap(), budget.clone()));
    let started = std::time::Instant::now();
    let result = Box::pin(observe_phase(&mut reader, &fixture.specification, phase, &fixture.cancellation)).await;
    // The original native result survives close; no assertion or next fixture command runs before retirement.
    let shutdown = reader.close().await;
    let queries = reader.queries();
    let metrics = reader.metrics();
    let observed = result.expect("owned direct transaction metadata read must complete");
    assert_eq!(shutdown, SourceShutdown::Quiescent);
    let metrics = metrics.expect("actual native reader work is required");
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!(metrics.active_tasks, 0);
    assert_eq!(metrics.untracked_tasks, 0);
    assert_eq!(metrics.templates.iter().map(|template| template.completed_tasks).sum::<u32>(), queries);
    assert_eq!(queries, SOURCE_QUERIES_PER_PHASE, "one setup and seven actual native read dispatches");
    assert!(queries <= budget.max_queries);
    fixture.completed_transaction_probe(shutdown);
    let receipt = serde_json::json!({
        "phase":phase.name,"native_focus":phase.focus,"native_partial_focus":phase.partial_focus,
        "native_execution_context":observed.context,
        "cache_rows":observed.full.caches,"native_cache_rows_including_lookahead":observed.full.native_rows,
        "cache_limited":observed.full.limited,"shared_slru_points":observed.full.slrus,
        "partial_cache_rows":observed.partial.caches,"partial_native_rows_including_lookahead":observed.partial.native_rows,
        "partial_cache_limited":observed.partial.limited,"native_retention":observed.retainers.snapshot,
        "retainer_limited":observed.retainers.limited,"source_queries_including_setup":queries,
        "source_query_cap":budget.max_queries,"statement_timeout_ms":budget.statement_timeout_ms,"total_timeout_ms":budget.total_timeout_ms,
        "elapsed_ms":started.elapsed().as_millis().to_string(),"native_work":metrics,"source_shutdown":"quiescent",
        "genuine_slru_wait_proved":false,"source_read_incident_admission_proved":false,"native_cost_qualified":false,
    });
    assert!(serde_json::to_vec(&receipt).unwrap().len() <= MAX_RECEIPT_BYTES);
    owned::write_automatic_receipt("transaction-metadata", &format!("{}.json", phase.name), &receipt).unwrap();
    Box::new(Captured { observed, queries, receipt })
}
fn actor<'a>(
    reading: &'a native_activity::transaction_metadata::Reading,
    identity: &NativeActivityIdentity,
) -> &'a PostgresSubtransactionCache {
    reading
        .caches
        .iter()
        .find(|cache| cache.actor.key.session == *identity)
        .expect("exact native backend generation must be present")
}
fn phase(fixture: &MaintenanceFixture, name: &'static str, focus: Role, partial: Role) -> Phase {
    Phase {
        name,
        focus: fixture.transaction_identity(focus).clone(),
        partial_focus: fixture.transaction_identity(partial).clone(),
    }
}
fn assert_native_clocks(captured: &Captured) {
    let point = &captured.observed;
    let bounds = ObservationBounds::new(&point.context.observed_from, &point.context.observed_until).unwrap();
    assert!(bounds.contains(&point.full.frame.at));
    assert!(bounds.contains(&point.partial.frame.at));
    assert_eq!(point.full.slrus.len(), 3);
    for slru in point.full.slrus.iter().chain(&point.partial.slrus) {
        assert!(slru.is_valid());
        assert!(bounds.contains(&slru.observed_at));
        assert!(
            slru.stats_reset.as_deref().is_none_or(|reset| canonical_time(reset).is_ok()),
            "native reset NULL remains unavailable; the fixture never resets cluster statistics"
        );
    }
    assert!(point.retainers.snapshot.holders.iter().all(|holder| bounds.contains(&holder.observed_at)));
}
fn assert_independent_retainer(captured: &Captured, identity: &NativeActivityIdentity) {
    let matching = captured
        .observed
        .retainers
        .snapshot
        .holders
        .iter()
        .filter(|holder| holder.session.as_ref() == Some(identity))
        .collect::<Vec<_>>();
    assert!(!matching.is_empty(), "independent native repeatable-read actor must expose its exact xmin generation");
    assert!(matching.iter().any(|holder| holder.source == "pg_stat_activity"
        && holder.horizons.iter().any(|horizon| horizon.native_name == "backend_xmin")));
}
async fn verify_native(fixture: &mut MaintenanceFixture) {
    Box::pin(fixture.prepare_transaction_metadata()).await;
    let writer = fixture.transaction_identity(Role::Writer).clone();
    let retainer = fixture.transaction_identity(Role::Retainer).clone();
    let other = fixture.transaction_identity(Role::OtherOverflow).clone();
    let native_phase = phase(fixture, "native-overflow", Role::Writer, Role::Writer);
    let initial = Box::pin(capture_phase(fixture, &native_phase)).await;
    let first = actor(&initial.observed.full, &writer);
    assert_eq!(first.overflowed, Some(true), "actual native flag, not SAVEPOINT/lock counts, owns the positive");
    assert!(first.cached_subxids.is_some());
    assert_eq!(first.actor.key.session, writer, "native backend-index mapping preserves the actual PID generation");
    assert_eq!(actor(&initial.observed.full, &other).overflowed, Some(true));
    assert!(!initial.observed.full.limited);
    assert!(initial.observed.partial.limited);
    assert_eq!(initial.observed.partial.caches.len(), 1);
    assert!(initial.observed.partial.native_rows > 1);
    assert_eq!(initial.observed.partial.caches[0].actor.key.session, writer);
    assert_independent_retainer(&initial, &retainer);
    assert_native_clocks(&initial);
    let original_backend_id = first.backend_id;
    let original_transaction = first.transaction_started_at.clone();
    let original_bytes = serde_json::to_vec(&initial.receipt).unwrap();
    Box::pin(fixture.rollback_writing_children()).await;
    let native_phase = phase(fixture, "native-rolled-back-children", Role::Writer, Role::Writer);
    let reduced = Box::pin(capture_phase(fixture, &native_phase)).await;
    let current = actor(&reduced.observed.full, &writer);
    assert_eq!(current.backend_id, original_backend_id);
    assert_eq!(current.transaction_started_at, original_transaction);
    assert_eq!(current.overflowed, Some(true));
    assert!(current.cached_subxids.is_some_and(|count| count < 64));
    assert_independent_retainer(&reduced, &retainer);
    assert_native_clocks(&reduced);
    Box::pin(fixture.rollback_writer_transaction()).await;
    let native_phase = phase(fixture, "native-ended-transaction", Role::Writer, Role::Writer);
    let ended = Box::pin(capture_phase(fixture, &native_phase)).await;
    let current = actor(&ended.observed.full, &writer);
    assert_eq!(current.overflowed, Some(false));
    assert_eq!(current.cached_subxids, Some(0));
    assert!(current.transaction_started_at.is_none());
    assert!(current.actor.key.transaction.is_none());
    assert!(current.actor.statement_identity.is_none());
    assert!(!current.actor.statement.is_available());
    assert_independent_retainer(&ended, &retainer);
    assert_native_clocks(&ended);
    Box::pin(fixture.replace_transaction_writer()).await;
    let replacement = fixture.transaction_identity(Role::Replacement).clone();
    assert_ne!(replacement, writer);
    let replaced = Box::pin(capture_phase(
        fixture,
        &Phase { name: "native-ended-backend", focus: writer.clone(), partial_focus: replacement.clone() },
    ))
    .await;
    assert!(replaced.observed.full.caches.iter().all(|cache| cache.actor.key.session != writer));
    let replacement_point = actor(&replaced.observed.partial, &replacement);
    assert_eq!(replacement_point.overflowed, Some(false));
    assert_eq!(replacement_point.cached_subxids, Some(0));
    assert!(replacement_point.transaction_started_at.is_none());
    assert!(replaced.observed.partial.limited, "a current overflow candidate must not be hidden by a one-row prefix");
    assert_native_clocks(&replaced);
    let total_queries = initial.queries + reduced.queries + ended.queries + replaced.queries;
    assert_eq!(total_queries, MAX_SOURCE_QUERIES);
    assert!(
        Box::pin(fixture.retire_transaction_metadata()).await,
        "awaited original actors and native absence must both settle"
    );
    assert_eq!(
        serde_json::to_vec(&initial.receipt).unwrap(),
        original_bytes,
        "later native state cannot repair an earlier read"
    );
    owned::write_automatic_receipt("transaction-metadata","direct-native-reader.json",&serde_json::json!({
        "source_phases":SOURCE_PHASES,"source_queries_including_each_setup":total_queries,"source_query_cap":MAX_SOURCE_QUERIES,
        "writer":writer,"replacement":replacement,"original_backend_id":original_backend_id,
        "replacement_backend_id":replacement_point.backend_id,"native_backend_slot_reused":replacement_point.backend_id==original_backend_id,
        "independent_xmin_retainer":retainer,"fixture":fixture.transaction_metadata_receipt(),
        "owned_reader_points_retained_in_private_receipts":true,"actual_backend_pid_reuse_required":false,"global_stats_reset_performed":false,
        "genuine_slru_wait_proved":false,"cause_proved":false,"source_read_incident_admission_proved":false,"cost_qualified":false,
    })).unwrap();
}
fn body(fixture: &mut MaintenanceFixture) -> futures_util::future::LocalBoxFuture<'_, ()> {
    Box::pin(verify_native(fixture))
}
#[tokio::test]
#[ignore = "requires the explicit nonce-owned PG16+ native fixture and private receipt directory"]
async fn native_direct_backend_overflow_reduced_cache_retainer_partial_and_ended_generation_are_observed_without_a_fake_slru_wait()
 {
    Box::pin(MaintenanceFixture::run_transaction_metadata(body)).await;
}
#[test]
fn actual_underlying_owned_native_futures_and_construction_are_bounded_without_polling_a_database() {
    fn sizes<A, F: Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (storage, output) = sizes(|fixture: &'static mut MaintenanceFixture| verify_native(fixture));
    assert!(storage <= 4096, "actual owned fixture body future: {storage}");
    assert!(output <= 128, "body output: {output}");
    let (storage, output) = sizes(
        |(reader, specification, phase, cancellation): (
            &'static mut ReadSession,
            &'static culprit_core::ConnectionSpec,
            &'static Phase,
            &'static CollectionCancellation,
        )| observe_phase(reader, specification, phase, cancellation),
    );
    assert!(storage <= 4096, "actual direct native phase future: {storage}");
    assert!(output <= 128, "native phase output: {output}");
    let (storage, output) =
        sizes(|(fixture, phase): (&'static mut MaintenanceFixture, &'static Phase)| capture_phase(fixture, phase));
    assert!(storage <= 4096, "actual settled capture phase future: {storage}");
    assert!(output <= 128, "capture output: {output}");
    let mut fixture = MaintenanceFixture::unconnected_transaction_future_fixture();
    let future = verify_native(&mut fixture);
    assert!(std::mem::size_of_val(&future) <= 4096);
    drop(future);
    let receipt = fixture.transaction_metadata_receipt();
    assert_eq!(
        receipt["fixture_dispatch_charges_including_ownership"], 2,
        "no actual admission is fabricated by construction"
    );
    assert_eq!(receipt["actor_open_attempts"], 0);
    assert_eq!(receipt["admin_dispatches"], 0);
}
