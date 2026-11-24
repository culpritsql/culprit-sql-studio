//! Owned disposable PostgreSQL 18 comparable intervention: unbatched per-row commits vs single-transaction
//! batching over identical rows -> real `PostgresProvider` shared `Operational` captures -> canonical reopen
//! -> `commit_pipeline` & Gold `CulpritIncidentsV1`, proving materially reduced shared WAL sync work without
//! charging server-wide WAL counters to a selected query.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisCapture, SynthesisInput, SynthesisRequest},
};
use culprit_core::{
    CommitPipelineCounterKind, CommitPipelineIntervalMechanism, CommitPipelineIntervalScope, CommitPipelineStage,
    MeasurementScope, Provider, limits::OperationGuard,
};
use culprit_postgres::{
    PostgresProvider,
    native_test_support::{self as owned, COMMIT_INTERVENTION_ROWS, CommitPipelineFixture},
};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;
use std::time::Duration;

struct Factory(PostgresProvider);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.telemetry_source(request)
    }
}

#[tokio::test]
#[ignore = "requires explicit nonce-owned PostgreSQL 18 fixture and private native safety receipt directory"]
async fn owned_batched_commits_materially_reduce_same_reset_shared_wal_sync_work_through_canonical_gold() {
    let mut fixture = Box::pin(CommitPipelineFixture::open()).await;
    let spec = fixture.specification.clone();
    let cancellation = CollectionCancellation::default();
    let provider = PostgresProvider::new(&spec).unwrap();
    let discovery =
        ResourceDiscoveryRequest { kind: SharedResourceKind::Instance, budget: CollectionBudget::default() };
    let resources = provider.telemetry_resources(&discovery).await.unwrap();
    assert_eq!(resources.len(), 1);
    let instance = resources[0].identity.clone();

    let mut captures = OwnedCaptures::new().unwrap();
    let probe_req = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Activity,
        budget: CollectionBudget {
            max_queries: 10,
            statement_timeout_ms: 2_000,
            total_timeout_ms: 5_000,
            ..CollectionBudget::default()
        },
    };
    let activity_probe = Box::pin(capture_shared(&spec, &mut captures, probe_req, &cancellation)).await;
    let probe_source = activity_probe.source().unwrap().clone();
    let shared_req = || CollectionRequest {
        subject: CollectionSubject::Shared { resource: instance.clone() },
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational },
        budget: CollectionBudget {
            max_queries: 10,
            statement_timeout_ms: 2_000,
            total_timeout_ms: 5_000,
            ..CollectionBudget::default()
        },
    };

    // Arm A: unbatched autocommit INSERTs (COMMIT_INTERVENTION_ROWS separate commits with synchronous_commit = on).
    let arm_a_before = Box::pin(capture_shared(&spec, &mut captures, shared_req(), &cancellation)).await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let arm_a_at = fixture.run_unbatched_arm().await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let arm_a_after = Box::pin(capture_shared(&spec, &mut captures, shared_req(), &cancellation)).await;

    // Arm B (comparable batching intervention): identical COMMIT_INTERVENTION_ROWS rows in a single transaction commit.
    let arm_b_before = Box::pin(capture_shared(&spec, &mut captures, shared_req(), &cancellation)).await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let arm_b_at = fixture.run_batched_arm().await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let arm_b_after = Box::pin(capture_shared(&spec, &mut captures, shared_req(), &cancellation)).await;

    fixture.close().await;

    for cap in [&activity_probe, &arm_a_before, &arm_a_after, &arm_b_before, &arm_b_after] {
        Box::pin(captures.verify_retained(cap)).await.unwrap();
    }

    let reopened = Box::pin(reopen_all(&captures, [&arm_a_before, &arm_a_after, &arm_b_before, &arm_b_after])).await;
    let reopened_refs = reopened.captures().collect::<Vec<_>>();
    let [r_a_before, r_a_after, r_b_before, r_b_after] = reopened_refs.as_slice() else { panic!() };

    let (report_a, gold_a) = Box::pin(evaluate_arm(&probe_source, &arm_a_at, r_a_before, r_a_after, 998_100)).await;
    let (report_b, gold_b) = Box::pin(evaluate_arm(&probe_source, &arm_b_at, r_b_before, r_b_after, 998_120)).await;

    assert_eq!(report_a.stage, Some(CommitPipelineStage::LocalLogSync));
    assert_eq!(report_b.stage, Some(CommitPipelineStage::LocalLogSync));

    let sync_ops = |report: &culprit_core::CommitPipelineReport| -> u64 {
        report
            .intervals
            .iter()
            .filter(|iv| iv.native_source == "pg_stat_io")
            .flat_map(|iv| &iv.values)
            .filter(|v| v.kind == CommitPipelineCounterKind::SyncOperations)
            .filter_map(|v| v.value.integer())
            .sum()
    };
    let wal_records = |report: &culprit_core::CommitPipelineReport| -> u64 {
        report
            .intervals
            .iter()
            .filter(|iv| iv.native_source == "pg_stat_wal")
            .flat_map(|iv| &iv.values)
            .filter(|v| v.kind == CommitPipelineCounterKind::LogRecords)
            .filter_map(|v| v.value.integer())
            .sum()
    };

    let unbatched_syncs = sync_ops(&report_a);
    let batched_syncs = sync_ops(&report_b);
    let unbatched_records = wal_records(&report_a);
    let batched_records = wal_records(&report_b);

    assert!(
        unbatched_syncs >= (COMMIT_INTERVENTION_ROWS as u64 / 2),
        "unbatched autocommits must record substantial WAL syncs: {unbatched_syncs}; intervals={:?}; gaps={:?}",
        report_a.intervals,
        report_a.interval_gaps,
    );
    assert!(
        batched_syncs * 5 < unbatched_syncs,
        "single-transaction batching must materially reduce WAL syncs ({batched_syncs} vs {unbatched_syncs})"
    );
    assert!(unbatched_records > batched_records, "autocommit XLOG commit records exceed single-commit records");

    for (label, report, gold) in [("unbatched", &report_a, &gold_a), ("batched", &report_b, &gold_b)] {
        assert!(
            report.intervals.iter().all(|iv| iv.measurement_scope == MeasurementScope::Server
                && iv.scope_basis == CommitPipelineIntervalScope::SharedServerIndependentOfDatabaseRole),
            "{label}"
        );
        let [inc] = gold.incidents.as_slice() else { panic!("{label}") };
        assert_eq!(inc.affected_queries.len(), 1, "{label}");
        assert!(inc.culprit_queries.is_empty() && inc.candidate_queries.is_empty(), "{label}");
        assert_eq!(inc.attribution_status, DerivedAttributionStatus::Unestablished, "{label}");
        assert!(inc.entry.reasons.contains(&t!("view.commit_pipeline.interval.limit.shared")), "{label}");
    }
    assert!(
        report_a.intervals.iter().any(|iv| iv.mechanisms.contains(&CommitPipelineIntervalMechanism::LocalSyncWork))
    );
    assert!(gold_a.incidents[0].developer_action.contains(&t!("view.commit_pipeline.interval.action.sync")));

    owned::write_automatic_receipt(
        "commit-pipeline",
        "comparable-batching-intervention.json",
        &json!({
            "intervention_rows_per_arm": COMMIT_INTERVENTION_ROWS,
            "unbatched_wal_sync_operations": unbatched_syncs,
            "batched_wal_sync_operations": batched_syncs,
            "unbatched_wal_records": unbatched_records,
            "batched_wal_records": batched_records,
            "source_queries": [
                arm_a_before.manifest.source_queries,
                arm_a_after.manifest.source_queries,
                arm_b_before.manifest.source_queries,
                arm_b_after.manifest.source_queries,
            ],
            "shared_wal_attributed_to_selected_query": false,
            "native_cost_qualified": false,
        }),
    )
    .unwrap();
}

async fn capture_shared(
    spec: &culprit_core::ConnectionSpec,
    captures: &mut OwnedCaptures,
    request: CollectionRequest,
    cancellation: &CollectionCancellation,
) -> Box<Captured> {
    let spec = spec.clone();
    let captured = Box::new(
        Box::pin(captures.capture("owned-commit-pipeline", request, cancellation, move |_| {
            Factory(PostgresProvider::new(&spec).unwrap())
        }))
        .await
        .unwrap(),
    );
    assert_eq!(captured.manifest.status, CollectionStatus::Complete);
    let metrics = captured.manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    captured
}

async fn reopen_all(captures: &OwnedCaptures, originals: [&Captured; 4]) -> SynthesisInput {
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout).unwrap();
    let request = SynthesisRequest {
        primary: originals[0].capture_ref().unwrap(),
        context: originals[1..].iter().map(|c| c.capture_ref().unwrap()).collect(),
        budget,
    };
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await.unwrap()
}

async fn evaluate_arm(
    probe_source: &CollectionDescription,
    at: &str,
    before: &SynthesisCapture,
    after: &SynthesisCapture,
    seq: u64,
) -> (culprit_core::CommitPipelineReport, DerivedDiagnosis) {
    let before_orig = before.verified_original().unwrap();
    let after_orig = after.verified_original().unwrap();
    let epoch = probe_source.observation.activity().unwrap().epoch.clone().unwrap();
    let pid = "4242";
    let backend_gen = "2026-01-01T00:00:01Z";
    let query_gen = "2026-01-01T00:00:02Z";
    let mut actor = test_support::activity_observation(DefinitionPayload::sql(
        "INSERT INTO events(id, payload) VALUES (1, 'redacted')",
        Engine::Postgres,
    ));
    actor.key = ActivityKey {
        session: NativeActivityIdentity { id: pid.into(), generation: backend_gen.into() },
        request: Some(NativeActivityIdentity { id: "statement".into(), generation: query_gen.into() }),
        transaction: None,
    };
    actor.statement_identity =
        Some(culprit_core::engines::postgres_activity_statement_identity(pid, backend_gen, query_gen));
    actor.state = "active".into();
    actor.scope = ActivityScopeBasis::SessionDatabase;
    actor.database = probe_source.target.database().map(|db| db.database.clone());
    actor.command = None;
    actor.blocking_status = ActivityBlockingStatus::Unknown;
    actor.waiting_on_lock = None;
    actor.elapsed_ms = Some("65000".into());
    actor.session_counters = ActivityCounters::default();
    actor.request_counters = Some(ActivityCounters::default());
    actor.wait = Some(ActivityWait {
        class: None,
        native_type: Some("IO: WALSync".into()),
        elapsed_ms: None,
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    let primary_source = CollectionDescription {
        target: probe_source.target.clone(),
        partition: probe_source.partition.clone(),
        source: probe_source.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some(epoch),
            database_writable: probe_source.observation.activity().unwrap().database_writable,
            observed_from: at.into(),
            observed_until: at.into(),
            at: at.into(),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ClientBackends,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(probe_source.source.clone(), 1),
        }),
    };
    let primary = retained(primary_source, vec![TelemetryRecord::Activity(Box::new(actor))], seq).await;
    let orig = primary.verified_original().unwrap();
    let first_ref = orig.reference(0).unwrap();
    let TelemetryRecord::Activity(obs) = &orig.records()[0].row().record else { panic!() };
    let update = observe_diagnostic_activity(
        orig.source(),
        orig.original(),
        &[DiagnosticActivityRow { reference: &first_ref, observation: obs }],
        None,
        &ActivityPolicy::default(),
        at,
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|ep| matches!(ep.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. }))
        .unwrap();
    let rev = history(episode, seq + 1).await;
    let rev_orig = rev.verified_original().unwrap();
    let borrowed: Vec<&dyn OriginalProjection> = vec![&rev_orig, &orig, &before_orig, &after_orig];
    let inputs = super::super::commit_pipeline::inputs(&borrowed[1..], orig.original()).unwrap();
    let report = culprit_checks::commit_pipeline::explain(&inputs, &first_ref).unwrap();
    let gold = ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-commit-pipeline-1")
        .unwrap()
        .analyze_originals(
            &borrowed,
            "2026-11-24T12:00:00Z",
            &DerivedDisplayContext { connection_alias: "owned-pg18".into(), environment: None, database_name: None },
            vec![],
        )
        .unwrap();
    gold.validate().unwrap();
    (report, gold)
}
