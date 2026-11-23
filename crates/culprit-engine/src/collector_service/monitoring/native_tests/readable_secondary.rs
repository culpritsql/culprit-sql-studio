//! Real PG18 physical recovery, normal default enrollment, selected source and canonical Gold publication.
use super::*;
use culprit_postgres::native_test_support::ReadableSecondaryFixture;

const QUESTION: SourceReadQuestion = SourceReadQuestion::ReadableSecondaryRecovery;
#[tokio::test]
#[ignore = "requires the explicit ownedPG18 physical pair, ~3-minute default-threshold/cooldown case and private receipt directory"]
async fn real_physical_standby_reader_conflict_uses_normal_controller_and_gold_without_inventing_cancelled_sql() {
    Box::pin(ReadableSecondaryFixture::run("readable-secondary", |fixture| Box::pin(prove(fixture)))).await;
}
#[tokio::test]
#[ignore = "requires the explicit ownedPG18 physical pair; short actual endpoint check without adverse/cooldown waits"]
async fn real_physical_standby_endpoint_decodes_actual_union_columns_before_adverse_waits() {
    Box::pin(ReadableSecondaryFixture::run("readable-secondary-endpoint", |fixture| Box::pin(prove_endpoint(fixture))))
        .await;
}
async fn prove_endpoint(fixture: &mut ReadableSecondaryFixture) {
    Box::pin(fixture.prepare()).await;
    let directory = owned::automatic_directory("readable-secondary-endpoint").unwrap();
    let (config, workspace, plan) =
        configured_spec(&fixture.specification, "CULPRIT_PG_REPLICATION_PASSWORD", &directory);
    let inventory = plan.inventory.as_ref().unwrap();
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let mut outbox = Box::new(Outbox::create(&directory.join("outbox"), OutboxLimits::default()).unwrap());
    let backend = plan.backend(&workspace);
    let mut events = Vec::new();
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events)).await;
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].schedule, "postgres.activity");
    let capture = events[0].capture.as_ref().unwrap();
    let baseline =
        Box::new(outbox.diagnostic_input(&capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap());
    let original = baseline
        .rows
        .iter()
        .find(|row| {
            matches!(
                &row.row.record, TelemetryRecord::Activity(actor) if actor.key.session == fixture.reader_identity
            )
        })
        .unwrap();
    let TelemetryRecord::Activity(actor) = &original.row.record else { panic!() };
    let snapshot = baseline.source.observation.activity().unwrap();
    let focus = ActivityRequestFocus {
        parent: original.reference.clone(),
        target: baseline.source.target.clone(),
        partition: baseline.source.partition.clone(),
        database_writable: snapshot.database_writable,
        source: baseline.source.source.clone(),
        epoch: snapshot.epoch.clone().unwrap(),
        original_observed_at: snapshot.at.clone(),
        statement_identity: actor.statement_identity.clone().unwrap(),
        original: actor.as_ref().clone(),
    };
    assert!(focus.is_readable_secondary_recovery_focus());
    let (point, metrics, queries) = Box::pin(fixture.recovery_endpoint(&focus)).await;
    assert!(point.is_valid());
    assert_eq!(point.native_epoch().as_deref(), Some(focus.epoch.as_str()));
    assert_eq!(point.conflicts.identity.id, "pg_stat_database_conflicts");
    assert_eq!(point.replay.measurements.identity.id, "pg_recovery");
    assert_eq!(queries, 6, "the same endpoint decoder pays setup, both native bounds and one endpoint");
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    let cancellation_attempts = Box::pin(fixture.cancel_reader()).await;
    assert_eq!(cancellation_attempts, 1);
    // This direct source smoke is not an adverse incident, a controller answer or a conflict delta.
    owned::write_automatic_receipt(
        "readable-secondary-endpoint",
        "native-endpoint-columns.json",
        &serde_json::json!({
            "baseline_capture":capture,"original":focus.parent,"endpoint":point,
            "metrics":metrics,"source_queries_including_setup":queries,
            "adverse_wait_exercised":false,"controller_followup_exercised":false,
            "fixture_reader_cancellation_attempts":cancellation_attempts,"original_cancellation_join_and_rollback_confirmed":true,
            "native_cpu":null,"native_cost_measured":false,
        }),
    )
    .unwrap();
}
async fn prove(fixture: &mut ReadableSecondaryFixture) {
    let (timeout, cancelled) = Box::pin(fixture.verify_native_reader_retirement()).await;
    assert!(timeout.is_valid() && cancelled.is_valid());
    Box::pin(fixture.prepare()).await;
    Box::pin(fixture.wait_for_default_adverse_reader()).await;
    let directory = owned::automatic_directory("readable-secondary").unwrap();
    let (config, workspace, plan) =
        configured_spec(&fixture.specification, "CULPRIT_PG_REPLICATION_PASSWORD", &directory);
    let inventory = plan.inventory.as_ref().unwrap();
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let outbox_path = directory.join("outbox");
    let mut outbox = Box::new(Outbox::create(&outbox_path, OutboxLimits::default()).unwrap());
    let synthesis =
        Box::new(synthesis::NativeSynthesis::install(&mut outbox, &workspace, &fixture.cancellation).await.unwrap());
    let backend = plan.backend(&workspace);
    let started = tokio::time::Instant::now();
    let mut events = Vec::new();
    let oracle;
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events)).await;
        assert_eq!(events[0].schedule, "postgres.activity");
        // Start the genuine120s replay conflict immediately after the adverse original. The admitted focus
        // precedes stable Settings, but still pays the same60s resource cooldown before its native read.
        Box::pin(fixture.begin_replay_conflict()).await;
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events)).await;
        assert_eq!(events[1].schedule, "postgres.readable-secondary-recovery");
        assert!(
            started.elapsed() >= Duration::from_secs(u64::from(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS))
        );
        // Observe the same live relation edge before the next cooldown can reach the server's conflict
        // cancellation boundary. This is the existing single oracle read, not a synthetic edge or retry.
        oracle = Box::pin(fixture.relation_edge_observed()).await;
    }
    let baseline_capture = events[0].capture.as_ref().unwrap().clone();
    let focused_capture = events[1].capture.as_ref().unwrap().clone();
    let baseline = Box::new(
        outbox.diagnostic_input(&baseline_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
    );
    let selected = Box::new(
        outbox.diagnostic_input(&focused_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
    );
    let original=baseline.rows.iter().find(|row| matches!(&row.row.record,TelemetryRecord::Activity(actor) if actor.key.session == fixture.reader_identity)).unwrap();
    let TelemetryRecord::Activity(victim) = &original.row.record else { panic!() };
    assert!(victim.elapsed_ms.as_deref().unwrap().parse::<u64>().unwrap() >= 60_000);
    let point = selected
        .rows
        .iter()
        .find_map(|row| match &row.row.record {
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::ReadableSecondaryRecovery(point),
                ..
            }) => Some(point.as_ref()),
            _ => None,
        })
        .unwrap();
    assert_eq!(point.focus.parent, original.reference);
    assert_eq!(point.focus.original.key, victim.key);
    assert_eq!(point.context.database_writable, Some(false));
    assert!(point.context.has_qualified_local_owner());
    assert!(point.context.target.database().unwrap().incarnation.is_none());
    assert_eq!(point.binding, FocusedRequestBinding::Matched);
    assert_eq!(point.membership, FocusedRequestMembership::Observed);
    assert_eq!(point.request_end, BlockingRequestState::SameStatement);
    let snapshot = selected.source.observation.snapshot().unwrap();
    assert_eq!(snapshot.coverage_state().unwrap(), culprit_core::SourceState::Partial);
    let readers = point.native_blocking_readers();

    if oracle {
        assert_eq!(readers.len(), 1, "a real generation-qualified relation edge must survive the production decoder");
    }
    for reader in &readers {
        assert!(oracle);
        assert_eq!(reader.session, fixture.reader_identity);
        assert_eq!(reader.role, RecoveryReaderRole::GrantedRelationHolder);
    }
    let manifest = Box::new(outbox.verify(&focused_capture.observation_id).await.unwrap());
    let intent = manifest.investigation.as_ref().unwrap().intent_for_source(&selected.source).unwrap();
    let DiagnosticFocus::SourceRead { focus } = &intent.focus else { panic!() };
    assert_eq!(focus.question, QUESTION);
    assert!(matches!(&intent.qualification, RecipeExecutionAdmission::BuiltinSafety { .. }));
    assert!(intent.qualification.cost_evidence().is_none());
    assert_eq!(intent.budget.max_queries, 9);
    assert_eq!(manifest.source_queries, 9);
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead {
            focus: focus.clone(),
            execution: Box::new(SourceReadExecutionScope::from_context(QUESTION, &point.context).unwrap()),
        },
        budget: intent.budget.clone(),
    };
    // Qualify the strict8-query LIVE branch while the same reader still holds its original statement.
    // Settings' next60s cooldown can reach the real120s conflict deadline; an ended branch legitimately
    // needs less native work and cannot prove live-path budget refusal.
    let short_caller = Box::pin(fixture.short_caller_source(&request)).await;
    {
        // The existing journal restores both completed jobs and resource cooldown. Keep the same Outbox,
        // native ownership and journal; this is not a new enrollment or an admission-clock reset.
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events)).await;
        assert_eq!(events[2].schedule, "postgres.settings", "stable Settings still runs after the admitted focus");
        assert!(
            started.elapsed() >= Duration::from_secs(2 * u64::from(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS))
        );
    }

    assert_eq!(events.len(), 3);
    let mut normal_source_queries = 0_u32;
    for (event, ceiling) in events.iter().zip([6, 9, 6]) {
        let value = outbox.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
        assert_eq!(value.request.budget.max_queries, ceiling);
        normal_source_queries = normal_source_queries.checked_add(value.source_queries).unwrap();
        assert_eq!(value.status, CollectionStatus::Complete);
        let metrics = value.read_metrics.as_ref().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        assert_eq!(metrics.templates.iter().map(|row| row.completed_tasks).sum::<u32>(), value.source_queries);
        assert!(value.source_queries <= value.request.budget.max_queries);
    }
    assert!(normal_source_queries <= 21, "two genuine baselines plus the original recovery follow-up");
    let mut archive = archive::NativeArchive::create(&directory, &fixture.cancellation).await.unwrap();
    let mut revisions = Vec::new();
    for entry in archive::retained(&outbox, &fixture.cancellation).await.unwrap() {
        revisions.extend(Box::pin(archive.capture(&outbox, &entry, &fixture.cancellation)).await.unwrap().revisions);
    }
    let history = Box::new(history::for_episode(&revisions, &intent.episode_id, &original.reference).unwrap());
    let stage = history.latest.source_reads.get(QUESTION).unwrap();
    let SourceReadStageState::Answered { answer } = &stage.state else {
        panic!("actual controller must retain its independent typed answer")
    };
    assert_eq!(answer.result().capture, focused_capture);
    assert_eq!(history.latest.first, original.reference);
    let stored = Box::pin(synthesis.drain(&mut outbox, &mut archive, &fixture.cancellation)).await.unwrap();
    let incident =
        stored.iter().find(|incident| incident.revision_reference == *history.references.last().unwrap()).unwrap();
    verify_gold(incident, victim, &readers);
    // The source's own adjacent endpoints stay honest even when this capture happens to observe zero.
    let ReadableSecondaryRecoveryNative::Postgres { before: Some(before), after: Some(_) } = &point.native else {
        panic!()
    };
    Box::pin(fixture.await_native_conflict()).await;
    let (after, endpoint_metrics, endpoint_queries) = Box::pin(fixture.recovery_endpoint(&point.focus)).await;
    let changes = after.changes_since(before).unwrap();
    assert!(changes.iter().any(|change|matches!(change,RecoveryConflictChange::Observed{kind:PostgresRecoveryConflictKind::Lock,change,..} if change.parse::<u64>().is_ok_and(|count|count>0))),"same-reset native conflict deltas use two actual native points, not a fabricated source answer");
    let (ended, ended_metrics, ended_queries) = Box::pin(fixture.ended_source(&request)).await;
    assert_eq!(ended.membership, FocusedRequestMembership::NotObserved);
    assert!(ended.native_blocking_readers().is_empty());
    assert!(matches!(ended.native, ReadableSecondaryRecoveryNative::Postgres { before: None, after: None }));
    assert_eq!(ended.binding, FocusedRequestBinding::ParticipantChanged);
    Box::pin(fixture.retire_reader()).await;
    let (missing, missing_metrics, missing_queries) = Box::pin(fixture.ended_source(&request)).await;
    assert_eq!(missing.binding, FocusedRequestBinding::ParticipantMissing);
    assert_eq!(missing.membership, FocusedRequestMembership::NotObserved);
    assert!(missing.native_blocking_readers().is_empty());

    // No new canonical child answer is installed for this manual ended-identity qualification probe.
    assert_eq!(history.latest.source_reads.get(QUESTION), Some(stage));
    write_receipt(RecoveryReceipt {
        plan: &plan,
        events: &events,
        normal_source_queries,
        point,
        oracle,
        timeout: &timeout,
        cancelled: &cancelled,
        short_caller: &short_caller,
        missing_metrics: &missing_metrics,
        missing_queries,
        changes: &changes,
        endpoint_metrics: &endpoint_metrics,
        endpoint_queries,
        ended_metrics: &ended_metrics,
        ended_queries,
        incident,
    });
    drop(outbox);
    let mut reopened = Outbox::open(&outbox_path).unwrap();
    for event in &events {
        reopened.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
    }
}
fn verify_gold(incident: &DerivedIncident, victim: &ActivityObservation, readers: &[RecoveryReaderEvidence]) {
    assert!(incident.affected_queries.iter().any(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,statement}) if actor==&victim.key&&statement==&victim.statement_identity)));
    if readers.is_empty() {
        assert!(incident.culprit_queries.is_empty());
    } else {
        assert_eq!(incident.culprit_queries.len(), 1);
        let owner = &incident.culprit_queries[0];
        assert_eq!(owner.causal_role.as_ref().unwrap().kind, DerivedCausalRoleKind::NativeGrantedRelationLockHolder);
        assert!(!owner.sql.is_available());
    }
    assert!(!incident.candidate_queries.is_empty());
    assert!(incident.mechanism.is_some());
    assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
}
struct RecoveryReceipt<'a> {
    plan: &'a Enrollment,
    events: &'a [FleetCollectionEvent],
    normal_source_queries: u32,
    point: &'a ReadableSecondaryRecoveryPoint,
    oracle: bool,
    timeout: &'a ReadWorkMetrics,
    cancelled: &'a ReadWorkMetrics,
    short_caller: &'a ReadWorkMetrics,
    missing_metrics: &'a ReadWorkMetrics,
    missing_queries: u32,
    changes: &'a [RecoveryConflictChange],
    endpoint_metrics: &'a ReadWorkMetrics,
    endpoint_queries: u32,
    ended_metrics: &'a ReadWorkMetrics,
    ended_queries: u32,
    incident: &'a DerivedIncident,
}
fn write_receipt(receipt: RecoveryReceipt<'_>) {
    owned::write_automatic_receipt("readable-secondary","automatic-native-recovery.json",&serde_json::json!({
        "native_timeout_metrics":receipt.timeout,"native_cancelled_caller_metrics":receipt.cancelled,
        "strict_caller_eight_metrics":receipt.short_caller,"missing_original_metrics":receipt.missing_metrics,"missing_original_queries_including_setup":receipt.missing_queries,
        "normal_enrollment":receipt.plan.summary,"events":receipt.events,
        "normal_source_capture_count":receipt.events.len(),"normal_source_query_cap_including_setup":21,
        "normal_source_query_ceilings_including_setup":[6,9,6],"focused_event_index":1,
        "replay_conflict_started_after":"postgres.activity","relation_oracle_phase":"after-focused-before-settings",
        "strict_caller_eight_phase":"after-focused-before-settings","settings_restores_same_journal_and_outbox":true,
        "normal_source_queries_including_setup":receipt.normal_source_queries,
        "source_point":receipt.point,"independent_native_relation_edge":receipt.oracle,
        "same_reset_native_changes_across_actual_points":receipt.changes,"qualification_endpoint_metrics":receipt.endpoint_metrics,"qualification_endpoint_queries_including_setup":receipt.endpoint_queries,
        "ended_identity_metrics":receipt.ended_metrics,"ended_identity_queries_including_setup":receipt.ended_queries,"stored_incident":receipt.incident,
        "threshold_running_ms":60_000,"resource_cooldown_seconds":culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS,
        "source_query_ceiling_including_setup":9,"source_cpu":null,"source_cost_measured":false,
        "cross_point_delta_is_not_one_capture":true,"default300_second_recovery_not_exercised":true,
    })).unwrap();
}
#[test]
fn native_recovery_orchestration_has_bounded_concrete_future_storage() {
    fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) = size(|fixture: &'static mut ReadableSecondaryFixture| prove(fixture));
    assert!(future <= 32 * 1024, "actual recovery body future is {future} bytes");
    assert!(output <= 8);
    let (probe, output) = size(|fixture: &'static mut ReadableSecondaryFixture| prove_endpoint(fixture));
    assert!(probe <= 32 * 1024, "actual endpoint probe future is {probe} bytes");
    assert!(output <= 8);
    let (outer, output) =
        size(|()| ReadableSecondaryFixture::run("readable-secondary", |fixture| Box::pin(prove(fixture))));
    assert!(outer <= 32 * 1024, "actual fixture outer future is {outer} bytes");
    assert!(output <= 8);
}
