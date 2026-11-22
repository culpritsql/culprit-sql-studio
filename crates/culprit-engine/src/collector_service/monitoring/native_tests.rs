//! Real source-owned admission through normal enrollment, the fleet controller and the sealed Outbox.
use super::*;
use crate::{NewConnection, secrets::Memory};
use culprit_collector::{fleet::FleetJournal, outbox::DiagnosticInput};
use culprit_core::{AuthKind, Engine, Environment};
use culprit_postgres::native_test_support::{self as owned, AutomaticCase, MaintenanceFixture, case};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

const NATIVE_CONNECTION: &str = "owned-builtin-pg";
const SOURCE_CAPTURE_CAP: usize = 3;
mod archive;
mod history;
mod synthesis;

#[tokio::test]
#[ignore = "requires the nonce-owned PostgreSQL18 fixture and explicit private safety receipt directory"]
async fn normal_builtin_pg_enrollment_automatically_explains_one_native_lock_incident_without_a_cost_receipt() {
    MaintenanceFixture::run_automatic(AutomaticCase::Focused, |fixture| Box::pin(prove(fixture, false))).await;
}

#[tokio::test]
#[ignore = "separate ~5-minute real300s-cadence recovery case; never credited from the shorter focus case"]
async fn normal_builtin_pg_incident_resolves_at_the_real_default_cadence_and_retains_the_original_answer() {
    MaintenanceFixture::run_automatic(AutomaticCase::DefaultCadenceRecovery, |fixture| Box::pin(prove(fixture, true)))
        .await;
}

fn configured(fixture: &MaintenanceFixture, directory: &Path) -> (CollectorConfig, Workspace, Enrollment) {
    let mut config = super::super::tests::config(directory);
    config.source_credentials.clear();
    config.monitoring = MonitoringPolicy::Standard { budget: CollectionBudget::default() };
    let target = &mut config.inventory.targets[0];
    target.id = NATIVE_CONNECTION.into();
    target.connection = NATIVE_CONNECTION.into();
    target.admission_group = "owned-native-instance".into();
    target.schedules.clear();
    let workspace = Workspace::open_in(&config.workspace).with_secrets(Arc::new(Memory::default()));
    let source = &fixture.specification;
    workspace
        .add_connection(NewConnection {
            name: NATIVE_CONNECTION.into(),
            input: format!(
                "postgres://postgres@127.0.0.1:{}/{}?sslmode=disable",
                source.port.unwrap(),
                source.database.as_ref().unwrap()
            ),
            engine: Some(Engine::Postgres),
            environment: Environment::Local,
            auth: Some(AuthKind::Password),
            password_env: Some("CULPRIT_PREVIEW_PG_PASSWORD".into()),
            ..Default::default()
        })
        .unwrap();
    let plan = Enrollment::resolve(&workspace, &config).unwrap();
    assert_eq!(plan.summary.automatic.len(), 1);
    assert_eq!(plan.summary.automatic[0].recipe_id, "postgres.activity");
    assert!(!plan.summary.gaps.iter().any(|gap| gap.recipe_id.as_deref() == Some("postgres.activity")));
    let inventory = plan.inventory.as_ref().unwrap();
    let schedule = &inventory.targets[0].schedules[0];
    assert_eq!(schedule.every_seconds, 300, "native acceptance uses the actual unchanged standard cadence");
    assert_eq!(schedule.budget.max_queries, 6);
    let ScheduledCollection::Activity { policy } = &schedule.collection else { panic!() };
    assert_eq!(policy.blocked_after_ms, 30_000, "native acceptance pays the actual standard attention threshold");
    assert_eq!(policy.running_after_ms, 60_000);
    let backend = plan.backend(&workspace);
    assert_eq!(
        backend
            .investigation_budget(NATIVE_CONNECTION, &CollectionSubject::Database, TelemetryCollection::Activity)
            .unwrap(),
        Some(CollectionBudget::default())
    );
    assert!(
        plan.profiles.values().all(|profile| profile.qualification().is_none() && profile.builtin_policy().is_some())
    );
    (config, workspace, plan)
}

async fn step(
    runner: &mut FleetRunner<'_>,
    cancellation: &CollectionCancellation,
    events: &mut Vec<FleetCollectionEvent>,
) {
    case::require_active(cancellation).unwrap();
    let result = runner
        .run(cancellation, Some(1), |event| {
            assert!(events.len() < SOURCE_CAPTURE_CAP);
            events.push(event.clone());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!((result.attempts, result.complete, result.incomplete), (1, 1, 0));
}

async fn prove(fixture: &mut MaintenanceFixture, recover_at_default_cadence: bool) {
    fixture.prepare().await;
    fixture.wait_for_writer().await;
    // The trigger is the engine's exact native lock-wait point, not an inferred/thresholded wait duration.
    // Default attention/cadence remain unchanged; the existing controller independently admits LockWait.
    case::require_active(&fixture.cancellation).unwrap();
    let case_name = if recover_at_default_cadence { "builtin-recovery" } else { "builtin-safety" };
    let directory = owned::automatic_directory(case_name).unwrap();
    let (config, workspace, plan) = configured(fixture, &directory);
    let inventory = plan.inventory.as_ref().unwrap();
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let outbox_path = directory.join("outbox");
    let mut outbox = Outbox::create(&outbox_path, OutboxLimits::default()).unwrap();
    // Normal default composition is installed before the fleet can commit native/diagnostic originals.
    let synthesis =
        Box::new(synthesis::NativeSynthesis::install(&mut outbox, &workspace, &fixture.cancellation).await.unwrap());
    let backend = plan.backend(&workspace);
    let started = tokio::time::Instant::now();
    let mut events = Vec::new();
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        step(&mut runner, &fixture.cancellation, &mut events).await;
        assert_eq!(events[0].schedule, "postgres.activity");
        step(&mut runner, &fixture.cancellation, &mut events).await;
        assert_eq!(
            events[1].schedule, "postgres.blocking",
            "the controller selects the original lock question automatically"
        );
        assert!(
            started.elapsed() >= Duration::from_secs(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS.into()),
            "the real scheduler must pay its resource cooldown, without advancing control time"
        );
        // Same original fixture transactions/CREATE owner are settled, never replaced to manufacture recovery.
        fixture.release_and_complete().await;
        if recover_at_default_cadence {
            step(&mut runner, &fixture.cancellation, &mut events).await;
            assert_eq!(events[2].schedule, "postgres.activity");
            assert!(started.elapsed() >= Duration::from_secs(300));
        }
    }
    let baseline_capture = events[0].capture.as_ref().unwrap();
    let focused_capture = events[1].capture.as_ref().unwrap();
    let baseline = Box::new(
        outbox.diagnostic_input(&baseline_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
    );
    let current = Box::new(
        outbox.diagnostic_input(&focused_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap(),
    );
    verify_original(fixture, &baseline, &current);
    let focused_manifest = Box::new(outbox.verify(&focused_capture.observation_id).await.unwrap());
    let lineage = focused_manifest.investigation.as_ref().unwrap();
    let intent = lineage.intent_for_source(&current.source).unwrap();
    assert!(matches!(intent.qualification, RecipeExecutionAdmission::BuiltinSafety { .. }));
    assert!(intent.qualification.cost_evidence().is_none());
    assert_eq!(intent.budget.max_queries, 25);
    let victim=baseline.rows.iter().find(|row|matches!(&row.row.record,TelemetryRecord::Activity(activity) if activity.key.session.id==fixture.builder_pid.to_string())).unwrap();
    let first = victim.reference.clone();
    let TelemetryRecord::Activity(victim) = &victim.row.record else { panic!() };
    let victim = victim.clone();
    let owner = baseline
        .rows
        .iter()
        .find_map(|row| match &row.row.record {
            TelemetryRecord::Activity(activity) if activity.key.session.id == fixture.holder_pid.to_string() => {
                Some(activity.key.session.clone())
            }
            _ => None,
        })
        .unwrap();
    let mut source_queries = 0;
    let mut originals = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let capture = event.capture.as_ref().unwrap();
        let manifest = outbox.verify(&capture.observation_id).await.unwrap();
        assert_eq!(manifest.status, CollectionStatus::Complete);
        let metrics = manifest.read_metrics.as_ref().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        assert_eq!(
            metrics.templates.iter().map(|template| template.completed_tasks).sum::<u32>(),
            manifest.source_queries
        );
        assert_eq!(manifest.request.budget.max_queries, [6, 25, 6][index]);
        assert!(manifest.source_queries <= manifest.request.budget.max_queries);
        source_queries += manifest.source_queries;
        originals
            .push(serde_json::json!({ "capture": capture, "source_queries_including_setup": manifest.source_queries,
            "source": manifest.description, "read_metrics": metrics }));
    }
    assert!(source_queries <= if recover_at_default_cadence { 37 } else { 31 });
    // Working checkpoints retire resolved episodes. Their canonical revision files remain the authority.
    let mut archive = archive::NativeArchive::create(&directory, &fixture.cancellation).await.unwrap();
    let mut revisions = Vec::new();
    let mut recovery_original = None;
    for entry in archive::retained(&outbox, &fixture.cancellation).await.unwrap() {
        assert_ne!(entry.request.collection(), TelemetryCollection::DerivedDiagnosis);
        let captured = Box::pin(archive.capture(&outbox, &entry, &fixture.cancellation)).await.unwrap();
        if recover_at_default_cadence && captured.original.capture == *events[2].capture.as_ref().unwrap() {
            recovery_original = Some(Box::new(captured));
        } else {
            revisions.extend(captured.revisions);
        }
    }
    let history = Box::new(history::for_original(&revisions, &first).unwrap());
    assert_eq!(history.first.first, first);
    assert_eq!(history.latest.first, first);
    assert!(matches!(history.latest.report, Some(DiagnosticReport::Blocking { .. })));
    assert!(history.latest.investigation_event.is_some());
    let active = outbox.diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 20).await.unwrap();
    let active =
        active.checkpoints.iter().flat_map(|point| &point.episodes).any(|episode| episode.id == history.first.id);
    if recover_at_default_cadence {
        assert_eq!(history.latest.lifecycle, DiagnosticLifecycle::Resolved);
        assert!(!active, "resolved working state retires while immutable history remains");
        let proof = history.latest.recovery.as_ref().unwrap();
        let recovered = recovery_original.as_ref().unwrap();
        assert_eq!(proof.capture(), &recovered.original);
        assert_eq!(proof.capture().capture, *events[2].capture.as_ref().unwrap());
        assert!(proof.matches_source(&history.latest, &recovered.source, &recovered.original));
    } else {
        assert!(active);
        assert!(history.latest.recovery.is_none(), "actor cleanup is not a controller recovery observation");
    }
    let stored_incidents = synthesis.drain(&mut outbox, &mut archive, &fixture.cancellation).await.unwrap();
    let expected_revision = history.references.last().unwrap();
    let incident = stored_incidents.iter().find(|incident| incident.revision_reference == *expected_revision).unwrap();
    synthesis::verify_incident(incident, &history.latest, expected_revision, &victim, &owner, focused_capture);
    assert_eq!(
        stored_incidents.iter().filter(|incident| incident.entry.episode_id == history.first.id).count(),
        history.references.len()
    );
    assert!(
        history
            .references
            .iter()
            .all(|reference| stored_incidents.iter().any(|incident| incident.revision_reference == *reference))
    );
    let stored_summary = stored_incidents
        .iter()
        .map(|incident| {
            serde_json::json!({
                "episode_id":incident.entry.episode_id,"revision_id":incident.entry.revision_id,
                "revision":incident.entry.revision,"lifecycle":incident.entry.lifecycle,
                "revision_reference":incident.revision_reference,"attribution_status":incident.attribution_status,
                "affected":incident.affected_queries.iter().map(|query|&query.identity).collect::<Vec<_>>(),
                "culprit_roles":incident.culprit_queries.iter().map(|query|&query.causal_role).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    owned::write_automatic_receipt(
        case_name,
        "automatic-source-incident.json",
        &serde_json::json!({
            "normal_enrollment": plan.summary, "events": events,
            "original_incident": history.latest, "originals": originals,
            "retained_revision_references":history.references,"stored_incidents":stored_summary,
            "installed_default_checks_synthesizer":true,"stored_output_native_queries":0,
            "controller_elapsed_ms": started.elapsed().as_millis().to_string(),
            "default_attention_ms": 30_000, "default_period_seconds": 300,
            "trigger": "native-lock-wait-point", "duration_threshold_exercised": false,
            "source_capture_cap": if recover_at_default_cadence { 3 } else { 2 },
            "source_query_cap_including_each_setup": if recover_at_default_cadence { 37 } else { 31 },
            "native_source_queries_including_setup": source_queries,
            "default_cadence_recovery_exercised": recover_at_default_cadence,
            "measured_cpu": null, "measured_cost_qualification": false,
        }),
    )
    .unwrap();
    // Close/reopen authenticates the same sealed originals after native actor retirement, without another login.
    drop(outbox);
    let mut reopened = Outbox::open(&outbox_path).unwrap();
    for event in &events {
        reopened.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
    }
}

fn verify_original(fixture: &MaintenanceFixture, baseline: &DiagnosticInput, current: &DiagnosticInput) {
    assert!(baseline.parent.capture.revision.is_some() && current.parent.capture.revision.is_some());
    let SourceObservation::FocusedBlocking { snapshot, result } = &current.source.observation else { panic!() };
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(result.membership, FocusedRequestMembership::Observed);
    assert_eq!(result.request_end, BlockingRequestState::SameStatement);
    assert!(!snapshot.coverage.complete_scope, "selected closure is not a complete database roster");
    let original = baseline.rows.iter().find(|row| {
        matches!(&row.row.record, TelemetryRecord::Activity(activity) if activity.key.session.id == fixture.builder_pid.to_string())
    }).unwrap();
    let TelemetryRecord::Activity(original_row) = &original.row.record else { panic!() };
    assert_eq!(result.focus.parent, original.reference);
    assert_eq!(result.focus.original.key, original_row.key);
    assert_eq!(result.focus.statement_identity, original_row.statement_identity.as_ref().unwrap().clone());
    let rows = current
        .rows
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(activity) = &row.row.record else { panic!() };
            ActivityFrameRow { observation: activity, reference: Some(&row.reference) }
        })
        .collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(&current.source, &rows, true).unwrap();
    assert_eq!(report.original, original.reference);
    assert!(report.context.has_qualified_local_owner());
    let original_holder = baseline
        .rows
        .iter()
        .find_map(|row| match &row.row.record {
            TelemetryRecord::Activity(activity) if activity.key.session.id == fixture.holder_pid.to_string() => {
                Some(activity.as_ref())
            }
            _ => None,
        })
        .unwrap();
    assert!(report.relationships.iter().any(|edge| {
        edge.waiter == original_row.key
            && edge.owner_observed
            && matches!(&edge.owner, ActivityWaitOwner::Session { id, generation: Some(generation) }
                if id == &original_holder.key.session.id && generation == &original_holder.key.session.generation)
    }));
    let owner =
        rows.iter().find(|row| row.observation.key.session.id == fixture.holder_pid.to_string()).unwrap().observation;
    assert!(owner.key.request.is_none() && owner.statement_identity.is_none());
    assert!(
        matches!(owner.statement, DefinitionPayload::Unavailable { .. }),
        "an idle owner's previous SQL is not current executor proof"
    );
}

#[test]
fn the_native_body_owns_concrete_future_storage_with_no_inline_completed_diagnostic_output() {
    fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) = size(|fixture: &'static mut MaintenanceFixture| prove(fixture, false));
    assert!(future <= 64 * 1024, "actual native body future is {future} bytes");
    assert!(output <= 8);
    let _boxed: fn(&mut MaintenanceFixture) -> Pin<Box<dyn Future<Output = ()> + '_>> =
        |fixture| Box::pin(prove(fixture, false));
}
