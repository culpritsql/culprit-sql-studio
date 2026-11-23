//! Same normal enrollment/controller/Gold path, supplied by nonce-owned native MySQL/MariaDB actors.
use super::native_support::{archive, history, synthesis};
use super::*;
use crate::{NewConnection, secrets::Memory};
use culprit_collector::{fleet::FleetJournal, outbox::DiagnosticInput};
use culprit_core::{AuthKind, Engine, Environment};
use culprit_mysql::native_test_support::{self as owned, BlockingFixture};
use std::{path::Path, sync::Arc, time::Duration};

const CONNECTION: &str = "owned-builtin-mysql";
#[derive(Clone, Copy, PartialEq, Eq)]
enum Completion {
    Focused,
    DefaultCadenceRecovery,
}
impl Completion {
    fn captures(self) -> usize {
        match self {
            Self::Focused => 2,
            Self::DefaultCadenceRecovery => 3,
        }
    }
}
#[tokio::test]
#[ignore = "requires explicit nonce-owned MySQL8.4 or MariaDB11.8 supervisor and private safety receipt directory"]
async fn normal_builtin_mysql_enrollment_automatically_explains_one_native_metadata_owner_without_a_cost_receipt() {
    BlockingFixture::run(|fixture| Box::pin(prove(fixture, Completion::Focused))).await;
}
#[tokio::test]
#[ignore = "separate real300s-cadence MySQL8.4/MariaDB11.8 recovery; requires nonce-owned supervisor and private safety receipts"]
async fn normal_builtin_mysql_incident_resolves_at_the_real_default_cadence_and_retains_the_original_answer() {
    BlockingFixture::run_default_cadence_recovery(|fixture| {
        Box::pin(prove(fixture, Completion::DefaultCadenceRecovery))
    })
    .await;
}
fn configured(source: &culprit_core::ConnectionSpec, directory: &Path) -> (CollectorConfig, Workspace, Enrollment) {
    let mut config = super::super::tests::config(directory);
    config.source_credentials.clear();
    config.monitoring = MonitoringPolicy::Standard { budget: CollectionBudget::default() };
    let target = &mut config.inventory.targets[0];
    target.id = CONNECTION.into();
    target.connection = CONNECTION.into();
    target.admission_group = "owned-mysql-instance".into();
    target.schedules.clear();
    let workspace = Workspace::open_in(&config.workspace).with_secrets(Arc::new(Memory::default()));
    workspace
        .add_connection(NewConnection {
            name: CONNECTION.into(),
            input: format!(
                "mysql://{}@127.0.0.1:{}/{}?sslmode=disabled",
                source.user.as_ref().unwrap(),
                source.port.unwrap(),
                source.database.as_ref().unwrap()
            ),
            engine: Some(Engine::Mysql),
            environment: Environment::Local,
            auth: Some(AuthKind::Password),
            password_env: Some("CULPRIT_SMOKE_PASSWORD".into()),
            ..Default::default()
        })
        .unwrap();
    let plan = Enrollment::resolve(&workspace, &config).unwrap();
    assert_eq!(plan.summary.automatic.len(), 1);
    assert_eq!(plan.summary.automatic[0].recipe_id, "mysql.activity");
    assert!(!plan.summary.gaps.iter().any(|gap| gap.recipe_id.as_deref() == Some("mysql.activity")));
    let schedule = &plan.inventory.as_ref().unwrap().targets[0].schedules[0];
    assert_eq!(schedule.every_seconds, 300, "actual unchanged standard cadence");
    assert_eq!(schedule.budget.max_queries, 20, "focused policy cannot enlarge baseline native work");
    let ScheduledCollection::Activity { policy } = &schedule.collection else { panic!() };
    assert_eq!((policy.blocked_after_ms, policy.running_after_ms), (30_000, 60_000));
    assert!(
        plan.profiles.values().all(|profile| profile.qualification().is_none() && profile.builtin_policy().is_some())
    );
    assert_eq!(
        plan.backend(&workspace)
            .investigation_budget(CONNECTION, &CollectionSubject::Database, TelemetryCollection::Activity)
            .unwrap(),
        Some(CollectionBudget::default())
    );
    (config, workspace, plan)
}
async fn step(
    runner: &mut FleetRunner<'_>,
    cancellation: &CollectionCancellation,
    events: &mut Vec<FleetCollectionEvent>,
    completion: Completion,
) {
    assert!(!cancellation.is_cancelled());
    let result = runner
        .run(cancellation, Some(1), |event| {
            assert!(events.len() < completion.captures(), "only the declared native source captures are admitted");
            events.push(event.clone());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!((result.attempts, result.complete, result.incomplete), (1, 1, 0));
}
async fn prove(fixture: &mut BlockingFixture, completion: Completion) {
    // Actual timeout and caller-retirement owners settle before the fixture's application lock is established.
    let cancellation = Box::pin(fixture.source_cancellation()).await;
    Box::pin(fixture.prepare()).await;
    Box::pin(fixture.wait_for_default_attention()).await;
    assert!(fixture.native_relation_observed);
    let directory = owned::automatic_directory().unwrap();
    let (config, workspace, plan) = configured(&fixture.specification, &directory);
    let inventory = plan.inventory.as_ref().unwrap();
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let outbox_path = directory.join("outbox");
    let mut outbox = Outbox::create(&outbox_path, OutboxLimits::default()).unwrap();
    let synthesis =
        Box::new(synthesis::NativeSynthesis::install(&mut outbox, &workspace, &fixture.cancellation).await.unwrap());
    let backend = plan.backend(&workspace);
    let started = tokio::time::Instant::now();
    let mut events = Vec::new();
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events, completion)).await;
        assert_eq!(events[0].schedule, "mysql.activity");
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events, completion)).await;
        assert_eq!(events[1].schedule, "mysql.blocking");
    }
    assert!(
        started.elapsed() >= Duration::from_secs(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS.into()),
        "real resource cooldown cannot be advanced by fixture control time"
    );
    let original_capture = events[0].capture.clone().unwrap();
    let focused_capture = events[1].capture.clone().unwrap();
    let baseline = Box::new(
        Box::pin(outbox.diagnostic_input(&original_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION))
            .await
            .unwrap(),
    );
    let focused = Box::new(
        Box::pin(outbox.diagnostic_input(&focused_capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION))
            .await
            .unwrap(),
    );
    let (first, victim, owner, request) = verify_original(fixture, &baseline, &focused);
    let manifest = Box::new(outbox.verify(&focused_capture.observation_id).await.unwrap());
    let intent = manifest.investigation.as_ref().unwrap().intent_for_source(&focused.source).unwrap();
    let strict = Box::pin(fixture.strict_caller_refusal(&request)).await;
    Box::pin(fixture.release_and_complete()).await;
    if completion == Completion::DefaultCadenceRecovery {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        Box::pin(step(&mut runner, &fixture.cancellation, &mut events, completion)).await;
        assert_eq!(events[2].schedule, "mysql.activity");
        assert!(started.elapsed() >= Duration::from_secs(300), "the original standard poll owns recovery");
    }
    assert_eq!(events.len(), completion.captures());
    let mut archive = Box::pin(archive::NativeArchive::create(&directory, &fixture.cancellation)).await.unwrap();
    let mut revisions = Vec::new();
    let mut recovery_original = None;
    for entry in archive::retained(&outbox, &fixture.cancellation).await.unwrap() {
        assert_ne!(entry.request.collection(), TelemetryCollection::DerivedDiagnosis);
        let captured = Box::pin(archive.capture(&outbox, &entry, &fixture.cancellation)).await.unwrap();
        if completion == Completion::DefaultCadenceRecovery
            && captured.original.capture == *events[2].capture.as_ref().unwrap()
        {
            recovery_original = Some(Box::new(captured));
        } else {
            revisions.extend(captured.revisions);
        }
    }
    // One original row may legitimately start both an attention episode and a lock-wait episode.
    // Follow the exact controller-selected episode, never merge their revision sequences.
    assert!(
        revisions.iter().any(|row| row.revision.episode.first == first && row.revision.episode.id != intent.episode_id)
    );
    let history = Box::new(history::for_episode(&revisions, &intent.episode_id, &first).unwrap());
    assert_eq!(history.first.first, first);
    assert_eq!(history.latest.first, first);
    assert!(matches!(history.latest.report, Some(DiagnosticReport::Blocking { .. })));
    assert!(history.latest.investigation_event.is_some());
    let active = outbox.diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 20).await.unwrap();
    let active =
        active.checkpoints.iter().flat_map(|point| &point.episodes).any(|episode| episode.id == history.first.id);
    match completion {
        Completion::Focused => {
            assert!(active);
            assert!(history.latest.recovery.is_none(), "fixture cleanup is not an observed controller recovery");
        }
        Completion::DefaultCadenceRecovery => {
            assert_eq!(history.latest.lifecycle, DiagnosticLifecycle::Resolved);
            assert!(!active, "resolved working state retires while immutable history remains");
            let proof = history.latest.recovery.as_ref().unwrap();
            let recovered = recovery_original.as_ref().unwrap();
            assert_eq!(proof.capture(), &recovered.original);
            assert_eq!(proof.capture().capture, *events[2].capture.as_ref().unwrap());
            assert!(proof.matches_source(&history.latest, &recovered.source, &recovered.original));
        }
    }
    let incidents = Box::pin(synthesis.drain(&mut outbox, &mut archive, &fixture.cancellation)).await.unwrap();
    let reference = history.references.last().unwrap();
    let incident = incidents.iter().find(|row| row.revision_reference == *reference).unwrap();
    synthesis::verify_incident(incident, &history.latest, reference, &victim, &owner, &focused_capture);
    assert_eq!(
        incidents.iter().filter(|row| row.entry.episode_id == history.first.id).count(),
        history.references.len()
    );
    assert!(
        history.references.iter().all(|reference| incidents.iter().any(|row| row.revision_reference == *reference))
    );
    Box::pin(write_receipt(
        &mut outbox,
        &plan,
        &events,
        &history,
        &incidents,
        &cancellation,
        &strict,
        started.elapsed(),
    ))
    .await;
    drop(outbox);
    let mut reopened = Outbox::open(&outbox_path).unwrap();
    for event in &events {
        reopened.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
    }
}
fn verify_original(
    fixture: &BlockingFixture,
    baseline: &DiagnosticInput,
    focused: &DiagnosticInput,
) -> (DiagnosticEvidenceRef, Box<ActivityObservation>, NativeActivityIdentity, CollectionRequest) {
    let SourceObservation::FocusedBlocking { snapshot, result } = &focused.source.observation else { panic!() };
    assert_eq!(result.binding, FocusedRequestBinding::Matched);
    assert_eq!(result.membership, FocusedRequestMembership::Observed);
    assert_eq!(result.request_end, BlockingRequestState::SameStatement);
    assert!(!snapshot.coverage.complete_scope, "one selected hop cannot be a database census");
    assert_eq!(result.closure.max_depth, 1);
    let original=baseline.rows.iter().find(|row|matches!(&row.row.record,TelemetryRecord::Activity(actor) if actor.key.session.id==fixture.waiter_id.to_string())).unwrap();
    let TelemetryRecord::Activity(victim) = &original.row.record else { panic!() };
    assert_eq!(result.focus.parent, original.reference);
    assert_eq!(result.focus.original.key, victim.key);
    let rows = focused
        .rows
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(actor) = &row.row.record else { panic!() };
            ActivityFrameRow { observation: actor, reference: Some(&row.reference) }
        })
        .collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(&focused.source, &rows, true).unwrap();
    assert_eq!(report.original, original.reference);
    assert!(report.context.has_qualified_local_owner());
    let owner =
        rows.iter().find(|row| row.observation.key.session.id == fixture.holder_id.to_string()).unwrap().observation;
    assert!(owner.idle && owner.key.request.is_none() && owner.statement_identity.is_none());
    assert!(!owner.statement.is_available(), "previous/idle SQL is not acquiring-query proof");
    assert!(report.relationships.iter().any(|edge|edge.waiter==victim.key&&edge.owner_observed&&matches!(&edge.owner,ActivityWaitOwner::Session{id,generation:Some(generation)} if id==&owner.key.session.id&&generation==&owner.key.session.generation)));
    (
        original.reference.clone(),
        victim.clone(),
        owner.key.session.clone(),
        CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::FocusedBlocking { focus: Box::new(result.focus.clone()) },
            budget: culprit_mysql::telemetry_recipes()
                .into_iter()
                .find(|recipe| recipe.id == "mysql.blocking")
                .unwrap()
                .budget,
        },
    )
}
async fn write_receipt(
    outbox: &mut Outbox,
    plan: &Enrollment,
    events: &[FleetCollectionEvent],
    history: &history::NativeHistory,
    incidents: &[DerivedIncident],
    cancellation: &serde_json::Value,
    strict: &ReadWorkMetrics,
    elapsed: Duration,
) {
    let mut originals = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let capture = event.capture.as_ref().unwrap();
        let manifest = outbox.verify(&capture.observation_id).await.unwrap();
        assert_eq!(manifest.status, CollectionStatus::Complete);
        assert_eq!(manifest.request.budget.max_queries, [20, 33, 20][index]);
        assert_eq!(manifest.source_queries, [20, 33, 20][index]);
        let metrics = manifest.read_metrics.as_ref().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        assert_eq!(metrics.templates.iter().map(|row| row.completed_tasks).sum::<u32>(), manifest.source_queries);
        if index == 1 {
            let intent = manifest
                .investigation
                .as_ref()
                .unwrap()
                .intent_for_source(manifest.description.as_ref().unwrap())
                .unwrap();
            assert!(matches!(intent.qualification, RecipeExecutionAdmission::BuiltinSafety { .. }));
            assert!(intent.qualification.cost_evidence().is_none());
        }
        originals.push(serde_json::json!({"capture":capture,"source":manifest.description,"source_queries_including_setup":manifest.source_queries,"read_metrics":metrics}));
    }
    // Serialization is synchronous and follows all native collection phases: no large JSON state in a login future.
    receipt(plan, events, history, incidents, cancellation, strict, elapsed, originals);
}
fn receipt(
    plan: &Enrollment,
    events: &[FleetCollectionEvent],
    history: &history::NativeHistory,
    incidents: &[DerivedIncident],
    cancellation: &serde_json::Value,
    strict: &ReadWorkMetrics,
    elapsed: Duration,
    originals: Vec<serde_json::Value>,
) {
    owned::write_receipt("automatic-source-incident.json",&serde_json::json!({
        "normal_enrollment":plan.summary,"events":events,"originals":originals,
        "original_incident":history.latest,"retained_revision_references":history.references,
        "stored_incidents":incidents,"installed_default_checks_synthesizer":true,
        "stored_output_native_queries":0,"controller_elapsed_ms":elapsed.as_millis().to_string(),
        "default_attention_ms":30_000,"default_cadence_seconds":300,
        "default_cadence_recovery_exercised":history.latest.recovery.is_some(),
        "source_capture_cap":events.len(),"source_query_cap_including_setup":if events.len()==3 {73}else {53},
        "strict32_caller_metrics":strict,
        "source_cancellation":cancellation,"measured_cpu":null,"measured_cost_qualification":false,
        "one_hop_only":true,"complete_head_chain":false,"ddl_sandwich_exercised":false,"row_lock_trigger_exercised":false,
    })).unwrap();
}
#[test]
fn native_automatic_body_stores_concrete_phase_futures_on_the_heap() {
    fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) =
        size(|fixture: &'static mut BlockingFixture| prove(fixture, Completion::DefaultCadenceRecovery));
    assert!(future <= 64 * 1024, "actual native automatic future is {future} bytes");
    assert!(output <= 8);
}
