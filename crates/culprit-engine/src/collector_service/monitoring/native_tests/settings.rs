//! Actual Standard enrollment and automatic Settings Gold, using the same owned source/canonical archive owners.
use super::*;

const CONNECTION: &str = "owned-builtin-pg-settings";
const EXPECTED_VALUE_ENV: &str = "CULPRIT_NATIVE_PG_SETTINGS_EXPECTED_AUTOVACUUM";
const SOURCE_CAPTURES: usize = 2;

#[tokio::test]
#[ignore = "requires nonce-owned PG18 startup autovacuum=off (or explicit healthy on), genuine policy promotion and private safety receipts"]
async fn normal_builtin_pg_settings_publish_the_existing_config_finding_and_zero_query_gold_without_a_receipt() {
    MaintenanceFixture::run_automatic(AutomaticCase::Focused, |fixture| Box::pin(prove(fixture))).await;
}

async fn prove(fixture: &mut MaintenanceFixture) {
    let expected = std::env::var(EXPECTED_VALUE_ENV).expect("explicit owned startup expectation");
    assert!(matches!(expected.as_str(), "off" | "on"));
    case::require_active(&fixture.cancellation).unwrap();
    let directory = owned::automatic_directory("builtin-safety").unwrap();
    let (config, workspace) =
        configured_workspace(&fixture.specification, "CULPRIT_PREVIEW_PG_PASSWORD", &directory, CONNECTION);
    let plan = Enrollment::resolve(&workspace, &config).unwrap();
    let mut enrolled = plan.summary.automatic.iter().map(|source| source.recipe_id.as_str()).collect::<Vec<_>>();
    enrolled.sort_unstable();
    assert_eq!(
        enrolled,
        ["postgres.activity", "postgres.settings"],
        "actual promoted normal catalog, no filtered test registration"
    );
    assert!(
        plan.profiles.values().all(|profile| profile.qualification().is_none() && profile.builtin_policy().is_some())
    );
    let inventory = plan.inventory.as_ref().unwrap();
    assert!(inventory.targets[0].schedules.iter().all(|schedule| schedule.every_seconds == 300));
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let mut outbox = Outbox::create(&directory.join("outbox"), OutboxLimits::default()).unwrap();
    let synthesis =
        Box::new(synthesis::NativeSynthesis::install(&mut outbox, &workspace, &fixture.cancellation).await.unwrap());
    let backend = plan.backend(&workspace);
    let mut events = Vec::new();
    let started = tokio::time::Instant::now();
    eprintln!("owned settings phase=normal-source-intake");
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        for _ in 0..SOURCE_CAPTURES {
            step(&mut runner, &fixture.cancellation, &mut events).await;
        }
    }
    let mut schedules = events.iter().map(|event| event.schedule.as_str()).collect::<Vec<_>>();
    schedules.sort_unstable();
    assert_eq!(schedules, ["postgres.activity", "postgres.settings"]);
    assert!(
        started.elapsed() >= Duration::from_secs(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS.into()),
        "the two normal baselines pay their real shared-resource cooldown"
    );
    let selected = events.iter().find(|event| event.schedule == "postgres.settings").unwrap().capture.as_ref().unwrap();
    let mut native_queries = 0_u32;
    for event in &events {
        let manifest = outbox.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
        assert_eq!(manifest.status, CollectionStatus::Complete);
        assert_eq!(manifest.request.budget.max_queries, 6);
        assert!(manifest.source_queries <= 6);
        let metrics = manifest.read_metrics.as_ref().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        assert_eq!(
            metrics.templates.iter().map(|template| template.completed_tasks).sum::<u32>(),
            manifest.source_queries
        );
        native_queries = native_queries.checked_add(manifest.source_queries).unwrap();
    }
    assert!(native_queries <= 12);
    // The existing raw archive owns its original30s allowance only after the real source cadence is paid.
    eprintln!("owned settings phase=canonical-original-and-gold");
    let mut archive = archive::NativeArchive::create(&directory, &fixture.cancellation).await.unwrap();
    let entry = archive::retained(&outbox, &fixture.cancellation)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.capture == *selected)
        .unwrap();
    let original = Box::pin(archive.capture(&outbox, &entry, &fixture.cancellation)).await.unwrap();
    let context = original.source.observation.snapshot_acquisition_context().unwrap();
    assert!(context.has_qualified_local_owner());
    assert_eq!(context.database_writable, Some(true));
    assert!(original.source.target.database().unwrap().incarnation.is_none());
    assert_eq!(original.source.partition, SourcePartition::Target);
    let values = original.settings.iter().filter(|setting| setting.name == "autovacuum").collect::<Vec<_>>();
    let [setting] = values.as_slice() else { panic!("actual startup setting missing or ambiguous") };
    assert_eq!(setting.scope, culprit_core::MeasurementScope::Server);
    assert_eq!(setting.native_context.as_deref(), Some("sighup"));
    assert_eq!(setting.native_source.as_deref(), Some("command line"));
    assert_eq!(setting.value_in_use.as_deref(), Some(expected.as_str()));
    let findings = synthesis.drain_findings(&mut outbox, &mut archive, &fixture.cancellation).await.unwrap();
    let matched = findings
        .iter()
        .filter(|row| row.finding.check == "config" && row.finding.subject.name == "autovacuum")
        .collect::<Vec<_>>();
    assert_eq!(
        matched.len(),
        usize::from(expected == "off"),
        "healthy startup is not credited as the planted positive"
    );
    if let [finding] = matched.as_slice() {
        assert_eq!(finding.finding.title, culprit_core::t!("check.config.autovacuum.title"));
        assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());
        assert_eq!(finding.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
        assert!(finding.query.is_none() && finding.cause.is_none());
        assert!(finding.inputs.iter().any(|reference| reference.capture_ref() == original.original));
    }
    let gold = archive::retained(&outbox, &fixture.cancellation)
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.request.collection() == TelemetryCollection::DerivedDiagnosis)
        .collect::<Vec<_>>();
    let mut assessed_settings = false;
    for entry in gold {
        let manifest = outbox.verify(&entry.capture.observation_id).await.unwrap();
        assert_eq!(manifest.source_queries, 0);
        let SourceObservation::DerivedDiagnosis(header) = &manifest.description.as_ref().unwrap().observation else {
            panic!()
        };
        if header.inputs.iter().any(|input| input.capture == original.original) {
            assert!(header.input_read_complete);
            assert_eq!(header.finding_evaluations.assessment_count, 1);
            assert_eq!(header.finding_evaluations.assessments[0].scope, DerivedFindingScope::Settings);
            assert!(
                header
                    .finding_evaluations
                    .checks
                    .iter()
                    .any(|state| state.status == culprit_core::CheckStatus::Unavailable)
            );
            assessed_settings = true;
        }
    }
    assert!(assessed_settings, "real default queued output retains zero/nonzero Settings assessments");
    owned::write_automatic_receipt(
        "builtin-safety",
        "automatic-settings-findings.json",
        &serde_json::json!({
            "normal_enrollment":true, "server_startup_autovacuum":expected,
            "expected_positive_exercised":expected=="off", "native_source_capture_count":events.len(),
            "native_source_queries_including_setup":native_queries, "native_source_query_cap":12,
            "settings_source_cap":6, "stored_output_native_queries":0,
            "canonical_original":original.original, "published_gold_findings":findings.len(),
            "settings_assessment_retained":assessed_settings,
            "controller_elapsed_ms":started.elapsed().as_millis(),
            "default_period_seconds":300,"default_cadence_recovery_exercised":false,
            "measured_cost_qualification":false,"measured_cpu":false,
        }),
    )
    .unwrap();
}

#[test]
fn normal_settings_orchestration_has_bounded_concrete_future_storage() {
    fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) = size(|fixture: &'static mut MaintenanceFixture| prove(fixture));
    assert!(future <= 32 * 1024, "actual settings body future is {future} bytes");
    assert!(output <= 8);
    let (outer, output) =
        size(|()| MaintenanceFixture::run_automatic(AutomaticCase::Focused, |fixture| Box::pin(prove(fixture))));
    assert!(outer <= 32 * 1024, "actual settings fixture future is {outer} bytes");
    assert!(output <= 8);
}
