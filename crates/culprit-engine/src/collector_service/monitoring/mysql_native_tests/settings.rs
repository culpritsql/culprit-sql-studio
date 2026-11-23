//! Normal two-baseline enrollment, immutable original and actual queued ConfigCheck/Gold on the owned fixture.
use super::*;
const EXPECTED_ENV: &str = "CULPRIT_NATIVE_MYSQL_SETTINGS_EXPECTED_SLOW_QUERY_LOG";
#[tokio::test]
#[ignore = "requires nonce-owned MySQL8.4/MariaDB11.8 startup slow_query_log OFF/ON and actual production Settings policy"]
async fn normal_builtin_mysql_settings_reach_config_check_and_queued_zero_query_gold() {
    BlockingFixture::run(|fixture| Box::pin(prove(fixture))).await;
}
async fn prove(fixture: &mut BlockingFixture) {
    let expected = std::env::var(EXPECTED_ENV).expect("explicit disposable fixture startup expectation");
    assert!(matches!(expected.as_str(), "OFF" | "ON"));
    let directory = owned::automatic_directory().unwrap();
    let (config, workspace, plan) = configured(&fixture.specification, &directory);
    let inventory = plan.inventory.as_ref().unwrap();
    assert_eq!(inventory.targets[0].schedules.len(), 2);
    assert!(inventory.targets[0].schedules.iter().all(|schedule| schedule.every_seconds == 300));
    let journal = FleetJournal::open(&directory.join("fleet"), inventory, &config.shard).await.unwrap();
    let mut outbox = Outbox::create(&directory.join("outbox"), OutboxLimits::default()).unwrap();
    let synthesis =
        Box::new(synthesis::NativeSynthesis::install(&mut outbox, &workspace, &fixture.cancellation).await.unwrap());
    let backend = plan.backend(&workspace);
    let mut events = Vec::new();
    let started = tokio::time::Instant::now();
    {
        let mut runner = FleetRunner::new(&backend, inventory, &config.shard, &journal, &mut outbox).unwrap();
        for _ in 0..2 {
            Box::pin(step(&mut runner, &fixture.cancellation, &mut events, Completion::Focused)).await;
        }
    }
    let mut schedules = events.iter().map(|event| event.schedule.as_str()).collect::<Vec<_>>();
    schedules.sort_unstable();
    assert_eq!(schedules, ["mysql.activity", "mysql.settings"]);
    assert!(
        started.elapsed() >= Duration::from_secs(culprit_collector::fleet::MIN_RESOURCE_GAP_SECONDS.into()),
        "normal sibling baseline pays the unchanged resource cooldown"
    );
    let selected = events.iter().find(|event| event.schedule == "mysql.settings").unwrap().capture.as_ref().unwrap();
    let mut source_queries = 0u32;
    for event in &events {
        let manifest = outbox.verify(&event.capture.as_ref().unwrap().observation_id).await.unwrap();
        let expected_queries = if event.schedule == "mysql.settings" { 18 } else { 20 };
        assert_eq!(manifest.status, CollectionStatus::Complete);
        assert_eq!(manifest.request.budget.max_queries, expected_queries);
        assert_eq!(manifest.source_queries, expected_queries);
        let metrics = manifest.read_metrics.as_ref().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        assert_eq!(metrics.templates.iter().map(|metric| metric.completed_tasks).sum::<u32>(), manifest.source_queries);
        source_queries += manifest.source_queries;
    }
    assert_eq!(source_queries, 38);
    let mut archive = Box::pin(archive::NativeArchive::create(&directory, &fixture.cancellation)).await.unwrap();
    let entry = archive::retained(&outbox, &fixture.cancellation)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.capture == *selected)
        .unwrap();
    let original = Box::new(Box::pin(archive.capture(&outbox, &entry, &fixture.cancellation)).await.unwrap());
    let execution = original.source.observation.snapshot_acquisition_context().unwrap();
    assert!(execution.has_qualified_local_owner());
    assert_eq!(execution.target, original.source.target);
    assert_eq!(execution.database_writable, Some(true));
    assert_eq!(original.source.partition, SourcePartition::Target);
    assert!(original.source.target.database().unwrap().incarnation.is_none());
    let context = original.source.observation.snapshot_settings_context().unwrap();
    assert!(context.is_valid_for(Engine::Mysql));
    let setting = original.settings.iter().find(|setting| setting.name == "slow_query_log").unwrap();
    assert_eq!(setting.scope, culprit_core::MeasurementScope::Server);
    assert_eq!(setting.value, None);
    assert_eq!(setting.value_in_use.as_deref(), Some(expected.as_str()));
    assert_eq!(setting.native_context.as_deref(), Some("global"));
    assert_eq!(setting.native_is_default, None);
    assert_eq!(
        original.settings.iter().find(|setting| setting.name == "performance_schema").unwrap().value_in_use.as_deref(),
        Some("ON")
    );
    assert_eq!(
        original
            .settings
            .iter()
            .find(|setting| setting.name == "long_query_time")
            .unwrap()
            .value_in_use
            .as_ref()
            .unwrap()
            .parse::<f64>()
            .unwrap(),
        0.2
    );
    let findings = Box::pin(synthesis.drain_findings(&mut outbox, &mut archive, &fixture.cancellation)).await.unwrap();
    let matched = findings
        .iter()
        .filter(|row| row.finding.check == "config" && row.finding.subject.name == "slow_query_log")
        .collect::<Vec<_>>();
    assert_eq!(
        matched.len(),
        usize::from(expected == "OFF"),
        "healthy native startup is an independent zero-finding evaluated case"
    );
    if let [finding] = matched.as_slice() {
        assert_eq!(finding.finding.title, culprit_core::t!("check.config.slow_query_log.title"));
        assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());
        assert_eq!(finding.guidance, DerivedGuidance::HistoricalRequiresFreshVerification);
        assert!(finding.query.is_none() && finding.cause.is_none());
        assert!(finding.inputs.iter().any(|reference| reference.capture_ref() == original.original));
    }
    let mut assessed = false;
    for entry in archive::retained(&outbox, &fixture.cancellation)
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.request.collection() == TelemetryCollection::DerivedDiagnosis)
    {
        let manifest = outbox.verify(&entry.capture.observation_id).await.unwrap();
        assert_eq!(manifest.source_queries, 0);
        let SourceObservation::DerivedDiagnosis(header) = &manifest.description.as_ref().unwrap().observation else {
            panic!()
        };
        if header.inputs.iter().any(|input| input.capture == original.original) {
            assert!(header.input_read_complete);
            assert_eq!(header.finding_evaluations.assessment_count, 1);
            assert_eq!(header.finding_evaluations.assessments[0].scope, DerivedFindingScope::Settings);
            assessed = true;
        }
    }
    assert!(assessed, "actual stored Settings output keeps evaluated scope even when its finding population is empty");
    owned::write_receipt("automatic-settings-findings.json",&serde_json::json!({"normal_enrollment":plan.summary,"native_format":context.format,"raw_version":context.version,"startup_slow_query_log":expected,"source_captures":events,"source_queries_including_setup":source_queries,"native_source_query_cap":38,"settings_source_queries":18,"stored_output_native_queries":0,"canonical_original":original.original,"settings_assessed":assessed,"findings":findings,"default_cadence_seconds":300,"source_cost_measured":false,"native_cpu_measured":false})).unwrap();
}
#[test]
fn normal_settings_native_body_keeps_bounded_concrete_future_storage() {
    fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (future, output) = size(|fixture: &'static mut BlockingFixture| prove(fixture));
    assert!(future <= 32 * 1024, "actual Settings future {future}");
    assert!(output <= 8);
}
