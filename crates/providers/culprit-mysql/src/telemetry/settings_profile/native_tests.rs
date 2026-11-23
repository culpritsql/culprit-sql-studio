//! Eventual production admission and existing collector/manifest owner; no fake verifier or native workload.
use super::*;
use culprit_collector::{
    capture::{self, SourceFactory},
    outbox::Outbox,
};
use culprit_core::Provider;
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::sync::atomic::{AtomicBool, Ordering};
struct Factory {
    provider: crate::MysqlProvider,
    admission: SourceProfileAdmission,
}
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.provider.telemetry_source_for_safety(request, &self.admission)
    }
}
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
        budget: bounded_budget(&CollectionBudget { page_rows: 2, page_bytes: 4096, ..Default::default() }),
    }
}
fn admission(request: &CollectionRequest, active: Arc<AtomicBool>) -> SourceProfileAdmission {
    let recipe = crate::telemetry_recipes().into_iter().find(|recipe| recipe.selector.matches(request)).unwrap();
    let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else {
        panic!("apply only the separate eventual production Settings policy locally for owned acceptance")
    };
    assert!(crate::verify_builtin_safety(&policy).unwrap());
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"owned-native-settings-route"),
        *policy,
        None,
        request.budget.clone(),
        move || active.load(Ordering::Acquire),
    )
    .unwrap();
    assert!(admission.qualification().is_none());
    admission
}
async fn owned() -> crate::MysqlProvider {
    let owner = std::env::var("CULPRIT_SMOKE_OWNER").unwrap();
    assert!(owner.len() == 24 && owner.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    assert_eq!(std::env::var("CULPRIT_SMOKE_ENGINE").unwrap(), "mysql");
    assert_eq!(std::env::var("CULPRIT_SMOKE_DATABASE").unwrap(), "culprit_smoke");
    let port = std::env::var("CULPRIT_MYSQL_TEST_PORT").unwrap().parse::<u16>().unwrap();
    assert!(port >= 1024 && port != 3306);
    let provider = super::super::native_tests::fixture();
    let mut reader = ReadSession::new(provider.connector.clone(), CollectionBudget::default(), Scope::Server);
    let context = metadata::connection_context(&mut reader, &provider.connector).await;
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let context = context.unwrap();
    assert_eq!(context.server_name, format!("culprit-smoke-mysql-{owner}"));
    assert_eq!(context.database_name, "culprit_smoke");
    assert!(!context.read_only);
    assert!(crate::builtin_safety::supported(context.flavor, &context.version));
    assert_eq!(
        reader.query_count(),
        9,
        "existing setup7+parameterized fixture-identity2, distinct from the source ledger18"
    );
    provider
}
#[tokio::test]
#[ignore = "requires nonce-owned MySQL8.4/MariaDB11.8 and deliberate actual production Settings policy"]
async fn owned_settings_profile_seals_eighteen_actual_phases_with_current_member_and_global_raw_values() {
    let request = request();
    let factory = Factory { provider: owned().await, admission: admission(&request, Arc::new(AtomicBool::new(true))) };
    let directory = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::create(&directory.path().join("outbox"), OutboxLimits::default()).unwrap();
    let mut lease = outbox.reserve("owned-global-settings", &request).await.unwrap();
    let capture = lease.identity().clone();
    let output = lease.directory().to_owned();
    let result = capture::collect(
        &SourceGate::default(),
        &factory,
        request.clone(),
        &output,
        capture.clone(),
        &CollectionCancellation::default(),
        Some(&mut lease),
    )
    .await;
    let finished = lease.finish().await;
    assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
    result.unwrap();
    finished.unwrap();
    let manifest = outbox.verify(&capture.observation_id).await.unwrap();
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert_eq!(manifest.source_queries, 18);
    let source = manifest.description.as_ref().unwrap();
    let context = source.observation.snapshot_acquisition_context().unwrap();
    assert!(context.has_qualified_local_owner());
    assert_eq!(context.database_writable, Some(true));
    assert_eq!(context.target, source.target);
    assert_eq!(source.partition, SourcePartition::Target);
    assert!(source.target.database().unwrap().incarnation.is_none());
    let metadata = source.observation.snapshot_settings_context().unwrap().clone();
    assert!(metadata.is_valid_for(culprit_core::Engine::Mysql));
    let metrics = manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    assert_eq!(metrics.templates.iter().map(|metric| metric.completed_tasks).sum::<u32>(), 18);
    let captured = super::super::native_tests::capture::read_capture(manifest, &request.budget, &output);
    assert!(!captured.records.is_empty());
    let mut names = BTreeSet::new();
    for row in &captured.records {
        let TelemetryRecord::Setting(setting) = &row.record else { panic!() };
        assert!(names.insert(setting.name.clone()));
        assert_eq!(setting.scope, culprit_core::MeasurementScope::Server);
        assert_eq!(setting.native_context.as_deref(), Some("global"));
        assert_eq!(setting.value, None);
        assert_eq!(setting.native_is_default, None);
        assert!(metadata.project(setting).is_some());
    }
    assert!(names.contains("slow_query_log") && names.contains("performance_schema"));
    assert!(
        captured.manifest.files.iter().map(|file| u64::from(file.bytes)).sum::<u64>()
            <= u64::from(request.budget.total_bytes)
    );
    assert!(captured.manifest.files.iter().all(|file| {
        file.pages > 0
            && u64::from(file.rows) <= u64::from(file.pages) * u64::from(request.budget.page_rows)
            && u64::from(file.bytes) <= u64::from(file.pages) * u64::from(request.budget.page_bytes)
    }));
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    eprintln!(
        "owned global Settings phases18; format={:?}; native rows={}; source cost/CPU remain unmeasured",
        metadata.format,
        captured.records.len()
    );
}
#[tokio::test]
#[ignore = "requires nonce-owned MySQL8.4/MariaDB11.8 and deliberate actual production Settings policy"]
async fn owned_settings_caller_seventeen_and_retired_profile_cannot_seal_or_replace_the_original_native_owner() {
    let directory = tempfile::tempdir().unwrap();
    let mut limited = request();
    limited.budget.max_queries = 17;
    let factory = Factory { provider: owned().await, admission: admission(&limited, Arc::new(AtomicBool::new(true))) };
    let output = directory.path().join("limited");
    let result =
        capture::collect_once(&SourceGate::default(), &factory, limited, &output, &CollectionCancellation::default())
            .await;
    if let Err(error) = result {
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
    }
    assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
    let manifest: CollectionManifest =
        serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::QueryLimit));
    assert_eq!(manifest.source_queries, 17);
    assert!(manifest.description.as_ref().unwrap().observation.snapshot_acquisition_context().is_none());
    let request = request();
    let active = Arc::new(AtomicBool::new(true));
    let provider = owned().await;
    let mut source = provider.telemetry_source_for_safety(&request, &admission(&request, active.clone())).unwrap();
    let before = source.describe().await.unwrap();
    assert!(before.observation.snapshot_acquisition_context().is_none());
    let CollectionRead::Page { next_cursor: Some(cursor), .. } =
        source.read_page(TelemetryStream::SettingObservations, None).await.unwrap()
    else {
        panic!()
    };
    active.store(false, Ordering::Release);
    let at_retirement = source.query_count();
    assert_eq!(at_retirement, 13);
    assert!(source.read_page(TelemetryStream::SettingObservations, Some(&cursor)).await.is_err());
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
    let metrics = source.read_metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert!(
        source.query_count() <= at_retirement + 2,
        "only the existing separately paid emergency cleanup can add control commands"
    );
}
