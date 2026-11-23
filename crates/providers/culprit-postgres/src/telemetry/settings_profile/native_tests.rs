//! The exact eventual production profile; Root applies the separate real policy promotion before execution.
use super::super::profile_native_tests::{Factory, admission, specification};
use super::*;
use culprit_collector::{
    capture::{self},
    outbox::Outbox,
};
use culprit_core::Provider;
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
        budget: bounded_budget(&CollectionBudget { page_rows: 2, ..Default::default() }),
    }
}
#[tokio::test]
#[ignore = "requires the existing nonce-owned PG18 fixture and deliberate normal Settings policy promotion"]
async fn owned_settings_profile_uses_six_actual_phases_and_seals_the_original_read_context() {
    let cancelled = CollectionCancellation::default();
    let spec = specification(&cancelled).await;
    let request = request();
    let active = Arc::new(AtomicBool::new(true));
    let factory =
        Factory { provider: crate::PostgresProvider::new(&spec).unwrap(), admission: admission(&request, active) };
    let root = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::create(&root.path().join("outbox"), OutboxLimits::default()).unwrap();
    let completed = crate::native_test_support::case::finish(Duration::from_secs(30), &cancelled, async {
        let mut lease = outbox.reserve("owned-settings-source", &request).await.unwrap();
        let identity = lease.identity().clone();
        let output = lease.directory().to_owned();
        let result = capture::collect(
            &SourceGate::default(),
            &factory,
            request.clone(),
            &output,
            identity.clone(),
            &cancelled,
            Some(&mut lease),
        )
        .await;
        assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
        let result = result.unwrap();
        assert_eq!(result.status, CollectionStatus::Complete);
        lease.finish().await.unwrap();
        let manifest = outbox.verify(&identity.observation_id).await.unwrap();
        (manifest, output)
    })
    .await;
    assert!(!completed.deadline_reached);
    let (manifest, output) = completed.value;
    assert_eq!(manifest.source_queries, 6, "SET1 +initial metadata/member2 +Settings1 +final metadata/member2");
    assert!(manifest.references.complete);
    let source = manifest.description.as_ref().unwrap();
    assert_eq!(source.partition, SourcePartition::Target);
    assert!(source.target.database().unwrap().incarnation.is_none());
    let context = source.observation.snapshot_acquisition_context().unwrap();
    assert!(context.has_qualified_local_owner());
    assert_eq!(context.target, source.target);
    assert!(context.database_writable.is_some());
    let metrics = manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid());
    assert!(!metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    assert_eq!(metrics.templates.iter().map(|metric| metric.completed_tasks).sum::<u32>(), 6);
    assert_eq!(metrics.templates.iter().map(|metric| metric.failures).sum::<u32>(), 0);
    let settings_id = digest(native::settings::capture_query(request.budget.max_snapshot_rows as usize + 1).as_bytes());
    assert_eq!(metrics.templates.iter().find(|metric| metric.template_id == settings_id).unwrap().completed_tasks, 1);
    let mut retained = vec![];
    for file in &manifest.files {
        let bytes = std::fs::read(output.join(&file.file)).unwrap();
        assert_eq!(file.sha256.as_deref(), Some(digest(&bytes).as_str()));
        assert_eq!(bytes.len() as u64, u64::from(file.bytes));
        assert!(bytes.ends_with(b"\n"));
        for line in bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()) {
            let row: ExportedRecord = serde_json::from_slice(line).unwrap();
            assert_eq!(row.capture, manifest.capture);
            assert_eq!(row, ExportedRecord::new(source, &manifest.capture, row.record.clone()).unwrap());
            let TelemetryRecord::Setting(setting) = row.record else { panic!("Settings stream only") };
            assert!(setting.is_valid());
            retained.push(setting);
        }
    }
    assert_eq!(retained.len(), source.observation.snapshot().unwrap().records as usize);
    assert!(retained.iter().any(|setting| setting.name == "autovacuum"));
    assert!(
        retained
            .iter()
            .any(|setting| setting.name == "work_mem" && setting.scope == culprit_core::MeasurementScope::Session)
    );
    assert!(!retained.iter().any(|setting| setting.name == "primary_conninfo" || setting.name.contains("password")));
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
}
#[tokio::test]
#[ignore = "requires the existing nonce-owned PG18 fixture and deliberate normal Settings policy promotion"]
async fn owned_settings_caller_five_and_retirement_preserve_the_original_source_shutdown() {
    let cancellation = CollectionCancellation::default();
    let spec = specification(&cancellation).await;
    let mut limited = request();
    limited.budget.max_queries = 5;
    let factory = Factory {
        provider: crate::PostgresProvider::new(&spec).unwrap(),
        admission: admission(&limited, Arc::new(AtomicBool::new(true))),
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("limited");
    let completed = crate::native_test_support::case::finish(
        Duration::from_secs(30),
        &cancellation,
        capture::collect_once(&SourceGate::default(), &factory, limited, &path, &cancellation),
    )
    .await;
    assert!(!completed.deadline_reached);
    match completed.value {
        Ok(manifest) => assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::QueryLimit)),
        Err(error) => assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit),
    }
    assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
    let manifest: CollectionManifest =
        serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::QueryLimit));
    assert_eq!(manifest.source_queries, 5);
    assert!(manifest.description.as_ref().unwrap().observation.snapshot_acquisition_context().is_none());
    let mut byte_limited = request();
    byte_limited.budget.page_bytes = 1024;
    byte_limited.budget.total_bytes = 1024;
    byte_limited.budget.max_sql_bytes = 1024;
    byte_limited.budget.max_plan_bytes = 1024;
    let byte_factory = Factory {
        provider: crate::PostgresProvider::new(&spec).unwrap(),
        admission: admission(&byte_limited, Arc::new(AtomicBool::new(true))),
    };
    let byte_path = root.path().join("byte-limited");
    let completed = crate::native_test_support::case::finish(
        Duration::from_secs(30),
        &cancellation,
        capture::collect_once(&SourceGate::default(), &byte_factory, byte_limited, &byte_path, &cancellation),
    )
    .await;
    assert!(!completed.deadline_reached);
    match completed.value {
        Ok(manifest) => assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::ByteLimit)),
        Err(error) => assert_eq!(CollectionStop::from_error(&error), CollectionStop::ByteLimit),
    }
    assert_eq!(byte_factory.provider.source_shutdown(), SourceShutdown::Quiescent);
    let bytes: CollectionManifest =
        serde_json::from_slice(&std::fs::read(byte_path.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(bytes.status, CollectionStatus::Incomplete(CollectionStop::ByteLimit));
    assert!(
        bytes.description.as_ref().is_none_or(|source| source.observation.snapshot_acquisition_context().is_none())
    );
    assert!(
        matches!(bytes.source_queries, 2 | 4),
        "the bounded metadata or Settings response fails before final checks"
    );
    assert!(!bytes.references.complete, "a byte-limited prefix is never a completed empty Settings population");
    let active = Arc::new(AtomicBool::new(true));
    let request = request();
    let provider = crate::PostgresProvider::new(&spec).unwrap();
    let mut source = provider.telemetry_source_for_profile(&request, &admission(&request, active.clone())).unwrap();
    active.store(false, Ordering::Release);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
}
