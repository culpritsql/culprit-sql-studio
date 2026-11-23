//! Native acceptance invokes the same eventual production catalog after deliberate local policy promotion.
use super::super::profile_native_tests::{Factory, admission, specification};
use super::*;
use culprit_collector::{
    capture::{self},
    outbox::Outbox,
};
use culprit_core::Provider;
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::StatementCounters },
        budget: bounded_budget(&CollectionBudget {
            max_snapshot_rows: MAX_NATIVE_ENTRIES,
            page_rows: 20,
            page_bytes: 32 * 1024,
            max_pages: MAX_NATIVE_ENTRIES,
            ..Default::default()
        }),
    }
}
#[tokio::test]
#[ignore = "requires the nonce-owned PG18/pgss1.12 fixture and deliberate normal counter policy promotion"]
async fn owned_counter_profile_seals_exact_native_points_with_six_actual_phases_and_no_query_text() {
    let cancellation = CollectionCancellation::default();
    let spec = specification(&cancellation).await;
    let request = request();
    let factory = Factory {
        provider: crate::PostgresProvider::new(&spec).unwrap(),
        admission: admission(&request, Arc::new(AtomicBool::new(true))),
    };
    let root = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::create(&root.path().join("outbox"), OutboxLimits::default()).unwrap();
    let completed = crate::native_test_support::case::finish(Duration::from_secs(30), &cancellation, async {
        let mut lease = outbox.reserve("owned-counter-profile", &request).await.unwrap();
        let identity = lease.identity().clone();
        let output = lease.directory().to_owned();
        let manifest = capture::collect(
            &SourceGate::default(),
            &factory,
            request.clone(),
            &output,
            identity.clone(),
            &cancellation,
            Some(&mut lease),
        )
        .await
        .unwrap();
        assert_eq!(manifest.status, CollectionStatus::Complete);
        assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
        lease.finish().await.unwrap();
        (outbox.verify(&identity.observation_id).await.unwrap(), output)
    })
    .await;
    assert!(!completed.deadline_reached);
    let (manifest, output) = completed.value;
    assert_eq!(manifest.source_queries, 6, "SET1 +metadata/member2 +text-disabled counters1 +metadata/member2");
    assert!(manifest.references.complete);
    assert!(
        manifest
            .files
            .iter()
            .all(|file| u64::from(file.bytes) <= u64::from(file.pages) * u64::from(request.budget.page_bytes)),
        "the canonical writer admitted every actual page under its byte cap"
    );
    let description = manifest.description.as_ref().unwrap();
    let context = description.observation.snapshot_acquisition_context().unwrap();
    assert_eq!(description.partition, context.partition);
    assert!(context.has_qualified_local_owner());
    assert!(context.database_writable.is_some());
    assert!(description.target.database().unwrap().incarnation.is_none());
    let bounds = ObservationBounds::new(&context.observed_from, &context.observed_until).unwrap();
    let metrics = manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!(metrics.templates.iter().map(|template| template.completed_tasks).sum::<u32>(), 6);
    let mut observations = Vec::new();
    for file in &manifest.files {
        let bytes = std::fs::read(output.join(&file.file)).unwrap();
        assert_eq!(file.sha256.as_deref(), Some(digest(&bytes).as_str()));
        assert_eq!(u64::from(file.bytes), bytes.len() as u64);
        assert!(bytes.ends_with(b"\n"));
        for line in bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()) {
            let row: ExportedRecord = serde_json::from_slice(line).unwrap();
            assert_eq!(row.capture, manifest.capture);
            assert_eq!(row, ExportedRecord::new(description, &manifest.capture, row.record.clone()).unwrap());
            let TelemetryRecord::Operational(observation) = row.record else { panic!("counter point only") };
            let OperationalFact::StatementCounters(sample) = &observation.fact else {
                panic!("no SQL or synthetic runtime")
            };
            assert!(bounds.contains(&sample.observed_at));
            assert!(matches!(sample.reset, culprit_core::ResetMarker::At(_)));
            assert!(
                sample.identity.qualifiers.contains_key("userid")
                    && sample.identity.qualifiers.contains_key("toplevel")
            );
            assert!(
                sample.identity.qualifiers.contains_key("stats_since")
                    && sample.identity.qualifiers.contains_key("postmaster_started_at")
            );
            assert_eq!(sample.member, None);
            assert_eq!(sample.scope, culprit_core::MeasurementScope::Database);
            observations.push(observation);
        }
    }
    assert!(!observations.is_empty(), "the owned seeded fixture contains native statement entries");
    let counters = StatementCounterSnapshot::from_source(description, observations).unwrap();
    assert!(counters.is_valid());
    assert_eq!(counters.context.as_deref(), Some(context));
    assert_eq!(description.observation.range().unwrap(), TelemetryRange::Current);
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
}
#[tokio::test]
#[ignore = "requires the nonce-owned PG18/pgss1.12 fixture and deliberate normal counter policy promotion"]
async fn owned_counter_caller_five_and_retirement_never_repair_incomplete_native_ownership() {
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
    let manifest: CollectionManifest =
        serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.source_queries, 5);
    assert!(manifest.description.as_ref().unwrap().observation.snapshot_acquisition_context().is_none());
    assert_eq!(factory.provider.source_shutdown(), SourceShutdown::Quiescent);
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
    let manifest: CollectionManifest =
        serde_json::from_slice(&std::fs::read(byte_path.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.status, CollectionStatus::Incomplete(CollectionStop::ByteLimit));
    assert!(!manifest.references.complete);
    assert!(
        manifest.description.as_ref().is_none_or(|source| source.observation.snapshot_acquisition_context().is_none())
    );
    assert_eq!(byte_factory.provider.source_shutdown(), SourceShutdown::Quiescent);
    let active = Arc::new(AtomicBool::new(true));
    let request = request();
    let provider = crate::PostgresProvider::new(&spec).unwrap();
    let mut source = provider.telemetry_source_for_profile(&request, &admission(&request, active.clone())).unwrap();
    active.store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
}
