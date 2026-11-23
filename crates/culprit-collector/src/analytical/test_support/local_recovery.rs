//! Real local capture/restore conformance retains the original Collector owner.
use crate::capture::SourceFactory;
use crate::capture::collect;
use crate::delivery::file_store::FileRecoveryStore;
use crate::delivery::recovery::stage_and_commit;
use crate::outbox::Outbox;
use async_trait::async_trait;
use culprit_core::{Result, telemetry::*};
use culprit_evidence::analytical::test_support::Recovery;
use culprit_evidence::delivery::contract::Contract;
use culprit_read::cancellation::CollectionCancellation;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Source {
    description: CollectionDescription,
    rows: Vec<TelemetryRecord>,
    closed: Arc<AtomicBool>,
}
#[async_trait]
impl SourceFactory for Source {
    async fn open(&self, _: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        Ok(Box::new(Self {
            description: self.description.clone(),
            rows: self.rows.clone(),
            closed: self.closed.clone(),
        }))
    }
}
#[async_trait]
impl TelemetrySource for Source {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        Ok(self.description.clone())
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        assert!(cursor.is_none());
        Ok(CollectionRead::Page {
            records: self.rows.iter().filter(|row| row.stream() == stream).cloned().collect(),
            next_cursor: None,
        })
    }
    fn query_count(&self) -> u32 {
        1
    }
    async fn finish(&mut self) -> Result<SourceCompletion> {
        Ok(SourceCompletion::Verified)
    }
    async fn close(&mut self) -> SourceShutdown {
        self.closed.store(true, Ordering::SeqCst);
        SourceShutdown::Quiescent
    }
}

pub async fn collected_resource_recovery(
    destination: &str,
    schema: &str,
    source: CollectionDescription,
    rows: Vec<TelemetryRecord>,
) -> Recovery {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    let selection = match &source.observation {
        SourceObservation::RetainedSamples(history) => {
            CollectionSelection::ResourceHistory { from: history.from.clone(), until: history.until.clone() }
        }
        SourceObservation::MssqlManagedInstanceResources(history) => {
            CollectionSelection::ResourceHistory { from: history.from.clone(), until: history.until.clone() }
        }
        SourceObservation::Activity(_) => CollectionSelection::Activity,
        observation => {
            let snapshot = observation.snapshot().expect("retained resource, Activity or snapshot fixture");
            CollectionSelection::Snapshot { dataset: snapshot.dataset }
        }
    };
    let subject = match &source.target {
        CollectionTarget::ConfiguredConnection(_) => panic!("native fixture cannot represent collector control"),
        CollectionTarget::Database(_) => CollectionSubject::Database,
        CollectionTarget::Shared(resource) => CollectionSubject::Shared { resource: resource.clone() },
    };
    let request = CollectionRequest { subject, selection, budget: CollectionBudget::default() };
    let closed = Arc::new(AtomicBool::new(false));
    let factory = Source { description: source.clone(), rows, closed: closed.clone() };
    let mut lease = outbox.reserve("resource-fixture", &request).await.unwrap();
    let directory = lease.directory().to_owned();
    let identity = lease.identity().clone();
    let manifest = collect(
        &culprit_read::admission::SourceGate::default(),
        &factory,
        request,
        &directory,
        identity.clone(),
        &CollectionCancellation::default(),
        Some(&mut lease),
    )
    .await
    .unwrap();
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert!(manifest.references.integrity_complete);
    lease.finish().await.unwrap();
    drop(outbox);
    let mut restored = Outbox::open(&path).unwrap();
    restored.recover().await.unwrap();
    let sealed = restored.verify(&identity.observation_id).await.unwrap();
    assert_eq!(sealed.capture, identity);
    assert_eq!(sealed.description.as_ref(), Some(&source));
    let contract = Contract::new("resource-fixture".into(), "shard".into(), destination.into(), schema.into()).unwrap();
    let prepared = restored.delivery().prepare(&identity.observation_id, contract.clone(), vec![]).await.unwrap();
    let store = FileRecoveryStore::create(&root.path().join("archive"), contract).unwrap();
    let owner = store.acquire().await.unwrap();
    let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    let mut objects = BTreeMap::new();
    for batch in &committed.manifest().batches {
        objects.insert(batch.object.clone(), std::fs::read(prepared.batch_path(batch)).unwrap());
    }
    Recovery::from_objects(committed.manifest().clone(), objects)
}
