//! Manufactured immutable warehouse originals for read-only conformance; no collector or native work.
use super::*;
use crate::{
    analytical::{
        ProjectionCatalog, RowProjection,
        catalog::PhysicalCatalog,
        envelope::Envelope,
        indexes::{ExpectedRow, IndexKey, ProjectionIndexWriter},
        projection, publication,
        read_store::{AnalyticalReadPage, AnalyticalReadStore},
    },
    delivery::{
        object_store::{
            ObjectRecoveryReader,
            state::{CommitIndex, Root},
            transport::ObjectReadTransport,
        },
        recovery::{CommittedRange, reopen},
    },
};
use serde_json::{Map, Value};
use std::sync::Arc;

pub struct ReadOnlyFixture {
    pub contract: Contract,
    pub manifest: RangeManifest,
    pub objects: Arc<Blobs>,
    pub store: Arc<ReadOnlyStore>,
}
pub struct ReadOnlyStore {
    catalog: Arc<PhysicalCatalog<JsonProjection>>,
    rows: BTreeMap<String, Vec<Vec<Value>>>,
    publication: Vec<Value>,
}
impl ReadOnlyFixture {
    pub async fn capture(source: CollectionDescription, records: Vec<TelemetryRecord>) -> Self {
        Self::capture_at(source, records, 770_001, "2020-01-01T12:00:00Z", "2020-01-01T12:00:01Z").await
    }
    /// Independent originals supply their sequence and acquisition clocks before immutable proofs are created.
    pub async fn capture_at(
        source: CollectionDescription,
        records: Vec<TelemetryRecord>,
        sequence: u64,
        collected_from: &str,
        collected_until: &str,
    ) -> Self {
        let catalog = Arc::new(PhysicalCatalog::<JsonProjection>::new().unwrap());
        let native = Recovery::capture("boundary:warehouse", catalog.digest(), source, records, sequence);
        let mut manifest = native.manifest;
        manifest.collected_from = collected_from.into();
        manifest.collected_until = collected_until.into();
        manifest.validate().unwrap();
        let objects = Arc::new(Blobs::default());
        let mut root = Root::new(manifest.contract.clone()).unwrap();
        root.epoch = 1;
        root.token = Some("controlled-writer".into());
        let high_sequence = manifest.capture.revision.as_ref().unwrap().sequence.parse().unwrap();
        let index = CommitIndex {
            store_id: root.store_id.clone(),
            order: 1,
            range_id: manifest.range_id.clone(),
            manifest_digest: manifest.digest().unwrap(),
            high_sequence,
            previous: None,
        };
        root.high_sequence = high_sequence;
        root.indexed_order = 1;
        root.head = Some(index.clone());
        root.validate(&manifest.contract).unwrap();
        {
            let mut stored = objects.objects.lock().unwrap();
            stored.insert("recovery.json".into(), serde_json::to_vec(&root).unwrap());
            stored.insert(index.manifest_key(), serde_json::to_vec(&manifest).unwrap());
            stored.insert(index.range_key(), serde_json::to_vec(&index).unwrap());
            stored.insert(index.order_key(), serde_json::to_vec(&index).unwrap());
            stored.extend(native.objects);
        }
        // Every store-bound proof uses the same committed namespace as the actual read-only owner.
        let recovery = ObjectRecoveryReader::open(objects.clone(), manifest.contract.clone()).await.unwrap();
        let range = reopen(&recovery, &manifest.range_id).await.unwrap();
        let indexes = ProjectionIndexWriter::new(objects.clone());
        let mut rows = BTreeMap::new();
        for batch in &range.manifest().batches {
            if !batch.delivery_required {
                continue;
            }
            let schema = catalog.get(batch.stream).unwrap();
            let bytes = objects.objects.lock().unwrap().get(&batch.object).unwrap().clone();
            let native = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice::<ExportedRecord>(line).unwrap())
                .collect::<Vec<_>>();
            let envelope = Envelope::committed(&range, &batch.batch_id).unwrap();
            let projected = native
                .iter()
                .enumerate()
                .map(|(i, row)| schema.project(&envelope.input(i + 1, row).unwrap()).unwrap())
                .collect::<Vec<_>>();
            let digests = projected
                .iter()
                .map(|row| {
                    ExpectedRow::new(projection::digest(row).unwrap(), schema.encode(row).unwrap().len()).unwrap()
                })
                .collect::<Vec<_>>();
            indexes
                .put(&IndexKey::for_batch(&range, &batch.batch_id, schema.digest()).unwrap(), &digests)
                .await
                .unwrap();
            let returned = projected.iter().map(|row| physical_values(schema, row)).collect();
            rows.insert(batch.batch_id.clone(), returned);
        }
        let publication =
            catalog.publication().project(&publication::input(catalog.as_ref(), &range).unwrap()).unwrap();
        let publication = physical_values(catalog.publication(), &publication);
        Self {
            contract: manifest.contract.clone(),
            manifest,
            objects,
            store: Arc::new(ReadOnlyStore { catalog, rows, publication }),
        }
    }
    pub fn transport(&self) -> Arc<dyn ObjectReadTransport> {
        self.objects.clone()
    }
}
// The query result contains physical cells, not the logical input used to compute its immutable projection digest.
fn physical_values(schema: &JsonProjection, projected: &[Value]) -> Vec<Value> {
    let object: Map<String, Value> = serde_json::from_slice(&schema.encode(projected).unwrap()).unwrap();
    schema.columns().iter().map(|column| object[&column.name].clone()).collect()
}

#[async_trait]
impl AnalyticalReadStore for ReadOnlyStore {
    fn destination(&self) -> &str {
        "boundary:warehouse"
    }
    fn catalog(&self) -> Arc<dyn ProjectionCatalog> {
        self.catalog.clone()
    }
    fn logical_values(&self, stream: TelemetryStream, row: &[Value]) -> Result<Map<String, Value>> {
        self.catalog.get(stream)?.logical_values(row)
    }
    async fn audit_membership(&self, _: &CommittedRange, _: usize) -> Result<usize> {
        Ok(2)
    }
    async fn rows(
        &self,
        _: &CommittedRange,
        batch: &str,
        after: usize,
        until: usize,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        let rows = self.rows.get(batch).unwrap()[after..until].to_vec();
        let response_bytes = serde_json::to_vec(&rows).unwrap().len();
        Ok(AnalyticalReadPage { rows, response_bytes })
    }
    async fn publication_rows(&self, _: &CommittedRange, _: usize) -> Result<AnalyticalReadPage> {
        let rows = vec![self.publication.clone()];
        let response_bytes = serde_json::to_vec(&rows).unwrap().len();
        Ok(AnalyticalReadPage { rows, response_bytes })
    }
    async fn fleet_candidates(
        &self,
        contract: &Contract,
        request: &crate::analytical::evidence::FleetResourceQuery,
        _: usize,
    ) -> Result<AnalyticalReadPage> {
        request.validate(contract)?;
        let values = self.catalog.publication().logical_values(&self.publication)?;
        let include = values.get("ContractId").and_then(Value::as_str) == Some(&contract.id()?)
            && values.get("SchemaDigest").and_then(Value::as_str) == Some(contract.schema_digest())
            && values.get("RecoveryStoreId").and_then(Value::as_str) == Some(request.recovery_store_id.as_str())
            && values.get("ResourceKind").and_then(Value::as_str) == Some("database")
            && values
                .get("CollectedFrom")
                .and_then(Value::as_str)
                .zip(values.get("CollectedUntil").and_then(Value::as_str))
                .is_some_and(|(from, until)| request.selection.accepts_acquisition(from, until))
            && request.selection.after.as_ref().is_none_or(|cursor| {
                values.get("ResourceId").and_then(Value::as_str).is_some_and(|id| id > cursor.resource_id.as_str())
            });
        let rows = if include { vec![self.publication.clone()] } else { vec![] };
        let response_bytes = serde_json::to_vec(&rows).unwrap().len();
        Ok(AnalyticalReadPage { rows, response_bytes })
    }
}
