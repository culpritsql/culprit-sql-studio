//! Exact committed-root/index/manifest reading; no provisioning, acquisition, PUT or scheduler capability.
pub mod bridge;
#[doc(hidden)]
pub mod state;
pub mod transport;
use super::{contract::*, coverage::coverage_key, evidence::*, recovery::*};
use async_trait::async_trait;
use culprit_core::{
    Result,
    telemetry::{DeliveryProblem, digest},
};
use serde::{Serialize, de::DeserializeOwned};
use state::*;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use transport::*;
fn integrity() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
fn transport_error() -> culprit_core::Error {
    DeliveryProblem::Transport.error()
}
pub struct ObjectRecoveryReader {
    id: String,
    contract: Contract,
    transport: Arc<dyn ObjectReadTransport>,
}
impl ObjectRecoveryReader {
    /// Constructing a reference proves no remote state. Each read validates the current authoritative root.
    pub fn from_namespace(
        transport: Arc<dyn ObjectReadTransport>,
        contract: Contract,
        store_id: String,
    ) -> Result<Self> {
        contract.validate()?;
        if !valid_digest(&store_id) {
            return Err(integrity());
        }
        Ok(Self { id: store_id, contract, transport })
    }
    pub fn contract(&self) -> &Contract {
        &self.contract
    }
    pub async fn open(transport: Arc<dyn ObjectReadTransport>, contract: Contract) -> Result<Self> {
        contract.validate()?;
        let (root, _) = read_json::<Root>(&*transport, ROOT_KEY, MAX_ROOT_BYTES)
            .await?
            .ok_or_else(|| DeliveryProblem::NotReady.error())?;
        root.validate(&contract)?;
        Ok(Self { id: root.store_id, contract, transport })
    }
    pub async fn root(&self) -> Result<(Root, String)> {
        let (root, etag) =
            read_json::<Root>(&*self.transport, ROOT_KEY, MAX_ROOT_BYTES).await?.ok_or_else(integrity)?;
        root.validate(&self.contract)?;
        if root.store_id != self.id {
            return Err(integrity());
        }
        Ok((root, etag))
    }
    pub async fn index(&self, key: &str, root: &Root) -> Result<Option<CommitIndex>> {
        let index = read_json::<CommitIndex>(&*self.transport, key, MAX_INDEX_BYTES).await?.map(|(index, _)| index);
        if let Some(index) = &index {
            index.validate(&self.id)?;
            if root.head.as_ref().is_none_or(|head| index.order > head.order) {
                return Err(integrity());
            }
        }
        Ok(index)
    }
    pub async fn manifest(&self, index: &CommitIndex) -> Result<RangeManifest> {
        let value = self.transport.get(&index.manifest_key()).await?.ok_or_else(integrity)?;
        let bytes = read_bytes(value, MAX_MANIFEST_BYTES).await?;
        if digest(&bytes) != index.manifest_digest {
            return Err(integrity());
        }
        let manifest: RangeManifest = serde_json::from_slice(&bytes).map_err(|_| integrity())?;
        manifest.validate()?;
        if manifest.contract != self.contract || manifest.range_id != index.range_id {
            return Err(integrity());
        }
        Ok(manifest)
    }
    pub async fn committed_index(&self, range_id: &str, root: &Root) -> Result<Option<CommitIndex>> {
        let Some(index) = self.index(&format!("ranges/{range_id}.json"), root).await? else {
            return Ok(root.head.as_ref().filter(|head| head.range_id == range_id).cloned());
        };
        if index.range_id != range_id {
            return Err(integrity());
        }
        Ok(Some(index))
    }
    pub async fn capture_for_index(
        &self,
        index: &CommitIndex,
        requested: &culprit_core::telemetry::CapturedRange,
        root: &Root,
    ) -> Result<culprit_core::telemetry::CaptureIdentity> {
        let committed = self.committed_index(&index.range_id, root).await?.ok_or_else(integrity)?;
        if &committed != index {
            return Err(integrity());
        }
        let manifest = self.manifest(index).await?;
        if coverage_key(&manifest.source.captured_range()?)? != coverage_key(requested)? {
            return Err(integrity());
        }
        Ok(manifest.capture)
    }
}
#[async_trait]
impl RecoveryRead for ObjectRecoveryReader {
    fn id(&self) -> &str {
        &self.id
    }
    async fn load(&self, range_id: &str) -> Result<RangeManifest> {
        if !valid_digest(range_id) {
            return Err(integrity());
        }
        let (root, _) = self.root().await?;
        let index = self.committed_index(range_id, &root).await?.ok_or_else(|| DeliveryProblem::NotReady.error())?;
        self.manifest(&index).await
    }
    async fn capture_manifest(&self, capture: &culprit_core::telemetry::CaptureIdentity) -> Result<RangeManifest> {
        let manifest = self.load(&range_id_for(&self.contract, capture)?).await?;
        if &manifest.capture != capture {
            return Err(integrity());
        }
        Ok(manifest)
    }
    async fn open_object(&self, batch: &PayloadObject) -> Result<RecoveryReader> {
        validate_payload(batch)?;
        let value = self.transport.get(&batch.object).await?.ok_or_else(integrity)?;
        if value.bytes != batch.bytes {
            return Err(integrity());
        }
        // The common RecoveryRead reader checks the complete length and digest at EOF.
        Ok(value.body)
    }
}
pub fn validate_payload(batch: &PayloadObject) -> Result<()> {
    if !valid_digest(&batch.sha256)
        || batch.object != format!("payloads/{}.ndjson", batch.sha256)
        || batch.bytes > MAX_RANGE_BYTES
    {
        return Err(integrity());
    }
    Ok(())
}
pub fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| integrity())?;
    if bytes.len() > limit {
        return Err(DeliveryProblem::Capacity.error());
    }
    Ok(bytes)
}
pub async fn read_json<T: DeserializeOwned>(
    transport: &dyn ObjectReadTransport,
    key: &str,
    limit: usize,
) -> Result<Option<(T, String)>> {
    let Some(value) = transport.get(key).await? else {
        return Ok(None);
    };
    if value.etag.is_empty() || value.etag.len() > 256 || value.etag.bytes().any(|b| b.is_ascii_control()) {
        return Err(integrity());
    }
    let etag = value.etag.clone();
    let bytes = read_bytes(value, limit).await?;
    let parsed = serde_json::from_slice(&bytes).map_err(|_| integrity())?;
    Ok(Some((parsed, etag)))
}
pub async fn read_bytes(mut value: ObjectRead, limit: usize) -> Result<Vec<u8>> {
    if value.bytes > limit as u64 {
        return Err(DeliveryProblem::Capacity.error());
    }
    let mut bytes = Vec::new();
    (&mut value.body).take(value.bytes + 1).read_to_end(&mut bytes).await.map_err(|_| transport_error())?;
    if bytes.len() as u64 != value.bytes {
        return Err(integrity());
    }
    Ok(bytes)
}
