//! Explicit recovery mutation capability; read-only callers receive only RecoveryRead.
use super::{contract::*, evidence::*, recovery::*};
use async_trait::async_trait;
use culprit_core::{Result, telemetry::DeliveryProblem};
use std::{path::Path, sync::Arc};
/// Returned by the recovery provider. Implementations must fence every authoritative mutation against this epoch.
#[derive(Clone)]
pub struct Fence {
    epoch: u64,
    token: String,
    store_id: String,
}
impl Fence {
    pub fn issued(epoch: u64, token: String, store_id: String) -> Result<Self> {
        if epoch == 0 || !bounded_id(&token) || !valid_digest(&store_id) {
            return Err(DeliveryProblem::Integrity.error());
        }
        Ok(Self { epoch, token, store_id })
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn store_id(&self) -> &str {
        &self.store_id
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedBatch {
    pub store_id: String,
    pub object: String,
    pub sha256: String,
    pub bytes: u64,
}
#[derive(Clone)]
pub struct CommitReceipt {
    pub store_id: String,
    pub manifest_digest: String,
    pub head: String,
}

#[async_trait]
pub trait RecoveryWrite: RecoveryRead {
    async fn stage(&self, batch: &PayloadObject, path: &Path) -> Result<StagedBatch>;
    async fn commit(
        &self,
        fence: &Fence,
        expected_head: Option<&str>,
        manifest: &RangeManifest,
        staged: &[StagedBatch],
    ) -> Result<CommitReceipt>;
}
pub async fn stage_batch(store: &dyn RecoveryWrite, batch: &BatchManifest, path: &Path) -> Result<StagedBatch> {
    let receipt = store.stage(&PayloadObject::from(batch), path).await?;
    check_staged(store.id(), batch, &receipt)?;
    Ok(receipt)
}
fn check_staged(store: &str, batch: &BatchManifest, receipt: &StagedBatch) -> Result<()> {
    if receipt.store_id != store
        || receipt.object != batch.object
        || receipt.sha256 != batch.sha256
        || receipt.bytes != batch.bytes
    {
        return Err(DeliveryProblem::Integrity.error());
    }
    Ok(())
}
pub async fn commit_staged(
    store: &dyn RecoveryWrite,
    fence: &Fence,
    expected_head: Option<&str>,
    manifest: Arc<RangeManifest>,
    staged: &[StagedBatch],
) -> Result<CommittedRange> {
    manifest.validate()?;
    if store.id() != fence.store_id() {
        return Err(DeliveryProblem::Fenced.error());
    }
    if staged.len() != manifest.batches.len() {
        return Err(DeliveryProblem::Integrity.error());
    }
    for (batch, receipt) in manifest.batches.iter().zip(staged) {
        check_staged(store.id(), batch, receipt)?;
    }
    let digest = manifest.digest()?;
    let receipt = store.commit(fence, expected_head, &manifest, staged).await?;
    if receipt.store_id != store.id() || receipt.manifest_digest != digest || receipt.head != manifest.range_id {
        return Err(DeliveryProblem::Integrity.error());
    }
    Ok(CommittedRange::acknowledged(manifest, receipt.store_id, digest))
}
