//! Read-only committed recovery capabilities. No local journal, source or scheduler is linked here.
use super::evidence::*;
pub use super::object::RecoveryRecords;
use async_trait::async_trait;
use culprit_core::{Result, telemetry::DeliveryProblem};
use std::{pin::Pin, sync::Arc};
/// The recovery provider owns transport; common consumers bound and verify the stream at EOF.
pub type RecoveryReader = Pin<Box<dyn tokio::io::AsyncRead + Send>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadObject {
    pub object: String,
    pub sha256: String,
    pub bytes: u64,
}
impl From<&BatchManifest> for PayloadObject {
    fn from(batch: &BatchManifest) -> Self {
        Self { object: batch.object.clone(), sha256: batch.sha256.clone(), bytes: batch.bytes }
    }
}

#[async_trait]
pub trait RecoveryRead: Send + Sync {
    fn id(&self) -> &str;
    async fn load(&self, range_id: &str) -> Result<RangeManifest>;
    async fn capture_manifest(&self, capture: &culprit_core::telemetry::CaptureIdentity) -> Result<RangeManifest>;
    async fn open_object(&self, batch: &PayloadObject) -> Result<RecoveryReader>;
}
/// Deliberately not deserializable: reopening a journal requires asking the recovery store for the committed manifest.
#[derive(Clone)]
pub struct CommittedRange {
    manifest: Arc<RangeManifest>,
    store_id: String,
    digest: String,
}
impl CommittedRange {
    pub(crate) fn acknowledged(manifest: Arc<RangeManifest>, store_id: String, digest: String) -> Self {
        Self { manifest, store_id, digest }
    }
    pub fn manifest(&self) -> &RangeManifest {
        &self.manifest
    }
    pub fn store_id(&self) -> &str {
        &self.store_id
    }
    pub fn manifest_digest(&self) -> &str {
        &self.digest
    }
}
pub async fn reopen(store: &dyn RecoveryRead, range_id: &str) -> Result<CommittedRange> {
    let manifest = store.load(range_id).await?;
    manifest.validate()?;
    if manifest.range_id != range_id {
        return Err(DeliveryProblem::Integrity.error());
    }
    let digest = manifest.digest()?;
    Ok(CommittedRange { manifest: Arc::new(manifest), store_id: store.id().into(), digest })
}

/// Commit order controls scheduling only; it never changes native source ordering or identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOrder {
    OldestFirst,
    NewestFirst,
}
#[derive(Clone)]
pub struct RecoveryPage {
    pub ranges: Vec<RangeManifest>,
    pub next_cursor: Option<String>,
    pub high_sequence: u64,
}
