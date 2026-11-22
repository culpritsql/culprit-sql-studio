//! Existing fenced recovery writer, composed over the sole immutable recovery reader.
use culprit_evidence::delivery::{mutations::*, recovery::*};
pub mod provider;
mod recovery;
#[cfg(test)]
pub(crate) mod tests;
use super::recovery::*;
use culprit_core::{
    Result,
    telemetry::{DeliveryProblem, digest},
};
use culprit_evidence::delivery::{
    contract::*,
    coverage::coverage_key,
    evidence::*,
    object_store::{ObjectRecoveryReader, encode, read_bytes, state::*, transport::*, validate_payload},
};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc};
use tokio::io::AsyncReadExt;
fn integrity() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
fn transport_error() -> culprit_core::Error {
    DeliveryProblem::Transport.error()
}
pub struct ObjectRecoveryStore {
    reader: ObjectRecoveryReader,
    transport: Arc<dyn ObjectTransport>,
}
impl std::ops::Deref for ObjectRecoveryStore {
    type Target = ObjectRecoveryReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}
impl ObjectRecoveryStore {
    /// Explicit provisioning. Opening a missing namespace never silently creates a replacement history.
    pub async fn create(transport: Arc<dyn ObjectTransport>, contract: Contract) -> Result<Self> {
        let root = Root::new(contract.clone())?;
        let bytes = encode(&root, MAX_ROOT_BYTES)?;
        if !matches!(transport.put(ROOT_KEY, bytes, WriteCondition::Absent).await?, ObjectWrite::Written) {
            return Err(DeliveryProblem::Conflict.error());
        }
        Ok(Self {
            reader: ObjectRecoveryReader::from_namespace(transport.clone(), contract, root.store_id)?,
            transport,
        })
    }

    async fn replace_root(&self, root: &Root, etag: &str) -> Result<()> {
        root.validate(self.reader.contract())?;
        let bytes = encode(root, MAX_ROOT_BYTES)?;
        match self.transport.put(ROOT_KEY, bytes, WriteCondition::Matches(etag)).await? {
            ObjectWrite::Written => Ok(()),
            ObjectWrite::ConditionNotMet => Err(DeliveryProblem::Conflict.error()),
        }
    }
    /// Call once during explicit owner startup. There is no lease-expiry takeover or automatic reacquisition.
    pub async fn acquire(&self) -> Result<Fence> {
        let (mut root, etag) = self.root().await?;
        root.epoch = root.epoch.checked_add(1).ok_or_else(integrity)?;
        let token = crate::durable_fs::random_id()?;
        root.token = Some(token.clone());
        self.replace_root(&root, &etag).await?;
        Fence::issued(root.epoch, token, self.reader.id().to_owned())
    }
    pub async fn checkpoint(&self, key: &str) -> Result<Option<String>> {
        Ok(self.root().await?.0.checkpoints.get(key).cloned())
    }
    async fn immutable(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let expected = digest(&bytes);
        let count = bytes.len();
        match self.transport.put(key, bytes, WriteCondition::Absent).await? {
            ObjectWrite::Written => Ok(()),
            ObjectWrite::ConditionNotMet => {
                let value = self.transport.get(key).await?.ok_or_else(integrity)?;
                let existing = read_bytes(value, count).await?;
                if existing.len() != count || digest(&existing) != expected {
                    return Err(integrity());
                }
                Ok(())
            }
        }
    }
    async fn verify_payload(&self, batch: &PayloadObject) -> Result<()> {
        validate_payload(batch)?;
        let value = self.transport.get(&batch.object).await?.ok_or_else(integrity)?;
        if value.bytes != batch.bytes {
            return Err(integrity());
        }
        let mut reader = value.body;
        let mut hash = Sha256::new();
        let mut total = 0u64;
        let mut buffer = vec![0u8; culprit_evidence::delivery::object::RECOVERY_CHUNK_BYTES];
        loop {
            let count = reader.read(&mut buffer).await.map_err(|_| transport_error())?;
            if count == 0 {
                break;
            }
            total = total.checked_add(count as u64).filter(|total| *total <= batch.bytes).ok_or_else(integrity)?;
            hash.update(&buffer[..count]);
        }
        let hash = hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        if total != batch.bytes || hash != batch.sha256 {
            return Err(integrity());
        }
        Ok(())
    }
    async fn repair_indexes(&self, root: &Root) -> Result<()> {
        let Some(head) = &root.head else {
            return Ok(());
        };
        if root.indexed_order == head.order {
            return Ok(());
        }
        let bytes = encode(head, MAX_INDEX_BYTES)?;
        self.immutable(&head.range_key(), bytes.clone()).await?;
        self.immutable(&head.order_key(), bytes.clone()).await?;
        let manifest = self.manifest(head).await?;
        let range = manifest.source.captured_range()?;
        let Some(key) = coverage_key(&range)? else {
            return Ok(());
        };
        let key = format!("coverage/{key}.json");
        match self.transport.put(&key, bytes, WriteCondition::Absent).await? {
            ObjectWrite::Written => Ok(()),
            ObjectWrite::ConditionNotMet => {
                let existing = self.index(&key, root).await?.ok_or_else(integrity)?;
                self.capture_for_index(&existing, &range, root).await.map(|_| ())
            }
        }
    }

    pub async fn open(transport: Arc<dyn ObjectTransport>, contract: Contract) -> Result<Self> {
        let reader = ObjectRecoveryReader::open(transport.clone(), contract).await?;
        Ok(Self { reader, transport })
    }
}
#[cfg(feature = "test-support")]
pub mod test_support;
