//! Metadata transport accounting wraps the existing verifier; it neither loads nor interprets an original.
use super::ReadFence;
use culprit_core::{Result, telemetry::DeliveryProblem};
use culprit_evidence::delivery::object_store::transport::{ObjectRead, ObjectReadTransport};
use std::sync::{Arc, Mutex};

/// An ordinary authenticated metadata read uses open/root/index/manifest, before its warehouse proof query.
const METADATA_READS_PER_QUERY: u32 = 4;
#[derive(Clone, Copy, Default)]
pub(in crate::evidence) struct MetadataWork {
    pub reads: u32,
    pub bytes: u64,
}
pub(in crate::evidence) struct MetadataAccounting {
    transport: Arc<dyn ObjectReadTransport>,
    max_reads: u32,
    max_bytes: u64,
    work: Mutex<MetadataWork>,
    fence: ReadFence,
}
impl MetadataAccounting {
    pub fn new(transport: Arc<dyn ObjectReadTransport>, queries: u32, proof_bytes: u64, fence: ReadFence) -> Arc<Self> {
        Arc::new(Self {
            transport,
            max_reads: queries.saturating_mul(METADATA_READS_PER_QUERY),
            max_bytes: proof_bytes,
            work: Mutex::new(MetadataWork::default()),
            fence,
        })
    }
    pub fn work(&self) -> Result<MetadataWork> {
        self.work.lock().map(|work| *work).map_err(|_| capacity())
    }
    fn admit(&self) -> Result<()> {
        let mut work = self.work.lock().map_err(|_| capacity())?;
        work.reads = work.reads.checked_add(1).filter(|reads| *reads <= self.max_reads).ok_or_else(capacity)?;
        Ok(())
    }
    fn charge(&self, bytes: u64) -> Result<()> {
        let mut work = self.work.lock().map_err(|_| capacity())?;
        work.bytes = work.bytes.checked_add(bytes).filter(|bytes| *bytes <= self.max_bytes).ok_or_else(capacity)?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl ObjectReadTransport for MetadataAccounting {
    async fn get(&self, key: &str) -> Result<Option<ObjectRead>> {
        // Native payload EOF/length/digest and bytes already have their separate explicit recovery allowance.
        // Projection indexes are read by ProjectionIndexes, not this metadata transport.
        self.fence.check()?;
        if key.starts_with("payloads/") {
            let result = self.transport.get(key).await?;
            self.fence.check()?;
            return Ok(result);
        }
        self.admit()?;
        let value = self.transport.get(key).await?;
        self.fence.check()?;
        if let Some(value) = &value {
            self.charge(value.bytes)?;
        }
        // The existing read_bytes owner checks actual length and EOF before success. A rejected/failed action
        // loses the whole remaining allowance, including requests whose transport did not return metrics.
        Ok(value)
    }
}
fn capacity() -> culprit_core::Error {
    DeliveryProblem::Capacity.error()
}

#[cfg(test)]
mod tests;
