//! Recovery scheduling and publication obligations, separate from shared read and mutation capabilities.
use crate::delivery::evidence::PreparedRange;
use async_trait::async_trait;
use culprit_core::{Result, telemetry::DeliveryProblem};
use culprit_evidence::delivery::heads::DiagnosticHeadPage;
use culprit_evidence::delivery::{mutations::*, recovery::*};
#[cfg(test)]
mod tests;
#[async_trait]
pub trait RecoveryStore: RecoveryWrite {
    async fn checkpoint_head(&self) -> Result<Option<String>>;
    /// A complete committed capture of this exact native range. Never inferred from a latest-N scan.
    async fn captured_range(
        &self,
        range: &culprit_core::telemetry::CapturedRange,
    ) -> Result<Option<culprit_core::telemetry::CaptureIdentity>>;

    async fn recover(&self, cursor: Option<&str>, limit: u32) -> Result<RecoveryPage>;
    /// Cold recovery enumerates unpublished work, rather than replaying the entire retained history.
    async fn pending_ranges(&self, cursor: Option<&str>, limit: u32, order: RecoveryOrder) -> Result<RecoveryPage>;
    /// Native inputs still lacking their initial diagnostic checkpoint, including already-published captures.
    async fn pending_diagnostic_inputs(&self, cursor: Option<&str>, limit: u32) -> Result<RecoveryPage>;
    /// Exact current obligation membership for an authenticated native capture. Sequence maxima prove no absence.
    async fn diagnostic_input_pending(
        &self,
        input: &culprit_core::telemetry::DiagnosticCaptureRef,
        controller_version: u32,
    ) -> Result<bool>;
    async fn retire(&self, fence: &Fence, publication: &super::sink::PublishedRange) -> Result<()>;
    /// Latest full diagnostic working set per target and controller scope.
    async fn diagnostic_heads(&self, cursor: Option<&str>, limit: u32) -> Result<DiagnosticHeadPage>;
    /// Complete membership in this store's committed original-group index, independent of capture identity.
    /// Every selected key has an explicit present/absent result from one validated root snapshot. Pointers remain
    /// hints: their immutable bodies and exact receipt positions must be authenticated before reuse.
    async fn diagnostic_original_groups(
        &self,
        keys: &[String],
    ) -> Result<culprit_evidence::delivery::originals::OriginalHistoryLookup>;

    /// One-key convenience uses the same complete selected lookup; it never falls back to a history walk.
    async fn diagnostic_original_group(
        &self,
        key: &str,
    ) -> Result<Option<culprit_evidence::delivery::originals::OriginalHistoryPointer>> {
        let keys = [key.to_owned()];
        self.diagnostic_original_groups(&keys).await?.into_one(key)
    }
}
pub async fn stage_and_commit(
    store: &dyn RecoveryStore,
    fence: &Fence,
    expected_head: Option<&str>,
    prepared: &PreparedRange,
) -> Result<CommittedRange> {
    let manifest = prepared.manifest();
    manifest.validate()?;
    if store.id() != fence.store_id() {
        return Err(DeliveryProblem::Fenced.error());
    }
    let mut staged = Vec::with_capacity(manifest.batches.len());
    for batch in &manifest.batches {
        staged.push(stage_batch(store, batch, &prepared.batch_path(batch)).await?);
    }
    commit_staged(store, fence, expected_head, prepared.manifest.clone(), &staged).await
}
