//! The existing one-root/index/manifest loader and metadata allowance. Absence is a value ONLY after a valid root.
use super::*;
use culprit_evidence::delivery::recovery::reopen;
use std::sync::Arc;

/// An exact original could not be located as committed in this selected recovery namespace. This is not a
/// fabricated source header, proof of deletion, native recovery, or permission to substitute a current capture.
#[derive(Debug)]
pub enum EvidenceMetadataLookup {
    Available(Box<EvidenceCaptureMetadata>),
    NotCommitted,
}

pub(super) struct OriginalRead<'a> {
    service: &'a EvidenceService,
    recovery: ObjectRecoveryReader,
    range: Option<CommittedRange>,
    accounting: Arc<MetadataAccounting>,
    budget: EvidenceReadBudget,
    fence: ReadFence,
}
impl<'a> OriginalRead<'a> {
    pub(super) async fn open(
        service: &'a EvidenceService,
        range_id: &str,
        budget: EvidenceReadBudget,
        fence: ReadFence,
    ) -> Result<Self> {
        let accounting = MetadataAccounting::new(
            service.recovery.clone(),
            budget.max_queries,
            budget.max_proof_bytes,
            fence.clone(),
        );
        // Missing/unvalidated root, transport, permission and integrity failures remain errors.
        let recovery = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
        let range = match reopen(&recovery, range_id).await {
            Ok(range) => Some(range),
            Err(error) if error.detail == Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::NotReady)) => None,
            Err(error) => return Err(error),
        };
        fence.check()?;
        Ok(Self { service, recovery, range, accounting, budget, fence })
    }
    pub(super) async fn read<T, F, Reading>(self, read: F) -> Result<(T, SessionUsage)>
    where
        F: FnOnce(&'a EvidenceService, CommittedRange, ObjectRecoveryReader, EvidenceReadBudget) -> Reading,
        Reading: Future<Output = Result<(T, EvidenceReadMetrics)>>,
    {
        let mut budget = self.budget;
        budget.max_proof_bytes = budget
            .max_proof_bytes
            .checked_sub(self.accounting.work()?.bytes)
            .filter(|remaining| *remaining > 0)
            .ok_or_else(capacity)?;
        self.fence.check()?;
        let range = self.range.ok_or_else(|| DeliveryProblem::NotReady.error())?;
        let (result, mut metrics) = read(self.service, range, self.recovery, budget).await?;
        metrics.proof_bytes = metrics.proof_bytes.checked_add(self.accounting.work()?.bytes).ok_or_else(capacity)?;
        Ok((result, SessionUsage { metrics, candidate_rows: 0 }))
    }
    pub(super) async fn metadata(
        self,
        reference: &DiagnosticCaptureRef,
    ) -> Result<(EvidenceMetadataLookup, SessionUsage)> {
        if self.range.is_none() {
            self.fence.check()?;
            let metrics = EvidenceReadMetrics { proof_bytes: self.accounting.work()?.bytes, ..Default::default() };
            return Ok((EvidenceMetadataLookup::NotCommitted, SessionUsage { metrics, candidate_rows: 0 }));
        }
        self.read(|service, range, _recovery, budget| async move {
            let metadata = service.reader.read_metadata(&range, budget).await?;
            if metadata.capture() != &reference.capture
                || metadata.capture_manifest_sha256() != reference.manifest_sha256
                || metadata.source().captured_range()?.scope_id != reference.source_scope_id
            {
                return Err(DeliveryProblem::Integrity.error());
            }
            let metrics = metadata.metrics().clone();
            Ok((EvidenceMetadataLookup::Available(Box::new(metadata)), metrics))
        })
        .await
    }
}
