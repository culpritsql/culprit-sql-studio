mod metadata;
use super::{EvidenceRequest, EvidenceService};
use culprit_core::{
    Result,
    telemetry::{CollectionStop, DeliveryProblem, DiagnosticCaptureRef, valid_digest},
};
use culprit_evidence::analytical::evidence::EvidenceCapture;
use culprit_evidence::analytical::evidence::EvidenceCaptureMetadata;
use culprit_evidence::analytical::evidence::EvidenceFamily;
use culprit_evidence::analytical::evidence::EvidenceReadBudget;
use culprit_evidence::analytical::evidence::EvidenceReadMetrics;
use culprit_evidence::analytical::evidence::IncidentDiscoveryPage;
use culprit_evidence::analytical::evidence::IncidentDiscoveryRequest;
use culprit_evidence::delivery::evidence::range_id_for;
use culprit_evidence::delivery::object_store::ObjectRecoveryReader;
use culprit_evidence::delivery::recovery::CommittedRange;
use culprit_evidence::delivery::recovery::reopen;
use culprit_read::cancellation::CollectionCancellation;
pub(super) use metadata::MetadataAccounting;
use std::{future::Future, time::Duration};
use tokio::time::Instant;

/// One caller action owns the cumulative warehouse/recovery allowance and an absolute deadline.
/// Transports retain their own lifecycle limits; dropping a read does not prove remote cancellation.
pub struct EvidenceSession<'a> {
    service: &'a EvidenceService,
    cancellation: &'a CollectionCancellation,
    deadline: Instant,
    remaining: Option<EvidenceReadBudget>,
    metrics: EvidenceReadMetrics,
}

impl<'a> EvidenceSession<'a> {
    pub(super) fn new(
        service: &'a EvidenceService,
        budget: EvidenceReadBudget,
        cancellation: &'a CollectionCancellation,
    ) -> Result<Self> {
        budget.validate()?;
        Ok(Self {
            service,
            cancellation,
            deadline: Instant::now() + Duration::from_millis(u64::from(budget.timeout_ms)),
            remaining: Some(budget),
            metrics: EvidenceReadMetrics::default(),
        })
    }

    pub(super) fn completed_metrics(&self) -> &EvidenceReadMetrics {
        &self.metrics
    }

    pub async fn read(&mut self, range_id: &str, families: &[EvidenceFamily]) -> Result<EvidenceCapture> {
        self.read_selected(range_id, families, None).await
    }

    /// Resolve the immutable range from the selected contract and original capture. Callers never supply a companion
    /// range SHA or substitute a current source. Pointer mismatch consumes this action just like any other failed read.
    pub async fn read_reference(
        &mut self,
        reference: &DiagnosticCaptureRef,
        families: &[EvidenceFamily],
    ) -> Result<EvidenceCapture> {
        let range_id = range_id_for(&self.service.contract, &reference.capture)?;
        self.read_selected(&range_id, families, Some(reference)).await
    }

    async fn read_selected(
        &mut self,
        range_id: &str,
        families: &[EvidenceFamily],
        expected: Option<&DiagnosticCaptureRef>,
    ) -> Result<EvidenceCapture> {
        self.read_with(
            range_id,
            |budget| {
                if expected.is_some_and(|reference| !reference.is_valid()) {
                    return Err(DeliveryProblem::InvalidContract.error());
                }
                EvidenceRequest { range_id: range_id.into(), families: families.to_vec(), budget }.validate()
            },
            |service, range, recovery, budget| async move {
                let capture = service.reader.read_capture(&range, families, budget, Some(&recovery)).await?;
                if let Some(reference) = expected {
                    crate::collected_resources::references::validate_capture_reference(&capture, reference)?;
                }
                let metrics = capture.metrics().clone();
                Ok((capture, metrics))
            },
        )
        .await
    }

    /// Exact source-only links retain their original header; no body or native fact is inferred from this read.
    pub async fn read_metadata_reference(
        &mut self,
        reference: &DiagnosticCaptureRef,
    ) -> Result<EvidenceCaptureMetadata> {
        let range_id = range_id_for(&self.service.contract, &reference.capture)?;
        self.read_with(
            &range_id,
            |_| if reference.is_valid() { Ok(()) } else { Err(DeliveryProblem::InvalidContract.error()) },
            |service, range, _recovery, budget| async move {
                let metadata = service.reader.read_metadata(&range, budget).await?;
                if metadata.capture() != &reference.capture
                    || metadata.capture_manifest_sha256() != reference.manifest_sha256
                    || metadata.source().captured_range()?.scope_id != reference.source_scope_id
                {
                    return Err(DeliveryProblem::Integrity.error());
                }
                let metrics = metadata.metrics().clone();
                Ok((metadata, metrics))
            },
        )
        .await
    }

    /// Reads only original capture metadata and its publication proof using this same action allowance.
    pub async fn read_metadata(&mut self, range_id: &str) -> Result<EvidenceCaptureMetadata> {
        self.read_with(
            range_id,
            |_| Ok(()),
            |service, range, _recovery, budget| async move {
                let metadata = service.reader.read_metadata(&range, budget).await?;
                let metrics = metadata.metrics().clone();
                Ok((metadata, metrics))
            },
        )
        .await
    }

    /// Discovery rows are bounded hints and use this same action allowance. No source database is opened.
    pub async fn incident_candidates(&mut self, request: &IncidentDiscoveryRequest) -> Result<IncidentDiscoveryPage> {
        self.perform(
            |budget| request.within_budget(budget).map(|_| ()),
            |service, budget, fence| async move {
                fence.check()?;
                let page = service.reader.incident_candidates(&service.contract, request, budget).await?;
                let used = SessionUsage { metrics: page.metrics.clone(), candidate_rows: page.charged_candidate_rows };
                Ok((page, used))
            },
        )
        .await
    }

    async fn read_with<T, F, Reading>(
        &mut self,
        range_id: &str,
        validate: impl FnOnce(EvidenceReadBudget) -> Result<()>,
        read: F,
    ) -> Result<T>
    where
        F: FnOnce(&'a EvidenceService, CommittedRange, ObjectRecoveryReader, EvidenceReadBudget) -> Reading,
        Reading: Future<Output = Result<(T, EvidenceReadMetrics)>>,
    {
        self.perform(
            |budget| {
                if !valid_digest(range_id) {
                    return Err(DeliveryProblem::InvalidContract.error());
                }
                validate(budget)
            },
            |service, budget, fence| async move {
                let accounting = MetadataAccounting::new(
                    service.recovery.clone(),
                    budget.max_queries,
                    budget.max_proof_bytes,
                    fence.clone(),
                );
                let recovery = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
                let range = reopen(&recovery, range_id).await?;
                let mut reader_budget = budget;
                reader_budget.max_proof_bytes = reader_budget
                    .max_proof_bytes
                    .checked_sub(accounting.work()?.bytes)
                    .filter(|bytes| *bytes > 0)
                    .ok_or_else(capacity)?;
                fence.check()?;
                let (result, mut metrics) = read(service, range, recovery, reader_budget).await?;
                metrics.proof_bytes = metrics.proof_bytes.checked_add(accounting.work()?.bytes).ok_or_else(capacity)?;
                Ok((result, SessionUsage { metrics, candidate_rows: 0 }))
            },
        )
        .await
    }

    pub(super) async fn perform<T, F, Reading>(
        &mut self,
        validate: impl FnOnce(EvidenceReadBudget) -> Result<()>,
        read: F,
    ) -> Result<T>
    where
        F: FnOnce(&'a EvidenceService, EvidenceReadBudget, ReadFence) -> Reading,
        Reading: Future<Output = Result<(T, SessionUsage)>>,
    {
        // Failed or cancelled work may have consumed an unreturned response. Its allowance cannot be renewed.
        let mut budget = self.remaining.take().ok_or_else(capacity)?;
        budget.page_rows = budget.page_rows.min(budget.max_rows);
        budget.page_bytes = u64::from(budget.page_bytes).min(budget.max_bytes) as u32;
        budget.validate()?;
        validate(budget)?;
        if Instant::now() >= self.deadline {
            return Err(CollectionStop::TimedOut.error());
        }
        let fence = ReadFence { cancellation: self.cancellation.clone(), deadline: self.deadline };
        let (result, used) = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            result = tokio::time::timeout_at(self.deadline, read(self.service, budget, fence)) => {
                result.map_err(|_| CollectionStop::TimedOut.error())??
            }
        };
        if Instant::now() >= self.deadline {
            return Err(CollectionStop::TimedOut.error());
        }
        // A final ready response can signal cancellation in the same poll that completed the read branch.
        // The consumed action cannot return evidence or regain allowance after that signal.
        if self.cancellation.is_cancelled() {
            return Err(CollectionStop::Cancelled.error());
        }
        let mut remaining = deduct(budget, &used.metrics)?;
        remaining.max_rows = remaining.max_rows.checked_sub(used.candidate_rows).ok_or_else(capacity)?;
        add_metrics(&mut self.metrics, &used.metrics)?;
        self.remaining = Some(remaining);
        Ok(result)
    }
}

#[derive(Clone)]
pub(super) struct ReadFence {
    cancellation: CollectionCancellation,
    deadline: Instant,
}
impl ReadFence {
    pub fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(CollectionStop::Cancelled.error());
        }
        if Instant::now() >= self.deadline {
            return Err(CollectionStop::TimedOut.error());
        }
        Ok(())
    }
}

pub(super) struct SessionUsage {
    pub(super) metrics: EvidenceReadMetrics,
    pub(super) candidate_rows: u32,
}

pub(super) fn deduct(mut budget: EvidenceReadBudget, used: &EvidenceReadMetrics) -> Result<EvidenceReadBudget> {
    budget.max_rows = budget.max_rows.checked_sub(used.selected_rows).ok_or_else(capacity)?;
    budget.max_queries = budget.max_queries.checked_sub(used.queries).ok_or_else(capacity)?;
    budget.max_bytes = budget.max_bytes.checked_sub(used.response_bytes).ok_or_else(capacity)?;
    budget.max_proof_bytes = budget.max_proof_bytes.checked_sub(used.proof_bytes).ok_or_else(capacity)?;
    if let Some(recovery) = &mut budget.recovery_payloads {
        recovery.max_objects = recovery.max_objects.checked_sub(used.recovery_objects).ok_or_else(capacity)?;
        recovery.max_bytes = recovery.max_bytes.checked_sub(used.recovery_bytes).ok_or_else(capacity)?;
    }
    Ok(budget)
}

pub(super) fn add_metrics(total: &mut EvidenceReadMetrics, used: &EvidenceReadMetrics) -> Result<()> {
    macro_rules! add { ($($field:ident),+) => { $(total.$field = total.$field.checked_add(used.$field).ok_or_else(capacity)?;)+ }; }
    add!(selected_rows, queries, pages, returned_rows, response_bytes, proof_bytes, recovery_objects, recovery_bytes);
    Ok(())
}

fn capacity() -> culprit_core::Error {
    DeliveryProblem::Capacity.error()
}
