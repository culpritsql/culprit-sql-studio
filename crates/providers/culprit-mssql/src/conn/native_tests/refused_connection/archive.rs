use super::*;
use culprit_collector::{
    delivery::{
        driver::DeliveryPolicy,
        file_sink::FileTelemetrySink,
        file_store::{FileOwnership, FileRecoveryStore},
        journal::DeliveryJournal,
        recovery::RecoveryStore,
        worker::{DeliveryJournals, DeliveryWorker},
    },
    outbox::Outbox,
};
use culprit_evidence::delivery::{
    contract::Contract,
    recovery::{RecoveryRead, RecoveryRecords, reopen},
};
use serde::Serialize;

const OUTBOX_CAPTURE_LIMIT: u32 = 2;
const OBSERVATION_POLL: Duration = Duration::from_millis(10);

pub(super) struct Archive {
    pub outbox: Outbox,
    store: Arc<FileRecoveryStore>,
    sink: FileTelemetrySink,
    ownership: FileOwnership,
    archive_journal: DeliveryJournal,
    analytical_journal: DeliveryJournal,
    contract: Contract,
}

#[derive(Serialize)]
pub(super) struct ArchivedAttempt {
    pub original: DiagnosticAttemptReceipt,
    archived_range_id: String,
    control_capture: CaptureIdentity,
    original_source_queries: u32,
    original_admission_charged: bool,
}

impl Archive {
    pub async fn create(root: &Path) -> Result<Self> {
        let contract = Contract::new(
            "owned-refused-loopback".into(),
            "single".into(),
            "file:owned-refused-loopback".into(),
            digest(b"owned-refused-loopback-canonical-control"),
        )?;
        let store = Arc::new(FileRecoveryStore::create(&root.join("archive"), contract.clone())?);
        let owner = store.acquire().await?;
        let fence = owner.fence().clone();
        let mut outbox = Outbox::create_remote(
            &root.join("outbox"),
            OutboxLimits { max_captures: OUTBOX_CAPTURE_LIMIT, ..Default::default() },
            store.id(),
        )?;
        outbox.recover().await?;
        outbox.begin_remote_epoch(&fence, 0).await?;
        Ok(Self {
            sink: FileTelemetrySink::create(&root.join("warehouse"), contract.clone(), store.clone())?,
            archive_journal: DeliveryJournal::create(&root.join("archive-journal"), contract.clone())?,
            analytical_journal: DeliveryJournal::create(&root.join("analytical-journal"), contract.clone())?,
            outbox,
            store,
            ownership: owner,
            contract,
        })
    }

    pub async fn commit_and_verify(
        &mut self,
        original: &capture::OriginalAttempt,
        cancellation: &CollectionCancellation,
    ) -> Result<ArchivedAttempt> {
        let previous = self.store.checkpoint_head().await?;
        let mut worker = DeliveryWorker::new(
            self.outbox.delivery(),
            DeliveryJournals { archive: &mut self.archive_journal, analytical: &mut self.analytical_journal },
            self.store.as_ref(),
            &self.sink,
            self.ownership.fence().clone(),
            self.contract.clone(),
            DeliveryPolicy { request_timeout_ms: 5000, ..Default::default() },
        )?;
        let worker_cancellation = CollectionCancellation::default();
        let report = |_: Option<&DeliveryEntry>, result: Result<DeliveryEntry>| result.map(|_| ());
        let mut running = Box::pin(worker.run(&worker_cancellation, &report));
        let mut retired = Box::pin(observe_retirement(&self.outbox, self.store.as_ref(), cancellation));
        let head = tokio::select! {
            result = &mut running => {
                result?;
                return Err(invalid_data());
            }
            result = &mut retired => {
                worker_cancellation.cancel();
                running.await?;
                result?
            }
        };
        if previous.as_ref() == Some(&head) {
            return Err(invalid_data());
        }
        let committed = reopen(self.store.as_ref(), &head).await?;
        let manifest = committed.manifest();
        if manifest.source_queries != 0
            || manifest.read_metrics.is_some()
            || manifest.source.observation.collection() != TelemetryCollection::CollectorConnections
            || manifest.source.is_partition_qualified()
            || manifest.batches.len() != 1
        {
            return Err(invalid_data());
        }
        let batch = &manifest.batches[0];
        let mut rows = RecoveryRecords::open(self.store.as_ref(), batch).await?;
        let row = rows.next().await?.ok_or_else(invalid_data)?;
        // EOF size/hash verification and the existing one-based canonical receipt owner both remain mandatory.
        if rows.next().await?.is_some() || batch.rows.len() != 1 {
            return Err(invalid_data());
        }
        batch.validate_row(&manifest.source, &manifest.capture, 1, &row)?;
        let TelemetryRecord::CollectorConnection(control) = &row.record else { return Err(invalid_data()) };
        if control.original != original.receipt
            || control.original_status.as_ref() != Some(&original.manifest.status)
            || control.original_state != OutboxCaptureState::Incomplete
            || control.original_evidence != CollectorConnectionOriginalEvidence::NoNativeEvidence
            || control.source_work != SourceWorkState::Quiescent
            || control.original_source_queries != Some(1)
            || control.collector_outcome
                != (CollectorCaptureOutcome::Failed { code: culprit_core::ErrorCode::ConnectionFailed })
            || control.connection_outcome() != CollectorConnectionOutcome::Failure
            || control.native.is_some()
            || !control.fits_source(&manifest.source)
            || serde_json::to_string(control).map_err(|_| invalid_data())?.contains(UNUSED_CREDENTIAL)
        {
            return Err(invalid_data());
        }
        Ok(ArchivedAttempt {
            original: original.receipt.clone(),
            archived_range_id: head,
            control_capture: manifest.capture.clone(),
            original_source_queries: original.manifest.source_queries,
            original_admission_charged: true,
        })
    }
}

async fn observe_retirement(
    outbox: &Outbox,
    store: &dyn RecoveryStore,
    cancellation: &CollectionCancellation,
) -> Result<String> {
    loop {
        crate::test_support::owned_case::require_active(cancellation)?;
        if outbox.status(OUTBOX_CAPTURE_LIMIT, None).await?.captures == 0 {
            return store.checkpoint_head().await?.ok_or_else(invalid_data);
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            _ = tokio::time::sleep(OBSERVATION_POLL) => {}
        }
    }
}
