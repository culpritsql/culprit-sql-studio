//! The native acceptance uses the ordinary local raw archive and its EOF verifier, never a test decoder.
use super::*;
use culprit_collector::delivery::{
    file_store::{FileOwnership, FileRecoveryStore},
    recovery::stage_and_commit,
};
use culprit_core::limits::{OperationGuard, OperationHandle};
use culprit_evidence::{
    analytical::catalog::LogicalCatalog,
    delivery::{contract::Contract, recovery::RecoveryRecords},
};

const MAX_ARCHIVE_CAPTURES: usize = 32;
const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ARCHIVE_ROWS: usize = 10_000;
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(30);

pub(in crate::collector_service::monitoring) struct NativeArchive {
    store: FileRecoveryStore,
    owner: FileOwnership,
    contract: Contract,
    head: Option<String>,
    _guard: OperationGuard,
    operation: OperationHandle,
    captures: usize,
    bytes: u64,
    rows: usize,
}
#[derive(Clone)]
pub(in crate::collector_service::monitoring) struct RetainedRevision {
    pub reference: DiagnosticEvidenceRef,
    pub revision: Box<DiagnosticRevision>,
}
pub(in crate::collector_service::monitoring) struct ArchivedCapture {
    pub original: DiagnosticCaptureRef,
    pub source: Box<CollectionDescription>,
    pub revisions: Vec<RetainedRevision>,
    pub incidents: Vec<DerivedIncident>,
}
impl NativeArchive {
    pub async fn create(directory: &Path, cancellation: &CollectionCancellation) -> Result<Box<Self>> {
        case::require_active(cancellation)?;
        let guard = OperationGuard::new(ARCHIVE_TIMEOUT)?;
        let operation = guard.handle();
        let schema = LogicalCatalog::new()?;
        let contract = Contract::new(
            "owned-native-automatic".into(),
            "owned-native".into(),
            "file:owned-native-proof".into(),
            schema.digest().into(),
        )?;
        let store = FileRecoveryStore::create(&directory.join("raw-evidence"), contract.clone())?;
        operation.check()?;
        case::require_active(cancellation)?;
        let owner = case::admitted(cancellation, store.acquire()).await?;
        Ok(Box::new(Self {
            store,
            owner,
            contract,
            head: None,
            _guard: guard,
            operation,
            captures: 0,
            bytes: 0,
            rows: 0,
        }))
    }
    pub fn operation(&self) -> OperationHandle {
        self.operation.clone()
    }
    fn check(&self, cancellation: &CollectionCancellation) -> Result<()> {
        case::require_active(cancellation)?;
        self.operation.check()
    }
    pub async fn capture(
        &mut self,
        outbox: &Outbox,
        entry: &OutboxEntry,
        cancellation: &CollectionCancellation,
    ) -> Result<ArchivedCapture> {
        self.check(cancellation)?;
        let capture = &entry.capture;
        if entry.state != OutboxCaptureState::Ready || entry.source_work != SourceWorkState::Quiescent {
            return Err(DeliveryProblem::Integrity.error());
        }
        // Ready charge bounds the ordinary prepare/copy/archive-read passes; this is not a native cost receipt.
        self.bytes = self
            .bytes
            .checked_add(entry.charged_bytes)
            .filter(|bytes| *bytes <= MAX_ARCHIVE_BYTES)
            .ok_or_else(|| DeliveryProblem::Capacity.error())?;
        self.captures = self
            .captures
            .checked_add(1)
            .filter(|n| *n <= MAX_ARCHIVE_CAPTURES)
            .ok_or_else(|| DeliveryProblem::Capacity.error())?;
        let prepared = case::admitted(
            cancellation,
            outbox.delivery().prepare(&capture.observation_id, self.contract.clone(), vec![]),
        )
        .await?;
        self.check(cancellation)?;
        let selected_rows = prepared.manifest().batches.iter().try_fold(0_usize, |sum, batch| {
            sum.checked_add(batch.rows.len()).ok_or_else(|| DeliveryProblem::Capacity.error())
        })?;
        self.rows = self
            .rows
            .checked_add(selected_rows)
            .filter(|rows| *rows <= MAX_ARCHIVE_ROWS)
            .ok_or_else(|| DeliveryProblem::Capacity.error())?;
        let committed = case::admitted(
            cancellation,
            stage_and_commit(&self.store, self.owner.fence(), self.head.as_deref(), &prepared),
        )
        .await?;
        self.head = Some(committed.manifest().range_id.clone());
        let manifest = committed.manifest();
        let original = DiagnosticCaptureRef {
            capture: manifest.capture.clone(),
            manifest_sha256: manifest.capture_manifest_sha256.clone(),
            source_scope_id: manifest.source.captured_range()?.scope_id,
        };
        let mut output = ArchivedCapture {
            original: original.clone(),
            source: Box::new(manifest.source.clone()),
            revisions: vec![],
            incidents: vec![],
        };
        for batch in &manifest.batches {
            self.check(cancellation)?;
            let mut input = case::admitted(cancellation, RecoveryRecords::open(&self.store, batch)).await?;
            let mut ordinal = 0_usize;
            while let Some(row) = case::admitted(cancellation, input.next()).await? {
                self.check(cancellation)?;
                ordinal += 1;
                batch.validate_row(&manifest.source, &manifest.capture, ordinal, &row)?;
                match &row.record {
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::DiagnosticRevision(revision),
                        ..
                    }) => {
                        output.revisions.push(RetainedRevision {
                            reference: DiagnosticEvidenceRef::from_export(
                                &manifest.source,
                                &manifest.capture_manifest_sha256,
                                &row,
                                u32::try_from(ordinal - 1).map_err(|_| DeliveryProblem::Capacity.error())?,
                            )?,
                            revision: revision.clone(),
                        });
                    }
                    TelemetryRecord::DerivedIncident(incident) => output.incidents.push(incident.as_ref().clone()),
                    _ => (),
                }
            }
            // No provisional prefix escapes; final None authenticates the object bytes and SHA to EOF.
            if ordinal != batch.rows.len() {
                return Err(DeliveryProblem::Integrity.error());
            }
        }
        self.check(cancellation)?;
        Ok(output)
    }
}

pub(in crate::collector_service::monitoring) async fn retained(
    outbox: &Outbox,
    cancellation: &CollectionCancellation,
) -> Result<Vec<OutboxEntry>> {
    case::require_active(cancellation)?;
    let status = case::admitted(cancellation, outbox.status(MAX_ARCHIVE_CAPTURES as u32, None)).await?;
    if status.next_cursor.is_some()
        || status
            .entries
            .iter()
            .any(|entry| entry.state != OutboxCaptureState::Ready || entry.source_work != SourceWorkState::Quiescent)
    {
        return Err(DeliveryProblem::Capacity.error());
    }
    let mut entries = status.entries;
    let mut positioned = entries
        .drain(..)
        .map(|entry| {
            let revision = entry.capture.revision.as_ref().ok_or_else(|| DeliveryProblem::Integrity.error())?;
            let position = (
                revision.owner_epoch.parse::<u64>().map_err(|_| DeliveryProblem::Integrity.error())?,
                revision.sequence.parse::<u64>().map_err(|_| DeliveryProblem::Integrity.error())?,
            );
            Ok((position, entry))
        })
        .collect::<Result<Vec<_>>>()?;
    positioned.sort_by_key(|(position, _)| *position);
    Ok(positioned.into_iter().map(|(_, entry)| entry).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_collector::analytical::test_support::LocalCaptureFixture;

    #[tokio::test]
    async fn local_archive_uses_canonical_eof_and_keeps_the_resolved_revision_outside_active_state() {
        let fixture = LocalCaptureFixture::diagnostic_history(true).await;
        let directory = tempfile::tempdir().unwrap();
        let cancellation = CollectionCancellation::default();
        let outbox = fixture.outbox.as_ref().unwrap();
        let mut archive = NativeArchive::create(directory.path(), &cancellation).await.unwrap();
        let entries = retained(outbox, &cancellation).await.unwrap();
        let mut captured = Vec::with_capacity(entries.len());
        for entry in &entries {
            captured.push(archive.capture(outbox, entry, &cancellation).await.unwrap());
        }
        let recorded = captured.iter().find(|recorded| recorded.original.capture == fixture.manifest.capture).unwrap();
        assert_eq!(recorded.original.capture, fixture.manifest.capture);
        assert_eq!(recorded.revisions.len(), 1);
        let episode = &recorded.revisions[0].revision.episode;
        assert_eq!(episode.lifecycle, DiagnosticLifecycle::Resolved);
        assert!(!episode.retained_in_checkpoint());
        assert!(captured.iter().all(|capture| capture.incidents.is_empty()));
        let revisions = captured.iter().flat_map(|capture| capture.revisions.iter().cloned()).collect::<Vec<_>>();
        let history = super::super::history::for_episode(&revisions, &episode.id, &episode.first).unwrap();
        assert_eq!(history.first.revision, "1");
        assert_eq!(history.latest.revision, "2");
        assert_eq!(history.references.len(), 2);
        assert_eq!(*history.latest, *episode);
        assert_eq!(history.first.first, history.latest.first);
        let original = captured.iter().find(|capture| capture.original == episode.first.capture_ref()).unwrap();
        assert!(matches!(original.source.observation, SourceObservation::Activity(_)));
        let proof = episode.recovery.as_ref().unwrap();
        let recovery = captured.iter().find(|capture| &capture.original == proof.capture()).unwrap();
        assert!(proof.matches_source(episode, &recovery.source, &recovery.original));
        let SourceObservation::Activity(frame) = &recovery.source.observation else { panic!() };
        assert_eq!(frame.observations, 0);
        assert!(frame.coverage.complete_scope);
        assert!(directory.path().join("raw-evidence").is_dir());
    }
    #[tokio::test]
    async fn cancelled_local_archive_admission_creates_no_storage_or_source_work() {
        let directory = tempfile::tempdir().unwrap();
        let cancellation = CollectionCancellation::default();
        cancellation.cancel();
        assert!(NativeArchive::create(directory.path(), &cancellation).await.is_err());
        assert!(!directory.path().join("raw-evidence").exists());
    }
}
