use super::*;
use crate::capture::{self, SourceFactory};
use crate::collection_files::ObservationFiles;
use culprit_read::{admission::SourceGate, cancellation::CollectionCancellation};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    FactoryFailure,
    ConnectionFailure,
    ChargedConnectionFailure,
    MissingRecorder,
    Unconfirmed,
    Partial,
}
#[derive(Default)]
struct Calls {
    opens: AtomicU32,
    describes: AtomicU32,
    closes: AtomicU32,
}
struct AttemptFactory {
    case: Case,
    calls: Arc<Calls>,
}
struct AttemptSource {
    case: Case,
    calls: Arc<Calls>,
    recorder: ReadConnectionRecorder,
}
fn original_request() -> CollectionRequest {
    super::super::tests::request()
}
#[async_trait::async_trait]
impl SourceFactory for AttemptFactory {
    async fn open(&self, _: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.calls.opens.fetch_add(1, Ordering::SeqCst);
        if self.case == Case::FactoryFailure {
            return Err(culprit_core::Error::new(culprit_core::ErrorCode::AuthFailed, "private-password-token-canary"));
        }
        Ok(Box::new(AttemptSource { case: self.case, calls: self.calls.clone(), recorder: Default::default() }))
    }
}
#[async_trait::async_trait]
impl TelemetrySource for AttemptSource {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.calls.describes.fetch_add(1, Ordering::SeqCst);
        if self.case == Case::MissingRecorder {
            return Err(culprit_core::Error::new(culprit_core::ErrorCode::QueryFailed, "private-query-failure-canary"));
        }
        let attempt = self.recorder.begin();
        attempt.connection_use(ReadConnectionUse::FreshConnection);
        let phase = attempt.phase(ReadConnectionStage::Acquire, None);
        if self.case == Case::Unconfirmed {
            phase.finish(ReadConnectionOutcome::Unconfirmed);
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        if self.case == Case::ChargedConnectionFailure {
            attempt.phase(ReadConnectionStage::Transport, None).finish(ReadConnectionOutcome::Failed {
                code: culprit_core::ErrorCode::ConnectionFailed,
                native_code: None,
            });
            phase.finish(ReadConnectionOutcome::Failed {
                code: culprit_core::ErrorCode::ConnectionFailed,
                native_code: None,
            });
            return Err(culprit_core::Error::new(culprit_core::ErrorCode::ConnectionFailed, "private-endpoint-canary"));
        }
        if self.case == Case::ConnectionFailure {
            phase.finish(ReadConnectionOutcome::Failed {
                code: culprit_core::ErrorCode::AuthFailed,
                native_code: Some(18456),
            });
            return Err(culprit_core::Error::new(culprit_core::ErrorCode::AuthFailed, "private-password-token-canary"));
        }
        attempt.acquired(1);
        phase.finish(ReadConnectionOutcome::Completed);
        Ok(super::super::tests::source())
    }
    fn query_count(&self) -> u32 {
        // A pre-dispatch admission charge remains paid even when acquisition fails without a reader.
        u32::from(self.case == Case::ChargedConnectionFailure)
    } // This source-boundary stub performs no native SQL or connection work.
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        let mut metrics = ReadMetricsRecorder::new(culprit_core::Engine::Mssql).snapshot().unwrap();
        metrics.connection_work = self.recorder.snapshot().map(Box::new);
        if self.case == Case::ChargedConnectionFailure {
            metrics.native_session_work =
                Some(Box::new(NativeSessionReadWork::MssqlSession(Box::new(MssqlSessionReadWork {
                    qualification_work: None,
                    settlement: NativeReadSettlement::Quiescent,
                    issue: Some(NativeReadWorkIssue::NativeScopeUnqualified),
                    interval: None,
                }))));
        }
        Some(metrics)
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        if cursor.is_some() {
            return Ok(CollectionRead::Stopped(CollectionStop::Cancelled));
        }
        Ok(CollectionRead::Page { records: super::super::tests::records(stream), next_cursor: Some("partial".into()) })
    }
    async fn finish(&mut self) -> Result<SourceCompletion> {
        panic!("partial original must not claim source completion")
    }
    async fn close(&mut self) -> SourceShutdown {
        self.calls.closes.fetch_add(1, Ordering::SeqCst);
        if self.case == Case::Unconfirmed { SourceShutdown::Unconfirmed } else { SourceShutdown::Quiescent }
    }
}
async fn attempt(outbox: &mut Outbox, case: Case) -> (CaptureIdentity, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    let factory = AttemptFactory { case, calls: calls.clone() };
    let request = original_request();
    let mut lease = outbox
        .reserve_connection("configured-route", &request, &digest(b"exact-route"), culprit_core::Engine::Mssql)
        .await
        .unwrap();
    let identity = lease.identity().clone();
    let directory = lease.directory().to_owned();
    let result = capture::collect(
        &SourceGate::default(),
        &factory,
        request,
        &directory,
        identity.clone(),
        &CollectionCancellation::default(),
        Some(&mut lease),
    )
    .await;
    let outcome = match &result {
        Ok(_) => CollectorCaptureOutcome::Returned,
        Err(error) => CollectorCaptureOutcome::Failed { code: error.code },
    };
    lease.finish_observed(outcome, 1.0).await.unwrap();
    assert_eq!(calls.opens.load(Ordering::SeqCst), 1);
    assert_eq!(calls.describes.load(Ordering::SeqCst), u32::from(case != Case::FactoryFailure));
    assert_eq!(calls.closes.load(Ordering::SeqCst), u32::from(case != Case::FactoryFailure));
    (identity, calls)
}
async fn materialized(outbox: &Outbox) -> CaptureIdentity {
    outbox.files.run(|| |files| files.materialize_connection_control().map(Option::unwrap)).await.unwrap()
}
async fn record(outbox: &mut Outbox, root: &Path, identity: &CaptureIdentity) -> CollectorConnectionObservation {
    let manifest = outbox.verify(&identity.observation_id).await.unwrap();
    assert_eq!(manifest.source_queries, 0);
    assert!(manifest.read_metrics.is_none());
    let bytes = fs::read(root.join("captures").join(&identity.observation_id).join(&manifest.files[0].file)).unwrap();
    let export: ExportedRecord = serde_json::from_slice(bytes.strip_suffix(b"\n").unwrap()).unwrap();
    let TelemetryRecord::CollectorConnection(row) = export.record else { panic!("canonical collector control row") };
    assert_eq!(manifest.description.as_ref().unwrap().target.database(), None);
    assert!(row.fits_source(manifest.description.as_ref().unwrap()));
    *row
}
#[tokio::test]
async fn real_preidentity_failure_and_unhooked_reader_keep_exact_failure_unknown_and_no_extra_source_calls() {
    for case in [Case::FactoryFailure, Case::ConnectionFailure, Case::MissingRecorder] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("outbox");
        let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
        let (original, calls) = attempt(&mut outbox, case).await;
        assert_eq!(outbox.status(10, None).await.unwrap().connection_controls_pending, 1);
        let output = materialized(&outbox).await;
        let row = record(&mut outbox, &path, &output).await;
        assert_eq!(row.original.capture, original);
        assert!(row.original.manifest_sha256.is_some());
        assert_eq!(row.original_evidence, CollectorConnectionOriginalEvidence::NoNativeEvidence);
        assert_eq!(
            row.connection_outcome(),
            if case == Case::ConnectionFailure {
                CollectorConnectionOutcome::Failure
            } else {
                CollectorConnectionOutcome::Unknown
            }
        );
        assert_eq!(row.original_source_queries, Some(0));
        assert!(row.native.is_none());
        assert!(matches!(row.collector_outcome, CollectorCaptureOutcome::Failed { .. }));
        assert!(!serde_json::to_string(&row).unwrap().contains("private-password-token-canary"));
        assert_eq!(outbox.status(10, None).await.unwrap().connection_controls_pending, 0);
        assert!(outbox.files.run(|| |files| files.materialize_connection_control()).await.unwrap().is_none());
        assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
        assert_eq!(calls.opens.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn interrupted_original_without_manifest_freezes_unknown_once_and_replays_the_same_output_allocation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    let mut lease = outbox
        .reserve_connection("configured-route", &original_request(), &digest(b"route"), culprit_core::Engine::Mssql)
        .await
        .unwrap();
    let original = lease.identity().clone();
    lease.source_started().await.unwrap();
    drop(lease);
    drop(outbox);
    let mut outbox = Outbox::open(&path).unwrap();
    outbox.recover().await.unwrap();
    let id = original.observation_id.clone();
    let (snapshot, output) = outbox
        .files
        .run(|| {
            move |files| {
                let mut pending = read_pending(&files.journal.conn, &id)?.unwrap();
                let snapshot = pending.snapshot.as_ref().unwrap().clone();
                let request = super::writer::request(&pending)?;
                let reservation = files.reserve(&pending.registration.target.name, &request)?;
                pending.output = Some(reservation.entry.identity.clone());
                assert_eq!(read_pending(&files.journal.conn, &id)?.unwrap().output, pending.output);
                Ok((snapshot, reservation.entry.identity))
            }
        })
        .await
        .unwrap();
    assert_eq!(snapshot.original.manifest_sha256, None);
    assert_eq!(snapshot.original_evidence, CollectorConnectionOriginalEvidence::ManifestUnavailable);
    assert_eq!(snapshot.source_work, SourceWorkState::Active);
    assert_eq!(snapshot.collector_outcome, CollectorCaptureOutcome::Unknown);
    assert!(snapshot.connections.is_none());
    assert!(snapshot.native.is_none());
    drop(outbox);
    let mut outbox = Outbox::open(&path).unwrap();
    outbox.recover().await.unwrap();
    assert_eq!(materialized(&outbox).await, output);
    assert_eq!(record(&mut outbox, &path, &output).await, snapshot);
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
}
#[tokio::test]
async fn live_capture_is_not_recovered_by_delivery_and_unconfirmed_settlement_stays_debt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    outbox.recover().await.unwrap();
    let delivery = outbox.delivery();
    let mut lease = outbox
        .reserve_connection("configured-route", &original_request(), &digest(b"route"), culprit_core::Engine::Mssql)
        .await
        .unwrap();
    lease.source_started().await.unwrap();
    assert!(delivery.next_archive().await.unwrap().is_none());
    let status = delivery.status(10, None).await.unwrap();
    assert_eq!(status.entries[0].state, OutboxCaptureState::Capturing);
    assert_eq!(status.entries[0].source_work, SourceWorkState::Active);
    let files =
        ObservationFiles::create(lease.directory(), original_request(), lease.identity().clone(), None).unwrap();
    files.finish(0).unwrap();
    lease.source_closed(SourceShutdown::Quiescent).await.unwrap();
    lease
        .finish_observed(CollectorCaptureOutcome::Failed { code: culprit_core::ErrorCode::QueryFailed }, 1.0)
        .await
        .unwrap();
    materialized(&outbox).await;
    let (original, _) = attempt(&mut outbox, Case::Unconfirmed).await;
    let output = materialized(&outbox).await;
    let row = record(&mut outbox, &path, &output).await;
    assert_eq!(row.original.capture, original);
    assert_eq!(row.source_work, SourceWorkState::Unconfirmed);
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 1);
    assert!(outbox.reserve("next-native", &original_request()).await.is_err());
}
#[tokio::test]
async fn control_archive_expires_only_positively_empty_preidentity_and_keeps_partial_native_originals() {
    use crate::delivery::recovery::stage_and_commit;
    for case in [Case::FactoryFailure, Case::Partial, Case::Unconfirmed] {
        let root = tempfile::tempdir().unwrap();
        let (store, owner, contract, mut outbox) = super::super::tests::eviction::remote_box(root.path()).await;
        let (original, _) = attempt(&mut outbox, case).await;
        let output = materialized(&outbox).await;
        let row = record(&mut outbox, &root.path().join("outbox"), &output).await;
        let prepared = outbox.delivery().prepare(&output.observation_id, contract, vec![]).await.unwrap();
        let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
        outbox.delivery().mark_archived(&committed).await.unwrap();
        let status = outbox.status(10, None).await.unwrap();
        if case == Case::FactoryFailure {
            assert_eq!(status.captures, 0);
            assert!(!root.path().join("outbox/captures").join(original.observation_id).exists());
        } else {
            assert_eq!(status.captures, 1);
            assert_eq!(status.entries[0].capture, original);
            if case == Case::Partial {
                assert_eq!(row.original_evidence, CollectorConnectionOriginalEvidence::MayContainNativeEvidence);
            } else {
                assert_eq!(status.source_debts, 1);
            }
        }
    }
}
#[tokio::test]
async fn output_headroom_refuses_before_native_open_and_pending_metadata_remains_charged_and_pinned() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits { max_captures: 1, ..Default::default() }).unwrap();
    let error = outbox
        .reserve_connection("configured-route", &original_request(), &digest(b"route"), culprit_core::Engine::Mssql)
        .await
        .err()
        .unwrap();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::Capacity)));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 0);
    drop(outbox);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    let (original, _) = attempt(&mut outbox, Case::FactoryFailure).await;
    assert_eq!(outbox.delivery().eviction_candidate(&original.observation_id).await.unwrap(), Some(false));
    let id = original.observation_id;
    let charged = outbox
        .files
        .run(|| {
            move |files| {
                assert!(read_pending(&files.journal.conn, &id)?.unwrap().snapshot.is_some());
                charged_bytes(&files.journal.conn)
            }
        })
        .await
        .unwrap();
    assert!(charged > 0);
}

#[tokio::test]
async fn cancelled_baseline_records_the_existing_stop_without_opening_any_native_factory() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    let calls = Arc::new(Calls::default());
    let factory = AttemptFactory { case: Case::FactoryFailure, calls: calls.clone() };
    let request = original_request();
    let mut lease = outbox
        .reserve_connection("configured-route", &request, &digest(b"route"), culprit_core::Engine::Mssql)
        .await
        .unwrap();
    let identity = lease.identity().clone();
    let directory = lease.directory().to_owned();
    let cancelled = CollectionCancellation::default();
    cancelled.cancel();
    let original = capture::collect(
        &SourceGate::default(),
        &factory,
        request,
        &directory,
        identity.clone(),
        &cancelled,
        Some(&mut lease),
    )
    .await
    .unwrap();
    assert_eq!(original.status, CollectionStatus::Incomplete(CollectionStop::Cancelled));
    assert_eq!(calls.opens.load(Ordering::SeqCst), 0);
    lease.finish_observed(CollectorCaptureOutcome::Returned, 0.5).await.unwrap();
    let output = materialized(&outbox).await;
    let row = record(&mut outbox, &path, &output).await;
    assert_eq!(row.original.capture, identity);
    assert_eq!(row.original_status, Some(original.status));
    assert_eq!(row.source_work, SourceWorkState::NotStarted);
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Unknown);
    assert!(row.connections.is_none());
    assert!(row.native.is_none());
}

#[tokio::test]
async fn positive_empty_proof_cannot_expire_an_original_that_gained_unverified_native_bytes() {
    use crate::delivery::recovery::stage_and_commit;
    for case in [Case::FactoryFailure, Case::ChargedConnectionFailure] {
        let root = tempfile::tempdir().unwrap();
        let (store, owner, contract, mut outbox) = super::super::tests::eviction::remote_box(root.path()).await;
        let (original, _) = attempt(&mut outbox, case).await;
        let output = materialized(&outbox).await;
        let original_path = root.path().join("outbox/captures").join(&original.observation_id);
        let manifest: CollectionManifest =
            serde_json::from_slice(&fs::read(original_path.join("manifest.json")).unwrap()).unwrap();
        fs::write(original_path.join(&manifest.files[0].file), b"unverified-native-bytes").unwrap();
        let prepared = outbox.delivery().prepare(&output.observation_id, contract, vec![]).await.unwrap();
        let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
        assert!(outbox.delivery().mark_archived(&committed).await.is_err());
        assert_eq!(fs::read(original_path.join(&manifest.files[0].file)).unwrap(), b"unverified-native-bytes");
        assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
    }
}
#[tokio::test]
async fn acknowledged_zero_evidence_expiry_resumes_after_unlink_without_replaying_the_source() {
    use crate::delivery::recovery::stage_and_commit;
    let root = tempfile::tempdir().unwrap();
    let (store, owner, contract, mut outbox) = super::super::tests::eviction::remote_box(root.path()).await;
    let (original, calls) = attempt(&mut outbox, Case::FactoryFailure).await;
    let output = materialized(&outbox).await;
    let prepared = outbox.delivery().prepare(&output.observation_id, contract, vec![]).await.unwrap();
    let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    let id = original.observation_id.clone();
    outbox.files.run(||move|files| {
        files.journal.conn.execute_batch(&format!("CREATE TRIGGER fail_empty_expiry BEFORE DELETE ON captures WHEN OLD.observation_id='{id}' BEGIN SELECT RAISE(FAIL,'expiry-stop'); END")).map_err(storage)?;Ok(())
    }).await.unwrap();
    assert!(outbox.delivery().mark_archived(&committed).await.is_err());
    assert!(!root.path().join("outbox/captures").join(&original.observation_id).exists());
    outbox
        .files
        .run(|| {
            |files| {
                files.journal.conn.execute_batch("DROP TRIGGER fail_empty_expiry").map_err(storage)?;
                Ok(())
            }
        })
        .await
        .unwrap();
    drop(outbox);
    let mut reopened = Outbox::open(&root.path().join("outbox")).unwrap();
    reopened.recover().await.unwrap();
    assert_eq!(calls.opens.load(Ordering::SeqCst), 1);
    assert!(!reopened.status(10, None).await.unwrap().entries.iter().any(|entry| entry.capture == original));
    assert_eq!(reopened.status(10, None).await.unwrap().connection_controls_pending, 0);
}

#[tokio::test]
async fn a_charged_failed_fresh_acquisition_expires_after_exact_control_archive_without_replaying_work() {
    use crate::delivery::recovery::{RecoveryStore, stage_and_commit};
    let root = tempfile::tempdir().unwrap();
    let (store, owner, contract, mut outbox) = super::super::tests::eviction::remote_box(root.path()).await;
    let path = root.path().join("outbox");
    let (original, calls) = attempt(&mut outbox, Case::ChargedConnectionFailure).await;
    let directory = path.join("captures").join(&original.observation_id);
    let manifest: CollectionManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.source_queries, 1, "the original admission charge must remain exact");
    assert!(manifest.description.is_none());
    let metrics = manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.templates.is_empty());
    assert!(metrics.native_session_work.is_some());
    assert!(metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    assert!(no_native_evidence(&directory, &manifest, SourceWorkState::Quiescent));
    for work in [SourceWorkState::NotStarted, SourceWorkState::Active, SourceWorkState::Unconfirmed] {
        assert!(!no_native_evidence(&directory, &manifest, work), "{work:?}");
    }
    assert_eq!(outbox.delivery().eviction_candidate(&original.observation_id).await.unwrap(), Some(false));
    let id = original.observation_id.clone();
    let frozen = outbox
        .files
        .run(|| move |files| Ok(read_pending(&files.journal.conn, &id)?.unwrap().snapshot.unwrap()))
        .await
        .unwrap();
    drop(outbox);
    let mut outbox = Outbox::open(&path).unwrap();
    outbox.recover().await.unwrap();
    // Recovery authenticates settled originals; allocation still needs a fresh remote ownership claim.
    let error = outbox.files.run(|| |files| files.materialize_connection_control()).await.unwrap_err();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::RecoveryRequired)));
    let stale = outbox.begin_diagnostic_epoch(owner.fence(), 0).await.unwrap_err();
    assert_eq!(stale.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Fenced)));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 1);
    assert_eq!(outbox.status(10, None).await.unwrap().connection_controls_pending, 1);
    drop(owner);
    let owner = store.acquire().await.unwrap();
    let high_sequence = store.recover(None, 1).await.unwrap().high_sequence;
    outbox.begin_diagnostic_epoch(owner.fence(), high_sequence).await.unwrap();
    let error = outbox.reserve("native-stays-fenced", &original_request()).await.err().unwrap();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::RecoveryRequired)));
    let output = materialized(&outbox).await;
    let row = record(&mut outbox, &path, &output).await;
    assert_eq!(row, frozen, "restart must retain the exact sealed snapshot, including original clocks");
    assert_eq!(row.original.capture, original);
    assert_eq!(row.original_source_queries, Some(1));
    assert_eq!(row.original_evidence, CollectorConnectionOriginalEvidence::NoNativeEvidence);
    assert_eq!(row.connection_outcome(), CollectorConnectionOutcome::Failure);
    assert_eq!(row.source_work, SourceWorkState::Quiescent);
    assert_eq!(row.connections, metrics.connection_work.as_deref().cloned());
    assert!(row.native.is_none());
    assert!(row.original.manifest_sha256.is_some());
    assert!(!serde_json::to_string(&row).unwrap().contains("private-endpoint-canary"));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
    let prepared = outbox.delivery().prepare(&output.observation_id, contract, vec![]).await.unwrap();
    let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    outbox.delivery().mark_archived(&committed).await.unwrap();
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 0);
    assert!(!directory.exists());
    drop(outbox);
    let mut outbox = Outbox::open(&path).unwrap();
    outbox.recover().await.unwrap();
    let status = outbox.status(10, None).await.unwrap();
    assert_eq!(status.captures, 0);
    assert_eq!(status.source_debts, 0);
    assert_eq!(status.connection_controls_pending, 0);
    assert!(outbox.files.run(|| |files| files.materialize_connection_control()).await.unwrap().is_none());
    assert_eq!(calls.opens.load(Ordering::SeqCst), 1);
    assert_eq!(calls.describes.load(Ordering::SeqCst), 1);
    assert_eq!(calls.closes.load(Ordering::SeqCst), 1);
}
