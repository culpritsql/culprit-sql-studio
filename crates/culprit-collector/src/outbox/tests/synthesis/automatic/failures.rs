use super::*;
use crate::synthesis::{SynthesisFailureWatermark, Synthesizer};
use std::sync::atomic::{AtomicBool, Ordering};

struct FailureOnce {
    ordinary: QueueAnalysis,
    first: AtomicBool,
    reason: CollectionStop,
}
impl Synthesizer for FailureOnce {
    fn analyzer(&self) -> ComponentVersion {
        self.ordinary.analyzer()
    }
    fn rules_version(&self) -> &str {
        self.ordinary.rules_version()
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        if self.first.swap(false, Ordering::AcqRel) {
            return Err(self.reason.error());
        }
        self.ordinary.synthesize(input)
    }
}
fn failing(reason: CollectionStop) -> (Arc<FailureOnce>, AutomaticSynthesis) {
    let analyzer = Arc::new(FailureOnce {
        ordinary: QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None },
        first: AtomicBool::new(true),
        reason,
    });
    let automatic = AutomaticSynthesis::new(analyzer.clone(), CompletedCaptureBudget::default()).unwrap();
    (analyzer, automatic)
}
fn empty_watermark(watermark: &SynthesisFailureWatermark) {
    assert_eq!(watermark.total, 0);
    assert!(watermark.latest.is_none());
}

#[tokio::test]
async fn explicit_local_budget_failures_retire_only_that_job_and_the_next_original_is_still_analyzed() {
    for reason in [CollectionStop::ByteLimit, CollectionStop::RowLimit, CollectionStop::TimedOut] {
        let temp = tempfile::tempdir().unwrap();
        let (root, mut outbox) = box_at(&temp);
        let (_, automatic) = failing(reason);
        install(&mut outbox, &automatic).await;
        let first = complete(&mut outbox).await;
        let second = complete(&mut outbox).await;
        let first = original(&root, &first);
        let second = original(&root, &second);
        let SynthesisTurn::Failed { primary, code } = turn(&outbox, &automatic).await else { panic!() };
        assert_eq!(primary, first);
        assert_eq!(code, reason.error().code);
        let state = backlog(&outbox).await;
        assert_eq!((state.pending, state.failed, state.failure_watermark.total), (1, 1, 1));
        let latest = state.failure_watermark.latest.unwrap();
        assert_eq!(latest.primary, first);
        assert_eq!(latest.code, code);
        assert!(matches!(turn(&outbox,&automatic).await,SynthesisTurn::Committed{primary,..} if primary==second));
        assert_eq!(backlog(&outbox).await.failure_watermark.total, 1);
        assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    }
}

struct HeldOnce {
    ordinary: QueueAnalysis,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl Synthesizer for HeldOnce {
    fn analyzer(&self) -> ComponentVersion {
        self.ordinary.analyzer()
    }
    fn rules_version(&self) -> &str {
        self.ordinary.rules_version()
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            let (mutex, ready) = self.release.as_ref();
            let mut released = mutex.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
        }
        self.ordinary.synthesize(input)
    }
}
#[tokio::test]
async fn an_actual_local_deadline_is_a_durable_failed_job_without_expiring_the_outer_owner_or_losing_later_work() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let (entered, started) = tokio::sync::oneshot::channel();
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let automatic = Arc::new(
        AutomaticSynthesis::new(
            Arc::new(HeldOnce {
                ordinary: QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None },
                entered: Mutex::new(Some(entered)),
                release: release.clone(),
            }),
            CompletedCaptureBudget { timeout: std::time::Duration::from_secs(1), ..Default::default() },
        )
        .unwrap(),
    );
    install(&mut outbox, &automatic).await;
    let first = complete(&mut outbox).await;
    let second = complete(&mut outbox).await;
    let owner = OperationGuard::new(std::time::Duration::from_secs(20)).unwrap();
    let handle = owner.handle();
    let delivery = outbox.delivery();
    let worker = automatic.clone();
    let task = tokio::spawn(async move { delivery.synthesize_pending(&worker, handle, ()).await });
    started.await.unwrap();
    // The real read/analysis deadline expires while its existing blocking owner remains held.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    {
        let (mutex, ready) = release.as_ref();
        *mutex.lock().unwrap() = true;
        ready.notify_one();
    }
    let SynthesisTurn::Failed { primary, code } = task.await.unwrap().unwrap() else { panic!() };
    assert_eq!(primary, original(&root, &first));
    assert_eq!(code, culprit_core::ErrorCode::Timeout);
    assert!(!owner.handle().is_cancelled());
    assert!(!owner.handle().remaining().is_zero());
    assert_eq!(backlog(&outbox).await.failure_watermark.total, 1);
    assert!(
        matches!(turn(&outbox,&automatic).await,SynthesisTurn::Committed{primary,..} if primary==original(&root,&second))
    );
    assert_eq!(counts(&outbox).await, (0, 2, 1, 0));
}

#[tokio::test]
async fn caller_cancellation_retains_the_job_and_pins_without_recording_an_analysis_loss() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let owner = Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let automatic = automatic(Arc::new(Mutex::new(vec![])), Some(owner.clone()));
    install(&mut outbox, &automatic).await;
    complete(&mut outbox).await;
    let handle = owner.lock().unwrap().as_ref().unwrap().handle();
    assert!(outbox.delivery().synthesize_pending(&automatic, handle, ()).await.is_err());
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    empty_watermark(&backlog(&outbox).await.failure_watermark);
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Committed { .. }));
    empty_watermark(&backlog(&outbox).await.failure_watermark);
}

struct NewRules(QueueAnalysis);
impl Synthesizer for NewRules {
    fn analyzer(&self) -> ComponentVersion {
        self.0.analyzer()
    }
    fn rules_version(&self) -> &str {
        "queue-controlled-2"
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let mut diagnosis = self.0.synthesize(input)?;
        diagnosis.description.rules_version = self.rules_version().into();
        diagnosis.description.analysis_id = diagnosis.description.expected_analysis_id()?;
        Ok(diagnosis)
    }
}

#[tokio::test]
async fn failed_original_archival_and_restart_retire_capture_markers_without_erasing_the_failure_watermark() {
    use crate::delivery::recovery::stage_and_commit;
    let temp = tempfile::tempdir().unwrap();
    let (store, owner, contract, mut outbox) = super::super::super::eviction::remote_box(temp.path()).await;
    let root = temp.path().join("outbox");
    let (_, automatic) = failing(CollectionStop::ByteLimit);
    install(&mut outbox, &automatic).await;
    let manifest = complete(&mut outbox).await;
    let original = original(&root, &manifest);
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Failed { .. }));
    let recorded = backlog(&outbox).await.failure_watermark;
    let prepared = outbox.delivery().prepare(&manifest.capture.observation_id, contract, vec![]).await.unwrap();
    let committed = stage_and_commit(&store, owner.fence(), None, &prepared).await.unwrap();
    assert!(outbox.delivery().evict(&committed).await.unwrap());
    assert_eq!(counts(&outbox).await, (0, 0, 0, 0));
    let state = backlog(&outbox).await;
    assert_eq!((state.pending, state.failed), (0, 0));
    assert_eq!(state.failure_watermark, recorded);
    assert_eq!(state.failure_watermark.latest.as_ref().unwrap().primary, original);
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Idle));
    drop(outbox);
    let mut outbox = Outbox::open(&root).unwrap();
    let changed = AutomaticSynthesis::new(
        Arc::new(NewRules(QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None })),
        CompletedCaptureBudget::default(),
    )
    .unwrap();
    install(&mut outbox, &changed).await;
    assert_ne!(changed.registration(), automatic.registration());
    assert_eq!(backlog(&outbox).await.failure_watermark, recorded);
    assert!(store.load(&committed.manifest().range_id).await.is_ok());
}

#[tokio::test]
async fn the_failed_job_pin_marker_and_watermark_commit_atomically_and_malformed_watermarks_refuse_health() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut outbox) = box_at(&temp);
    let (analysis, automatic) = failing(CollectionStop::RowLimit);
    install(&mut outbox, &automatic).await;
    complete(&mut outbox).await;
    journal_fault(&outbox,"CREATE TRIGGER fail_watermark BEFORE INSERT ON synthesis_failure_watermark BEGIN SELECT RAISE(ABORT,'controlled'); END").await;
    let owner = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    assert!(outbox.delivery().synthesize_pending(&automatic, owner.handle(), ()).await.is_err());
    assert_eq!(counts(&outbox).await, (1, 0, 0, 1));
    empty_watermark(&backlog(&outbox).await.failure_watermark);
    journal_fault(&outbox, "DROP TRIGGER fail_watermark").await;
    analysis.first.store(true, Ordering::Release);
    assert!(matches!(turn(&outbox, &automatic).await, SynthesisTurn::Failed { .. }));
    assert_eq!(backlog(&outbox).await.failure_watermark.total, 1);
    journal_fault(&outbox, "UPDATE synthesis_failure_watermark SET latest_json='{}'").await;
    let owner = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    assert!(outbox.delivery().synthesis_backlog(owner.handle(), ()).await.is_err());
}
