use super::*;
use crate::synthesis::{SynthesisInput, SynthesisRequest, Synthesizer};
use culprit_core::{AnalysisOrigin, ComponentVersion, limits::OperationGuard};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

fn original(root: &Path, manifest: &CollectionManifest) -> DiagnosticCaptureRef {
    DiagnosticCaptureRef {
        capture: manifest.capture.clone(),
        manifest_sha256: digest(
            &fs::read(root.join("captures").join(&manifest.capture.observation_id).join("manifest.json")).unwrap(),
        ),
        source_scope_id: manifest.description.as_ref().unwrap().captured_range().unwrap().scope_id,
    }
}
fn selected(primary: DiagnosticCaptureRef, context: Vec<DiagnosticCaptureRef>) -> SynthesisRequest {
    SynthesisRequest { primary, context, budget: CompletedCaptureBudget::default() }
}

struct ControlledAnalysis {
    invalid_reference: bool,
    cancel: Option<Arc<Mutex<Option<OperationGuard>>>>,
}
impl Synthesizer for ControlledAnalysis {
    fn analyzer(&self) -> ComponentVersion {
        ComponentVersion::package("controlled-offline-analysis", "1", None)
    }
    fn rules_version(&self) -> &str {
        "controlled-1"
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let mut description = DerivedDiagnosisDescription {
            analysis_id: String::new(),
            analyzer: self.analyzer(),
            rules_version: "controlled-1".into(),
            language: "en".into(),
            display: input.display().clone(),
            recorded_at: input.recorded_at().into(),
            observed_from: input.primary().collected_from().into(),
            observed_until: input.primary().collected_until().into(),
            inputs: input
                .captures()
                .map(|capture| DerivedInputReference {
                    capture: capture.original().clone(),
                    role: DerivedInputRole::Observation,
                    catalog: None,
                })
                .collect(),
            origin: AnalysisOrigin::Collected,
            input_read_complete: true,
            counts: DerivedDiagnosisCounts { incidents: 0, findings: 1, changes: 0 },
            finding_evaluations: Default::default(),
            change_evaluations: vec![],
            context_issues: input.derived_context_issues(),
            output_coverage: DerivedOutputCoverageSet {
                incidents: DerivedOutputCoverage::ContextUnavailable {
                    reason: DerivedOutputGap::NoDiagnosticRevisions,
                },
                findings: DerivedOutputCoverage::Evaluated,
                changes: DerivedOutputCoverage::ContextUnavailable {
                    reason: DerivedOutputGap::CatalogComparisonUnavailable,
                },
            },
        };
        description.analysis_id = description.expected_analysis_id()?;
        let mut reference = input.primary().reference(0)?;
        if self.invalid_reference {
            reference.payload_sha256 = digest(b"not-the-authenticated-row");
        }
        let finding = culprit_core::Finding {
            id: "controlled:retained".into(),
            check: "controlled".into(),
            category: culprit_core::Category::Queries,
            severity: culprit_core::Severity::Low,
            actionability: culprit_core::Actionability::Information,
            confidence: culprit_core::Confidence::High,
            title: "Captured evidence is available".into(),
            summary: "The selected capture was read completely".into(),
            subject: culprit_core::Subject {
                kind: culprit_core::SubjectKind::Database,
                name: "Selected database".into(),
            },
            impact: Default::default(),
            metrics: vec![],
            queries: vec![],
            fix: None,
            verify: vec!["Inspect the retained capture".into()],
            verification_basis: culprit_core::VerificationBasis::Rescan,
            caveats: vec![],
            last_seen: None,
            inactive: false,
            facts: Default::default(),
        };
        let diagnosis = DerivedDiagnosis {
            findings: vec![DerivedFinding {
                analysis_id: description.analysis_id.clone(),
                database: input
                    .primary()
                    .source()
                    .target
                    .database()
                    .map(IncidentDatabaseSelector::from_route)
                    .transpose()?,
                display: input.display().clone(),
                finding,
                inputs: vec![reference],
                query: None,
                cause: None,
                guidance: DerivedGuidance::HistoricalRequiresFreshVerification,
            }],
            description,
            incidents: vec![],
            changes: vec![],
        };
        if let Some(guard) = &self.cancel {
            drop(guard.lock().unwrap().take());
        }
        Ok(diagnosis)
    }
}
fn analyzer() -> Arc<dyn Synthesizer> {
    Arc::new(ControlledAnalysis { invalid_reference: false, cancel: None })
}

#[tokio::test]
async fn synthesis_seals_real_canonical_output_without_native_work_and_replays_the_same_receipt_after_restart() {
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let parent = original(&root, &native);
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let output = outbox
        .delivery()
        .synthesize(selected(parent.clone(), vec![]), operation.handle(), (), analyzer())
        .await
        .unwrap();
    let manifest = outbox.verify(&output.capture.observation_id).await.unwrap();
    assert_eq!(manifest.source_queries, 0);
    assert_eq!(manifest.request.collection(), TelemetryCollection::DerivedDiagnosis);
    assert_eq!(
        manifest.files.iter().map(|file| (file.stream, file.rows)).collect::<Vec<_>>(),
        vec![
            (TelemetryStream::DerivedIncidents, 0),
            (TelemetryStream::DerivedFindings, 1),
            (TelemetryStream::DerivedChanges, 0),
        ]
    );
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
    drop(outbox);
    let outbox = Outbox::open(&root).unwrap();
    let replay =
        outbox.delivery().synthesize(selected(parent, vec![]), operation.handle(), (), analyzer()).await.unwrap();
    assert_eq!(replay, output, "restart and a new synthesis timestamp must not duplicate the diagnosis");
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
}

#[tokio::test]
async fn cumulative_context_limits_duplicate_captures_and_a_late_corrupt_file_never_reach_the_analyzer() {
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let first = complete(&mut outbox).await;
    let second = complete(&mut outbox).await;
    let primary = original(&root, &first);
    let context = original(&root, &second);
    let called = Arc::new(AtomicBool::new(false));
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    for request in [
        selected(primary.clone(), vec![primary.clone()]),
        SynthesisRequest {
            budget: CompletedCaptureBudget { max_rows: 5, ..Default::default() },
            ..selected(primary.clone(), vec![context.clone()])
        },
    ] {
        let flag = called.clone();
        assert!(
            outbox
                .delivery()
                .with_synthesis_inputs(request, operation.handle(), (), move |_| {
                    flag.store(true, Ordering::Release);
                    Ok(())
                })
                .await
                .is_err()
        );
        assert!(!called.load(Ordering::Acquire));
    }
    let file = root.join("captures").join(&second.capture.observation_id).join(&second.files.last().unwrap().file);
    let mut bytes = fs::read(&file).unwrap();
    bytes.push(b'x');
    fs::write(&file, bytes).unwrap();
    let flag = called.clone();
    assert!(
        outbox
            .delivery()
            .with_synthesis_inputs(selected(primary, vec![context]), operation.handle(), (), move |_| {
                flag.store(true, Ordering::Release);
                Ok(())
            })
            .await
            .is_err()
    );
    assert!(!called.load(Ordering::Acquire), "no successful prefix may escape a failed canonical EOF proof");
}

#[tokio::test]
async fn an_invented_output_row_reference_and_caller_cancellation_cannot_publish_a_diagnosis() {
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let parent = original(&root, &native);
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let invalid = Arc::new(ControlledAnalysis { invalid_reference: true, cancel: None });
    assert!(
        outbox.delivery().synthesize(selected(parent.clone(), vec![]), operation.handle(), (), invalid).await.is_err()
    );
    let cancel_owner =
        Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let handle = cancel_owner.lock().unwrap().as_ref().unwrap().handle();
    let cancelled = Arc::new(ControlledAnalysis { invalid_reference: false, cancel: Some(cancel_owner) });
    assert!(outbox.delivery().synthesize(selected(parent, vec![]), handle, (), cancelled).await.is_err());
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 1);
}

#[tokio::test]
async fn a_delivery_synthesis_handle_never_recovers_or_interrupts_a_live_native_lease() {
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let parent = original(&root, &native);
    let delivery = outbox.delivery();
    let mut live = outbox.reserve("fixture", &request()).await.unwrap();
    live.source_started().await.unwrap();
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let error =
        delivery.synthesize(selected(parent.clone(), vec![]), operation.handle(), (), analyzer()).await.unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::RecoveryRequired))));
    let status = delivery.status(10, None).await.unwrap();
    let owner = status.entries.iter().find(|entry| entry.capture == *live.identity()).unwrap();
    assert_eq!(owner.state, OutboxCaptureState::Capturing);
    assert_eq!(owner.source_work, SourceWorkState::Active);
    live.source_closed(SourceShutdown::Quiescent).await.unwrap();
    let mut files = ObservationFiles::create(live.directory(), request(), live.identity().clone(), None).unwrap();
    files.describe(source()).unwrap();
    for (index, stream) in TelemetryCollection::QueryHistory.streams().iter().enumerate() {
        files.write_stream(index, &records(*stream)).unwrap();
    }
    files.confirm_source(SourceCompletion::Verified).unwrap();
    files.finish(0).unwrap();
    live.finish().await.unwrap();
    assert!(delivery.synthesize(selected(parent, vec![]), operation.handle(), (), analyzer()).await.is_ok());
    assert_eq!(delivery.status(10, None).await.unwrap().source_debts, 0);
}

#[tokio::test]
async fn a_known_committed_diagnosis_keeps_its_receipt_after_caller_cancellation() {
    use crate::outbox::{artifacts::VerificationObserver, completed_capture::CaptureWork};
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let parent = original(&root, &native);
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let handle = operation.handle();
    let output = outbox
        .delivery()
        .with_synthesis_worker(selected(parent, vec![]), handle.clone(), (), move |files, read, input| {
            let analyzer = ControlledAnalysis { invalid_reference: false, cancel: None };
            let diagnosis = analyzer.synthesize(&input)?;
            let receipt = files.commit_synthesized_output(&input, diagnosis, &analyzer.analyzer(), &|| read.check())?;
            drop(operation);
            assert!(read.check().is_err());
            Ok(CaptureWork::Committed(receipt))
        })
        .await
        .unwrap();
    assert!(handle.is_cancelled());
    assert_eq!(outbox.verify(&output.capture.observation_id).await.unwrap().status, CollectionStatus::Complete);
}

#[tokio::test]
async fn cancellation_during_local_stream_writes_cannot_be_recovered_as_a_completed_diagnosis() {
    use crate::outbox::{artifacts::VerificationObserver, completed_capture::CaptureWork};
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let parent = original(&root, &native);
    let owner = Arc::new(Mutex::new(Some(OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap())));
    let handle = owner.lock().unwrap().as_ref().unwrap().handle();
    let observed = Arc::new(AtomicBool::new(false));
    let witness = observed.clone();
    let directory = root.clone();
    let error = outbox
        .delivery()
        .with_synthesis_worker(selected(parent.clone(), vec![]), handle, (), move |files, read, input| {
            let analyzer = ControlledAnalysis { invalid_reference: false, cancel: None };
            let diagnosis = analyzer.synthesize(&input)?;
            let check = || {
                for entry in fs::read_dir(directory.join("captures")).unwrap() {
                    let path = entry.unwrap().path().join("manifest.json");
                    let Some(manifest) =
                        fs::read(path).ok().and_then(|bytes| serde_json::from_slice::<CollectionManifest>(&bytes).ok())
                    else {
                        continue;
                    };
                    if manifest.request.collection() == TelemetryCollection::DerivedDiagnosis
                        && manifest.files.iter().any(|file| file.status == CollectionStatus::Complete)
                    {
                        witness.store(true, Ordering::Release);
                        drop(owner.lock().unwrap().take());
                    }
                }
                read.check()
            };
            files.commit_synthesized_output(&input, diagnosis, &analyzer.analyzer(), &check).map(CaptureWork::Committed)
        })
        .await
        .unwrap_err();
    assert!(observed.load(Ordering::Acquire), "cancellation occurred after real stream sealing began");
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    outbox.recover().await.unwrap();
    assert_eq!(
        outbox.status(10, None).await.unwrap().ready,
        1,
        "an incomplete output cannot become Ready on restart recovery"
    );
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    assert!(outbox.delivery().synthesize(selected(parent, vec![]), operation.handle(), (), analyzer()).await.is_ok());
}

struct OversizedAnalysis;
impl Synthesizer for OversizedAnalysis {
    fn analyzer(&self) -> ComponentVersion {
        analyzer().analyzer()
    }
    fn rules_version(&self) -> &str {
        "controlled-1"
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let mut output = ControlledAnalysis { invalid_reference: false, cancel: None }.synthesize(input)?;
        let mut row = output.findings.remove(0);
        row.finding.summary = "x".repeat(80 * 1024);
        output.findings = (0..256)
            .map(|index| {
                let mut row = row.clone();
                row.finding.id = format!("controlled:{index}");
                row
            })
            .collect();
        output.description.counts.findings = 256;
        output.validate()?;
        Ok(output)
    }
}
#[tokio::test]
async fn individually_valid_rows_over_the_aggregate_allowance_are_rejected_before_capture_allocation() {
    let temp = tempfile::TempDir::new().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let native = complete(&mut outbox).await;
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let error = outbox
        .delivery()
        .synthesize(selected(original(&root, &native), vec![]), operation.handle(), (), Arc::new(OversizedAnalysis))
        .await
        .unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Capacity))));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 1);
}

mod automatic;
