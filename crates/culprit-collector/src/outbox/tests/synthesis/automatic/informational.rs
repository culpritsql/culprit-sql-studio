//! Informational rows remain authenticated capture-wide inputs without acquiring invented semantic IDs.
use super::*;
use culprit_evidence::analytical::test_support::{derived_diagnosis_output, informational_query_evidence};

async fn informational_capture(outbox: &mut Outbox) -> CollectionManifest {
    let (_, records) = informational_query_evidence();
    crate::outbox::tests::informational::capture(outbox, records).await
}

fn capture_wide_analysis() -> Arc<dyn Synthesizer> {
    Arc::new(QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None })
}

async fn assert_empty_output(outbox: &mut Outbox, output: &DiagnosticCaptureRef, primary: &DiagnosticCaptureRef) {
    let manifest = outbox.verify(&output.capture.observation_id).await.unwrap();
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert_eq!(manifest.source_queries, 0);
    assert!(manifest.files.iter().all(|file| file.rows == 0));
    let SourceObservation::DerivedDiagnosis(description) = &manifest.description.as_ref().unwrap().observation else {
        panic!("the ordinary output writer must retain the derived header")
    };
    assert!(description.input_read_complete);
    assert_eq!(description.counts, DerivedDiagnosisCounts { incidents: 0, findings: 0, changes: 0 });
    assert_eq!(description.inputs.len(), 1);
    assert_eq!(&description.inputs[0].capture, primary);
}

#[tokio::test]
async fn manual_capture_wide_synthesis_preserves_unidentified_native_rows_and_refuses_direct_row_references() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let manifest = informational_capture(&mut outbox).await;
    assert_eq!(manifest.status, CollectionStatus::Complete);
    assert!(manifest.references.integrity_complete);
    assert!(!manifest.references.complete);
    let primary = original(&root, &manifest);
    let expected_source = manifest.description.unwrap();
    let expected_coverage = manifest.references;
    let expected_rows = manifest.files.iter().map(|file| file.rows as usize).sum::<usize>();
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    outbox
        .delivery()
        .with_synthesis_inputs(selected(primary.clone(), vec![]), operation.handle(), (), move |input| {
            let capture = input.primary();
            assert_eq!(capture.source(), &expected_source);
            assert_eq!(capture.reference_coverage(), &expected_coverage);
            assert_eq!(capture.records().len(), expected_rows);
            assert_eq!(capture.captured_rows(TelemetryStream::QueryRuntime), Some(3));
            assert_eq!(capture.captured_rows(TelemetryStream::QueryWaits), Some(1));
            let references = capture.row_references().collect::<Result<Vec<_>>>()?;
            assert_eq!(references.len(), expected_rows - 3);
            let mut unresolved = 0;
            for (position, row) in capture.records().iter().enumerate() {
                if row.record_id.is_none() {
                    unresolved += 1;
                    assert!(capture.reference(position).is_err());
                } else {
                    assert!(references.contains(&capture.reference(position)?));
                }
                match &row.record {
                    TelemetryRecord::QueryRuntime(runtime) => assert_eq!(runtime.executions, "9007199254740993"),
                    TelemetryRecord::QueryWaits(wait) => {
                        assert_eq!(wait.total, MeasurementValue::Integer("9007199254740993".into()))
                    }
                    _ => (),
                }
            }
            assert_eq!(unresolved, 3);
            Ok(())
        })
        .await
        .unwrap();
    let output = outbox
        .delivery()
        .synthesize(selected(primary.clone(), vec![]), operation.handle(), (), capture_wide_analysis())
        .await
        .unwrap();
    assert_empty_output(&mut outbox, &output, &primary).await;
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 2);
}

#[tokio::test]
async fn automatic_capture_wide_synthesis_keeps_unresolved_reference_coverage_without_a_failed_job() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let seen = Arc::new(Mutex::new(vec![]));
    let automatic = automatic(seen.clone(), None);
    install(&mut outbox, &automatic).await;
    let manifest = informational_capture(&mut outbox).await;
    let primary = original(&root, &manifest);
    let SynthesisTurn::Committed { primary: actual, output, queue_retired } = turn(&outbox, &automatic).await else {
        panic!("a completely authenticated informational capture must reach capture-wide synthesis")
    };
    assert_eq!(actual, primary);
    assert!(queue_retired);
    assert_empty_output(&mut outbox, &output, &primary).await;
    let health = backlog(&outbox).await;
    assert_eq!((health.pending, health.failed, health.failure_watermark.total), (0, 0, 0));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].primary, primary);
    assert_eq!(seen[0].primary_rows, manifest.files.iter().map(|file| file.rows as usize).sum::<usize>());
    assert!(seen[0].context.is_empty());
}

struct InventedInformationalReference {
    analysis: QueueAnalysis,
}
impl Synthesizer for InventedInformationalReference {
    fn analyzer(&self) -> ComponentVersion {
        self.analysis.analyzer()
    }
    fn rules_version(&self) -> &str {
        self.analysis.rules_version()
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let mut output = self.analysis.synthesize(input)?;
        let capture = input.primary();
        let (position, row) = capture.records().iter().enumerate().find(|(_, row)| row.record_id.is_none()).unwrap();
        assert!(capture.reference(position).is_err());
        let invented = DiagnosticEvidenceRef {
            capture: capture.original().capture.clone(),
            manifest_sha256: capture.original().manifest_sha256.clone(),
            source_scope_id: capture.original().source_scope_id.clone(),
            stream: row.record.stream(),
            native_key: row.record.native_identity(),
            batch_id: row.batch_id.clone(),
            record_id: digest(b"invented-semantic-row-id"),
            payload_sha256: row.payload_sha256()?,
            representation_version: row.representation_version,
            row_ordinal: capture.row_ordinals()[position],
        };
        assert!(invented.is_valid(), "the emitted reference is structurally plausible but unauthenticated");
        assert!(invented.validate_canonical_record(capture.source(), &row.record).is_err());
        let (_, mut example) = derived_diagnosis_output(false, false);
        let mut finding = example.findings.remove(0);
        finding.analysis_id = output.description.analysis_id.clone();
        finding.database = capture.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
        finding.display = input.display().clone();
        finding.inputs = vec![invented];
        output.findings.push(finding);
        output.description.counts.findings = 1;
        output.description.output_coverage.findings = DerivedOutputCoverage::Evaluated;
        output.validate()?;
        Ok(output)
    }
}
fn invented_analysis() -> Arc<dyn Synthesizer> {
    Arc::new(InventedInformationalReference {
        analysis: QueueAnalysis { seen: Arc::new(Mutex::new(vec![])), cancel: None },
    })
}

#[tokio::test]
async fn manual_synthesis_cannot_publish_an_invented_reference_to_an_authenticated_informational_row() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let manifest = informational_capture(&mut outbox).await;
    let operation = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let error = outbox
        .delivery()
        .synthesize(selected(original(&root, &manifest), vec![]), operation.handle(), (), invented_analysis())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 1);
    assert_eq!(outbox.status(10, None).await.unwrap().ready, 1);
}

#[tokio::test]
async fn automatic_synthesis_records_a_failed_job_instead_of_publishing_an_invented_informational_reference() {
    let temp = tempfile::tempdir().unwrap();
    let (root, mut outbox) = box_at(&temp);
    let automatic = AutomaticSynthesis::new(invented_analysis(), CompletedCaptureBudget::default()).unwrap();
    install(&mut outbox, &automatic).await;
    let manifest = informational_capture(&mut outbox).await;
    let primary = original(&root, &manifest);
    let SynthesisTurn::Failed { primary: actual, code } = turn(&outbox, &automatic).await else {
        panic!("an invented emitted reference cannot produce a successful diagnosis")
    };
    assert_eq!(actual, primary);
    assert_eq!(code, DeliveryProblem::Integrity.error().code);
    assert_eq!(outbox.status(10, None).await.unwrap().captures, 1);
    let health = backlog(&outbox).await;
    assert_eq!((health.pending, health.failed, health.failure_watermark.total), (0, 1, 1));
    assert_eq!(health.failure_watermark.latest.unwrap().primary, primary);
}
