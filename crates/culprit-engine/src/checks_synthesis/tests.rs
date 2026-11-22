use super::test_support::CaptureFinding;
use super::*;
use culprit_collector::analytical::test_support::LocalCaptureFixture;
use culprit_collector::outbox::CompletedCaptureBudget;
use culprit_collector::synthesis::SynthesisRequest;
use culprit_core::limits::OperationGuard;
use culprit_evidence::analytical::test_support;
use std::sync::atomic::{AtomicBool, Ordering};

fn synthesizer() -> ChecksSynthesizer {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "controlled-rules-1").unwrap()
}
fn original(fixture: &LocalCaptureFixture) -> DiagnosticCaptureRef {
    fixture.original_reference()
}

async fn run(
    fixture: &LocalCaptureFixture,
    context: Vec<DiagnosticCaptureRef>,
    synthesizer: ChecksSynthesizer,
) -> Result<DerivedDiagnosis> {
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    fixture
        .outbox
        .as_ref()
        .unwrap()
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest { primary: original(fixture), context, budget: CompletedCaptureBudget::default() },
            guard.handle(),
            (),
            move |input| synthesizer.synthesize(&input),
        )
        .await
}
#[tokio::test]
async fn sealed_query_original_reuses_native_checks_without_warehouse_handles_or_source_work() {
    let (source, records) = test_support::query_evidence();
    let fixture = LocalCaptureFixture::query_history(source, records).await;
    assert_eq!(fixture.manifest.source_queries, 0);
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    let output = fixture
        .outbox
        .as_ref()
        .unwrap()
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest {
                primary: original(&fixture),
                context: vec![],
                budget: CompletedCaptureBudget::default(),
            },
            guard.handle(),
            (),
            move |input| {
                let policy = synthesizer();
                let native = input.primary().verified_original()?;
                let expected = crate::collected_evidence::analyze_original_with_catalogs(
                    &native,
                    &native.original().key()?,
                    &policy.pipeline,
                    &crate::collected_evidence::catalogs::CatalogContext::default(),
                )?;
                let output = policy.synthesize(&input)?;
                let expected = expected
                    .into_iter()
                    .flat_map(|analysis| analysis.findings)
                    .map(|finding| culprit_core::redact::redact_finding(&finding, native.source().target.engine()))
                    .collect::<Vec<_>>();
                assert_eq!(output.findings.iter().map(|row| row.finding.clone()).collect::<Vec<_>>(), expected);
                assert_eq!(output.description.inputs[0].capture, *input.primary().original());
                assert_eq!(output.description.recorded_at, canonical_time(input.recorded_at())?);
                assert_eq!(
                    output
                        .findings
                        .iter()
                        .map(|row| &row.display.connection_alias)
                        .collect::<std::collections::BTreeSet<_>>(),
                    if output.findings.is_empty() {
                        Default::default()
                    } else {
                        std::collections::BTreeSet::from([&input.display().connection_alias])
                    }
                );
                assert_eq!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
                assert!(matches!(
                    output.description.output_coverage.changes,
                    DerivedOutputCoverage::ContextUnavailable {
                        reason: DerivedOutputGap::CatalogComparisonUnavailable
                    }
                ));
                assert!(output.changes.is_empty());
                assert!(output.incidents.is_empty());
                assert!(!serde_json::to_string(&output).unwrap().contains("fixture-private-literal"));
                Ok(output)
            },
        )
        .await
        .unwrap();
    output.validate().unwrap();
}
#[tokio::test]
async fn resolved_and_pending_revisions_keep_canonical_history_and_unknown_unopened_queries() {
    for resolved in [false, true] {
        let fixture = LocalCaptureFixture::diagnostic_history(resolved).await;
        let output = run(&fixture, vec![], synthesizer()).await.unwrap();
        assert_eq!(output.incidents.len(), 1);
        let incident = &output.incidents[0];
        assert_eq!(
            incident.entry.lifecycle,
            if resolved { DiagnosticLifecycle::Resolved } else { DiagnosticLifecycle::Active }
        );
        assert_eq!(incident.entry.original.as_ref(), Some(&incident.revision_reference));
        assert!(!incident.entry.evidence.is_empty());
        assert!(incident.affected_queries.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.caller.is_none());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(matches!(output.description.output_coverage.changes, DerivedOutputCoverage::ContextUnavailable { .. }));
        assert_eq!(output.description.output_coverage.incidents, DerivedOutputCoverage::Evaluated);
    }
}
#[tokio::test]
async fn original_affected_query_is_not_relabelled_as_culprit_and_native_clocks_stay_independent() {
    let fixture = LocalCaptureFixture::diagnostic_history(false).await;
    let SourceObservation::Diagnostics(header) = &fixture.manifest.description.as_ref().unwrap().observation else {
        panic!()
    };
    let output = run(&fixture, vec![header.parent.clone()], synthesizer()).await.unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.affected_queries.iter().all(|query| query.causal_role.is_none()));
    assert!(matches!(incident.affected_queries[0].identity, Some(DerivedQueryIdentity::Activity { .. })));
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
    assert!(incident.original_interval.is_none());
    assert_eq!(incident.entry.original.as_ref(), Some(&incident.revision_reference));
    assert!(output.description.contains_capture_for(&incident.affected_queries[0].evidence[0]));
    assert!(
        chrono::DateTime::parse_from_rfc3339(&output.description.recorded_at).unwrap()
            >= chrono::DateTime::parse_from_rfc3339(&output.description.observed_until).unwrap()
    );
}
#[tokio::test]
async fn cancelled_or_tampered_originals_do_not_enter_the_pure_pipeline() {
    let (source, records) = test_support::query_evidence();
    let fixture = LocalCaptureFixture::query_history(source, records).await;
    let entered = Arc::new(AtomicBool::new(false));
    let observed = entered.clone();
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    let operation = guard.handle();
    drop(guard);
    let result = fixture
        .outbox
        .as_ref()
        .unwrap()
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest { primary: original(&fixture), context: vec![], budget: Default::default() },
            operation,
            (),
            move |_| {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!entered.load(Ordering::SeqCst));
    let reference = original(&fixture);
    let file = &fixture.manifest.files[0];
    std::fs::write(fixture.capture_directory.join(&file.file), b"private-synthesis-tamper").unwrap();
    let observed = entered.clone();
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    let error = fixture
        .outbox
        .as_ref()
        .unwrap()
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest { primary: reference, context: vec![], budget: Default::default() },
            guard.handle(),
            (),
            move |_| {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert!(!entered.load(Ordering::SeqCst));
    assert!(!serde_json::to_string(&error).unwrap().contains("private-synthesis-tamper"));
}
#[tokio::test]
async fn unqualified_task_point_has_no_caller_or_allocating_query_but_checked_child_can_retain_db_client() {
    let cases = test_support::task_space_evidence_cases();
    for qualified in [true, false] {
        let (source, mut records) = cases[0].clone();
        records.truncate(1);
        let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TaskSpace(task), .. }) =
            &mut records[0]
        else {
            panic!()
        };
        if qualified {
            task.request.as_mut().unwrap().client = Some(ActivityClient {
                application: Some("native-app".into()),
                user: Some("native-user".into()),
                host: None,
            });
        } else {
            task.request = None;
            task.before_check = None;
            task.after_check = None;
            task.statement_identity = None;
            task.plan_identity = None;
        }
        let mut source = source;
        let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
        header.records = 1;
        header.sources[0] = StateSourceCoverage::observed(header.sources[0].source.clone(), 1, true);
        let fixture = LocalCaptureFixture::snapshot(source, records).await;
        let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
        fixture
            .outbox
            .as_ref()
            .unwrap()
            .delivery()
            .with_synthesis_inputs(
                SynthesisRequest { primary: original(&fixture), context: vec![], budget: Default::default() },
                guard.handle(),
                (),
                move |input| {
                    let view = input.primary().verified_original()?;
                    let explanation = explanations::original_observation(&view, 0)?;
                    if qualified {
                        assert_eq!(explanation.candidates.len(), 1);
                        let caller = explanation.candidates[0].caller.as_ref().unwrap();
                        assert!(caller.is_valid());
                        assert_eq!(caller.client.as_ref().unwrap().user.as_deref(), Some("native-user"));
                        assert_eq!(caller.evidence.stream, TelemetryStream::OperationalSamples);
                    } else {
                        assert!(explanation.candidates.is_empty());
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn injected_pipeline_rules_and_capture_wide_basis_are_preserved_and_output_is_bounded_before_retention() {
    for many in [false, true] {
        let (source, records) = test_support::query_evidence();
        let fixture = LocalCaptureFixture::query_history(source, records).await;
        let pipeline = Arc::new(culprit_checks::Pipeline::builtin().with_check(Arc::new(CaptureFinding { many })));
        let policy = ChecksSynthesizer::new(pipeline, "controlled-plugin-policy-2").unwrap();
        assert_eq!(policy.rules_version(), "controlled-plugin-policy-2");
        let result = run(&fixture, vec![], policy).await;
        if many {
            assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
        } else {
            let output = result.unwrap();
            let finding = output.findings.iter().find(|row| row.finding.check == "controlled-capture").unwrap();
            assert!(finding.inputs.is_empty());
            assert!(!output.description.inputs.is_empty());
            assert_eq!(output.description.rules_version, "controlled-plugin-policy-2");
        }
    }
}

#[tokio::test]
async fn independent_native_populations_retain_compact_assessments_when_findings_are_empty_or_partial() {
    for (source, records) in test_support::query_population_evidence_cases().into_iter().take(4) {
        let fixture = LocalCaptureFixture::query_history(source.clone(), records).await;
        let output = run(&fixture, vec![], synthesizer()).await.unwrap();
        let evaluations = &output.description.finding_evaluations;
        assert!(evaluations.population_count > 0);
        assert_eq!(evaluations.population_count as usize, evaluations.populations.len());
        assert_eq!(evaluations.omitted_populations, 0);
        assert!(evaluations.checks.iter().any(|state| state.status == CheckStatus::Unavailable));
        assert!(evaluations.populations.iter().all(|population| population.input == 0));
        assert!(output.description.input_read_complete);
        assert!(!output.description.limitations().is_empty());
        assert!(serde_json::to_vec(&output.description).unwrap().len() < 128 * 1024);
    }
}
#[tokio::test]
async fn missing_context_adds_plain_reasons_without_changing_native_lifecycle_or_selected_body_completeness() {
    let fixture = LocalCaptureFixture::diagnostic_history(true).await;
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    fixture
        .outbox
        .as_ref()
        .unwrap()
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest { primary: original(&fixture), context: vec![], budget: Default::default() },
            guard.handle(),
            (),
            move |input| {
                let originals =
                    input.captures().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
                let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
                let output = synthesizer().analyze_originals(
                    &borrowed,
                    input.recorded_at(),
                    input.display(),
                    vec![DerivedContextIssue::MissingOriginal, DerivedContextIssue::CatalogNotRetainedAsOf],
                )?;
                assert!(output.description.input_read_complete);
                assert_eq!(output.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
                assert!(
                    output.incidents[0].entry.reasons.contains(&DerivedContextIssue::MissingOriginal.explanation())
                );
                assert!(output.incidents[0].affected_queries.is_empty());
                assert!(output.incidents[0].culprit_queries.is_empty());
                assert_eq!(output.description.limitations().len(), 4);
                Ok(())
            },
        )
        .await
        .unwrap();
}
#[test]
fn real_saved_check_shapes_share_state_and_bound_large_multi_population_details_without_healthy_absence() {
    use culprit_core::collected_analysis::{
        CollectedAnalysisLimit, CollectedAnalysisLimitation, CollectedQueryAnalysisResult, CollectedQueryPopulation,
    };
    let report: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/mssql-demo.report.json")).unwrap();
    let assessments: Vec<CheckAssessment> = serde_json::from_value(report["assessments"].clone()).unwrap();
    let locked: Vec<LockedCheck> = serde_json::from_value(report["locked"].clone()).unwrap();
    let mut evaluations = evaluation::Evaluations::new();
    for index in 0..100u32 {
        let analysis = CollectedQueryAnalysisResult {
            runtime_totals: None,
            population: CollectedQueryPopulation::Population { identity: NativeIdentity::new(index.to_string()) },
            query_inputs: vec![],
            workload: None,
            findings: vec![],
            assessments: assessments.clone(),
            locked: locked.clone(),
            limitations: vec![CollectedAnalysisLimitation {
                record_id: None,
                reason: CollectedAnalysisLimit::MissingSchema,
            }],
            provenance: None,
        };
        evaluations.add(&analysis, 0, &[], Engine::Mssql).unwrap();
    }
    let compact = evaluations.finish();
    assert_eq!(compact.population_count, 100);
    let expected_ids = assessments
        .iter()
        .map(|assessment| assessment.check.as_str())
        .chain(locked.iter().map(|lock| lock.check.id.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(compact.checks.len(), expected_ids.len());
    for lock in &locked {
        let state = compact.checks.iter().find(|state| state.check == lock.check.id).unwrap();
        assert_eq!(state.status, CheckStatus::Unavailable);
        assert_eq!(state.missing, lock.missing);
        let original = assessments.iter().find(|assessment| assessment.check == lock.check.id).unwrap();
        assert_eq!(
            state.reason,
            original.reason.as_ref().map(|reason| culprit_core::redact::redact_prose(reason, Engine::Mssql))
        );
    }
    assert_eq!(compact.populations.len() as u32 + compact.omitted_populations, 100);
    assert!(compact.omitted_populations > 0, "the manifest budget preserves explicit omitted coverage");
    assert!(serde_json::to_vec(&compact).unwrap().len() <= MAX_DERIVED_EVALUATION_BYTES);
}

#[test]
fn assessment_and_lock_are_one_unavailable_check_while_repeated_or_contradictory_facets_refuse() {
    use culprit_core::collected_analysis::{CollectedQueryAnalysisResult, CollectedQueryPopulation};
    let report: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/mssql-demo.report.json")).unwrap();
    let assessment: CheckAssessment = serde_json::from_value(
        report["assessments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|assessment| assessment["check"] == "repeated-request")
            .unwrap()
            .clone(),
    )
    .unwrap();
    let lock: LockedCheck = serde_json::from_value(report["locked"][0].clone()).unwrap();
    assert_eq!(assessment.check, lock.check.id);
    let mut analysis = CollectedQueryAnalysisResult {
        runtime_totals: None,
        population: CollectedQueryPopulation::Unknown,
        query_inputs: vec![],
        workload: None,
        findings: vec![],
        assessments: vec![assessment.clone()],
        locked: vec![lock.clone()],
        limitations: vec![],
        provenance: None,
    };
    let mut inventory = evaluation::Evaluations::new();
    inventory.add(&analysis, 0, &[], Engine::Mssql).unwrap();
    let compact = inventory.finish();
    assert_eq!(compact.checks.len(), 1);
    assert_eq!(compact.populations[0].checks.len(), 1);
    assert_eq!(compact.checks[0].reason, assessment.reason);
    assert_eq!(compact.checks[0].missing, lock.missing);
    assert_eq!(compact.checks[0].status, CheckStatus::Unavailable);
    analysis.assessments[0].status = CheckStatus::Assessed;
    assert!(evaluation::Evaluations::new().add(&analysis, 0, &[], Engine::Mssql).is_err());
    analysis.assessments[0] = assessment.clone();
    analysis.assessments.push(assessment);
    assert!(evaluation::Evaluations::new().add(&analysis, 0, &[], Engine::Mssql).is_err());
    analysis.assessments.pop();
    analysis.locked.push(lock);
    assert!(evaluation::Evaluations::new().add(&analysis, 0, &[], Engine::Mssql).is_err());
}
