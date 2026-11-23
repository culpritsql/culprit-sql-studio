use super::*;
use culprit_core::collected_analysis::CollectedAnalysisLimit as Limit;
use culprit_evidence::{
    analytical::{
        evidence::{EvidenceFamily, EvidenceReadBudget, EvidenceReader},
        indexes::ProjectionIndexes,
        test_support::{self, ReadOnlyFixture, SettingsFindingCase},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::reopen},
};
use std::sync::atomic::{AtomicU64, Ordering};

fn evidence(complete: bool, calls: &str) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, _) = test_support::settings_finding_evidence(SettingsFindingCase::Enabled);
    let SourceObservation::SnapshotAcquisition { snapshot, execution, settings_context } = &mut source.observation
    else {
        panic!("actual final acquisition fixture")
    };
    *settings_context = None;
    snapshot.dataset = SnapshotDataset::StatementCounters;
    snapshot.records = 1;
    snapshot.sources = vec![StateSourceCoverage::observed("pg_stat_statements".into(), 1, complete)];
    source.partition = execution.partition.clone();
    let sample = StatementCounterSample {
        member: None,
        identity: NativeQueryIdentity {
            query_id: "17".into(),
            qualifiers: [
                ("dbid".into(), "16384".into()),
                ("userid".into(), "41".into()),
                ("toplevel".into(), "true".into()),
                ("stats_since".into(), "2020-01-01T10:00:00Z".into()),
                ("postmaster_started_at".into(), "2020-01-01T00:00:00Z".into()),
            ]
            .into(),
        },
        scope: MeasurementScope::Database,
        observed_at: snapshot.observed_until.clone(),
        reset: ResetMarker::At("2020-01-01T10:00:00Z".into()),
        totals: vec![
            StatementCounter {
                key: RuntimeMetricKey::Executions,
                unit: RuntimeMetricUnit::Count,
                total: MeasurementValue::Integer(calls.into()),
            },
            StatementCounter {
                key: RuntimeMetricKey::Duration,
                unit: RuntimeMetricUnit::Milliseconds,
                total: MeasurementValue::Real(if calls == "0" { 0.0 } else { 20.0 }),
            },
        ],
        distributions: vec![],
    };
    let records = vec![TelemetryRecord::Operational(OperationalObservation {
        source: "pg_stat_statements".into(),
        key: sample.key().unwrap(),
        fact: OperationalFact::StatementCounters(Box::new(sample)),
    })];
    (source, records)
}

async fn seal_original(source: CollectionDescription, records: Vec<TelemetryRecord>) -> EvidenceCapture {
    static NEXT: AtomicU64 = AtomicU64::new(8_200_000);
    let fixture = ReadOnlyFixture::capture_at(
        source,
        records,
        NEXT.fetch_add(1, Ordering::SeqCst),
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let transport = fixture.transport();
    let recovery = ObjectRecoveryReader::open(transport.clone(), fixture.contract).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store, Arc::new(ProjectionIndexes::new(transport)));
    reader
        .read_capture(&range, &[EvidenceFamily::OperationalSamples], EvidenceReadBudget::default(), None)
        .await
        .unwrap()
}

fn project(capture: &EvidenceCapture) -> DerivedDiagnosis {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "controlled-counter-assessment-1")
        .unwrap()
        .synthesize_captures(
            capture,
            &[],
            "2020-01-01T12:02:00Z",
            &DerivedDisplayContext {
                connection_alias: "Approved counter alias".into(),
                environment: None,
                database_name: None,
            },
        )
        .unwrap()
}
fn assessment(output: &DerivedDiagnosis) -> &DerivedFindingAssessment {
    assert_eq!(output.description.finding_evaluations.assessment_count, 1);
    let [assessment] = output.description.finding_evaluations.assessments.as_slice() else { panic!() };
    assert_eq!(assessment.scope, DerivedFindingScope::StatementCounters);
    assessment
}

#[tokio::test]
async fn canonical_pg_counter_point_has_explicit_application_analysis_gaps_and_no_fabricated_findings() {
    let (source, records) = evidence(true, "4");
    let capture = seal_original(source, records).await;
    assert!(capture.is_read_complete());
    let original = serde_json::to_vec(capture.source()).unwrap();
    let output = project(&capture);
    output.validate().unwrap();
    assert!(output.findings.is_empty() && output.incidents.is_empty());
    let assessed = assessment(&output);
    for reason in [
        Limit::MissingQueryDefinition,
        Limit::UnknownStatementOrigin,
        Limit::RuntimeDenominatorUnavailable,
        Limit::ExecutionPopulationUnknown,
    ] {
        assert!(assessed.limitations.contains(&reason));
    }
    assert_eq!(assessed.input, 0);
    assert!(assessed.checks.is_empty());
    assert_eq!(output.description.inputs[0].capture, capture.verified_original().unwrap().original().clone());
    assert!(matches!(
        output.description.output_coverage.findings,
        DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::AnalysisSourceUnavailable }
    ));
    assert!(output.description.limitations().contains(&culprit_core::t!("view.synthesis.limit.analysis_source")));
    assert_eq!(serde_json::to_vec(capture.source()).unwrap(), original);
    let serialized = serde_json::to_string(&output).unwrap();
    for private in ["fixture.invalid", "native-pg-system", "native-pg-member"] {
        assert!(!serialized.contains(private));
    }
    let reopened: DerivedDiagnosis = serde_json::from_str(&serialized).unwrap();
    reopened.validate().unwrap();
    assert_eq!(assessment(&reopened), assessed);
}

#[tokio::test]
async fn native_partial_and_measured_zero_points_do_not_become_healthy_empty_application_workloads() {
    for (complete, calls) in [(false, "4"), (true, "0")] {
        let (source, records) = evidence(complete, calls);
        let capture = seal_original(source, records).await;
        let output = project(&capture);
        assert!(output.findings.is_empty());
        assert!(output.description.input_read_complete, "selected canonical EOF is independent of native coverage");
        assert_eq!(assessment(&output).limitations.contains(&Limit::RuntimeBounds), !complete);
        assert!(matches!(
            output.description.output_coverage.findings,
            DerivedOutputCoverage::ContextUnavailable { .. }
        ));
    }
}

#[tokio::test]
async fn raw_integer_precision_and_missing_duration_stay_explicit_without_invented_sql_or_origin() {
    let (source, records) = evidence(true, "9007199254740993");
    let capture = seal_original(source, records).await;
    let output = project(&capture);
    assert!(assessment(&output).limitations.contains(&Limit::CounterPrecision));
    let (source, mut records) = evidence(true, "4");
    let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
    let OperationalFact::StatementCounters(sample) = &mut row.fact else { panic!() };
    sample.totals.retain(|counter| counter.key != RuntimeMetricKey::Duration);
    let capture = seal_original(source, records).await;
    let output = project(&capture);
    assert!(assessment(&output).limitations.contains(&Limit::DurationUnavailable));
    assert!(output.findings.is_empty());
}

#[tokio::test]
async fn legacy_and_distributed_originals_preserve_native_scope_without_a_database_wide_application_claim() {
    for (source, records) in test_support::statement_work_evidence_cases() {
        let capture = seal_original(source, records).await;
        let output = project(&capture);
        output.validate().unwrap();
        assert!(output.findings.is_empty());
        assert!(assessment(&output).limitations.contains(&Limit::ExecutionPopulationUnknown));
        assert!(matches!(
            output.description.output_coverage.findings,
            DerivedOutputCoverage::ContextUnavailable { .. }
        ));
    }
}

#[cfg(feature = "collector")]
#[tokio::test]
async fn automatic_completed_counter_capture_seals_its_assessment_through_the_existing_queue_with_zero_native_work() {
    use culprit_collector::{
        analytical::test_support::LocalCaptureFixture,
        outbox::CompletedCaptureBudget,
        synthesis::{AutomaticSynthesis, SynthesisTurn},
    };
    use culprit_core::limits::OperationGuard;
    let policy =
        ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "automatic-counter-assessment-1")
            .unwrap();
    let automatic = AutomaticSynthesis::new(Arc::new(policy), CompletedCaptureBudget::default()).unwrap();
    let (source, records) = evidence(true, "4");
    let mut fixture = Box::pin(LocalCaptureFixture::with_automatic_synthesis(source, records, &automatic)).await;
    assert_eq!(fixture.manifest.status, CollectionStatus::Complete);
    let original = fixture.original_reference();
    let outbox = fixture.outbox.as_mut().unwrap();
    let guard = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
    let SynthesisTurn::Committed { primary, output, queue_retired } =
        outbox.delivery().synthesize_pending(&automatic, guard.handle(), ()).await.unwrap()
    else {
        panic!("actual queued synthesis commits")
    };
    assert_eq!(primary, original);
    assert!(queue_retired);
    let sealed = outbox.verify(&output.capture.observation_id).await.unwrap();
    assert_eq!(sealed.status, CollectionStatus::Complete);
    assert_eq!(sealed.source_queries, 0);
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    let SourceObservation::DerivedDiagnosis(header) = sealed.description.unwrap().observation else { panic!() };
    assert_eq!(header.inputs[0].capture, original);
    assert_eq!(header.counts.findings, 0);
    assert_eq!(header.finding_evaluations.assessments[0].scope, DerivedFindingScope::StatementCounters);
    assert!(header.finding_evaluations.assessments[0].limitations.contains(&Limit::MissingQueryDefinition));
    assert!(matches!(header.output_coverage.findings, DerivedOutputCoverage::ContextUnavailable { .. }));
}
