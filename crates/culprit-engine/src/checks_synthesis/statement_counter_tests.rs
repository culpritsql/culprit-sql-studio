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

fn identity_for(query_id: &str, user: &str, toplevel: bool, since: &str) -> NativeQueryIdentity {
    NativeQueryIdentity {
        query_id: query_id.into(),
        qualifiers: [
            ("dbid".into(), "16384".into()),
            ("userid".into(), user.into()),
            ("toplevel".into(), toplevel.to_string()),
            ("stats_since".into(), canonical_time(since).unwrap()),
            ("postmaster_started_at".into(), canonical_time("2020-01-01T00:00:00Z").unwrap()),
        ]
        .into(),
    }
}

fn counter_row(
    identity: NativeQueryIdentity,
    observed_at: &str,
    calls: u64,
    total_ms: f64,
    hits: u64,
    reads: u64,
    rows: u64,
) -> TelemetryRecord {
    let since = identity.qualifiers["stats_since"].clone();
    let sample = StatementCounterSample {
        member: None,
        identity,
        scope: MeasurementScope::Database,
        observed_at: observed_at.into(),
        reset: ResetMarker::At(since),
        totals: vec![
            StatementCounter {
                key: RuntimeMetricKey::Executions,
                unit: RuntimeMetricUnit::Count,
                total: MeasurementValue::Integer(calls.to_string()),
            },
            StatementCounter {
                key: RuntimeMetricKey::Duration,
                unit: RuntimeMetricUnit::Milliseconds,
                total: MeasurementValue::Real(total_ms),
            },
            StatementCounter {
                key: RuntimeMetricKey::Rows,
                unit: RuntimeMetricUnit::Count,
                total: MeasurementValue::Integer(rows.to_string()),
            },
            StatementCounter {
                key: RuntimeMetricKey::Native("postgres.shared-block-hits".into()),
                unit: RuntimeMetricUnit::Pages { bytes_per_page: 8192 },
                total: MeasurementValue::Integer(hits.to_string()),
            },
            StatementCounter {
                key: RuntimeMetricKey::Native("postgres.shared-block-reads".into()),
                unit: RuntimeMetricUnit::Pages { bytes_per_page: 8192 },
                total: MeasurementValue::Integer(reads.to_string()),
            },
            StatementCounter {
                key: RuntimeMetricKey::TemporaryBlocksWritten,
                unit: RuntimeMetricUnit::Pages { bytes_per_page: 8192 },
                total: MeasurementValue::Integer("512".into()),
            },
        ],
        distributions: vec![],
    };
    TelemetryRecord::Operational(OperationalObservation {
        source: "pg_stat_statements".into(),
        key: sample.key().unwrap(),
        fact: OperationalFact::StatementCounters(Box::new(sample)),
    })
}

fn definition_capture_from(
    counter_source: &CollectionDescription,
    from: &str,
    until: &str,
    entries: Vec<(NativeQueryIdentity, StatementOrigin, DefinitionPayload)>,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mut source = counter_source.clone();
    source.source = "postgres-statement-definitions".into();
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut source.observation else { panic!() };
    snapshot.dataset = SnapshotDataset::StatementDefinitions;
    snapshot.observed_from = from.into();
    snapshot.observed_until = until.into();
    snapshot.records = entries.len() as u32;
    snapshot.sources = vec![StateSourceCoverage::observed("pg_stat_statements".into(), entries.len() as u32, true)];
    execution.observed_from = from.into();
    execution.observed_until = until.into();
    let records = entries
        .into_iter()
        .map(|(identity, origin, sql)| {
            let def = QueryDefinition {
                id: identity.definition_id(&source).unwrap(),
                origin,
                monitoring_template_id: None,
                identity,
                variant_family: None,
                context: Default::default(),
                context_available: false,
                source_text_bytes: Some("64".into()),
                sql,
            };
            TelemetryRecord::ObjectDefinition(
                ObjectDefinitionRecord::new(
                    "pg_stat_statements".into(),
                    ObjectDefinitionFact::Statement(Box::new(def)),
                )
                .unwrap(),
            )
        })
        .collect();
    (source, records)
}

async fn seal_stream(
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    family: EvidenceFamily,
    collected_from: &str,
    collected_until: &str,
) -> EvidenceCapture {
    static NEXT_STREAM: AtomicU64 = AtomicU64::new(8_300_000);
    let fixture = ReadOnlyFixture::capture_at(
        source,
        records,
        NEXT_STREAM.fetch_add(1, Ordering::SeqCst),
        collected_from,
        collected_until,
    )
    .await;
    let transport = fixture.transport();
    let recovery = ObjectRecoveryReader::open(transport.clone(), fixture.contract).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let reader = EvidenceReader::new(fixture.store, Arc::new(ProjectionIndexes::new(transport)));
    reader.read_capture(&range, &[family], EvidenceReadBudget::default(), None).await.unwrap()
}

fn project_with_context(primary: &EvidenceCapture, context: &[&EvidenceCapture]) -> DerivedDiagnosis {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "controlled-counter-definitions-1")
        .unwrap()
        .synthesize_captures(
            primary,
            context,
            "2020-01-01T12:05:00Z",
            &DerivedDisplayContext {
                connection_alias: "Approved counter alias".into(),
                environment: None,
                database_name: None,
            },
        )
        .unwrap()
}

#[tokio::test]
async fn same_owner_pg_definitions_and_counters_emit_performance_findings_without_cross_entry_or_culprit_pollution() {
    let (mut counter_source, _) = evidence(true, "1");
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut counter_source.observation else {
        panic!()
    };
    snapshot.observed_from = "2020-01-01T12:00:00Z".into();
    snapshot.observed_until = "2020-01-01T12:00:01Z".into();
    execution.observed_from = snapshot.observed_from.clone();
    execution.observed_until = snapshot.observed_until.clone();

    let heavy_id = identity_for("101", "41", true, "2020-01-01T08:00:00Z");
    let culprit_id = identity_for("102", "41", true, "2020-01-01T08:00:00Z");
    let nested_id = identity_for("101", "41", false, "2020-01-01T08:00:00Z");
    let counter_records = vec![
        counter_row(heavy_id.clone(), "2020-01-01T12:00:00.500Z", 50, 90_000.0, 200_000, 50_000, 50),
        counter_row(culprit_id.clone(), "2020-01-01T12:00:00.500Z", 500, 500_000.0, 100, 0, 500),
        counter_row(nested_id.clone(), "2020-01-01T12:00:00.500Z", 50, 80_000.0, 100_000, 10_000, 50),
    ];
    let SourceObservation::SnapshotAcquisition { snapshot, .. } = &mut counter_source.observation else { panic!() };
    snapshot.records = counter_records.len() as u32;
    snapshot.sources =
        vec![StateSourceCoverage::observed("pg_stat_statements".into(), counter_records.len() as u32, true)];

    let (defs_source, defs_records) = definition_capture_from(
        &counter_source,
        "2020-01-01T11:30:00Z",
        "2020-01-01T11:30:01Z",
        vec![
            (
                heavy_id.clone(),
                StatementOrigin::Application,
                DefinitionPayload::sql(
                    "SELECT payload FROM public.orders WHERE tenant_secret = 'private-9264' AND status = 'open'",
                    Engine::Postgres,
                ),
            ),
            (
                culprit_id,
                StatementOrigin::Culprit,
                DefinitionPayload::sql("/* culprit */ SELECT count(*) FROM pg_stat_activity", Engine::Postgres),
            ),
            (
                nested_id,
                StatementOrigin::Application,
                DefinitionPayload::sql("SELECT inner_step FROM public.audit_log", Engine::Postgres),
            ),
        ],
    );

    let counters = seal_stream(
        counter_source.clone(),
        counter_records.clone(),
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:02Z",
    )
    .await;
    let defs = seal_stream(
        defs_source.clone(),
        defs_records,
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T11:30:00Z",
        "2020-01-01T11:30:02Z",
    )
    .await;
    let output = project_with_context(&counters, &[&defs]);
    output.validate().unwrap();
    assert_eq!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
    assert!(!output.findings.is_empty(), "heavy application query produces ordinary performance findings");
    let serialized = serde_json::to_string(&output).unwrap();
    assert!(!serialized.contains("private-9264"));
    assert!(!serialized.contains("pg_stat_activity"), "culprit-tagged queries never enter application findings");
    assert!(!serialized.contains("audit_log"), "nested statements do not duplicate top-level database work");
    let heavy = output.findings.iter().find(|finding| finding.finding.check == "heavy-query").expect("heavy-query");
    let derived_query = heavy.query.as_ref().expect("bound original query definition");
    assert_eq!(derived_query.identity, heavy_id);
    assert_eq!(derived_query.reference.capture_ref(), defs.verified_original().unwrap().original().clone());
    assert_eq!(heavy.inputs.len(), 2, "both counter point and statement definition row are referenced");
    assert!(heavy.finding.fix.as_ref().is_none_or(|fix| fix.sql.is_none()), "no executable DDL without schema");

    // Same queryid under a different userid or stats_since never borrows another entry's definition.
    let mut wrong_user_id = heavy_id.clone();
    wrong_user_id.qualifiers.insert("userid".into(), "99".into());
    let (wrong_defs_source, wrong_defs_records) = definition_capture_from(
        &counter_source,
        "2020-01-01T11:30:00Z",
        "2020-01-01T11:30:01Z",
        vec![(
            wrong_user_id,
            StatementOrigin::Application,
            DefinitionPayload::sql("SELECT payload FROM public.orders", Engine::Postgres),
        )],
    );
    let wrong_defs = seal_stream(
        wrong_defs_source,
        wrong_defs_records,
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T11:30:00Z",
        "2020-01-01T11:30:02Z",
    )
    .await;
    let unjoined = project_with_context(&counters, &[&wrong_defs]);
    unjoined.validate().unwrap();
    assert!(unjoined.findings.is_empty());
    assert!(assessment(&unjoined).limitations.contains(&Limit::MissingQueryDefinition));

    // Foreign process epoch or standby-vs-primary role mismatch falls back to counter-only limitations.
    let mut foreign_defs_source = defs_source;
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut foreign_defs_source.observation
    else {
        panic!()
    };
    snapshot.records = 0;
    snapshot.sources = vec![StateSourceCoverage::observed("pg_stat_statements".into(), 0, true)];
    execution.database_writable = Some(false);
    let foreign_defs = seal_stream(
        foreign_defs_source,
        vec![],
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T11:30:00Z",
        "2020-01-01T11:30:02Z",
    )
    .await;
    let mismatched = project_with_context(&counters, &[&foreign_defs]);
    assert!(matches!(
        mismatched.description.output_coverage.findings,
        DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::AnalysisSourceUnavailable }
    ));
}

#[tokio::test]
async fn restricted_entry_retains_its_measured_denominator_and_paired_counter_deltas_bound_the_interval() {
    let (mut earlier_source, _) = evidence(true, "1");
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut earlier_source.observation else {
        panic!()
    };
    snapshot.observed_from = "2020-01-01T10:00:00Z".into();
    snapshot.observed_until = "2020-01-01T10:00:01Z".into();
    execution.observed_from = snapshot.observed_from.clone();
    execution.observed_until = snapshot.observed_until.clone();

    let app_id = identity_for("201", "41", true, "2020-01-01T08:00:00Z");
    let restricted_id = identity_for("202", "42", true, "2020-01-01T08:00:00Z");
    let earlier_records = vec![
        counter_row(app_id.clone(), "2020-01-01T10:00:00.500Z", 10, 1_000.0, 100, 10, 10),
        counter_row(restricted_id.clone(), "2020-01-01T10:00:00.500Z", 10, 1_000.0, 100, 10, 10),
    ];
    let SourceObservation::SnapshotAcquisition { snapshot, .. } = &mut earlier_source.observation else { panic!() };
    snapshot.records = 2;
    snapshot.sources = vec![StateSourceCoverage::observed("pg_stat_statements".into(), 2, true)];

    // Over 10:00..12:00 (2h), restricted_id takes 99,000ms while app_id takes only 1,000ms (1% share < 5%).
    // Retaining restricted_id's cost in the denominator prevents app_id from falsely claiming 100% share!
    let mut later_source = earlier_source.clone();
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut later_source.observation else {
        panic!()
    };
    snapshot.observed_from = "2020-01-01T12:00:00Z".into();
    snapshot.observed_until = "2020-01-01T12:00:01Z".into();
    execution.observed_from = snapshot.observed_from.clone();
    execution.observed_until = snapshot.observed_until.clone();
    let later_records = vec![
        counter_row(app_id.clone(), "2020-01-01T12:00:00.500Z", 20, 2_000.0, 200, 20, 20),
        counter_row(restricted_id.clone(), "2020-01-01T12:00:00.500Z", 110, 100_000.0, 200, 20, 110),
    ];

    let (defs_source, defs_records) = definition_capture_from(
        &later_source,
        "2020-01-01T11:55:00Z",
        "2020-01-01T11:55:01Z",
        vec![
            (
                app_id,
                StatementOrigin::Application,
                DefinitionPayload::sql("SELECT id FROM public.accounts WHERE id = $1", Engine::Postgres),
            ),
            (
                restricted_id,
                StatementOrigin::Unknown,
                DefinitionPayload::Unavailable { reason: PayloadUnavailable::Restricted },
            ),
        ],
    );

    let earlier = seal_stream(
        earlier_source,
        earlier_records,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T10:00:00Z",
        "2020-01-01T10:00:02Z",
    )
    .await;
    let later = seal_stream(
        later_source,
        later_records,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:02Z",
    )
    .await;
    let defs = seal_stream(
        defs_source,
        defs_records,
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T11:55:00Z",
        "2020-01-01T11:55:02Z",
    )
    .await;

    let output = project_with_context(&later, &[&defs, &earlier]);
    output.validate().unwrap();
    assert_eq!(output.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
    assert!(
        !output.findings.iter().any(|finding| finding.finding.check == "heavy-query"),
        "restricted statement cost remains in the denominator so a 1% query is not flagged as 100% heavy"
    );
    let assessed = assessment(&output);
    assert!(assessed.limitations.contains(&Limit::UnknownStatementOrigin));
    assert!(assessed.limitations.contains(&Limit::QueryTextUnavailable));
}

#[cfg(feature = "collector")]
#[tokio::test]
async fn automatic_synthesis_selects_retained_same_owner_definitions_and_publishes_pg_performance_findings() {
    use culprit_collector::{
        analytical::test_support::LocalCaptureFixture,
        outbox::CompletedCaptureBudget,
        synthesis::{AutomaticSynthesis, SynthesisTurn},
    };
    use culprit_core::limits::OperationGuard;

    let policy =
        ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "automatic-pg-counter-findings-1")
            .unwrap();
    let automatic = AutomaticSynthesis::new(Arc::new(policy), CompletedCaptureBudget::default()).unwrap();

    let (mut counter_source, _) = evidence(true, "1");
    let SourceObservation::SnapshotAcquisition { snapshot, execution, .. } = &mut counter_source.observation else {
        panic!()
    };
    snapshot.observed_from = "2020-01-01T12:00:00Z".into();
    snapshot.observed_until = "2020-01-01T12:00:01Z".into();
    execution.observed_from = snapshot.observed_from.clone();
    execution.observed_until = snapshot.observed_until.clone();

    let heavy_id = identity_for("301", "41", true, "2020-01-01T09:00:00Z");
    let counter_records =
        vec![counter_row(heavy_id.clone(), "2020-01-01T12:00:00.500Z", 40, 80_000.0, 90_000, 30_000, 40)];
    let SourceObservation::SnapshotAcquisition { snapshot, .. } = &mut counter_source.observation else { panic!() };
    snapshot.records = counter_records.len() as u32;
    snapshot.sources =
        vec![StateSourceCoverage::observed("pg_stat_statements".into(), counter_records.len() as u32, true)];

    let (defs_source, defs_records) = definition_capture_from(
        &counter_source,
        "2020-01-01T11:15:00Z",
        "2020-01-01T11:15:01Z",
        vec![(
            heavy_id,
            StatementOrigin::Application,
            DefinitionPayload::sql(
                "SELECT line_total FROM public.invoice_lines WHERE invoice_id = $1",
                Engine::Postgres,
            ),
        )],
    );

    let mut fixture =
        Box::pin(LocalCaptureFixture::with_automatic_synthesis(defs_source, defs_records, &automatic)).await;
    let defs_ref = fixture.original_reference();
    let counters_ref = Box::pin(fixture.append_original(counter_source, counter_records)).await;

    let outbox = fixture.outbox.as_mut().unwrap();
    let mut counter_output = None;
    loop {
        let guard = OperationGuard::new(CompletedCaptureBudget::default().timeout).unwrap();
        let turn = outbox.delivery().synthesize_pending(&automatic, guard.handle(), ()).await.unwrap();
        let SynthesisTurn::Committed { primary, output, queue_retired } = turn else { break };
        assert!(queue_retired);
        let sealed = outbox.verify(&output.capture.observation_id).await.unwrap();
        assert_eq!(sealed.status, CollectionStatus::Complete);
        assert_eq!(sealed.source_queries, 0);
        if primary == counters_ref {
            counter_output = Some(sealed);
        }
    }
    let final_manifest = counter_output.expect("counter capture synthesized");
    let SourceObservation::DerivedDiagnosis(header) = final_manifest.description.unwrap().observation else { panic!() };
    assert_eq!(header.output_coverage.findings, DerivedOutputCoverage::Evaluated);
    assert!(header.counts.findings >= 1, "automatic synthesis joined retained definitions to emit findings");
    assert!(header.inputs.iter().any(|input| input.capture == counters_ref));
    assert!(header.inputs.iter().any(|input| input.capture == defs_ref));
}
