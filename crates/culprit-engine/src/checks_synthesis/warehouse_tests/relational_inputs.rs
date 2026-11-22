//! Genuine canonical readers supply all four originals before the pure synthesizer runs.
use super::*;
use culprit_core::collected_analysis::EvidenceGapReason;
use test_support::{TvpLoopCase, relational_input_question_fixture};

struct Captures {
    activity: EvidenceCapture,
    work: EvidenceCapture,
    result: EvidenceCapture,
    diagnostics: EvidenceCapture,
    warehouses: Vec<Warehouse>,
}
fn reference(capture: &EvidenceCapture, position: usize) -> DiagnosticEvidenceRef {
    capture.verified_original().unwrap().reference(position).unwrap()
}
async fn original(
    source: CollectionDescription,
    rows: Vec<TelemetryRecord>,
    sequence: u64,
    family: EvidenceFamily,
    from: &str,
    until: &str,
) -> (Warehouse, EvidenceCapture) {
    let warehouse =
        Warehouse::open(ReadOnlyFixture::capture_at(source, rows, sequence, from, until).await, false).await;
    let capture = match family {
        EvidenceFamily::ObjectDefinitions => {
            full_original(&warehouse, family, EvidenceGapReason::DetailInRecovery).await
        }
        EvidenceFamily::OperationalSamples => {
            full_original(&warehouse, family, EvidenceGapReason::NativeRepresentationRequired).await
        }
        _ => warehouse.read(&[family]).await,
    };
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    (warehouse, capture)
}
async fn full_original(
    warehouse: &Warehouse,
    family: EvidenceFamily,
    expected_gap: EvidenceGapReason,
) -> EvidenceCapture {
    let projection = warehouse.read(&[family]).await;
    assert_eq!(warehouse.work().2, 0, "projection-only intake does not open native payloads");
    if projection.is_read_complete() {
        assert!(projection.gaps().is_empty());
        return projection;
    }
    assert!(projection.records().is_empty());
    let [gap] = projection.gaps() else { panic!("exact native original gap: {:?}", projection.gaps()) };
    assert_eq!(gap.reason, expected_gap);
    assert_eq!(gap.stream, Some(family.stream()));
    let batches =
        warehouse.range.manifest().batches.iter().filter(|batch| batch.stream == family.stream()).collect::<Vec<_>>();
    let [batch] = batches.as_slice() else { panic!("one selected original native batch") };
    assert_eq!(gap.batch_id.as_deref(), Some(batch.batch_id.as_str()));
    assert_eq!(gap.row_ordinal, Some(1));
    assert_eq!(warehouse.work().2, 0, "projection-only read does not open native payloads");
    assert!(batch.bytes > 1);
    let error = warehouse
        .read_with_native_payloads(&[family], RecoveryPayloadBudget { max_objects: 1, max_bytes: batch.bytes - 1 })
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(warehouse.work().2, 0, "short native allowance refuses before the payload read");
    let capture = warehouse
        .read_with_native_payloads(&[family], RecoveryPayloadBudget { max_objects: 1, max_bytes: batch.bytes })
        .await
        .unwrap();
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    assert_eq!(capture.metrics().recovery_objects, 1);
    assert_eq!(capture.metrics().recovery_bytes, batch.bytes);
    assert_eq!(warehouse.work().2, 1);
    capture
}
async fn captures(case: TvpLoopCase) -> Captures {
    let fixture = relational_input_question_fixture(case);
    let (wa, activity) = original(
        fixture.activity_source,
        vec![fixture.activity_row.record],
        880001,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let basis = reference(&activity, 0);
    let mut work_source = fixture.work_source;
    let SourceObservation::CurrentRequestWork { result, .. } = &mut work_source.observation else { panic!() };
    result.focus.as_mut().unwrap().parent = basis.clone();
    let (ww, work) = original(
        work_source,
        vec![fixture.work_row.record],
        880002,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:01Z",
        "2020-01-01T12:00:02Z",
    )
    .await;
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(activity.source()).unwrap(),
        fixture.episode.signal,
        fixture.episode.first_seen_at,
        basis.clone(),
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: reference(&work, 0) });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::RelationalInputContext,
        &episode,
        work.source(),
        &reference(&work, 0),
        work.records()[0].row(),
    )
    .unwrap()
    .unwrap();
    let mut source = fixture.source;
    let SourceObservation::SourceRead { focus_key, .. } = &mut source.observation else { panic!() };
    *focus_key = focus.key().unwrap();
    let mut rows = fixture.records;
    let TelemetryRecord::ObjectDefinition(definition) = &mut rows[0] else { panic!() };
    let ObjectDefinitionFact::MssqlRetainedPlan(retained) = &mut definition.fact else { panic!() };
    if let Some(proof) = retained.relational_input_proof.take() {
        retained.relational_input_proof =
            Some(MssqlRelationalInputProof::for_retained(&source, retained, proof.bindings).unwrap());
    }
    let definition = ObjectDefinitionRecord::new(definition.source.clone(), definition.fact.clone()).unwrap();
    rows[0] = TelemetryRecord::ObjectDefinition(definition);
    let (wr, result) = original(
        source,
        rows,
        880003,
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T12:00:03Z",
        "2020-01-01T12:00:04Z",
    )
    .await;
    let refs = result.records().iter().enumerate().map(|(i, _)| reference(&result, i)).collect::<Vec<_>>();
    let selected = refs.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = RelationalInputAnswer::from_originals(
        &focus,
        activity.source(),
        activity.records()[0].row(),
        result.source(),
        &result.verified_original().unwrap().original().clone(),
        &selected,
    )
    .unwrap();
    let episode = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::RelationalInputs(Box::new(answer))) },
        )
        .unwrap();
    let revision = DiagnosticRevision::between(None, episode, "2020-01-01T12:00:05Z").unwrap().unwrap();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: activity.source().target.clone(),
        input: basis.capture_ref(),
        part: 0,
        parts: 1,
        episodes: vec![revision.episode.clone()],
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let source = CollectionDescription {
        target: activity.source().target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: basis.capture_ref(),
            step_id: digest(b"controlled-relational-answer-step"),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(revision.episode.scope.key().unwrap(), revision.episode.scope.clone())].into(),
        }),
    };
    let rows = vec![
        TelemetryRecord::Operational(OperationalObservation {
            source: DIAGNOSTICS_SOURCE.into(),
            key: revision.id.clone(),
            fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
        }),
        TelemetryRecord::DiagnosticCheckpoint(checkpoint),
    ];
    let (wd, diagnostics) = original(
        source,
        rows,
        880004,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:05Z",
        "2020-01-01T12:00:06Z",
    )
    .await;
    Captures { activity, work, result, diagnostics, warehouses: vec![wa, ww, wr, wd] }
}
fn run(c: &Captures) -> Result<DerivedDiagnosis> {
    policy(false).synthesize_captures(&c.diagnostics, &[&c.activity, &c.work, &c.result], RECORDED_AT, &display())
}
#[tokio::test]
#[cfg(feature = "mssql")]
async fn exact_tvp_answer_uses_existing_native_insights_and_keeps_original_victim_and_later_cache_candidate_distinct() {
    let c = captures(TvpLoopCase::RepeatedAccess).await;
    let before = c.warehouses.iter().map(Warehouse::work).collect::<Vec<_>>();
    let output = run(&c).unwrap();
    let [incident] = output.incidents.as_slice() else { panic!() };
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(matches!(&incident.affected_queries[0].identity, Some(DerivedQueryIdentity::Activity { .. })));
    let [candidate] = incident.candidate_queries.as_slice() else { panic!() };
    assert!(matches!(&candidate.identity, Some(DerivedQueryIdentity::CachedStatement(_))));
    assert!(candidate.caller.is_none() && candidate.causal_role.is_none());
    assert!(incident.mechanism.as_ref().unwrap().contains(&t!("view.relational_input_question.retained_context")));
    assert!(incident.entry.reasons.contains(&t!("view.relational_input_question.gap.plan")));
    assert_eq!(c.warehouses.iter().map(Warehouse::work).collect::<Vec<_>>(), before);
    assert!(output.description.input_read_complete);
    output.validate().unwrap();
}
#[tokio::test]
async fn missing_binding_or_estimated_type_never_creates_a_harmful_reuse_or_culprit_verdict() {
    for case in
        [TvpLoopCase::MissingBinding, TvpLoopCase::Estimated, TvpLoopCase::HealthySeek, TvpLoopCase::PartialSource]
    {
        let c = captures(case).await;
        let output = run(&c).unwrap_or_else(|error| panic!("legitimate retained context {case:?}: {error:?}"));
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.entry.reasons.contains(&t!("view.relational_input_question.execution_limit")));
        if case == TvpLoopCase::MissingBinding || !cfg!(feature = "mssql") {
            assert!(incident.candidate_queries.is_empty());
        }
        assert!(incident.entry.reasons.contains(&t!("view.relational_input_question.gap.values")));
        assert!(incident.entry.reasons.contains(&t!("view.relational_input_question.gap.work")));
    }
}

#[tokio::test]
#[cfg(feature = "mssql")]
async fn a_forged_native_node_binding_is_refused_after_canonical_hydration_without_repair_or_source_work() {
    let c = captures(TvpLoopCase::WrongBinding).await;
    assert!(c.result.is_read_complete());
    let before = c.warehouses.iter().map(Warehouse::work).collect::<Vec<_>>();
    let error = run(&c).unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
    assert_eq!(c.warehouses.iter().map(Warehouse::work).collect::<Vec<_>>(), before);
}
