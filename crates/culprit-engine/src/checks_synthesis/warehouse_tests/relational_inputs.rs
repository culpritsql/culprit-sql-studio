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

#[cfg(feature = "mssql")]
async fn custom_retained_gold(
    sql: &str,
    xml: &str,
    bindings: Vec<MssqlRelationalInputBinding>,
    module_tvp: bool,
    reused_generation: bool,
    resolved: bool,
    seq: u64,
) -> DerivedDiagnosis {
    let mut fixture = relational_input_question_fixture(TvpLoopCase::RepeatedAccess);
    let (wa, activity) = original(
        fixture.activity_source.clone(),
        vec![fixture.activity_row.record],
        seq,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let basis = reference(&activity, 0);
    let SourceObservation::CurrentRequestWork { result, .. } = &mut fixture.work_source.observation else { panic!() };
    result.focus.as_mut().unwrap().parent = basis.clone();
    let (ww, work) = original(
        fixture.work_source,
        vec![fixture.work_row.record],
        seq + 1,
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
    let padded_sql = format!("{sql} /* {} */", "pad ".repeat(120));
    let padded_xml = xml.replace("__SQL__", &padded_sql);
    retained.statement = MssqlCachedPayload {
        payload: DefinitionPayload::sql(&padded_sql, Engine::Mssql),
        source_utf16_bytes: Some((padded_sql.encode_utf16().count() * 2).to_string()),
        native_sha256: Some(digest(padded_sql.as_bytes())),
    };
    retained.context.source_text_bytes = retained.statement.source_utf16_bytes.clone();
    retained.relational_input_proof = None;
    retained.parameter_context_proof = None;
    if !module_tvp {
        retained.module_parameters = None;
        retained.current_type_schema = None;
    }
    if reused_generation {
        retained.context.identity.plan_generation = "77".into();
    }
    retained.batch_plan = MssqlCachedPayload {
        payload: DefinitionPayload::plan(&padded_xml, Engine::Mssql),
        source_utf16_bytes: Some((padded_xml.encode_utf16().count() * 2).to_string()),
        native_sha256: Some(digest(padded_xml.as_bytes())),
    };
    if !bindings.is_empty() {
        retained.relational_input_proof =
            Some(MssqlRelationalInputProof::for_retained(&source, retained, bindings).unwrap());
    }
    rows[0] = TelemetryRecord::ObjectDefinition(
        ObjectDefinitionRecord::new(definition.source.clone(), definition.fact.clone()).unwrap(),
    );

    let (wr, result_cap) = original(
        source,
        rows,
        seq + 2,
        EvidenceFamily::ObjectDefinitions,
        "2020-01-01T12:00:03Z",
        "2020-01-01T12:00:04Z",
    )
    .await;
    let refs = result_cap.records().iter().enumerate().map(|(i, _)| reference(&result_cap, i)).collect::<Vec<_>>();
    let selected = refs.iter().zip(result_cap.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
    let answer = RelationalInputAnswer::from_originals(
        &focus,
        activity.source(),
        activity.records()[0].row(),
        result_cap.source(),
        &result_cap.verified_original().unwrap().original().clone(),
        &selected,
    )
    .unwrap();
    let mut answered = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::RelationalInputs(Box::new(answer))) },
        )
        .unwrap();

    let mut recovery_cap_opt = None;
    if resolved {
        let mut rec_act_source = fixture.activity_source;
        let SourceObservation::Activity(frame) = &mut rec_act_source.observation else { panic!() };
        frame.at = "2020-01-01T12:00:04.5Z".into();
        frame.observed_from = frame.at.clone();
        frame.observed_until = frame.at.clone();
        frame.native_rows = 0;
        frame.observations = 0;
        frame.coverage = StateSourceCoverage::observed(rec_act_source.source.clone(), 0, true);
        let (_, rec_act) = original(
            rec_act_source,
            vec![],
            seq + 3,
            EvidenceFamily::ActivityObservations,
            "2020-01-01T12:00:04.5Z",
            "2020-01-01T12:00:04.6Z",
        )
        .await;
        answered.lifecycle = DiagnosticLifecycle::Resolved;
        answered.recovery = Some(DiagnosticRecoveryProof::CompleteScope {
            capture: rec_act.verified_original().unwrap().original().clone(),
        });
        recovery_cap_opt = Some(rec_act);
    }

    let revision = DiagnosticRevision::between(None, answered, "2020-01-01T12:00:05Z").unwrap().unwrap();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: activity.source().target.clone(),
        input: basis.capture_ref(),
        part: 0,
        parts: 1,
        episodes: if revision.episode.retained_in_checkpoint() { vec![revision.episode.clone()] } else { vec![] },
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let diag_source = CollectionDescription {
        target: activity.source().target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: basis.capture_ref(),
            step_id: digest(format!("tvp-track-63-step-{seq}").as_bytes()),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(revision.episode.scope.key().unwrap(), revision.episode.scope.clone())].into(),
        }),
    };
    let diag_rows = vec![
        TelemetryRecord::Operational(OperationalObservation {
            source: DIAGNOSTICS_SOURCE.into(),
            key: revision.id.clone(),
            fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
        }),
        TelemetryRecord::DiagnosticCheckpoint(checkpoint),
    ];
    let (wd, diagnostics) = original(
        diag_source,
        diag_rows,
        seq + 4,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:05Z",
        "2020-01-01T12:00:06Z",
    )
    .await;
    let _ = (wa, ww, wr, wd);
    let mut ctx_refs: Vec<&EvidenceCapture> = vec![&activity, &work, &result_cap];
    if let Some(ref rec) = recovery_cap_opt {
        ctx_refs.push(rec);
    }
    policy(false).synthesize_captures(&diagnostics, &ctx_refs, RECORDED_AT, &display()).unwrap()
}

#[tokio::test]
#[cfg(feature = "mssql")]
async fn tvp_vs_local_table_variable_accurate_estimates_skew_and_large_to_small_grants_preserve_distinct_gold_remedies()
{
    use culprit_core::plan_facts::{RelationalInput, RelationalInputKind, RelationalInputProvenance};

    // 1. Small-to-large TVP loop gives TVP-specific advice (explicitly rejecting local table-variable deferred compilation).
    let tvp_c = captures(TvpLoopCase::RepeatedAccess).await;
    let tvp_out = run(&tvp_c).unwrap();
    let [tvp_inc] = tvp_out.incidents.as_slice() else { panic!() };
    assert!(tvp_inc.developer_action.contains(&t!("insight.misestimate.fix.tvp", table = "@Rows")));
    assert!(!tvp_inc.developer_action.contains(&t!("insight.misestimate.fix.local_table_variable", table = "@Rows")));

    // 2. Local table variable gives local-table-variable deferred-compilation advice, never TVP READONLY advice.
    let local_sql = "SELECT value FROM @Rows WHERE value = 9264";
    let local_xml = r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="DECLARE @Rows TABLE (value int NOT NULL)" StatementType="DECLARE TABLE"/><StmtSimple StatementId="2" StatementText="__SQL__" QueryHash="0x0123456789ABCDEF" StatementType="SELECT" StatementSubTreeCost="15"><QueryPlan>
<RelOp NodeId="0" PhysicalOp="Sort" LogicalOp="Sort" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="15">
  <OutputList><ColumnReference Table="[@Rows]" Column="value"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualExecutions="1"/></RunTimeInformation>
  <Sort Distinct="false"><OrderBy><OrderByColumn Ascending="true"><ColumnReference Table="[@Rows]" Column="value"/></OrderByColumn></OrderBy>
    <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="1" EstimatedRowsRead="1" EstimatedTotalSubtreeCost="1">
      <OutputList><ColumnReference Table="[@Rows]" Column="value"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualRowsRead="5000" ActualExecutions="1" ActualEndOfScans="1" ActualRebinds="0" ActualRewinds="0"/></RunTimeInformation>
      <TableScan Ordered="false"><Object Table="[@Rows]"/></TableScan>
    </RelOp>
  </Sort>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let local_out = custom_retained_gold(
        local_sql,
        local_xml,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::LocalTableVariable,
                provenance: RelationalInputProvenance::NativeDeclaration,
            },
        }],
        false,
        false,
        false,
        8_930_000,
    )
    .await;
    let [local_inc] = local_out.incidents.as_slice() else { panic!() };
    assert!(local_inc.developer_action.contains(&t!("insight.misestimate.fix.local_table_variable", table = "@Rows")));
    assert!(!local_inc.developer_action.contains(&t!("insight.misestimate.fix.tvp", table = "@Rows")));

    // 3. Accurate non-one estimates and healthy counted inputs attach no false misestimate/loop remedy.
    for case in [TvpLoopCase::AccurateEstimate, TvpLoopCase::HealthyCountedInput, TvpLoopCase::FilteredAndAggregated] {
        let c = captures(case).await;
        let out = run(&c).unwrap();
        out.validate().unwrap();
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty());
        if matches!(case, TvpLoopCase::AccurateEstimate | TvpLoopCase::HealthyCountedInput) {
            assert!(!inc.developer_action.contains(&t!("insight.misestimate.fix.tvp", table = "@Rows")), "{case:?}");
        } else {
            // FilteredAndAggregated preserves separate root output (1 row) from 20k input / 10k filtered rows.
            assert!(inc.mechanism.as_deref().unwrap().contains(&t!("insight.nested_loops.tvp.result", actual = "1")));
        }
    }

    // 4. Equal input row counts (20,000 est & actual accessed) with skewed filter (1 est vs 20,000 actual) report selectivity, not volume.
    let skew_sql = "SELECT COUNT_BIG(*) FROM @Rows AS r WHERE r.value = 1";
    let skew_xml = r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="__SQL__" QueryHash="0x0123456789ABCDEF" StatementType="SELECT" StatementSubTreeCost="10"><QueryPlan>
<RelOp NodeId="0" PhysicalOp="Stream Aggregate" LogicalOp="Aggregate" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="10">
  <OutputList><ColumnReference Column="Expr1000"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1" ActualExecutions="1"/></RunTimeInformation>
  <StreamAggregate><DefinedValues><DefinedValue><ColumnReference Column="Expr1000"/><ScalarOperator ScalarString="COUNT_BIG(*)"><Aggregate AggType="countstar" Distinct="false"/></ScalarOperator></DefinedValue></DefinedValues>
    <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="1" EstimatedRowsRead="20000" EstimatedTotalSubtreeCost="9">
      <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="20000" ActualRowsRead="20000" ActualExecutions="1" ActualEndOfScans="1" ActualRebinds="0" ActualRewinds="0"/></RunTimeInformation>
      <TableScan Ordered="false"><Object Table="[@Rows]" Alias="[r]"/>
        <Predicate><ScalarOperator ScalarString="[r].[value]=(1)"><Compare CompareOp="EQ"><ScalarOperator><Identifier><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></Identifier></ScalarOperator><ScalarOperator><Const ConstValue="(1)"/></ScalarOperator></Compare></ScalarOperator></Predicate>
      </TableScan>
    </RelOp>
  </StreamAggregate>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let skew_out = custom_retained_gold(
        skew_sql,
        skew_xml,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::TableValuedParameter,
                provenance: RelationalInputProvenance::NativeParameterMetadata,
            },
        }],
        true,
        false,
        false,
        8_930_020,
    )
    .await;
    let [skew_inc] = skew_out.incidents.as_slice() else { panic!() };
    assert!(skew_inc.mechanism.as_deref().unwrap().contains(&t!("insight.relational_input.selectivity")));
    assert!(!skew_inc.mechanism.as_deref().unwrap().contains(&t!("insight.relational_input.completed_input_volume")));

    // 5. Large-to-small cached TVP plan with oversized memory grant links TVP grant investigation.
    let grant_sql = "SELECT DISTINCT r.value FROM @Rows AS r";
    let grant_xml = r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="__SQL__" QueryHash="0x0123456789ABCDEF" StatementType="SELECT" StatementSubTreeCost="20"><QueryPlan>
<MemoryGrantInfo SerialRequiredMemory="1024" SerialDesiredMemory="65536" RequiredMemory="1024" DesiredMemory="65536" RequestedMemory="65536" GrantWaitTime="0" GrantedMemory="65536" MaxUsedMemory="512"/>
<RelOp NodeId="0" PhysicalOp="Sort" LogicalOp="Distinct Sort" Parallel="false" EstimateRows="20000" EstimatedTotalSubtreeCost="20">
  <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1" ActualExecutions="1"/></RunTimeInformation>
  <Sort Distinct="true"><OrderBy><OrderByColumn Ascending="true"><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OrderByColumn></OrderBy>
    <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="20000" EstimatedRowsRead="20000" EstimatedTotalSubtreeCost="5">
      <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1" ActualRowsRead="1" ActualExecutions="1" ActualEndOfScans="1" ActualRebinds="0" ActualRewinds="0"/></RunTimeInformation>
      <TableScan Ordered="false"><Object Table="[@Rows]" Alias="[r]"/></TableScan>
    </RelOp>
  </Sort>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let grant_out = custom_retained_gold(
        grant_sql,
        grant_xml,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::TableValuedParameter,
                provenance: RelationalInputProvenance::NativeParameterMetadata,
            },
        }],
        true,
        false,
        false,
        8_930_040,
    )
    .await;
    let [grant_inc] = grant_out.incidents.as_slice() else { panic!() };
    assert!(grant_inc.developer_action.contains(&t!("insight.memory_grant.tvp.investigate")));
}

#[tokio::test]
#[cfg(feature = "mssql")]
async fn competing_implicit_conversion_other_table_spill_reused_handles_and_resolved_tvp_episodes_stay_honest() {
    use culprit_core::plan_facts::{RelationalInput, RelationalInputKind, RelationalInputProvenance};

    // 1. Implicit conversion + spill caused by dbo.Items (while @Rows estimate 10 == actual 10) surfaces competing causes without blaming @Rows.
    let comp_sql = "SELECT r.value, i.code FROM @Rows AS r JOIN dbo.Items AS i ON r.value = i.id WHERE i.code = N'A'";
    let comp_xml = r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="__SQL__" QueryHash="0x0123456789ABCDEF" StatementType="SELECT" StatementSubTreeCost="40"><QueryPlan>
<Warnings><PlanAffectingConvert ConvertIssue="Seek Plan" Expression="CONVERT_IMPLICIT(nvarchar(32),[db].[dbo].[Items].[code],0)"/></Warnings>
<RelOp NodeId="0" PhysicalOp="Hash Match" LogicalOp="Inner Join" Parallel="false" EstimateRows="10" EstimatedTotalSubtreeCost="40">
  <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/><ColumnReference Database="[db]" Schema="[dbo]" Table="[Items]" Alias="[i]" Column="code"/></OutputList>
  <Warnings><SpillToTempDb SpillLevel="1" SpilledThreadCount="1"/></Warnings>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="10" ActualExecutions="1"/></RunTimeInformation>
  <Hash>
    <BuildResidual><ScalarOperator ScalarString="[r].[value]=[db].[dbo].[Items].[id]"/></BuildResidual>
    <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="10" EstimatedRowsRead="10" EstimatedTotalSubtreeCost="1">
      <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="10" ActualRowsRead="10" ActualExecutions="1" ActualEndOfScans="1" ActualRebinds="0" ActualRewinds="0"/></RunTimeInformation>
      <TableScan Ordered="false"><Object Table="[@Rows]" Alias="[r]"/></TableScan>
    </RelOp>
    <RelOp NodeId="2" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" Parallel="false" EstimateRows="10" EstimatedRowsRead="50000" EstimatedTotalSubtreeCost="35">
      <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[Items]" Alias="[i]" Column="code"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="50000" ActualRowsRead="50000" ActualExecutions="1"/></RunTimeInformation>
      <IndexScan Ordered="false"><Object Database="[db]" Schema="[dbo]" Table="[Items]" Index="[PK_Items]" Alias="[i]" IndexKind="Clustered"/>
        <Predicate><ScalarOperator ScalarString="CONVERT_IMPLICIT(nvarchar(32),[db].[dbo].[Items].[code],0)=N'A'"><Compare CompareOp="EQ"><ScalarOperator><Convert DataType="nvarchar" Length="64" Style="0" Implicit="true"><ScalarOperator><Identifier><ColumnReference Database="[db]" Schema="[dbo]" Table="[Items]" Alias="[i]" Column="code"/></Identifier></ScalarOperator></Convert></ScalarOperator><ScalarOperator><Const ConstValue="N'A'"/></ScalarOperator></Compare></ScalarOperator></Predicate>
      </IndexScan>
    </RelOp>
  </Hash>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let comp_out = custom_retained_gold(
        comp_sql,
        comp_xml,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::TableValuedParameter,
                provenance: RelationalInputProvenance::NativeParameterMetadata,
            },
        }],
        true,
        false,
        false,
        8_930_060,
    )
    .await;
    comp_out.validate().unwrap();
    let [comp_inc] = comp_out.incidents.as_slice() else { panic!() };
    assert!(comp_inc.culprit_queries.is_empty());
    // Competing spill and dbo.Items misestimate are surfaced while @Rows is NOT given false TVP misestimate blame.
    assert!(!comp_inc.developer_action.contains(&t!("insight.misestimate.fix.tvp", table = "@Rows")));
    assert!(comp_inc.developer_action.contains(&t!("insight.misestimate.fix.mssql", table = "Items")));
    assert!(comp_inc.mechanism.as_deref().unwrap().contains(&t!("insight.misestimate.detail.planned_spilled")));

    // 2. Reused plan_handle / changed generation after module recreation withholds candidate attribution.
    let reused_out = custom_retained_gold(
        comp_sql,
        comp_xml,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::TableValuedParameter,
                provenance: RelationalInputProvenance::NativeParameterMetadata,
            },
        }],
        true,
        true,
        false,
        8_930_080,
    )
    .await;
    let [reused_inc] = reused_out.incidents.as_slice() else { panic!() };
    assert!(reused_inc.candidate_queries.is_empty() && reused_inc.culprit_queries.is_empty());
    assert!(reused_inc.entry.reasons.contains(&t!("view.relational_input_question.changed")));

    // 3. Resolved TVP episode preserves original victim, retained plan candidate, and TVP mechanism after recovery.
    let resolved_out = custom_retained_gold(
        "SELECT DISTINCT r.value FROM @Rows AS r",
        r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="__SQL__" QueryHash="0x0123456789ABCDEF" StatementType="SELECT" StatementSubTreeCost="15"><QueryPlan>
<RelOp NodeId="0" PhysicalOp="Sort" LogicalOp="Distinct Sort" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="15">
  <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualExecutions="1"/></RunTimeInformation>
  <Sort Distinct="true"><OrderBy><OrderByColumn Ascending="true"><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OrderByColumn></OrderBy>
    <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="1" EstimatedRowsRead="1" EstimatedTotalSubtreeCost="1">
      <OutputList><ColumnReference Table="[@Rows]" Alias="[r]" Column="value"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualRowsRead="5000" ActualExecutions="1" ActualEndOfScans="1" ActualRebinds="0" ActualRewinds="0"/></RunTimeInformation>
      <TableScan Ordered="false"><Object Table="[@Rows]" Alias="[r]"/></TableScan>
    </RelOp>
  </Sort>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#,
        vec![MssqlRelationalInputBinding {
            node_id: 1,
            input: RelationalInput {
                kind: RelationalInputKind::TableValuedParameter,
                provenance: RelationalInputProvenance::NativeParameterMetadata,
            },
        }],
        true,
        false,
        true,
        8_930_100,
    )
    .await;
    resolved_out.validate().unwrap();
    let [res_inc] = resolved_out.incidents.as_slice() else { panic!() };
    assert_eq!(res_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(res_inc.affected_queries.len(), 1);
    assert_eq!(res_inc.candidate_queries.len(), 1);
    assert!(res_inc.culprit_queries.is_empty());
    assert!(res_inc.developer_action.contains(&t!("insight.misestimate.fix.tvp", table = "@Rows")));
}
