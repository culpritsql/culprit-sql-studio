//! Canonical warehouse originals exercise Track 6.1 parameter sensitivity, healthy PSP variants,
//! competing causes, recycled incarnations and Query Store compilation follow-ups into Gold.
use super::*;
use culprit_core::{
    Actionability, FindingRuntimeVariation, ScenarioKind, Severity,
    collected_analysis::EvidenceGapReason,
    plan_compare::NativeRuntimeInputRelation,
    plan_facts::{
        PlanParameter, PlanParameterComparison, PlanParameterContext, PlanParameterIssue, PlanParameterUse,
        PlanParameterUseKind, PlanParameterValueState,
    },
};
use test_support::{
    ParameterEvidenceCase, QueryCohortCase, QueryCompilationFollowupCase, TvpLoopCase, parameter_evidence,
    query_cohort_evidence, query_compilation_followup_original, query_compilation_followup_result,
    relational_input_question_fixture,
};

fn reference(capture: &EvidenceCapture, position: usize) -> DiagnosticEvidenceRef {
    capture.verified_original().unwrap().reference(position).unwrap()
}

async fn seal(
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
        EvidenceFamily::ObjectDefinitions => hydrate(&warehouse, family, EvidenceGapReason::DetailInRecovery).await,
        EvidenceFamily::OperationalSamples => {
            hydrate(&warehouse, family, EvidenceGapReason::NativeRepresentationRequired).await
        }
        _ => warehouse.read(&[family]).await,
    };
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    (warehouse, capture)
}

async fn hydrate(warehouse: &Warehouse, family: EvidenceFamily, _expected_gap: EvidenceGapReason) -> EvidenceCapture {
    let projection = warehouse.read(&[family]).await;
    if projection.is_read_complete() {
        return projection;
    }
    let bytes: u64 = warehouse
        .range
        .manifest()
        .batches
        .iter()
        .filter(|batch| batch.stream == family.stream())
        .map(|batch| batch.bytes)
        .sum();
    let objects =
        warehouse.range.manifest().batches.iter().filter(|batch| batch.stream == family.stream()).count() as u32;
    warehouse
        .read_with_native_payloads(
            &[family],
            RecoveryPayloadBudget { max_objects: objects.max(1), max_bytes: bytes.max(1) },
        )
        .await
        .unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarCase {
    CachedLookup,
    SelectiveScan,
    EqualIntegerValues,
    Evidence(ParameterEvidenceCase),
    IncarnationChanged,
    NotObserved,
}

fn scalar_lookup_xml(sql: &str) -> String {
    format!(
        r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="{sql}" StatementType="SELECT" QueryHash="0x0123456789ABCDEF" StatementSubTreeCost="15" RetrievedFromCache="true"><QueryPlan>
<ParameterList><ColumnReference Column="@customer" ParameterDataType="int" ParameterCompiledValue="(2)" ParameterRuntimeValue="(1)"/></ParameterList>
<RelOp NodeId="0" PhysicalOp="Nested Loops" LogicalOp="Inner Join" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="15">
  <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="payload"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualExecutions="1"/></RunTimeInformation>
  <NestedLoops Optimized="false" WithUnorderedPrefetch="true">
    <OuterReferences><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="id"/></OuterReferences>
    <RelOp NodeId="1" PhysicalOp="Index Seek" LogicalOp="Index Seek" Parallel="false" EstimateRows="1" EstimatedRowsRead="1" EstimatedTotalSubtreeCost="1">
      <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="id"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualRowsRead="5000" ActualExecutions="1"/></RunTimeInformation>
      <IndexScan Ordered="true"><Object Database="[app]" Schema="[dbo]" Table="[orders]" Index="[ix_orders_customer]" Alias="[o]" IndexKind="NonClustered"/>
        <SeekPredicates><SeekPredicateNew><SeekKeys><Prefix ScanType="EQ"><RangeColumns><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="customer"/></RangeColumns><RangeExpressions><ScalarOperator ScalarString="[@customer]"><Identifier><ColumnReference Column="@customer"/></Identifier></ScalarOperator></RangeExpressions></Prefix></SeekKeys></SeekPredicateNew></SeekPredicates>
      </IndexScan>
    </RelOp>
    <RelOp NodeId="2" PhysicalOp="Key Lookup" LogicalOp="Key Lookup" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="14">
      <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="payload"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualExecutions="5000" ActualLogicalReads="15000"/></RunTimeInformation>
      <IndexScan Lookup="true" Ordered="true"><Object Database="[app]" Schema="[dbo]" Table="[orders]" Index="[pk_orders]" Alias="[o]" IndexKind="Clustered"/>
        <SeekPredicates><SeekPredicateNew><SeekKeys><Prefix ScanType="EQ"><RangeColumns><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="id"/></RangeColumns><RangeExpressions><ScalarOperator ScalarString="[app].[dbo].[orders].[id] as [o].[id]"><Identifier><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="id"/></Identifier></ScalarOperator></RangeExpressions></Prefix></SeekKeys></SeekPredicateNew></SeekPredicates>
      </IndexScan>
    </RelOp>
  </NestedLoops>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
    )
}

fn selective_scan_xml(sql: &str) -> String {
    format!(
        r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="{sql}" StatementType="SELECT" QueryHash="0x0123456789ABCDEF" StatementSubTreeCost="25" RetrievedFromCache="true"><QueryPlan>
<ParameterList><ColumnReference Column="@customer" ParameterDataType="int" ParameterCompiledValue="(1)" ParameterRuntimeValue="(2)"/></ParameterList>
<RelOp NodeId="1" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" Parallel="false" EstimateRows="50000" EstimatedRowsRead="50000" EstimatedTotalSubtreeCost="25">
  <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="payload"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="1" ActualRowsRead="50000" ActualExecutions="1" ActualLogicalReads="2000"/></RunTimeInformation>
  <IndexScan Ordered="false"><Object Database="[app]" Schema="[dbo]" Table="[orders]" Index="[pk_orders]" Alias="[o]" IndexKind="Clustered"/>
    <Predicate><ScalarOperator ScalarString="[app].[dbo].[orders].[customer] as [o].[customer]=[@customer]"><Compare CompareOp="EQ"><ScalarOperator><Identifier><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="customer"/></Identifier></ScalarOperator><ScalarOperator><Identifier><ColumnReference Column="@customer"/></Identifier></ScalarOperator></Compare></ScalarOperator></Predicate>
  </IndexScan>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
    )
}

fn build_scalar_retained(
    source: &CollectionDescription,
    template: &MssqlRetainedPlanDefinition,
    case: ScalarCase,
) -> Vec<TelemetryRecord> {
    if case == ScalarCase::NotObserved {
        return vec![];
    }
    let mut retained = match case {
        ScalarCase::Evidence(ev) => {
            let (_, mut rows) = parameter_evidence(ev);
            if rows.is_empty() {
                return vec![];
            }
            let TelemetryRecord::ObjectDefinition(def) = rows.remove(0) else { panic!() };
            let ObjectDefinitionFact::MssqlRetainedPlan(mut inner) = def.fact else { panic!() };
            inner.context = template.context.clone();
            if ev != ParameterEvidenceCase::StatementUnavailable {
                inner.context.source_text_bytes = inner.statement.source_utf16_bytes.clone();
            }
            *inner
        }
        ScalarCase::CachedLookup | ScalarCase::EqualIntegerValues | ScalarCase::IncarnationChanged => {
            let sql = format!(
                "SELECT payload FROM dbo.orders WHERE customer = @customer /* private-9264 {} */",
                "pad ".repeat(120)
            );
            let xml = scalar_lookup_xml(&sql);
            let mut context = template.context.clone();
            context.query_hash = Some("0x0123456789ABCDEF".into());
            context.source_text_bytes = Some((sql.encode_utf16().count() * 2).to_string());
            if case == ScalarCase::IncarnationChanged {
                context.identity.plan_generation = "99".into();
            }
            let equal = case == ScalarCase::EqualIntegerValues;
            let param_ctx = PlanParameterContext {
                retrieved_from_cache: Some(true),
                parameters: vec![PlanParameter {
                    name: Some("@customer".into()),
                    data_type: Some("int".into()),
                    compiled: PlanParameterValueState::Recorded,
                    runtime: PlanParameterValueState::Recorded,
                    comparison: PlanParameterComparison::DifferentRepresentation,
                    integer_values_equal: Some(equal),
                }],
                uses: vec![PlanParameterUse {
                    parameter_index: 0,
                    node_id: 1,
                    kind: PlanParameterUseKind::EqualitySeek,
                }],
                issues: vec![],
            };
            let mut def = MssqlRetainedPlanDefinition {
                context,
                statement: MssqlCachedPayload {
                    payload: DefinitionPayload::sql(&sql, Engine::Mssql),
                    source_utf16_bytes: Some((sql.encode_utf16().count() * 2).to_string()),
                    native_sha256: Some(digest(sql.as_bytes())),
                },
                module_parameters: None,
                current_type_schema: None,
                relational_input_proof: None,
                parameter_context_proof: None,
                statement_executions: Some("50".into()),
                last_start_without_offset: Some("2020-01-01T11:59:59.003".into()),
                database_last_query_plan_stats: Some(true),
                batch_plan: MssqlCachedPayload {
                    payload: DefinitionPayload::plan(&xml, Engine::Mssql),
                    source_utf16_bytes: Some((xml.encode_utf16().count() * 2).to_string()),
                    native_sha256: Some(digest(xml.as_bytes())),
                },
            };
            def.parameter_context_proof =
                Some(MssqlParameterContextProof::for_retained(source, &def, param_ctx).unwrap());
            def
        }
        ScalarCase::SelectiveScan => {
            let sql = format!(
                "SELECT payload FROM dbo.orders WHERE customer = @customer /* scan-canary-7711 {} */",
                "pad ".repeat(120)
            );
            let xml = selective_scan_xml(&sql);
            let mut context = template.context.clone();
            context.query_hash = Some("0x0123456789ABCDEF".into());
            context.source_text_bytes = Some((sql.encode_utf16().count() * 2).to_string());
            let param_ctx = PlanParameterContext {
                retrieved_from_cache: Some(true),
                parameters: vec![PlanParameter {
                    name: Some("@customer".into()),
                    data_type: Some("int".into()),
                    compiled: PlanParameterValueState::Recorded,
                    runtime: PlanParameterValueState::Recorded,
                    comparison: PlanParameterComparison::DifferentRepresentation,
                    integer_values_equal: Some(false),
                }],
                uses: vec![],
                issues: vec![PlanParameterIssue::UnsupportedUse],
            };
            let mut def = MssqlRetainedPlanDefinition {
                context,
                statement: MssqlCachedPayload {
                    payload: DefinitionPayload::sql(&sql, Engine::Mssql),
                    source_utf16_bytes: Some((sql.encode_utf16().count() * 2).to_string()),
                    native_sha256: Some(digest(sql.as_bytes())),
                },
                module_parameters: None,
                current_type_schema: None,
                relational_input_proof: None,
                parameter_context_proof: None,
                statement_executions: Some("50".into()),
                last_start_without_offset: Some("2020-01-01T11:59:59.003".into()),
                database_last_query_plan_stats: Some(true),
                batch_plan: MssqlCachedPayload {
                    payload: DefinitionPayload::plan(&xml, Engine::Mssql),
                    source_utf16_bytes: Some((xml.encode_utf16().count() * 2).to_string()),
                    native_sha256: Some(digest(xml.as_bytes())),
                },
            };
            def.parameter_context_proof =
                Some(MssqlParameterContextProof::for_retained(source, &def, param_ctx).unwrap());
            def
        }
        ScalarCase::NotObserved => unreachable!(),
    };
    retained.relational_input_proof = None;
    if let Some(proof) = retained.parameter_context_proof.take() {
        retained.parameter_context_proof =
            Some(MssqlParameterContextProof::for_retained(source, &retained, proof.context).unwrap());
    }
    let record = ObjectDefinitionRecord::new(
        culprit_core::source_id::RETAINED_PLANS.into(),
        ObjectDefinitionFact::MssqlRetainedPlan(Box::new(retained)),
    )
    .unwrap();
    vec![TelemetryRecord::ObjectDefinition(record)]
}

async fn synthesize_scalar_episode(case: ScalarCase, seq_base: u64) -> DerivedDiagnosis {
    let mut fixture = relational_input_question_fixture(TvpLoopCase::MissingBinding);
    let TelemetryRecord::ObjectDefinition(template_rec) = &fixture.records[0] else { panic!() };
    let ObjectDefinitionFact::MssqlRetainedPlan(template) = &template_rec.fact else { panic!() };
    let template = (**template).clone();

    let (_, activity) = seal(
        fixture.activity_source,
        vec![fixture.activity_row.record],
        seq_base,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let basis = reference(&activity, 0);
    let SourceObservation::CurrentRequestWork { result, .. } = &mut fixture.work_source.observation else { panic!() };
    result.focus.as_mut().unwrap().parent = basis.clone();
    let (_, work) = seal(
        fixture.work_source,
        vec![fixture.work_row.record],
        seq_base + 1,
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
    let rows = {
        let SourceObservation::SourceRead { focus_key, .. } = &mut source.observation else { panic!() };
        *focus_key = focus.key().unwrap();
        let built = build_scalar_retained(&source, &template, case);
        let SourceObservation::SourceRead { snapshot, .. } = &mut source.observation else { panic!() };
        snapshot.records = built.len() as u32;
        let state_complete = case != ScalarCase::Evidence(ParameterEvidenceCase::PartialSource);
        snapshot.sources = vec![if case == ScalarCase::Evidence(ParameterEvidenceCase::SourceUnavailable) {
            StateSourceCoverage::unavailable(
                culprit_core::source_id::RETAINED_PLANS.into(),
                culprit_core::ErrorCode::PermissionDenied,
                None,
            )
        } else {
            StateSourceCoverage::observed(
                culprit_core::source_id::RETAINED_PLANS.into(),
                built.len() as u32,
                state_complete,
            )
        }];
        if !built.is_empty() {
            // Re-bind proof after final snapshot header adjustment so source scope matches byte-for-byte.
            let mut rows = built;
            let TelemetryRecord::ObjectDefinition(rec) = &mut rows[0] else { panic!() };
            let ObjectDefinitionFact::MssqlRetainedPlan(ret) = &mut rec.fact else { panic!() };
            if let Some(proof) = ret.parameter_context_proof.take() {
                ret.parameter_context_proof =
                    Some(MssqlParameterContextProof::for_retained(&source, ret, proof.context).unwrap());
            }
            rows[0] = TelemetryRecord::ObjectDefinition(
                ObjectDefinitionRecord::new(rec.source.clone(), rec.fact.clone()).unwrap(),
            );
            rows
        } else {
            built
        }
    };

    let (_, result_cap) = seal(
        source,
        rows,
        seq_base + 2,
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
    let diag_source = CollectionDescription {
        target: activity.source().target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: basis.capture_ref(),
            step_id: digest(format!("scalar-parameter-step-{seq_base}").as_bytes()),
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
    let (_, diagnostics) = seal(
        diag_source,
        diag_rows,
        seq_base + 3,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:05Z",
        "2020-01-01T12:00:06Z",
    )
    .await;
    policy(false).synthesize_captures(&diagnostics, &[&activity, &work, &result_cap], RECORDED_AT, &display()).unwrap()
}

#[tokio::test]
#[cfg(feature = "mssql")]
async fn small_to_large_and_large_to_small_cached_scalar_plans_reach_gold_as_candidates_with_conditional_remedies() {
    let lookup_out = synthesize_scalar_episode(ScalarCase::CachedLookup, 8_850_000).await;
    lookup_out.validate().unwrap();
    let [incident] = lookup_out.incidents.as_slice() else { panic!() };
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.culprit_queries.is_empty(), "retained scalar reuse never becomes an exclusive culprit");
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(matches!(&incident.affected_queries[0].identity, Some(DerivedQueryIdentity::Activity { .. })));
    let [candidate] = incident.candidate_queries.as_slice() else { panic!() };
    assert!(matches!(&candidate.identity, Some(DerivedQueryIdentity::CachedStatement(_))));
    assert!(candidate.causal_role.is_none());
    let mech = incident.mechanism.as_deref().expect("parameterized lookup mechanism");
    assert!(mech.contains(&t!("insight.parameter_lookup.limit")));
    assert!(incident.developer_action.contains(&t!("insight.parameter_lookup.verify")) == false);
    assert!(incident.verification.contains(&t!("insight.parameter_lookup.verify")));
    assert!(incident.entry.reasons.contains(&t!("view.relational_input_question.execution_limit")));
    let encoded = serde_json::to_string(&lookup_out).unwrap();
    assert!(!encoded.contains("private-9264"));

    let scan_out = synthesize_scalar_episode(ScalarCase::SelectiveScan, 8_850_010).await;
    scan_out.validate().unwrap();
    let [scan_inc] = scan_out.incidents.as_slice() else { panic!() };
    assert!(scan_inc.culprit_queries.is_empty());
    assert_eq!(scan_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(scan_inc.candidate_queries.len(), 1);
    assert!(scan_inc.mechanism.as_deref().unwrap().contains("Clustered Index Scan"));
    assert!(scan_inc.developer_action.contains(&t!("insight.parameter_context.fix")));
    assert!(!scan_inc.developer_action.contains("INCLUDE"), "large-to-small scan never invents a lookup INCLUDE fix");
    assert!(!serde_json::to_string(&scan_out).unwrap().contains("scan-canary-7711"));

    let seek_out =
        synthesize_scalar_episode(ScalarCase::Evidence(ParameterEvidenceCase::AccessMismatch), 8_850_020).await;
    seek_out.validate().unwrap();
    let [seek_inc] = seek_out.incidents.as_slice() else { panic!() };
    assert_eq!(seek_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(seek_inc.developer_action.contains(&t!("insight.parameter_context.access_fix")));
}

#[tokio::test]
async fn healthy_psp_variants_covering_access_and_equal_integers_never_manufacture_parameter_candidates_or_culprits() {
    for (idx, case) in [
        ScalarCase::EqualIntegerValues,
        ScalarCase::Evidence(ParameterEvidenceCase::HealthyNativeVariant),
        ScalarCase::Evidence(ParameterEvidenceCase::AccurateAccess),
        ScalarCase::Evidence(ParameterEvidenceCase::SameRepresentation),
        ScalarCase::Evidence(ParameterEvidenceCase::UnusedDifferentParameter),
        ScalarCase::Evidence(ParameterEvidenceCase::Estimated),
        ScalarCase::Evidence(ParameterEvidenceCase::MissingRuntimeParameter),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_scalar_episode(case, 8_850_100 + idx as u64 * 10).await;
        out.validate().unwrap();
        let [incident] = out.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty(), "{case:?}");
        assert!(incident.candidate_queries.is_empty(), "healthy/equal/unqualified {case:?} adds no candidate");
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished, "{case:?}");
    }

    // Healthy PSP child variants without a dispatcher runtime row and converged plans emit zero parameter findings in Gold.
    for (idx, cohort_case) in
        [QueryCohortCase::HealthyVariantFamily, QueryCohortCase::ConvergedPlans].into_iter().enumerate()
    {
        let (source, records) = query_cohort_evidence(cohort_case);
        let warehouse = Warehouse::open(
            ReadOnlyFixture::capture_at(
                source,
                records,
                8_850_300 + idx as u64,
                "2020-01-01T12:00:00Z",
                "2020-01-01T12:00:05Z",
            )
            .await,
            false,
        )
        .await;
        let capture = warehouse.read(QUERY_FAMILIES).await;
        assert!(capture.is_read_complete());
        let synthesizer =
            ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "psp-healthy-gold-1").unwrap();
        let diagnosis = synthesizer.synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
        diagnosis.validate().unwrap();
        assert_eq!(diagnosis.description.output_coverage.findings, DerivedOutputCoverage::Evaluated);
        assert!(
            !diagnosis
                .findings
                .iter()
                .any(|f| matches!(f.finding.check.as_str(), "parameter-sensitive" | "plan-regression")),
            "{cohort_case:?}"
        );
    }
}

#[tokio::test]
async fn recycled_incarnations_vanished_plans_and_compilation_followups_preserve_incidents_without_stale_attribution() {
    for (idx, (case, expected_reason)) in [
        (ScalarCase::IncarnationChanged, t!("view.relational_input_question.changed")),
        (ScalarCase::NotObserved, t!("view.relational_input_question.not_observed")),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_scalar_episode(case, 8_850_400 + idx as u64 * 10).await;
        out.validate().unwrap();
        let [incident] = out.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1, "resolved/transient incident preserves original victim");
        assert!(incident.candidate_queries.is_empty() && incident.culprit_queries.is_empty());
        assert!(incident.entry.reasons.contains(&expected_reason));
    }

    for (idx, case) in QueryCompilationFollowupCase::ALL.iter().copied().enumerate() {
        let seq = 8_850_500 + idx as u64 * 10;
        let orig = query_compilation_followup_original();
        let (_, history_cap) = seal(
            orig.source.clone(),
            orig.records.clone(),
            seq,
            EvidenceFamily::QueryRuntime,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let history_warehouse = Warehouse::open(
            ReadOnlyFixture::capture_at(
                orig.source.clone(),
                orig.records.clone(),
                seq + 1,
                "2020-01-01T12:00:00Z",
                "2020-01-01T12:00:01Z",
            )
            .await,
            false,
        )
        .await;
        let full_history = history_warehouse.read(QUERY_FAMILIES).await;
        drop(history_cap);
        let runtime_pos = full_history
            .records()
            .iter()
            .position(|r| DiagnosticOriginalCondition::from_record(full_history.source(), &r.row().record).is_some())
            .unwrap();
        let basis = reference(&full_history, runtime_pos);
        let update = observe_diagnostic_original(
            full_history.source(),
            &basis.capture_ref(),
            &[DiagnosticOriginalRow { reference: &basis, record: &full_history.records()[runtime_pos].row().record }],
            None,
            &[],
            "2020-01-01T12:00:01.000000000Z",
        )
        .unwrap();
        let episode = update.checkpoint.episodes[0].finish_original_observation_planning().unwrap();
        let focus = episode.source_read_focus(SourceReadQuestion::CompilationHistory).unwrap().unwrap();
        let execution = SourceReadExecutionScope::from_context(
            focus.question,
            &focus.original_context(full_history.source()).unwrap(),
        )
        .unwrap();
        let (comp_source, comp_rows) =
            query_compilation_followup_result(full_history.source(), &orig.records, &focus, &execution, case);
        let (_, comp_cap) = seal(
            comp_source,
            comp_rows,
            seq + 2,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:02Z",
            "2020-01-01T12:00:03Z",
        )
        .await;
        let comp_refs = comp_cap.records().iter().enumerate().map(|(i, _)| reference(&comp_cap, i)).collect::<Vec<_>>();
        let selected = comp_refs
            .iter()
            .zip(comp_cap.records())
            .filter(|(_, r)| focus.retains_result_record(&r.row().record))
            .map(|(r, row)| (r, row.row()))
            .collect::<Vec<_>>();
        let answer = QueryCompilationAnswer::from_originals(
            &focus,
            full_history.source(),
            full_history.records()[runtime_pos].row(),
            comp_cap.source(),
            &comp_cap.verified_original().unwrap().original().clone(),
            &selected,
        )
        .unwrap();
        let answered = episode
            .enroll_source_read(focus.clone())
            .unwrap()
            .transition_source_read(&focus, SourceReadStageState::Collecting)
            .unwrap()
            .transition_source_read(
                &focus,
                SourceReadStageState::Answered {
                    answer: Box::new(SourceReadAnswer::QueryCompilation(Box::new(answer))),
                },
            )
            .unwrap();
        let revision = DiagnosticRevision::between(None, answered, "2020-01-01T12:00:04Z").unwrap().unwrap();
        let mut checkpoint = DiagnosticCheckpointPart {
            id: String::new(),
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            target: full_history.source().target.clone(),
            input: basis.capture_ref(),
            part: 0,
            parts: 1,
            episodes: if revision.episode.retained_in_checkpoint() { vec![revision.episode.clone()] } else { vec![] },
        };
        checkpoint.id = checkpoint.expected_id().unwrap();
        let diag_source = CollectionDescription {
            target: full_history.source().target.clone(),
            partition: SourcePartition::Target,
            source: DIAGNOSTICS_SOURCE.into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
                controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
                parent: basis.capture_ref(),
                step_id: digest(format!("compilation-followup-step-{seq}").as_bytes()),
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
        let (_, diag_cap) = seal(
            diag_source,
            diag_rows,
            seq + 3,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:04Z",
            "2020-01-01T12:00:05Z",
        )
        .await;
        let out =
            policy(false).synthesize_captures(&diag_cap, &[&full_history, &comp_cap], RECORDED_AT, &display()).unwrap();
        out.validate().unwrap();
        let [incident] = out.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.culprit_queries.is_empty() && incident.candidate_queries.is_empty(), "{case:?}");
        assert!(incident.original_interval.is_some());
        if matches!(case, QueryCompilationFollowupCase::Exact | QueryCompilationFollowupCase::Partial) {
            assert!(incident.mechanism.is_some(), "{case:?}");
            assert!(incident.entry.reasons.contains(&t!("view.compilation.limit.lifetime")));
        } else {
            assert!(incident.mechanism.is_none(), "changed/missing/unavailable {case:?} attaches no foreign entry");
        }
    }
}

#[tokio::test]
#[cfg(feature = "mssql")]
async fn competing_forcing_and_same_input_ab_comparisons_keep_conditional_non_exclusive_verdicts() {
    // Forced plan in QueryHistory retains its forcing caveat and non-executable informational actionability in Gold.
    let (source, mut records) = query_cohort_evidence(QueryCohortCase::PlanVariation);
    for row in &mut records {
        if let TelemetryRecord::PlanDefinition(plan) = row
            && plan.identity.plan_id == "47"
        {
            plan.native_properties.insert("is_forced_plan".into(), "true".into());
        }
    }
    let warehouse = Warehouse::open(
        ReadOnlyFixture::capture_at(source, records, 8_850_700, "2020-01-01T12:00:00Z", "2020-01-01T12:00:05Z").await,
        false,
    )
    .await;
    let capture = warehouse.read(QUERY_FAMILIES).await;
    let synthesizer =
        ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "forced-variation-gold-1").unwrap();
    let diagnosis = synthesizer.synthesize_captures(&capture, &[], RECORDED_AT, &display()).unwrap();
    diagnosis.validate().unwrap();
    let finding = diagnosis.findings.iter().find(|f| f.finding.check == "parameter-sensitive").expect("plan variation");
    assert_eq!(finding.finding.actionability, Actionability::Information);
    assert_eq!(finding.finding.severity, Severity::Low);
    assert!(matches!(finding.finding.facts.runtime_variation, Some(FindingRuntimeVariation::PlanAverages { .. })));
    assert!(finding.finding.fix.as_ref().unwrap().sql.is_none());

    // Current-source same-input A/B comparison distinguishes unequal vs equal compile-time integers without exclusive blame.
    let sql = "SELECT payload FROM dbo.orders WHERE customer = @customer";
    let reused = culprit_plans::mssql::parse_statement(&scalar_lookup_xml(sql), None).unwrap();
    let fresh_scan_xml = r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="SELECT payload FROM dbo.orders WHERE customer = @customer" StatementType="SELECT" QueryHash="0x0123456789ABCDEF" StatementSubTreeCost="5" RetrievedFromCache="false"><QueryPlan>
<ParameterList><ColumnReference Column="@customer" ParameterDataType="int" ParameterCompiledValue="(1)" ParameterRuntimeValue="(1)"/></ParameterList>
<RelOp NodeId="10" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" Parallel="false" EstimateRows="5000" EstimatedRowsRead="5000" EstimatedTotalSubtreeCost="5">
  <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="payload"/></OutputList>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="5000" ActualRowsRead="5000" ActualExecutions="1" ActualLogicalReads="500"/></RunTimeInformation>
  <IndexScan Ordered="false"><Object Database="[app]" Schema="[dbo]" Table="[orders]" Index="[pk_orders]" Alias="[o]" IndexKind="Clustered"/></IndexScan>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let fresh_scan = culprit_plans::mssql::parse_statement(fresh_scan_xml, None).unwrap();
    let pair = culprit_plans::mssql::runtime_input_pair(&reused, &fresh_scan);
    assert_eq!(pair.relation(), NativeRuntimeInputRelation::Same);
    let cmp = culprit_core::plan_compare::compare_native_pair(pair, "Cached lookup", "Fresh scan", &Default::default());
    let scenario = cmp.scenarios.iter().find(|s| s.kind == ScenarioKind::Parameters).expect("parameter A/B scenario");
    assert!(scenario.detail.as_deref().unwrap().contains(&t!("core.plan_compare.scalar_reuse.limit")));
    assert!(!scenario.detail.as_deref().unwrap().contains(&t!("core.plan_compare.scalar_reuse.equal_compile_input")));
}
