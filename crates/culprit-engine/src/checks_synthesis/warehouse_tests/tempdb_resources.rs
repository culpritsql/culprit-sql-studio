//! Canonical warehouse originals exercise Track 6.2 tempdb and temporary-resource mechanisms:
//! internal sort/hash/spool vs user temp objects, later WAITFOR after earlier batch allocation,
//! bad estimates vs legitimate volume, concurrent modest reservations + workload-group governance,
//! PVS/ADR retention vs tempdb data/log capacity vs XTP metadata memory vs shared PFS/GAM/SGAM,
//! missing task generations, request reuse, and recovery before enrichment.
use super::*;
use culprit_core::{
    SourceOutcome, SourceState, TaskAllocationActivity, collected_analysis::EvidenceGapReason, source_id,
};
use culprit_evidence::original_projection::OriginalProjection;
use test_support::{
    CleanupCase, StorageCase, TempdbMetadataMemoryCase, cleanup_evidence, maintenance_memory_evidence_cases,
    request_memory_evidence_cases, reservation_demand_evidence_cases, shared_page_wait_evidence_cases,
    storage_evidence, tempdb_metadata_memory_evidence, temporary_resource_basis, temporary_resource_result,
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
        EvidenceFamily::OperationalSamples => hydrate(&warehouse, family).await,
        _ => warehouse.read(&[family]).await,
    };
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    (warehouse, capture)
}

async fn hydrate(warehouse: &Warehouse, family: EvidenceFamily) -> EvidenceCapture {
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
enum TaskFollowupCase {
    InternalSortHashSpool,
    UserTempObjectsLaterWaitfor,
    MixedAllocations,
    RequestReused,
    MissingTaskGeneration,
    EndedRequest,
    UnavailableSource,
}

async fn synthesize_tempdb_followup(case: TaskFollowupCase, seq: u64) -> DerivedDiagnosis {
    let (basis_source, basis_records) = temporary_resource_basis();
    let (_, basis) = seal(
        basis_source,
        basis_records,
        seq,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let basis_orig = basis.verified_original().unwrap();
    let basis_refs = (0..basis_orig.records().len()).map(|i| basis_orig.reference(i).unwrap()).collect::<Vec<_>>();
    let basis_rows = basis_orig
        .records()
        .iter()
        .zip(&basis_refs)
        .map(|(row, r)| DiagnosticOriginalRow { reference: r, record: &row.row().record })
        .collect::<Vec<_>>();
    let observed = observe_diagnostic_original(
        basis_orig.source(),
        basis_orig.original(),
        &basis_rows,
        None,
        &[],
        "2020-01-01T12:00:02Z",
    )
    .unwrap();
    let episode =
        observed.checkpoint.episodes.into_iter().next().unwrap().finish_original_observation_planning().unwrap();
    let focus = episode.source_read_focus(SourceReadQuestion::TemporarySpaceContributors).unwrap().unwrap();
    let execution = SourceReadExecutionScope::from_context(
        focus.question,
        &NativeReadContext::from_snapshot(basis_orig.source()).unwrap(),
    )
    .unwrap();

    let base_state =
        if case == TaskFollowupCase::UnavailableSource { SourceState::Unavailable } else { SourceState::Collected };
    let (mut result_source, mut result_records) = temporary_resource_result(&focus, &execution, base_state);
    if case != TaskFollowupCase::UnavailableSource {
        result_records.truncate(1);
        let TelemetryRecord::Operational(row) = &mut result_records[0] else { panic!() };
        let OperationalFact::TaskSpace(task) = &mut row.fact else { panic!() };
        match case {
            TaskFollowupCase::InternalSortHashSpool => {
                task.user_allocated_pages = Some("0".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("4096".into());
                task.internal_deallocated_pages = Some("0".into());
                task.request.as_mut().unwrap().statement = DefinitionPayload::sql(
                    "SELECT customer_id, SUM(total) FROM dbo.orders GROUP BY customer_id ORDER BY SUM(total) DESC",
                    Engine::Mssql,
                );
            }
            TaskFollowupCase::UserTempObjectsLaterWaitfor => {
                task.user_allocated_pages = Some("2048".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("0".into());
                task.internal_deallocated_pages = Some("0".into());
                // Current SQL is a later WAITFOR DELAY statement in the batch after earlier #temp allocation.
                task.request.as_mut().unwrap().statement =
                    DefinitionPayload::sql("WAITFOR DELAY '00:00:10'", Engine::Mssql);
            }
            TaskFollowupCase::MixedAllocations => {
                task.user_allocated_pages = Some("1024".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("2048".into());
                task.internal_deallocated_pages = Some("0".into());
            }
            TaskFollowupCase::RequestReused => {
                task.user_allocated_pages = Some("0".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("4096".into());
                task.internal_deallocated_pages = Some("0".into());
                task.after_check.as_mut().unwrap().state = TaskRequestState::Changed;
                task.request.as_mut().unwrap().statement =
                    DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing };
                task.statement_identity = None;
                task.plan_identity = None;
            }
            TaskFollowupCase::MissingTaskGeneration => {
                task.user_allocated_pages = Some("1024".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("0".into());
                task.internal_deallocated_pages = Some("0".into());
                task.request = None;
                task.before_check = None;
                task.after_check = None;
                task.statement_identity = None;
                task.plan_identity = None;
            }
            TaskFollowupCase::EndedRequest => {
                task.user_allocated_pages = Some("512".into());
                task.user_deallocated_pages = Some("0".into());
                task.internal_allocated_pages = Some("0".into());
                task.internal_deallocated_pages = Some("0".into());
                task.after_check.as_mut().unwrap().state = TaskRequestState::Missing;
                task.request.as_mut().unwrap().statement =
                    DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing };
                task.statement_identity = None;
                task.plan_identity = None;
            }
            TaskFollowupCase::UnavailableSource => unreachable!(),
        }
        row.key = task.key().unwrap();
        let SourceObservation::SourceRead { snapshot, .. } = &mut result_source.observation else { panic!() };
        snapshot.records = 1;
        snapshot.sources = vec![StateSourceCoverage::observed(
            source_id::TASK_SPACE.into(),
            1,
            matches!(
                case,
                TaskFollowupCase::InternalSortHashSpool
                    | TaskFollowupCase::UserTempObjectsLaterWaitfor
                    | TaskFollowupCase::MixedAllocations
            ),
        )];
    }

    let (_, result_cap) = seal(
        result_source,
        result_records,
        seq + 1,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:10Z",
        "2020-01-01T12:00:11Z",
    )
    .await;
    let res_orig = result_cap.verified_original().unwrap();
    let res_refs = (0..res_orig.records().len()).map(|i| res_orig.reference(i).unwrap()).collect::<Vec<_>>();
    let selected = res_refs.iter().zip(res_orig.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
    let answer = TemporaryResourceAnswer::from_originals(
        &focus,
        basis_orig.source(),
        basis_orig.records()[0].row(),
        res_orig.source(),
        res_orig.original(),
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
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::TemporaryResources(Box::new(answer))) },
        )
        .unwrap();
    let revision = DiagnosticRevision::between(None, answered, "2020-01-01T12:00:12Z").unwrap().unwrap();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().target.clone(),
        input: basis_orig.original().clone(),
        part: 0,
        parts: 1,
        episodes: if revision.episode.retained_in_checkpoint() { vec![revision.episode.clone()] } else { vec![] },
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let diag_source = CollectionDescription {
        target: basis.source().target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: basis_orig.original().clone(),
            step_id: digest(format!("tempdb-followup-step-{seq}").as_bytes()),
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
    drop(basis_orig);
    drop(res_orig);
    let (_, diag_cap) = seal(
        diag_source,
        diag_rows,
        seq + 2,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:12Z",
        "2020-01-01T12:00:13Z",
    )
    .await;
    policy(false).synthesize_captures(&diag_cap, &[&basis, &result_cap], RECORDED_AT, &display()).unwrap()
}

#[tokio::test]
async fn internal_sort_hash_user_temp_later_waitfor_and_unchecked_task_generations_stay_distinct_in_gold() {
    let internal = synthesize_tempdb_followup(TaskFollowupCase::InternalSortHashSpool, 8_910_000).await;
    internal.validate().unwrap();
    let [inc] = internal.incidents.as_slice() else { panic!() };
    assert!(inc.culprit_queries.is_empty());
    assert_eq!(inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(inc.candidate_queries.len(), 1);
    assert!(inc.mechanism.as_deref().unwrap().contains(&t!("view.task_space.activity.internal")));
    assert!(inc.developer_action.contains(&t!("view.task_space.investigate.internal")));

    // Later WAITFOR DELAY after earlier #temp allocation keeps current SQL as a candidate only, with explicit batch caveat.
    let waitfor = synthesize_tempdb_followup(TaskFollowupCase::UserTempObjectsLaterWaitfor, 8_910_010).await;
    waitfor.validate().unwrap();
    let [wf_inc] = waitfor.incidents.as_slice() else { panic!() };
    assert!(wf_inc.culprit_queries.is_empty(), "current WAITFOR SQL must never be blamed as the allocator");
    assert_eq!(wf_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    let [wf_cand] = wf_inc.candidate_queries.as_slice() else { panic!() };
    assert!(wf_cand.causal_role.is_none());
    let DefinitionPayload::Available { text, .. } = &wf_cand.sql else { panic!() };
    assert!(text.contains("WAITFOR"));
    assert!(wf_inc.mechanism.as_deref().unwrap().contains(&t!("view.task_space.activity.user")));
    assert!(wf_inc.mechanism.as_deref().unwrap().contains(&t!("view.task_space.context.checked")));
    assert!(wf_inc.entry.reasons.contains(&t!("view.task_space.limit.context")));

    let mixed = synthesize_tempdb_followup(TaskFollowupCase::MixedAllocations, 8_910_020).await;
    let [mx_inc] = mixed.incidents.as_slice() else { panic!() };
    assert!(mx_inc.mechanism.as_deref().unwrap().contains(&t!("view.task_space.activity.mixed")));
    assert!(mx_inc.developer_action.contains(&t!("view.task_space.investigate.mixed")));

    // Request reuse, missing task generation, ended request and unavailable TaskSpace withhold candidates while preserving mechanisms/limits.
    for (idx, (case, expected_text, expected_limit)) in [
        (
            TaskFollowupCase::RequestReused,
            t!("view.task_space.context.changed"),
            t!("view.task_space.limit.source_partial"),
        ),
        (
            TaskFollowupCase::MissingTaskGeneration,
            t!("view.task_space.context.unverified"),
            t!("view.task_space.limit.source_partial"),
        ),
        (
            TaskFollowupCase::EndedRequest,
            t!("view.task_space.context.missing"),
            t!("view.task_space.limit.source_partial"),
        ),
        (
            TaskFollowupCase::UnavailableSource,
            t!("view.task_space.summary.empty"),
            t!("view.task_space.limit.source_unavailable"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_tempdb_followup(case, 8_910_100 + idx as u64 * 10).await;
        out.validate().unwrap();
        let [incident] = out.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty() && incident.candidate_queries.is_empty(), "{case:?}");
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished, "{case:?}");
        assert!(incident.mechanism.as_deref().unwrap().contains(&expected_text), "{case:?}");
        assert!(incident.entry.reasons.contains(&expected_limit), "{case:?}");
    }
}

#[tokio::test]
async fn concurrent_modest_reservations_workload_group_tempdb_governance_and_maintenance_grants_discriminate_cleanly() {
    let demand_cases = reservation_demand_evidence_cases();
    // Case 0: one oversized reservation (12,288 KiB of 16,384 KiB).
    // Case 1: several concurrent modest reservations (2,048 + 4,096 + 4,096 KiB).
    for (idx, (source, rows)) in demand_cases.into_iter().take(2).enumerate() {
        let (_, cap) = seal(
            source,
            rows,
            8_910_200 + idx as u64 * 10,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let orig = cap.verified_original().unwrap();
        let waiter_pos = orig
            .records()
            .iter()
            .position(|r| {
                matches!(
                    &r.row().record,
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::ExecutionGrant(g),
                        ..
                    }) if g.state == ExecutionGrantState::Waiting
                )
            })
            .unwrap();
        let explanation = explanations::request_memory(&orig, waiter_pos).unwrap();
        if idx == 0 {
            assert_eq!(explanation.candidates.len(), 1);
            assert!(
                explanation.action.as_deref().unwrap().contains(&t!("view.request_memory.demand.larger_investigation"))
            );
        } else {
            assert_eq!(explanation.candidates.len(), 3, "all three concurrent modest consumers remain candidates");
            assert!(
                explanation
                    .action
                    .as_deref()
                    .unwrap()
                    .contains(&t!("view.request_memory.demand.several_investigation"))
            );
        }
    }

    // Workload-group tempdb space governance retains group limits, peak and violation counts without blaming current request as allocator.
    let (gov_source, gov_rows) = request_memory_evidence_cases()
        .into_iter()
        .find(|(_, rows)| {
            rows.iter().any(|r| {
                matches!(
                    r,
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::ExecutionGrant(g),
                        ..
                    }) if g.workload_group_tempdb.is_some()
                )
            })
        })
        .unwrap();
    let (_, gov_cap) = seal(
        gov_source,
        gov_rows,
        8_910_250,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let gov_orig = gov_cap.verified_original().unwrap();
    let gov_waiter = gov_orig
        .records()
        .iter()
        .position(|r| {
            matches!(
                &r.row().record,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::ExecutionGrant(g),
                    ..
                }) if g.state == ExecutionGrantState::Waiting
            )
        })
        .unwrap();
    let gov_exp = explanations::request_memory(&gov_orig, gov_waiter).unwrap();
    assert!(gov_exp.mechanism.as_deref().unwrap().contains(&t!("view.request_memory.tempdb.violations", count = "3")));
    assert!(gov_exp.action.as_deref().unwrap().contains(&t!("view.request_memory.tempdb.investigation")));
    assert!(gov_exp.limitations.contains(&t!("view.request_memory.tempdb.limit")));

    // Maintenance index rebuild reservation vs application waiter stays distinct.
    let (maint_source, maint_rows) = maintenance_memory_evidence_cases().remove(0);
    let (_, maint_cap) = seal(
        maint_source,
        maint_rows,
        8_910_260,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let maint_orig = maint_cap.verified_original().unwrap();
    let maint_waiter = maint_orig
        .records()
        .iter()
        .position(|r| {
            matches!(
                &r.row().record,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::ExecutionGrant(g),
                    ..
                }) if g.state == ExecutionGrantState::Waiting
            )
        })
        .unwrap();
    let maint_exp = explanations::request_memory(&maint_orig, maint_waiter).unwrap();
    assert_eq!(maint_exp.candidates.len(), 1);
    let DefinitionPayload::Available { text, .. } = &maint_exp.candidates[0].sql else { panic!() };
    assert!(text.contains("ALTER INDEX"));
}

#[tokio::test]
async fn tempdb_data_vs_log_xtp_metadata_memory_adr_pvs_and_shared_allocation_maps_keep_independent_scopes() {
    // Shared tempdb data vs transaction log: log-full/reuse wait never recommends log backups on tempdb.
    let (log_source, log_rows) = storage_evidence(StorageCase::SharedTempdbLog);
    let (_, log_cap) = seal(
        log_source,
        log_rows,
        8_910_300,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let storage = crate::collected_resources::storage(&log_cap).unwrap();
    let temp_log = storage.report.entries.iter().find(|e| e.source == source_id::LOG_CAPACITY).unwrap();
    assert!(temp_log.explanation.contains(&t!("view.storage.temp_log.active")));
    assert!(!temp_log.explanation.to_ascii_lowercase().contains("backup log"));

    // Tempdb XTP metadata memory: VARHEAP reservation vs disabled feature state.
    for (idx, case) in [TempdbMetadataMemoryCase::Reserved, TempdbMetadataMemoryCase::Disabled].into_iter().enumerate()
    {
        let (xtp_source, xtp_rows) = tempdb_metadata_memory_evidence(case);
        let (_, xtp_cap) = seal(
            xtp_source,
            xtp_rows,
            8_910_320 + idx as u64 * 10,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let xtp = crate::collected_resources::tempdb_metadata_memory(&xtp_cap).unwrap();
        if case == TempdbMetadataMemoryCase::Reserved {
            assert_eq!(xtp.report.metadata_enabled, Some(true));
            assert_eq!(xtp.report.entries.len(), 1);
            assert_eq!(xtp.report.entries[0].explanation, t!("view.tempdb_metadata_memory.usage.reservation"));
        } else {
            assert_eq!(xtp.report.metadata_enabled, Some(false));
            assert!(xtp.report.entries.is_empty());
        }
    }

    // ADR/PVS persistent version store skip reason stays a mechanism observation without inventing rebuild ownership.
    let (pvs_source, pvs_rows) = cleanup_evidence(CleanupCase::PersistentVersions);
    let (_, pvs_cap) = seal(
        pvs_source,
        pvs_rows,
        8_910_350,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let pvs = crate::collected_resources::cleanup(&pvs_cap).unwrap();
    let pvs_entry =
        pvs.report.entries.iter().find(|e| e.reference.source == source_id::PERSISTENT_VERSION_STORE).unwrap();
    assert!(pvs_entry.explanation.contains(&t!("view.cleanup.pvs.off_row")));
    assert!(pvs.report.limitations.contains(&t!("view.cleanup.limit.ownership")));

    // Shared tempdb PFS/GAM/SGAM page waits keep the waiting request in its application database without last-leaf advice.
    let (shared_source, shared_rows) = shared_page_wait_evidence_cases().remove(0);
    let (_, shared_cap) = seal(
        shared_source,
        shared_rows,
        8_910_370,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let shared_orig = shared_cap.verified_original().unwrap();
    let page_exp = explanations::original_observation(&shared_orig, 0).unwrap();
    assert!(page_exp.culprits.is_empty());
    let page_view = crate::collected_resources::page_waits(&shared_cap, None).unwrap();
    assert!(!page_view.report.entries.is_empty());
    for entry in &page_view.report.entries {
        assert!(!entry.investigation.contains("OPTIMIZE_FOR_SEQUENTIAL_KEY"));
    }
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::PageWait(pw), .. }) =
        &shared_orig.records()[0].row().record
    else {
        panic!()
    };
    assert_eq!(pw.request.database.as_deref(), Some("db"));
    assert_eq!(pw.target.as_ref().unwrap().header.database_id, 2);

    // Bad estimate spill vs legitimate high-volume spill without misestimate.
    let spill_xml = |est: u64| {
        format!(
            r#"<ShowPlanXML Version="1.539" Build="16.0.1000.6" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementText="SELECT customer, payload FROM dbo.orders ORDER BY payload" StatementType="SELECT" QueryHash="0x1111222233334444" StatementSubTreeCost="20"><QueryPlan>
<RelOp NodeId="0" PhysicalOp="Sort" LogicalOp="Sort" Parallel="false" EstimateRows="{est}" EstimatedTotalSubtreeCost="20">
  <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Column="payload"/></OutputList>
  <Warnings><SpillToTempDb SpillLevel="2" SpilledThreadCount="1"/></Warnings>
  <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="50000" ActualExecutions="1"/></RunTimeInformation>
  <Sort Distinct="false">
    <OrderBy><OrderByColumn Ascending="true"><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Column="payload"/></OrderByColumn></OrderBy>
    <RelOp NodeId="1" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" Parallel="false" EstimateRows="{est}" EstimatedRowsRead="{est}" EstimatedTotalSubtreeCost="5">
      <OutputList><ColumnReference Database="[app]" Schema="[dbo]" Table="[orders]" Column="payload"/></OutputList>
      <RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="50000" ActualRowsRead="50000" ActualExecutions="1"/></RunTimeInformation>
      <IndexScan Ordered="false"><Object Database="[app]" Schema="[dbo]" Table="[orders]" Index="[pk_orders]" IndexKind="Clustered"/></IndexScan>
    </RelOp>
  </Sort>
</RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
        )
    };
    let mut bad_est = culprit_plans::mssql::parse_statement(&spill_xml(1), None).unwrap();
    let mut legit_vol = culprit_plans::mssql::parse_statement(&spill_xml(50_000), None).unwrap();
    culprit_core::insights::annotate(&mut bad_est);
    culprit_core::insights::annotate(&mut legit_vol);
    let bad_rules: std::collections::BTreeSet<_> =
        bad_est.insights.as_ref().unwrap().items.iter().map(|i| i.rule.as_str()).collect();
    let legit_rules: std::collections::BTreeSet<_> =
        legit_vol.insights.as_ref().unwrap().items.iter().map(|i| i.rule.as_str()).collect();
    assert!(bad_rules.contains("spill") && bad_rules.contains("misestimate"));
    assert!(legit_rules.contains("spill") && !legit_rules.contains("misestimate"));
    let _ = SourceOutcome { source: source_id::TASK_SPACE.into(), state: SourceState::Collected, reason: None };
    let _ = TaskAllocationActivity::InternalObjects;
    let _ = EvidenceGapReason::MissingRows;
    let _ = reference(&shared_cap, 0);
    let _: &dyn OriginalProjection = &shared_orig;
}
