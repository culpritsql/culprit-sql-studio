//! Canonical warehouse originals exercise Track 6.4 page-latch attribution and target classification:
//! 1. Sequential rightmost-leaf INSERT contention vs two INSERT patterns + unrelated expensive SELECT.
//! 2. Non-rightmost leaf, branch, heap, and unclassified metadata targets (suppressing sequential-key fixes).
//! 3. Allocation-page contention (PFS, GAM, SGAM, IAM) vs physical I/O waits (PAGEIOLATCH).
//! 4. Page/index/request-ID reuse and missing permissions / unmapped pages.
//! 5. Direct parallel-task holder attribution vs task-only / sentinel / changing holders.
//! 6. Shared tempdb PFS/GAM/SGAM vs application index leaf keeping the waiter in its application database.
//! 7. Resolved contention before enrichment/publication and cross-provider wait boundaries (PG, MySQL, Yugabyte).
use super::*;
use culprit_core::WaitResourceKind;
use test_support::{
    PageTaskHolderCase, PageWriterCase, page_task_holder_fixture, page_writer_fixture, shared_page_wait_evidence_cases,
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

async fn seal_diagnostics(episode: DiagnosticEpisode, seq: u64) -> EvidenceCapture {
    let revision = DiagnosticRevision::between(None, episode, "2020-01-01T12:00:05Z").unwrap().unwrap();
    let parent = match &revision.episode.recovery {
        Some(
            DiagnosticRecoveryProof::CompleteScope { capture }
            | DiagnosticRecoveryProof::FocusedParticipantGone { capture, .. },
        ) => capture.clone(),
        None => revision.episode.latest.capture_ref(),
    };
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: revision.episode.scope.target.clone(),
        input: parent.clone(),
        part: 0,
        parts: 1,
        episodes: if revision.episode.retained_in_checkpoint() { vec![revision.episode.clone()] } else { vec![] },
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let source = CollectionDescription {
        target: revision.episode.scope.target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent,
            step_id: digest(format!("page-latch-step-{seq}").as_bytes()),
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
    seal(source, rows, seq, EvidenceFamily::OperationalSamples, "2020-01-01T12:00:05Z", "2020-01-01T12:00:06Z").await.1
}

async fn synthesize_writers(
    case: PageWriterCase,
    mutate_writers: impl FnOnce(&mut CollectionDescription, &mut PageWaitWriterObservation),
    resolved: bool,
    seq: u64,
) -> DerivedDiagnosis {
    let mut fixture = page_writer_fixture(case);
    let (_, act_cap) = seal(
        fixture.activity_source.clone(),
        fixture.activity_rows,
        seq,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let (_, page_cap) = seal(
        fixture.page_source,
        fixture.page_rows,
        seq + 1,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00.2Z",
        "2020-01-01T12:00:01Z",
    )
    .await;
    let waiter_ref = reference(&act_cap, 0);
    let page_ref = reference(&page_cap, 0);
    let cand_refs = (1..act_cap.records().len()).map(|idx| reference(&act_cap, idx)).collect::<Vec<_>>();

    let mut source = fixture.source;
    let TelemetryRecord::Operational(mut op) = fixture.records.remove(0) else { panic!() };
    let OperationalFact::PageWaitWriters(mut row) = op.fact else { panic!() };
    {
        let SourceObservation::PageWaitWriters { focus, .. } = &mut source.observation else { panic!() };
        focus.waiter.participant.parent = waiter_ref.clone();
        focus.page = page_ref.clone();
        for ((focus_cand, row_cand), new_ref) in
            focus.candidates.iter_mut().zip(row.candidates.iter_mut()).zip(&cand_refs)
        {
            focus_cand.parent = new_ref.clone();
            row_cand.parent = new_ref.clone();
            if let Some(old_proof) = row_cand.binding.take() {
                row_cand.binding = Some(
                    MssqlPageWritePlanProof::new(
                        &focus.target,
                        row_cand,
                        old_proof.native_plan_sha256,
                        old_proof.native_statement_sha256,
                        old_proof.accesses,
                    )
                    .unwrap(),
                );
            }
        }
    }
    mutate_writers(&mut source, &mut row);
    let SourceObservation::PageWaitWriters { focus, .. } = &source.observation else { panic!() };
    row.focus_key = focus.key().unwrap();
    op.key = row.key().unwrap();
    op.fact = OperationalFact::PageWaitWriters(row);
    let focus_clone = focus.clone();

    let (_, writers_cap) = seal(
        source,
        vec![TelemetryRecord::Operational(op)],
        seq + 2,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:02Z",
        "2020-01-01T12:00:03Z",
    )
    .await;
    let writers_ref = reference(&writers_cap, 0);

    let signal = DiagnosticSignal::PageWait {
        key: focus_clone.waiter.participant.original.key.clone(),
        statement_identity: Some(focus_clone.waiter.participant.statement_identity.clone()),
        resource: focus_clone.waiter.resource.clone(),
    };
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(act_cap.source()).unwrap(),
        signal,
        focus_clone.waiter.participant.original_observed_at.clone(),
        waiter_ref.clone(),
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::PageWaitWriters {
        observation: writers_ref,
        page: Box::new(page_ref),
        candidates: focus_clone.candidates.iter().map(|c| c.parent.clone()).collect(),
        blocking_contexts: vec![],
    });

    let mut rec_cap_opt = None;
    if resolved {
        let mut rec_source = fixture.activity_source;
        let SourceObservation::Activity(frame) = &mut rec_source.observation else { panic!() };
        frame.at = "2020-01-01T12:00:04Z".into();
        frame.observed_from = frame.at.clone();
        frame.observed_until = frame.at.clone();
        frame.native_rows = 0;
        frame.observations = 0;
        frame.coverage = StateSourceCoverage::observed(rec_source.source.clone(), 0, true);
        let (_, rec_cap) = seal(
            rec_source,
            vec![],
            seq + 3,
            EvidenceFamily::ActivityObservations,
            "2020-01-01T12:00:04Z",
            "2020-01-01T12:00:04.1Z",
        )
        .await;
        episode.lifecycle = DiagnosticLifecycle::Resolved;
        episode.recovery = Some(DiagnosticRecoveryProof::CompleteScope {
            capture: rec_cap.verified_original().unwrap().original().clone(),
        });
        rec_cap_opt = Some(rec_cap);
    }

    let diag_cap = seal_diagnostics(episode, seq + 4).await;
    let mut ctx: Vec<&EvidenceCapture> = vec![&act_cap, &page_cap, &writers_cap];
    if let Some(ref rec) = rec_cap_opt {
        ctx.push(rec);
    }
    let out = policy(false).synthesize_captures(&diag_cap, &ctx, RECORDED_AT, &display()).unwrap();
    out.validate().unwrap();
    out
}

async fn synthesize_focused_page(
    mutate_page: impl FnOnce(&mut PageWaitObservation),
    resolved: bool,
    seq: u64,
) -> DerivedDiagnosis {
    let fixture = page_writer_fixture(PageWriterCase::TargetWriter);
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::PageWait(base_page), .. }) =
        &fixture.page_rows[0]
    else {
        panic!()
    };
    let mut page = base_page.as_ref().clone();
    mutate_page(&mut page);
    let mut act_source = fixture.activity_source;
    let SourceObservation::Activity(frame) = &mut act_source.observation else { panic!() };
    frame.native_rows = 1;
    frame.observations = 1;
    frame.coverage = StateSourceCoverage::collected(act_source.source.clone(), 1);
    let original_at = frame.at.clone();
    let epoch = frame.epoch.clone().unwrap();
    let (_, act_cap) = seal(
        act_source.clone(),
        vec![TelemetryRecord::from(page.request.clone())],
        seq,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let waiter_ref = reference(&act_cap, 0);
    let focus = ActivityWaitFocus {
        participant: ActivityRequestFocus {
            parent: waiter_ref.clone(),
            target: act_source.target.clone(),
            partition: act_source.partition.clone(),
            database_writable: Some(true),
            source: act_source.source.clone(),
            epoch,
            original_observed_at: original_at.clone(),
            original: page.request.clone(),
            statement_identity: page
                .request
                .statement_identity
                .clone()
                .unwrap_or_else(|| NativeIdentity::new("native-sql-61").qualified("statement_start_offset", "0")),
        },
        resource: page.resource.clone(),
    };
    let snapshot = StateSnapshotDescription {
        dataset: SnapshotDataset::PageWaitTargets,
        observed_from: "2020-01-01T12:00:00.200000000Z".into(),
        observed_until: "2020-01-01T12:00:00.900000000Z".into(),
        records: 1,
        sources: vec![StateSourceCoverage::observed(
            page.source.clone(),
            1,
            page.resolution == PageWaitResolution::ObservedAtBothBounds,
        )],
    };
    let focused_source = CollectionDescription {
        target: act_source.target.clone(),
        partition: act_source.partition.clone(),
        source: page.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::FocusedPageWait {
            snapshot: snapshot.clone(),
            result: Box::new(ActivityWaitFocusResult {
                focus: focus.clone(),
                observed_from: snapshot.observed_from,
                observed_until: snapshot.observed_until,
                source: FocusSourceState::Matched,
                request: page.after_check.as_ref().map_or(PageWaitRequestState::SameStatement, |check| check.request),
                wait: page.after_check.as_ref().map_or(PageWaitRelation::OriginalObserved, |check| check.wait),
            }),
        },
    };
    let page_key = page.key().unwrap();
    let page_rec = TelemetryRecord::Operational(OperationalObservation {
        source: page.source.clone(),
        key: page_key,
        fact: OperationalFact::PageWait(Box::new(page)),
    });
    let (_, page_cap) = seal(
        focused_source,
        vec![page_rec],
        seq + 1,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00.2Z",
        "2020-01-01T12:00:00.9Z",
    )
    .await;
    let page_ref = reference(&page_cap, 0);

    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(act_cap.source()).unwrap(),
        DiagnosticSignal::PageWait {
            key: focus.participant.original.key.clone(),
            statement_identity: Some(focus.participant.statement_identity.clone()),
            resource: focus.resource.clone(),
        },
        original_at,
        waiter_ref.clone(),
    )
    .unwrap();
    let mut rec_cap_opt = None;
    if resolved {
        episode.analysis = DiagnosticAnalysisStatus::Deferred;
        episode.issue = Some(DiagnosticAnalysisIssue::EvidenceUnavailable);
        let mut rec_source = act_source;
        let SourceObservation::Activity(frame) = &mut rec_source.observation else { panic!() };
        frame.at = "2020-01-01T12:00:02Z".into();
        frame.observed_from = frame.at.clone();
        frame.observed_until = frame.at.clone();
        frame.native_rows = 0;
        frame.observations = 0;
        frame.coverage = StateSourceCoverage::observed(rec_source.source.clone(), 0, true);
        let (_, rec_cap) = seal(
            rec_source,
            vec![],
            seq + 2,
            EvidenceFamily::ActivityObservations,
            "2020-01-01T12:00:02Z",
            "2020-01-01T12:00:02.1Z",
        )
        .await;
        episode.lifecycle = DiagnosticLifecycle::Resolved;
        episode.recovery = Some(DiagnosticRecoveryProof::CompleteScope {
            capture: rec_cap.verified_original().unwrap().original().clone(),
        });
        rec_cap_opt = Some(rec_cap);
    } else {
        episode.analysis = DiagnosticAnalysisStatus::Analyzed;
        episode.report = Some(DiagnosticReport::PageWait { observation: page_ref, blocking_contexts: vec![] });
    }
    let diag_cap = seal_diagnostics(episode, seq + 3).await;
    let mut ctx: Vec<&EvidenceCapture> = vec![&act_cap, &page_cap];
    if let Some(ref rec) = rec_cap_opt {
        ctx.push(rec);
    }
    let out = policy(false).synthesize_captures(&diag_cap, &ctx, RECORDED_AT, &display()).unwrap();
    out.validate().unwrap();
    out
}

async fn synthesize_holders(case: PageTaskHolderCase, seq: u64) -> DerivedDiagnosis {
    let mut fixture = page_task_holder_fixture(case);
    let (_, act_cap) = seal(
        fixture.activity_source,
        fixture.activity_rows,
        seq,
        EvidenceFamily::ActivityObservations,
        "2020-01-01T12:00:00Z",
        "2020-01-01T12:00:00.1Z",
    )
    .await;
    let (_, page_cap) = seal(
        fixture.page_source,
        fixture.page_rows,
        seq + 1,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:00.25Z",
        "2020-01-01T12:00:00.75Z",
    )
    .await;
    let parent_ref = reference(&act_cap, 0);
    let page_ref = reference(&page_cap, 0);

    let mut source = fixture.source;
    let TelemetryRecord::Operational(mut op) = fixture.records.remove(0) else { panic!() };
    let OperationalFact::PageTaskHolders(mut row) = op.fact else { panic!() };
    let focus_clone = {
        let SourceObservation::PageTaskHolders { focus, .. } = &mut source.observation else { panic!() };
        focus.participant.parent = parent_ref.clone();
        focus.page = page_ref.clone();
        row.focus_key = focus.key().unwrap();
        focus.clone()
    };
    op.key = row.key().unwrap();
    op.fact = OperationalFact::PageTaskHolders(row);

    let (_, holders_cap) = seal(
        source,
        vec![TelemetryRecord::Operational(op)],
        seq + 2,
        EvidenceFamily::OperationalSamples,
        "2020-01-01T12:00:02Z",
        "2020-01-01T12:00:03.9Z",
    )
    .await;
    let holders_ref = reference(&holders_cap, 0);

    let signal = DiagnosticSignal::PageTaskWait {
        key: focus_clone.participant.original.key.clone(),
        statement_identity: focus_clone.participant.statement_identity.clone(),
        parent: Box::new(parent_ref.clone()),
        page: Box::new(page_ref.clone()),
        task: Box::new(focus_clone.task.clone()),
        database_writable: focus_clone.participant.database_writable.unwrap(),
    };
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(act_cap.source()).unwrap(),
        signal,
        focus_clone.task.observed_at.clone(),
        page_ref.clone(),
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::PageTaskHolders {
        observation: holders_ref,
        page: Box::new(page_ref),
        parent: Box::new(parent_ref.clone()),
    });
    let diag_cap = seal_diagnostics(episode, seq + 3).await;
    let out = policy(false)
        .synthesize_captures(&diag_cap, &[&act_cap, &page_cap, &holders_cap], RECORDED_AT, &display())
        .unwrap();
    out.validate().unwrap();
    out
}

#[tokio::test]
async fn sequential_leaf_inserts_multiple_writers_and_non_rightmost_or_non_leaf_targets_discriminate_cleanly_in_gold() {
    // 1. Sequential rightmost-leaf INSERT contention identifies index, end-of-leaf 0:0, and sequential-key candidate advice
    // while keeping the unrelated SELECT read-only and never promoting writers to culprits.
    let hot_out = synthesize_writers(PageWriterCase::EndOfLeafInsert, |_, _| {}, false, 9_100_000).await;
    let [hot_inc] = hot_out.incidents.as_slice() else { panic!() };
    assert!(hot_inc.culprit_queries.is_empty());
    assert_eq!(hot_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(hot_inc.affected_queries.len(), 1);
    assert_eq!(hot_inc.candidate_queries.len(), 2);
    let hot_mech = hot_inc.mechanism.as_deref().unwrap();
    assert!(hot_mech.contains(&t!(
        "view.page_waits.target.index",
        schema = "dbo",
        object = "orders",
        index = "orders_id"
    )));
    assert!(hot_mech.contains(&t!("view.page_writers.insert_hotspot.candidate")));
    assert!(hot_mech.contains(&t!("view.page_writers.insert_hotspot.identity")));
    assert!(hot_mech.contains(&t!("view.page_writers.relation.read_only")));
    assert!(hot_inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
    assert!(hot_inc.verification.contains(&t!("view.page_writers.insert_hotspot.verify")));

    // 2. Non-rightmost leaf (next_page_id != 0) suppresses end-of-leaf / OPTIMIZE_FOR_SEQUENTIAL_KEY advice.
    let mid_out = synthesize_writers(
        PageWriterCase::EndOfLeafInsert,
        |_, row| {
            let header = &mut row.page.as_mut().unwrap().target.as_mut().unwrap().header;
            header.next_file_id = Some(1);
            header.next_page_id = Some(55);
        },
        false,
        9_100_010,
    )
    .await;
    let [mid_inc] = mid_out.incidents.as_slice() else { panic!() };
    assert!(!mid_inc.mechanism.as_deref().unwrap().contains(&t!("view.page_writers.insert_hotspot.candidate")));
    assert!(!mid_inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
    assert!(mid_inc.developer_action.contains(&t!("view.page_writers.investigate.target")));

    // 3. Read-only concurrent SELECT and other-table INSERT never receive target-writer or sequential-key blame.
    for case in [PageWriterCase::ReadOnly, PageWriterCase::OtherTarget] {
        let out = synthesize_writers(case, |_, _| {}, false, 9_100_020 + (case as u64) * 10).await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty());
        assert!(!inc.developer_action.contains(&t!("view.page_writers.investigate.target")));
        assert!(!inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
        assert!(inc.developer_action.contains(&t!("view.page_writers.investigate.other")));
    }

    // 3b. Two distinct INSERT patterns plus an unrelated expensive SELECT keep both writers as TargetObjectWrite candidates,
    // keep the SELECT read-only, and never promote either writer to an exclusive culprit.
    {
        let mut fixture = page_writer_fixture(PageWriterCase::EndOfLeafInsert);
        let second_sql = "INSERT INTO dbo.orders(customer_id, source_channel) VALUES (9264, N'batch')";
        let TelemetryRecord::Activity(base_writer) = &fixture.activity_rows[1] else { panic!() };
        let mut second_writer = base_writer.as_ref().clone();
        second_writer.key.session =
            NativeActivityIdentity { id: "74".into(), generation: "original-writer-login-2".into() };
        second_writer.key.request =
            Some(NativeActivityIdentity { id: "0".into(), generation: "original-writer-request-second".into() });
        second_writer.statement = DefinitionPayload::sql(second_sql, Engine::Mssql);
        second_writer.statement_identity = Some(
            NativeIdentity::new("original-native-writer-statement-second")
                .qualified("native_object", "sys.dm_exec_requests.sql_handle")
                .qualified("statement_start_offset", "0")
                .qualified("statement_end_offset", "-1"),
        );
        fixture.activity_rows.push(TelemetryRecord::from(second_writer.clone()));
        {
            let SourceObservation::Activity(frame) = &mut fixture.activity_source.observation else { panic!() };
            frame.native_rows = fixture.activity_rows.len() as u32;
            frame.observations = fixture.activity_rows.len() as u32;
            frame.coverage = StateSourceCoverage::collected(
                fixture.activity_source.source.clone(),
                fixture.activity_rows.len() as u32,
            );
        }
        let (_, act_cap) = seal(
            fixture.activity_source.clone(),
            fixture.activity_rows,
            9_100_060,
            EvidenceFamily::ActivityObservations,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:00.1Z",
        )
        .await;
        let (_, page_cap) = seal(
            fixture.page_source,
            fixture.page_rows,
            9_100_061,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:00.2Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let waiter_ref = reference(&act_cap, 0);
        let page_ref = reference(&page_cap, 0);
        let cand_refs = (1..act_cap.records().len()).map(|idx| reference(&act_cap, idx)).collect::<Vec<_>>();
        let mut source = fixture.source;
        let TelemetryRecord::Operational(mut op) = fixture.records.remove(0) else { panic!() };
        let OperationalFact::PageWaitWriters(mut row) = op.fact else { panic!() };
        let second_plan_xml = format!(
            r#"<ShowPlanXML Version="1.539" xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements>
            <StmtSimple StatementId="1" StatementText="{second_sql}" StatementType="INSERT" StatementSubTreeCost="0.1"><QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="128">
            <RelOp NodeId="1" PhysicalOp="Clustered Index Insert" LogicalOp="Insert" EstimateRows="1" EstimateIO="0.01" EstimateCPU="0.001" EstimatedTotalSubtreeCost="0.1" Parallel="false">
            <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[orders]" Column="customer_id" /></OutputList>
            <Update DMLRequestSort="false"><Object Database="[db]" Schema="[dbo]" Table="[orders]" Index="[orders_id]" IndexKind="Clustered" />
            <SetPredicate><ScalarOperator ScalarString="[db].[dbo].[orders].[customer_id]=(9264)"><ScalarExpressionList><ScalarOperator><MultipleAssign><Assign><ColumnReference Database="[db]" Schema="[dbo]" Table="[orders]" Column="customer_id" /><ScalarOperator><Const ConstValue="(9264)" /></ScalarOperator></Assign></MultipleAssign></ScalarOperator></ScalarExpressionList></ScalarOperator></SetPredicate>
            <RelOp NodeId="2" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="1" EstimateCPU="0.001" EstimatedTotalSubtreeCost="0.001"><OutputList /><ConstantScan><Values><Row><ScalarOperator ScalarString="(9264)"><Const ConstValue="(9264)" /></ScalarOperator></Row></Values></ConstantScan></RelOp></Update></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
        );
        {
            let SourceObservation::PageWaitWriters { focus, .. } = &mut source.observation else { panic!() };
            focus.waiter.participant.parent = waiter_ref.clone();
            focus.page = page_ref.clone();
            focus.candidates.push(PageWaitWriterRequest {
                parent: cand_refs[2].clone(),
                key: second_writer.key.clone(),
                statement_identity: second_writer.statement_identity.clone().unwrap(),
            });
            let mut second_cand = PageWaitWriterCandidate {
                parent: cand_refs[2].clone(),
                request_state: PageWaitRequestState::SameStatement,
                request: Some(second_writer.clone()),
                plan_identity: Some(
                    NativeIdentity::new("AABBCC02").qualified("native_object", "sys.dm_exec_requests.plan_handle"),
                ),
                plan: DefinitionPayload::plan(&second_plan_xml, Engine::Mssql),
                relation: PageWriterRelation::TargetObjectWrite,
                binding: None,
            };
            second_cand.binding = Some(
                MssqlPageWritePlanProof::new(
                    &focus.target,
                    &second_cand,
                    digest(second_plan_xml.as_bytes()),
                    digest(second_sql.as_bytes()),
                    vec![MssqlPageWriteAccess { node_id: 1, operation: PageWriteOperation::Insert }],
                )
                .unwrap(),
            );
            row.candidates.push(second_cand);
            for ((focus_cand, row_cand), new_ref) in
                focus.candidates.iter_mut().zip(row.candidates.iter_mut()).zip(&cand_refs)
            {
                focus_cand.parent = new_ref.clone();
                row_cand.parent = new_ref.clone();
                if let Some(old_proof) = row_cand.binding.take() {
                    row_cand.binding = Some(
                        MssqlPageWritePlanProof::new(
                            &focus.target,
                            row_cand,
                            old_proof.native_plan_sha256,
                            old_proof.native_statement_sha256,
                            old_proof.accesses,
                        )
                        .unwrap(),
                    );
                }
            }
            row.focus_key = focus.key().unwrap();
        }
        let SourceObservation::PageWaitWriters { focus, .. } = &source.observation else { panic!() };
        let focus_clone = focus.clone();
        op.key = row.key().unwrap();
        op.fact = OperationalFact::PageWaitWriters(row);
        let (_, writers_cap) = seal(
            source,
            vec![TelemetryRecord::Operational(op)],
            9_100_062,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:02Z",
            "2020-01-01T12:00:03Z",
        )
        .await;
        let writers_ref = reference(&writers_cap, 0);
        let mut ep = DiagnosticEpisode::start(
            DiagnosticScope::from_source(act_cap.source()).unwrap(),
            DiagnosticSignal::PageWait {
                key: focus_clone.waiter.participant.original.key.clone(),
                statement_identity: Some(focus_clone.waiter.participant.statement_identity.clone()),
                resource: focus_clone.waiter.resource.clone(),
            },
            focus_clone.waiter.participant.original_observed_at.clone(),
            waiter_ref,
        )
        .unwrap();
        ep.analysis = DiagnosticAnalysisStatus::Analyzed;
        ep.report = Some(DiagnosticReport::PageWaitWriters {
            observation: writers_ref,
            page: Box::new(page_ref),
            candidates: cand_refs,
            blocking_contexts: vec![],
        });
        let diag_cap = seal_diagnostics(ep, 9_100_063).await;
        let out = policy(false)
            .synthesize_captures(&diag_cap, &[&act_cap, &page_cap, &writers_cap], RECORDED_AT, &display())
            .unwrap();
        out.validate().unwrap();
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty());
        assert_eq!(inc.candidate_queries.len(), 3);
        assert!(inc.candidate_queries.iter().all(|q| q.causal_role.is_none()));
        let insert_candidates = inc
            .candidate_queries
            .iter()
            .filter(|q| matches!(&q.sql, DefinitionPayload::Available { text, .. } if text.starts_with("INSERT")))
            .count();
        let select_candidates = inc
            .candidate_queries
            .iter()
            .filter(|q| matches!(&q.sql, DefinitionPayload::Available { text, .. } if text.starts_with("SELECT")))
            .count();
        assert_eq!(insert_candidates, 2);
        assert_eq!(select_candidates, 1);
        assert!(inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
        assert!(!inc.developer_action.contains(&t!("view.page_writers.investigate.other")));
    }

    // 4. IndexBranch, HeapData, and Unclassified metadata pages avoid sequential-key / last-leaf fixes.
    for (idx, (kind, page_type, level, index_id, expected_action)) in [
        (PageWaitTargetKind::IndexBranch, 2u32, Some(1u32), Some("1"), key!("view.page_waits.investigation.branch")),
        (PageWaitTargetKind::HeapData, 1u32, Some(0u32), Some("0"), key!("view.page_waits.investigation.heap")),
        (PageWaitTargetKind::Unclassified, 99u32, None, None, key!("view.page_waits.investigation.unclassified")),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_focused_page(
            |page| {
                let target = page.target.as_mut().unwrap();
                target.kind = kind;
                target.header.page_type = Some(page_type);
                target.header.level = level;
                target.header.index_id = index_id.map(str::to_owned);
                if kind != PageWaitTargetKind::IndexBranch {
                    target.object = None;
                }
            },
            false,
            9_100_100 + (idx as u64) * 10,
        )
        .await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.developer_action.contains(&culprit_core::i18n::t(expected_action, &[])));
        assert!(!inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
    }
}

#[tokio::test]
async fn allocation_pages_physical_io_shared_tempdb_and_reused_or_unmapped_targets_preserve_exact_boundaries() {
    // 1. Allocation pages (PFS, GAM, SGAM, IAM) identify exact allocation structure + allocation verification, never last-leaf fixes.
    for (idx, (kind, page_type, inv_key)) in [
        (PageWaitTargetKind::Pfs, 11u32, key!("view.page_waits.investigation.pfs")),
        (PageWaitTargetKind::Gam, 8u32, key!("view.page_waits.investigation.gam")),
        (PageWaitTargetKind::Sgam, 9u32, key!("view.page_waits.investigation.sgam")),
        (PageWaitTargetKind::Iam, 10u32, key!("view.page_waits.investigation.iam")),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_focused_page(
            |page| {
                let target = page.target.as_mut().unwrap();
                target.kind = kind;
                target.header.page_type = Some(page_type);
                target.header.object_id = None;
                target.header.index_id = None;
                target.header.partition_id = None;
                target.object = None;
            },
            false,
            9_100_200 + (idx as u64) * 10,
        )
        .await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.developer_action.contains(&culprit_core::i18n::t(inv_key, &[])));
        assert!(inc.verification.contains(&t!("view.page_waits.verify.allocation")));
        assert!(!inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
    }

    // 2. Physical I/O wait (PAGEIOLATCH_SH / WaitResourceKind::PageIo) classifies as storage I/O, never in-memory PAGELATCH.
    let io_out = synthesize_focused_page(
        |page| {
            page.resource.kind = WaitResourceKind::PageIo;
            let wait = page.request.wait.as_mut().unwrap();
            wait.native_type = Some("PAGEIOLATCH_SH".into());
            wait.identified_resources = vec![page.resource.clone()];
        },
        false,
        9_100_260,
    )
    .await;
    let [io_inc] = io_out.incidents.as_slice() else { panic!() };
    assert!(io_inc.mechanism.as_deref().unwrap().contains(&t!("view.page_waits.io.explanation")));
    assert!(!io_inc.mechanism.as_deref().unwrap().contains(&t!("view.page_waits.memory.explanation")));
    assert!(io_inc.developer_action.contains(&t!("view.page_waits.investigation.io")));

    // 3. Page/index/request-ID reuse and missing permissions / unmapped pages reject stale attribution and suppress unsupported fixes.
    for (idx, case) in [
        PageWriterCase::ChangedTarget,
        PageWriterCase::ChangedRequest,
        PageWriterCase::MissingRequest,
        PageWriterCase::PermissionUnavailable,
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_writers(case, |_, _| {}, false, 9_100_300 + (idx as u64) * 10).await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.candidate_queries.is_empty() && inc.culprit_queries.is_empty(), "{case:?}");
        assert!(!inc.developer_action.contains(&t!("view.page_writers.insert_hotspot.investigate")));
    }

    for (idx, resolution) in [
        PageWaitResolution::ChangedMetadata,
        PageWaitResolution::PermissionDenied,
        PageWaitResolution::Unavailable,
        PageWaitResolution::UnrecognizedAddress,
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_focused_page(
            |page| {
                page.resolution = resolution;
                page.target = None;
                if resolution == PageWaitResolution::UnrecognizedAddress {
                    page.observed_address = None;
                }
            },
            false,
            9_100_360 + (idx as u64) * 10,
        )
        .await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert_eq!(inc.affected_queries.len(), 1);
        assert!(inc.culprit_queries.is_empty());
        assert!(inc.developer_action.contains(&t!("view.page_waits.investigation.unresolved")));
    }

    // 4. Shared tempdb PFS/GAM/SGAM keeps the waiting request in its application database without relabeling tempdb or blaming peers.
    let shared_cases = shared_page_wait_evidence_cases();
    for (idx, (source, rows)) in shared_cases.into_iter().enumerate().filter(|(_, (_, rows))| !rows.is_empty()) {
        let (_, cap) = seal(
            source,
            rows,
            9_100_420 + (idx as u64) * 5,
            EvidenceFamily::OperationalSamples,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let analyzed = crate::collected_resources::page_waits(&cap, None).unwrap();
        assert_eq!(analyzed.report.entries.len(), 1);
        assert!(analyzed.report.entries[0].verify.contains(&t!("view.page_waits.verify.allocation")));
        let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::PageWait(obs), .. }) =
            &cap.records()[0].row().record
        else {
            panic!()
        };
        assert_eq!(obs.target.as_ref().unwrap().header.database_id, 2);
        assert_ne!(obs.request.database.as_deref(), Some("tempdb"));
    }
}

#[tokio::test]
async fn direct_task_holders_resolved_episodes_and_non_mssql_provider_waits_never_manufacture_unsupported_owners() {
    // 1. Direct task holder at both bounds establishes NativeReportedTaskHolder; TaskOnly, Sentinel, ChangedPage, ChangedRequest do not.
    let both_out = synthesize_holders(PageTaskHolderCase::QualifiedBoth, 9_100_500).await;
    let [both_inc] = both_out.incidents.as_slice() else { panic!() };
    assert_eq!(both_inc.culprit_queries.len(), 1);
    assert_eq!(both_inc.candidate_queries.len(), 1);
    assert_eq!(both_inc.attribution_status, DerivedAttributionStatus::Established);
    assert_eq!(
        both_inc.culprit_queries[0].causal_role.as_ref().unwrap().kind,
        DerivedCausalRoleKind::NativeReportedTaskHolder
    );
    assert!(both_inc.developer_action.contains(&t!("view.page_holders.investigate.request")));
    assert!(both_inc.verification.contains(&t!("view.page_holders.verify")));

    for (idx, (case, expect_empty_candidates)) in [
        (PageTaskHolderCase::TaskOnly, true),
        (PageTaskHolderCase::Sentinel, true),
        (PageTaskHolderCase::ChangedPage, false),
        (PageTaskHolderCase::ChangedRequest, false),
        (PageTaskHolderCase::PermissionUnavailable, true),
    ]
    .into_iter()
    .enumerate()
    {
        let out = synthesize_holders(case, 9_100_520 + (idx as u64) * 10).await;
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty(), "{case:?}");
        assert_ne!(inc.attribution_status, DerivedAttributionStatus::Established, "{case:?}");
        if expect_empty_candidates {
            assert!(inc.candidate_queries.is_empty(), "{case:?}");
        }
    }

    // 2. Contention resolving before enrichment preserves original wait and marks lifecycle Resolved without stale attribution.
    let res_pending = synthesize_focused_page(|_| {}, true, 9_100_600).await;
    let [res_inc] = res_pending.incidents.as_slice() else { panic!() };
    assert_eq!(res_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(res_inc.affected_queries.len(), 1);
    assert!(res_inc.culprit_queries.is_empty());

    let res_writers = synthesize_writers(PageWriterCase::EndOfLeafInsert, |_, _| {}, true, 9_100_620).await;
    let [res_w_inc] = res_writers.incidents.as_slice() else { panic!() };
    assert_eq!(res_w_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(res_w_inc.candidate_queries.len(), 2);
    assert!(res_w_inc.culprit_queries.is_empty());

    // 3. PostgreSQL LWLock/BufferPin, MySQL wait/synch/mutex, and Yugabyte DocDB waits preserve native wait without inventing SQL Server hot-page owners.
    for (idx, (engine, wait_type)) in [
        (Engine::Postgres, "LWLock:BufferContent"),
        (Engine::Mysql, "wait/synch/sxlock/innodb/btr_search_latch"),
        (Engine::Yugabyte, "DocDB:RocksDBBlockCache"),
    ]
    .into_iter()
    .enumerate()
    {
        let (mut act_source, _) = test_support::activity_evidence_cases().remove(0);
        act_source.target = DatabaseIdentity::server(engine, "db", "fixture.invalid", 5432, None).into();
        act_source.partition = SourcePartition::Target;
        let SourceObservation::Activity(frame) = &mut act_source.observation else { panic!() };
        frame.epoch = None;
        frame.database_writable = None;
        frame.native_rows = 1;
        frame.observations = 1;
        frame.coverage = StateSourceCoverage::observed(act_source.source.clone(), 1, false);
        let mut obs =
            test_support::activity_observation(DefinitionPayload::sql("SELECT id FROM orders WHERE id = 9264", engine));
        obs.statement_identity = None;
        obs.session_counters = ActivityCounters::default();
        obs.request_counters = Some(ActivityCounters::default());
        obs.database = Some("db".into());
        obs.elapsed_ms = Some("65000".into());
        obs.wait = Some(ActivityWait {
            class: None,
            native_type: Some(wait_type.into()),
            elapsed_ms: Some("65000".into()),
            owners: vec![],
            resources: None,
            identified_resources: vec![],
        });
        obs.waiting_on_lock = Some(false);
        let obs_key = obs.key.clone();
        let at = frame.at.clone();
        let (_, cap) = seal(
            act_source,
            vec![TelemetryRecord::from(obs)],
            9_100_700 + (idx as u64) * 10,
            EvidenceFamily::ActivityObservations,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let basis = reference(&cap, 0);
        let ep = DiagnosticEpisode::start(
            DiagnosticScope::from_source(cap.source()).unwrap(),
            DiagnosticSignal::Activity {
                key: obs_key,
                condition: ActivityCondition::LongRunning,
                policy: ActivityPolicy::default(),
            },
            at,
            basis.clone(),
        )
        .unwrap();
        let diag = seal_diagnostics(ep, 9_100_705 + (idx as u64) * 10).await;
        let out = policy(false).synthesize_captures(&diag, &[&cap], RECORDED_AT, &display()).unwrap();
        out.validate().unwrap();
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert_eq!(inc.affected_queries.len(), 1);
        assert!(inc.culprit_queries.is_empty() && inc.candidate_queries.is_empty(), "{engine:?}");
    }
}
