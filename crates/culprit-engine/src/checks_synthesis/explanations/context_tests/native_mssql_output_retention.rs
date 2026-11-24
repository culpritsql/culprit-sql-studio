//! Owned SQL Server 2022 `ASYNC_NETWORK_IO` native acceptance:
//! 1. Single-request sort stream paused after row 1 holds a positive execution memory grant AND a shared table lock,
//!    blocking a collateral DDL waiter on `LCK_M_SCH_M`.
//! 2. Real `Activity` -> `SourceRead(OutputGrantRetention)` (`MemorySource`) + `FocusedBlocking` captures.
//! 3. Draining the stream unblocks the waiter -> ended `OutputGrantRetention` + ended `FocusedBlocking` + post-drain `Activity`.
//! 4. Canonical reopen after actor retirement -> Gold `DerivedDiagnosis` (active + resolved) for both mechanisms,
//!    without inferring HTTP calls, application loops, sustained duration, or network faults.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisCapture, SynthesisInput, SynthesisRequest},
};
use culprit_core::{blocking::OutputBlockingStatus, limits::OperationGuard};
use culprit_mssql::native_test_support::{self, OUTPUT_STREAM_ROWS, OutputRetentionFixture, SourceEndpoint};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;

struct Factory(SourceEndpoint);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.open(request)
    }
}

#[tokio::test]
#[ignore = "requires nonce-owned loopback SQL Server with CULPRIT_MSSQL_SOURCE_TEST_PORT/PASSWORD and explicit CULPRIT_NATIVE_COST_RECEIPT_DIR"]
async fn owned_mssql_output_wait_retains_grant_and_lock_then_drains_and_resolves_through_canonical_gold() {
    native_test_support::run_output_retention_case(|mut fixture, cancellation| {
        Box::pin(async move { Box::pin(prove(&mut fixture, &cancellation)).await })
    })
    .await;
}

async fn prove(fixture: &mut OutputRetentionFixture<'_>, cancellation: &CollectionCancellation) -> Result<()> {
    let endpoint = fixture.endpoint()?;
    let mut captures = OwnedCaptures::new()?;
    eprintln!("owned mssql output retention phase: start actors");
    let (output_id, waiter_id) = Box::pin(fixture.start(cancellation)).await?;

    eprintln!("owned mssql output retention phase: capture basis Activity");
    let basis = Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;
    let policy = ActivityPolicy { blocked_after_ms: 1000, running_after_ms: 1000 };
    eprintln!("owned mssql output retention phase: select episodes");
    let (output_ep, output_focus, output_exec, lock_ep, lock_focus) =
        select_episodes(&basis, &output_id, &waiter_id, &policy)?;

    eprintln!("owned mssql output retention phase: capture OutputGrantRetention");
    let grant_sel =
        CollectionSelection::SourceRead { focus: Box::new(output_focus.clone()), execution: Box::new(output_exec) };
    let grant_cap = Box::pin(capture(&mut captures, &endpoint, grant_sel.clone(), cancellation)).await?;

    eprintln!("owned mssql output retention phase: capture FocusedBlocking");
    let block_sel = CollectionSelection::FocusedBlocking { focus: Box::new(lock_focus) };
    let block_cap = Box::pin(capture(&mut captures, &endpoint, block_sel.clone(), cancellation)).await?;

    // Intervention: drain the paused ASYNC_NETWORK_IO stream and verify the collateral DDL waiter completes.
    eprintln!("owned mssql output retention phase: drain stream & finish collateral waiter");
    let drained_rows = Box::pin(fixture.drain_and_finish(cancellation)).await?;
    assert_eq!(drained_rows, OUTPUT_STREAM_ROWS);

    eprintln!("owned mssql output retention phase: capture post-drain ended controls");
    let grant_ended = Box::pin(capture(&mut captures, &endpoint, grant_sel, cancellation)).await?;
    let block_ended = Box::pin(capture(&mut captures, &endpoint, block_sel, cancellation)).await?;
    let recovered = Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;

    eprintln!("owned mssql output retention phase: verify retained & reopen");
    for cap in [&basis, &grant_cap, &block_cap, &grant_ended, &block_ended, &recovered] {
        Box::pin(captures.verify_retained(cap)).await?;
    }

    let reopened =
        Box::pin(reopen_all(&captures, [&basis, &grant_cap, &block_cap, &grant_ended, &block_ended, &recovered]))
            .await?;
    let reopened_refs = reopened.captures().collect::<Vec<_>>();
    let [r_basis, r_grant, r_block, r_grant_ended, r_block_ended, r_recovered] = reopened_refs.as_slice() else {
        panic!()
    };

    eprintln!("owned mssql output retention phase: evaluate OutputGrantRetention active + ended + resolved Gold");
    // 1. Verify OutputGrantRetention positive, ended, and resolved Gold.
    let grant_answer = build_grant_answer(r_basis, r_grant, &output_focus, &output_id)?;
    let grant_read = r_grant.verified_original()?.operational_read()?;
    let grant_report = culprit_checks::output_grant_retention::explain(&grant_answer, &grant_read, None)?;
    assert_eq!(grant_report.status, OutputGrantRetentionStatus::Observed);

    let grant_ended_answer = build_grant_answer(r_basis, r_grant_ended, &output_focus, &output_id)?;
    let grant_ended_read = r_grant_ended.verified_original()?.operational_read()?;
    let grant_ended_report =
        culprit_checks::output_grant_retention::explain(&grant_ended_answer, &grant_ended_read, None)?;
    assert_ne!(grant_ended_report.status, OutputGrantRetentionStatus::Observed);

    let answered_output_ep = output_ep
        .enroll_source_read(output_focus.clone())?
        .transition_source_read(&output_focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &output_focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::OutputGrantRetention(Box::new(grant_answer))),
            },
        )?;
    let grant_hist = history(answered_output_ep.clone(), 996_100).await;
    let grant_gold = synthesize(&reopened, &[&grant_hist], &[r_basis, r_grant])?;
    grant_gold.validate()?;
    let [grant_inc] = grant_gold.incidents.as_slice() else { panic!() };
    assert_eq!(grant_inc.affected_queries.len(), 1);
    assert!(grant_inc.culprit_queries.is_empty());
    assert!(grant_inc.mechanism.is_some());

    let resolved_output_rev = resolve_episode(r_recovered, &answered_output_ep, &policy)?;
    let grant_res_hist = history_revision(resolved_output_rev, 996_101).await;
    let grant_res_gold = synthesize(&reopened, &[&grant_res_hist, &grant_hist], &[r_basis, r_grant, r_recovered])?;
    grant_res_gold.validate()?;
    assert!(grant_res_gold.incidents.iter().any(|i| i.entry.lifecycle == DiagnosticLifecycle::Resolved));

    // 2. Verify OutputBlocking positive, ended, and resolved Gold.
    let block_orig = r_block.verified_original()?;
    let block_refs = (0..block_orig.records().len()).map(|i| block_orig.reference(i)).collect::<Result<Vec<_>>>()?;
    let block_frame_rows = block_refs
        .iter()
        .zip(block_orig.records())
        .map(|(r, row)| {
            let TelemetryRecord::Activity(obs) = &row.row().record else { panic!() };
            ActivityFrameRow { observation: obs, reference: Some(r) }
        })
        .collect::<Vec<_>>();
    let block_report = culprit_checks::focused_blocking::explain(block_orig.source(), &block_frame_rows, true)?;
    assert_eq!(block_report.output_retention.evidence.status, OutputBlockingStatus::Observed);

    let block_diag_rows = block_refs
        .iter()
        .zip(block_orig.records())
        .map(|(r, row)| {
            let TelemetryRecord::Activity(obs) = &row.row().record else { panic!() };
            DiagnosticActivityRow { reference: r, observation: obs }
        })
        .collect::<Vec<_>>();
    let block_ckpt = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: r_basis.verified_original()?.source().target.clone(),
        input: r_basis.verified_original()?.original().clone(),
        episodes: vec![lock_ep],
    };
    let answered_lock_rev = analyze_diagnostic_blocking(
        block_orig.source(),
        block_orig.original(),
        &block_diag_rows,
        &block_ckpt,
        &chrono::Utc::now().to_rfc3339(),
        None,
    )?
    .revisions
    .into_iter()
    .find(|r| matches!(r.episode.signal, DiagnosticSignal::LockWait { .. }))
    .unwrap();
    drop(block_orig);

    let ended_block_orig = r_block_ended.verified_original()?;
    let SourceObservation::FocusedBlocking { result: ended_block_res, .. } = &ended_block_orig.source().observation
    else {
        panic!()
    };
    assert_eq!(ended_block_res.membership, FocusedRequestMembership::NotObserved);

    let lock_hist = history_revision(answered_lock_rev.clone(), 996_110).await;
    let lock_gold = synthesize(&reopened, &[&lock_hist], &[r_basis, r_block])?;
    lock_gold.validate()?;
    let [lock_inc] = lock_gold.incidents.as_slice() else { panic!() };
    assert_eq!(lock_inc.affected_queries.len(), 1);
    assert!(!lock_inc.culprit_queries.is_empty());
    assert!(lock_inc.culprit_queries.iter().all(|q| !q.sql.is_available()));

    let resolved_lock_rev = resolve_episode(r_recovered, &answered_lock_rev.episode, &policy)?;
    let lock_res_hist = history_revision(resolved_lock_rev, 996_111).await;
    let lock_res_gold = synthesize(&reopened, &[&lock_res_hist, &lock_hist], &[r_basis, r_block, r_recovered])?;
    lock_res_gold.validate()?;
    assert!(lock_res_gold.incidents.iter().any(|i| i.entry.lifecycle == DiagnosticLifecycle::Resolved));

    native_test_support::save_output_retention_receipt(&json!({
        "drained_rows": drained_rows,
        "source_queries": [
            basis.manifest.source_queries,
            grant_cap.manifest.source_queries,
            block_cap.manifest.source_queries,
            grant_ended.manifest.source_queries,
            block_ended.manifest.source_queries,
            recovered.manifest.source_queries,
        ],
        "grant_gold_attribution": grant_inc.attribution_status,
        "blocking_gold_attribution": lock_inc.attribution_status,
        "http_or_network_fault_inferred": false,
        "native_cost_qualified": false,
    }))?;
    Ok(())
}

fn synthesize(
    input: &SynthesisInput,
    histories: &[&EvidenceCapture],
    captures: &[&SynthesisCapture],
) -> Result<DerivedDiagnosis> {
    let hist_origs = histories.iter().map(|h| h.verified_original()).collect::<Result<Vec<_>>>()?;
    let cap_origs = captures.iter().map(|c| c.verified_original()).collect::<Result<Vec<_>>>()?;
    let mut originals: Vec<&dyn OriginalProjection> = Vec::new();
    originals.extend(hist_origs.iter().map(|o| o as &dyn OriginalProjection));
    originals.extend(cap_origs.iter().map(|o| o as &dyn OriginalProjection));
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-mssql-output-retention-1")?
        .analyze_originals(&originals, input.recorded_at(), input.display(), vec![])
}

async fn capture(
    captures: &mut OwnedCaptures,
    endpoint: &SourceEndpoint,
    selection: CollectionSelection,
    cancellation: &CollectionCancellation,
) -> Result<Box<Captured>> {
    let endpoint = endpoint.clone();
    let request = endpoint.request(selection)?;
    let cap = Box::new(
        Box::pin(captures.capture("owned-mssql-output-retention", request, cancellation, move |_| Factory(endpoint)))
            .await?,
    );
    assert_eq!(cap.manifest.status, CollectionStatus::Complete);
    let metrics = cap.manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    Ok(cap)
}

async fn reopen_all(captures: &OwnedCaptures, originals: [&Captured; 6]) -> Result<SynthesisInput> {
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout)?;
    let request = SynthesisRequest {
        primary: originals[0].capture_ref()?,
        context: originals[1..].iter().map(|c| c.capture_ref()).collect::<Result<Vec<_>>>()?,
        budget,
    };
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await
}

fn select_episodes(
    basis: &Captured,
    output_id: &NativeActivityIdentity,
    waiter_id: &NativeActivityIdentity,
    policy: &ActivityPolicy,
) -> Result<(DiagnosticEpisode, SourceReadFocus, SourceReadExecutionScope, DiagnosticEpisode, ActivityRequestFocus)> {
    let source = basis.source()?;
    let refs = (0..basis.records.len()).map(|i| basis.reference(i)).collect::<Result<Vec<_>>>()?;
    let diag_rows = basis
        .records
        .iter()
        .zip(&refs)
        .map(|(row, r)| {
            let TelemetryRecord::Activity(obs) = &row.record else { panic!() };
            DiagnosticActivityRow { reference: r, observation: obs }
        })
        .collect::<Vec<_>>();
    let update = observe_diagnostic_activity(
        source,
        &basis.capture_ref()?,
        &diag_rows,
        None,
        policy,
        &chrono::Utc::now().to_rfc3339(),
    )?;
    let output_pos = basis
        .records
        .iter()
        .position(|r| matches!(&r.record, TelemetryRecord::Activity(obs) if obs.key.session == *output_id))
        .unwrap();
    let waiter_pos = basis
        .records
        .iter()
        .position(|r| matches!(&r.record, TelemetryRecord::Activity(obs) if obs.key.session == *waiter_id))
        .unwrap();
    let output_ep = update
        .checkpoint
        .episodes
        .iter()
        .find(|ep| matches!(&ep.signal, DiagnosticSignal::ClientResultOutput { key, .. } if key.session == *output_id))
        .cloned()
        .unwrap();
    let output_focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::OutputGrantRetention,
        &output_ep,
        source,
        &refs[output_pos],
        &basis.records[output_pos],
    )?
    .unwrap();
    let output_exec =
        SourceReadExecutionScope::from_context(output_focus.question, &output_focus.original_context(source)?)?;

    let lock_ep = update
        .checkpoint
        .episodes
        .iter()
        .find(|ep| matches!(&ep.signal, DiagnosticSignal::LockWait { key, .. } if key.session == *waiter_id))
        .cloned()
        .unwrap();
    let TelemetryRecord::Activity(waiter_obs) = &basis.records[waiter_pos].record else { panic!() };
    let DiagnosticFocusPlan::Ready(planned) = lock_ep.plan_focus(source, waiter_obs, &refs[waiter_pos])? else {
        panic!()
    };
    let DiagnosticFocus::Blocking { focus: lock_focus } = *planned else { panic!() };
    Ok((output_ep, output_focus, output_exec, lock_ep, *lock_focus))
}

fn build_grant_answer(
    basis: &SynthesisCapture,
    result: &SynthesisCapture,
    focus: &SourceReadFocus,
    output_id: &NativeActivityIdentity,
) -> Result<OutputGrantRetentionAnswer> {
    let b_orig = basis.verified_original()?;
    let b_pos = b_orig
        .records()
        .iter()
        .position(|r| matches!(&r.row().record, TelemetryRecord::Activity(obs) if obs.key.session == *output_id))
        .unwrap();
    let r_orig = result.verified_original()?;
    let refs = (0..r_orig.records().len()).map(|i| r_orig.reference(i)).collect::<Result<Vec<_>>>()?;
    let pairs = refs.iter().zip(r_orig.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
    OutputGrantRetentionAnswer::from_originals(
        focus,
        b_orig.source(),
        b_orig.records()[b_pos].row(),
        r_orig.source(),
        r_orig.original(),
        &pairs,
    )
}

fn resolve_episode(
    recovered: &SynthesisCapture,
    episode: &DiagnosticEpisode,
    policy: &ActivityPolicy,
) -> Result<DiagnosticRevision> {
    let orig = recovered.verified_original()?;
    let refs = (0..orig.records().len()).map(|i| orig.reference(i)).collect::<Result<Vec<_>>>()?;
    let rows = orig
        .records()
        .iter()
        .zip(&refs)
        .map(|(row, r)| {
            let TelemetryRecord::Activity(obs) = &row.row().record else { panic!() };
            DiagnosticActivityRow { reference: r, observation: obs }
        })
        .collect::<Vec<_>>();
    let prev = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: episode.scope.target.clone(),
        input: episode.latest.capture_ref(),
        episodes: vec![episode.clone()],
    };
    let update = observe_diagnostic_activity(
        orig.source(),
        orig.original(),
        &rows,
        Some(&prev),
        policy,
        &chrono::Utc::now().to_rfc3339(),
    )?;
    Ok(update
        .revisions
        .into_iter()
        .find(|r| r.episode.id == episode.id && r.episode.lifecycle == DiagnosticLifecycle::Resolved)
        .unwrap())
}
