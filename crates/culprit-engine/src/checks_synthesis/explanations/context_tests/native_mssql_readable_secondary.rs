//! Owned 2-node SQL Server 2022 Always On AG readable-secondary redo-blocking native acceptance:
//! 1. Phase 1 (load control): active long-running reader on the readable secondary (`Updateability = READ_ONLY`)
//!    with NO pending DDL redo -> `SourceRead(ReadableSecondaryRecovery)` observes `readers.is_empty()` and Gold
//!    keeps `CandidatesOnly` with zero culprits.
//! 2. Phase 2 (positive AG redo block): `ALTER TABLE` on the async-commit primary blocks secondary redo
//!    (`DB STARTUP` / `PARALLEL REDO TASK`) on `LCK_M_SCH_M` behind the exact secondary reader session ->
//!    `SourceRead(ReadableSecondaryRecovery)` (`14` dispatches, `Quiescent`) proves `RecoveryReaderRole::NativeRedoBlocker`
//!    and Gold emits `DerivedAttributionStatus::Established` (`NativeWaitOwner::Session`, `NativeBlockingOwner`,
//!    `!sql.is_available()`).
//! 3. Phase 3 (intervention & resolution): draining and retiring the secondary reader immediately unblocks redo
//!    (`redo_col` visible on secondary) -> ended `ReadableSecondaryRecovery` (`NotObserved`) + post-recovery
//!    `Activity` -> canonical reopen -> `DiagnosticLifecycle::Resolved` Gold retaining the native culprit and action.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisCapture, SynthesisInput, SynthesisRequest},
};
use culprit_core::limits::OperationGuard;
use culprit_mssql::native_test_support::{self, AG_REDO_STREAM_ROWS, AgRedoBlockingFixture, SourceEndpoint};
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
#[ignore = "requires explicit nonce-owned 2-node SQL Server 2022 AG secondary/primary fixture and private cost receipt directory"]
async fn owned_mssql_ag_readable_secondary_redo_blocker_and_drain_intervention_reach_canonical_gold() {
    native_test_support::run_ag_redo_blocking_case(|fixture, cancellation| {
        Box::pin(async move { Box::pin(prove(fixture, &cancellation)).await })
    })
    .await;
}

async fn prove(fixture: &mut AgRedoBlockingFixture, cancellation: &CollectionCancellation) -> Result<()> {
    let endpoint = fixture.secondary_endpoint();
    let mut captures = OwnedCaptures::new()?;
    let policy = ActivityPolicy { blocked_after_ms: 1000, running_after_ms: 1000 };

    eprintln!("owned mssql ag secondary phase 1: seed & start paused secondary reader");
    let reader_id = Box::pin(fixture.prepare_and_start_secondary_reader(cancellation)).await?;

    let basis = Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;
    assert_eq!(basis.source()?.observation.activity().unwrap().database_writable, Some(false));
    let (episode, focus, execution) = select_secondary_episode(&basis, &reader_id, &policy)?;
    let recovery_sel =
        CollectionSelection::SourceRead { focus: Box::new(focus.clone()), execution: Box::new(execution) };

    eprintln!("owned mssql ag secondary phase 1b: capture non-conflicting ReadableSecondaryRecovery control");
    let no_redo_cap = Box::pin(capture(&mut captures, &endpoint, recovery_sel.clone(), cancellation)).await?;
    assert_eq!(no_redo_cap.manifest.source_queries, 14);

    eprintln!("owned mssql ag secondary phase 2: block secondary AG redo on LCK_M_SCH_M behind reader");
    Box::pin(fixture.block_secondary_redo_on_reader(&reader_id, cancellation)).await?;
    let blocked_cap = Box::pin(capture(&mut captures, &endpoint, recovery_sel.clone(), cancellation)).await?;
    assert_eq!(blocked_cap.manifest.source_queries, 14);

    eprintln!("owned mssql ag secondary phase 3: drain reader, confirm redo applied, capture ended + recovered");
    let drained_rows = Box::pin(fixture.drain_reader_and_await_redo(&reader_id, cancellation)).await?;
    assert_eq!(drained_rows, AG_REDO_STREAM_ROWS);

    let ended_cap = Box::pin(capture(&mut captures, &endpoint, recovery_sel, cancellation)).await?;
    let recovered_act =
        Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;

    for cap in [&basis, &no_redo_cap, &blocked_cap, &ended_cap, &recovered_act] {
        Box::pin(captures.verify_retained(cap)).await?;
    }

    let reopened =
        Box::pin(reopen_all(&captures, [&basis, &no_redo_cap, &blocked_cap, &ended_cap, &recovered_act])).await?;
    let refs = reopened.captures().collect::<Vec<_>>();
    let [r_basis, r_no_redo, r_blocked, r_ended, r_recovered] = refs.as_slice() else { panic!() };

    // 1. Non-conflicting secondary reader control -> zero culprits, CandidatesOnly.
    let (no_redo_ans, no_redo_pt) = build_recovery_answer(r_basis, r_no_redo, &focus, &reader_id)?;
    let no_redo_report = culprit_checks::readable_secondary_recovery::explain(&no_redo_ans, Some(&no_redo_pt))?;
    assert!(no_redo_report.readers.is_empty());
    let no_redo_ep = episode
        .clone()
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::ReadableSecondaryRecovery(Box::new(no_redo_ans))),
            },
        )?;
    let no_redo_hist = history(no_redo_ep, 998_600).await;
    let no_redo_gold = synthesize(&reopened, &[&no_redo_hist], &[r_basis, r_no_redo])?;
    no_redo_gold.validate()?;
    let [no_redo_inc] = no_redo_gold.incidents.as_slice() else { panic!() };
    assert!(no_redo_inc.culprit_queries.is_empty());
    assert_eq!(no_redo_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);

    // 2. Blocked AG redo positive -> NativeRedoBlocker culprit (`!sql.is_available()`), Established.
    let (blocked_ans, blocked_pt) = build_recovery_answer(r_basis, r_blocked, &focus, &reader_id)?;
    let blocked_report = culprit_checks::readable_secondary_recovery::explain(&blocked_ans, Some(&blocked_pt))?;
    assert_eq!(blocked_report.readers.len(), 1);
    assert_eq!(blocked_report.readers[0].role, RecoveryReaderRole::NativeRedoBlocker);
    assert_eq!(blocked_report.readers[0].session, reader_id);
    assert_eq!(blocked_report.summary, t!("view.secondary_recovery.mssql.summary"));
    assert_eq!(blocked_report.investigation, t!("view.secondary_recovery.mssql.action"));

    let answered_ep = episode
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::ReadableSecondaryRecovery(Box::new(blocked_ans))),
            },
        )?;
    let active_hist = history(answered_ep.clone(), 998_610).await;
    let active_gold = synthesize(&reopened, &[&active_hist], &[r_basis, r_blocked])?;
    active_gold.validate()?;
    let [active_inc] = active_gold.incidents.as_slice() else { panic!() };
    assert_eq!(active_inc.attribution_status, DerivedAttributionStatus::Established);
    assert_eq!(active_inc.affected_queries.len(), 1);
    assert_eq!(active_inc.culprit_queries.len(), 1);
    let culprit = &active_inc.culprit_queries[0];
    assert_eq!(culprit.causal_role, Some(DerivedCausalRole::new(DerivedCausalRoleKind::NativeBlockingOwner)));
    assert!(!culprit.sql.is_available(), "current reader SQL is never claimed as the lock-acquiring statement");
    assert_eq!(
        culprit.identity,
        Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session {
            id: reader_id.id.clone(),
            generation: Some(reader_id.generation.clone()),
        }))
    );

    // 3. Ended ReadableSecondaryRecovery + Resolved Gold after drain & redo completion.
    let (ended_ans, ended_pt) = build_recovery_answer(r_basis, r_ended, &focus, &reader_id)?;
    assert_eq!(ended_pt.membership, FocusedRequestMembership::NotObserved);
    let ended_report = culprit_checks::readable_secondary_recovery::explain(&ended_ans, Some(&ended_pt))?;
    assert!(ended_report.readers.is_empty());

    let resolved_rev = resolve_episode(r_recovered, &answered_ep, &policy)?;
    let resolved_hist = history_revision(resolved_rev, 998_611).await;
    let resolved_gold = synthesize(&reopened, &[&resolved_hist, &active_hist], &[r_basis, r_blocked, r_recovered])?;
    resolved_gold.validate()?;
    let [resolved_inc] = resolved_gold.incidents.as_slice() else { panic!() };
    assert_eq!(resolved_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(resolved_inc.attribution_status, DerivedAttributionStatus::Established);
    assert_eq!(resolved_inc.culprit_queries.len(), 1);
    assert!(!resolved_inc.culprit_queries[0].sql.is_available());

    native_test_support::save_ag_redo_blocking_receipt(&json!({
        "drained_rows": drained_rows,
        "reader_session": reader_id,
        "source_queries": [
            basis.manifest.source_queries,
            no_redo_cap.manifest.source_queries,
            blocked_cap.manifest.source_queries,
            ended_cap.manifest.source_queries,
            recovered_act.manifest.source_queries,
        ],
        "no_redo_attribution": no_redo_inc.attribution_status,
        "blocked_redo_attribution": active_inc.attribution_status,
        "resolved_lifecycle": resolved_inc.entry.lifecycle,
        "acquiring_sql_invented": false,
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
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-mssql-ag-secondary-1")?
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
        Box::pin(captures.capture("owned-mssql-ag-secondary", request, cancellation, move |_| Factory(endpoint)))
            .await?,
    );
    assert_eq!(cap.manifest.status, CollectionStatus::Complete);
    let metrics = cap.manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    Ok(cap)
}

async fn reopen_all(captures: &OwnedCaptures, originals: [&Captured; 5]) -> Result<SynthesisInput> {
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout)?;
    let request = SynthesisRequest {
        primary: originals[0].capture_ref()?,
        context: originals[1..].iter().map(|c| c.capture_ref()).collect::<Result<Vec<_>>>()?,
        budget,
    };
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await
}

fn select_secondary_episode(
    basis: &Captured,
    reader_id: &NativeActivityIdentity,
    policy: &ActivityPolicy,
) -> Result<(DiagnosticEpisode, SourceReadFocus, SourceReadExecutionScope)> {
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
    let pos = basis
        .records
        .iter()
        .position(|r| matches!(&r.record, TelemetryRecord::Activity(obs) if obs.key.session == *reader_id))
        .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .iter()
        .find(|ep| {
            matches!(
                &ep.signal,
                DiagnosticSignal::Activity { key, condition: ActivityCondition::LongRunning, .. }
                    if key.session == *reader_id
            )
        })
        .cloned()
        .unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::ReadableSecondaryRecovery,
        &episode,
        source,
        &refs[pos],
        &basis.records[pos],
    )?
    .unwrap();
    let execution = SourceReadExecutionScope::from_context(focus.question, &focus.original_context(source)?)?;
    Ok((episode, focus, execution))
}

fn build_recovery_answer(
    basis: &SynthesisCapture,
    result: &SynthesisCapture,
    focus: &SourceReadFocus,
    reader_id: &NativeActivityIdentity,
) -> Result<(ReadableSecondaryRecoveryAnswer, ReadableSecondaryRecoveryPoint)> {
    let b_orig = basis.verified_original()?;
    let b_pos = b_orig
        .records()
        .iter()
        .position(|r| matches!(&r.row().record, TelemetryRecord::Activity(obs) if obs.key.session == *reader_id))
        .unwrap();
    let r_orig = result.verified_original()?;
    let refs = (0..r_orig.records().len()).map(|i| r_orig.reference(i)).collect::<Result<Vec<_>>>()?;
    let pairs = refs.iter().zip(r_orig.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
    let answer = ReadableSecondaryRecoveryAnswer::from_originals(
        focus,
        b_orig.source(),
        b_orig.records()[b_pos].row(),
        r_orig.source(),
        r_orig.original(),
        &pairs,
    )?;
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::ReadableSecondaryRecovery(point),
        ..
    }) = &r_orig.records()[0].row().record
    else {
        panic!()
    };
    Ok((answer, (**point).clone()))
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
