//! Owned SQL Server 2022 parallel operator/task native acceptance:
//! 1. Real parallel scan (`MAXDOP 2`) on `dbo.culprit_parallel_items` with query-local profiling reads non-matching rows
//!    (`rows_read > rows_returned`) and holds 2 checked parallel worker tasks at the `Clustered Index Scan` node (`LCK_M_S`).
//! 2. `Activity` -> `CurrentRequestWork` -> `SourceRead(CurrentExecutionProfile)` captures real parallel operator/task
//!    context (`ParallelOperatorContext::observations` non-empty, `checked_tasks_at_node >= 2`, `ExecutionTaskWaitKind::NativeLock`).
//! 3. Comparable intervention: under the exact same held `tenant = 0` row locks, selective index seek on `tenant = 7`
//!    completes immediately with exact matching row count (`100`) without scanning `tenant = 0` pages.
//! 4. Gate release retires the parallel actor -> ended `CurrentExecutionProfile` + post-recovery `Activity` -> canonical
//!    reopen -> active & resolved Gold `DerivedDiagnosis` (`CandidatesOnly`, zero `culprit_queries`, table + predicate +
//!    conditional lock remedy, and explicit non-skew / independent-endpoint caveats).
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisCapture, SynthesisInput, SynthesisRequest},
};
use culprit_core::limits::OperationGuard;
use culprit_mssql::native_test_support::{
    self, PARALLEL_APPLICATION, PARALLEL_MATCHING_ROWS, ParallelOperatorFixture, SourceEndpoint,
};
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
async fn owned_parallel_operator_profile_and_indexed_intervention_reach_canonical_gold_honestly() {
    native_test_support::run_parallel_operator_case(|mut fixture, cancellation| {
        Box::pin(async move { Box::pin(prove(&mut fixture, &cancellation)).await })
    })
    .await;
}

async fn prove(fixture: &mut ParallelOperatorFixture<'_>, cancellation: &CollectionCancellation) -> Result<()> {
    let endpoint = fixture.endpoint()?;
    let mut captures = OwnedCaptures::new()?;
    let identity = Box::pin(fixture.start(cancellation)).await?;

    let basis = Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;
    let policy = ActivityPolicy { blocked_after_ms: 30_000, running_after_ms: 1000 };
    let (episode, participant) = select_parallel_request(&basis, &identity, &policy)?;

    let work = Box::pin(capture(
        &mut captures,
        &endpoint,
        CollectionSelection::CurrentRequestWork { focus: Box::new(participant.clone()) },
        cancellation,
    ))
    .await?;

    let (episode, focus) = build_profile_question(episode, &work)?;
    let execution = SourceReadExecutionScope::from_context(focus.question, &basis.activity_context()?)?;
    let profile_sel =
        CollectionSelection::SourceRead { focus: Box::new(focus.clone()), execution: Box::new(execution) };
    let profiled = Box::pin(capture(&mut captures, &endpoint, profile_sel.clone(), cancellation)).await?;

    // Comparable intervention while the exact same `tenant = 0` row locks are still held!
    let (seek_rows, seek_elapsed_ms) = Box::pin(fixture.run_indexed_intervention(cancellation)).await?;
    assert_eq!(seek_rows, PARALLEL_MATCHING_ROWS);
    assert!(seek_elapsed_ms < 2_000, "selective index seek must bypass locked tenant=0 pages: {seek_elapsed_ms}ms");

    Box::pin(fixture.finish(cancellation)).await?;

    let ended = Box::pin(capture(&mut captures, &endpoint, profile_sel, cancellation)).await?;
    let recovered = Box::pin(capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation)).await?;

    for cap in [&basis, &work, &profiled, &ended, &recovered] {
        Box::pin(captures.verify_retained(cap)).await?;
    }

    let reopened = Box::pin(reopen_all(&captures, [&basis, &work, &profiled, &ended, &recovered])).await?;
    let refs = reopened.captures().collect::<Vec<_>>();
    let [r_basis, r_work, r_profiled, r_ended, r_recovered] = refs.as_slice() else { panic!() };

    let answer = build_profile_answer(r_basis, r_profiled, &focus)?;
    let p_orig = r_profiled.verified_original()?;
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &p_orig.records()[0].row().record
    else {
        panic!()
    };
    let report = culprit_checks::current_execution_profile::explain(&answer, point)?;
    assert!(
        !report.parallel_context.observations.is_empty(),
        "real parallel operator observations expected; gaps={:?}",
        report.parallel_context.gaps
    );
    assert!(
        report
            .parallel_context
            .observations
            .iter()
            .all(|obs| { obs.checked_tasks_at_node >= 2 && obs.kind == ExecutionTaskWaitKind::NativeLock })
    );
    let scan_observations = report
        .parallel_context
        .observations
        .iter()
        .filter(|obs| {
            obs.operator.object.as_deref().is_some_and(|obj| obj.contains("culprit_parallel_items"))
                && obs.read_more_than_returned()
        })
        .count();
    assert!(
        scan_observations >= 2,
        "both parallel scan threads must read non-matching rows: {:?}",
        report.parallel_context.observations
    );
    assert!(
        report.parallel_context.gaps.contains(&ParallelOperatorContextGap::BlockingOwnerIdentityUnavailable),
        "task lock wait must keep blocker generation gap explicit"
    );
    drop(p_orig);

    let ended_answer = build_profile_answer(r_basis, r_ended, &focus)?;
    assert_eq!(ended_answer.membership, FocusedRequestMembership::NotObserved);

    let answered_ep = episode
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(answer))),
            },
        )?;
    let active_hist = history(answered_ep.clone(), 994_100).await;
    let active_gold = synthesize(&reopened, &[&active_hist], &[r_basis, r_work, r_profiled])?;
    active_gold.validate()?;
    let [inc] = active_gold.incidents.as_slice() else { panic!() };
    assert_eq!(inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(inc.culprit_queries.is_empty());
    assert!(!inc.affected_queries.is_empty() && !inc.candidate_queries.is_empty());
    assert!(inc.mechanism.as_ref().unwrap().contains("culprit_parallel_items"));
    assert!(inc.developer_action.contains(&t!("view.execution_profile.operator_context.lock_action")));
    assert!(inc.verification.contains(&t!("view.execution_profile.operator_context.lock_verify")));
    assert!(inc.entry.reasons.contains(&t!("view.execution_profile.operator_context.independent")));
    assert!(inc.entry.reasons.contains(&t!("view.execution_profile.operator_context.not_skew")));
    let selected = inc
        .candidate_queries
        .iter()
        .find(|q| {
            q.sql.is_available()
                && q.caller.as_ref().is_some_and(|c| {
                    c.client.as_ref().and_then(|cl| cl.application.as_deref()) == Some(PARALLEL_APPLICATION)
                })
        })
        .expect("candidate query must retain native SQL and caller");
    assert!(selected.causal_role.is_none());

    let resolved_rev = resolve_episode(r_recovered, &answered_ep, &policy)?;
    let res_hist = history_revision(resolved_rev, 994_101).await;
    let res_gold = synthesize(&reopened, &[&res_hist, &active_hist], &[r_basis, r_work, r_profiled, r_recovered])?;
    res_gold.validate()?;
    assert!(res_gold.incidents.iter().any(|i| i.entry.lifecycle == DiagnosticLifecycle::Resolved));

    native_test_support::save_parallel_operator_receipt(&json!({
        "parallel_observations": report.parallel_context.observations.len(),
        "parallel_gaps": report.parallel_context.gaps,
        "indexed_intervention_rows": seek_rows,
        "indexed_intervention_elapsed_ms": seek_elapsed_ms,
        "source_queries": [
            basis.manifest.source_queries,
            work.manifest.source_queries,
            profiled.manifest.source_queries,
            ended.manifest.source_queries,
            recovered.manifest.source_queries,
        ],
        "gold_attribution": inc.attribution_status,
        "culprit_claimed": false,
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
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-parallel-operator-1")?
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
        Box::pin(captures.capture("owned-parallel-operator", request, cancellation, move |_| Factory(endpoint)))
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

fn select_parallel_request(
    basis: &Captured,
    session: &NativeActivityIdentity,
    policy: &ActivityPolicy,
) -> Result<(DiagnosticEpisode, ActivityRequestFocus)> {
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
        .position(|r| {
            matches!(&r.record, TelemetryRecord::Activity(obs)
                if obs.key.session == *session && obs.scope == ActivityScopeBasis::RequestDatabase)
        })
        .unwrap();
    let TelemetryRecord::Activity(obs) = &basis.records[pos].record else { panic!() };
    let ep = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|ep| {
            matches!(&ep.signal, DiagnosticSignal::Activity { key, condition: ActivityCondition::LongRunning, .. }
                if key.session == *session)
        })
        .unwrap();
    let DiagnosticFocusPlan::Ready(planned) = ep.plan_focus(source, obs, &refs[pos])? else { panic!() };
    let DiagnosticFocus::CurrentRequestWork { focus } = *planned else { panic!() };
    Ok((ep, *focus))
}

fn build_profile_question(
    mut episode: DiagnosticEpisode,
    work: &Captured,
) -> Result<(DiagnosticEpisode, SourceReadFocus)> {
    let [row] = work.records.as_slice() else { panic!() };
    let reference = work.reference(0)?;
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: reference.clone() });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        work.source()?,
        &reference,
        row,
    )?
    .unwrap();
    Ok((episode, focus))
}

fn build_profile_answer(
    basis: &SynthesisCapture,
    result: &SynthesisCapture,
    focus: &SourceReadFocus,
) -> Result<CurrentExecutionProfileAnswer> {
    let b_orig = basis.verified_original()?;
    let b_row =
        b_orig.records().iter().find(|r| r.row().record.native_identity() == focus.basis.native_key).unwrap().row();
    let r_orig = result.verified_original()?;
    let r_ref = r_orig.reference(0)?;
    CurrentExecutionProfileAnswer::from_originals(
        focus,
        b_orig.source(),
        b_row,
        r_orig.source(),
        r_orig.original(),
        &[(&r_ref, r_orig.records()[0].row())],
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
