//! Owned SQL Server 2022 worker-starvation native acceptance:
//! 1. Genuine `max worker threads = 128` saturation burst:
//!    - Proves diagnostic worker unavailability (`!= Complete`) on an already-warmed isolated `Pool` during full saturation.
//!    - Captures real `Snapshot(Operational)` (`QueueKind::WorkerTasks > 0` -> `DiagnosticOriginalCondition::MssqlWorkerQueue`)
//!      through `Outbox` during the staggered `THREADPOOL` turnover window.
//! 2. Drains the transient burst waves while sustained lock-waiting (`WORKER_LOCK_WAITER_APP`) and parallel
//!    (`WORKER_PARALLEL_APP`) contributors remain active -> captures `Outbox`-authenticated `SourceRead(WorkerStarvation)`
//!    (`CollectionStatus::Complete`, `SourceShutdown::Quiescent`).
//! 3. Releases the blocker gate and resets `max worker threads = 0` -> captures post-intervention `Snapshot(Operational)`
//!    (`QueueKind::WorkerTasks == "0"`) -> canonical reopen -> Gold `DerivedDiagnosis` (answered + stopped controls).
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisCapture, SynthesisInput, SynthesisRequest},
};
use culprit_core::limits::OperationGuard;
use culprit_mssql::native_test_support::{
    self, SourceEndpoint, WORKER_LOCK_WAITER_APP, WORKER_PARALLEL_APP, WorkerStarvationFixture,
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
async fn owned_mssql_worker_starvation_burst_and_controlled_census_reach_canonical_gold_with_quiescent_settlement() {
    native_test_support::run_worker_starvation_case(|mut fixture, cancellation| {
        Box::pin(async move { Box::pin(prove(&mut fixture, &cancellation)).await })
    })
    .await;
}

async fn prove(fixture: &mut WorkerStarvationFixture<'_>, cancellation: &CollectionCancellation) -> Result<()> {
    fixture.initialize_tables(cancellation).await?;
    let endpoint = fixture.endpoint()?;
    let mut captures = OwnedCaptures::new()?;

    let started = Box::pin(fixture.start_starvation_episode(cancellation)).await?;
    assert!(started.warm_open_queries > 0, "isolated diagnostic pool must already hold an open warm session");
    assert_ne!(
        started.starved_probe_status,
        CollectionStatus::Complete,
        "full 128-worker saturation must block diagnostic worker execution despite an already-open TCP connection: {started:?}"
    );

    // Capture genuine `SCHEDULERS` `work_queue_count > 0` (`MssqlWorkerQueue`) as the staggered waves turn over workers!
    let shared_subject = CollectionSubject::Shared { resource: started.instance.clone() };
    let op_sel = CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational };
    let basis =
        Box::pin(capture_subject(&mut captures, &endpoint, shared_subject.clone(), op_sel.clone(), cancellation))
            .await?;
    let (episode, focus, execution, queue_during_burst) = build_worker_question(&basis)?;
    assert!(
        queue_during_burst > 0,
        "native scheduler work_queue_count must be positive during saturation: {queue_during_burst}"
    );

    Box::pin(fixture.drain_staggered_burst_waves(cancellation)).await?;

    let worker_subject = execution.subject();
    let worker_sel = CollectionSelection::SourceRead { focus: Box::new(focus.clone()), execution: Box::new(execution) };
    let worker_cap =
        Box::pin(capture_subject(&mut captures, &endpoint, worker_subject, worker_sel, cancellation)).await?;

    Box::pin(fixture.finish_all(cancellation)).await?;

    let post_op = Box::pin(capture_subject(&mut captures, &endpoint, shared_subject, op_sel, cancellation)).await?;
    let queue_after_recovery = worker_queue_count(&post_op)?;
    assert_eq!(
        queue_after_recovery, 0,
        "releasing the gate and resetting max worker threads must clear scheduler work_queue_count"
    );

    for cap in [&basis, &worker_cap, &post_op] {
        Box::pin(captures.verify_retained(cap)).await?;
    }

    let reopened = Box::pin(reopen_all(&captures, [&basis, &worker_cap, &post_op])).await?;
    let refs = reopened.captures().collect::<Vec<_>>();
    let [r_basis, r_worker, _r_post_op] = refs.as_slice() else { panic!() };

    let answer = build_worker_answer(r_basis, r_worker, &focus)?;
    let w_orig = r_worker.verified_original()?;
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlWorkerStarvation(point),
        ..
    }) = &w_orig.records()[0].row().record
    else {
        panic!()
    };
    let report = culprit_checks::worker_starvation::explain(&answer, point)?;
    assert!(
        answer.evidence.assigned_workers >= 3,
        "waiter + parallel contributors must be observed: {:?}",
        answer.evidence
    );
    assert!(answer.evidence.contributions.iter().any(|c| {
        c.kind == WorkerContributionKind::AssignedParallelWorkers && c.request.session == started.parallel_id
    }));
    assert!(answer.evidence.contributions.iter().any(|c| {
        c.kind == WorkerContributionKind::LockWaitingAssignedWorkers && c.request.session == started.waiter_id
    }));
    drop(w_orig);

    // Active Gold with answered WorkerStarvation census.
    let answered_ep = episode
        .clone()
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::WorkerStarvation(Box::new(answer))) },
        )?;
    let active_hist = history(answered_ep, 993_100).await;
    let active_gold = synthesize(&reopened, &[&active_hist], &[r_basis, r_worker])?;
    active_gold.validate()?;
    let [active_inc] = active_gold.incidents.as_slice() else { panic!() };
    assert_eq!(active_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(
        active_inc.culprit_queries.is_empty(),
        "once the transient queue drains before census, sustained holders remain candidates rather than false culprits"
    );
    assert!(!active_inc.candidate_queries.is_empty());
    assert!(active_inc.candidate_queries.iter().any(|q| {
        q.causal_role.is_none()
            && q.caller.as_ref().and_then(|c| c.client.as_ref()).and_then(|cl| cl.application.as_deref())
                == Some(WORKER_PARALLEL_APP)
    }));
    assert!(active_inc.candidate_queries.iter().any(|q| {
        q.causal_role.is_none()
            && q.caller.as_ref().and_then(|c| c.client.as_ref()).and_then(|cl| cl.application.as_deref())
                == Some(WORKER_LOCK_WAITER_APP)
    }));
    assert!(active_inc.mechanism.as_ref().unwrap().contains(&report.summary));
    assert!(active_inc.entry.reasons.contains(&t!("view.worker_starvation.units")));

    // Stopped Gold when the diagnostic worker itself timed out during full 128-worker saturation.
    let stopped_ep = episode
        .enroll_source_read(focus.clone())?
        .transition_source_read(&focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            &focus,
            SourceReadStageState::Stopped {
                issue: DiagnosticAnalysisIssue::SourceUnavailable {
                    code: Some(ErrorCode::Timeout),
                    stopped: Some(CollectionStop::TimedOut),
                },
            },
        )?;
    let stopped_hist = history(stopped_ep, 993_101).await;
    let stopped_gold = synthesize(&reopened, &[&stopped_hist], &[r_basis])?;
    stopped_gold.validate()?;
    let [stopped_inc] = stopped_gold.incidents.as_slice() else { panic!() };
    assert_eq!(stopped_inc.entry.lifecycle, DiagnosticLifecycle::Uncertain);
    assert!(stopped_inc.culprit_queries.is_empty() && stopped_inc.candidate_queries.is_empty());
    assert!(stopped_inc.entry.reasons.contains(&t!("view.incident_history.source_stage", state = "stopped")));
    assert!(stopped_inc.entry.reasons.contains(&t!("view.worker_starvation.question")));

    native_test_support::save_worker_starvation_receipt(&json!({
        "warm_open_queries": started.warm_open_queries,
        "starved_probe_status": format!("{:?}", started.starved_probe_status),
        "starved_probe_shutdown": format!("{:?}", started.starved_probe_shutdown),
        "worker_queue_during_burst": queue_during_burst,
        "worker_queue_after_recovery": queue_after_recovery,
        "assigned_workers": report.evidence.assigned_workers,
        "observed_tasks": report.evidence.observed_tasks,
        "source_queries": [
            basis.manifest.source_queries,
            worker_cap.manifest.source_queries,
            post_op.manifest.source_queries,
        ],
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
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-worker-starvation-1")?
        .analyze_originals(&originals, input.recorded_at(), input.display(), vec![])
}

async fn capture_subject(
    captures: &mut OwnedCaptures,
    endpoint: &SourceEndpoint,
    subject: CollectionSubject,
    selection: CollectionSelection,
    cancellation: &CollectionCancellation,
) -> Result<Box<Captured>> {
    let endpoint = endpoint.clone();
    let request = endpoint.request_for_subject(subject, selection)?;
    let cap = Box::new(
        Box::pin(captures.capture("owned-worker-starvation", request, cancellation, move |_| Factory(endpoint)))
            .await?,
    );
    assert_eq!(cap.manifest.status, CollectionStatus::Complete);
    let metrics = cap.manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    Ok(cap)
}

async fn reopen_all(captures: &OwnedCaptures, originals: [&Captured; 3]) -> Result<SynthesisInput> {
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout)?;
    let request = SynthesisRequest {
        primary: originals[0].capture_ref()?,
        context: originals[1..].iter().map(|c| c.capture_ref()).collect::<Result<Vec<_>>>()?,
        budget,
    };
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await
}

fn worker_queue_count(op_cap: &Captured) -> Result<u64> {
    let mut saw_scheduler = false;
    let mut total = 0u64;
    for r in &op_cap.records {
        if let TelemetryRecord::Operational(OperationalObservation {
            source,
            fact: OperationalFact::CounterSet(sample),
            ..
        }) = &r.record
            && source == culprit_core::source_id::SCHEDULERS
            && let Ok(point) = MssqlSchedulerPoint::from_sample(sample)
        {
            let value = |name| {
                MSSQL_SCHEDULER_FIELDS.iter().find(|field| field.name == name).and_then(|field| point.field(field).ok())
            };
            if matches!(value("status"), Some(MssqlNativeFieldValue::Observed("VISIBLE ONLINE")))
                && matches!(value("is_online"), Some(MssqlNativeFieldValue::Observed("1")))
            {
                saw_scheduler = true;
                if let Some(MssqlNativeFieldValue::Observed(count)) = value("work_queue_count") {
                    total += count.parse::<u64>().unwrap();
                }
            }
        }
    }
    assert!(saw_scheduler, "Shared Operational capture must include VISIBLE ONLINE scheduler rows");
    Ok(total)
}

fn build_worker_question(
    basis: &Captured,
) -> Result<(DiagnosticEpisode, SourceReadFocus, SourceReadExecutionScope, u64)> {
    let count = worker_queue_count(basis)?;
    let source = basis.source()?;
    let refs = (0..basis.records.len()).map(|i| basis.reference(i)).collect::<Result<Vec<_>>>()?;
    let rows = basis
        .records
        .iter()
        .zip(&refs)
        .map(|(row, reference)| DiagnosticOriginalRow { reference, record: &row.record })
        .collect::<Vec<_>>();
    let observed =
        observe_diagnostic_original(source, &basis.capture_ref()?, &rows, None, &[], &chrono::Utc::now().to_rfc3339())?;
    let episode = observed
        .checkpoint
        .episodes
        .into_iter()
        .map(|ep| ep.finish_original_observation_planning())
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .find(|ep| ep.source_read_focus(SourceReadQuestion::WorkerStarvation).ok().flatten().is_some())
        .expect("positive worker-queue count must start MssqlWorkerQueue episode");
    let focus = episode.source_read_focus(SourceReadQuestion::WorkerStarvation)?.unwrap();
    let execution = SourceReadExecutionScope::from_context(focus.question, &focus.original_context(source)?)?;
    Ok((episode, focus, execution, count))
}

fn build_worker_answer(
    basis: &SynthesisCapture,
    worker_cap: &SynthesisCapture,
    focus: &SourceReadFocus,
) -> Result<WorkerStarvationAnswer> {
    let b_orig = basis.verified_original()?;
    let b_row =
        b_orig.records().iter().find(|r| r.row().record.native_identity() == focus.basis.native_key).unwrap().row();
    let w_orig = worker_cap.verified_original()?;
    let w_ref = w_orig.reference(0)?;
    WorkerStarvationAnswer::from_originals(
        focus,
        b_orig.source(),
        b_row,
        w_orig.source(),
        w_orig.original(),
        &[(&w_ref, w_orig.records()[0].row())],
    )
}
