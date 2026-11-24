//! Actual neutral capture/proof/EOF then the same pure synthesizer, without Collector or source credentials.
use super::*;
use test_support::{worker_starvation_basis, worker_starvation_result};

async fn basis(shared: bool, sequence: u64) -> (EvidenceCapture, DiagnosticEpisode) {
    let (source, records) = worker_starvation_basis(shared);
    let capture = retained(source, records, sequence).await;
    let original = capture.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let episode = if shared {
        let rows = original
            .records()
            .iter()
            .enumerate()
            .map(|(position, row)| (original.reference(position).unwrap(), &row.row().record))
            .collect::<Vec<_>>();
        let rows =
            rows.iter().map(|(reference, record)| DiagnosticOriginalRow { reference, record }).collect::<Vec<_>>();
        observe_diagnostic_original(original.source(), original.original(), &rows, None, &[], "2020-01-01T12:00:02Z")
            .unwrap()
            .checkpoint
            .episodes
            .into_iter()
            .next()
            .unwrap()
            .finish_original_observation_planning()
            .unwrap()
    } else {
        let TelemetryRecord::Activity(actor) = &original.records()[0].row().record else { panic!() };
        DiagnosticEpisode::start(
            DiagnosticScope::from_source(original.source()).unwrap(),
            DiagnosticSignal::WorkerStarvation {
                key: actor.key.clone(),
                statement_identity: actor.statement_identity.clone(),
            },
            original.source().observation.activity().unwrap().at.clone(),
            reference,
        )
        .unwrap()
    };
    drop(original);
    (capture, episode)
}
async fn answered(
    shared: bool,
    sequence: u64,
    mutate: impl FnOnce(&mut MssqlWorkerStarvationPoint),
) -> (EvidenceCapture, EvidenceCapture, EvidenceCapture) {
    let (original, episode) = basis(shared, sequence).await;
    let focus = episode.source_read_focus(SourceReadQuestion::WorkerStarvation).unwrap().unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(original.source()).unwrap())
            .unwrap();
    let (source, mut records) = worker_starvation_result(&focus, &execution);
    let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
    let OperationalFact::MssqlWorkerStarvation(point) = &mut row.fact else { panic!() };
    mutate(point);
    row.key = point.key().unwrap();
    let result = retained(source, records, sequence + 1).await;
    let input = original.verified_original().unwrap();
    let output = result.verified_original().unwrap();
    let reference = output.reference(0).unwrap();
    let answer = WorkerStarvationAnswer::from_originals(
        &focus,
        input.source(),
        input.records()[0].row(),
        output.source(),
        output.original(),
        &[(&reference, output.records()[0].row())],
    )
    .unwrap();
    let episode = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::WorkerStarvation(Box::new(answer))) },
        )
        .unwrap();
    drop(input);
    drop(output);
    let history = history(episode, sequence + 2).await;
    (original, result, history)
}
#[tokio::test]
async fn worker_question_uses_canonical_originals_and_keeps_victim_blocker_parallel_and_external_roles_distinct() {
    let (original, result, history) = answered(false, 870_001, |_| {}).await;
    let diagnosis = project(&[&history, &original, &result], vec![]).unwrap();
    let incident = &diagnosis.incidents[0];
    diagnosis.validate().unwrap();
    assert!(!incident.affected_queries.is_empty());
    assert!(incident.affected_queries.iter().all(|query| query.causal_role.is_none()));
    assert!(incident.culprit_queries.iter().any(|query| matches!(&query.identity,
        Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session{id,..})) if id=="63")));
    let contributor=incident.culprit_queries.iter().find(|query|matches!(&query.identity,
        Some(DerivedQueryIdentity::SharedActivity{actor,database:Some(database),..}) if actor.session.id=="63"&&database=="foreign-observed-db")).unwrap();
    assert_eq!(contributor.causal_role.as_ref().unwrap().kind, DerivedCausalRoleKind::AssignedWorkerContributor);
    assert!(!contributor.sql.is_available());
    assert_eq!(
        contributor.caller.as_ref().unwrap().client.as_ref().unwrap().application.as_deref(),
        Some("db-client-63")
    );
    assert!(incident.candidate_queries.iter().any(|query| query.sql.is_available()
        && matches!(&query.identity,
        Some(DerivedQueryIdentity::SharedActivity{actor,..}) if actor.session.id=="63")));
    assert!(!incident.culprit_queries.iter().any(|query| matches!(&query.identity,
        Some(DerivedQueryIdentity::SharedActivity{actor,..}) if matches!(actor.session.id.as_str(),"61"|"62"|"64"))));
    assert!(incident.mechanism.as_ref().unwrap().contains(&t!(
        "view.worker_starvation.summary",
        tasks = "5",
        workers = "4",
        waits = "2"
    )));
    assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
    let restored: DerivedDiagnosis = serde_json::from_slice(&serde_json::to_vec(&diagnosis).unwrap()).unwrap();
    assert_eq!(restored, diagnosis);
    restored.validate().unwrap();
}
#[tokio::test]
async fn sessionless_shared_trigger_never_invents_a_database_or_victim_and_retains_unassigned_threadpool() {
    let (original, result, history) = answered(true, 870_011, |point| {
        point.tasks.clear();
        point.requests.clear();
        point.threadpool_waits.retain(|wait| wait.session_id.is_none());
    })
    .await;
    let retained_revision = history
        .records()
        .iter()
        .find_map(|row| {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::DiagnosticRevision(revision),
                ..
            }) = &row.row().record
            else {
                return None;
            };
            Some(revision.as_ref())
        })
        .expect("the immutable terminal revision survives canonical reopening");
    assert!(
        active_checkpoint(retained_revision).episodes.is_empty(),
        "an answered shared capture-point is retired only from working state"
    );
    assert!(
        retained_revision.episode.source_reads.get(SourceReadQuestion::WorkerStarvation).unwrap().result().is_some()
    );
    let diagnosis = project(&[&history, &original, &result], vec![]).unwrap();
    let incident = &diagnosis.incidents[0];
    assert_eq!(incident.entry.episode_id, retained_revision.episode.id);
    assert!(incident.entry.original.as_ref().is_some_and(|reference| reference.capture == *history.capture()));
    assert!(incident.database.is_none());
    assert!(incident.affected_queries.is_empty());
    assert!(incident.culprit_queries.is_empty());
    assert!(incident.entry.reasons.contains(&t!("view.worker_starvation.unassigned", count = "1")));
    assert!(incident.entry.reasons.contains(&t!("view.worker_starvation.scheduler_unknown")));
}
#[tokio::test]
async fn changed_actors_withhold_sql_callers_and_culprits_while_same_point_counts_and_unresolved_context_survive() {
    let (original, result, history) = answered(false, 870_021, |point| {
        for row in &mut point.requests {
            row.after_check.as_mut().unwrap().state = TaskRequestState::Changed;
        }
    })
    .await;
    let diagnosis = project(&[&history, &original, &result], vec![]).unwrap();
    let incident = &diagnosis.incidents[0];
    assert!(incident.candidate_queries.is_empty() && incident.culprit_queries.is_empty());
    assert!(incident.entry.reasons.contains(&t!("view.worker_starvation.request_unverified")));
    let without_context = project(&[&history, &original], vec![]).unwrap();
    assert!(
        without_context.incidents[0].culprit_queries.is_empty(),
        "a serialized answer cannot replace unavailable original bodies"
    );
}
#[tokio::test]
async fn shared_worker_scope_and_actual_point_are_required_and_a_tampered_answer_cannot_supply_an_actor() {
    let (original, result, history) = answered(false, 870_031, |_| {}).await;
    let before = project(&[&history, &original, &result], vec![]).unwrap();
    before.validate().unwrap();
    let raw = history.verified_original().unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::DiagnosticRevision(revision),
        ..
    }) = &raw.records()[0].row().record
    else {
        panic!()
    };
    let mut episode = revision.episode.clone();
    let mut stages = episode.source_reads.iter().cloned().collect::<Vec<_>>();
    let stage = stages.iter_mut().find(|stage| stage.focus.question == SourceReadQuestion::WorkerStarvation).unwrap();
    let SourceReadStageState::Answered { answer } = &mut stage.state else { panic!() };
    let SourceReadAnswer::WorkerStarvation(answer) = answer.as_mut() else { panic!() };
    answer.evidence.assigned_workers += 1;
    let projections = [
        history.verified_original().unwrap(),
        original.verified_original().unwrap(),
        result.verified_original().unwrap(),
    ];
    let originals = projections.iter().map(|input| input as &dyn OriginalProjection).collect::<Vec<_>>();
    let accepted = match SourceReadStages::from_stages(stages) {
        Ok(stages) => {
            episode.source_reads = stages;
            for_episode(&originals, &episode).is_ok()
        }
        Err(_) => false,
    };
    assert!(!accepted, "native proof must match the retained full point, not answer counts");
}

#[tokio::test]
async fn query_detail_cap_is_explicit_while_the_full_bounded_worker_point_remains_authenticated() {
    let (original, result, history) = answered(false, 870_041, |point| {
        for id in 80..100 {
            let mut request = point.requests[2].clone();
            request.observation.key.session.id = id.to_string();
            request.observation.key.session.generation = format!("native-login-{id}");
            let mut task = point.tasks[2].clone();
            task.address = format!("{id:016X}");
            task.session_id = Some(id as i16);
            task.request = Some(request.observation.key.clone());
            let worker = task.worker.as_mut().unwrap();
            worker.address = format!("{:016X}", id + 1000);
            worker.task_address = Some(task.address.clone());
            point.tasks.push(task);
            point.requests.push(request);
        }
    })
    .await;
    let diagnosis = project(&[&history, &original, &result], vec![]).unwrap();
    diagnosis.validate().unwrap();
    let incident = &diagnosis.incidents[0];
    assert!(
        incident.affected_queries.len() + incident.culprit_queries.len() + incident.candidate_queries.len()
            <= MAX_DERIVED_QUERIES
    );
    assert!(incident.entry.reasons.contains(&t!(
        "view.worker_starvation.presentation_limited",
        count = culprit_core::format::digits(MAX_PRESENTED_WORKER_ACTORS.to_string())
    )));
    let raw = result.verified_original().unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlWorkerStarvation(point),
        ..
    }) = &raw.records()[0].row().record
    else {
        panic!()
    };
    assert_eq!(point.tasks.len(), 25);
    assert_eq!(point.requests.len(), 24);
}

#[tokio::test]
async fn a_parallel_tasks_foreign_blocker_cannot_be_rebound_to_the_request_coordinators_owner() {
    let (original, result, history) = answered(false, 870_051, |point| {
        point.tasks[1].wait.as_mut().unwrap().blocking_session_id = Some(64);
        let other = point.tasks[4].address.clone();
        point.tasks[1].wait.as_mut().unwrap().blocking_task_address = Some(other);
    })
    .await;
    let diagnosis = project(&[&history, &original, &result], vec![]).unwrap();
    diagnosis.validate().unwrap();
    assert!(!diagnosis.incidents[0].culprit_queries.iter().any(|query| {
        query.causal_role.as_ref().is_some_and(|role| role.kind == DerivedCausalRoleKind::NativeBlockingOwner)
    }));
    assert!(diagnosis.incidents[0].culprit_queries.iter().any(|query| {
        query.causal_role.as_ref().is_some_and(|role| role.kind == DerivedCausalRoleKind::AssignedWorkerContributor)
    }));
}

#[tokio::test]
async fn unavailable_diagnostic_worker_preserves_starvation_episode_without_false_recovery_or_culprit() {
    for (idx, (shared, issue, expected_reason)) in [
        (false, DiagnosticAnalysisIssue::BudgetExhausted, "budget"),
        (
            false,
            DiagnosticAnalysisIssue::SourceUnavailable {
                code: Some(ErrorCode::Timeout),
                stopped: Some(CollectionStop::TimedOut),
            },
            "source",
        ),
        (true, DiagnosticAnalysisIssue::EvidenceUnavailable, "evidence"),
    ]
    .into_iter()
    .enumerate()
    {
        let seq = 870_100 + idx as u64 * 10;
        let (original, episode) = basis(shared, seq).await;
        let focus = episode.source_read_focus(SourceReadQuestion::WorkerStarvation).unwrap().unwrap();
        let stopped = episode
            .enroll_source_read(focus.clone())
            .unwrap()
            .transition_source_read(&focus, SourceReadStageState::Collecting)
            .unwrap()
            .transition_source_read(&focus, SourceReadStageState::Stopped { issue })
            .unwrap();
        let rev = history(stopped, seq + 1).await;
        let diagnosis = project(&[&rev, &original], vec![]).unwrap();
        diagnosis.validate().unwrap();
        let [incident] = diagnosis.incidents.as_slice() else { panic!() };
        assert_eq!(
            incident.entry.lifecycle,
            if shared { DiagnosticLifecycle::Uncertain } else { DiagnosticLifecycle::Active }
        );
        assert_eq!(incident.affected_queries.len(), usize::from(!shared));
        assert!(incident.culprit_queries.is_empty() && incident.candidate_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(incident.entry.reasons.contains(&t!("view.incident_history.source_stage", state = "stopped")));
        assert!(incident.entry.reasons.contains(&t!("view.worker_starvation.question")));
        assert!(incident.entry.reasons.contains(&t!("view.incident_history.issue", reason = expected_reason)));
    }
}

#[tokio::test]
async fn competing_blocking_parallel_and_preemptive_mechanisms_and_resolved_recovery_hold_in_gold() {
    // 1. Pure lock-waiting worker occupation: NativeBlockingOwner only, no AssignedWorkerContributor.
    let (b_orig, b_res, b_hist) = answered(false, 870_200, |point| {
        point.tasks.retain(|t| matches!(t.session_id, Some(61 | 62)));
        let db = point.requests[0].observation.database.clone();
        for r in &mut point.requests {
            if r.observation.key.session.id == "63" {
                r.observation.scope = ActivityScopeBasis::RequestDatabase;
                r.observation.database = db.clone();
            }
        }
    })
    .await;
    let b_gold = project(&[&b_hist, &b_orig, &b_res], vec![]).unwrap();
    b_gold.validate().unwrap();
    let b_inc = &b_gold.incidents[0];
    assert!(
        b_inc
            .culprit_queries
            .iter()
            .all(|q| { q.causal_role.as_ref().is_some_and(|r| r.kind == DerivedCausalRoleKind::NativeBlockingOwner) })
    );
    assert!(!b_inc.culprit_queries.is_empty());
    assert!(b_inc.mechanism.as_ref().unwrap().contains(&t!(
        "view.worker_starvation.mechanism",
        kind = "blocked",
        count = "1"
    )));

    // 2. Pure parallel worker occupation: AssignedWorkerContributor only, task-vs-DOP disclaimer explicit.
    let (p_orig, p_res, p_hist) = answered(false, 870_210, |point| {
        point.tasks.retain(|t| t.session_id != Some(62));
        point.requests.retain(|r| r.observation.key.session.id != "62");
        for t in &mut point.tasks {
            if let Some(w) = &mut t.worker {
                w.is_preemptive = Some(false);
            }
        }
    })
    .await;
    let p_gold = project(&[&p_hist, &p_orig, &p_res], vec![]).unwrap();
    p_gold.validate().unwrap();
    let p_inc = &p_gold.incidents[0];
    assert!(p_inc.culprit_queries.iter().all(|q| {
        q.causal_role.as_ref().is_some_and(|r| r.kind == DerivedCausalRoleKind::AssignedWorkerContributor)
    }));
    assert!(!p_inc.culprit_queries.is_empty());
    assert!(p_inc.mechanism.as_ref().unwrap().contains(&t!(
        "view.worker_starvation.mechanism",
        kind = "parallel",
        count = "2"
    )));
    assert!(p_inc.entry.reasons.contains(&t!("view.worker_starvation.units")));

    // 3. Pure preemptive external wait occupation: AssignedWorkerContributor with "preemptive" mechanism.
    let (e_orig, e_res, e_hist) = answered(false, 870_220, |point| {
        point.tasks.retain(|t| t.session_id != Some(62));
        point.requests.retain(|r| r.observation.key.session.id != "62");
        for t in &mut point.tasks {
            if t.session_id == Some(63) {
                t.execution_context_id = Some(0);
                if let Some(wait) = &mut t.wait {
                    wait.execution_context_id = Some(0);
                }
                if let Some(w) = &mut t.worker {
                    w.is_preemptive = Some(true);
                }
            }
        }
        // Keep unique (session_id, request_id, execution_context_id) by retaining one preemptive task for session 63.
        let mut seen = false;
        point.tasks.retain(|t| {
            if t.session_id == Some(63) {
                if seen {
                    return false;
                }
                seen = true;
            }
            true
        });
    })
    .await;
    let e_gold = project(&[&e_hist, &e_orig, &e_res], vec![]).unwrap();
    e_gold.validate().unwrap();
    let e_inc = &e_gold.incidents[0];
    assert!(e_inc.mechanism.as_ref().unwrap().contains(&t!(
        "view.worker_starvation.mechanism",
        kind = "preemptive",
        count = "1"
    )));
    assert!(e_inc.culprit_queries.iter().any(|q| {
        q.causal_role.as_ref().is_some_and(|r| r.kind == DerivedCausalRoleKind::AssignedWorkerContributor)
    }));

    // 4. Resolved lifecycle retains the answered worker starvation culprits, mechanism, and caller.
    let raw_hist = p_hist.verified_original().unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::DiagnosticRevision(active_rev),
        ..
    }) = &raw_hist.records()[0].row().record
    else {
        panic!()
    };
    let mut empty_act = p_orig.verified_original().unwrap().source().clone();
    let SourceObservation::Activity(header) = &mut empty_act.observation else { panic!() };
    header.at = "2020-01-01T12:00:30Z".into();
    header.observed_from = header.at.clone();
    header.observed_until = header.at.clone();
    header.observations = 0;
    header.native_rows = 0;
    header.coverage = StateSourceCoverage::collected(empty_act.source.clone(), 0);
    let recovered = retained(empty_act, vec![], 870_230).await;
    let rec_orig = recovered.verified_original().unwrap();
    let prev = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: active_rev.episode.scope.target.clone(),
        input: active_rev.episode.latest.capture_ref(),
        episodes: vec![active_rev.episode.clone()],
    };
    let resolved_rev = observe_diagnostic_activity(
        rec_orig.source(),
        rec_orig.original(),
        &[],
        Some(&prev),
        &ActivityPolicy::default(),
        "2020-01-01T12:00:31Z",
    )
    .unwrap()
    .revisions
    .into_iter()
    .find(|r| r.episode.lifecycle == DiagnosticLifecycle::Resolved)
    .unwrap();
    drop(raw_hist);
    drop(rec_orig);
    let res_hist = history_revision(resolved_rev, 870_231).await;
    let res_gold = project(&[&res_hist, &p_orig, &p_res, &recovered], vec![]).unwrap();
    res_gold.validate().unwrap();
    let [res_inc] = res_gold.incidents.as_slice() else { panic!() };
    assert_eq!(res_inc.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(res_inc.culprit_queries, p_inc.culprit_queries);
    assert_eq!(res_inc.mechanism, p_inc.mechanism);
}
