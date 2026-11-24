//! Owned disposable PostgreSQL 18 Subtransaction / MultiXact metadata contention & competing-retainer
//! interventions through real `PostgresProvider` (`Activity` + `SourceRead(TransactionMetadata)`) and canonical
//! Gold synthesis:
//! 1. Arm 1 (`Subtransaction` wait): real `PREPARE TRANSACTION` holds the oldest `CleanupHorizon::Data` XID,
//!    a logical slot holds `catalog_xmin` (`Catalog` only), a newer `REPEATABLE READ` session holds `backend_xmin`,
//!    and a 66-savepoint backend overflows `pg_stat_get_backend_subxact` -> Gold reports `CandidatesOnly` for the
//!    overflowing backend, records `pg_prepared_xacts` as the oldest `Data` retainer, and never blames the newer
//!    `REPEATABLE READ` session or claims ORM/exclusive cause.
//! 2. Arm 2 (`COMMIT PREPARED` intervention): releasing the prepared transaction shifts `oldest_observed(Data)`
//!    to the live `REPEATABLE READ` session -> Gold surfaces the session-only candidate (`!sql.is_available()`)
//!    with `view.transaction_metadata.xmin_session.action`.
//! 3. Arm 3 (`MultiXact` wait + real concurrent `FOR KEY SHARE` / `FOR SHARE` row locks): two authenticated
//!    same-reset `PostgresProvider` `TransactionMetadata` points bracket real MultiXact creation -> Gold surfaces
//!    `PostgresSlruChange::Observed` on `multixact_offset` / `multixact_member` with `view.transaction_metadata.multixact.{mechanism,action}`
//!    and zero culprits, then resolves cleanly after actor retirement (`records == 0`).
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
};
use culprit_core::Provider;
use culprit_postgres::{
    PostgresProvider,
    native_test_support::{
        self as owned, MULTIXACT_ROWS, TXMETA_OVERFLOW_APP, TXMETA_RETAINER_APP, TransactionMetadataInterventionFixture,
    },
};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;

struct Factory(PostgresProvider);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.telemetry_source(request)
    }
}

#[tokio::test]
#[ignore = "requires explicit nonce-owned PostgreSQL 18 fixture with max_prepared_transactions/logical slots and private safety receipt directory"]
async fn owned_pg18_subtrans_multixact_and_competing_retainer_interventions_reach_canonical_gold() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let holder = std::rc::Rc::new(tokio::sync::Mutex::new(
                Box::pin(TransactionMetadataInterventionFixture::open()).await,
            ));
            let inner = holder.clone();
            let joined = tokio::task::spawn_local(async move {
                let mut guard = inner.lock().await;
                prove(&mut guard).await;
            })
            .await;
            Box::pin(holder.lock().await.close()).await;
            if let Err(err) = joined {
                std::panic::resume_unwind(err.into_panic());
            }
        })
        .await;
}

async fn prove(fixture: &mut TransactionMetadataInterventionFixture) {
    let spec = fixture.specification.clone();
    let cancellation = CollectionCancellation::default();
    let mut captures = OwnedCaptures::new().unwrap();

    Box::pin(fixture.start_competing_retainers_and_overflow()).await;

    // Real baseline Activity capture through PostgresProvider + OwnedCaptures.
    let activity_req = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Activity,
        budget: CollectionBudget {
            max_queries: 10,
            statement_timeout_ms: 2_000,
            total_timeout_ms: 5_000,
            ..CollectionBudget::default()
        },
    };
    let activity_cap = Box::pin(capture_activity(&spec, &mut captures, activity_req.clone(), &cancellation)).await;
    Box::pin(captures.verify_retained(&activity_cap)).await.unwrap();

    let focus_obs = activity_cap
        .records
        .iter()
        .find_map(|r| match &r.record {
            TelemetryRecord::Activity(obs) if obs.key.session == fixture.focus_identity => Some((**obs).clone()),
            _ => None,
        })
        .expect("real Activity capture must observe focus actor");
    assert!(focus_obs.statement_identity.is_some());

    // Arm 1: Subtransaction wait with competing Prepared Transaction (oldest Data XID) + Logical Slot + newer REPEATABLE READ session + 66-savepoint overflow.
    let (sub_basis, sub_ep, sub_focus, sub_exec) = Box::pin(build_basis_episode(
        activity_cap.source().unwrap(),
        &focus_obs,
        TransactionMetadataWait::Subtransaction,
        "LWLock: SubtransSLRU",
        998_500,
    ))
    .await;
    let (arm1_desc, arm1_records, arm1_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &sub_focus, &sub_exec)).await;
    assert_eq!(arm1_records.len(), 1);
    let arm1_ret = retained(arm1_desc, arm1_records, 998_501).await;
    let arm1_gold = Box::pin(evaluate_single_point(&sub_basis, &arm1_ret, &sub_ep, &sub_focus, 998_502)).await;
    let [arm1_inc] = arm1_gold.incidents.as_slice() else { panic!() };
    assert!(arm1_inc.culprit_queries.is_empty());
    assert_eq!(arm1_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(
        arm1_inc.candidate_queries.iter().any(|c| {
            matches!(&c.identity, Some(DerivedQueryIdentity::Activity { actor, .. }) if actor.session == fixture.overflow_identity)
                && c.sql.is_available()
                && c.caller.as_ref().and_then(|cl| cl.client.as_ref()).and_then(|cl| cl.application.as_deref())
                    == Some(TXMETA_OVERFLOW_APP)
        }),
        "66-savepoint overflowing backend is retained as a candidate with SQL and caller context"
    );
    assert!(
        !arm1_inc.candidate_queries.iter().any(|c| {
            matches!(&c.identity, Some(DerivedQueryIdentity::Activity { actor, .. }) if actor.session == fixture.retainer_identity)
        }),
        "younger session retainer must not be blamed while pg_prepared_xacts holds the older Data horizon"
    );
    assert!(arm1_inc.entry.reasons.iter().any(|r| r.contains("pg_prepared_xacts")));
    assert!(!arm1_inc.entry.reasons.iter().any(|r| r.contains("pg_replication_slots")));

    // Arm 2 (Intervention A): COMMIT PREPARED -> oldest Data horizon shifts directly to live session `backend_xid`!
    Box::pin(fixture.commit_prepared()).await;
    let arm2_act = Box::pin(capture_activity(&spec, &mut captures, activity_req.clone(), &cancellation)).await;
    Box::pin(captures.verify_retained(&arm2_act)).await.unwrap();
    let arm2_focus_obs = arm2_act
        .records
        .iter()
        .find_map(|r| match &r.record {
            TelemetryRecord::Activity(obs) if obs.key.session == fixture.focus_identity => Some((**obs).clone()),
            _ => None,
        })
        .unwrap();
    let (arm2_basis, arm2_ep, arm2_focus, arm2_exec) = Box::pin(build_basis_episode(
        arm2_act.source().unwrap(),
        &arm2_focus_obs,
        TransactionMetadataWait::Subtransaction,
        "LWLock: SubtransSLRU",
        998_508,
    ))
    .await;
    let (arm2_desc, arm2_records, arm2_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &arm2_focus, &arm2_exec)).await;
    assert_eq!(arm2_records.len(), 1);
    let arm2_ret = retained(arm2_desc, arm2_records, 998_510).await;
    let arm2_gold = Box::pin(evaluate_single_point(&arm2_basis, &arm2_ret, &arm2_ep, &arm2_focus, 998_511)).await;
    let [arm2_inc] = arm2_gold.incidents.as_slice() else { panic!() };
    assert!(arm2_inc.culprit_queries.is_empty());
    assert_eq!(arm2_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(!arm2_inc.entry.reasons.iter().any(|r| r.contains("pg_prepared_xacts")));
    let retainer_cand = arm2_inc
        .candidate_queries
        .iter()
        .find(|c| {
            matches!(&c.identity, Some(DerivedQueryIdentity::Activity { actor, .. })
                if actor.session == fixture.retainer_identity && actor.request.is_none())
        })
        .expect("after COMMIT PREPARED, live session retainer becomes the oldest Data horizon candidate");
    assert!(!retainer_cand.sql.is_available(), "previous/current SQL is never invented as acquiring statement");
    assert_eq!(
        retainer_cand.caller.as_ref().and_then(|cl| cl.client.as_ref()).and_then(|cl| cl.application.as_deref()),
        Some(TXMETA_RETAINER_APP)
    );
    assert!(arm2_inc.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));

    // Arm 2b (Intervention A2): pure read-only REPEATABLE READ `backend_xmin` (`backend_xid IS NULL`) is the UNIQUE Data horizon!
    Box::pin(fixture.switch_retainer_to_repeatable_read_xmin()).await;
    let arm2b_act = Box::pin(capture_activity(&spec, &mut captures, activity_req, &cancellation)).await;
    Box::pin(captures.verify_retained(&arm2b_act)).await.unwrap();
    let focus_obs = arm2b_act
        .records
        .iter()
        .find_map(|r| match &r.record {
            TelemetryRecord::Activity(obs) if obs.key.session == fixture.focus_identity => Some((**obs).clone()),
            _ => None,
        })
        .unwrap();
    let (arm2b_basis, arm2b_ep, arm2b_focus, arm2b_exec) = Box::pin(build_basis_episode(
        arm2b_act.source().unwrap(),
        &focus_obs,
        TransactionMetadataWait::Subtransaction,
        "LWLock: SubtransSLRU",
        998_512,
    ))
    .await;
    let (arm2b_desc, arm2b_records, arm2b_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &arm2b_focus, &arm2b_exec)).await;
    assert_eq!(arm2b_records.len(), 1);
    let arm2b_ret = retained(arm2b_desc, arm2b_records, 998_514).await;
    let arm2b_gold = Box::pin(evaluate_single_point(&arm2b_basis, &arm2b_ret, &arm2b_ep, &arm2b_focus, 998_515)).await;
    let [arm2b_inc] = arm2b_gold.incidents.as_slice() else { panic!() };
    assert!(arm2b_inc.culprit_queries.is_empty());
    assert_eq!(arm2b_inc.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(
        arm2b_inc.candidate_queries.len(),
        1,
        "unique REPEATABLE READ backend_xmin retainer is the sole candidate"
    );
    assert!(arm2b_inc.entry.reasons.iter().any(|r| r.contains("backend_xmin")));
    assert!(arm2b_inc.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));
    assert!(
        arm2b_inc.candidate_queries.iter().any(|c| {
            matches!(&c.identity, Some(DerivedQueryIdentity::Activity { actor, .. })
                if actor.session == fixture.retainer_identity && actor.request.is_none())
                && !c.sql.is_available()
        }),
        "read-only REPEATABLE READ backend_xmin session is surfaced as a session-only candidate without invented SQL"
    );

    // Arm 3 (Intervention B): Clear retainer + slot, then bracket real concurrent FOR KEY SHARE / FOR SHARE MultiXact burst!
    Box::pin(fixture.clear_retainers_and_overflow()).await;
    let (mx_basis, mx_ep, mx_focus, mx_exec) = Box::pin(build_basis_episode(
        arm2b_act.source().unwrap(),
        &focus_obs,
        TransactionMetadataWait::MultiXact,
        "LWLock: MultiXactOffsetSLRU",
        998_520,
    ))
    .await;
    let (mx_before_desc, mx_before_records, mx_before_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &mx_focus, &mx_exec)).await;
    assert_eq!(mx_before_records.len(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    Box::pin(fixture.run_multixact_burst()).await;
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let (mx_after_desc, mx_after_records, mx_after_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &mx_focus, &mx_exec)).await;
    assert_eq!(mx_after_records.len(), 1);

    let mx_before_ret = retained(mx_before_desc, mx_before_records, 998_521).await;
    let mx_after_ret = retained(mx_after_desc, mx_after_records, 998_522).await;
    let (mx_answered_ep, mx_report, mx_gold) =
        Box::pin(evaluate_with_prior_slru(&mx_basis, &mx_before_ret, &mx_after_ret, &mx_ep, &mx_focus, 998_523)).await;

    let mx_positive_delta = mx_report
        .slru_changes
        .iter()
        .filter(|ch| {
            matches!(
                ch,
                PostgresSlruChange::Observed {
                    kind: PostgresSlruKind::MultiXactMember | PostgresSlruKind::MultiXactOffset,
                    ..
                }
            )
        })
        .flat_map(|ch| match ch {
            PostgresSlruChange::Observed { counters, .. } => counters.as_slice(),
            _ => &[],
        })
        .filter_map(|c| c.change.as_deref()?.parse::<u64>().ok())
        .sum::<u64>();
    assert!(
        mx_positive_delta > 0,
        "real concurrent FOR KEY SHARE / FOR SHARE burst must advance MultiXact SLRU counters: {:?}",
        mx_report.slru_changes
    );
    let [mx_inc] = mx_gold.incidents.as_slice() else { panic!() };
    assert!(mx_inc.culprit_queries.is_empty());
    assert!(
        !mx_inc.candidate_queries.iter().any(|c| {
            matches!(&c.identity, Some(DerivedQueryIdentity::Activity { actor, .. })
                if actor.session == fixture.retainer_identity || actor.session == fixture.overflow_identity)
        }),
        "retired retainer and overflow sessions must be absent in Arm 3"
    );
    assert!(mx_inc.candidate_queries.iter().all(|c| !c.sql.is_available()));
    assert!(mx_inc.mechanism.as_deref().unwrap().contains(&t!("view.transaction_metadata.multixact.mechanism")));
    assert!(mx_inc.developer_action.contains(&t!("view.transaction_metadata.multixact.action")));

    // Arm 4: Retire focus actor -> ended PostgresProvider TransactionMetadata capture (`records == 0`) + resolved Gold!
    Box::pin(fixture.retire_focus_and_confirm_absent()).await;
    let (ended_desc, ended_records, ended_queries) =
        Box::pin(read_transaction_metadata_source(&spec, &mx_focus, &mx_exec)).await;
    assert!(ended_records.is_empty(), "retired focus backend must produce an empty TransactionMetadata point");
    let SourceObservation::SourceRead { snapshot: ended_snap, .. } = &ended_desc.observation else { panic!() };
    assert_eq!(ended_snap.records, 0);

    let recovered_req = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Activity,
        budget: CollectionBudget {
            max_queries: 10,
            statement_timeout_ms: 2_000,
            total_timeout_ms: 5_000,
            ..CollectionBudget::default()
        },
    };
    let recovered_act = Box::pin(capture_activity(&spec, &mut captures, recovered_req, &cancellation)).await;
    Box::pin(captures.verify_retained(&recovered_act)).await.unwrap();
    let rec_ret = retained(
        recovered_act.source().unwrap().clone(),
        recovered_act.records.iter().map(|r| r.record.clone()).collect(),
        998_530,
    )
    .await;
    let rec_orig = rec_ret.verified_original().unwrap();
    let rec_refs = (0..rec_ret.records().len()).map(|i| rec_orig.reference(i).unwrap()).collect::<Vec<_>>();
    let rec_rows = rec_refs
        .iter()
        .zip(rec_ret.records())
        .map(|(r, row)| {
            let TelemetryRecord::Activity(obs) = &row.row().record else { panic!() };
            DiagnosticActivityRow { reference: r, observation: obs }
        })
        .collect::<Vec<_>>();
    let prev_ckpt = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: mx_answered_ep.scope.target.clone(),
        input: mx_answered_ep.latest.capture_ref(),
        episodes: vec![mx_answered_ep.clone()],
    };
    let resolved_update = observe_diagnostic_activity(
        rec_ret.source(),
        rec_orig.original(),
        &rec_rows,
        Some(&prev_ckpt),
        &ActivityPolicy::default(),
        &chrono::Utc::now().to_rfc3339(),
    )
    .unwrap();
    let resolved_rev = resolved_update
        .revisions
        .into_iter()
        .find(|r| r.episode.id == mx_answered_ep.id && r.episode.lifecycle == DiagnosticLifecycle::Resolved)
        .expect("complete post-retirement Activity resolves the MultiXact episode");
    let active_hist = history(mx_answered_ep, 998_531).await;
    let resolved_hist = history_revision(resolved_rev, 998_532).await;
    let resolved_gold = project_at(
        &[&resolved_hist, &active_hist, &mx_basis, &mx_before_ret, &mx_after_ret, &rec_ret],
        vec![],
        &chrono::Utc::now().to_rfc3339(),
    )
    .unwrap();
    resolved_gold.validate().unwrap();
    assert_eq!(resolved_gold.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert!(resolved_gold.incidents[0].culprit_queries.is_empty());

    owned::write_automatic_receipt(
        "transaction-metadata",
        "competing-retainers-and-multixact-interventions.json",
        &json!({
            "multixact_rows": MULTIXACT_ROWS,
            "multixact_positive_slru_counter_delta": mx_positive_delta,
            "source_queries": [arm1_queries, arm2_queries, arm2b_queries, mx_before_queries, mx_after_queries, ended_queries],
            "orm_or_exclusive_culprit_claimed": false,
            "native_cost_qualified": false,
        }),
    )
    .unwrap();
}

async fn capture_activity(
    spec: &culprit_core::ConnectionSpec,
    captures: &mut OwnedCaptures,
    request: CollectionRequest,
    cancellation: &CollectionCancellation,
) -> Box<Captured> {
    let spec = spec.clone();
    let cap = Box::new(
        Box::pin(captures.capture("owned-pg18-txmeta", request, cancellation, move |_| {
            Factory(PostgresProvider::new(&spec).unwrap())
        }))
        .await
        .unwrap(),
    );
    assert_eq!(cap.manifest.status, CollectionStatus::Complete);
    let metrics = cap.manifest.read_metrics.as_ref().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    cap
}

async fn read_transaction_metadata_source(
    spec: &culprit_core::ConnectionSpec,
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
) -> (CollectionDescription, Vec<TelemetryRecord>, u32) {
    let provider = PostgresProvider::new(spec).unwrap();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead {
            focus: Box::new(focus.clone()),
            execution: Box::new(execution.clone()),
        },
        budget: CollectionBudget::default(),
    };
    let mut source = provider.telemetry_source(&request).unwrap();
    let description = source.describe().await.unwrap();
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!("expected single bounded OperationalSamples page");
    };
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
    let queries = source.query_count();
    let metrics = source.read_metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    (description, records, queries)
}

async fn build_basis_episode(
    activity_source: &CollectionDescription,
    focus_obs: &ActivityObservation,
    mechanism: TransactionMetadataWait,
    native_wait: &str,
    seq: u64,
) -> (EvidenceCapture, DiagnosticEpisode, SourceReadFocus, SourceReadExecutionScope) {
    let mut basis_src = activity_source.clone();
    if let SourceObservation::Activity(frame) = &mut basis_src.observation {
        frame.native_rows = 1;
        frame.observations = 1;
        frame.coverage = StateSourceCoverage::collected(basis_src.source.clone(), 1);
    }
    let mut actor = focus_obs.clone();
    actor.wait = Some(ActivityWait {
        class: Some(mechanism.class()),
        native_type: Some(native_wait.into()),
        elapsed_ms: None,
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    let basis = retained(basis_src, vec![TelemetryRecord::Activity(Box::new(actor))], seq).await;
    let orig = basis.verified_original().unwrap();
    let first_ref = orig.reference(0).unwrap();
    let TelemetryRecord::Activity(obs) = &basis.records()[0].row().record else { panic!() };
    let update = observe_diagnostic_activity(
        basis.source(),
        orig.original(),
        &[DiagnosticActivityRow { reference: &first_ref, observation: obs }],
        None,
        &ActivityPolicy::default(),
        &basis.source().observation.activity().unwrap().at,
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|ep| matches!(ep.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
        .unwrap();
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
            .unwrap();
    (basis, episode, focus, execution)
}

async fn evaluate_single_point(
    basis: &EvidenceCapture,
    result: &EvidenceCapture,
    episode: &DiagnosticEpisode,
    focus: &SourceReadFocus,
    seq: u64,
) -> DerivedDiagnosis {
    let cur = result.verified_original().unwrap();
    let refs = (0..result.records().len()).map(|i| cur.reference(i).unwrap()).collect::<Vec<_>>();
    let pairs = refs.iter().zip(result.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        focus,
        basis.source(),
        basis.records()[0].row(),
        result.source(),
        cur.original(),
        &pairs,
    )
    .unwrap();
    let answered = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    let revisions = history(answered, seq).await;
    let out = project_at(&[&revisions, basis, result], vec![], &chrono::Utc::now().to_rfc3339()).unwrap();
    out.validate().unwrap();
    out
}

async fn evaluate_with_prior_slru(
    basis: &EvidenceCapture,
    before: &EvidenceCapture,
    after: &EvidenceCapture,
    episode: &DiagnosticEpisode,
    focus: &SourceReadFocus,
    seq: u64,
) -> (DiagnosticEpisode, TransactionMetadataReport, DerivedDiagnosis) {
    let before_orig = before.verified_original().unwrap();
    let after_orig = after.verified_original().unwrap();
    let after_ref = after_orig.reference(0).unwrap();
    let answer = TransactionMetadataAnswer::from_originals(
        focus,
        basis.source(),
        basis.records()[0].row(),
        after.source(),
        after_orig.original(),
        &[(&after_ref, after.records()[0].row())],
    )
    .unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PostgresTransactionMetadata(after_point),
        ..
    }) = &after.records()[0].row().record
    else {
        panic!()
    };
    let slru_originals = [
        culprit_checks::transaction_metadata::SlruOriginal {
            source: before.source(),
            reference: before_orig.reference(0).unwrap(),
            record: &before.records()[0].row().record,
            read_complete: before.is_read_complete(),
        },
        culprit_checks::transaction_metadata::SlruOriginal {
            source: after.source(),
            reference: after_ref.clone(),
            record: &after.records()[0].row().record,
            read_complete: after.is_read_complete(),
        },
    ];
    let report = culprit_checks::transaction_metadata::explain(&answer, Some(after_point), &slru_originals).unwrap();
    let answered = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    let revisions = history(answered.clone(), seq).await;
    let out = project_at(&[&revisions, basis, before, after], vec![], &chrono::Utc::now().to_rfc3339()).unwrap();
    out.validate().unwrap();
    (answered, report, out)
}
