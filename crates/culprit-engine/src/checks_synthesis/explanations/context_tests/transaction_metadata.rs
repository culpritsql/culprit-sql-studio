use super::*;
use culprit_evidence::analytical::test_support::{
    TransactionMetadataCase as Case, transaction_metadata_answer, transaction_metadata_original,
    transaction_metadata_result,
};
async fn scenario(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode) {
    let (basis, result, episode, _) = scenario_with_slru(case, sequence, None).await;
    (basis, result, episode)
}
enum InitialPoint {
    Ordinary,
    Retained(test_support::TransactionMetadataSlruCase),
    Continuation(test_support::TransactionMetadataContinuationCase),
    Xmin(test_support::TransactionMetadataXminCase),
}
async fn scenario_with_slru(
    case: Case,
    sequence: u64,
    slru: Option<test_support::TransactionMetadataSlruCase>,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, Option<EvidenceCapture>) {
    scenario_point(case, sequence, slru.map_or(InitialPoint::Ordinary, InitialPoint::Retained)).await
}
async fn scenario_point(
    case: Case,
    sequence: u64,
    selection: InitialPoint,
) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode, Option<EvidenceCapture>) {
    let mechanism = if case == Case::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    };
    let input = transaction_metadata_original(mechanism);
    let basis = retained(input.source, vec![input.original.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let TelemetryRecord::Activity(actor) = &basis.records()[0].row().record else { panic!() };
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(basis.source()).unwrap(),
        DiagnosticSignal::TransactionMetadataWait {
            key: actor.key.clone(),
            statement_identity: actor.statement_identity.clone(),
            mechanism,
        },
        basis.source().observation.activity().unwrap().at.clone(),
        reference,
    )
    .unwrap();
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let mut execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
            .unwrap();
    if case == Case::Standby {
        let SourceReadExecutionScope::Local(scope) = &mut execution else { panic!() };
        scope.database_writable = false;
    }
    let (source, rows, context) = match selection {
        InitialPoint::Retained(case) => {
            let [before, (source, rows)] = test_support::transaction_metadata_slru_pair(&focus, &execution, case);
            (source, rows, Some(before))
        }
        InitialPoint::Continuation(case) => {
            let (source, rows) = test_support::transaction_metadata_continuation_result(
                &focus,
                &execution,
                case,
                test_support::TransactionMetadataObservation::Initial,
            );
            (source, rows, None)
        }
        InitialPoint::Xmin(case) => {
            let (source, rows) = test_support::transaction_metadata_xmin_result(&focus, &execution, case);
            (source, rows, None)
        }
        InitialPoint::Ordinary => {
            let (source, rows) = transaction_metadata_result(&focus, &execution, case);
            (source, rows, None)
        }
    };
    let result = retained(source, rows, sequence + 1).await;
    let context =
        if let Some((source, rows)) = context { Some(retained(source, rows, sequence + 2).await) } else { None };
    let current = result.verified_original().unwrap();
    let references =
        (0..result.records().len()).map(|position| current.reference(position).unwrap()).collect::<Vec<_>>();
    let rows =
        references.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        &focus,
        basis.source(),
        basis.records()[0].row(),
        result.source(),
        current.original(),
        &rows,
    )
    .unwrap();
    let episode = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    (basis, result, episode, context)
}
#[tokio::test]
async fn direct_cache_and_multixact_context_keeps_affected_candidate_callers_and_exact_xmin_source_without_any_culprit()
{
    for (case, sequence) in [
        (Case::Overflow, 910_000),
        (Case::ReducedCacheOverflow, 910_010),
        (Case::MultiXact, 910_020),
        (Case::Standby, 910_030),
    ] {
        let (basis, result, episode) = scenario(case, sequence).await;
        let revisions = history(episode, sequence + 2).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.caller.is_some());
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(
            incident.attribution_status,
            if case == Case::Standby {
                DerivedAttributionStatus::Unestablished
            } else {
                DerivedAttributionStatus::CandidatesOnly
            }
        );
        for candidate in &incident.candidate_queries {
            assert!(candidate.causal_role.is_none());
            assert!(candidate.caller.is_some());
            assert!(candidate.sql.is_available());
            assert_ne!(
                candidate.identity, incident.affected_queries[0].identity,
                "a distinct overflowing backend remains a candidate, never a victim relabeled as culprit"
            );
            assert!(!serde_json::to_string(candidate).unwrap().contains("transaction-private-canary"));
        }
        assert!(incident.mechanism.as_ref().is_some_and(|words| !words.is_empty()));
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(
            incident
                .entry
                .reasons
                .iter()
                .any(|reason| reason.contains("pg_replication_slots") && reason.contains("1234")),
            "the exact observed xmin source is retained without a global pin claim"
        );
        assert!(output.findings.is_empty(), "point context is not an integrity or exclusive-cause finding");
    }
}
#[tokio::test]
async fn native_false_null_partial_changed_and_empty_context_are_never_a_healthy_or_unsupported_causal_answer() {
    for (index, case) in
        [Case::Clear, Case::UnknownCache, Case::Partial, Case::ChangedRequest, Case::Empty, Case::Unavailable]
            .into_iter()
            .enumerate()
    {
        let (basis, result, episode) = scenario(case, 911_000 + index as u64 * 10).await;
        let revisions = history(episode, 911_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.entry.reasons.is_empty());
        if matches!(case, Case::Clear | Case::UnknownCache | Case::Empty | Case::Unavailable) {
            assert!(incident.candidate_queries.is_empty());
            assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        }
        assert!(incident.mechanism.is_some());
        if matches!(case, Case::Empty | Case::Unavailable) {
            assert_eq!(result.records().len(), 0, "no dummy row represents an empty source capture");
        }
        if matches!(case, Case::Partial | Case::UnknownCache | Case::ChangedRequest) {
            assert_eq!(result.source().observation.snapshot().unwrap().coverage_state().unwrap(), SourceState::Partial);
        }
    }
}
#[test]
fn shared_pure_view_preserves_independent_slru_reset_and_observation_clocks_and_refuses_edited_authority() {
    let (answer, point) = transaction_metadata_answer(Case::Overflow);
    let point = point.unwrap();
    let report = culprit_checks::transaction_metadata::explain(&answer, Some(&point), &[]).unwrap();
    assert_eq!(report.observed_overflows.len(), 1);
    assert_eq!(report.observed_xmin_sources.len(), 1);
    let mut later = point.clone();
    later.slrus[0].observed_at = "2020-01-01T12:00:03.500000Z".into();
    later.slrus[0].stats_reset = Some("2020-01-01T12:00:03.600000Z".into());
    let report = culprit_checks::transaction_metadata::explain(&answer, Some(&later), &[]).unwrap();
    assert!(
        report.limitations.len()
            > culprit_checks::transaction_metadata::explain(&answer, Some(&point), &[]).unwrap().limitations.len()
    );
    let mut outside = point.clone();
    outside.slrus[0].observed_at = "2020-01-01T12:00:05Z".into();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&outside), &[]).is_err());
    let mut edited = answer.clone();
    edited.overflowed_backends.clear();
    assert!(culprit_checks::transaction_metadata::explain(&edited, Some(&point), &[]).is_err());
    let mut edited = point.clone();
    edited.database = "foreign".into();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited), &[]).is_err());
    let mut edited = point.clone();
    edited.caches[1].actor.key.session = edited.caches[0].actor.key.session.clone();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited), &[]).is_err());
    let mut edited = point.clone();
    edited.slrus[0].blocks_hit = Some("1.0".into());
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited), &[]).is_err());
    assert!(
        culprit_checks::transaction_metadata::explain(&answer, None, &[]).unwrap().observed_overflows.is_empty(),
        "missing authenticated context cannot reuse compact counts as query evidence"
    );
}
#[test]
fn required_nullable_cache_and_slru_fields_roundtrip_null_without_a_compatibility_default() {
    let (_, point) = transaction_metadata_answer(Case::UnknownCache);
    let point = point.unwrap();
    let mut json = serde_json::to_value(&point).unwrap();
    assert!(json["caches"][0]["overflowed"].is_null());
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json.clone()).unwrap().is_valid());
    json["caches"][0].as_object_mut().unwrap().remove("overflowed");
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json).is_err());
    let mut json = serde_json::to_value(&point).unwrap();
    json["slrus"][0]["stats_reset"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json.clone()).unwrap().is_valid());
    json["slrus"][0].as_object_mut().unwrap().remove("stats_reset");
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json).is_err());
}

#[tokio::test]
async fn retained_slru_endpoints_reach_gold_with_exact_refs_without_importing_other_actors_xmin_or_culprits() {
    use test_support::TransactionMetadataSlruCase as Slru;
    for (index, case) in [Slru::Continued, Slru::LatePoint, Slru::Partial, Slru::Zero].into_iter().enumerate() {
        let sequence = 913_000 + index as u64 * 10;
        let (basis, result, episode, context) = scenario_with_slru(Case::Overflow, sequence, Some(case)).await;
        let context = context.unwrap();
        let old = context.verified_original().unwrap().reference(0).unwrap();
        let current = result.verified_original().unwrap().reference(0).unwrap();
        let revisions = history(episode, sequence + 3).await;
        let output = project(&[&revisions, &basis, &result, &context], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        assert!(incident.entry.evidence.iter().any(|row| row.reference.as_ref() == Some(&old)));
        assert!(incident.entry.evidence.iter().any(|row| row.reference.as_ref() == Some(&current)));
        assert!(incident.entry.reasons.iter().any(|words| words.contains("1234")));
        assert!(
            !incident.entry.reasons.iter().any(|words| words.contains("9999")),
            "another capture's xmin is not this answer's retention evidence"
        );
        assert!(incident.candidate_queries.iter().all(
            |query| !matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session.id=="49")
        ));
        assert!(output.description.inputs.iter().any(|input| input.capture == old.capture_ref()));
        assert!(output.findings.is_empty());
    }
}
#[tokio::test]
async fn missing_fields_foreign_reset_or_unordered_context_never_becomes_request_causality() {
    use test_support::TransactionMetadataSlruCase as Slru;
    for (index, case) in [
        Slru::UnknownReset,
        Slru::ChangedReset,
        Slru::ResetAfterPoint,
        Slru::ForeignMember,
        Slru::Restart,
        Slru::ChangedRole,
        Slru::Overlapping,
        Slru::MissingCounter,
        Slru::Decreased,
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = 914_000 + index as u64 * 10;
        let (basis, result, episode, context) = scenario_with_slru(Case::Overflow, sequence, Some(case)).await;
        let context = context.unwrap();
        let revisions = history(episode, sequence + 3).await;
        let output = project(&[&revisions, &basis, &result, &context], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.entry.reasons.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        assert!(incident.candidate_queries.iter().all(
            |query| !matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor.session.id=="49")
        ));
    }
}

mod continuation;

#[tokio::test]
async fn exact_native_xmin_sessions_reach_gold_as_session_only_candidates_with_conditional_lifetime_advice() {
    use test_support::TransactionMetadataXminCase as Xmin;
    for (index, case) in [Xmin::Session, Xmin::PreviousStatement, Xmin::Partial].into_iter().enumerate() {
        let (basis, result, episode, _) =
            scenario_point(Case::Clear, 945_000 + index as u64 * 10, InitialPoint::Xmin(case)).await;
        assert!(result.is_read_complete());
        let revisions = history(episode, 945_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        output.validate().unwrap();
        let incident = &output.incidents[0];
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        let candidate = incident.candidate_queries.iter().find(|candidate| {
            matches!(&candidate.identity,
            Some(DerivedQueryIdentity::Activity {actor,..}) if actor.session.id=="77")
        });
        let candidate = candidate.expect("qualified native xmin session is not lost after overflow-only projection");
        let Some(DerivedQueryIdentity::Activity { actor, statement }) = &candidate.identity else { panic!() };
        assert_eq!(actor.session.id, "77");
        assert!(actor.request.is_none() && actor.transaction.is_none() && statement.is_none());
        assert!(!candidate.sql.is_available(), "previous/current SQL and acquiring request are not invented");
        assert!(candidate.causal_role.is_none());
        let caller = candidate.caller.as_ref().unwrap();
        assert_eq!(caller.client.as_ref().unwrap().application.as_deref(), Some("native-retainer-application"));
        assert!(caller.session_context.is_none());
        let original = result.verified_original().unwrap();
        assert_eq!(candidate.evidence, vec![original.reference(0).unwrap()]);
        assert!(candidate.evidence.contains(&caller.evidence));
        assert!(incident.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));
        assert!(incident.verification.contains(&t!("view.transaction_metadata.xmin_session.verify")));
        assert!(output.findings.is_empty());
    }
}

#[tokio::test]
async fn unresolved_future_foreign_prepared_and_unqualified_retention_sources_remain_facts_without_a_query_or_primary_fix()
 {
    use test_support::TransactionMetadataXminCase as Xmin;
    for (index, case) in [
        Xmin::UnresolvedSession,
        Xmin::FutureGeneration,
        Xmin::ForeignServerSession,
        Xmin::PreparedTransaction,
        Xmin::UnqualifiedHorizon,
    ]
    .into_iter()
    .enumerate()
    {
        let (basis, result, episode, _) =
            scenario_point(Case::Clear, 945_100 + index as u64 * 10, InitialPoint::Xmin(case)).await;
        let revisions = history(episode, 945_102 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        output.validate().unwrap();
        let incident = &output.incidents[0];
        assert!(incident.candidate_queries.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(!incident.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));
        assert!(
            incident.entry.reasons.iter().any(|reason| reason.contains("backend_xmin")),
            "the original retainer fact is preserved"
        );
    }
}

#[tokio::test]
async fn a_reused_retainer_pid_keeps_its_own_backend_generation_and_never_borrows_the_old_cache_sql_or_caller() {
    let (basis, result, episode, _) =
        scenario_point(Case::Clear, 945_200, InitialPoint::Xmin(test_support::TransactionMetadataXminCase::ReusedPid))
            .await;
    let revisions = history(episode, 945_202).await;
    let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
    output.validate().unwrap();
    let incident = &output.incidents[0];
    let candidates = incident
        .candidate_queries
        .iter()
        .filter(|query| {
            matches!(&query.identity,
        Some(DerivedQueryIdentity::Activity {actor,..}) if actor.session.id=="43")
        })
        .collect::<Vec<_>>();
    assert_eq!(candidates.len(), 2);
    let retainer = candidates
        .iter()
        .find(|query| {
            matches!(&query.identity,
        Some(DerivedQueryIdentity::Activity {actor,..}) if actor.request.is_none())
        })
        .unwrap();
    let Some(DerivedQueryIdentity::Activity { actor, .. }) = &retainer.identity else { panic!() };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PostgresTransactionMetadata(point),
        ..
    }) = &result.records()[0].row().record
    else {
        panic!()
    };
    let native_session = point.retention.holders.iter().find_map(|holder| holder.session.as_ref()).unwrap();
    assert_eq!(actor.session, *native_session);
    assert!(!retainer.sql.is_available());
    assert_eq!(
        retainer.caller.as_ref().unwrap().client.as_ref().unwrap().application.as_deref(),
        Some("native-retainer-application")
    );
    assert!(candidates.iter().any(|query| query.sql.is_available()));
    assert!(incident.culprit_queries.is_empty());
}

#[tokio::test]
async fn live_wait_family_discrimination_separates_subtransaction_multixact_and_ordinary_transaction_locks_through_gold()
 {
    for (idx, (mechanism, native_wait, expected_mech, expected_action)) in [
        (
            TransactionMetadataWait::Subtransaction,
            "LWLock: SubtransBuffer",
            t!("view.transaction_metadata.subtransaction.mechanism"),
            t!("view.transaction_metadata.subtransaction.action"),
        ),
        (
            TransactionMetadataWait::MultiXact,
            "LWLock: MultiXactOffsetSLRU",
            t!("view.transaction_metadata.multixact.mechanism"),
            t!("view.transaction_metadata.multixact.action"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let seq = 945_300 + idx as u64 * 20;
        let mut input = transaction_metadata_original(mechanism);
        let TelemetryRecord::Activity(actor) = &mut input.original.record else { panic!() };
        actor.wait.as_mut().unwrap().native_type = Some(native_wait.into());
        let basis = retained(input.source, vec![input.original.record], seq).await;
        let orig = basis.verified_original().unwrap();
        let first_ref = orig.reference(0).unwrap();
        let TelemetryRecord::Activity(obs) = &basis.records()[0].row().record else { panic!() };
        let update = observe_diagnostic_activity(
            basis.source(),
            orig.original(),
            &[DiagnosticActivityRow { reference: &first_ref, observation: obs }],
            None,
            &ActivityPolicy::default(),
            "2020-01-01T12:00:02Z",
        )
        .unwrap();
        let episode = update
            .checkpoint
            .episodes
            .into_iter()
            .find(|ep| matches!(ep.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
            .expect("metadata LWLock wait starts a TransactionMetadataWait episode");
        assert!(matches!(
            episode.signal,
            DiagnosticSignal::TransactionMetadataWait { mechanism: actual, .. } if actual == mechanism
        ));
        let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
        let execution =
            SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
                .unwrap();
        let case = if mechanism == TransactionMetadataWait::MultiXact { Case::MultiXact } else { Case::Overflow };
        let (res_src, res_rows) = transaction_metadata_result(&focus, &execution, case);
        let result = retained(res_src, res_rows, seq + 1).await;
        let cur = result.verified_original().unwrap();
        let refs = (0..result.records().len()).map(|i| cur.reference(i).unwrap()).collect::<Vec<_>>();
        let pairs = refs.iter().zip(result.records()).map(|(r, row)| (r, row.row())).collect::<Vec<_>>();
        let answer = TransactionMetadataAnswer::from_originals(
            &focus,
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
            .transition_source_read(&focus, SourceReadStageState::Collecting)
            .unwrap()
            .transition_source_read(
                &focus,
                SourceReadStageState::Answered {
                    answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
                },
            )
            .unwrap();
        let revisions = history(answered, seq + 2).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        output.validate().unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert!(incident.mechanism.as_deref().unwrap().contains(&expected_mech));
        assert!(incident.developer_action.contains(&expected_action));
        assert!(incident.culprit_queries.is_empty());
    }

    // Competing ordinary transactionid / tuple / ProcArray waits never start a TransactionMetadataWait episode.
    for (idx, (native_wait, lock_wait)) in
        [("Lock: transactionid", true), ("Lock: tuple", true), ("LWLock: ProcArray", false), ("IO: SLRURead", false)]
            .into_iter()
            .enumerate()
    {
        let mut input = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
        let TelemetryRecord::Activity(actor) = &mut input.original.record else { panic!() };
        actor.wait = Some(ActivityWait {
            class: None,
            native_type: Some(native_wait.into()),
            elapsed_ms: Some("65000".into()),
            owners: vec![],
            resources: None,
            identified_resources: vec![],
        });
        actor.waiting_on_lock = Some(lock_wait);
        actor.blocking_status = ActivityBlockingStatus::Unknown;
        let basis = retained(input.source, vec![input.original.record], 945_400 + idx as u64 * 5).await;
        let orig = basis.verified_original().unwrap();
        let first_ref = orig.reference(0).unwrap();
        let TelemetryRecord::Activity(obs) = &basis.records()[0].row().record else { panic!() };
        let update = observe_diagnostic_activity(
            basis.source(),
            orig.original(),
            &[DiagnosticActivityRow { reference: &first_ref, observation: obs }],
            None,
            &ActivityPolicy::default(),
            "2020-01-01T12:00:02Z",
        )
        .unwrap();
        assert!(
            !update
                .checkpoint
                .episodes
                .iter()
                .any(|ep| matches!(ep.signal, DiagnosticSignal::TransactionMetadataWait { .. })),
            "competing {native_wait} must never start a TransactionMetadataWait episode"
        );
    }
}

#[tokio::test]
async fn competing_prepared_slot_and_xmin_session_retainers_select_the_actual_oldest_data_horizon_and_preserve_resolved_gold()
 {
    // 1. Prepared transaction holds older Data XID (distance 9000) than backend_xmin session 77 (distance 500)
    //    and replication slot (distance 400): Gold records pg_prepared_xacts without blaming session 77.
    // 2. Prepared transaction removed, replication slot holds catalog_xmin only (distance 9500, Catalog-only!)
    //    while session 77 holds backend_xmin (distance 500, Data): Gold selects session 77 as session-only
    //    candidate with xmin_session.action and preserves it after episode recovery.
    for (arm, seq) in [("prepared-oldest", 945_500_u64), ("session-oldest-despite-older-catalog-slot", 945_520_u64)] {
        let input = transaction_metadata_original(TransactionMetadataWait::Subtransaction);
        let basis = retained(input.source, vec![input.original.record], seq).await;
        let orig = basis.verified_original().unwrap();
        let first_ref = orig.reference(0).unwrap();
        let TelemetryRecord::Activity(actor) = &basis.records()[0].row().record else { panic!() };
        let episode = DiagnosticEpisode::start(
            DiagnosticScope::from_source(basis.source()).unwrap(),
            DiagnosticSignal::TransactionMetadataWait {
                key: actor.key.clone(),
                statement_identity: actor.statement_identity.clone(),
                mechanism: TransactionMetadataWait::Subtransaction,
            },
            basis.source().observation.activity().unwrap().at.clone(),
            first_ref,
        )
        .unwrap();
        let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
        let execution =
            SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
                .unwrap();
        let (res_src, mut res_rows) = test_support::transaction_metadata_xmin_result(
            &focus,
            &execution,
            test_support::TransactionMetadataXminCase::Session,
        );
        let TelemetryRecord::Operational(op_row) = &mut res_rows[0] else { panic!() };
        let OperationalFact::PostgresTransactionMetadata(point) = &mut op_row.fact else { panic!() };
        let observed_at = point.retention.holders[0].observed_at.clone();
        let db_name = point.database.clone();

        // Slot holder: in arm 1 holds a newer Data xmin (distance 400); in arm 2 holds only Catalog xmin (distance 9500).
        point.retention.holders[0].horizons = if arm == "prepared-oldest" {
            vec![RetentionHorizon {
                protects: vec![CleanupHorizon::Data, CleanupHorizon::Catalog, CleanupHorizon::SharedCatalog],
                native_name: "xmin".into(),
                native_value: "1600".into(),
                transactions_behind: Some("400".into()),
            }]
        } else {
            vec![RetentionHorizon {
                protects: vec![CleanupHorizon::Catalog, CleanupHorizon::SharedCatalog],
                native_name: "catalog_xmin".into(),
                native_value: "500".into(),
                transactions_behind: Some("9500".into()),
            }]
        };
        // Session 77 holder: Data backend_xmin (distance 500).
        point.retention.holders[1].horizons = vec![RetentionHorizon {
            protects: vec![CleanupHorizon::Data, CleanupHorizon::Catalog, CleanupHorizon::SharedCatalog],
            native_name: "backend_xmin".into(),
            native_value: "1500".into(),
            transactions_behind: Some("500".into()),
        }];

        if arm == "prepared-oldest" {
            point.retention.holders.push(RetentionObservation {
                source: "pg_prepared_xacts".into(),
                identity: NativeIdentity::new("pg_prepared_xacts").qualified("native_id", "1000"),
                observed_at,
                scope: culprit_core::MeasurementScope::Database,
                holder: culprit_core::CleanupHolder::PreparedTransaction,
                name: "1000".into(),
                horizons: vec![RetentionHorizon {
                    protects: vec![CleanupHorizon::Data, CleanupHorizon::Catalog, CleanupHorizon::SharedCatalog],
                    native_name: "transaction".into(),
                    native_value: "1000".into(),
                    transactions_behind: Some("9000".into()),
                }],
                session: None,
                unresolved_session_id: None,
                transaction: Some(RetentionTransactionContext {
                    native_id: Some("1000".into()),
                    started_at: None,
                    prepared_at: Some("2020-01-01T11:50:00Z".into()),
                }),
                query: None,
                client: Some(ActivityClient { application: None, user: Some("postgres".into()), host: None }),
                activity: culprit_core::TransactionActivity::Prepared,
                native_state: None,
                age_seconds: Some(600.0),
                database: Some(db_name),
                code_ownership: PayloadUnavailable::NotCollected,
            });
            for src in &mut point.retention.sources {
                if src.source == "pg_prepared_xacts" {
                    src.state = SourceState::Collected;
                }
            }
        }
        point.retainer_rows = point.retention.holders.len() as u32;
        op_row.key = point.key().unwrap();
        assert!(point.fits_source_scope(&res_src));

        let result = retained(res_src, res_rows, seq + 1).await;
        let cur = result.verified_original().unwrap();
        let cur_ref = cur.reference(0).unwrap();
        let answer = TransactionMetadataAnswer::from_originals(
            &focus,
            basis.source(),
            basis.records()[0].row(),
            result.source(),
            cur.original(),
            &[(&cur_ref, result.records()[0].row())],
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
                    answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
                },
            )
            .unwrap();
        let revisions = history(answered.clone(), seq + 2).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        output.validate().unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty());

        if arm == "prepared-oldest" {
            assert!(
                incident.candidate_queries.is_empty(),
                "newer session 77 must not be blamed when prepared xact is older"
            );
            assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
            assert!(!incident.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));
            assert!(incident.entry.reasons.iter().any(|r| r.contains("pg_prepared_xacts") && r.contains("1000")));
        } else {
            assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
            assert_eq!(incident.candidate_queries.len(), 1);
            assert!(matches!(
                &incident.candidate_queries[0].identity,
                Some(DerivedQueryIdentity::Activity { actor, .. }) if actor.session.id == "77" && actor.request.is_none()
            ));
            assert!(incident.developer_action.contains(&t!("view.transaction_metadata.xmin_session.action")));

            // Verify recovery preserves the session-only candidate and conditional lifetime action in Gold.
            let mut empty_src = basis.source().clone();
            let SourceObservation::Activity(frame) = &mut empty_src.observation else { panic!() };
            frame.at = "2020-01-01T12:03:00Z".into();
            frame.observed_from = frame.at.clone();
            frame.observed_until = frame.at.clone();
            frame.native_rows = 0;
            frame.observations = 0;
            frame.coverage = StateSourceCoverage::collected(empty_src.source.clone(), 0);
            let empty_cap = retained(empty_src, vec![], seq + 3).await;
            let prev = DiagnosticCheckpoint {
                controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
                target: answered.scope.target.clone(),
                input: answered.latest.capture_ref(),
                episodes: vec![answered],
            };
            let rec_update = observe_diagnostic_activity(
                empty_cap.source(),
                empty_cap.verified_original().unwrap().original(),
                &[],
                Some(&prev),
                &ActivityPolicy::default(),
                "2020-01-01T12:03:01Z",
            )
            .unwrap();
            let rec_rev = rec_update.revisions.into_iter().next().unwrap();
            assert_eq!(rec_rev.episode.lifecycle, DiagnosticLifecycle::Resolved);
            let rec_hist = history_revision(rec_rev, seq + 4).await;
            let resolved_out =
                project_at(&[&rec_hist, &revisions, &basis, &result, &empty_cap], vec![], "2020-01-01T12:04:00Z")
                    .unwrap();
            resolved_out.validate().unwrap();
            assert_eq!(resolved_out.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
            assert_eq!(resolved_out.incidents[0].candidate_queries.len(), 1);
            assert!(resolved_out.incidents[0].culprit_queries.is_empty());
        }
    }
}
