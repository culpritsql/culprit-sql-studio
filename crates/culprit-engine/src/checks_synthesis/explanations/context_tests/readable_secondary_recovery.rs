use super::*;
use culprit_evidence::analytical::{
    evidence::EvidenceGapReason,
    test_support::{
        ReadableSecondaryCase as Case, readable_secondary_answer, readable_secondary_original,
        readable_secondary_result,
    },
};

async fn scenario(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode) {
    let input = readable_secondary_original(case.engine());
    let basis = retained(input.source, vec![input.row.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let TelemetryRecord::Activity(actor) = &basis.records()[0].row().record else { panic!() };
    let update = observe_diagnostic_activity(
        basis.source(),
        original.original(),
        &[DiagnosticActivityRow { reference: &reference, observation: actor }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:01Z",
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .iter()
        .find(|episode| {
            matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
        })
        .unwrap()
        .clone();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::ReadableSecondaryRecovery,
        &episode,
        basis.source(),
        &reference,
        basis.records()[0].row(),
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
            .unwrap();
    let (source, rows) = readable_secondary_result(&focus, &execution, case);
    let result = retained(source, rows, sequence + 1).await;
    let current = result.verified_original().unwrap();
    let references = (0..result.records().len()).map(|i| current.reference(i).unwrap()).collect::<Vec<_>>();
    let rows =
        references.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = ReadableSecondaryRecoveryAnswer::from_originals(
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
                answer: Box::new(SourceReadAnswer::ReadableSecondaryRecovery(Box::new(answer))),
            },
        )
        .unwrap();
    (basis, result, episode)
}
#[tokio::test]
async fn native_relation_and_redo_edges_retain_session_ownership_without_acquiring_sql_or_global_namespace() {
    for (index, case) in [Case::PostgresRelation, Case::MssqlRedo].into_iter().enumerate() {
        let (basis, result, episode) = scenario(case, 950_000 + index as u64 * 10).await;
        assert!(basis.source().target.database().unwrap().incarnation.is_none());
        let revisions = history(episode, 950_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1);
        assert_eq!(incident.culprit_queries.len(), 1);
        assert!(incident.culprit_queries[0].causal_role.is_some());
        assert!(!incident.culprit_queries[0].sql.is_available(), "current SQL is not the acquiring statement");
        assert!(matches!(incident.culprit_queries[0].identity, Some(DerivedQueryIdentity::NativeWaitOwner(_))));
        assert!(incident.mechanism.is_some());
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(output.findings.is_empty(), "a selected recovery context does not establish a general health finding");
    }
}
#[tokio::test]
async fn aggregate_conflicts_snapshot_wait_empty_limited_ended_and_reused_points_never_name_a_reader_as_culprit() {
    for (index, case) in [
        Case::PostgresSnapshotWait,
        Case::PostgresNoStartup,
        Case::PostgresPendingOnly,
        Case::PostgresLimited,
        Case::PostgresEnded,
        Case::PostgresReusedReader,
        Case::MssqlOtherBlocker,
        Case::MssqlOtherWait,
        Case::MssqlEnded,
        Case::MssqlLimited,
        Case::MssqlNoRedo,
        Case::Unavailable,
    ]
    .into_iter()
    .enumerate()
    {
        let (basis, result, episode) = scenario(case, 951_000 + index as u64 * 10).await;
        let revisions = history(episode, 951_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty(), "{case:?}");
        assert_ne!(incident.attribution_status, DerivedAttributionStatus::Established, "{case:?}");
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.mechanism.is_some());
    }
}
#[test]
fn exact_reset_counter_windows_keep_zero_large_values_and_every_comparability_gap_explicit() {
    for case in [
        Case::PostgresRelation,
        Case::PostgresZero,
        Case::PostgresUnknownReset,
        Case::PostgresResetChanged,
        Case::PostgresCounterDecreased,
        Case::PostgresUnknownStatsMode,
    ] {
        let (answer, point) = readable_secondary_answer(case);
        let point = point.unwrap();
        let report = culprit_checks::readable_secondary_recovery::explain(&answer, Some(&point)).unwrap();
        assert_eq!(report.conflict_changes.len(), PostgresRecoveryConflictKind::ALL.len());
        match case {
            Case::PostgresRelation=>assert!(report.conflict_changes.iter().all(|change|matches!(change,RecoveryConflictChange::Observed{before,after,change,..} if before=="9007199254740993"&&after=="9007199254740994"&&change=="1"))),
            Case::PostgresZero=>assert!(report.conflict_changes.iter().all(|change|matches!(change,RecoveryConflictChange::Observed{change,..}if change=="0"))),
            _=>assert!(report.conflict_changes.iter().all(|change|matches!(change,RecoveryConflictChange::Unavailable{..}))),
        }
        let mut foreign = point.clone();
        let SourcePartition::InstanceLocal { epoch, .. } = &mut foreign.context.partition else { panic!() };
        *epoch = Some("other-native-member-process".into());
        assert!(culprit_checks::readable_secondary_recovery::explain(&answer, Some(&foreign)).is_err());
    }
}
#[tokio::test]
async fn public_projection_withholds_native_authority_and_only_paid_exact_eof_recovery_restores_it() {
    for (index, case) in [Case::PostgresRelation, Case::MssqlRedo].into_iter().enumerate() {
        let (source, records) = culprit_evidence::analytical::test_support::readable_secondary_evidence(case);
        let fixture = Fixture::with_sequence(source, records, 952_000 + index as u64 * 10).await;
        let projection = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
        assert!(!projection.is_read_complete());
        assert!(projection.gaps().iter().all(|gap| matches!(
            gap.reason,
            EvidenceGapReason::DetailInRecovery | EvidenceGapReason::NativeRepresentationRequired
        )));
        let opens = fixture.recovery_object_reads();
        let short = fixture
            .try_read_families_with_recovery(
                &[EvidenceFamily::OperationalSamples],
                RecoveryPayloadBudget { max_bytes: 1, ..Default::default() },
            )
            .await;
        assert!(short.is_err());
        assert_eq!(fixture.recovery_object_reads(), opens);
        let full = fixture
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;
        assert!(full.is_read_complete());
        assert_eq!(full.records().len(), 1);
        assert!(fixture.recovery_object_reads() > opens);
    }
}

#[tokio::test]
async fn topology_failover_writable_promotion_and_member_mismatch_refuse_mixed_role_join_and_preserve_resolved_history()
{
    for (idx, case) in [Case::PostgresRelation, Case::MssqlRedo].into_iter().enumerate() {
        let seq = 953_000 + idx as u64 * 20;
        let (basis, result, episode) = scenario(case, seq).await;
        let focus = episode.source_reads.iter().next().unwrap().focus.clone();

        // Writable promotion / member-epoch failover on result must be refused by ReadableSecondaryRecoveryAnswer.
        let mut promoted_src = result.source().clone();
        if let SourceObservation::SourceRead { execution, .. } = &mut promoted_src.observation {
            execution.database_writable = Some(true);
        }
        let cur = result.verified_original().unwrap();
        let cur_ref = cur.reference(0).unwrap();
        assert!(
            ReadableSecondaryRecoveryAnswer::from_originals(
                &focus,
                basis.source(),
                basis.records()[0].row(),
                &promoted_src,
                cur.original(),
                &[(&cur_ref, result.records()[0].row())],
            )
            .is_err(),
            "writable promotion cannot attach primary evidence to a readable-secondary episode"
        );

        let mut failover_src = result.source().clone();
        if let SourceObservation::SourceRead { execution, .. } = &mut failover_src.observation
            && let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut execution.partition
        {
            member.native = NativeIdentity::new("promoted-other-replica");
        }
        assert!(
            ReadableSecondaryRecoveryAnswer::from_originals(
                &focus,
                basis.source(),
                basis.records()[0].row(),
                &failover_src,
                cur.original(),
                &[(&cur_ref, result.records()[0].row())],
            )
            .is_err(),
            "replica failover member change cannot satisfy the original secondary scope"
        );

        // CompleteScope recovery on the same secondary member resolves the incident while retaining the native culprit.
        let mut empty_src = basis.source().clone();
        let SourceObservation::Activity(frame) = &mut empty_src.observation else { panic!() };
        frame.at = "2020-01-01T12:05:00Z".into();
        frame.observed_from = frame.at.clone();
        frame.observed_until = frame.at.clone();
        frame.native_rows = 0;
        frame.observations = 0;
        frame.coverage = StateSourceCoverage::collected(empty_src.source.clone(), 0);
        let empty = retained(empty_src, vec![], seq + 3).await;
        let prev = DiagnosticCheckpoint {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            target: episode.scope.target.clone(),
            input: episode.latest.capture_ref(),
            episodes: vec![episode.clone()],
        };
        let update = observe_diagnostic_activity(
            empty.source(),
            empty.verified_original().unwrap().original(),
            &[],
            Some(&prev),
            &ActivityPolicy::default(),
            "2020-01-01T12:05:01Z",
        )
        .unwrap();
        let recovered = update.revisions.into_iter().find(|r| r.episode.id == episode.id).unwrap();
        assert_eq!(recovered.episode.lifecycle, DiagnosticLifecycle::Resolved);
        let active_hist = history(episode, seq + 4).await;
        let resolved_hist = history_revision(recovered, seq + 5).await;
        let out = project_at(&[&resolved_hist, &active_hist, &basis, &result, &empty], vec![], "2020-01-01T12:06:00Z")
            .unwrap();
        out.validate().unwrap();
        let [incident] = out.incidents.as_slice() else { panic!() };
        assert_eq!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Established);
        assert_eq!(incident.culprit_queries.len(), 1);
        assert!(!incident.culprit_queries[0].sql.is_available());
    }
}

#[tokio::test]
async fn long_running_secondary_load_without_startup_or_redo_wait_keeps_current_sql_candidate_only_without_culprit() {
    for (idx, case) in [Case::PostgresNoStartup, Case::MssqlNoRedo].into_iter().enumerate() {
        let seq = 953_100 + idx as u64 * 10;
        let (basis, result, episode) = scenario(case, seq).await;
        let revisions = history(episode, seq + 2).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        output.validate().unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
        assert_eq!(incident.candidate_queries.len(), 1);
        assert!(incident.entry.reasons.contains(&t!("view.secondary_recovery.limit.owner")));
    }
}
