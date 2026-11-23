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
