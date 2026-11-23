use super::*;
use culprit_core::source_id;
use culprit_evidence::analytical::test_support::CleanupCase;

fn local_resource(
    (mut source, records): (CollectionDescription, Vec<TelemetryRecord>),
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    source.observation = SourceObservation::LocalResource {
        snapshot: source.observation.snapshot().unwrap().clone(),
        database_writable: true,
    };
    assert!(source.partition_is_consistent());
    (source, records)
}

fn observe_episode(capture: &EvidenceCapture, at: &str) -> DiagnosticEpisode {
    let original = capture.verified_original().unwrap();
    let references =
        (0..original.records().len()).map(|position| original.reference(position).unwrap()).collect::<Vec<_>>();
    let rows = original
        .records()
        .iter()
        .zip(&references)
        .map(|(row, reference)| DiagnosticOriginalRow { reference, record: &row.row().record })
        .collect::<Vec<_>>();
    observe_diagnostic_original(original.source(), original.original(), &rows, None, &[], at)
        .unwrap()
        .checkpoint
        .episodes
        .into_iter()
        .next()
        .unwrap()
        .finish_original_observation_planning()
        .unwrap()
}

#[tokio::test]
async fn cleanup_originals_project_shared_cleanup_report_to_gold_without_inventing_allocating_sql() {
    for (case, expected_context, expected_action) in [
        (
            CleanupCase::LocalLoggedGenerator,
            t!("view.cleanup.transaction.context.logged_versioning"),
            t!("view.cleanup.transaction.investigate_logged"),
        ),
        (
            CleanupCase::LocalSnapshotProtector,
            t!("view.cleanup.transaction.context.snapshot_zero_log"),
            t!("view.cleanup.transaction.investigate_snapshot"),
        ),
        (
            CleanupCase::LocalSnapshotZeroVersions,
            t!("view.cleanup.transaction.context.snapshot_zero_log"),
            t!("view.cleanup.transaction.investigate_snapshot"),
        ),
        (
            CleanupCase::LocalSnapshotUnknownVersions,
            t!("view.cleanup.transaction.context.snapshot_zero_log"),
            t!("view.cleanup.transaction.investigate_snapshot"),
        ),
    ] {
        let (source, records) = local_resource(test_support::cleanup_evidence(case));
        let basis = retained(source, records, 920_001).await;
        let expected_report = crate::collected_resources::cleanup(&basis).unwrap().report;
        let expected_entry = expected_report
            .entries
            .iter()
            .find(|entry| entry.reference.source == source_id::OPEN_TRANSACTIONS)
            .unwrap();
        let episode = observe_episode(&basis, "2020-01-01T12:00:02Z");
        let revisions = history(episode, 920_002).await;
        let output = project(&[&revisions, &basis], vec![]).unwrap();
        output.validate().unwrap();
        assert_eq!(output.incidents.len(), 1);
        let incident = &output.incidents[0];
        assert_eq!(incident.mechanism.as_deref(), Some(expected_entry.explanation.as_str()));
        assert!(incident.mechanism.as_deref().unwrap().contains(&expected_context), "{case:?}");
        assert_eq!(incident.developer_action, expected_entry.investigation);
        assert!(incident.developer_action.contains(&expected_action), "{case:?}");
        assert_eq!(incident.verification, expected_entry.verify);
        assert!(incident.affected_queries.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.candidate_queries.is_empty());
        assert!(incident.caller.is_none());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        for limitation in &expected_report.limitations {
            assert!(incident.entry.reasons.contains(limitation), "{case:?}: missing limitation {limitation}");
        }
    }
}

#[tokio::test]
async fn cleanup_original_retains_sibling_pvs_skip_mechanisms_and_resolved_history_across_recovery() {
    let (mut source, mut records) = local_resource(test_support::cleanup_evidence(CleanupCase::LocalSnapshotProtector));
    let (_, mut pvs_records) = test_support::cleanup_evidence(CleanupCase::PersistentVersions);
    let pvs_row = pvs_records
        .drain(..)
        .find(|row| {
            matches!(
                row,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlPersistentVersionStore(_),
                    ..
                })
            )
        })
        .unwrap();
    records.push(pvs_row);
    let SourceObservation::LocalResource { snapshot, .. } = &mut source.observation else { panic!() };
    snapshot.records = records.len() as u32;
    *snapshot.sources.iter_mut().find(|coverage| coverage.source == source_id::PERSISTENT_VERSION_STORE).unwrap() =
        StateSourceCoverage::collected(source_id::PERSISTENT_VERSION_STORE.into(), 1);

    let basis = retained(source, records, 921_001).await;
    let basis_original = basis.verified_original().unwrap();
    let pvs_position = basis_original
        .records()
        .iter()
        .position(|row| {
            matches!(
                &row.row().record,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlPersistentVersionStore(_),
                    ..
                })
            )
        })
        .unwrap();
    let pvs_reference = basis_original.reference(pvs_position).unwrap();
    let active_episode = observe_episode(&basis, "2020-01-01T12:00:02Z");
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().target.clone(),
        input: basis_original.original().clone(),
        episodes: vec![active_episode.clone()],
    };

    let (mut recovered_source, recovered_records) =
        local_resource(test_support::cleanup_evidence(CleanupCase::LocalRecoveredHorizon));
    let SourceObservation::LocalResource { snapshot, .. } = &mut recovered_source.observation else { panic!() };
    snapshot.observed_from = "2020-01-01T12:00:05Z".into();
    snapshot.observed_until = "2020-01-01T12:00:06Z".into();
    let recovered = retained(recovered_source, recovered_records, 921_002).await;
    let recovered_original = recovered.verified_original().unwrap();
    let recovered_refs = (0..recovered_original.records().len())
        .map(|position| recovered_original.reference(position).unwrap())
        .collect::<Vec<_>>();
    let recovered_rows = recovered_original
        .records()
        .iter()
        .zip(&recovered_refs)
        .map(|(row, reference)| DiagnosticOriginalRow { reference, record: &row.row().record })
        .collect::<Vec<_>>();
    let resolved_update = observe_diagnostic_original(
        recovered_original.source(),
        recovered_original.original(),
        &recovered_rows,
        Some(&checkpoint),
        &[],
        "2020-01-01T12:00:07Z",
    )
    .unwrap();
    assert_eq!(resolved_update.revisions.len(), 1);
    let resolved_revision = resolved_update.revisions.into_iter().next().unwrap();
    assert_eq!(resolved_revision.episode.lifecycle, DiagnosticLifecycle::Resolved);
    let revisions = history_revision(resolved_revision, 921_003).await;

    let output = project(&[&revisions, &basis, &recovered], vec![]).unwrap();
    output.validate().unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
    let mechanism = incident.mechanism.as_deref().unwrap();
    assert!(mechanism.contains(&t!("view.cleanup.transaction.context.snapshot_zero_log")));
    assert!(mechanism.contains(&t!("view.cleanup.pvs.off_row")));
    assert!(incident.developer_action.contains(&t!("view.cleanup.transaction.investigate_snapshot")));
    assert!(incident.verification.contains(&t!("view.cleanup.transaction.verify")));
    assert!(incident.verification.contains(&t!("view.cleanup.pvs.verify")));
    assert!(incident.entry.evidence.iter().any(|evidence| evidence.reference.as_ref() == Some(&pvs_reference)));
    assert!(incident.culprit_queries.is_empty());
    assert!(incident.affected_queries.is_empty());

    let missing = project(&[&revisions], vec![DerivedContextIssue::MissingOriginal]).unwrap();
    assert_eq!(missing.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert!(missing.incidents[0].mechanism.is_none());
    assert!(missing.incidents[0].entry.reasons.contains(&DerivedContextIssue::MissingOriginal.explanation()));
}

#[tokio::test]
async fn cleanup_original_combined_with_task_space_followup_retains_cleanup_mechanism_and_task_candidates() {
    let (basis, result, episode, answer) = temporary(SourceState::Collected).await;
    let revisions = history(answered(episode, answer), 922_001).await;
    let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
    output.validate().unwrap();
    let incident = &output.incidents[0];
    assert!(
        incident
            .mechanism
            .as_deref()
            .is_some_and(|text| text.contains(&t!("view.cleanup.transaction.context.snapshot_zero_log")))
    );
    assert!(incident.developer_action.contains(&t!("view.cleanup.transaction.investigate_snapshot")));
    assert_eq!(incident.verification, t!("view.cleanup.transaction.verify"));
    assert_eq!(incident.candidate_queries.len(), 2);
    assert!(incident.culprit_queries.is_empty());
    assert!(incident.affected_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert!(incident.entry.reasons.contains(&t!("view.cleanup.limit.ownership")));
}

#[tokio::test]
async fn cleanup_original_observation_refuses_non_horizon_record_positions_and_unqualified_local_owners() {
    let (source, records) = local_resource(test_support::cleanup_evidence(CleanupCase::LocalLoggedGenerator));
    let basis = retained(source.clone(), records.clone(), 923_001).await;
    let view = basis.verified_original().unwrap();
    let version_store_position = view
        .records()
        .iter()
        .position(|row| {
            matches!(&row.row().record, TelemetryRecord::Operational(point) if point.source == source_id::VERSION_STORE)
        })
        .unwrap();
    assert!(original_observation(&view, version_store_position).is_err());
    assert!(original_observation(&view, view.records().len()).is_err());
}
