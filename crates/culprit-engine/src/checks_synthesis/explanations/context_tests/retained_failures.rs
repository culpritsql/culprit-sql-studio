use super::*;
use culprit_evidence::analytical::test_support::{DiagnosticOriginalCase as Case, diagnostic_original_evidence};

fn observe_all(capture: &EvidenceCapture, at: &str) -> Vec<DiagnosticEpisode> {
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
        .revisions
        .into_iter()
        .map(|revision| revision.episode.finish_original_observation_planning().unwrap())
        .collect()
}

async fn history_many(episodes: Vec<DiagnosticEpisode>, sequence: u64) -> EvidenceCapture {
    assert!(!episodes.is_empty());
    let revisions = episodes
        .into_iter()
        .map(|episode| DiagnosticRevision::between(None, episode, "2020-01-01T12:00:20Z").unwrap().unwrap())
        .collect::<Vec<_>>();
    let first = &revisions[0];
    let parent = first.episode.latest.capture_ref();
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: first.episode.scope.target.clone(),
        input: parent.clone(),
        part: 0,
        parts: 1,
        episodes: revisions
            .iter()
            .map(|revision| revision.episode.clone())
            .filter(DiagnosticEpisode::retained_in_checkpoint)
            .collect(),
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    assert!(checkpoint.is_valid());
    let source = CollectionDescription {
        target: first.episode.scope.target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent,
            step_id: digest(b"retained-failures-gold-step"),
            analyzed_at: first.analyzed_at.clone(),
            records: revisions.len() as u32,
            checkpoint_records: 1,
            scope_bindings: [(first.episode.scope.key().unwrap(), first.episode.scope.clone())].into(),
        }),
    };
    let mut records = revisions
        .into_iter()
        .map(|revision| {
            TelemetryRecord::Operational(OperationalObservation {
                source: DIAGNOSTICS_SOURCE.into(),
                key: revision.id.clone(),
                fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
            })
        })
        .collect::<Vec<_>>();
    records.push(TelemetryRecord::DiagnosticCheckpoint(checkpoint));
    retained(source, records, sequence).await
}

#[tokio::test]
async fn retained_oom_originals_project_memory_failure_mechanisms_to_gold_without_inventing_culprit_queries() {
    let (source, records) = diagnostic_original_evidence(Case::Memory);
    let basis = retained(source, records, 940_001).await;
    let expected = crate::collected_resources::memory_failures(&basis).unwrap().report;
    let episodes = observe_all(&basis, "2020-01-01T12:00:02Z");
    assert_eq!(episodes.len(), 2);
    let revisions = history_many(episodes, 940_002).await;

    let output = project(&[&revisions, &basis], vec![]).unwrap();
    output.validate().unwrap();
    assert_eq!(output.incidents.len(), 2);
    for incident in &output.incidents {
        assert!(expected.entries.iter().any(|entry| {
            incident.victim_summary == entry.title
                && incident.mechanism.as_deref() == Some(entry.explanation.as_str())
                && incident.developer_action == entry.investigation
                && incident.verification == entry.verify
        }));
        assert!(incident.affected_queries.is_empty());
        assert!(incident.culprit_queries.is_empty());
        assert!(incident.candidate_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        for limitation in &expected.limitations {
            assert!(incident.entry.reasons.contains(limitation), "missing OOM limitation: {limitation}");
        }
    }
}

#[tokio::test]
async fn retained_native_failures_across_mssql_postgres_and_mysql_project_to_gold_without_false_timeout_or_ownership() {
    for (sequence, case, expected_count, has_candidates) in [
        (941_010u64, Case::MysqlError, 4usize, false),
        (941_020u64, Case::PostgresFailure, 2usize, true),
        (941_030u64, Case::MssqlExtendedEvent, 3usize, true),
        (941_040u64, Case::MssqlDatabaseExtendedEvent, 3usize, true),
    ] {
        let (source, records) = diagnostic_original_evidence(case);
        let basis = retained(source, records, sequence).await;
        let expected = crate::collected_resources::native_failures(&basis).unwrap().report;
        let episodes = observe_all(&basis, "2020-01-01T12:00:02Z");
        assert_eq!(episodes.len(), expected_count, "{case:?}");
        let revisions = history_many(episodes, sequence + 1).await;

        let output = project(&[&revisions, &basis], vec![]).unwrap();
        output.validate().unwrap();
        assert_eq!(output.incidents.len(), expected_count, "{case:?}");
        let serialized = serde_json::to_string(&output).unwrap();
        assert!(!serialized.contains("private-pg-log-canary"));
        assert!(!serialized.contains("private-xe-canary"));

        for incident in &output.incidents {
            assert!(
                expected.entries.iter().any(|entry| {
                    incident.victim_summary == entry.title
                        && incident.mechanism.as_deref() == Some(entry.explanation.as_str())
                        && incident.developer_action == entry.investigation
                        && incident.verification == entry.verify
                }),
                "{case:?}: {}",
                incident.victim_summary
            );
            assert!(incident.affected_queries.is_empty(), "{case:?}");
            assert!(incident.culprit_queries.is_empty(), "{case:?}");
            if has_candidates {
                assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly, "{case:?}");
                assert_eq!(incident.candidate_queries.len(), 1, "{case:?}");
                assert!(incident.candidate_queries[0].identity.is_none(), "no session generation is invented");
                assert!(incident.candidate_queries[0].causal_role.is_none());
                if matches!(case, Case::MssqlExtendedEvent | Case::MssqlDatabaseExtendedEvent) {
                    assert_eq!(
                        incident.candidate_queries[0]
                            .caller
                            .as_ref()
                            .and_then(|caller| caller.client.as_ref())
                            .and_then(|client| client.application.as_deref()),
                        Some("native-client-label")
                    );
                }
            } else {
                assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished, "{case:?}");
                assert!(incident.candidate_queries.is_empty(), "{case:?}");
            }
            for limitation in &expected.limitations {
                assert!(incident.entry.reasons.contains(limitation), "{case:?}: missing {limitation}");
            }
        }
        if matches!(case, Case::MssqlExtendedEvent | Case::MssqlDatabaseExtendedEvent) {
            assert!(
                output
                    .incidents
                    .iter()
                    .any(|incident| incident.victim_summary == t!("native_logs.mssql.attention.title")
                        && incident.mechanism.as_deref() == Some(&t!("native_logs.mssql.attention.explanation")))
            );
        }
    }

    let (routine_source, routine_records) = diagnostic_original_evidence(Case::MssqlRoutineError);
    let routine_basis = retained(routine_source, routine_records, 941_090).await;
    let routine_report = crate::collected_resources::native_failures(&routine_basis).unwrap().report;
    assert_eq!(routine_report.entries.len(), 2, "ordinary handled errors remain readable in retained diagnostics");
    assert!(observe_all(&routine_basis, "2020-01-01T12:00:02Z").is_empty(), "routine errors do not open incidents");
}
