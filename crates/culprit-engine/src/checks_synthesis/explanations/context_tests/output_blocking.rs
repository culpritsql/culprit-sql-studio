use super::*;
use culprit_core::blocking::{OutputBlockingDependency, OutputBlockingStatus};
use test_support::{OutputBlockingCaseFixture as Case, output_blocking_fixture};
async fn scenario(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, DiagnosticRevision) {
    let fixture = output_blocking_fixture(case);
    let basis = retained(fixture.original_source, vec![fixture.original.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let TelemetryRecord::Activity(victim) = &original.records()[0].row().record else { panic!() };
    let update = observe_diagnostic_activity(
        original.source(),
        original.original(),
        &[DiagnosticActivityRow { reference: &reference, observation: victim }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:00.5Z",
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::LockWait { .. }))
        .unwrap();
    let DiagnosticFocusPlan::Ready(focus) = episode.plan_focus(original.source(), victim, &reference).unwrap() else {
        panic!()
    };
    let DiagnosticFocus::Blocking { focus } = *focus else { panic!() };
    let mut source = fixture.source;
    let SourceObservation::FocusedBlocking { result, .. } = &mut source.observation else { panic!() };
    result.focus = *focus;
    drop(original);
    let captured = retained(source, fixture.records, sequence + 1).await;
    let output = captured.verified_original().unwrap();
    let references =
        (0..output.records().len()).map(|position| output.reference(position).unwrap()).collect::<Vec<_>>();
    let rows = references
        .iter()
        .zip(output.records())
        .map(|(reference, row)| {
            let TelemetryRecord::Activity(observation) = &row.row().record else { panic!() };
            DiagnosticActivityRow { reference, observation }
        })
        .collect::<Vec<_>>();
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().target.clone(),
        input: basis.verified_original().unwrap().original().clone(),
        episodes: vec![episode],
    };
    let answered = analyze_diagnostic_blocking(
        output.source(),
        output.original(),
        &rows,
        &checkpoint,
        "2020-01-01T12:00:10Z",
        None,
    )
    .unwrap()
    .revisions
    .into_iter()
    .find(|revision| matches!(revision.episode.signal, DiagnosticSignal::LockWait { .. }))
    .unwrap();
    drop(output);
    (basis, captured, answered)
}
#[tokio::test]
async fn canonical_output_blocking_keeps_native_session_culprits_and_contextual_sql_separate_for_both_native_contracts()
{
    for (index, case) in [Case::Mssql, Case::MissingSql, Case::PgRelation, Case::PgSessionOnly].into_iter().enumerate()
    {
        let (basis, result, revision) = scenario(case, 950_000 + index as u64 * 10).await;
        let rows = result
            .records()
            .iter()
            .map(|row| {
                let TelemetryRecord::Activity(observation) = &row.row().record else { panic!() };
                ActivityFrameRow { observation, reference: None }
            })
            .collect::<Vec<_>>();
        let report = culprit_checks::focused_blocking::explain(result.source(), &rows, true).unwrap();
        assert_eq!(report.output_retention.evidence.status, OutputBlockingStatus::Observed);
        let [proof] = report.output_retention.evidence.cases.as_slice() else { panic!() };
        assert_eq!(
            matches!(proof.dependency, OutputBlockingDependency::GrantedRelation { .. }),
            case == Case::PgRelation
        );
        assert_eq!(proof.owner.current_requests.as_deref(), Some("1"));
        if matches!(case, Case::PgRelation | Case::PgSessionOnly) {
            assert!(proof.owner.wait_ms.is_none());
        }
        let history = history_revision(revision, 950_002 + index as u64 * 10).await;
        let diagnosis = project(&[&history, &basis, &result], vec![]).unwrap();
        diagnosis.validate().unwrap();
        let [incident] = diagnosis.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(
            incident
                .culprit_queries
                .iter()
                .any(|query| query.identity == Some(DerivedQueryIdentity::NativeWaitOwner(proof.owner.owner.clone())))
        );
        assert!(incident.culprit_queries.iter().all(|query| !query.sql.is_available()));
        assert!(incident.culprit_queries.iter().any(|query| {
            query
                .caller
                .as_ref()
                .and_then(|caller| caller.client.as_ref())
                .and_then(|client| client.application.as_deref())
                == Some("db-reported-output-owner")
        }));
        if case == Case::MissingSql {
            assert!(incident.candidate_queries.is_empty());
        }
        assert!(incident.mechanism.is_some());
        assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
        assert!(!serde_json::to_string(&diagnosis).unwrap().contains("output-lock-private-canary"));
        assert!(incident.entry.reasons.contains(&t!("view.output_blocking.limit.cause")));
    }
}
#[test]
fn mars_unknown_rosters_unrelated_output_ended_partial_parallel_and_missing_proofs_never_become_output_retention() {
    for case in [
        Case::OtherWait,
        Case::UnrelatedOutput,
        Case::Mars,
        Case::UnknownRoster,
        Case::MissingStatement,
        Case::ChangedOwner,
        Case::Ended,
        Case::Partial,
        Case::Parallel,
        Case::LockPrefix,
        Case::MissingDetails,
    ] {
        let fixture = output_blocking_fixture(case);
        let rows = fixture
            .records
            .iter()
            .map(|row| {
                let TelemetryRecord::Activity(observation) = row else { panic!() };
                ActivityFrameRow { observation, reference: None }
            })
            .collect::<Vec<_>>();
        if let Ok(report) = culprit_checks::focused_blocking::explain(&fixture.source, &rows, true) {
            assert_ne!(report.output_retention.evidence.status, OutputBlockingStatus::Observed, "{case:?}");
            assert!(report.output_retention.evidence.cases.is_empty(), "{case:?}");
            assert!(report.output_retention.limitations.contains(&t!("view.output_blocking.limit.absence")));
        }
    }
    let fixture = output_blocking_fixture(Case::Mssql);
    let rows = fixture
        .records
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(observation) = row else { panic!() };
            ActivityFrameRow { observation, reference: None }
        })
        .collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(&fixture.source, &rows, false).unwrap();
    assert_ne!(report.output_retention.evidence.status, OutputBlockingStatus::Observed);
}
#[tokio::test]
async fn resolution_retires_work_but_preserves_the_exact_historical_output_dependency_and_never_authorizes_a_new_action()
 {
    let (basis, result, revision) = scenario(Case::Mssql, 951_000).await;
    let episode = revision.episode.clone();
    let answered = history_revision(revision, 951_002).await;
    let mut source = basis.source().clone();
    let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
    frame.at = "2020-01-01T12:00:12Z".into();
    frame.observed_from = frame.at.clone();
    frame.observed_until = frame.at.clone();
    frame.observations = 0;
    frame.native_rows = 0;
    frame.coverage = StateSourceCoverage::collected(source.source.clone(), 0);
    let empty = retained(source, vec![], 951_003).await;
    let original = empty.verified_original().unwrap();
    let previous = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().target.clone(),
        input: episode.latest.capture_ref(),
        episodes: vec![episode.clone()],
    };
    let update = observe_diagnostic_activity(
        original.source(),
        original.original(),
        &[],
        Some(&previous),
        &ActivityPolicy::default(),
        "2020-01-01T12:00:13Z",
    )
    .unwrap();
    let resolved = update
        .revisions
        .into_iter()
        .find(|revision| {
            revision.episode.id == episode.id && revision.episode.lifecycle == DiagnosticLifecycle::Resolved
        })
        .unwrap();
    assert!(resolved.episode.report.is_some());
    assert!(update.checkpoint.episodes.is_empty());
    let history = history_revision(resolved, 951_004).await;
    let diagnosis = project(&[&history, &answered, &basis, &result, &empty], vec![]).unwrap();
    let current =
        diagnosis.incidents.iter().find(|incident| incident.entry.lifecycle == DiagnosticLifecycle::Resolved).unwrap();
    assert!(current.mechanism.is_some());
    assert!(!current.culprit_queries.is_empty());
    assert!(current.entry.reasons.contains(&t!("view.output_blocking.limit.point")));
}

#[tokio::test]
async fn output_blocking_never_infers_http_loops_network_faults_or_sustained_duration() {
    for (idx, case) in [Case::Mssql, Case::PgRelation].into_iter().enumerate() {
        let (basis, result, revision) = scenario(case, 952_000 + (idx as u64) * 10).await;
        let hist = history_revision(revision, 952_002 + (idx as u64) * 10).await;
        let diagnosis = project(&[&hist, &basis, &result], vec![]).unwrap();
        let [incident] = diagnosis.incidents.as_slice() else { panic!() };
        let combined = format!(
            "{}\n{}\n{}",
            incident.mechanism.as_deref().unwrap_or_default(),
            incident.developer_action,
            incident.verification
        )
        .to_ascii_lowercase();
        for forbidden in ["http", "microservice", "packet loss", "nic ", "buffer every"] {
            assert!(
                !combined.contains(forbidden),
                "{case:?} contained forbidden client/network inference: {forbidden}"
            );
        }
        assert!(incident.entry.reasons.contains(&t!("view.output_blocking.limit.point")));
        assert!(incident.entry.reasons.contains(&t!("view.output_blocking.limit.cause")));
    }
}
