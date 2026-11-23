//! Same controller, admission, bounded source and immutable incident owner for the MSSQL native profile.
use super::*;
use culprit_evidence::analytical::test_support::{MssqlWorkCase, mssql_current_work_evidence};

fn original_activity() -> Frame {
    let (detail, _) = mssql_current_work_evidence(MssqlWorkCase::Observed);
    let SourceObservation::CurrentRequestWork { result, .. } = detail.observation else { panic!() };
    let focus = result.focus.unwrap();
    let source = CollectionDescription {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        source: focus.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            at: focus.original_observed_at.clone(),
            observed_from: focus.original_observed_at.clone(),
            observed_until: focus.original_observed_at.clone(),
            epoch: Some(focus.epoch.clone()),
            database_writable: focus.database_writable,
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::DefaultDatabaseConnections,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(focus.source.clone(), 1),
        }),
    };
    let mut original = focus.original.clone();
    original.elapsed_ms = Some("120000".into());
    original.state = "running".into();
    original.idle = false;
    Frame { source, records: vec![TelemetryRecord::Activity(Box::new(original))] }
}
fn current_frame(focus: &ActivityRequestFocus, case: MssqlWorkCase) -> Frame {
    let (mut source, records) = mssql_current_work_evidence(case);
    let SourceObservation::CurrentRequestWork { result, .. } = &mut source.observation else { panic!() };
    result.focus = Some(Box::new(focus.clone()));
    Frame { source, records }
}

#[tokio::test]
async fn mssql_current_work_uses_existing_adaptive_owner_and_pins_its_own_result_without_rebinding_original() {
    let mut runtime = Runtime::from_fixture(fixture_for(original_activity()).await).await;
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    let focus = intent.focus.activity_participant().unwrap();
    assert_eq!(focus.target.engine(), Engine::Mssql);
    runtime.fixture.backend.current_work = Some(current_frame(focus, MssqlWorkCase::Observed));
    let original = runtime.fixture.outbox.diagnostic_evidence(&focus.parent).await.unwrap();
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let calls = runtime.native_calls();
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    assert_eq!(runtime.native_calls(), calls);
    let revisions = runtime.revisions(&capture.capture).await;
    let [revision] = revisions.as_slice() else { panic!() };
    assert_eq!(revision.episode.analysis, DiagnosticAnalysisStatus::Analyzed);
    assert_eq!(revision.episode.lifecycle, DiagnosticLifecycle::Active);
    assert!(matches!(&revision.episode.report, Some(DiagnosticReport::CurrentRequestWork { .. })));
    assert!(revision.inputs.contains(&focus.parent));
    assert_eq!(runtime.fixture.outbox.diagnostic_evidence(&focus.parent).await.unwrap(), original);
    runtime.reconcile().await;
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}

#[tokio::test]
async fn unobserved_mssql_request_is_persisted_as_unknown_evidence_without_ending_or_recovering_the_episode() {
    let mut runtime = Runtime::from_fixture(fixture_for(original_activity()).await).await;
    assert!(runtime.enroll().await);
    let intent = selected_intent(&runtime).await;
    runtime.fixture.backend.current_work =
        Some(current_frame(intent.focus.activity_participant().unwrap(), MssqlWorkCase::NotObserved));
    let job = runtime.take_focus().await;
    let result = runtime.execute(&job).await;
    let Outcome::FocusedComplete { result: capture } = &result.outcome else { panic!() };
    let capture = capture.clone();
    runtime.finish(result.outcome).await;
    let policies = runtime.policies();
    analyze_pending(&mut runtime.fixture.outbox, &policies, &CollectionCancellation::default(), 10, None)
        .await
        .unwrap();
    let revisions = runtime.revisions(&capture.capture).await;
    let [revision] = revisions.as_slice() else { panic!() };
    assert_eq!(revision.episode.analysis, DiagnosticAnalysisStatus::Partial);
    assert_eq!(revision.episode.issue, Some(DiagnosticAnalysisIssue::EvidenceUnavailable));
    assert_eq!(revision.episode.lifecycle, DiagnosticLifecycle::Active);
    assert!(revision.episode.recovery.is_none());
    assert!(revision.episode.report.is_none());
    let calls = runtime.native_calls();
    runtime.reconcile().await;
    assert!(!runtime.enroll().await);
    assert_eq!(runtime.native_calls(), calls);
}

mod execution_profiles;
mod profile_preference;
mod relational_inputs;
