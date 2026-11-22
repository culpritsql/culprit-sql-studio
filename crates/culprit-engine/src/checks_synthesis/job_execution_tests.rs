//! Existing canonical corpus/proof owners exercise job interpretation without a live provider or a second decoder.
use super::*;
use crate::collected_evidence::RetainedEvidenceFixture as Fixture;
use culprit_evidence::analytical::{
    evidence::{EvidenceCapture, EvidenceFamily},
    test_support::{self, JobExecutionCase as Case},
};
fn policy() -> ChecksSynthesizer {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "job-record-rules-1").unwrap()
}
fn display() -> DerivedDisplayContext {
    DerivedDisplayContext { connection_alias: "Explicit job alias".into(), environment: None, database_name: None }
}
async fn read_case(case: Case) -> EvidenceCapture {
    let (source, records) = test_support::job_execution_evidence(case);
    let fixture = Fixture::with_collector_bounds_and_sequence(
        source,
        records,
        800,
        "2026-11-22T10:00:00Z",
        "2026-11-22T10:00:01Z",
    )
    .await
    .unwrap();
    fixture.read_families(&[EvidenceFamily::OperationalSamples]).await
}
#[tokio::test]
async fn canonical_job_profiles_share_one_projection_and_publish_only_actual_utc_records_with_exact_refs() {
    for &case in Case::ALL {
        let capture = read_case(case).await;
        let original = capture.verified_original().unwrap();
        let before = serde_json::to_value(capture.metrics()).unwrap();
        let raw = serde_json::to_value(capture.source()).unwrap();
        let read = crate::collected_resources::jobs::from_original(&original).unwrap();
        let projection = culprit_checks::jobs::execution_projection(&read).unwrap();
        let via_sdk = crate::collected_resources::jobs(&capture).unwrap();
        assert_eq!(via_sdk.report.executions, projection, "{case:?}");
        let output = policy().synthesize_captures(&capture, &[], "2026-11-22T10:01:00Z", &display()).unwrap();
        let DerivedChangeEvaluation::JobExecutions { input, assessment } = &output.description.change_evaluations[0]
        else {
            panic!("job scope");
        };
        assert_eq!(*input, 0);
        assert_eq!(assessment, &projection.assessment, "{case:?}");
        assert_eq!(output.description.output_coverage.changes, DerivedOutputCoverage::Evaluated);
        assert_eq!(output.changes.len(), projection.events.len(), "{case:?}");
        for row in &output.changes {
            assert_eq!(row.inputs.len(), 1);
            assert_eq!(row.inputs[0].capture_ref(), *original.original());
            let metadata = row.change.job_execution.as_ref().unwrap();
            assert!(row.change.job_execution_is_valid());
            assert_eq!(
                row.inputs[0].native_key,
                NativeRecordIdentity::Operational {
                    source: metadata.reference.source.clone(),
                    key: metadata.reference.observation_key.clone()
                }
            );
            assert!(row.change.table.is_none() && row.change.object.is_none());
            let mut unbound = row.clone();
            unbound.inputs.clear();
            assert!(!unbound.is_valid());
            let mut foreign = row.clone();
            foreign.inputs[0].native_key = NativeRecordIdentity::Operational {
                source: metadata.reference.source.clone(),
                key: digest(b"another native record"),
            };
            assert!(!foreign.is_valid());
        }
        assert!(
            output.incidents.is_empty() && output.findings.is_empty(),
            "job records are not fabricated query causes"
        );
        assert_eq!(serde_json::to_value(capture.metrics()).unwrap(), before);
        assert_eq!(serde_json::to_value(capture.source()).unwrap(), raw);
        assert!(!serde_json::to_string(&output).unwrap().contains("private-agent-machine"));
        output.validate().unwrap();
    }
}
#[tokio::test]
async fn summary_retry_and_step_records_are_not_summed_as_runs_or_attached_to_current_definitions() {
    let capture = read_case(Case::AgentSummaryAndRetries).await;
    let report = crate::collected_resources::jobs(&capture).unwrap().report;
    assert_eq!((report.executions.assessment.run_records, report.executions.assessment.step_records), (1, 2));
    assert!(report.executions.events.is_empty());
    assert!(report.executions.entries.iter().any(|entry|matches!(&entry.diagnostics,JobExecutionDiagnostics::Mssql {retries_attempted,native_status:2,..} if retries_attempted=="2")));
    let edited = read_case(Case::AgentEditedDefinition).await;
    let report = crate::collected_resources::jobs(&edited).unwrap().report;
    let entry = &report.executions.entries[0];
    assert_eq!(entry.label.as_ref().unwrap().text, "Original recorded step");
    assert_eq!(entry.label.as_ref().unwrap().origin, JobExecutionLabelOrigin::Stored);
    assert!(!serde_json::to_string(entry).unwrap().contains("Changed current"));
}
#[tokio::test]
async fn native_local_dst_zero_unknown_and_restart_do_not_acquire_utc_or_sql_session_authority() {
    for case in
        [Case::AgentDst, Case::AgentZeroDate, Case::AgentUnknownStatus, Case::AgentRestart, Case::MysqlLastStart]
    {
        let capture = read_case(case).await;
        let report = crate::collected_resources::jobs(&capture).unwrap().report;
        assert!(report.executions.events.is_empty(), "{case:?}");
        assert!(report.executions.assessment.unplaced_records > 0);
        for entry in &report.executions.entries {
            assert_eq!(entry.placement, JobExecutionPlacement::NativeLocal);
            assert!(!matches!(entry.time, JobExecutionTime::Utc { .. }));
            match (&entry.time, case) {
                (
                    JobExecutionTime::MssqlLocal { started_without_offset, duration_seconds, .. },
                    Case::AgentZeroDate,
                ) => {
                    assert!(started_without_offset.is_none());
                    assert_eq!(duration_seconds.as_deref(), Some("0"));
                }
                (_, Case::AgentUnknownStatus) => assert_eq!(entry.status, JobExecutionStatus::Unknown),
                (_, Case::MysqlLastStart) => assert_eq!(entry.status, JobExecutionStatus::Unknown),
                _ => {}
            }
        }
    }
}
#[tokio::test]
async fn duplicate_run_identity_is_withheld_before_display_cap_and_errors_only_never_supply_run_counts() {
    let capture = read_case(Case::TimescaleDuplicateRun).await;
    let original = capture.verified_original().unwrap();
    let read = crate::collected_resources::jobs::from_original(&original).unwrap();
    let report = culprit_checks::jobs::explain_bounded(&read, 1).unwrap();
    assert_eq!(report.executions.entries.len(), 1);
    assert!(report.executions.events.is_empty());
    assert_eq!(report.executions.assessment.unplaced_records, 2);
    assert!(
        report.executions.entries.iter().all(|entry| entry.placement == JobExecutionPlacement::AmbiguousNativeIdentity)
    );
    let errors = read_case(Case::TimescaleErrorsOnly).await;
    let report = crate::collected_resources::jobs(&errors).unwrap().report;
    assert_eq!(report.executions.assessment.run_records, 0);
    assert_eq!(report.executions.assessment.error_records, 1);
    assert_eq!(report.executions.entries[0].status, JobExecutionStatus::Unknown);
    assert!(report.executions.events.is_empty());
}
#[tokio::test]
async fn denied_zero_and_partial_native_scope_keep_a_job_assessment_and_original_coverage_in_gold() {
    for case in [Case::TimescaleDenied, Case::TimescalePartial] {
        let capture = read_case(case).await;
        let output = policy().synthesize_captures(&capture, &[], "2026-11-22T10:01:00Z", &display()).unwrap();
        let DerivedChangeEvaluation::JobExecutions { assessment, .. } = &output.description.change_evaluations[0]
        else {
            panic!()
        };
        if case == Case::TimescaleDenied {
            assert!(output.changes.is_empty());
            assert_eq!(assessment.record_count(), Some(0));
            assert!(assessment.sources.iter().all(|source| source.state == SourceState::Unavailable));
        } else {
            assert!(assessment.sources.iter().all(|source| source.state == SourceState::Partial));
        }
        assert!(!output.description.limitations().is_empty());
        output.validate().unwrap();
    }
}
#[cfg(feature = "collector")]
#[tokio::test]
async fn utc_and_unplaced_job_originals_seal_gold_with_zero_native_work_through_the_existing_writer() {
    use culprit_collector::{analytical::test_support::LocalCaptureFixture, synthesis::SynthesisRequest};
    use culprit_core::limits::OperationGuard;
    for case in [Case::TimescaleFailed, Case::AgentSummaryAndRetries] {
        let (source, rows) = test_support::job_execution_evidence(case);
        let mut fixture = Box::pin(LocalCaptureFixture::snapshot(source, rows)).await;
        let primary = fixture.original_reference();
        let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
        let outbox = fixture.outbox.as_mut().unwrap();
        let reference = outbox
            .delivery()
            .synthesize(
                SynthesisRequest { primary, context: vec![], budget: Default::default() },
                guard.handle(),
                (),
                Arc::new(policy()),
            )
            .await
            .unwrap();
        let sealed = outbox.verify(&reference.capture.observation_id).await.unwrap();
        assert_eq!(sealed.source_queries, 0);
        assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
        let SourceObservation::DerivedDiagnosis(description) = &sealed.description.as_ref().unwrap().observation else {
            panic!()
        };
        let DerivedChangeEvaluation::JobExecutions { assessment, .. } = &description.change_evaluations[0] else {
            panic!()
        };
        assert_eq!(description.counts.changes, if case == Case::TimescaleFailed { 1 } else { 0 });
        assert_eq!(assessment.utc_placed_records, description.counts.changes);
    }
}

#[tokio::test]
async fn saved_scan_timeline_requires_the_same_original_namespace_and_preserves_scope_refusal() {
    let capture = read_case(Case::TimescaleFailed).await;
    let original = capture.verified_original().unwrap();
    let read = crate::collected_resources::jobs::from_original(&original).unwrap();
    let mut snapshot: Box<Snapshot> =
        serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/postgres-demo.json")))
            .unwrap();
    let database = read.context().target.database().unwrap().clone();
    snapshot.server.engine = database.engine;
    snapshot.server.database = database.database.clone();
    snapshot.bind_identity(database);
    snapshot.taken_at = "2026-11-15T12:01:00Z".into();
    snapshot.operations.jobs = Some(Box::new(read));
    let expected = culprit_checks::jobs::execution_events_snapshot(&snapshot).unwrap();
    assert_eq!(expected.len(), 1);
    let actual = crate::history::diff(None, &snapshot, vec![]);
    assert!(actual.iter().any(|event| event == &expected[0]));
    let mut foreign = snapshot.clone();
    foreign.identity.as_mut().unwrap().database = "different-database".into();
    assert!(culprit_checks::jobs::execution_events_snapshot(&foreign).unwrap().is_empty());
    assert!(!crate::history::diff(None, &foreign, vec![]).iter().any(|event| event.kind == EventKind::JobExecution));
    let mut unknown = snapshot.clone();
    unknown.identity = None;
    assert!(culprit_checks::jobs::execution_events_snapshot(&unknown).unwrap().is_empty());
    let original_scope = serde_json::to_value(&unknown.operations.jobs).unwrap();
    assert!(
        culprit_checks::jobs::explain_snapshot(&unknown)
            .unwrap()
            .unwrap()
            .limitations
            .contains(&culprit_core::t!("view.observations.limit.namespace"))
    );
    assert_eq!(serde_json::to_value(&unknown.operations.jobs).unwrap(), original_scope);
    let shared = read_case(Case::AgentSummaryAndRetries).await;
    let original = shared.verified_original().unwrap();
    snapshot.operations.jobs = Some(Box::new(crate::collected_resources::jobs::from_original(&original).unwrap()));
    assert!(culprit_checks::jobs::execution_events_snapshot(&snapshot).unwrap().is_empty());
}

#[tokio::test]
async fn missing_retained_history_body_does_not_become_zero_jobs_or_complete_native_history() {
    let (source, records) = test_support::job_execution_evidence(Case::TimescaleFailed);
    let index = records
        .iter()
        .position(|row| {
            matches!(
                row,
                TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TimescaleJobRun(_), .. })
            )
        })
        .unwrap();
    let fixture = Fixture::with_collector_bounds_and_sequence(
        source.clone(),
        records,
        801,
        "2026-11-22T10:00:00Z",
        "2026-11-22T10:00:01Z",
    )
    .await
    .unwrap();
    fixture.remove_row(TelemetryStream::OperationalSamples, index);
    let capture = fixture.read_families(&[EvidenceFamily::OperationalSamples]).await;
    assert!(!capture.is_read_complete());
    assert_eq!(capture.source(), &source);
    let output = policy().synthesize_captures(&capture, &[], "2026-11-22T10:01:00Z", &display()).unwrap();
    assert!(!output.description.input_read_complete);
    assert!(output.changes.is_empty());
    let DerivedChangeEvaluation::JobExecutions { assessment, .. } = &output.description.change_evaluations[0] else {
        panic!()
    };
    assert!(!assessment.read_complete);
    assert_eq!(assessment.run_records, 0);
    let history = assessment.sources.iter().find(|source| source.source == TIMESCALE_JOB_RUNS_SOURCE).unwrap();
    assert_eq!((history.native_rows, history.observed_rows), (1, 0));
    assert!(!history.complete_scope);
    assert!(!output.description.limitations().is_empty());
    output.validate().unwrap();
}
