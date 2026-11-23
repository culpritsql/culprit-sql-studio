//! Real canonical originals -> one interval view -> unchanged common Gold producer, with no native reread.
use super::*;
use culprit_core::{
    CommitPipelineIntervalIssue as Issue, CommitPipelineIntervalMechanism as Mechanism, CommitPipelineIntervalScope,
    MeasurementScope,
};
use test_support::{CommitPipelineIntervalCase as Case, commit_pipeline_interval_fixture};

async fn originals(case: Case, sequence: u64) -> Vec<EvidenceCapture> {
    let mut captures = Vec::new();
    for (index, input) in commit_pipeline_interval_fixture(case).originals.into_iter().enumerate() {
        captures.push(Box::pin(retained(input.source, input.records, sequence + index as u64)).await);
    }
    captures
}
#[tokio::test]
async fn shared_pg17_and_pg18_native_epochs_explain_sync_work_without_database_cost_or_query_blame() {
    for (index, case) in
        [Case::Postgres17, Case::Postgres18, Case::LargeInteger, Case::ChangedPrimaryRole].into_iter().enumerate()
    {
        let captures = Box::pin(originals(case, 1_260_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        let [interval] = report.intervals.as_slice() else { panic!("{case:?}: {:?}", report.interval_gaps) };
        assert_eq!(interval.measurement_scope, MeasurementScope::Server);
        assert_eq!(interval.scope_basis, CommitPipelineIntervalScope::SharedServerIndependentOfDatabaseRole);
        assert!(report.inputs.iter().all(|input| input.context.as_ref().is_none_or(|context| !matches!(
            context.target,
            CollectionTarget::Shared(_)
        )
            || context.database_writable.is_none())));
        assert!(interval.mechanisms.contains(&Mechanism::LocalSyncWork));
        assert!(interval.mechanisms.contains(&Mechanism::LocalWriteWork));
        assert_eq!(
            interval
                .values
                .iter()
                .find(|value| value.kind == CommitPipelineCounterKind::SyncOperations)
                .unwrap()
                .value
                .integer(),
            Some(2)
        );
        assert_eq!(
            interval
                .values
                .iter()
                .find(|value| value.kind == CommitPipelineCounterKind::SyncTime)
                .unwrap()
                .value
                .exact_real(),
            Some(20.5)
        );
        if case == Case::LargeInteger {
            assert_eq!(
                interval
                    .values
                    .iter()
                    .find(|value| value.kind == CommitPipelineCounterKind::GeneratedBytes)
                    .unwrap()
                    .value
                    .integer(),
                Some(17)
            );
        }
        let revision = Box::pin(history(log_episode(&observe(&captures[0], None)), 1_260_100 + index as u64)).await;
        let mut input = vec![&revision];
        input.extend(captures.iter());
        let output = project(&input, vec![]).unwrap();
        output.validate().unwrap();
        let incident = &output.incidents[0];
        assert!(!incident.affected_queries.is_empty());
        assert!(incident.culprit_queries.is_empty() && incident.candidate_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(
            incident
                .mechanism
                .as_deref()
                .unwrap()
                .contains(&culprit_core::t!("view.commit_pipeline.interval.mechanism"))
        );
        assert!(incident.developer_action.contains(&culprit_core::t!("view.commit_pipeline.interval.action.sync")));
        assert!(incident.verification.contains(&culprit_core::t!("view.commit_pipeline.interval.verify")));
        assert!(incident.entry.reasons.contains(&culprit_core::t!("view.commit_pipeline.interval.limit.shared")));
        assert_ne!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
    }
}
#[tokio::test]
async fn native_reset_owner_scope_coverage_and_clock_refusals_do_not_select_older_agreeable_inputs() {
    let cases = [
        (Case::UnknownReset, Issue::UnknownReset),
        (Case::ChangedReset, Issue::ChangedReset),
        (Case::FutureReset, Issue::ResetAfterObservation),
        (Case::Decreased, Issue::CounterDecreased),
        (Case::Partial, Issue::IncompleteSource),
        // This fixture changes the declared Shared resource itself; it supplies no endpoint for this resource.
        (Case::ForeignMember, Issue::MissingEndpoint),
        (Case::Restart, Issue::NativeOwnerChanged),
        (Case::DuplicateBefore, Issue::AmbiguousEndpoint),
        (Case::OverlappingWindow, Issue::ObservationOrder),
        (Case::Unbracketed, Issue::MissingEndpoint),
        (Case::CloserUnknownReset, Issue::UnknownReset),
        (Case::ChangedPopulation, Issue::CounterMissing),
        (Case::CloserUnavailable, Issue::IncompleteSource),
        (Case::MssqlUnknownReset, Issue::UnknownReset),
        (Case::MssqlUnknownRole, Issue::DatabaseRoleUnknown),
        (Case::MssqlChangedRole, Issue::DatabaseRoleChanged),
    ];
    for (index, (case, reason)) in cases.into_iter().enumerate() {
        let captures = Box::pin(originals(case, 1_261_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        assert!(report.intervals.is_empty(), "{case:?}");
        assert!(report.interval_gaps.iter().any(|gap| gap.reason == reason), "{case:?}: {:?}", report.interval_gaps);
        assert_eq!(
            report.stage,
            Some(if matches!(case, Case::MssqlUnknownReset | Case::MssqlUnknownRole | Case::MssqlChangedRole) {
                CommitPipelineStage::LocalLogFlush
            } else {
                CommitPipelineStage::LocalLogSync
            })
        );
        assert!(!report.limitations.is_empty());
    }
}
#[tokio::test]
async fn zero_or_missing_timing_and_no_recorded_work_never_prove_fast_storage_or_recovery() {
    for (index, case) in [Case::TimingZero, Case::TimingMissing, Case::NoWork].into_iter().enumerate() {
        let captures = Box::pin(originals(case, 1_262_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        assert_eq!(report.intervals.len(), 1);
        assert!(report.limitations.contains(&culprit_core::t!("view.commit_pipeline.interval.limit.timing")));
        let interval = &report.intervals[0];
        if case == Case::TimingZero {
            assert_eq!(
                interval
                    .values
                    .iter()
                    .find(|value| value.kind == CommitPipelineCounterKind::SyncTime)
                    .unwrap()
                    .value
                    .integer(),
                Some(0)
            );
            assert!(interval.mechanisms.contains(&Mechanism::LocalSyncWork));
        } else if case == Case::TimingMissing {
            assert!(!interval.values.iter().any(|value| value.kind == CommitPipelineCounterKind::SyncTime));
            assert!(report.interval_gaps.iter().any(|gap| gap.reason == Issue::CounterMissing));
        } else {
            assert!(interval.mechanisms.is_empty());
            assert!(interval.values.iter().all(|value| value.value.exact_real() == Some(0.0)));
        }
        let revision = Box::pin(history(log_episode(&observe(&captures[0], None)), 1_262_100 + index as u64)).await;
        let mut input = vec![&revision];
        input.extend(captures.iter());
        let output = project(&input, vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.affected_queries.is_empty());
        assert_ne!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
    }
}
#[tokio::test]
async fn incomplete_eof_or_changed_original_counter_receipt_cannot_supply_an_interval() {
    let captures = Box::pin(originals(Case::Postgres17, 1_263_000)).await;
    let original = captures.iter().map(|capture| capture.verified_original().unwrap()).collect::<Vec<_>>();
    let borrowed = original.iter().map(|capture| capture as &dyn OriginalProjection).collect::<Vec<_>>();
    let mut input = super::super::super::commit_pipeline::inputs(&borrowed, original[0].original()).unwrap();
    let reference = original[0].reference(0).unwrap();
    input[2].rows.clear();
    input[2].read_complete = false;
    let report = culprit_checks::commit_pipeline::explain(&input, &reference).unwrap();
    assert!(report.intervals.is_empty());
    assert!(report.interval_gaps.iter().any(|gap| gap.reason == Issue::IncompleteRead));
    assert_eq!(
        report.interval_gaps.iter().find(|gap| gap.reason == Issue::IncompleteRead).unwrap().after_capture.as_ref(),
        Some(original[2].original())
    );
    let mut input = super::super::super::commit_pipeline::inputs(&borrowed, original[0].original()).unwrap();
    input[2].rows[0].reference.payload_sha256 = digest(b"changed-native-counter-original");
    assert!(culprit_checks::commit_pipeline::explain(&input, &reference).is_err());
}

#[tokio::test]
async fn exact_native_write_and_remote_acknowledgement_keep_distinct_actions_despite_the_same_shared_counter_work() {
    for (index, case) in [Case::NativeWrite, Case::RemoteAcknowledgement].into_iter().enumerate() {
        let captures = Box::pin(originals(case, 1_264_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        assert_eq!(report.intervals.len(), 1);
        assert!(report.intervals[0].mechanisms.contains(&Mechanism::LocalWriteWork));
        if case == Case::NativeWrite {
            assert_eq!(report.stage, Some(CommitPipelineStage::LocalLogWrite));
            assert!(report.investigation.contains(&culprit_core::t!("view.commit_pipeline.interval.action.write")));
            assert!(report.investigation.contains(&culprit_core::t!("view.commit_pipeline.interval.action.buffer")));
        } else {
            assert_eq!(report.stage, Some(CommitPipelineStage::SynchronousAcknowledgement));
            assert!(
                !report
                    .mechanism
                    .as_deref()
                    .unwrap()
                    .contains(&culprit_core::t!("view.commit_pipeline.interval.mechanism"))
            );
            for key in [
                "view.commit_pipeline.interval.action.write",
                "view.commit_pipeline.interval.action.sync",
                "view.commit_pipeline.interval.action.buffer",
            ] {
                assert!(!report.investigation.contains(&culprit_core::i18n::t(key, &[])));
            }
        }
    }
}

mod replacements;
