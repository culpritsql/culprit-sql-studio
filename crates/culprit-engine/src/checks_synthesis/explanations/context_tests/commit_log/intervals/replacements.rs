//! The existing canonical assembler supplies both the older agreeable pair and its nearer replacement.
use super::*;
use culprit_core::{MAX_COMMIT_PIPELINE_FACTS, MAX_DERIVED_CAPTURES};

#[tokio::test]
async fn nearer_changed_process_or_acquisition_member_withholds_the_older_agreeable_interval_and_gold_claim() {
    for (index, case) in [
        Case::CloserRestartBefore,
        Case::CloserRestartAfter,
        Case::CloserRestartPair,
        Case::CloserMemberBefore,
        Case::CloserMemberAfter,
    ]
    .into_iter()
    .enumerate()
    {
        let captures = Box::pin(originals(case, 1_265_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        assert!(report.intervals.is_empty(), "{case:?}: older valid-looking counter endpoints must not win");
        let nearer = captures[3].verified_original().unwrap();
        assert!(
            report.interval_gaps.iter().any(|gap| {
                gap.reason == Issue::NativeOwnerChanged
                    && (gap.before_capture.as_ref() == Some(nearer.original())
                        || gap.after_capture.as_ref() == Some(nearer.original()))
            }),
            "{case:?}: {:?}",
            report.interval_gaps
        );
        let revision = Box::pin(history(log_episode(&observe(&captures[0], None)), 1_265_100 + index as u64)).await;
        let mut selected = vec![&revision];
        selected.extend(captures.iter());
        let output = project(&selected, vec![]).unwrap();
        output.validate().unwrap();
        let incident = &output.incidents[0];
        assert!(incident.culprit_queries.is_empty() && incident.candidate_queries.is_empty());
        assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        assert!(
            !incident
                .mechanism
                .as_deref()
                .unwrap()
                .contains(&culprit_core::t!("view.commit_pipeline.interval.mechanism"))
        );
        assert_ne!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
    }
}

#[tokio::test]
async fn nearer_missing_owner_and_empty_partial_originals_retain_exact_capture_barriers() {
    for (index, case) in [Case::CloserUnknownOwner, Case::CloserEmptyUnknownOwner].into_iter().enumerate() {
        let captures = Box::pin(originals(case, 1_266_000 + index as u64 * 10)).await;
        let report = explain(&captures);
        let nearer = captures[3].verified_original().unwrap();
        assert!(report.intervals.is_empty(), "{case:?}");
        assert!(
            report.interval_gaps.iter().any(|gap| {
                gap.reason == Issue::IncompleteSource
                    && (gap.before_capture.as_ref() == Some(nearer.original())
                        || gap.after_capture.as_ref() == Some(nearer.original()))
            }),
            "{case:?}: {:?}",
            report.interval_gaps
        );
        if case == Case::CloserEmptyUnknownOwner {
            assert!(nearer.records().is_empty());
            let gap = report.interval_gaps.iter().find(|gap| gap.reason == Issue::IncompleteSource).unwrap();
            assert_eq!(gap.after_capture.as_ref(), Some(nearer.original()));
            assert!(gap.after.is_none(), "a genuine zero-row capture gets no fabricated row reference");
        }
    }
}

#[tokio::test]
async fn a_distinct_shared_resource_keeps_the_exact_original_bracket_and_values() {
    let captures = Box::pin(originals(Case::UnrelatedSharedResource, 1_267_000)).await;
    let baseline = explain(&captures[..3]);
    let extended = explain(&captures);
    assert_eq!(baseline.intervals.len(), 1);
    assert_eq!(extended.intervals, baseline.intervals);
    let before = captures[1].verified_original().unwrap();
    let after = captures[2].verified_original().unwrap();
    assert_eq!(extended.intervals[0].before.capture_ref(), *before.original());
    assert_eq!(extended.intervals[0].after.capture_ref(), *after.original());
    assert!(!extended.facts_limited);
    for other in &captures[3..] {
        let other = other.verified_original().unwrap();
        assert_ne!(other.source().target, before.source().target);
        assert!(!extended.intervals.iter().any(|interval| {
            interval.before.capture_ref() == *other.original() || interval.after.capture_ref() == *other.original()
        }));
    }
}

#[tokio::test]
async fn more_than_the_frame_limit_of_other_resources_cannot_spend_the_primary_interpretation_budget() {
    let captures = Box::pin(originals(Case::ManyUnrelatedSharedFrames, 1_268_000)).await;
    assert_eq!(captures.len(), 3 + MAX_COMMIT_PIPELINE_FACTS + 1);
    assert!(captures.len() <= MAX_DERIVED_CAPTURES);
    let baseline = explain(&captures[..3]);
    let extended = explain(&captures);
    assert_eq!(baseline.intervals.len(), 1);
    assert_eq!(extended.intervals, baseline.intervals);
    assert_eq!(serde_json::to_value(&extended.inputs).unwrap(), serde_json::to_value(&baseline.inputs).unwrap());
    assert_eq!(serde_json::to_value(&extended.facts).unwrap(), serde_json::to_value(&baseline.facts).unwrap());
    assert_eq!(extended.examined_rows, baseline.examined_rows);
    assert!(!extended.facts_limited);
    assert!(!extended.interval_gaps.iter().any(|gap| gap.reason == Issue::PresentationLimited));
}
