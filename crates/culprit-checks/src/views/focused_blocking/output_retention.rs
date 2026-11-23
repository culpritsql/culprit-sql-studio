//! Words render the one native dependency interpreter; no new detector or client/network diagnosis.
use super::*;
use culprit_core::{
    OutputBlockingExplanation, OutputBlockingReport,
    blocking::{OutputBlockingDependency, OutputBlockingIssue, OutputBlockingStatus},
};
pub(super) fn explain(frame: &ActivityFrameEvidence<'_>) -> Result<OutputBlockingReport> {
    let evidence = culprit_core::blocking::output_blocking(frame)?;
    let summary = match evidence.status {
        OutputBlockingStatus::Observed => t!("view.output_blocking.summary.observed"),
        OutputBlockingStatus::NotEstablished => t!("view.output_blocking.summary.not_established"),
        OutputBlockingStatus::Unavailable => t!("view.output_blocking.summary.unavailable"),
    };
    let explanations = evidence
        .cases
        .iter()
        .map(|case| {
            let mechanism = match &case.dependency {
                OutputBlockingDependency::NativeSession => t!("view.output_blocking.mechanism.session"),
                OutputBlockingDependency::GrantedRelation { .. } => t!("view.output_blocking.mechanism.relation"),
            };
            OutputBlockingExplanation {
                key: case.key.clone(),
                victim_summary: t!("view.output_blocking.victim"),
                mechanism,
                investigation: t!("view.output_blocking.action"),
                verify: t!("view.output_blocking.verify"),
            }
        })
        .collect();
    let mut limitations = vec![
        t!("view.output_blocking.limit.point"),
        t!("view.output_blocking.limit.cause"),
        t!("view.output_blocking.limit.sql"),
    ];
    if evidence.status != OutputBlockingStatus::Observed {
        limitations.push(t!("view.output_blocking.limit.absence"));
    }
    if evidence.cases.iter().any(|case| matches!(&case.dependency, OutputBlockingDependency::NativeSession)) {
        limitations.push(t!("view.output_blocking.limit.resource"));
    }
    if evidence.cases.iter().any(|case| case.owner.wait_ms.is_none()) {
        limitations.push(t!("view.output_blocking.limit.duration"));
    }
    let mut seen_issue_keys = std::collections::BTreeSet::new();
    for issue in &evidence.issues {
        let key = match issue {
            OutputBlockingIssue::UnknownRequestPopulation => culprit_core::key!("view.output_blocking.limit.roster"),
            OutputBlockingIssue::MultipleCurrentRequests => culprit_core::key!("view.output_blocking.limit.mars"),
            OutputBlockingIssue::UnqualifiedStatement => culprit_core::key!("view.output_blocking.limit.statement"),
            OutputBlockingIssue::UnqualifiedOriginal
            | OutputBlockingIssue::IncompleteRead
            | OutputBlockingIssue::LockEvidenceLimited => culprit_core::key!("view.output_blocking.limit.coverage"),
            OutputBlockingIssue::PointChanged => culprit_core::key!("view.output_blocking.limit.changed"),
            OutputBlockingIssue::OwnerContextMissing
            | OutputBlockingIssue::NativeDetailsNotRecorded
            | OutputBlockingIssue::UnknownNativeWait => culprit_core::key!("view.output_blocking.limit.details"),
        };
        if seen_issue_keys.insert(key) {
            limitations.push(culprit_core::i18n::t(key, &[]));
        }
    }
    Ok(OutputBlockingReport { evidence, summary, explanations, limitations })
}
