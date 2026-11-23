//! One bounded companion compares retained native counter epochs; no source work or query attribution.
use super::*;
use std::collections::BTreeMap;
mod compare;
mod points;
mod values;
mod words;
pub(super) use words::render;

struct Point<'a> {
    reference: &'a DiagnosticEvidenceRef,
    set: &'a NativeCounterSet,
    values: Vec<CommitPipelineCounter>,
}
struct Frame<'a> {
    original: &'a Original<'a>,
    context: Option<NativeReadContext>,
    observed_from: &'a str,
    observed_until: &'a str,
    native_source: &'a str,
    coverage: StateSourceCoverage,
    points: Vec<Point<'a>>,
    key: String,
}

pub(super) fn append<'a>(originals: &'a [Original<'a>], report: &mut CommitPipelineReport) -> Result<()> {
    if report.stage.is_none() {
        return Ok(());
    }
    let frames = points::collect(originals, report)?;
    if report.facts_limited {
        gap(report, None, None, None, CommitPipelineIntervalIssue::PresentationLimited);
        return Ok(());
    }
    if frames.is_empty() && report.interval_gaps.is_empty() {
        gap(report, None, None, None, CommitPipelineIntervalIssue::MissingEndpoint);
    }
    let mut groups = BTreeMap::<String, Vec<Frame<'_>>>::new();
    for frame in frames {
        groups.entry(frame.key.clone()).or_default().push(frame);
    }
    for group in groups.into_values() {
        if report.intervals.len() == MAX_COMMIT_PIPELINE_FACTS {
            gap(report, None, None, None, CommitPipelineIntervalIssue::PresentationLimited);
            report.facts_limited = true;
            break;
        }
        compare::append(&group, report)?;
    }
    Ok(())
}

fn instant(value: &str) -> Option<String> {
    canonical_time(value).ok()
}
fn gap(
    report: &mut CommitPipelineReport,
    before: Option<&DiagnosticEvidenceRef>,
    after: Option<&DiagnosticEvidenceRef>,
    counter: Option<CommitPipelineCounterKind>,
    reason: CommitPipelineIntervalIssue,
) {
    admit_gap(
        report,
        CommitPipelineIntervalGap {
            before_capture: before.map(DiagnosticEvidenceRef::capture_ref),
            after_capture: after.map(DiagnosticEvidenceRef::capture_ref),
            before: before.cloned(),
            after: after.cloned(),
            counter,
            reason,
        },
    );
}
fn frame_gap(
    report: &mut CommitPipelineReport,
    before: Option<&Frame<'_>>,
    after: Option<&Frame<'_>>,
    reason: CommitPipelineIntervalIssue,
) {
    admit_gap(
        report,
        CommitPipelineIntervalGap {
            before_capture: before.map(|frame| frame.original.original.clone()),
            after_capture: after.map(|frame| frame.original.original.clone()),
            before: None,
            after: None,
            counter: None,
            reason,
        },
    );
}
fn admit_gap(report: &mut CommitPipelineReport, entry: CommitPipelineIntervalGap) {
    if report.interval_gaps.len() < MAX_COMMIT_PIPELINE_FACTS {
        if !report.interval_gaps.contains(&entry) {
            report.interval_gaps.push(entry);
        }
    } else {
        report.facts_limited = true;
    }
}
