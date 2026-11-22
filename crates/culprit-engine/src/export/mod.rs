//! Native sharing for findings/reports. Pure plan/diff formatting is owned by culprit-plans.
mod html;
mod markdown;
pub use culprit_core::export::{ExportFile, ExportFormat, ExportNote, ExportOptions};
use culprit_core::{Confidence, Environment, Finding, Plan, PlanDiff, Report, Severity, t};
pub use culprit_plans::export::{file_stem, native_extension, plan_native, shares_note};
pub(crate) use culprit_plans::export::{node_label, prepared, rows_phrase, sql_text};
pub use html::{finding_html, report_html};
pub(crate) use markdown::finding_detail_shared;
pub use markdown::{finding_detail, finding_markdown, report_markdown, verification_markdown};

pub(crate) fn shared_report(report: &Report) -> Report {
    crate::analysis::shared(report.clone().into(), &[])
}

pub(crate) fn shareable(report: &Report, shared: &Report, finding: &Finding) -> Finding {
    let engine = report.snapshot.server.engine;
    let rechecked = shared.findings.iter().find(|f| f.id == finding.id);
    let mut redacted = culprit_core::redact::redact_finding(rechecked.unwrap_or(finding), engine);
    // Rechecked fixes were generated from already redacted evidence. Keep their safe SQL constants and syntax;
    // masking them again would turn executable settings and diagnostic scripts into placeholder SQL.
    // A plugin finding the recheck cannot reproduce retains the full fallback redaction, including its fix SQL.
    if let Some(source) = rechecked
        && let (Some(fix), Some(original)) = (&mut redacted.fix, &source.fix)
    {
        fix.sql = original.sql.clone();
    }
    redacted
}

pub(crate) fn report_engine_label(report: &Report) -> &'static str {
    report.snapshot.server.name()
}

fn environment_id(environment: Environment) -> String {
    format!("{environment:?}").to_lowercase()
}

fn severity_id(severity: Severity) -> &'static str {
    match severity {
        Severity::High => "high",
        Severity::Medium => "medium",
        Severity::Low => "low",
    }
}

fn confidence_id(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
    }
}

pub(crate) fn finding_line(report: &Report, finding: &Finding, impact: String) -> String {
    let has_impact = if impact.is_empty() { "no" } else { "yes" };
    t!(
        "engine.export.finding_line",
        has_impact = has_impact,
        impact = impact,
        actionability = finding.actionability.label(),
        severity = severity_id(finding.severity),
        confidence = confidence_id(finding.confidence),
        server = server_line(report)
    )
}

pub(crate) fn stats_line(workload: &culprit_core::Workload, q: &culprit_core::QueryStat) -> String {
    let cost = workload.cost_of(q);
    // One run or two far apart: what they took, never an average that hides it (`QueryStat::few_runs`).
    let share = culprit_core::format::percent(cost.share);
    match (q.few_runs_took(), cost.totals) {
        (None, None) => t!(
            "engine.export.stats.average",
            share = share,
            runs = cost.runs,
            mean = culprit_core::format::duration(cost.mean_ms)
        ),
        (None, Some(totals)) => t!(
            "engine.export.stats.average_totals",
            share = share,
            runs = cost.runs,
            mean = culprit_core::format::duration(cost.mean_ms),
            totals = totals
        ),
        (Some(took), None) => t!("engine.export.stats.took", share = share, runs = cost.runs, took = took),
        (Some(took), Some(totals)) => {
            t!("engine.export.stats.took_totals", share = share, runs = cost.runs, took = took, totals = totals)
        }
    }
}

pub(crate) fn exported_at() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string()
}

pub(crate) fn server_line(report: &Report) -> String {
    let s = &report.snapshot;
    t!("engine.export.server_line", product = &s.server.product, environment = environment_id(s.environment))
}

/// Native callers resolve the export clock here; supplied plans use the shared pure renderer.
pub fn plan_html(plan: &Plan, title: &str, options: &ExportOptions) -> String {
    culprit_plans::export::plan_html(plan, title, options, &exported_at())
}
pub fn plan_markdown(plan: &Plan, title: &str, options: &ExportOptions) -> String {
    culprit_plans::export::plan_markdown(plan, title, options, &exported_at())
}
pub fn diff_html(a: &Plan, b: &Plan, diff: &PlanDiff, name_a: &str, name_b: &str, options: &ExportOptions) -> String {
    culprit_plans::export::diff_html(a, b, diff, name_a, name_b, options, &exported_at())
}
pub fn diff_markdown(
    a: &Plan,
    b: &Plan,
    diff: &PlanDiff,
    name_a: &str,
    name_b: &str,
    options: &ExportOptions,
) -> String {
    culprit_plans::export::diff_markdown(a, b, diff, name_a, name_b, options, &exported_at())
}

#[cfg(test)]
mod tests;
