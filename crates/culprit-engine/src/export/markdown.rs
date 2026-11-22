//! Finding/report/verification wording retains its Checks and report-reanalysis owners.
use super::{
    ExportOptions, exported_at, finding_line, node_label, prepared, rows_phrase, server_line, sql_text, stats_line,
};
use culprit_core::{Finding, Metric, Report, Verification, VerifyStatus, format, t};
use culprit_plans::export::markdown::{cell, code, fence, footer, notes_section, redaction_note, text};
use std::fmt::Write;

/// A finding's metric as the brief says it (`culprit_checks::metric_text`): a total wait to the second, as its title.
pub(crate) fn metric_text(m: &Metric) -> String {
    culprit_checks::metric_text(m)
}

/// One finding in full for a digest (a chat thread, a ticket): the same pieces as [`finding_markdown`], always
/// redacted, as data for the consumer to lay out.
pub fn finding_detail(report: &Report, finding: &Finding) -> culprit_core::DigestDetail {
    let shared = super::shared_report(report);
    detail(report, super::shareable(report, &shared, finding))
}

/// Digest assembly already holds a report checked from redacted input; do not repeat analysis per finding.
pub(crate) fn finding_detail_shared(report: &Report, finding: &Finding) -> culprit_core::DigestDetail {
    detail(report, super::shareable(report, report, finding))
}

fn detail(report: &Report, f: Finding) -> culprit_core::DigestDetail {
    let s = &report.snapshot;
    let engine = s.server.engine;
    let queries = s
        .workload
        .as_ref()
        .map(|w| {
            f.queries
                .iter()
                .filter_map(|r| w.query(&r.id).map(|q| (r, q)))
                .take(2)
                .map(|(r, q)| {
                    let plan = q.plan.as_ref().and_then(|plan| {
                        let plan = culprit_core::redact::redact_plan(plan);
                        // The step the finding names, else its plan's working step where the finding is about one.
                        let node = culprit_checks::finding_step(&f, r, &plan)?;
                        // A plan without costs (SQLite) names the step, and no share or estimate it doesn't have; a
                        // share only where the plan can state one.
                        let mut line = match (plan.weighed(), culprit_checks::finding_step_share(&plan, node)) {
                            (true, Some(s)) => format!(
                                "{}: {} of the plan's cost, {}",
                                node_label(node),
                                format::percent(s),
                                rows_phrase(node)
                            ),
                            (true, None) => format!("{}: {}", node_label(node), rows_phrase(node)),
                            (false, _) => node_label(node),
                        };
                        if let Some(p) = node.predicate.as_deref().or(node.seek_predicate.as_deref()) {
                            let _ = write!(line, ", on `{}`", format::predicate(p));
                        }
                        Some(line)
                    });
                    culprit_core::DigestQuery {
                        stats: stats_line(w, q),
                        sql: culprit_core::redact::redact_sql(q.text.trim(), engine),
                        plan,
                        note: r.note.clone(),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    culprit_core::DigestDetail {
        confidence: format!("{:?}", f.confidence).to_lowercase(),
        summary: f.summary,
        metrics: f
            .metrics
            .iter()
            .take(6)
            .map(|m| culprit_core::DigestMetric { label: m.label.clone(), value: metric_text(m) })
            .collect(),
        queries,
        fix: f.fix,
        verify: f.verify,
        caveats: f.caveats,
    }
}

/// One finding as a PR section: what's happening, the evidence, the query and its plan, the fix and how to verify it.
pub fn finding_markdown(report: &Report, finding: &Finding, options: &ExportOptions) -> String {
    let mut out = String::new();
    let shared = options.redact.then(|| super::shared_report(report));
    finding_section(&mut out, report, shared.as_ref(), finding, options, "##");
    footer(&mut out, Some(&finding.id), &exported_at());
    out
}

/// `shared` is the report checked from its redacted snapshot when the export redacts (see `shared_report`).
fn finding_section(
    out: &mut String,
    report: &Report,
    shared: Option<&Report>,
    finding: &Finding,
    options: &ExportOptions,
    level: &str,
) {
    let s = &report.snapshot;
    let redacted;
    let finding = match shared {
        Some(shared) => {
            redacted = super::shareable(report, shared, finding);
            &redacted
        }
        None => finding,
    };
    let _ = writeln!(out, "{level} {}\n", text(&finding.title));
    let impact = if finding.impact.label.is_empty() { String::new() } else { format!("**{}**", finding.impact.label) };
    let _ = writeln!(out, "{}\n", finding_line(report, finding, impact));
    let _ = writeln!(out, "{}\n", text(&finding.summary));
    if !finding.metrics.is_empty() {
        for m in &finding.metrics {
            let _ = writeln!(out, "- {}: **{}**", text(&m.label), text(&metric_text(m)));
        }
        let _ = writeln!(out);
    }
    let engine = s.server.engine;
    let queries: Vec<_> =
        finding.queries.iter().filter_map(|r| s.workload.as_ref()?.query(&r.id).map(|q| (r, q))).take(3).collect();
    for (r, q) in &queries {
        let w = s.workload.as_ref().expect("filtered on workload");
        let _ = writeln!(out, "**{}** · {}\n", t!("engine.export.query", id = code(&q.id)), stats_line(w, q));
        fence(out, "sql", &sql_text(&q.text, engine, options));
        if let Some(plan) = &q.plan {
            let plan = prepared(plan, options);
            // The step the finding names, else its plan's working step where the finding is about one.
            if let Some(node) = culprit_checks::finding_step(finding, r, &plan) {
                let step = format!("**{}**", node_label(node));
                let predicate =
                    node.predicate.as_deref().or(node.seek_predicate.as_deref()).map(|p| code(&format::predicate(p)));
                let share = plan.weighed().then(|| culprit_checks::finding_step_share(&plan, node));
                let mut line = match (share, predicate) {
                    (None, None) => t!("engine.export.md.step.plain", step = step),
                    (None, Some(predicate)) => t!("engine.export.md.step.on", step = step, predicate = predicate),
                    (Some(None), None) => t!("engine.export.md.step.rows", step = step, rows = rows_phrase(node)),
                    (Some(None), Some(predicate)) => t!(
                        "engine.export.md.step.rows_on",
                        step = step,
                        rows = rows_phrase(node),
                        predicate = predicate
                    ),
                    (Some(Some(s)), None) => t!(
                        "engine.export.md.step.share_rows",
                        step = step,
                        share = format::percent(s),
                        rows = rows_phrase(node)
                    ),
                    (Some(Some(s)), Some(predicate)) => t!(
                        "engine.export.md.step.share_rows_on",
                        step = step,
                        share = format::percent(s),
                        rows = rows_phrase(node),
                        predicate = predicate
                    ),
                };
                // Where the plan can't state the step's share, why.
                if let Some(why) = culprit_plans::export::finding_no_share_reason(&plan, node, Some(q)) {
                    let _ = write!(line, " {}", text(&why.replace('`', "")));
                }
                let _ = writeln!(out, "{line}\n");
            }
        }
    }
    if let Some(fix) = &finding.fix {
        let _ = writeln!(out, "**{}** {}\n", t!("engine.export.md.fix"), text(&fix.summary));
        if let Some(sql) = &fix.sql {
            fence(out, "sql", sql);
        }
        if let Some(cost) = &fix.cost {
            let _ = writeln!(out, "_{}_ {}\n", t!("engine.export.md.cost"), text(cost));
        }
    }
    if !finding.verify.is_empty() {
        let _ = writeln!(out, "**{}**\n", t!("engine.export.how_to_verify"));
        for (i, step) in finding.verify.iter().enumerate() {
            let _ = writeln!(out, "{}. {step}", i + 1);
        }
        // Only where a re-scan can show it gone: not for statements that stopped, nor on running totals.
        if culprit_checks::gone_after_rescan(shared.unwrap_or(report), finding) {
            let command = format!("`culprit verify <connection> {}`", finding.id);
            let _ = writeln!(
                out,
                "{}. {}",
                finding.verify.len() + 1,
                t!("engine.export.md.verify_after", command = command)
            );
        }
        out.push('\n');
    }
    if !finding.caveats.is_empty() {
        let _ = writeln!(out, "**{}**\n", t!("engine.export.before_you_act"));
        for c in &finding.caveats {
            let _ = writeln!(out, "- {c}");
        }
        let _ = writeln!(out);
    }
    if let Some(plan) = queries.first().and_then(|(_, q)| q.plan.as_ref()) {
        notes_section(out, &options.notes, plan);
    }
}

/// Every finding of a scan: a summary table and the top findings in full.
pub fn report_markdown(report: &Report, options: &ExportOptions) -> String {
    let mut out = String::new();
    let s = &report.snapshot;
    let n = report.findings.len();
    let _ = writeln!(out, "## {}\n", t!("engine.export.md.report_title", n = n, count = n.to_string()));
    let _ =
        writeln!(out, "{}\n", t!("engine.export.md.report_scanned", server = server_line(report), when = &s.taken_at));
    redaction_note(&mut out, options);
    let shared = options.redact.then(|| super::shared_report(report));
    if report.findings.is_empty() {
        let _ = writeln!(out, "{}\n", t!("engine.export.nothing_stands_out"));
    } else {
        let (finding, impact) = (t!("engine.export.column.finding"), t!("engine.export.column.impact"));
        let _ = writeln!(out, "| | {finding} | {impact} |\n|---|---|---|");
        for f in &report.findings {
            let title = match &shared {
                Some(shared) => super::shareable(report, shared, f).title,
                None => f.title.clone(),
            };
            let _ = writeln!(out, "| {} | {} | {} |", severity_dot(f), cell(&title), cell(&f.impact.label));
        }
        let _ = writeln!(out);
        for f in report.findings.iter().take(5) {
            finding_section(&mut out, report, shared.as_ref(), f, options, "###");
        }
        if report.findings.len() > 5 {
            let other = report.findings.len() - 5;
            let line = t!(
                "engine.export.md.other_findings",
                n = other,
                count = other.to_string(),
                command = "`culprit show <connection> <id>`"
            );
            let _ = writeln!(out, "_{line}_\n");
        }
    }
    footer(&mut out, None, &exported_at());
    out
}

fn severity_dot(f: &Finding) -> &'static str {
    if !f.is_actionable() {
        return "○";
    }
    match f.severity {
        culprit_core::Severity::High => "🔴",
        culprit_core::Severity::Medium => "🟠",
        culprit_core::Severity::Low => "🔵",
    }
}

/// The verification as Markdown, for a PR comment after the fix shipped.
pub fn verification_markdown(v: &Verification) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "**{}**\n", text(&v.summary));
    for f in v.findings.iter().chain(v.new_findings.iter()) {
        let mark = match f.status {
            VerifyStatus::Fixed => format!("✅ {}", t!("engine.export.verify.fixed")),
            VerifyStatus::StillPresent => format!("⏳ {}", t!("engine.export.verify.still_present")),
            VerifyStatus::Unknown => format!("❔ {}", t!("engine.export.verify.unknown")),
            VerifyStatus::New => format!("🆕 {}", t!("engine.export.verify.new")),
        };
        let impact = match (&f.before, &f.after) {
            (Some(b), Some(a)) if b != a => format!(" ({b} → {a})"),
            (Some(b), None) => format!(" ({})", t!("engine.export.verify.was", before = b)),
            (_, Some(a)) => format!(" ({a})"),
            _ => String::new(),
        };
        let _ = writeln!(out, "- {mark}: {}{}", text(&f.title), text(&impact));
        if let Some(note) = &f.note {
            let _ = writeln!(out, "  {}", text(note));
        }
    }
    let (before, after) = culprit_core::changes::range_times(&v.before_taken_at, &v.after_taken_at);
    let _ = writeln!(out, "\n<sub>{}</sub>", t!("engine.export.md.scans", before = before, after = after));
    out
}
