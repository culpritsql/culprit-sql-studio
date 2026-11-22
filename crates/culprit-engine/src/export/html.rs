//! Native finding/report pages reuse the pure escaped page/SVG primitives.
use super::markdown::metric_text;
use super::{
    ExportOptions, exported_at, finding_line, node_label, prepared, report_engine_label, rows_phrase, server_line,
    sql_text, stats_line,
};
use culprit_core::{Finding, Report, Severity, Unit, format, t};
use culprit_plans::export::html::{notes_html, page};
use culprit_plans::export::svg::{diagram, escape};
use std::collections::HashMap;
use std::fmt::Write;

fn severity(f: &Finding) -> &'static str {
    if !f.is_actionable() {
        return "low";
    }
    match f.severity {
        Severity::High => "high",
        Severity::Medium => "medium",
        Severity::Low => "low",
    }
}

/// `shared` is the report checked from its redacted snapshot when the export redacts (see `shared_report`).
fn finding_body(
    out: &mut String,
    report: &Report,
    shared: Option<&Report>,
    finding: &Finding,
    options: &ExportOptions,
    level: &str,
    index: usize,
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
    let _ = write!(out, r#"<section class="finding"><{level}>{}</{level}>"#, escape(&finding.title));
    let _ = write!(
        out,
        r#"<p class="sub"><span class="sev {}"></span>{}</p>"#,
        severity(finding),
        escape(&finding_line(report, finding, finding.impact.label.clone()))
    );
    let _ = write!(out, "<p>{}</p>", escape(&finding.summary));
    if !finding.metrics.is_empty() {
        out.push_str(r#"<div class="metrics">"#);
        for m in &finding.metrics {
            if matches!(m.unit, Unit::Text) {
                let _ = write!(
                    out,
                    r#"<div class="fig metric-text"><div class="l">{}</div><div class="v">{}</div></div>"#,
                    escape(&m.label),
                    escape(&metric_text(m))
                );
                continue;
            }
            let _ = write!(
                out,
                r#"<div class="fig"><div class="v">{}</div><div class="l">{}</div></div>"#,
                escape(&metric_text(m)),
                escape(&m.label)
            );
        }
        out.push_str("</div>");
    }
    let engine = s.server.engine;
    let queries: Vec<_> =
        finding.queries.iter().filter_map(|r| s.workload.as_ref()?.query(&r.id).map(|q| (r, q))).take(3).collect();
    for (qi, (r, q)) in queries.iter().enumerate() {
        let w = s.workload.as_ref().expect("filtered on workload");
        let _ = write!(
            out,
            r#"<h2>{}</h2><p class="qmeta">{}</p><pre>{}</pre>"#,
            escape(&t!("engine.export.query", id = &q.id)),
            escape(&stats_line(w, q)),
            escape(&sql_text(&q.text, engine, options))
        );
        if let Some(plan) = &q.plan {
            let plan = prepared(plan, options);
            // The step the finding names, else its plan's working step where the finding is about one.
            let focus = culprit_checks::finding_step(finding, r, &plan).map(|n| n.id);
            let label = t!("engine.export.page.plan_of_query", id = &q.id);
            let _ = write!(
                out,
                r#"<div class="wrap" style="margin-top:12px">{}</div>"#,
                diagram(&plan, &HashMap::new(), focus, &label, &format!("f{index}q{qi}"))
            );
            if let Some(node) = focus.and_then(|id| plan.nodes().into_iter().find(|n| n.id == id)) {
                let step = format!("<b>{}</b>", escape(&node_label(node)));
                let predicate = node
                    .predicate
                    .as_deref()
                    .or(node.seek_predicate.as_deref())
                    .map(|p| format!("<code>{}</code>", escape(&format::predicate(p))));
                // A share only where the plan can state one, and rows only where it estimates them.
                let share = plan.weighed().then(|| culprit_checks::finding_step_share(&plan, node));
                let rows = || escape(&rows_phrase(node));
                let said = match (share, predicate) {
                    (None, None) => t!("engine.export.page.step.plain", step = step),
                    (None, Some(predicate)) => t!("engine.export.page.step.on", step = step, predicate = predicate),
                    (Some(None), None) => t!("engine.export.page.step.rows", step = step, rows = rows()),
                    (Some(None), Some(predicate)) => {
                        t!("engine.export.page.step.rows_on", step = step, rows = rows(), predicate = predicate)
                    }
                    (Some(Some(s)), None) => {
                        t!("engine.export.page.step.share_rows", step = step, share = format::percent(s), rows = rows())
                    }
                    (Some(Some(s)), Some(predicate)) => t!(
                        "engine.export.page.step.share_rows_on",
                        step = step,
                        share = format::percent(s),
                        rows = rows(),
                        predicate = predicate
                    ),
                };
                let why = culprit_plans::export::finding_no_share_reason(&plan, node, Some(q))
                    .map(|w| format!(" {}", escape(&w.replace('`', ""))))
                    .unwrap_or_default();
                let _ = write!(out, r#"<p class="note" style="margin-top:8px">{said}{why}</p>"#);
            }
        }
    }
    if let Some(fix) = &finding.fix {
        let _ = write!(
            out,
            r#"<h2>{}</h2><div class="callout"><p>{}</p>"#,
            escape(&t!("engine.export.fix")),
            escape(&fix.summary)
        );
        if let Some(sql) = &fix.sql {
            let _ = write!(out, "<pre>{}</pre>", escape(sql.trim()));
        }
        if let Some(c) = &fix.cost {
            let _ = write!(
                out,
                r#"<p class="note" style="margin:10px 0 0">{}</p>"#,
                escape(&t!("engine.export.cost", cost = c))
            );
        }
        out.push_str("</div>");
    }
    if !finding.verify.is_empty() {
        let _ = write!(out, r#"<h2>{}</h2><ol class="verify">"#, escape(&t!("engine.export.how_to_verify")));
        for step in &finding.verify {
            let _ = write!(out, "<li>{}</li>", escape(step));
        }
        // Only where a re-scan can show it gone: not for statements that stopped, nor on running totals.
        if culprit_checks::gone_after_rescan(shared.unwrap_or(report), finding) {
            let command = format!("<code>culprit verify &lt;connection&gt; {}</code>", escape(&finding.id));
            let _ = write!(out, "<li>{}</li>", t!("engine.export.page.verify_after", command = command));
        }
        out.push_str("</ol>");
    }
    if !finding.caveats.is_empty() {
        let _ = write!(out, r#"<h2>{}</h2><ul class="more">"#, escape(&t!("engine.export.before_you_act")));
        for c in &finding.caveats {
            let _ = write!(out, "<li>{}</li>", escape(c));
        }
        out.push_str("</ul>");
    }
    if let Some(plan) = queries.first().and_then(|(_, q)| q.plan.as_ref()) {
        out.push_str(&notes_html(&options.notes, plan));
    }
    out.push_str("</section>");
}

/// One finding as a page: what's happening, the evidence, the query with its plan (the step to look at outlined), the
/// fix and how to verify it.
pub fn finding_html(report: &Report, finding: &Finding, options: &ExportOptions) -> String {
    let mut body = String::new();
    let shared = options.redact.then(|| super::shared_report(report));
    let title = match &shared {
        Some(shared) => super::shareable(report, shared, finding).title,
        None => finding.title.clone(),
    };
    finding_body(&mut body, report, shared.as_ref(), finding, options, "h1", 0);
    let chip = t!("engine.export.page.finding_chip", id = &finding.id);
    page(&title, &[report_engine_label(report).to_string(), chip], &body, options, &exported_at())
}

/// Every finding of a scan on one page, highest impact first.
pub fn report_html(report: &Report, options: &ExportOptions) -> String {
    let n = report.findings.len();
    let mut body = format!(
        r#"<h1>{}</h1><p class="sub">{}</p>"#,
        escape(&t!("engine.export.page.opportunities", n = n, count = n.to_string())),
        escape(&t!("engine.export.page.report_sub", server = server_line(report), when = &report.snapshot.taken_at))
    );
    if n == 0 {
        let _ = write!(body, "<p>{}</p>", escape(&t!("engine.export.nothing_stands_out")));
    } else {
        let shared = options.redact.then(|| super::shared_report(report));
        body.push_str(r#"<ul class="scen" style="margin-top:18px">"#);
        for f in &report.findings {
            let title = match &shared {
                Some(shared) => super::shareable(report, shared, f).title,
                None => f.title.clone(),
            };
            let _ = write!(
                body,
                r#"<li><span class="tag">{}</span><div><div class="t"><span class="sev {}"></span>{}</div></div></li>"#,
                escape(&f.impact.label),
                severity(f),
                escape(&title)
            );
        }
        body.push_str("</ul>");
        for (i, f) in report.findings.iter().take(10).enumerate() {
            finding_body(&mut body, report, shared.as_ref(), f, options, "h1", i + 1);
        }
    }
    page(
        &t!("engine.export.page.report_title"),
        &[report_engine_label(report).to_string()],
        &body,
        options,
        &exported_at(),
    )
}
