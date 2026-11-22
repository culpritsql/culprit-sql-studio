//! Source-free plan/diff export rendering. The caller supplies the clock; renderers perform no I/O or analysis.
pub mod html;
pub mod markdown;
mod shares;
pub mod svg;
pub use culprit_core::export::{ExportFile, ExportFormat, ExportNote, ExportOptions};
use culprit_core::redact::{redact_native, redact_plan, redact_sql};
use culprit_core::{Engine, Error, Plan, PlanDiff, PlanKind, PlanNode, Result, t};
pub use html::{diff_html, plan_html};
pub use markdown::{diff_markdown, plan_markdown};
pub use shares::{finding_no_share_reason, no_share_reason, shares_note};
use std::borrow::Cow;

pub fn plan_native(plan: &Plan, options: &ExportOptions) -> Option<String> {
    let raw = plan.raw.as_deref()?;
    Some(if options.redact { redact_native(raw, plan.engine) } else { raw.to_string() })
}

pub fn native_extension(plan: &Plan) -> &'static str {
    let raw = plan.raw.as_deref().unwrap_or("").trim_start();
    if raw.starts_with('<') {
        "sqlplan"
    } else if raw.starts_with('{') || raw.starts_with('[') {
        "json"
    } else {
        "txt"
    }
}

pub fn file_stem(title: &str) -> String {
    let mut out = String::new();
    for c in title.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
        if out.len() >= 60 {
            break;
        }
    }
    let out = out.trim_end_matches('-').to_string();
    if out.is_empty() { "culprit".into() } else { out }
}

pub fn prepared<'a>(plan: &'a Plan, options: &ExportOptions) -> Cow<'a, Plan> {
    if options.redact { Cow::Owned(redact_plan(plan)) } else { Cow::Borrowed(plan) }
}

pub fn sql_text(text: &str, engine: Engine, options: &ExportOptions) -> String {
    if options.redact { redact_sql(text.trim(), engine) } else { text.trim().to_string() }
}

pub fn plan_engine_label(plan: &Plan) -> &'static str {
    let mariadb =
        plan.engine == Engine::Mysql && plan.facts().flavor.as_ref().is_some_and(|f| f.eq_ignore_ascii_case("mariadb"));
    if mariadb { "MariaDB" } else { plan.engine.label() }
}

pub fn kind_label(kind: PlanKind) -> String {
    match kind {
        PlanKind::Estimated => t!("common.plan_kind.estimated"),
        PlanKind::Generic => t!("common.plan_kind.generic"),
        PlanKind::Actual => t!("common.plan_kind.actual"),
    }
}

pub fn kind_inline(kind: PlanKind) -> String {
    match kind {
        PlanKind::Estimated => t!("engine.export.plan_kind_inline.estimated"),
        PlanKind::Generic => t!("engine.export.plan_kind_inline.generic"),
        PlanKind::Actual => t!("engine.export.plan_kind_inline.actual"),
    }
}

pub fn share(plan: &Plan, node: &PlanNode) -> Option<f64> {
    plan.step_share(node)
}

pub fn rows_out(node: &PlanNode) -> Option<f64> {
    node.act_rows.or_else(|| (!node.facts().estimated_rows_unavailable).then(|| node.est_rows_total()))
}

pub fn estimated_cost(plan: &Plan) -> String {
    if plan.facts().estimated_cost_unavailable {
        t!("engine.export.unavailable")
    } else if plan.total_cost < 10.0 {
        format!("{:.3}", plan.total_cost)
    } else {
        culprit_core::format::count(plan.total_cost)
    }
}

pub fn table(node: &PlanNode) -> Option<String> {
    let object = node.object.as_deref()?;
    Some(object.rsplit('.').next().unwrap_or(object).chars().filter(|c| !matches!(c, '[' | ']' | '"' | '`')).collect())
}

pub fn where_(node: &PlanNode) -> String {
    match (table(node), node.index.as_deref()) {
        (Some(t), Some(i)) => format!("{t} · {i}"),
        (Some(t), None) => t,
        (None, _) => node.detail.clone().unwrap_or_default(),
    }
}

pub fn rows_text(node: &PlanNode) -> String {
    if node.facts().estimated_rows_unavailable {
        return node.act_rows.map_or_else(
            || t!("engine.export.unavailable"),
            |act| format!("{} → {}", t!("engine.export.unavailable"), culprit_core::format::count(act)),
        );
    }
    let est = node.est_rows_total();
    match node.act_rows {
        Some(act) => format!("{} → {}", culprit_core::format::count(est), culprit_core::format::count(act)),
        None => culprit_core::format::count(est),
    }
}

pub fn rows_phrase(node: &PlanNode) -> String {
    let Some(last) = rows_out(node) else { return t!("engine.export.row_count_unavailable") };
    t!("engine.export.rows_phrase", n = last.round(), rows = rows_text(node))
}

pub fn top_costs(plan: &Plan, limit: usize) -> Vec<(&'_ PlanNode, f64)> {
    let mut list: Vec<(&PlanNode, f64)> = plan.nodes().into_iter().filter_map(|n| Some((n, share(plan, n)?))).collect();
    list.sort_by(|a, b| b.1.total_cmp(&a.1));
    list.into_iter().filter(|(_, s)| *s >= 0.01).take(limit).collect()
}

pub fn node_label(node: &PlanNode) -> String {
    match table(node) {
        Some(table) => t!("engine.export.step_on", op = &node.op, table = table),
        None => node.op.clone(),
    }
}

/// A plan as Markdown, HTML or its native retained text. The explicit clock is presentation only.
pub fn export_plan(
    plan: &Plan,
    title: &str,
    format: ExportFormat,
    options: &ExportOptions,
    exported_at: &str,
) -> Result<ExportFile> {
    let stem = file_stem(title);
    Ok(match format {
        ExportFormat::Html => {
            ExportFile { name: format!("{stem}.html"), contents: plan_html(plan, title, options, exported_at), format }
        }
        ExportFormat::Markdown => ExportFile {
            name: format!("{stem}.md"),
            contents: plan_markdown(plan, title, options, exported_at),
            format,
        },
        ExportFormat::Native => {
            let contents = plan_native(plan, options).ok_or_else(|| {
                Error::invalid(t!("engine.share.no_native")).with_hint(t!("engine.share.no_native_hint"))
            })?;
            ExportFile { name: format!("{stem}.{}", native_extension(plan)), contents, format }
        }
    })
}

/// Borrowed identity of one comparison. This is neither serialized state nor native execution authority.
pub struct DiffExport<'a> {
    pub before: &'a Plan,
    pub after: &'a Plan,
    pub difference: &'a PlanDiff,
    pub before_name: &'a str,
    pub after_name: &'a str,
}

/// Render one supplied comparison with a caller-owned clock and redaction policy.
pub fn export_diff(
    comparison: &DiffExport<'_>,
    format: ExportFormat,
    options: &ExportOptions,
    exported_at: &str,
) -> Result<ExportFile> {
    let DiffExport { before: a, after: b, difference: diff, before_name: name_a, after_name: name_b } = *comparison;
    let stem = file_stem(&format!("compare {name_a} {name_b}"));
    Ok(match format {
        ExportFormat::Html => ExportFile {
            name: format!("{stem}.html"),
            contents: diff_html(a, b, diff, name_a, name_b, options, exported_at),
            format,
        },
        _ => ExportFile {
            name: format!("{stem}.md"),
            contents: diff_markdown(a, b, diff, name_a, name_b, options, exported_at),
            format: ExportFormat::Markdown,
        },
    })
}

#[cfg(test)]
mod tests;
