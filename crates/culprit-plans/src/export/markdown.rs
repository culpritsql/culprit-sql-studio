//! Markdown plan/diff rendering and escaped text primitives with an explicit caller-owned footer time.
use super::{
    ExportNote, ExportOptions, estimated_cost, kind_inline, plan_engine_label, prepared, rows_text, share, shares_note,
    top_costs, where_,
};
use culprit_core::{ChangeKind, Plan, PlanDiff, PlanNode, format, t};
use std::fmt::Write;

const MAX_STEPS: usize = 40;
const NBSP: char = '\u{a0}';

/// A table cell: kept on one line, with HTML, Markdown and pipes escaped.
pub fn cell(raw: &str) -> String {
    text(raw)
}

/// Database-sourced text (names, titles built from them) outside code: HTML and Markdown that would render in a pull
/// request or wiki become plain characters. A table named `<img src=x onerror=…>` stays text.
pub fn text(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            // Characters Markdown would turn into links, images, emphasis or headings inside a line.
            '[' | ']' | '*' | '_' | '`' | '|' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' | '\r' => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

/// Inline code that survives backticks in the text.
pub fn code(text: &str) -> String {
    if text.contains('`') { format!("`` {text} ``") } else { format!("`{text}`") }
}

pub fn fence(out: &mut String, lang: &str, text: &str) {
    let ticks = if text.contains("```") { "````" } else { "```" };
    let _ = writeln!(out, "{ticks}{lang}\n{}\n{ticks}\n", text.trim_end());
}

pub fn redaction_note(out: &mut String, options: &ExportOptions) {
    if options.redact {
        let _ = writeln!(out, "> {}\n", t!("engine.export.redacted"));
    }
}

/// When it was exported, and the finding it is when it is one.
pub fn footer(out: &mut String, finding: Option<&str>, exported_at: &str) {
    let footer = match finding {
        Some(id) => t!("engine.export.md.footer_finding", when = text(exported_at), id = code(id)),
        None => t!("engine.export.md.footer", when = text(exported_at)),
    };
    let _ = writeln!(out, "<sub>{footer}</sub>");
}

/// The table's column of rows: estimated, or estimated and actual when the plan ran.
pub fn rows_column(plan: &Plan) -> String {
    if plan.kind == culprit_core::PlanKind::Actual {
        t!("engine.export.column.rows_actual")
    } else {
        t!("engine.export.column.rows_estimated")
    }
}

/// One plan: where the cost goes, the operators as an indented table, warnings, notes and the statement.
pub fn plan_markdown(plan: &Plan, title: &str, options: &ExportOptions, exported_at: &str) -> String {
    let plan = prepared(plan, options);
    let mut out = String::new();
    let count = plan.nodes().len();
    let _ = writeln!(out, "## {}\n", text(title.trim()));
    let line = t!(
        "engine.export.plan_line",
        engine = plan_engine_label(&plan),
        kind = kind_inline(plan.kind),
        n = count,
        count = count.to_string(),
        cost = estimated_cost(&plan)
    );
    let _ = writeln!(out, "{line}\n");
    redaction_note(&mut out, options);
    plan_body(&mut out, &plan, &options.notes);
    footer(&mut out, None, exported_at);
    out
}

fn plan_body(out: &mut String, plan: &Plan, notes: &[ExportNote]) {
    let costs: Vec<String> =
        top_costs(plan, 4).iter().map(|(n, s)| format!("**{}** {}", format::percent(*s), cell(&label(n)))).collect();
    if !costs.is_empty() {
        let _ = writeln!(out, "**{}** {}\n", t!("engine.export.md.where_cost_goes"), costs.join(" · "));
    }
    let hot = plan.hottest().map(|n| n.id);
    let _ = writeln!(
        out,
        "| {} | {} | {} | {} | {} |",
        t!("engine.export.column.step"),
        t!("engine.export.column.table_index"),
        t!("engine.export.column.cost"),
        rows_column(plan),
        t!("engine.export.column.notes")
    );
    let _ = writeln!(out, "|---|---|---:|---:|---|");
    let mut rows = Vec::new();
    walk(&plan.root, 0, &mut rows);
    for (node, depth) in rows.iter().take(MAX_STEPS) {
        let indent: String = std::iter::repeat_n(NBSP, depth * 3).collect();
        let arrow = if *depth > 0 { "↳ " } else { "" };
        let mut notes_cell: Vec<String> = node.warnings.iter().map(|w| w.label().to_string()).collect();
        if Some(node.id) == hot && share(plan, node).is_some_and(|s| s >= 0.05) {
            notes_cell.insert(0, t!("engine.export.most_expensive"));
        }
        // An exchange's cost is the charge for passing rows between threads: never the step where the work is.
        if node.kind == culprit_core::NodeKind::Parallelism && share(plan, node).is_some_and(|s| s >= 0.05) {
            notes_cell.insert(0, t!("engine.export.passing_rows"));
        }
        let _ = writeln!(
            out,
            "| {indent}{arrow}{} | {} | {} | {} | {} |",
            cell(&node.op),
            cell(&where_(node)),
            // No share where the plan can't state one (no costs, a step it doesn't cost, a default guess).
            share(plan, node).map(format::percent).unwrap_or_else(|| "—".into()),
            rows_text(node),
            cell(&notes_cell.join(", "))
        );
    }
    if rows.len() > MAX_STEPS {
        let more = rows.len() - MAX_STEPS;
        let _ = writeln!(out, "\n_{}_", t!("engine.export.md.more_operators", n = more, count = more.to_string()));
    }
    let _ = writeln!(out);
    if let Some(note) = shares_note(plan, None) {
        let _ = writeln!(out, "_{}_\n", text(&note.replace('`', "")));
    }
    let predicates: Vec<String> = rows
        .iter()
        .filter(|(n, _)| share(plan, n).is_some_and(|s| s >= 0.05))
        .flat_map(|(n, _)| {
            let mut lines = Vec::new();
            if let Some(p) = &n.seek_predicate {
                let line =
                    t!("engine.export.seeks_on", step = cell(&label(n)), predicate = code(&format::predicate(p)));
                lines.push(format!("- {line}"));
            }
            if let Some(p) = &n.predicate {
                let line = t!("engine.export.filters", step = cell(&label(n)), predicate = code(&format::predicate(p)));
                lines.push(format!("- {line}"));
            }
            lines
        })
        .collect();
    if !predicates.is_empty() {
        let _ = writeln!(out, "**{}**\n\n{}\n", t!("engine.export.predicates_heading"), predicates.join("\n"));
    }
    if !plan.warnings.is_empty() {
        let names: Vec<String> = plan.warnings.iter().map(|w| w.label()).collect();
        let _ = writeln!(out, "**{}** {}\n", t!("engine.export.md.plan_warnings"), names.join(", "));
    }
    if !plan.index_hints.is_empty() {
        let _ = writeln!(out, "**{}**\n", t!("engine.export.index_hints_heading"));
        for h in &plan.index_hints {
            let keys: Vec<String> = h.equality.iter().chain(h.inequality.iter()).cloned().collect();
            let (table, keys, percent) = (code(&h.table), keys.join(", "), format::percent(h.impact_pct / 100.0));
            let hint = if h.include.is_empty() {
                t!("engine.export.index_hint", table = table, keys = keys, percent = percent)
            } else {
                let columns = h.include.join(", ");
                t!("engine.export.index_hint_include", table = table, keys = keys, columns = columns, percent = percent)
            };
            let _ = writeln!(out, "- {hint}");
        }
        let _ = writeln!(out);
    }
    notes_section(out, notes, plan);
    if let Some(statement) = &plan.statement {
        let _ = writeln!(out, "<details><summary>{}</summary>\n", t!("engine.export.statement"));
        fence(out, "sql", statement);
        let _ = writeln!(out, "</details>\n");
    }
}

pub fn notes_section(out: &mut String, notes: &[ExportNote], plan: &Plan) {
    if notes.is_empty() {
        return;
    }
    let _ = writeln!(out, "**{}**\n", t!("engine.export.insights"));
    for n in notes {
        let on = n
            .node
            .and_then(|id| plan.nodes().into_iter().find(|x| x.id == id))
            .map(|x| format!(" ({})", label(x)))
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "- **{}**{on}{}",
            cell(&n.title),
            n.detail.as_deref().map(|d| format!(" — {d}")).unwrap_or_default()
        );
        if let Some(fix) = &n.fix {
            let _ = writeln!(out, "  {}", t!("engine.export.note_fix", fix = fix));
        }
    }
    let _ = writeln!(out);
}

fn walk<'a>(node: &'a PlanNode, depth: usize, out: &mut Vec<(&'a PlanNode, usize)>) {
    out.push((node, depth));
    for c in &node.children {
        walk(c, depth + 1, out);
    }
}

fn label(node: &PlanNode) -> String {
    super::node_label(node)
}

/// Two plans compared: the lead sentence, the numbers side by side, what changed, why the optimizer may have chosen
/// differently, and each plan's steps.
pub fn diff_markdown(
    a: &Plan,
    b: &Plan,
    diff: &PlanDiff,
    name_a: &str,
    name_b: &str,
    options: &ExportOptions,
    exported_at: &str,
) -> String {
    let redacted_diff = options.redact.then(|| crate::compare_redacted(a, name_a, b, name_b));
    let (a, b) = (prepared(a, options), prepared(b, options));
    let diff = redacted_diff.as_ref().unwrap_or(diff);
    let mut out = String::new();
    let _ = writeln!(out, "## {}\n", t!("engine.export.plan_comparison", a = name_a, b = name_b));
    redaction_note(&mut out, options);
    if let Some((lead, rest)) = diff.headlines.split_first() {
        let _ = writeln!(out, "**{}**\n", lead);
        for h in rest {
            let _ = writeln!(out, "- {h}");
        }
        if !rest.is_empty() {
            let _ = writeln!(out);
        }
    }
    let _ = writeln!(out, "| | {} | {} |\n|---|---:|---:|", cell(name_a), cell(name_b));
    let row = |label: String, a: String, b: String| format!("| {label} | {a} | {b} |");
    let _ = writeln!(
        out,
        "{}",
        row(t!("engine.export.row.engine"), plan_engine_label(&a).into(), plan_engine_label(&b).into())
    );
    let _ = writeln!(out, "{}", row(t!("engine.export.plan"), kind_inline(a.kind), kind_inline(b.kind)));
    let _ = writeln!(out, "{}", row(t!("engine.export.row.estimated_cost"), estimated_cost(&a), estimated_cost(&b)));
    if let (Some(ta), Some(tb)) = (diff.time_a_ms, diff.time_b_ms) {
        let _ =
            writeln!(out, "{}", row(t!("engine.export.row.actual_time"), format::duration(ta), format::duration(tb)));
    }
    let operators = row(t!("engine.export.row.operators"), a.nodes().len().to_string(), b.nodes().len().to_string());
    let _ = writeln!(out, "{operators}\n");
    if !diff.changes.is_empty() {
        let _ = writeln!(out, "### {}\n", t!("engine.export.what_changed"));
        for c in diff.changes.iter().take(20) {
            let detail = c.detail.as_deref().map(|d| format!(" — {d}")).unwrap_or_default();
            let _ = writeln!(out, "- **{}** {}{}", change_word(c.kind), text(&c.title), text(&detail));
        }
        let _ = writeln!(out);
    }
    if !diff.scenarios.is_empty() {
        let _ = writeln!(out, "### {}\n", t!("engine.export.why_differently"));
        for s in &diff.scenarios {
            let detail = s.detail.as_deref().map(|d| format!(" {d}")).unwrap_or_default();
            let _ = writeln!(out, "- **{}**{}", text(&s.title), text(&detail));
        }
        let _ = writeln!(out);
    }
    for (name, plan) in [(name_a, &a), (name_b, &b)] {
        let count = plan.nodes().len();
        let summary = t!("engine.export.md.plan_operators", name = text(name), n = count, count = count.to_string());
        let _ = writeln!(out, "<details><summary>{summary}</summary>\n");
        plan_body(&mut out, plan, &[]);
        let _ = writeln!(out, "</details>\n");
    }
    notes_section(&mut out, &options.notes, &b);
    footer(&mut out, None, exported_at);
    out
}

/// What kind of change a comparison found: "Access", "Cost".
pub fn change_word(kind: ChangeKind) -> String {
    match kind {
        ChangeKind::Access => t!("engine.export.change.access"),
        ChangeKind::Operator => t!("engine.export.change.operator"),
        ChangeKind::Added => t!("engine.export.change.added"),
        ChangeKind::Removed => t!("engine.export.change.removed"),
        ChangeKind::Cost => t!("engine.export.change.cost"),
        ChangeKind::Rows => t!("engine.export.change.rows"),
        ChangeKind::Warning => t!("engine.export.change.warning"),
    }
}

#[cfg(test)]
mod tests {
    use super::{cell, text};

    #[test]
    fn database_text_stays_text() {
        assert_eq!(text("T <img src=x onerror=alert(1)>"), "T &lt;img src=x onerror=alert(1)&gt;");
        assert_eq!(text("[click](https://example.com)"), "\\[click\\](https://example.com)");
        assert_eq!(text("order_items is *hot*"), "order\\_items is \\*hot\\*");
        assert_eq!(cell("a | b\nc"), "a \\| b c");
    }
}
