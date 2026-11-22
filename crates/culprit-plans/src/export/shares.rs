//! One cost-share explanation for plan views, brief findings, native exports and portable exports.
use culprit_core::{Plan, PlanNode, QueryStat, format, t};

/// Why a plan that weighs its steps states no share of its cost for step `node` (`Plan::step_share`), as a sentence to
/// follow the step's name; `None` where it states one, or on a plan without costs (which has no shares to explain).
/// The true reason, with what the statement `q`'s runs measured where it is known: a plan costed for far fewer rows
/// than its runs handle isn't "cheap", and nor is one that reads a whole table big enough to matter on every run (a
/// catalog's or a small table's whole read is no reason, and isn't said). Every surface that says
/// why a step has no share (the brief, the exports, the plan viewer, the CLI's tree) says it from here; the app
/// consumes it through `Plan.view`.
pub fn no_share_reason(plan: &Plan, node: &PlanNode, q: Option<&QueryStat>) -> Option<String> {
    if !plan.weighed() || plan.step_share(node).is_some() {
        return None;
    }
    let uncosted = plan.uncosted_steps();
    if !uncosted.is_empty() {
        let steps: Vec<String> = uncosted.iter().map(|n| step_name(n)).collect();
        return Some(t!("view.plan_shares.uncosted_steps", n = steps.len(), steps = format::list(&steps)));
    }
    match plan.guess(node) {
        Some(culprit_core::Guess::Rows) => return Some(t!("view.plan_shares.guessed_rows")),
        Some(culprit_core::Guess::Runs) => return Some(t!("view.plan_shares.guessed_runs")),
        None => {}
    }
    if !culprit_core::insights::does_real_work(plan) {
        return Some(cheap_plan_reason(plan, q));
    }
    Some(t!("view.plan_shares.uncosted_step", step = step_name(node)))
}

/// Why a finding's step states no share ([`finding_step_share`]): [`no_share_reason`], except that a step the finding
/// is about which reads its table whole on a cheap plan is said as that read, whatever the table's size. The finding is
/// what shows that read matters; the size bar of `insights::whole_read_matters` is for plans no finding points at.
/// Where its runs measured a misestimate, that stays the reason. Included in the app's `Plan.view` projection.
pub fn finding_no_share_reason(plan: &Plan, node: &PlanNode, q: Option<&QueryStat>) -> Option<String> {
    use culprit_core::insights::CheapPlanRuns;
    let why = no_share_reason(plan, node, q)?;
    let cheap = plan.uncosted_steps().is_empty()
        && !plan.has_guess()
        && !culprit_core::insights::does_real_work(plan)
        && culprit_core::insights::estimated_work_threshold(plan).is_some();
    let misestimated = matches!(
        culprit_core::insights::cheap_plan_runs(plan, q),
        CheapPlanRuns::MoreRows { .. } | CheapPlanRuns::MoreReads { .. }
    );
    Some(if cheap && !misestimated && node.reads_table_whole() { whole_read_reason(plan, node) } else { why })
}

/// "…too small for a share…, but `Seq Scan` reads all 3.9K rows of public.apps on every run."
fn whole_read_reason(plan: &Plan, scan: &PlanNode) -> String {
    let cost = cost_text(plan.total_cost.max(plan.root.subtree_cost));
    let table = if scan.object.is_some() { "named" } else { "unidentified" };
    let object = scan.object.as_deref().unwrap_or_default();
    match scan.table_rows.filter(|rows| *rows > 0.0) {
        Some(rows) => t!(
            "view.plan_shares.whole_rows",
            cost = cost,
            operator = &scan.op,
            table = table,
            object = object,
            n = rows,
            count = format::count(rows)
        ),
        None => t!(
            "view.plan_shares.whole_unmeasured_rows",
            cost = cost,
            operator = &scan.op,
            table = table,
            object = object
        ),
    }
}

/// Why no step of a plan that weighs its steps shows a share of its cost, said once under the plan wherever its steps
/// are listed (the plan viewer, the CLI's tree, the exports, MCP, the readings); `None` when every costed step shows
/// one. `q` is the statement it was planned for, where known: its runs say whether a cheap plan's runs are cheap.
pub fn shares_note(plan: &Plan, q: Option<&QueryStat>) -> Option<String> {
    if !plan.weighed() {
        return None;
    }
    if !plan.uncosted_steps().is_empty() {
        return no_share_reason(plan, &plan.root, q);
    }
    if !culprit_core::insights::does_real_work(plan) {
        return Some(cheap_plan_reason(plan, q));
    }
    let guesses = plan.guesses();
    if guesses.is_empty() {
        return None;
    }
    Some(if guesses.iter().any(|(_, guess)| *guess == culprit_core::Guess::Runs) {
        t!("view.plan_shares.guess_note_repeated")
    } else {
        t!("view.plan_shares.guess_note")
    })
}

/// Why a plan that does no real work puts a share of its cost on no step, as its statement's runs measured it
/// (`insights::cheap_plan_runs`): its costs rest on a guess, on an estimate its runs far exceed, or under the bar while
/// a scan reads a whole table that can matter (`insights::whole_read_matters`: `SCAN_ROWS` rows or more, not the
/// catalog's), or its runs are slow; else the plan is cheap.
fn cheap_plan_reason(plan: &Plan, q: Option<&QueryStat>) -> String {
    use culprit_core::insights::CheapPlanRuns;
    if culprit_core::insights::estimated_work_threshold(plan).is_none() {
        return t!("view.plan_shares.uncalibrated");
    }
    // Whether the plan is cheap rests on its total, and a guess anywhere is in the total.
    if plan.has_guess() {
        return t!("view.plan_shares.guessed_costs");
    }
    match culprit_core::insights::cheap_plan_runs(plan, q) {
        CheapPlanRuns::MoreRows { expected, rows: actual } => t!(
            "view.plan_shares.more_rows",
            expected_n = expected,
            expected = format::count(expected),
            action = if q.is_some_and(QueryStat::writes_rows) { "write" } else { "read" },
            actual_n = actual,
            actual = format::count(actual)
        ),
        CheapPlanRuns::MoreReads { reads } => t!(
            "view.plan_shares.more_reads",
            n = reads.round(),
            reads = format::count(reads.round()),
            unit = read_unit_for_message(plan)
        ),
        CheapPlanRuns::ScansTable(id) => match plan.nodes().into_iter().find(|node| node.id == id) {
            Some(scan) => whole_read_reason(plan, scan),
            None => {
                t!("view.plan_shares.whole_missing_step", cost = cost_text(plan.total_cost.max(plan.root.subtree_cost)))
            }
        },
        CheapPlanRuns::Slow(ms) => slow_plan_reason(q, ms),
        CheapPlanRuns::Cheap | CheapPlanRuns::Unmeasured | CheapPlanRuns::NotCheap => t!("view.plan_shares.cheap"),
    }
}

/// Protocol selectors come from the existing native metric facts, never translated unit labels.
fn read_unit_for_message(plan: &Plan) -> &'static str {
    match plan.engine.spec().statement_metrics().reads {
        culprit_core::engines::ReadUnit::Pages => "pages",
        culprit_core::engines::ReadUnit::RowsExamined => "rows_examined",
    }
}

/// What the captured plan's completed runs took. A handful with one outlier is said as a total and a maximum,
/// rather than an average that describes none of them. Past tense also holds for statements that stopped.
fn slow_plan_reason(q: Option<&QueryStat>, mean: f64) -> String {
    let average = || t!("view.plan_shares.slow_average", duration = format::duration(mean));
    let Some(q) = q else { return average() };
    let (runs, total, max) = match q.captured_plan_variant() {
        Some(plan) => (plan.executions, plan.total_time_ms, plan.max_time_ms),
        None => (q.executions, q.total_time_ms, q.max_time_ms),
    };
    if runs == 1.0 {
        return t!("view.plan_shares.slow_one", duration = format::duration(total));
    }
    if let Some(max) = max.filter(|_| total > 0.0).map(|maximum| maximum.clamp(0.0, total)) {
        if runs == 2.0 && max >= (total - max) * 2.0 {
            return t!(
                "view.plan_shares.slow_two",
                slowest = format::duration(max),
                other = format::duration(total - max)
            );
        }
        if runs <= 10.0 && max >= mean * 2.0 {
            return t!(
                "view.plan_shares.slow_outlier",
                n = runs,
                runs = format::count(runs),
                total = format::duration(total),
                slowest = format::duration(max)
            );
        }
    }
    average()
}

/// A plan's estimated cost as the engine writes it: whole units from 10, else three significant digits (SQL Server's
/// point lookups cost hundredths).
fn cost_text(cost: f64) -> String {
    if cost >= 10.0 {
        format::digits(format!("{cost:.0}"))
    } else {
        let digits = (2 - cost.max(1e-9).log10().floor() as i32).clamp(0, 6) as usize;
        format::digits(format!("{cost:.digits$}"))
    }
}

/// "`Sort` (using filesort)": a step and what it does.
fn step_name(node: &PlanNode) -> String {
    match node.detail.as_deref() {
        Some(detail) => format!("`{}` ({})", node.op, detail.replace('`', "")),
        None => format!("`{}`", node.op),
    }
}

#[cfg(test)]
mod tests;
