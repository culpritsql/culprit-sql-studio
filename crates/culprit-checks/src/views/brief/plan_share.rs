//! Plan step selection and step cost-share explanations shared by the agent brief, plan viewer (`Plan.view`), CLI tree,
//! and exports.

use super::*;

/// The step of `plan` a reader of `finding` looks at, for the statement `r` refers to: the step the finding names, else,
/// for a finding about where a statement's plan does its work, the plan's working step, where the plan can state its
/// share. None for a finding about something else (a result's size, how often a statement runs, what it waits for): the
/// plan's costliest step isn't what it is about, and naming it would point the reader at a step the finding never
/// weighed. Every surface that shows a finding's step (the brief, the exports) picks it here.
pub fn finding_step<'a>(finding: &Finding, r: &QueryRef, plan: &'a Plan) -> Option<&'a PlanNode> {
    if let Some(named) = r.plan_node.and_then(|id| plan.nodes().into_iter().find(|n| n.id == id)) {
        return Some(named);
    }
    if !culprit_core::plan_view::PLAN_FOCUS_CHECKS.contains(&finding.check.as_str()) {
        return None;
    }
    culprit_core::insights::working_step(plan).filter(|n| plan.step_share(n).is_some())
}

/// The share of its plan's cost a finding's step is said to have: `Plan::step_share`, which states one only in a plan
/// that does real work, as heavy-query says it (a cheap plan's costliest step isn't where its statement's time goes,
/// even where it reads a whole table), and never for a step whose cost rests on a guess. Every surface that shows a
/// finding's step (the brief, the exports, the app's finding view) states this share.
pub fn finding_step_share(plan: &Plan, node: &PlanNode) -> Option<f64> {
    plan.step_share(node)
}
