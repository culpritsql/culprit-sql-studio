//! The persisted answer keeps native input kind separate from execution or harmful-reuse attribution.
use super::*;

pub(super) fn append(answer: &RelationalInputAnswer, reasons: &mut Vec<String>) {
    reasons.push(match &answer.selection {
        RelationalInputSelection::Observed { input_proof, retained_plan_available, .. } => {
            let (tvps, variables) = input_proof
                .as_ref()
                .map(|proof| {
                    use culprit_core::plan_facts::RelationalInputKind;
                    (
                        proof
                            .bindings
                            .iter()
                            .filter(|binding| {
                                binding.input.qualified_kind() == RelationalInputKind::TableValuedParameter
                            })
                            .count(),
                        proof
                            .bindings
                            .iter()
                            .filter(|binding| binding.input.qualified_kind() == RelationalInputKind::LocalTableVariable)
                            .count(),
                    )
                })
                .unwrap_or((0, 0));
            if input_proof.is_some() {
                t!(
                    "view.relational_input_question.inputs",
                    tvps = format::count(tvps as f64),
                    variables = format::count(variables as f64)
                )
            } else if *retained_plan_available {
                t!("view.relational_input_question.input_unknown")
            } else {
                t!("view.relational_input_question.plan_unavailable")
            }
        }
        RelationalInputSelection::NotObserved => t!("view.relational_input_question.not_observed"),
        RelationalInputSelection::IncarnationChanged => t!("view.relational_input_question.changed"),
    });
    reasons.push(t!(
        "view.relational_input_question.bounds",
        from = &answer.cache_observed_from,
        until = &answer.cache_observed_until,
        result_from = &answer.result_context.observed_from,
        result_until = &answer.result_context.observed_until
    ));
    reasons.push(t!("view.relational_input_question.execution_limit"));
    reasons.extend(answer.unresolved.iter().map(|gap| match gap {
        RelationalInputGap::AffectedExecutionPlan => t!("view.relational_input_question.gap.plan"),
        RelationalInputGap::RuntimeParameterValues => t!("view.relational_input_question.gap.values"),
        RelationalInputGap::RuntimeInputCardinality => t!("view.relational_input_question.gap.rows"),
        RelationalInputGap::ComparableExecutionWork => t!("view.relational_input_question.gap.work"),
        RelationalInputGap::BlockingAndGrantDemand => t!("view.relational_input_question.gap.waits"),
        RelationalInputGap::StatisticsAndForcingContext => t!("view.relational_input_question.gap.statistics"),
    }));
}
