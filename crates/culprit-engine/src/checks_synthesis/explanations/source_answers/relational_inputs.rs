//! A later retained plan supplies context and candidates, never the affected request's executing plan.
use super::*;
use culprit_core::insights::{RetainedParameterMechanism, retained_parameter_mechanism};

pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &RelationalInputAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    if !answer.is_valid() {
        return Err(invalid());
    }
    let RelationalInputSelection::Observed { observation, input_proof, .. } = &answer.selection else { return Ok(()) };
    let Some((original, position)) = resolve(originals, observation)? else { return Ok(()) };
    if original.original() != &answer.result
        || original.source().observation.execution_context() != Some(&answer.result_context)
    {
        return Err(invalid());
    }
    let TelemetryRecord::ObjectDefinition(definition) = &original.records()[position].row().record else {
        return Err(invalid());
    };
    let ObjectDefinitionFact::MssqlRetainedPlan(native) = &definition.fact else { return Err(invalid()) };
    if native.relational_input_proof.as_ref() != input_proof.as_deref() {
        return Err(invalid());
    }
    let entry = crate::collected_resources::explain_cached_plan_original(
        original.source(),
        SnapshotDataset::RetainedPlans,
        definition,
    )?;
    for limitation in entry.limitations {
        push_once(&mut explanation.limitations, limitation.explanation);
    }
    if let Some(proof) = input_proof
        && !proof.matches_retained(original.source(), native)
    {
        return Err(invalid());
    }
    if let Some(proof) = &native.parameter_context_proof
        && !proof.matches_retained(original.source(), native)
    {
        return Err(invalid());
    }
    let Some(plan) = entry.plan else { return Ok(()) };

    let qualified_relational = input_proof
        .as_ref()
        .map(|proof| {
            plan.nodes()
                .into_iter()
                .filter(|node| {
                    proof.bindings.iter().any(|binding| {
                        binding.node_id == node.id
                            && node.facts().relational_input == Some(binding.input)
                            && binding.input.qualified_kind() != culprit_core::plan_facts::RelationalInputKind::Unknown
                    })
                })
                .map(|node| node.id)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();

    let parameter_mechanism =
        native.parameter_context_proof.is_some().then(|| retained_parameter_mechanism(&plan)).flatten();
    if qualified_relational.is_empty() && parameter_mechanism.is_none() {
        return Ok(());
    }

    explanation.candidates.push(DerivedIncidentQuery {
        causal_role: None,
        identity: Some(DerivedQueryIdentity::CachedStatement(native.context.clone())),
        sql: native.statement.payload.clone(),
        caller: None,
        evidence: vec![observation.as_ref().clone(), answer.work.clone()],
    });

    let mut mechanisms = Vec::new();
    let mut actions = Vec::new();
    let mut verifies = Vec::new();

    if !qualified_relational.is_empty() {
        let all_items = plan.insights.as_ref().map(|insights| insights.items.as_slice()).unwrap_or(&[]);
        let relational_insights = all_items
            .iter()
            .filter(|insight| insight.nodes.iter().any(|node| qualified_relational.contains(node)))
            .collect::<Vec<_>>();
        let competing_insights = all_items
            .iter()
            .filter(|insight| {
                !insight.nodes.iter().any(|node| qualified_relational.contains(node))
                    && matches!(
                        insight.rule.as_str(),
                        "implicit-conversion"
                            | "spill"
                            | "memory-grant"
                            | "memory-grant-feedback"
                            | "misestimate"
                            | "nested-loops"
                    )
            })
            .collect::<Vec<_>>();
        let details = relational_insights
            .iter()
            .chain(&competing_insights)
            .map(|insight| insight.detail.as_str())
            .collect::<Vec<_>>();
        let context = t!("view.relational_input_question.retained_context");
        mechanisms.push(if details.is_empty() { context } else { format!("{context} {}", details.join(" ")) });
        push_once(&mut actions, t!("view.relational_input_question.action"));
        for insight in relational_insights.into_iter().chain(competing_insights) {
            if !insight.fix.is_empty() {
                push_once(&mut actions, insight.fix.clone());
            }
        }
        verifies.push(t!("view.relational_input_question.verify"));
    }

    if let Some(mechanism) = parameter_mechanism
        && let Some((param_mech, param_action, param_verify)) = render_parameter_mechanism(&plan, &mechanism)
    {
        mechanisms.push(param_mech);
        actions.push(param_action);
        verifies.push(param_verify);
    }

    if !mechanisms.is_empty() {
        explanation.mechanism = Some(mechanisms.join("\n\n"));
    }
    if !actions.is_empty() {
        explanation.action = Some(actions.join("\n\n"));
    }
    if !verifies.is_empty() {
        explanation.verify = Some(verifies.join("\n\n"));
    }
    push_once(&mut explanation.limitations, t!("view.relational_input_question.execution_limit"));
    Ok(())
}

fn render_parameter_mechanism(plan: &Plan, mechanism: &RetainedParameterMechanism) -> Option<(String, String, String)> {
    let items = &plan.insights.as_ref()?.items;
    let param_ctx = items.iter().find(|item| item.rule == "parameter-context")?;
    match mechanism {
        RetainedParameterMechanism::CachedLookup { lookup_nodes } => {
            let lookup = items
                .iter()
                .find(|item| item.rule == "key-lookups" && item.nodes.iter().any(|id| lookup_nodes.contains(id)));
            let detail = match lookup {
                Some(lookup) => format!("{} {}", param_ctx.detail, lookup.detail),
                None => param_ctx.detail.clone(),
            };
            let action = lookup.map_or_else(|| param_ctx.fix.clone(), |item| item.fix.clone());
            let verify = lookup.map_or_else(|| param_ctx.verify.clone(), |item| item.verify.clone());
            Some((detail, action, verify))
        }
        RetainedParameterMechanism::UnderestimatedSeek { .. } => {
            let action = format!("{} {}", param_ctx.fix, t!("insight.parameter_context.access_fix"));
            Some((param_ctx.detail.clone(), action, param_ctx.verify.clone()))
        }
        RetainedParameterMechanism::CachedSelectiveScan { scan_nodes } => {
            let scan = items
                .iter()
                .find(|item| item.rule == "scan-filter" && item.nodes.iter().any(|id| scan_nodes.contains(id)));
            let detail = match scan {
                Some(scan) => format!("{} {}", param_ctx.detail, scan.detail),
                None => param_ctx.detail.clone(),
            };
            let action = match scan {
                Some(scan) => format!("{} {}", param_ctx.fix, scan.fix),
                None => param_ctx.fix.clone(),
            };
            Some((detail, action, param_ctx.verify.clone()))
        }
    }
}

fn push_once(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
