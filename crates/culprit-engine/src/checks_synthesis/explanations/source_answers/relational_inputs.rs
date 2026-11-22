//! A later retained plan supplies context and candidates, never the affected request's executing plan.
use super::*;

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
    let Some(proof) = input_proof else { return Ok(()) };
    if !proof.matches_retained(original.source(), native) {
        return Err(invalid());
    }
    let Some(plan) = entry.plan else { return Ok(()) };
    let qualified = plan
        .nodes()
        .into_iter()
        .filter(|node| {
            proof.bindings.iter().any(|binding| {
                binding.node_id == node.id
                    && node.facts().relational_input == Some(binding.input)
                    && binding.input.qualified_kind() != culprit_core::plan_facts::RelationalInputKind::Unknown
            })
        })
        .map(|node| node.id)
        .collect::<std::collections::BTreeSet<_>>();
    if qualified.is_empty() {
        return Ok(());
    }
    explanation.candidates.push(DerivedIncidentQuery {
        causal_role: None,
        identity: Some(DerivedQueryIdentity::CachedStatement(native.context.clone())),
        sql: native.statement.payload.clone(),
        caller: None,
        evidence: vec![observation.as_ref().clone(), answer.work.clone()],
    });
    let details = plan
        .insights
        .as_ref()
        .into_iter()
        .flat_map(|insights| &insights.items)
        .filter(|insight| insight.nodes.iter().any(|node| qualified.contains(node)))
        .map(|insight| insight.detail.as_str())
        .collect::<Vec<_>>();
    let context = t!("view.relational_input_question.retained_context");
    explanation.mechanism = Some(if details.is_empty() { context } else { format!("{context} {}", details.join(" ")) });
    explanation.action = Some(t!("view.relational_input_question.action"));
    explanation.verify = Some(t!("view.relational_input_question.verify"));
    push_once(&mut explanation.limitations, t!("view.relational_input_question.execution_limit"));
    Ok(())
}
fn push_once(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
