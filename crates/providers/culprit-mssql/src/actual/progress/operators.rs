//! The existing selected-statement parser owns XML/semantic decisions. No runtime from cached XML is copied.
use culprit_core::{Engine, PlanNode, telemetry::*};
use culprit_plans::mssql;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn selected(
    plan: &MssqlCurrentRequestPlan,
    request_query_hash: Option<&str>,
    profiles: &[MssqlOperatorThreadProfile],
) -> MssqlExecutionOperators {
    let MssqlCurrentRequestPlan::Observed { context, plan: DefinitionPayload::Available { text, sha256, .. }, .. } =
        plan
    else {
        return unavailable(MssqlExecutionOperatorsUnavailable::PlanUnavailable);
    };
    let Some(request_hash) =
        request_query_hash.filter(|hash| !hash.is_empty() && !hash.bytes().all(|byte| byte == b'0'))
    else {
        return unavailable(MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable);
    };
    let Some(cached_hash) = context.query_hash.as_deref().filter(|hash| same_hash(hash, request_hash)) else {
        return unavailable(MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable);
    };
    let Ok(plan) = mssql::parse_native_statement(text, Some(cached_hash)) else {
        return unavailable(MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable);
    };
    if plan.facts().query_hash.as_deref().is_none_or(|hash| !same_hash(hash, cached_hash)) {
        return unavailable(MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable);
    }
    project(&plan, context, sha256, profiles)
}
fn project(
    plan: &culprit_core::Plan,
    context: &CachedStatementContext,
    sha256: &str,
    profiles: &[MssqlOperatorThreadProfile],
) -> MssqlExecutionOperators {
    let selected = profiles.iter().map(|profile| profile.node_id).collect::<BTreeSet<_>>();
    let mut nodes = BTreeMap::new();
    if !collect(&plan.root, None, &selected, &mut nodes) || nodes.len() != selected.len() {
        return unavailable(MssqlExecutionOperatorsUnavailable::AmbiguousNode);
    }
    let nodes = nodes.into_values().collect::<Vec<_>>();
    if nodes.iter().any(|node| !node.is_valid()) {
        return unavailable(MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable);
    }
    MssqlExecutionOperators::Observed { context: Box::new(context.clone()), plan_sha256: sha256.to_owned(), nodes }
}
fn collect(
    node: &PlanNode,
    parent: Option<u32>,
    selected: &BTreeSet<u32>,
    result: &mut BTreeMap<u32, MssqlExecutionOperator>,
) -> bool {
    if selected.contains(&node.id) {
        let facts = node.facts();
        let operator = MssqlExecutionOperator {
            node_id: node.id,
            parent_node_id: parent,
            kind: node.kind,
            parallel: facts.parallel_operator,
            inner_join: facts.inner_join,
            hash_build_input: facts.hash_build_input,
            relational_input: facts.relational_input,
            native_operator: node.op.clone(),
            object: node.object.clone(),
            index: node.index.clone(),
            predicate: predicate(node.predicate.as_deref()),
            seek_predicate: predicate(node.seek_predicate.as_deref()),
        };
        if result.insert(node.id, operator).is_some() {
            return false;
        }
    }
    node.children.iter().all(|child| collect(child, Some(node.id), selected, result))
}
pub(crate) fn original_context_matches(context: &CachedStatementContext, request_hash: Option<&str>) -> bool {
    context.is_valid()
        && context.origin == StatementOrigin::Application
        && request_hash.zip(context.query_hash.as_deref()).is_some_and(|(request, cached)| same_hash(request, cached))
}
fn same_hash(left: &str, right: &str) -> bool {
    let left = left.trim_start_matches("0x");
    let right = right.trim_start_matches("0x");
    left.len() == 16
        && left.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !left.bytes().all(|byte| byte == b'0')
        && left.eq_ignore_ascii_case(right)
}
fn predicate(text: Option<&str>) -> DefinitionPayload {
    match text {
        Some(text) if text.len() <= MAX_EXECUTION_PROFILE_PREDICATE_BYTES => {
            DefinitionPayload::sql(text, Engine::Mssql)
        }
        Some(_) => DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized },
        None => DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
    }
}
fn unavailable(reason: MssqlExecutionOperatorsUnavailable) -> MssqlExecutionOperators {
    MssqlExecutionOperators::Unavailable { reason }
}
mod original;
pub(crate) use original::from_point;
#[cfg(test)]
mod tests;
