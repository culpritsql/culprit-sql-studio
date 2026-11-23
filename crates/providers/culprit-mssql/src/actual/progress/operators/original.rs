//! Classify the same selected native payload after its held owner completed final verification.
use super::*;
use crate::activity::current_request_work::OriginalCacheInputs;

pub(crate) fn from_point(
    point: &MssqlCurrentExecutionProfilePoint,
    request_query_hash: Option<&str>,
    original: Option<Box<OriginalCacheInputs>>,
) -> (MssqlExecutionOperators, Option<Box<MssqlExecutionRelationalInputs>>) {
    let missing = |reason| (unavailable(reason), None);
    let Some(work) = &point.work else { return missing(MssqlExecutionOperatorsUnavailable::PlanUnavailable) };
    let rows = match &point.profiles {
        MssqlExecutionProfiles::Observed { rows, .. } => rows.as_slice(),
        MssqlExecutionProfiles::Unavailable { .. } => &[],
    };
    let Some(original) = original else { return (selected(&work.current_plan, request_query_hash, rows), None) };
    let MssqlCurrentRequestPlan::Observed { context, plan: DefinitionPayload::Available { sha256, .. }, .. } =
        &work.current_plan
    else {
        return missing(MssqlExecutionOperatorsUnavailable::PlanUnavailable);
    };
    if context.as_ref() != &original.context || !original_context_matches(context, request_query_hash) {
        return missing(MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable);
    }
    let Ok(plan) = mssql::parse_cached_statement(
        &original.plan,
        mssql::CachedInputContext {
            statement: context,
            module_parameters: original.module_parameters.as_ref(),
            text: Some(&original.statement),
            origin: mssql::PlanInputOrigin::OriginalNative { read: Some(&point.context) },
        },
    ) else {
        return missing(MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable);
    };
    if plan
        .facts()
        .query_hash
        .as_deref()
        .zip(context.query_hash.as_deref())
        .is_none_or(|(parsed, cached)| !same_hash(parsed, cached))
    {
        return missing(MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable);
    }
    let mut inputs = MssqlExecutionRelationalInputs {
        context: context.clone(),
        module_parameters: original.module_parameters,
        statement_utf16_bytes: (original.statement.encode_utf16().count() * 2).to_string(),
        statement_native_sha256: digest(original.statement.as_bytes()),
        plan_utf16_bytes: (original.plan.encode_utf16().count() * 2).to_string(),
        plan_native_sha256: digest(original.plan.as_bytes()),
        proof: None,
    };
    let projected = project(&plan, context, sha256, rows);
    let MssqlExecutionOperators::Observed { nodes, .. } = &projected else { return (projected, None) };
    let bindings = nodes.iter().filter_map(MssqlExecutionOperator::variable_input_binding).collect::<Vec<_>>();
    if !bindings.is_empty() {
        let Ok(proof) = MssqlRelationalInputProof::for_execution(point, &inputs, bindings) else {
            return (unavailable(MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable), Some(Box::new(inputs)));
        };
        inputs.proof = Some(Box::new(proof));
    }
    if !inputs.is_valid_for(context) {
        return missing(MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable);
    }
    (projected, Some(Box::new(inputs)))
}
