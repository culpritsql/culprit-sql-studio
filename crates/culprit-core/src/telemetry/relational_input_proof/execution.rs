//! Current-execution input classifications share the existing proof owner and authenticate their own inputs.
//! This does not authenticate raw caller data: the original canonical receipt remains mandatory.
use super::*;

impl MssqlRelationalInputProof {
    pub fn for_execution(
        point: &MssqlCurrentExecutionProfilePoint,
        inputs: &MssqlExecutionRelationalInputs,
        mut bindings: Vec<MssqlRelationalInputBinding>,
    ) -> Result<Self> {
        bindings.sort_by_key(|binding| binding.node_id);
        let source_scope = execution_scope(point)?;
        let value =
            Self { input_sha256: execution_input(&source_scope, point, inputs, &bindings)?, source_scope, bindings };
        if !value.matches_execution(point, inputs) {
            return Err(invalid());
        }
        Ok(value)
    }
    pub fn matches_execution(
        &self,
        point: &MssqlCurrentExecutionProfilePoint,
        inputs: &MssqlExecutionRelationalInputs,
    ) -> bool {
        self.is_valid()
            && Self::execution_inputs_match(point, inputs)
            && execution_scope(point).is_ok_and(|scope| scope == self.source_scope)
            && (self.bindings.iter().all(|binding| binding.input.kind != RelationalInputKind::TableValuedParameter)
                || inputs.module_parameters.as_ref().is_some_and(|parameters| !parameters.table_parameters.is_empty()))
            && execution_input(&self.source_scope, point, inputs, &self.bindings)
                .is_ok_and(|expected| expected == self.input_sha256)
    }
    pub(in crate::telemetry) fn execution_inputs_match(
        point: &MssqlCurrentExecutionProfilePoint,
        inputs: &MssqlExecutionRelationalInputs,
    ) -> bool {
        execution_payloads(point).is_ok_and(|(context, _)| inputs.is_valid_for(context))
            && execution_scope(point).is_ok()
    }
}
fn execution_scope(point: &MssqlCurrentExecutionProfilePoint) -> Result<String> {
    if !point.context.has_qualified_local_owner()
        || point.context.target.engine() != crate::Engine::Mssql
        || point.context.database_writable.is_none()
        || !CurrentRequestWorkProfile::MssqlRequest.accepts_focus(&point.focus)
        || !focused_request_original_scope_matches(&point.focus, &point.context)
        || point.binding != FocusedRequestBinding::Matched
        || point.membership != FocusedRequestMembership::Observed
    {
        return Err(invalid());
    }
    encoded_digest(&(
        "mssql-current-execution-relational-input-scope-v1",
        &point.context,
        &point.focus.parent,
        &point.focus.original.key,
        &point.focus.statement_identity,
    ))
}
fn execution_payloads(
    point: &MssqlCurrentExecutionProfilePoint,
) -> Result<(&CachedStatementContext, &DefinitionPayload)> {
    let Some(work) = &point.work else { return Err(invalid()) };
    let MssqlCurrentRequestPlan::Observed { context, plan, .. } = &work.current_plan else { return Err(invalid()) };
    if !context.is_valid()
        || context.origin != StatementOrigin::Application
        || !work.current_plan.matches_statement(&point.focus.statement_identity)
        || !work.fits_context(&point.context)
        || !point.statement.is_valid()
        || !point.statement.is_available()
        || !plan.is_valid()
        || !plan.is_available()
    {
        return Err(invalid());
    }
    Ok((context, plan))
}
fn execution_input(
    scope: &str,
    point: &MssqlCurrentExecutionProfilePoint,
    inputs: &MssqlExecutionRelationalInputs,
    bindings: &[MssqlRelationalInputBinding],
) -> Result<String> {
    let (context, plan) = execution_payloads(point)?;
    if !inputs.is_valid_for(context) {
        return Err(invalid());
    }
    encoded_digest(&(
        "mssql-current-execution-relational-input-v1",
        scope,
        context,
        &inputs.module_parameters,
        (&inputs.statement_utf16_bytes, &inputs.statement_native_sha256),
        (&inputs.plan_utf16_bytes, &inputs.plan_native_sha256),
        &point.statement,
        plan,
        bindings,
    ))
}
