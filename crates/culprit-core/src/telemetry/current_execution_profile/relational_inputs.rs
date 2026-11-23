//! Bounded original selected-payload metadata, never a later module lookup or a runtime-value attestation.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlExecutionRelationalInputs {
    pub context: Box<CachedStatementContext>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub module_parameters: Option<MssqlModuleParameters>,
    pub statement_utf16_bytes: String,
    pub statement_native_sha256: String,
    pub plan_utf16_bytes: String,
    pub plan_native_sha256: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub proof: Option<Box<MssqlRelationalInputProof>>,
}
impl MssqlExecutionRelationalInputs {
    pub fn is_valid(&self) -> bool {
        self.context.is_valid()
            && self.context.origin == StatementOrigin::Application
            && self.context.query_hash.as_deref().is_some_and(|value| {
                super::super::cached_statements::native_query_hash(value)
                    && value.strip_prefix("0x").is_some_and(|hex| hex.bytes().any(|byte| byte != b'0'))
            })
            && self.context.identity.has_plan_handle()
            && self.context.identity.has_sql_handle()
            && self.module_parameters.as_ref().is_none_or(|parameters| parameters.is_valid_for(&self.context))
            && native_bytes(&self.statement_utf16_bytes, MAX_CURRENT_REQUEST_WORK_SQL_BYTES)
            && native_bytes(&self.plan_utf16_bytes, MAX_CURRENT_REQUEST_WORK_PLAN_BYTES)
            && valid_digest(&self.statement_native_sha256)
            && valid_digest(&self.plan_native_sha256)
            && self.proof.as_ref().is_none_or(|proof| proof.is_valid())
    }
    pub fn is_valid_for(&self, context: &CachedStatementContext) -> bool {
        self.is_valid() && self.context.as_ref() == context
    }
    pub(super) fn matches_point(&self, point: &MssqlCurrentExecutionProfilePoint) -> bool {
        MssqlRelationalInputProof::execution_inputs_match(point, self)
            && self.proof.as_ref().is_none_or(|proof| proof.matches_execution(point, self))
    }
    pub(super) fn matches_nodes(&self, nodes: &[MssqlExecutionOperator]) -> bool {
        let mut bindings = nodes.iter().filter_map(MssqlExecutionOperator::variable_input_binding).collect::<Vec<_>>();
        bindings.sort_by_key(|binding| binding.node_id);
        match &self.proof {
            Some(proof) => proof.bindings == bindings,
            None => bindings.is_empty(),
        }
    }
}
fn native_bytes(value: &str, maximum: usize) -> bool {
    value.parse::<u64>().is_ok_and(|bytes| {
        bytes > 0 && bytes <= maximum as u64 && bytes.is_multiple_of(2) && bytes.to_string() == value
    })
}

#[cfg(test)]
mod tests;
