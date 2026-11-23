//! Compact selected-plan semantics are filled by the existing native parser; display labels drive no decision.
use super::*;
use crate::plan_facts::RelationalInput;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlExecutionOperator {
    pub node_id: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    pub parent_node_id: Option<u32>,
    pub kind: crate::NodeKind,
    #[serde(deserialize_with = "Option::deserialize")]
    pub parallel: Option<bool>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub inner_join: Option<bool>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub hash_build_input: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub relational_input: Option<RelationalInput>,
    pub native_operator: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub object: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub index: Option<String>,
    pub predicate: DefinitionPayload,
    pub seek_predicate: DefinitionPayload,
}
impl MssqlExecutionOperator {
    pub fn variable_input_binding(&self) -> Option<MssqlRelationalInputBinding> {
        self.relational_input
            .filter(|input| {
                matches!(
                    input.qualified_kind(),
                    crate::plan_facts::RelationalInputKind::TableValuedParameter
                        | crate::plan_facts::RelationalInputKind::LocalTableVariable
                )
            })
            .map(|input| MssqlRelationalInputBinding { node_id: self.node_id, input })
    }
    pub fn is_valid(&self) -> bool {
        self.node_id <= i32::MAX as u32
            && self.parent_node_id.is_none_or(|node| node != self.node_id && node <= i32::MAX as u32)
            && self.hash_build_input.is_none_or(|node| node != self.node_id && node <= i32::MAX as u32)
            && self.relational_input.is_none_or(RelationalInput::is_valid)
            && (self.variable_input_binding().is_none()
                || matches!(self.kind, crate::NodeKind::Scan | crate::NodeKind::Seek))
            && !self.native_operator.is_empty()
            && self.native_operator.len() <= 1024
            && !self.native_operator.chars().any(char::is_control)
            && [&self.object, &self.index]
                .into_iter()
                .flatten()
                .all(|name| !name.is_empty() && name.len() <= 2048 && !name.chars().any(char::is_control))
            && [&self.predicate, &self.seek_predicate]
                .into_iter()
                .all(|value| value.is_valid() && payload_bounded(value, MAX_EXECUTION_PROFILE_PREDICATE_BYTES))
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlExecutionOperatorsUnavailable {
    PlanUnavailable,
    QueryIdentityUnavailable,
    SelectedPlanUnavailable,
    AmbiguousNode,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MssqlExecutionOperators {
    Observed { context: Box<CachedStatementContext>, plan_sha256: String, nodes: Vec<MssqlExecutionOperator> },
    Unavailable { reason: MssqlExecutionOperatorsUnavailable },
}
impl MssqlExecutionOperators {
    pub(super) fn fits_point(
        &self,
        point: &MssqlCurrentExecutionProfilePoint,
        profiles: &[MssqlOperatorThreadProfile],
    ) -> bool {
        let Self::Observed { context, plan_sha256, nodes } = self else { return true };
        let Some(work) = &point.work else { return false };
        let MssqlCurrentRequestPlan::Observed {
            context: actual,
            plan: DefinitionPayload::Available { sha256, .. },
            observed_from: cache_from,
            observed_until: cache_until,
        } = &work.current_plan
        else {
            return false;
        };
        let MssqlExecutionProfiles::Observed { observed_from, observed_until, .. } = &point.profiles else {
            return false;
        };
        let mut ids = BTreeSet::new();
        context.is_valid()
            && ObservationBounds::new(cache_from, observed_from).is_some()
            && ObservationBounds::new(observed_until, cache_until).is_some()
            && context == actual
            && valid_digest(plan_sha256)
            && plan_sha256 == sha256
            && nodes.len() <= MAX_EXECUTION_PROFILE_ROWS
            && nodes.iter().all(|node| node.is_valid() && ids.insert(node.node_id))
            && profiles.iter().all(|profile| ids.contains(&profile.node_id))
            && nodes.iter().all(|node| profiles.iter().any(|profile| profile.node_id == node.node_id))
            && match &point.relational_inputs {
                Some(inputs) => inputs.matches_nodes(nodes),
                None => nodes.iter().all(|node| {
                    node.relational_input.is_none_or(|input| {
                        input.qualified_kind() != crate::plan_facts::RelationalInputKind::TableValuedParameter
                    })
                }),
            }
    }
    pub fn node(&self, id: u32) -> Option<&MssqlExecutionOperator> {
        match self {
            Self::Observed { nodes, .. } => nodes.iter().find(|node| node.node_id == id),
            _ => None,
        }
    }
}
