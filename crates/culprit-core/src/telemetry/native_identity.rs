//! Native identifiers remain source-owned; common collection code interprets no provider-specific components.
use super::*;
use std::collections::BTreeMap;

pub const MAX_NATIVE_QUALIFIERS: usize = 32;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct NativeIdentity {
    pub id: String,
    pub qualifiers: BTreeMap<String, String>,
}
impl NativeIdentity {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into(), qualifiers: BTreeMap::new() }
    }
    pub fn qualified(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.qualifiers.insert(name.into(), value.into());
        self
    }
    /// A deterministic content key, independent of native authority or global namespace qualification.
    pub fn key(&self) -> Result<String> {
        serde_json::to_vec(self).map(|bytes| digest(&bytes)).map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub fn is_valid(&self) -> bool {
        valid_key(&self.id) && native_properties_valid(&self.qualifiers)
    }
}
pub(super) fn native_properties_valid(properties: &BTreeMap<String, String>) -> bool {
    properties.len() <= MAX_NATIVE_QUALIFIERS
        && properties.iter().all(|(key, value)| valid_key(key) && valid_key(value))
}

/// A known native entity can remain unresolved when its definition was not retained or cannot be read.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct NativeDefinitionReference {
    pub native: NativeIdentity,
    pub definition_id: Option<String>,
}
impl NativeDefinitionReference {
    pub fn is_valid(&self) -> bool {
        self.native.is_valid() && self.definition_id.as_deref().is_none_or(valid_digest)
    }
    pub fn is_resolved(&self) -> bool {
        self.definition_id.is_some()
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct StatementReferences {
    /// None means that even the native query identity could not be resolved.
    pub query: Option<NativeDefinitionReference>,
    /// None means the source supplies no plan for this measurement. An unresolved known plan is explicit.
    pub plan: Option<NativeDefinitionReference>,
}
impl StatementReferences {
    pub fn is_valid(&self) -> bool {
        self.query.as_ref().is_none_or(NativeDefinitionReference::is_valid)
            && self.plan.as_ref().is_none_or(NativeDefinitionReference::is_valid)
            && (self.plan.as_ref().is_none_or(|plan| !plan.is_resolved())
                || self.query.as_ref().is_some_and(NativeDefinitionReference::is_resolved))
    }
    pub fn is_resolved(&self) -> bool {
        self.query.as_ref().is_some_and(NativeDefinitionReference::is_resolved)
            && self.plan.as_ref().is_none_or(NativeDefinitionReference::is_resolved)
    }
}

impl StatementReferences {
    /// One pure source-scoped definition binding, reused by workload joins and archive detail links.
    /// A numerically equal identifier in another incarnation/namespace is never a fallback.
    pub fn bind_query_definition<'a>(
        &self,
        source: &CollectionDescription,
        definition: Option<&'a QueryDefinition>,
    ) -> Result<Option<&'a QueryDefinition>> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let Some(reference) = &self.query else { return Ok(None) };
        let Some(id) = &reference.definition_id else { return Ok(None) };
        let identity = NativeQueryIdentity {
            query_id: reference.native.id.clone(),
            qualifiers: reference.native.qualifiers.clone(),
        };
        if identity.definition_id(source)? != *id
            || definition.is_some_and(|value| value.id != *id || value.identity != identity)
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        Ok(definition)
    }
    pub fn bind_plan_definition<'a>(
        &self,
        source: &CollectionDescription,
        definition: Option<&'a PlanDefinition>,
    ) -> Result<Option<&'a PlanDefinition>> {
        self.bind_query_definition(source, None)?;
        let Some(query) = &self.query else { return Ok(None) };
        let Some(query_id) = &query.definition_id else { return Ok(None) };
        let Some(reference) = &self.plan else { return Ok(None) };
        let Some(id) = &reference.definition_id else { return Ok(None) };
        let identity = NativePlanIdentity {
            plan_id: reference.native.id.clone(),
            qualifiers: reference.native.qualifiers.clone(),
            query: NativeQueryIdentity {
                query_id: query.native.id.clone(),
                qualifiers: query.native.qualifiers.clone(),
            },
        };
        if identity.definition_id(source)? != *id
            || definition.is_some_and(|value| {
                value.id != *id
                    || value.identity != identity
                    || value.query_definition_id.as_ref().is_some_and(|parent| parent != query_id)
            })
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        // An independent cache entry is retained context, not this runtime's definition namespace.
        Ok(definition.filter(|value| value.query_definition_id.as_ref() == Some(query_id)))
    }
}
