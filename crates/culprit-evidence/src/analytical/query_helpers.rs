//! Configured query assets bind one actual archive namespace. They never authorize source reads or diagnose SQL.
use crate::delivery::contract::Contract;
use culprit_core::{
    ComponentVersion, Result,
    telemetry::{DeliveryProblem, valid_digest},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryHelperPolicy {
    pub name: String,
    pub analyzer: ComponentVersion,
    pub rules_version: String,
    /// Earliest retained publication admitted by these helpers, not a claim of complete native history.
    pub history_from: String,
}
impl QueryHelperPolicy {
    pub fn validate(&self) -> Result<()> {
        let name = self.name.as_bytes();
        if name.is_empty()
            || name.len() > 48
            || !name[0].is_ascii_alphabetic()
            || !name.iter().all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            || !label(&self.analyzer.component)
            || !self.analyzer.version.as_deref().is_some_and(label)
            || self.analyzer.revision.as_deref().is_some_and(|revision| !label(revision))
            || !label(&self.rules_version)
            || !utc(&self.history_from)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct QueryHelperBinding {
    pub policy: QueryHelperPolicy,
    pub contract_id: String,
    pub schema_digest: String,
    pub recovery_store_id: String,
}
impl QueryHelperBinding {
    /// Only the actual selected root supplies recovery_store_id. No advertised warehouse row selects a namespace.
    pub fn new(policy: QueryHelperPolicy, contract: &Contract, recovery_store_id: &str) -> Result<Self> {
        let value = Self {
            policy,
            contract_id: contract.id()?,
            schema_digest: contract.schema_digest().into(),
            recovery_store_id: recovery_store_id.into(),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<()> {
        self.policy.validate()?;
        if [&self.contract_id, &self.schema_digest, &self.recovery_store_id].into_iter().any(|id| !valid_digest(id)) {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Serialize)]
pub struct QueryHelperAssets {
    pub binding: QueryHelperBinding,
    pub commands: Vec<String>,
    /// Executable text belongs to the selected store; callers only choose text versus JSON output.
    pub script: String,
    pub examples: Vec<QueryExample>,
}
#[derive(Serialize)]
pub struct QueryExample {
    pub name: &'static str,
    pub query: String,
}
/// Substitute only markers in the original asset. Quoted policy text is never scanned for another marker.
pub fn render_friendly_query(template: &str, values: &[(&str, String)]) -> Result<String> {
    let mut output = String::new();
    let mut remainder = template;
    while let Some(position) = remainder.find('$') {
        output.push_str(&remainder[..position]);
        remainder = &remainder[position..];
        let (key, value) = values
            .iter()
            .filter(|(key, _)| remainder.starts_with(key))
            .max_by_key(|(key, _)| key.len())
            .ok_or_else(invalid)?;
        output.push_str(value);
        remainder = &remainder[key.len()..];
    }
    output.push_str(remainder);
    Ok(output)
}
pub fn utc(value: &str) -> bool {
    value.len() <= 64
        && chrono::DateTime::parse_from_rfc3339(value)
            .is_ok_and(|time| time.offset().local_minus_utc() == 0 && time.timestamp_nanos_opt().is_some())
}
fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::InvalidContract.error()
}
#[cfg(test)]
mod tests;
