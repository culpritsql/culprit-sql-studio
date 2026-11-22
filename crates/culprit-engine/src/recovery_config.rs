//! Recovery adapter configuration contains only an independently named credential reference.
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryConfig {
    pub provider: String,
    pub options: serde_json::Value,
    pub credential: String,
}

#[cfg(any(feature = "collector", feature = "evidence"))]
pub(crate) fn valid_provider_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}
