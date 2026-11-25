//! Security, encryption, authentication, auditing, backup retention, and integrity-check posture facts.
//!
//! Unobserved fields remain `None` (never `false` or `0`) so checks lock cleanly as `Insufficient`
//! rather than claiming an unverified database has zero posture risks.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const POSTURE_SOURCE: &str = "posture";

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostureSnapshot {
    /// `true` when public network access / `PubliclyAccessible` is enabled on the server or instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub public_network_access: Option<bool>,
    /// `true` when private endpoints / VPC-only connectivity are confirmed active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub private_endpoint_only: Option<bool>,
    /// `true` when transport encryption (TLS/SSL) is strictly enforced for client connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub tls_enforced: Option<bool>,
    /// Configured minimum TLS version string when reported (e.g. `"1.2"`, `"1.0"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub min_tls_version: Option<String>,
    /// Active unencrypted client sessions observed during the snapshot, if inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub unencrypted_sessions: Option<u64>,
    /// `true` when SQL / local password authentication is enabled (as opposed to Entra/IAM-only auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub password_auth_enabled: Option<bool>,
    /// `true` when server or database security auditing / audit log export is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub auditing_enabled: Option<bool>,
    /// Configured point-in-time / automated backup retention window in days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub backup_retention_days: Option<u32>,
    /// Age in days since the most recent completed full database backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub last_full_backup_days: Option<f64>,
    /// Age in days since the last known-good physical integrity check (`DBCC CHECKDB` / `dbccLastKnownGood`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub last_integrity_check_days: Option<f64>,
    /// Non-secret coverage notes describing partial control-plane or catalog visibility.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coverage_notes: Vec<String>,
}
