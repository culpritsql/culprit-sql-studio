//! Security, transport encryption, authentication, auditing, backup retention, and physical
//! integrity posture checks (`Category::Posture`).
//!
//! Posture checks never claim a share of `db-time` (`db_time_share == None`) and lock as
//! `Insufficient` when control-plane or catalog posture fields were not observed.

use crate::support::*;
use culprit_core::{
    Actionability, Category, Check, CheckInfo, Confidence, Engine, Finding, Fix, Metric, Severity, Snapshot,
    SubjectKind, Unit, format,
};

mod audit_disabled;
mod backup_retention_short;
mod integrity_check_overdue;
mod password_auth_enabled;
mod public_endpoint;
mod unencrypted_connections;

#[cfg(test)]
mod tests;

pub(crate) const CHECKS: &[&dyn Check] = &[
    &public_endpoint::PublicEndpointCheck,
    &unencrypted_connections::UnencryptedConnectionsCheck,
    &password_auth_enabled::PasswordAuthEnabledCheck,
    &audit_disabled::AuditDisabledCheck,
    &backup_retention_short::BackupRetentionShortCheck,
    &integrity_check_overdue::IntegrityCheckOverdueCheck,
];
