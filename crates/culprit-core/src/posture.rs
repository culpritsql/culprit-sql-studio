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

/// Worst-case positive local-to-UTC timezone offset on Earth (UTC+14, Line Islands).
/// Subtracting 14 hours when comparing an offset-free local engine timestamp against a UTC observation clock
/// guarantees unknown server timezones never exaggerate backup or `DBCC CHECKDB` age across check thresholds.
const MAX_LOCAL_AHEAD_OF_UTC_SECS: i64 = 14 * 3_600;
const MIN_NEVER_CHECKED_DB_AGE_DAYS: f64 = 14.0;

impl PostureSnapshot {
    pub fn has_any_observation(&self) -> bool {
        self.public_network_access.is_some()
            || self.private_endpoint_only.is_some()
            || self.tls_enforced.is_some()
            || self.min_tls_version.is_some()
            || self.unencrypted_sessions.is_some()
            || self.password_auth_enabled.is_some()
            || self.auditing_enabled.is_some()
            || self.backup_retention_days.is_some()
            || self.last_full_backup_days.is_some()
            || self.last_integrity_check_days.is_some()
            || !self.coverage_notes.is_empty()
    }

    /// Merge complementary posture sources (e.g. database catalog/DMV facts + cloud control-plane settings)
    /// without overwriting already-established fields.
    pub fn merge(&mut self, other: Self) {
        if self.public_network_access.is_none() {
            self.public_network_access = other.public_network_access;
        }
        if self.private_endpoint_only.is_none() {
            self.private_endpoint_only = other.private_endpoint_only;
        }
        if self.tls_enforced.is_none() {
            self.tls_enforced = other.tls_enforced;
        }
        if self.min_tls_version.is_none() {
            self.min_tls_version = other.min_tls_version;
        }
        if self.unencrypted_sessions.is_none() {
            self.unencrypted_sessions = other.unencrypted_sessions;
        }
        if self.password_auth_enabled.is_none() {
            self.password_auth_enabled = other.password_auth_enabled;
        }
        if self.auditing_enabled.is_none() {
            self.auditing_enabled = other.auditing_enabled;
        }
        if self.backup_retention_days.is_none() {
            self.backup_retention_days = other.backup_retention_days;
        }
        if self.last_full_backup_days.is_none() {
            self.last_full_backup_days = other.last_full_backup_days;
        }
        if self.last_integrity_check_days.is_none() {
            self.last_integrity_check_days = other.last_integrity_check_days;
        }
        for note in other.coverage_notes {
            if !self.coverage_notes.contains(&note) {
                self.coverage_notes.push(note);
            }
        }
    }
}

/// Conservative lower-bound age in days for an offset-free SQL Server timestamp (`YYYY-MM-DDTHH:MM:SS[.fff]`)
/// relative to a canonical RFC3339 UTC observation timestamp. Sentinel dates (`1900-01-01`, `1753-01-01`,
/// `0001-01-01`) return `None`.
pub fn conservative_offset_free_age_days(without_offset: &str, observed_utc: &str) -> Option<f64> {
    let trimmed = without_offset.trim();
    if trimmed.is_empty()
        || trimmed.starts_with("1900-01-01")
        || trimmed.starts_with("1753-01-01")
        || trimmed.starts_with("0001-01-01")
    {
        return None;
    }
    let observed = chrono::DateTime::parse_from_rfc3339(observed_utc).ok()?.with_timezone(&chrono::Utc);
    let naive = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f"))
        .ok()?;
    let raw_secs = observed.naive_utc().signed_duration_since(naive).num_seconds();
    if raw_secs < -MAX_LOCAL_AHEAD_OF_UTC_SECS {
        return None;
    }
    let conservative_secs = (raw_secs - MAX_LOCAL_AHEAD_OF_UTC_SECS).max(0);
    Some((conservative_secs as f64) / 86_400.0)
}

/// Derive conservative `DBCC CHECKDB` age in days for SQL Server / Azure SQL Managed Instance writable primaries.
/// Azure SQL Database manages physical page verification in cloud storage and reports `1900-01-01`, so it stays `None`.
pub fn mssql_integrity_check_age_days(
    properties: &crate::telemetry::DatabasePropertiesObservation,
    is_azure_sql_database: bool,
) -> Option<f64> {
    if is_azure_sql_database
        || properties.in_standby == Some(true)
        || properties.catalog_read_only == Some(true)
        || properties.updateability.as_deref().is_some_and(|u| u.eq_ignore_ascii_case("READ_ONLY"))
        || matches!(
            &properties.local_replica_role,
            Some(crate::telemetry::MssqlDatabaseReplicaRole::LocalAvailabilityGroup { is_primary: Some(false), .. })
        )
    {
        return None;
    }
    let raw = properties.last_good_checkdb_without_offset.as_deref()?.trim();
    if raw.starts_with("1900-01-01") {
        let created = properties.catalog_created_without_offset.as_deref()?;
        let created_age = conservative_offset_free_age_days(created, &properties.observed_at)?;
        return (created_age > MIN_NEVER_CHECKED_DB_AGE_DAYS).then_some(created_age);
    }
    conservative_offset_free_age_days(raw, &properties.observed_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{
        DatabasePropertiesObservation, DatabasePropertiesProfile, DatabasePropertyCode, MssqlDatabaseReplicaRole,
        MssqlDatabaseReplicaRoleSource,
    };

    fn sample_properties() -> DatabasePropertiesObservation {
        DatabasePropertiesObservation {
            profile: DatabasePropertiesProfile::MssqlSysDatabases,
            database: "orders".into(),
            native_database_id: "7".into(),
            catalog_database_id: "7".into(),
            catalog_created_without_offset: Some("2025-01-01T00:00:00".into()),
            local_replica_role: Some(MssqlDatabaseReplicaRole::NotInAvailabilityGroup {
                observed_at: "2026-03-20T12:00:00Z".into(),
            }),
            observed_at: "2026-03-20T12:00:00Z".into(),
            catalog_state: DatabasePropertyCode { code: Some(0), description: Some("ONLINE".into()) },
            user_access: DatabasePropertyCode { code: Some(0), description: Some("MULTI_USER".into()) },
            updateability: Some("READ_WRITE".into()),
            catalog_read_only: Some(false),
            in_standby: Some(false),
            collation: None,
            compatibility_level: Some(160),
            snapshot_isolation: DatabasePropertyCode { code: None, description: None },
            read_committed_snapshot: None,
            auto_shrink: None,
            auto_create_statistics: None,
            auto_update_statistics: None,
            auto_update_statistics_async: None,
            forced_parameterization: None,
            query_store_enabled: None,
            broker_enabled: None,
            page_verify: DatabasePropertyCode { code: Some(2), description: Some("CHECKSUM".into()) },
            recovery_model: DatabasePropertyCode { code: Some(1), description: Some("FULL".into()) },
            delayed_durability: DatabasePropertyCode { code: None, description: None },
            containment: DatabasePropertyCode { code: None, description: None },
            encrypted: None,
            ledger_enabled: None,
            change_feed_enabled: None,
            published: None,
            subscribed: None,
            merge_published: None,
            distributor: None,
            accelerated_recovery_enabled: None,
            last_good_checkdb_without_offset: Some("2026-02-18T12:00:00".into()),
            target_recovery_time_seconds: Some(60),
        }
    }

    #[test]
    fn conservative_age_subtracts_max_positive_timezone_and_rejects_sentinels() {
        assert_eq!(conservative_offset_free_age_days("1900-01-01T00:00:00", "2026-03-20T12:00:00Z"), None);
        // 15 days 0h nominal minus 14h conservative timezone allowance = 14.4166... days (> 14.0).
        let age = conservative_offset_free_age_days("2026-03-05T12:00:00", "2026-03-20T12:00:00Z").unwrap();
        assert!((age - (15.0 - 14.0 / 24.0)).abs() < 1e-6);
    }

    #[test]
    fn mssql_integrity_check_skips_azure_sql_db_and_replicas_and_handles_never_checked_databases() {
        let props = sample_properties();
        assert!(mssql_integrity_check_age_days(&props, true).is_none(), "Azure SQL DB must stay None");

        let mut secondary = props.clone();
        secondary.local_replica_role = Some(MssqlDatabaseReplicaRole::LocalAvailabilityGroup {
            source: MssqlDatabaseReplicaRoleSource::HadrDatabaseReplicaStates,
            observed_at: "2026-03-20T12:00:00Z".into(),
            replica_id: "11111111-2222-3333-4444-555555555555".into(),
            group_database_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
            is_primary: Some(false),
        });
        assert!(mssql_integrity_check_age_days(&secondary, false).is_none());

        let age = mssql_integrity_check_age_days(&props, false).unwrap();
        assert!(age > 29.0 && age < 30.0);

        let mut never_checked_young = props.clone();
        never_checked_young.last_good_checkdb_without_offset = Some("1900-01-01T00:00:00".into());
        never_checked_young.catalog_created_without_offset = Some("2026-03-15T12:00:00".into());
        assert!(mssql_integrity_check_age_days(&never_checked_young, false).is_none());

        let mut never_checked_old = props;
        never_checked_old.last_good_checkdb_without_offset = Some("1900-01-01T00:00:00".into());
        assert!(mssql_integrity_check_age_days(&never_checked_old, false).unwrap() > 400.0);
    }

    #[test]
    fn merge_combines_db_and_cloud_posture_without_overwriting_existing_observations() {
        let mut db = PostureSnapshot {
            unencrypted_sessions: Some(2),
            last_integrity_check_days: Some(21.0),
            coverage_notes: vec!["db-note".into()],
            ..Default::default()
        };
        db.merge(PostureSnapshot {
            public_network_access: Some(true),
            unencrypted_sessions: Some(0),
            backup_retention_days: Some(3),
            coverage_notes: vec!["db-note".into(), "cloud-note".into()],
            ..Default::default()
        });
        assert_eq!(db.unencrypted_sessions, Some(2));
        assert_eq!(db.public_network_access, Some(true));
        assert_eq!(db.backup_retention_days, Some(3));
        assert_eq!(db.last_integrity_check_days, Some(21.0));
        assert_eq!(db.coverage_notes, vec!["db-note", "cloud-note"]);
    }
}
