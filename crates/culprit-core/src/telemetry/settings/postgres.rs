//! Native PostgreSQL configuration values share one checked scan/retained projection.
use super::*;
use crate::{MeasurementScope, Setting};

mod read;
pub use read::*;

const PRIMARY_MAINTENANCE_SETTINGS: &[&str] = &[
    "autovacuum",
    "autovacuum_vacuum_scale_factor",
    "autovacuum_analyze_scale_factor",
    "autovacuum_vacuum_threshold",
    "autovacuum_vacuum_max_threshold",
    "autovacuum_analyze_threshold",
    "autovacuum_multixact_freeze_max_age",
    "autovacuum_freeze_max_age",
];

fn applies_to_role(name: &str, database_writable: Option<bool>) -> bool {
    !PRIMARY_MAINTENANCE_SETTINGS.contains(&name) || database_writable == Some(true)
}

impl SettingObservation {
    /// Native recovery role is separate from session write permission. Maintenance settings remain
    /// raw facts on a standby, but do not establish an active primary-maintenance problem there.
    pub fn to_postgres_setting_for_role(&self, database_writable: Option<bool>) -> Option<Setting> {
        if !applies_to_role(&self.name, database_writable) {
            return None;
        }
        self.to_postgres_setting()
    }

    /// Project the observed native value. Missing values and unsupported scopes remain unavailable.
    /// This conversion does not establish a source owner, a complete catalog or action authority.
    pub fn to_postgres_setting(&self) -> Option<Setting> {
        if !self.is_valid() || self.namespace != "pg_catalog.pg_settings" || self.nature != SettingNature::Configuration
        {
            return None;
        }
        let scope = match self.scope {
            MeasurementScope::Server => "server",
            MeasurementScope::Session => "session",
            _ => return None,
        };
        let observed = self.value_in_use.as_deref().or(self.value.as_deref())?;
        Some(Setting {
            name: self.name.clone(),
            value: pretty_value(observed, self.unit.as_deref())?,
            default_value: self.default_value.as_deref().and_then(|value| pretty_value(value, self.unit.as_deref())),
            scope: scope.into(),
        })
    }
}

fn pretty_value(value: &str, unit: Option<&str>) -> Option<String> {
    let Some(unit) = unit else { return Some(value.into()) };
    let number = match value.trim().parse::<i64>() {
        Ok(number) => number,
        Err(_) if matches!(unit, "B" | "kB" | "8kB" | "16kB" | "32kB" | "MB" | "ms" | "s" | "min") => return None,
        Err(_) => return Some(value.into()),
    };
    if number < 0 {
        return Some(number.to_string());
    }
    const MEMORY: &[(&str, i64)] = &[("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10), ("B", 1)];
    const TIME: &[(&str, i64)] = &[("d", 86_400_000), ("h", 3_600_000), ("min", 60_000), ("s", 1_000), ("ms", 1)];
    let (base, scale) = match unit {
        "B" => (MEMORY, 1),
        "kB" => (MEMORY, 1 << 10),
        "8kB" => (MEMORY, 8 << 10),
        "16kB" => (MEMORY, 16 << 10),
        "32kB" => (MEMORY, 32 << 10),
        "MB" => (MEMORY, 1 << 20),
        "ms" => (TIME, 1),
        "s" => (TIME, 1_000),
        "min" => (TIME, 60_000),
        _ => return Some(format!("{value} {unit}")),
    };
    let amount = number.checked_mul(scale)?;
    if amount == 0 {
        return Some(format!("0{}", base.last().map(|(unit, _)| *unit).unwrap_or("")));
    }
    for (label, size) in base {
        if amount % size == 0 {
            return Some(format!("{}{label}", amount / size));
        }
    }
    Some(amount.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn native(value: Option<&str>, unit: Option<&str>) -> SettingObservation {
        SettingObservation {
            source: "pg_settings".into(),
            namespace: "pg_catalog.pg_settings".into(),
            name: "work_mem".into(),
            scope: MeasurementScope::Server,
            nature: SettingNature::Configuration,
            value: None,
            value_in_use: value.map(str::to_owned),
            value_for_secondary: None,
            unit: unit.map(str::to_owned),
            native_context: Some("sighup".into()),
            native_source: Some("configuration file".into()),
            pending_restart: Some(false),
            native_is_default: None,
            default_value: None,
            default_source: SettingDefaultSource::Unknown,
        }
    }
    #[test]
    fn reviewed_native_units_and_negative_sentinels_keep_their_original_strings() {
        for (value, unit, expected) in [
            ("16384", Some("8kB"), "128MB"),
            ("4096", Some("kB"), "4MB"),
            ("200", Some("ms"), "200ms"),
            ("60", Some("s"), "1min"),
            ("4", None, "4"),
            ("-1", Some("kB"), "-1"),
            ("0", Some("kB"), "0B"),
            ("off", None, "off"),
        ] {
            assert_eq!(native(Some(value), unit).to_postgres_setting().unwrap().value, expected);
        }
    }
    #[test]
    fn missing_values_native_scopes_and_defaults_do_not_become_empty_or_global_configuration() {
        assert!(native(None, None).to_postgres_setting().is_none());
        let mut value = native(Some("4096"), Some("kB"));
        value.scope = MeasurementScope::Session;
        assert_eq!(value.to_postgres_setting().unwrap().scope, "session");
        value.scope = MeasurementScope::Database;
        assert!(value.to_postgres_setting().is_none());
        value.scope = MeasurementScope::Server;
        value.namespace = "foreign.settings".into();
        assert!(value.to_postgres_setting().is_none());
        value.namespace = "pg_catalog.pg_settings".into();
        value.value_in_use = None;
        value.value = Some("4096".into());
        assert_eq!(value.to_postgres_setting().unwrap().value, "4MB");
        value.default_source = SettingDefaultSource::NativeValue;
        value.default_value = Some("8192".into());
        assert_eq!(value.to_postgres_setting().unwrap().default_value.as_deref(), Some("8MB"));
    }
    #[test]
    fn native_recovery_or_unknown_role_withholds_primary_maintenance_advice_without_erasing_values() {
        let mut setting = native(Some("off"), None);
        setting.name = "autovacuum".into();
        assert_eq!(setting.to_postgres_setting().unwrap().value, "off");
        assert!(setting.to_postgres_setting_for_role(Some(true)).is_some());
        assert!(setting.to_postgres_setting_for_role(Some(false)).is_none());
        assert!(setting.to_postgres_setting_for_role(None).is_none());
        setting.name = "shared_buffers".into();
        setting.value_in_use = Some("128".into());
        setting.unit = Some("MB".into());
        assert_eq!(setting.to_postgres_setting_for_role(Some(false)).unwrap().value, "128MB");
    }

    #[test]
    fn native_number_scaling_refuses_overflow_without_wrapping_or_saturating() {
        let largest = i64::MAX / 1024;
        assert!(native(Some(&largest.to_string()), Some("kB")).to_postgres_setting().is_some());
        assert!(native(Some(&(largest + 1).to_string()), Some("kB")).to_postgres_setting().is_none());
        assert!(native(Some(&i64::MAX.to_string()), Some("MB")).to_postgres_setting().is_none());
        assert!(native(Some("9223372036854775808"), Some("kB")).to_postgres_setting().is_none());
        assert!(native(Some(&i64::MIN.to_string()), Some("kB")).to_postgres_setting().is_some());
    }
}
