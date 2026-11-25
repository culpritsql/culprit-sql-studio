//! Pure native SHOW GLOBAL VARIABLES projection shared by ordinary scans and retained Settings.
use super::*;
use crate::{MeasurementScope, Setting};

const SETTINGS: [(&str, Option<&str>, Option<&str>); 23] = [
    ("performance_schema_digests_size", Some("10000"), Some("-1")),
    ("performance_schema_max_digest_length", Some("1024"), Some("1024")),
    ("performance_schema_max_sql_text_length", Some("1024"), Some("1024")),
    ("max_digest_length", Some("1024"), Some("1024")),
    ("innodb_buffer_pool_size", Some("134217728"), Some("134217728")),
    ("innodb_page_size", Some("16384"), Some("16384")),
    ("tmp_table_size", Some("16777216"), Some("16777216")),
    ("max_heap_table_size", Some("16777216"), Some("16777216")),
    // Which engine keeps internal temporary tables in memory, and TempTable's memory for all of them together (1 GB
    // on 8.0; 8.4 takes 3% of the host's memory, between 1 and 4 GB).
    ("internal_tmp_mem_storage_engine", Some("TempTable"), None),
    ("temptable_max_ram", Some(""), None),
    ("sort_buffer_size", Some("262144"), Some("2097152")),
    ("join_buffer_size", Some("262144"), Some("262144")),
    ("long_query_time", Some("10.000000"), Some("10.000000")),
    ("slow_query_log", Some("OFF"), Some("OFF")),
    ("log_output", Some("FILE"), Some("FILE")),
    ("performance_schema", Some("ON"), Some("OFF")),
    ("max_connections", Some("151"), Some("151")),
    ("innodb_flush_log_at_trx_commit", Some("1"), Some("1")),
    ("transaction_isolation", Some("REPEATABLE-READ"), Some("REPEATABLE-READ")),
    ("tx_isolation", None, Some("REPEATABLE-READ")),
    ("max_execution_time", Some("0"), None),
    ("max_statement_time", None, Some("0.000000")),
    ("userstat", None, Some("OFF")),
];

impl NativeSettingsFormat {
    pub fn global_variable_names(self) -> Option<Vec<&'static str>> {
        if !matches!(self, Self::MysqlGlobalVariables | Self::MariaDbGlobalVariables) {
            return None;
        }
        Some(
            SETTINGS
                .iter()
                .filter(|(name, _, _)| self.variable_default(name).is_some())
                .map(|(name, _, _)| *name)
                .collect(),
        )
    }
    fn variable_default(self, name: &str) -> Option<&'static str> {
        SETTINGS.iter().find(|(setting, _, _)| *setting == name).and_then(|(_, mysql, maria)| match self {
            Self::MysqlGlobalVariables => *mysql,
            Self::MariaDbGlobalVariables => *maria,
            Self::PostgresSettings | Self::MssqlSettings | Self::AzureSqlManagedInstanceSettings => None,
        })
    }
    /// Native SHOW returns a global effective value, no saved value or native default verdict.
    pub fn global_variable(self, name: &str, value: Option<String>) -> Option<SettingObservation> {
        let default = self.variable_default(name)?;
        let default = (!default.is_empty()).then(|| default.to_owned());
        let observation = SettingObservation {
            source: "global-variables".into(),
            namespace: "global".into(),
            name: name.into(),
            scope: MeasurementScope::Server,
            nature: SettingNature::Configuration,
            value: None,
            value_in_use: value,
            value_for_secondary: None,
            unit: variable_unit(name).map(str::to_owned),
            native_context: Some("global".into()),
            native_source: None,
            pending_restart: None,
            native_is_default: None,
            default_source: if default.is_some() {
                SettingDefaultSource::EngineSpec
            } else {
                SettingDefaultSource::Unknown
            },
            default_value: default,
        };
        observation.is_valid().then_some(observation)
    }
    pub fn project_global_variable(self, observation: &SettingObservation) -> Option<Setting> {
        if !observation.is_valid()
            || observation.source != "global-variables"
            || observation.namespace != "global"
            || observation.scope != MeasurementScope::Server
            || observation.nature != SettingNature::Configuration
            || observation.native_context.as_deref() != Some("global")
            || observation.value.is_some()
            || observation.value_for_secondary.is_some()
            || observation.unit.as_deref() != variable_unit(&observation.name)
        {
            return None;
        }
        let expected = self.global_variable(&observation.name, observation.value_in_use.clone())?;
        if expected.default_source != observation.default_source
            || expected.default_value != observation.default_value
            || observation.native_is_default.is_some()
            || observation.pending_restart.is_some()
        {
            return None;
        }
        Some(Setting {
            name: observation.name.clone(),
            value: observation.value_in_use.clone()?,
            default_value: observation.default_value.clone(),
            scope: "server".into(),
        })
    }
}
fn variable_unit(name: &str) -> Option<&'static str> {
    match name {
        "performance_schema_max_digest_length"
        | "performance_schema_max_sql_text_length"
        | "max_digest_length"
        | "innodb_buffer_pool_size"
        | "innodb_page_size"
        | "tmp_table_size"
        | "max_heap_table_size"
        | "temptable_max_ram"
        | "sort_buffer_size"
        | "join_buffer_size" => Some("B"),
        "long_query_time" | "max_statement_time" => Some("s"),
        "max_execution_time" => Some("ms"),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
