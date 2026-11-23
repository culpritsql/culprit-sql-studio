//! Source-declared native Settings interpretation; public labels never establish its private read owner.
use super::*;
use crate::{DatabaseProduct, Engine, ServerInfo, Setting};

const MAX_NATIVE_VERSION_BYTES: usize = 128;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum NativeSettingsFormat {
    PostgresSettings,
    MysqlGlobalVariables,
    MariaDbGlobalVariables,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct NativeSettingsContext {
    pub format: NativeSettingsFormat,
    /// PostgreSQL server_version_num, or the original MySQL/MariaDB VERSION() value.
    pub version: String,
}
impl NativeSettingsContext {
    pub fn is_valid(&self) -> bool {
        if self.version.is_empty() || self.version.len() > MAX_NATIVE_VERSION_BYTES || !self.version.is_ascii() {
            return false;
        }
        self.normalized_version().is_some()
    }
    pub fn is_valid_for(&self, engine: Engine) -> bool {
        self.is_valid() && self.format.engine() == engine
    }
    /// Project observed values without applying a role policy. PostgreSQL applicability remains on its
    /// original raw cohort; missing values are never converted into empty strings or native defaults.
    pub fn project(&self, observation: &SettingObservation) -> Option<Setting> {
        if !self.is_valid() {
            return None;
        }
        match self.format {
            NativeSettingsFormat::PostgresSettings => observation.to_postgres_setting(),
            NativeSettingsFormat::MysqlGlobalVariables | NativeSettingsFormat::MariaDbGlobalVariables => {
                self.format.project_global_variable(observation)
            }
        }
    }
    /// Apply only the original native product/version. This does not fill current edition/platform,
    /// database size, counter reset or a different capture's identity.
    pub fn apply_server_metadata(&self, server: &mut ServerInfo) -> bool {
        if !self.is_valid_for(server.engine) {
            return false;
        }
        let Some(version) = self.normalized_version() else { return false };
        let Some(product) = server.engine.spec().products().iter().find(|product| product.id == self.format.product())
        else {
            return false;
        };
        server.product = format!("{} {}", product.label, version.split('-').next().unwrap_or(&version));
        server.version = version;
        true
    }
    fn normalized_version(&self) -> Option<String> {
        match self.format {
            NativeSettingsFormat::PostgresSettings => {
                let version = canonical_number(&self.version)?;
                if version < 90_600 {
                    return None;
                }
                Some(if version >= 100_000 {
                    format!("{}.{}", version / 10_000, version % 10_000)
                } else {
                    format!("{}.{}.{}", version / 10_000, version / 100 % 100, version % 100)
                })
            }
            NativeSettingsFormat::MysqlGlobalVariables => {
                if self.version.to_ascii_lowercase().contains("mariadb") {
                    return None;
                }
                version_parts(&self.version).map(|_| self.version.clone())
            }
            NativeSettingsFormat::MariaDbGlobalVariables => {
                let version = self.version.strip_prefix("5.5.5-").unwrap_or(&self.version);
                let (numeric, suffix) = version.split_once("-MariaDB")?;
                numeric_triplet(numeric)?;
                if !suffix.is_empty() && (!suffix.starts_with('-') || !build_suffix(&suffix[1..])) {
                    return None;
                }
                Some(version.into())
            }
        }
    }
}
impl NativeSettingsFormat {
    pub fn engine(self) -> Engine {
        match self {
            Self::PostgresSettings => Engine::Postgres,
            Self::MysqlGlobalVariables | Self::MariaDbGlobalVariables => Engine::Mysql,
        }
    }
    fn product(self) -> DatabaseProduct {
        match self {
            Self::PostgresSettings => DatabaseProduct::Postgres,
            Self::MysqlGlobalVariables => DatabaseProduct::Mysql,
            Self::MariaDbGlobalVariables => DatabaseProduct::Mariadb,
        }
    }
}
fn canonical_number(value: &str) -> Option<u32> {
    let number = value.parse::<u32>().ok()?;
    (number.to_string() == value).then_some(number)
}
fn numeric_triplet(value: &str) -> Option<[u32; 3]> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let version = [canonical_number(parts[0])?, canonical_number(parts[1])?, canonical_number(parts[2])?];
    (version[0] > 0).then_some(version)
}
fn build_suffix(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+' | b'~'))
}
fn version_parts(version: &str) -> Option<[u32; 3]> {
    let (numeric, suffix) =
        version.split_once('-').map_or((version, None), |(numeric, suffix)| (numeric, Some(suffix)));
    if suffix.is_some_and(|suffix| !build_suffix(suffix)) {
        return None;
    }
    numeric_triplet(numeric)
}

#[cfg(test)]
mod tests;
