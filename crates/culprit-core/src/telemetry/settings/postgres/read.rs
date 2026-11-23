//! One read cohort owns native values and their acquisition; display strings cannot restore that ownership.
use super::*;
use crate::{DatabaseIdentity, Engine, Snapshot};
use std::collections::BTreeSet;

const MAX_POSTGRES_SETTINGS_READ: usize = 1_000;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum PostgresSettingsAcquisition {
    Completed {
        context: Box<NativeReadContext>,
    },
    Unqualified {
        before: Option<Box<NativeReadContext>>,
        after: Option<Box<NativeReadContext>>,
        reason: PostgresSettingsAcquisitionGap,
    },
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PostgresSettingsAcquisitionGap {
    ContextUnavailable,
    SourceChanged,
    ReadFailed,
    SharingWithheld,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresSettingsRead {
    pub settings_context: NativeSettingsContext,
    pub observations: Vec<SettingObservation>,
    /// EOF of this source's selected settings roster, not a census of every PostgreSQL setting.
    pub population_complete: bool,
    pub acquisition: PostgresSettingsAcquisition,
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PostgresSettingsAdviceLimit {
    MissingAcquisition,
    UnqualifiedAcquisition,
    RecoveryMaintenanceInactive,
    PartialPopulation,
    UnavailableValue,
    SessionValue,
    InvalidObservation,
}

/// A pure advice view, never native admission, a current action owner or a replacement for the raw observations.
pub struct PostgresConfigurationAdvice {
    pub settings: Vec<Setting>,
    pub limitations: Vec<PostgresSettingsAdviceLimit>,
}
impl PostgresConfigurationAdvice {
    pub fn setting(&self, name: &str) -> Option<&Setting> {
        self.settings.iter().find(|setting| setting.name.eq_ignore_ascii_case(name))
    }
    pub fn is_complete(&self) -> bool {
        self.limitations.is_empty()
    }
}
impl PostgresSettingsRead {
    pub fn is_valid(&self) -> bool {
        let mut names = BTreeSet::new();
        self.settings_context.is_valid_for(Engine::Postgres)
            && self.settings_context.format == NativeSettingsFormat::PostgresSettings
            && self.observations.len() <= MAX_POSTGRES_SETTINGS_READ
            && self.observations.iter().all(|observation| {
                observation.is_valid()
                    && observation.source == "pg_settings"
                    && observation.namespace == "pg_catalog.pg_settings"
                    && names.insert(observation.name.as_str())
            })
            && match &self.acquisition {
                PostgresSettingsAcquisition::Completed { context } => {
                    qualified_context(context) && context.database_writable.is_some()
                }
                PostgresSettingsAcquisition::Unqualified { before, after, .. } => [before, after]
                    .into_iter()
                    .flatten()
                    .all(|context| context.is_valid() && context.target.engine() == Engine::Postgres),
            }
    }

    pub fn configuration(&self, database: Option<&DatabaseIdentity>) -> PostgresConfigurationAdvice {
        let mut advice = PostgresConfigurationAdvice { settings: vec![], limitations: vec![] };
        if !self.is_valid() {
            advice.limitations.push(PostgresSettingsAdviceLimit::InvalidObservation);
            return advice;
        }
        if self.observations.is_empty() {
            advice.limitations.push(PostgresSettingsAdviceLimit::UnavailableValue);
        }
        let context = match &self.acquisition {
            PostgresSettingsAcquisition::Completed { context }
                if context.target.database() == database && database.is_some() =>
            {
                Some(context)
            }
            _ => {
                advice.limitations.push(PostgresSettingsAdviceLimit::UnqualifiedAcquisition);
                None
            }
        };
        let role = context.and_then(|context| context.database_writable);
        if !self.population_complete {
            advice.limitations.push(PostgresSettingsAdviceLimit::PartialPopulation);
        }
        for observation in &self.observations {
            let issue = if observation.scope != MeasurementScope::Server {
                Some(PostgresSettingsAdviceLimit::SessionValue)
            } else if let Some(setting) = self.settings_context.project(observation) {
                if applies_to_role(&observation.name, role) {
                    advice.settings.push(setting);
                    None
                } else {
                    Some(if role == Some(false) {
                        PostgresSettingsAdviceLimit::RecoveryMaintenanceInactive
                    } else {
                        PostgresSettingsAdviceLimit::UnqualifiedAcquisition
                    })
                }
            } else {
                Some(PostgresSettingsAdviceLimit::UnavailableValue)
            };
            if let Some(issue) = issue
                && !advice.limitations.contains(&issue)
            {
                advice.limitations.push(issue);
            }
        }
        advice
    }

    /// Sharing withholds the raw cohort and acquisition; it neither rewrites a native member nor grants a new one.
    pub fn withdraw_for_sharing(&mut self) {
        self.observations.clear();
        self.population_complete = false;
        self.acquisition = PostgresSettingsAcquisition::Unqualified {
            before: None,
            after: None,
            reason: PostgresSettingsAcquisitionGap::SharingWithheld,
        };
    }
}
fn qualified_context(context: &NativeReadContext) -> bool {
    context.has_qualified_local_owner()
        && context.target.database().is_some()
        && context.target.engine() == Engine::Postgres
        && matches!(&context.partition, SourcePartition::InstanceLocal { member: Some(member), .. }
            if member.kind == SharedResourceKind::Instance)
}

impl Snapshot {
    /// Only this provider's explicit cohort can qualify primary-maintenance settings. The display list,
    /// the probe's standby setting and the collector's read-only session are independent observations.
    pub fn postgres_configuration(&self) -> Option<PostgresConfigurationAdvice> {
        if self.server.engine != Engine::Postgres {
            return None;
        }
        Some(match &self.operations.postgres_settings {
            Some(read) => read.configuration(self.identity.as_ref()),
            None => PostgresConfigurationAdvice {
                settings: vec![],
                limitations: vec![PostgresSettingsAdviceLimit::MissingAcquisition],
            },
        })
    }
}

#[cfg(test)]
mod tests;
