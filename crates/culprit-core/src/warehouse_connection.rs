//! Archive connections are read-only warehouse/recovery routes, independent of analyzed-engine connections.
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseAzureProfile {
    pub name: String,
    /// Explicit CLI tenant selection, or the CLI's selected tenant when absent. No device-code fallback.
    pub tenant: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseAwsProfile {
    pub name: String,
    pub profile: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum WarehouseEndpoint {
    Adx { endpoint: String, database: String, authentication: WarehouseAzureProfile },
    Clickhouse { endpoint: String, database: String, username: String, credential_name: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum WarehouseRecovery {
    AzureBlob { account: String, container: String, prefix: String, authentication: WarehouseAzureProfile },
    S3 { bucket: String, region: String, prefix: String, expected_owner: String, authentication: WarehouseAwsProfile },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseConnectionSettings {
    pub pipeline: String,
    pub shard: String,
    pub warehouse: WarehouseEndpoint,
    pub recovery: WarehouseRecovery,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseConnectionRef {
    pub name: String,
    /// A consistency binding to one saved archive revision, never native identity or authorization by itself.
    pub revision: String,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseConnectionInfo {
    pub reference: WarehouseConnectionRef,
    pub settings: WarehouseConnectionSettings,
    /// Recorded keychain persistence, not proof that the operating system will allow a later password read.
    pub password_saved: bool,
}
/// A write-only command input. Rust cannot serialize the supplied password into metadata, reports or errors.
#[derive(Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SaveWarehouseConnection {
    pub name: String,
    pub settings: WarehouseConnectionSettings,
    pub password: Option<String>,
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseAnalysisLimitations {
    pub observed_from: String,
    pub observed_until: String,
    pub reasons: Vec<String>,
}
/// The canonical incident result plus existing source-owned explanation text for a desktop reader.
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseIncidentView {
    pub report: crate::IncidentReadReport,
    pub analysis_limitations: Vec<WarehouseAnalysisLimitations>,
}
impl WarehouseIncidentView {
    pub fn new(mut report: crate::IncidentReadReport) -> Self {
        let analysis_limitations = report
            .analyses
            .iter()
            .map(|analysis| WarehouseAnalysisLimitations {
                observed_from: analysis.description.observed_from.clone(),
                observed_until: analysis.description.observed_until.clone(),
                reasons: analysis.description.limitations(),
            })
            .collect();
        for incident in &mut report.incidents {
            for query in incident
                .affected_queries
                .iter_mut()
                .chain(&mut incident.culprit_queries)
                .chain(&mut incident.candidate_queries)
            {
                if let Some(role) = &mut query.causal_role {
                    role.explanation.render();
                }
            }
        }
        Self { report, analysis_limitations }
    }
}
