//! Retained replica-local values and the current reader have separate namespaces and clocks.
use super::*;

pub const MSSQL_READABLE_RESOURCE_HISTORY_SOURCE: &str = "azure-readable-resource-history";

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlReadableResourceLimitation {
    HistoricalMemberUnknown,
    HistoricalRoleUnknown,
    SampleContinuityUnknown,
    CurrentMemberUnknown,
}

/// The native view retains about one hour. Its role code is reported at acquisition; it is not a historical
/// executor/member witness. In particular, the current reader's ReplicaID is never copied into a sample.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlReadableResourceHistory {
    pub history: SampleHistoryDescription,
    pub current_reader: Box<NativeReadContext>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub observed_role: Option<MssqlResourceReplicaRole>,
}
impl MssqlReadableResourceHistory {
    pub fn is_valid(&self) -> bool {
        self.history.is_valid()
            && self.current_reader.is_valid()
            && self.current_reader.target.engine() == crate::Engine::Mssql
            && self.current_reader.target.database().is_some_and(|database| database.incarnation.is_some())
            && self.current_reader.database_writable == Some(false)
            && matches!(&self.current_reader.partition, SourcePartition::InstanceLocal { epoch: Some(epoch), .. } if valid_digest(epoch))
            && ObservationBounds::new(&self.current_reader.observed_until, &self.history.observed_at).is_some()
            && self.observed_role != Some(MssqlResourceReplicaRole::Primary)
            && (self.observed_role.is_some() || self.history.expected_samples == 0)
    }
    pub fn matches_source(&self, source: &CollectionDescription) -> bool {
        self.is_valid()
            && source.source == MSSQL_READABLE_RESOURCE_HISTORY_SOURCE
            && source.target == self.current_reader.target
            // The retained row's native member/epoch is unknown. The current reader cannot supply it.
            && source.partition == SourcePartition::Target
            && source.continuity == SourceContinuity::Unverified
    }
    pub fn accepts(&self, row: &ResourceMeasurement) -> bool {
        self.is_valid()
            && row.is_valid()
            && row.mssql_managed_instance.is_none()
            && row.replica_id.is_none()
            && row.replica_role == self.observed_role.map(NativeResourceReplicaRole::MssqlDbResourceStats)
            && self.observed_role.is_some()
            && row.metrics.iter().all(|metric| {
                matches!(metric.scope, crate::MeasurementScope::Database | crate::MeasurementScope::Server)
            })
            && ObservationBounds::new(&self.history.from, &row.at).is_some()
            && DateTime::parse_from_rfc3339(&row.at).ok() < DateTime::parse_from_rfc3339(&self.history.until).ok()
    }
    pub fn limitations(&self) -> Vec<MssqlReadableResourceLimitation> {
        let mut limitations = vec![
            MssqlReadableResourceLimitation::HistoricalMemberUnknown,
            MssqlReadableResourceLimitation::HistoricalRoleUnknown,
            MssqlReadableResourceLimitation::SampleContinuityUnknown,
        ];
        if !self.current_reader.has_qualified_local_owner() {
            limitations.push(MssqlReadableResourceLimitation::CurrentMemberUnknown);
        }
        limitations
    }
}

#[cfg(test)]
mod tests;
