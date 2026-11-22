use super::*;
use culprit_core::{DerivedContextIssue as Issue, MeasurementScope};

/// The same native-to-Setting mapping is used by scans, retained analysis and historical comparisons.
pub(crate) fn project(
    original: &dyn OriginalProjection,
    value: &SettingObservation,
) -> std::result::Result<Setting, Issue> {
    if value.nature == SettingNature::Unknown {
        return Err(Issue::PartialCatalog);
    }
    if !matches!(
        value.namespace.as_str(),
        "sys.databases"
            | "sys.database_scoped_configurations"
            | "sys.configurations"
            | "sys.database_query_store_options"
    ) {
        return Err(Issue::PartialCatalog);
    }
    if value.scope == MeasurementScope::Server
        && !matches!(original.source().partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) })
    {
        return Err(Issue::UnqualifiedCatalogScope);
    }
    value.to_mssql_setting().ok_or(Issue::PartialCatalog)
}
