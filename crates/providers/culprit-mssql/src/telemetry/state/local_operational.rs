//! Explicit database-local Operational families over the original held acquisition and final authority.
//! Native SQL, decoders, pressure admission and per-family outcomes remain with their existing owners.
use super::*;
use crate::operations::native;
use culprit_core::{FileScope, MeasurementScope};

pub(super) fn allows(family: point::Family) -> bool {
    matches!(
        family,
        point::Family::Operation(native::Kind::Log | native::Kind::Filegroups)
            | point::Family::FileIo
            | point::Family::VersionStore
            | point::Family::PersistentVersions
    )
}
pub(super) fn validate(
    owner: &subject::Identity,
    family: point::Family,
    result: &Result<point::ReadOutcome>,
) -> Result<()> {
    let subject::Identity::LocalDatabase(owner) = owner else { return Err(invalid_data()) };
    if !allows(family) {
        return Err(invalid_data());
    }
    let Ok(point::ReadOutcome::Observed(batch)) = result else { return Ok(()) };
    let database_id = owner.database_id.to_string();
    let database = owner.activity.database.database.as_str();
    for record in &batch.values {
        let TelemetryRecord::Operational(row) = record else { return Err(invalid_data()) };
        if row.source != family.source().name || !row.is_valid() {
            return Err(invalid_data());
        }
        let matches = match (family, &row.fact) {
            (point::Family::Operation(native::Kind::Filegroups), OperationalFact::MssqlDatabaseFile(file)) => {
                file.native_database_id == database_id && file.database == database
            }
            (point::Family::Operation(native::Kind::Filegroups), OperationalFact::Space(space)) => {
                space.population == StoragePopulation::WritableOnlineDataFilegroups
                    && space.database.as_deref() == Some(database)
            }
            (point::Family::Operation(native::Kind::Log), OperationalFact::Space(space)) => {
                space.population == StoragePopulation::DatabaseTransactionLog
                    && space.database.as_deref() == Some(database)
            }
            (point::Family::FileIo, OperationalFact::FileIo(file)) => {
                file.scope == FileScope::Database
                    && file.native_database_id == database_id
                    && file.database.as_deref().is_none_or(|name| name == database)
            }
            (point::Family::VersionStore, OperationalFact::Counter(sample)) => {
                sample.scope == MeasurementScope::Database
                    && sample.nature == CounterNature::Gauge
                    && sample.unit == CounterUnit::Kibibytes
            }
            (point::Family::PersistentVersions, OperationalFact::MssqlPersistentVersionStore(_)) => true,
            _ => false,
        };
        if !matches {
            return Err(CollectionStop::SourceChanged.error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
