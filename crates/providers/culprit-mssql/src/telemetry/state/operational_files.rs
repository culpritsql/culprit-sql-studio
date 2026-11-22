//! One existing FILE_CAPACITY batch on the held local database owner; other Operational families stay unavailable.
use super::*;
use crate::operations::native;

pub(super) fn allows(family: point::Family) -> bool {
    matches!(family, point::Family::Operation(native::Kind::Filegroups))
}
pub(super) fn check_transport(owner: &subject::Identity, generation: Option<u64>) -> Result<()> {
    let subject::Identity::LocalDatabase(owner) = owner else { return Err(invalid_data()) };
    if owner.connection_generation().is_none_or(|expected| expected == 0 || Some(expected) != generation) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
pub(super) fn validate(owner: &subject::Identity, result: &Result<point::ReadOutcome>) -> Result<()> {
    let subject::Identity::LocalDatabase(owner) = owner else { return Err(invalid_data()) };
    let Ok(point::ReadOutcome::Observed(batch)) = result else { return Ok(()) };
    let database_id = owner.database_id.to_string();
    for row in &batch.values {
        let TelemetryRecord::Operational(row) = row else { return Err(invalid_data()) };
        if row.source != culprit_core::source_id::FILE_CAPACITY {
            return Err(invalid_data());
        }
        let matches = match &row.fact {
            OperationalFact::MssqlDatabaseFile(file) => {
                file.native_database_id == database_id && file.database == owner.activity.database.database
            }
            OperationalFact::Space(space) => {
                space.population == StoragePopulation::WritableOnlineDataFilegroups
                    && space.database.as_deref() == Some(owner.activity.database.database.as_str())
            }
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
