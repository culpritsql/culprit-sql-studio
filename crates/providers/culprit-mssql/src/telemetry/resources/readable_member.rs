//! Readonly resource history uses the same native window query/decoder and pager. The native current
//! role/reader is separate from the historical executor/member and never becomes sample.replica_id.
//! https://learn.microsoft.com/azure/azure-sql/database/read-scale-out#monitor-and-troubleshoot-read-only-replicas
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-db-resource-stats-azure-sql-database
use super::*;
use crate::{activity::IdentityDetail, local_source};

pub(super) async fn identity(reader: &mut impl NativeRead, host: &str, port: u16) -> Result<DatabaseContext> {
    let observed = crate::database_identity::read_azure(reader).await?;
    let database = observed.identity(host, port);
    let physical = observed.physical_incarnation().ok_or_else(decode::malformed)?;
    match observed.writable {
        Some(true) => Ok(DatabaseContext::Primary { database, physical: physical.into() }),
        Some(false) => {
            let generation = connection(reader)?;
            let (context, _) = local_source::read_initial(
                reader,
                host,
                port,
                IdentityDetail::AzureDatabase,
                local_source::Detail::Authority,
            )
            .await?;
            if connection(reader)? != generation
                || context.activity.database != database
                || context.physical_database.as_deref() != Some(physical)
                || context.activity.writable != Some(false)
            {
                return Err(CollectionStop::SourceChanged.error());
            }
            context.readable_resource_history_reader()?;
            Ok(DatabaseContext::Readable(Box::new(context)))
        }
        None => Err(CollectionStop::SourceUnavailable.error()),
    }
}
fn connection(reader: &impl NativeRead) -> Result<u64> {
    reader
        .connection_generation()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| CollectionStop::SourceUnavailable.error())
}
fn require_connection(reader: &impl NativeRead, context: &local_source::Context) -> Result<()> {
    if context.connection_generation() != Some(connection(reader)?) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
pub(super) async fn recheck(
    reader: &mut impl NativeRead,
    before: &local_source::Context,
    host: &str,
    port: u16,
) -> Result<local_source::Context> {
    require_connection(reader, before)?;
    let after = before.recheck(reader, host, port).await?;
    require_connection(reader, before)?;
    if !before.same_observed_source(&after) || ObservationBounds::new(&before.activity.at, &after.activity.at).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    after.readable_resource_history_reader()?;
    Ok(after)
}
pub(super) fn observation(context: &DatabaseContext, inventory: &Inventory) -> Result<SourceObservation> {
    match context {
        DatabaseContext::Primary { .. } => Ok(SourceObservation::RetainedSamples(inventory.history.clone())),
        DatabaseContext::Readable(context) => {
            let history = MssqlReadableResourceHistory {
                history: inventory.history.clone(),
                current_reader: Box::new(context.readable_resource_history_reader()?),
                observed_role: inventory.replica_role.and_then(MssqlResourceReplicaRole::from_native_code),
            };
            if !history.is_valid() {
                return Err(decode::malformed());
            }
            Ok(SourceObservation::MssqlReadableResources(Box::new(history)))
        }
    }
}
pub(super) fn validate_inventory(context: &DatabaseContext, inventory: &Inventory) -> Result<()> {
    let valid = match context {
        DatabaseContext::Primary { .. } => inventory.replica_role.is_none_or(|role| role == 0),
        DatabaseContext::Readable(_) => inventory.replica_role.is_none_or(|role| {
            matches!(
                MssqlResourceReplicaRole::from_native_code(role),
                Some(
                    MssqlResourceReplicaRole::HighAvailabilitySecondary
                        | MssqlResourceReplicaRole::GeoReplicationForwarder
                        | MssqlResourceReplicaRole::NamedReplica
                )
            )
        }),
    };
    if !valid {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
pub(super) fn validate_page(
    context: &DatabaseContext,
    inventory: &Inventory,
    rows: &[ResourceMeasurement],
) -> Result<()> {
    if matches!(context, DatabaseContext::Readable(_))
        && rows.iter().any(|sample| {
            sample.replica_role
                != inventory
                    .replica_role
                    .and_then(MssqlResourceReplicaRole::from_native_code)
                    .map(NativeResourceReplicaRole::MssqlDbResourceStats)
                || sample.replica_id.is_some()
        })
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
pub(super) async fn page(
    reader: &mut impl NativeRead,
    context: &local_source::Context,
    host: &str,
    port: u16,
    window: &Window,
    cursor: Option<&str>,
    limit: u32,
) -> ReadResult<Vec<ResourceMeasurement>> {
    recheck(reader, context, host, port).await?;
    // An actual native error/cancel/budget stop returns immediately: no replacement SET or post-error read.
    let samples = resources::windowed_on_reader(reader, window, cursor, limit, context).await?;
    require_connection(reader, context)?;
    let rows = samples
        .into_iter()
        .map(|sample| {
            if sample.replica_role.is_none_or(|role| role == MssqlResourceReplicaRole::Primary) {
                return Err(CollectionStop::SourceChanged.error());
            }
            Ok(sample.exported())
        })
        .collect::<Result<Vec<_>>>()?;
    recheck(reader, context, host, port).await?;
    Ok(rows)
}

#[cfg(test)]
mod tests;
