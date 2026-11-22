//! Optional source-free incident lookup. Concrete warehouse/auth/storage composition stays outside this interface.
use crate::ConnectionInfo;
use culprit_core::{DatabaseIdentity, IncidentReadReport, Result};
use culprit_read::cancellation::CollectionCancellation;
use std::{future::Future, pin::Pin};

pub type WarehouseIncidentTask<'a> = Pin<Box<dyn Future<Output = Result<IncidentReadReport>> + Send + 'a>>;

/// The host installs one independently configured archive reader. Implementations own their bounded action
/// deadline and settle their existing read owner after cancellation; this interface grants no native-source rights.
pub trait WarehouseIncidentReader: Send + Sync {
    fn incident<'a>(
        &'a self,
        database: &'a DatabaseIdentity,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a>;
}

/// Saved route metadata selects archive candidates only. It proves no current native incarnation or role.
pub fn saved_incident_database(info: &ConnectionInfo) -> Result<DatabaseIdentity> {
    let database = info.database.as_deref().filter(|database| !database.trim().is_empty()).ok_or_else(route_unknown)?;
    if info.host.trim().is_empty() || info.engine.spec().default_port().is_none() {
        return Err(route_unknown());
    }
    Ok(DatabaseIdentity::server(info.engine, database, &info.host, info.engine.network_port(info.port)?, None))
}
fn route_unknown() -> culprit_core::Error {
    culprit_core::Error::invalid(culprit_core::t!("incident.cli.route_required"))
}

#[cfg(test)]
mod tests;
