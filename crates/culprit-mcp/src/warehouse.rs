//! One explicit host grant for read-only access to the injected reader's configured archive namespace.
use culprit_core::{Error, ErrorCode, Result, t};
use culprit_engine::{WarehouseFleetReader, WarehouseIncidentReader};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Native saved-connection access is independent. This grant permits observed-resource listing, fleet rollups and
/// incident/artifact reads inside the reader's configured warehouse/recovery namespace. Backend credentials keep
/// their own rights. Revocation is monotonic: a host must deliberately install a new delegation to grant access again.
pub struct WarehouseDelegation {
    reader: Arc<dyn WarehouseIncidentReader>,
    fleet: Option<Arc<dyn WarehouseFleetReader>>,
    active: AtomicBool,
}

impl WarehouseDelegation {
    pub fn read_only(reader: Arc<dyn WarehouseIncidentReader>) -> Self {
        Self { reader, fleet: None, active: AtomicBool::new(true) }
    }

    pub fn with_fleet(mut self, fleet: Arc<dyn WarehouseFleetReader>) -> Self {
        self.fleet = Some(fleet);
        self
    }

    pub fn revoke(&self) {
        self.active.store(false, Ordering::Release);
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::new(ErrorCode::PermissionDenied, t!("mcp.warehouse.access_denied")))
        }
    }

    pub(crate) fn reader(&self) -> Result<&dyn WarehouseIncidentReader> {
        self.check()?;
        Ok(self.reader.as_ref())
    }

    pub(crate) fn fleet(&self) -> Result<&dyn WarehouseFleetReader> {
        self.check()?;
        self.fleet.as_deref().ok_or_else(|| Error::new(ErrorCode::Unsupported, t!("mcp.fleet_attention.unavailable")))
    }
}
