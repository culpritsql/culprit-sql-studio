//! Segregated read-only fleet triage interface. Single-resource incident/window readers stay independent (ISP).
use culprit_core::{Result, WarehouseFleetReport, WarehouseFleetRequest};
use culprit_read::cancellation::CollectionCancellation;
use std::{future::Future, pin::Pin};

pub type WarehouseFleetTask<'a> = Pin<Box<dyn Future<Output = Result<WarehouseFleetReport>> + Send + 'a>>;

pub trait WarehouseFleetReader: Send + Sync {
    fn fleet<'a>(
        &'a self,
        request: &'a WarehouseFleetRequest,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseFleetTask<'a>;
}
