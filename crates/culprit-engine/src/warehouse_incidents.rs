//! Optional source-free incident lookup. Concrete warehouse/auth/storage composition stays outside this interface.
use culprit_core::{
    IncidentReadReport, PublishedResourceRequest, PublishedResourceSelector, PublishedResources, Result,
};
use culprit_read::cancellation::CollectionCancellation;
use std::{future::Future, pin::Pin};

pub type WarehouseIncidentTask<'a> = Pin<Box<dyn Future<Output = Result<IncidentReadReport>> + Send + 'a>>;
pub type WarehouseResourcesTask<'a> = Pin<Box<dyn Future<Output = Result<PublishedResources>> + Send + 'a>>;

/// The host installs one independently configured archive reader. Implementations own their bounded action
/// deadline and settle their existing read owner after cancellation; this interface grants no native-source rights.
pub trait WarehouseIncidentReader: Send + Sync {
    fn resources<'a>(
        &'a self,
        request: &'a PublishedResourceRequest,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a>;
    fn incident<'a>(
        &'a self,
        resource: &'a PublishedResourceSelector,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a>;
}
