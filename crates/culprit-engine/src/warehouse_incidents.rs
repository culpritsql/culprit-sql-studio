//! Optional source-free incident lookup. Concrete warehouse/auth/storage composition stays outside this interface.
use culprit_core::{
    IncidentReadReport, PublishedResourceRequest, PublishedResourceSelector, PublishedResources, Result,
    WarehouseArtifactQuery, WarehouseArtifactReport, WarehouseWindowQuery, WarehouseWindowReport,
};
use culprit_read::cancellation::CollectionCancellation;
use std::{future::Future, pin::Pin};

pub type WarehouseIncidentTask<'a> = Pin<Box<dyn Future<Output = Result<IncidentReadReport>> + Send + 'a>>;
pub type WarehouseArtifactsTask<'a> = Pin<Box<dyn Future<Output = Result<WarehouseArtifactReport>> + Send + 'a>>;
pub type WarehouseWindowTask<'a> = Pin<Box<dyn Future<Output = Result<WarehouseWindowReport>> + Send + 'a>>;
pub type WarehouseResourcesTask<'a> = Pin<Box<dyn Future<Output = Result<PublishedResources>> + Send + 'a>>;

/// The host installs one independently configured archive reader. Implementations own their bounded action
/// deadline and settle their existing read owner after cancellation; this interface grants no native-source rights.
pub trait WarehouseIncidentReader: Send + Sync {
    fn resources<'a>(
        &'a self,
        request: &'a PublishedResourceRequest,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a>;
    fn window<'a>(
        &'a self,
        request: &'a WarehouseWindowQuery,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseWindowTask<'a>;
    fn artifacts<'a>(
        &'a self,
        request: &'a WarehouseArtifactQuery,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseArtifactsTask<'a>;
    fn incident<'a>(
        &'a self,
        resource: &'a PublishedResourceSelector,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a>;
}
