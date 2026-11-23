//! Saved read-only archive connections. Native database routes, factories and source credentials are absent.
mod credentials;
mod storage;
use crate::{
    Workspace,
    evidence::{EvidenceRuntime, EvidenceService},
    secrets::SecretStore,
};
use culprit_core::{
    telemetry::{CollectionStop, DeliveryProblem},
    *,
};
use culprit_read::cancellation::CollectionCancellation;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

const MAX_READ_HANDLES: usize = 128;
#[derive(Clone)]
pub(crate) struct WarehouseConnections {
    storage: storage::Storage,
    runtime: Arc<EvidenceRuntime>,
    // Retain the real transport/credential owners across cancelled/replaced UI calls. No proof/body cache.
    // Retired revisions remain bounded, since dropping a caller does not prove its native HTTP worker ended.
    handles: Arc<Mutex<BTreeMap<String, Arc<EvidenceService>>>>,
}
impl WarehouseConnections {
    pub fn new(config: PathBuf) -> Self {
        Self {
            storage: storage::Storage::new(config),
            runtime: Arc::new(EvidenceRuntime::default()),
            handles: Default::default(),
        }
    }
    fn with_runtime(mut self, runtime: Arc<EvidenceRuntime>) -> Self {
        self.runtime = runtime;
        self.handles = Default::default();
        self
    }
    pub(crate) fn clear_handles(&mut self) {
        self.handles = Default::default();
    }
    fn prepare(
        &self,
        reference: &WarehouseConnectionRef,
        secrets: Arc<dyn SecretStore>,
        cancellation: &CollectionCancellation,
        deadline: Instant,
    ) -> Result<Arc<EvidenceService>> {
        let mut handles = self.handles.lock().map_err(|_| capacity())?;
        // The worker can wait behind another revision's preparation after its caller has ended.
        check(cancellation, deadline)?;
        let saved = self.storage.current(reference)?;
        check(cancellation, deadline)?;
        if let Some(service) = handles.get(&reference.revision) {
            return Ok(service.clone());
        }
        if handles.len() >= MAX_READ_HANDLES {
            return Err(capacity());
        }
        let (config, credentials) = credentials::compose(&saved, secrets)?;
        check(cancellation, deadline)?;
        let service = Arc::new(self.runtime_configured(config, credentials)?);
        // Existing production factories construct clients only. Already-started HTTP work is never
        // declared quiet by this check; it remains in the unchanged service/transport owner.
        check(cancellation, deadline)?;
        handles.insert(reference.revision.clone(), service.clone());
        Ok(service)
    }
    fn runtime_configured(
        &self,
        config: crate::evidence::EvidenceConfig,
        credentials: Arc<dyn culprit_auth::CredentialResolver>,
    ) -> Result<EvidenceService> {
        self.runtime.evidence_with_credentials(&config, credentials)
    }
}
impl Workspace {
    pub fn with_warehouse_runtime(mut self, runtime: Arc<EvidenceRuntime>) -> Self {
        self.warehouse = self.warehouse.with_runtime(runtime);
        self
    }
    pub fn warehouse_connections(&self) -> Result<Vec<WarehouseConnectionInfo>> {
        self.warehouse.storage.list()
    }
    pub fn add_warehouse_connection(&self, input: SaveWarehouseConnection) -> Result<WarehouseConnectionInfo> {
        self.warehouse.storage.add(input, self.secrets.as_ref())
    }
    pub fn remove_warehouse_connection(&self, reference: &WarehouseConnectionRef) -> Result<()> {
        self.warehouse.storage.remove(reference, self.secrets.as_ref())
    }
    pub async fn warehouse_resources(
        &self,
        reference: &WarehouseConnectionRef,
        request: &PublishedResourceRequest,
        cancellation: &CollectionCancellation,
    ) -> Result<PublishedResources> {
        self.with_warehouse_read(
            reference,
            cancellation,
            culprit_evidence::analytical::evidence::EvidenceReadBudget::default(),
            |service, budget| async move { service.resources(request, budget, cancellation).await },
        )
        .await
    }
    pub async fn warehouse_incident(
        &self,
        reference: &WarehouseConnectionRef,
        resource: &PublishedResourceSelector,
        around: &str,
        cancellation: &CollectionCancellation,
    ) -> Result<IncidentReadReport> {
        self.with_warehouse_read(
            reference,
            cancellation,
            crate::evidence::IncidentRequest::default_budget(),
            |service, budget| async move { service.resource_incident(resource, around, budget, cancellation).await },
        )
        .await
    }
    pub async fn warehouse_window(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseWindowQuery,
        cancellation: &CollectionCancellation,
    ) -> Result<WarehouseWindowReport> {
        self.read_warehouse_window(reference, request, cancellation, None).await
    }
    pub async fn warehouse_window_with_permit(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseWindowQuery,
        cancellation: &CollectionCancellation,
        permit: crate::evidence::EvidenceReadPermit,
    ) -> Result<WarehouseWindowReport> {
        self.read_warehouse_window(reference, request, cancellation, Some(permit)).await
    }
    async fn read_warehouse_window(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseWindowQuery,
        cancellation: &CollectionCancellation,
        permit: Option<crate::evidence::EvidenceReadPermit>,
    ) -> Result<WarehouseWindowReport> {
        self.with_warehouse_read(
            reference,
            cancellation,
            crate::evidence::IncidentRequest::default_budget(),
            |service, budget| async move {
                match permit {
                    Some(permit) => service.window_with_permit(request, budget, cancellation, permit).await,
                    None => service.window(request, budget, cancellation).await,
                }
            },
        )
        .await
    }
    pub async fn warehouse_artifacts(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseArtifactQuery,
        cancellation: &CollectionCancellation,
    ) -> Result<WarehouseArtifactReport> {
        self.read_warehouse_artifacts(reference, request, cancellation, None).await
    }
    pub async fn warehouse_artifacts_with_permit(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseArtifactQuery,
        cancellation: &CollectionCancellation,
        permit: crate::evidence::EvidenceReadPermit,
    ) -> Result<WarehouseArtifactReport> {
        self.read_warehouse_artifacts(reference, request, cancellation, Some(permit)).await
    }
    async fn read_warehouse_artifacts(
        &self,
        reference: &WarehouseConnectionRef,
        request: &WarehouseArtifactQuery,
        cancellation: &CollectionCancellation,
        permit: Option<crate::evidence::EvidenceReadPermit>,
    ) -> Result<WarehouseArtifactReport> {
        request.validate()?;
        self.with_warehouse_read(
            reference,
            cancellation,
            EvidenceService::artifact_budget(request),
            |service, budget| async move {
                match permit {
                    Some(permit) => service.artifacts_with_permit(request, budget, cancellation, permit).await,
                    None => service.artifacts(request, budget, cancellation).await,
                }
            },
        )
        .await
    }
    async fn with_warehouse_read<T, F, Reading>(
        &self,
        reference: &WarehouseConnectionRef,
        cancellation: &CollectionCancellation,
        budget: culprit_evidence::analytical::evidence::EvidenceReadBudget,
        read: F,
    ) -> Result<T>
    where
        F: FnOnce(Arc<EvidenceService>, culprit_evidence::analytical::evidence::EvidenceReadBudget) -> Reading,
        Reading: std::future::Future<Output = Result<T>>,
    {
        let deadline = Instant::now() + Duration::from_millis(u64::from(budget.timeout_ms));
        let preparing = async {
            check(cancellation, deadline)?;
            let manager = self.warehouse.clone();
            let selected = reference.clone();
            let secrets = self.secrets.clone();
            let preparing = cancellation.clone();
            let service =
                crate::run_blocking(move || manager.prepare(&selected, secrets, &preparing, deadline)).await?;
            self.require_warehouse(reference).await?;
            check(cancellation, deadline)?;
            Ok::<_, Error>(service)
        };
        // Preparation retains its existing bounded command/client owner. Once admitted, the service owns the
        // remaining action deadline and cooperatively settles its read/CPU work instead of losing its awaiter.
        let service = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            result = tokio::time::timeout_at(deadline, preparing) => {
                result.map_err(|_| CollectionStop::TimedOut.error())??
            }
        };
        let report = read(service, remaining(budget, deadline)?).await?;
        self.require_warehouse(reference).await?;
        check(cancellation, deadline)?;
        Ok(report)
    }
    async fn require_warehouse(&self, reference: &WarehouseConnectionRef) -> Result<()> {
        let storage = self.warehouse.storage.clone();
        let reference = reference.clone();
        crate::run_blocking(move || storage.current(&reference).map(|_| ())).await
    }
}
fn check(cancellation: &CollectionCancellation, deadline: Instant) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(CollectionStop::Cancelled.error());
    }
    if Instant::now() >= deadline {
        return Err(CollectionStop::TimedOut.error());
    }
    Ok(())
}
fn remaining(
    mut budget: culprit_evidence::analytical::evidence::EvidenceReadBudget,
    deadline: Instant,
) -> Result<culprit_evidence::analytical::evidence::EvidenceReadBudget> {
    budget.timeout_ms = u32::try_from(deadline.saturating_duration_since(Instant::now()).as_millis())
        .map_err(|_| capacity())?
        .min(budget.timeout_ms);
    if budget.timeout_ms == 0 {
        return Err(CollectionStop::TimedOut.error());
    }
    Ok(budget)
}
fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}
fn invalid() -> Error {
    Error::invalid(culprit_core::t!("warehouse.connection.invalid"))
}
#[cfg(all(test, feature = "warehouse-clickhouse", feature = "evidence-azure", feature = "auth-azure"))]
mod tests;
