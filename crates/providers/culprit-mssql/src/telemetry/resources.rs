//! Shared resource-history lifecycle; native scopes own identity, interpretation and primary policy.
use super::{decode, description, resource_rows::ResourcePager};
use crate::conn::Pool;
use crate::read::{NativeRead, ReadError, ReadResult, ReadSession as Session};
use crate::resources::{self, Inventory, Window};
use async_trait::async_trait;
use culprit_core::{DatabaseIdentity, Result, telemetry::*};
use std::sync::Arc;

mod managed_instance;
mod readable_member;
pub(super) use managed_instance::ManagedInstanceResourceSource;
pub(super) use managed_instance::bounded_budget as managed_instance_budget;

const ENCODED_ROW_ALLOWANCE: u32 = 4096;

#[async_trait]
pub(super) trait ResourceScope: Send {
    type Context: Send + Sync + PartialEq;
    fn source(&self, context: &Self::Context) -> &'static str;
    fn target(&self, context: &Self::Context) -> CollectionTarget;
    fn observation(&self, _: &Self::Context, inventory: &Inventory) -> Result<SourceObservation> {
        Ok(SourceObservation::RetainedSamples(inventory.history.clone()))
    }
    fn validate_inventory(&self, _: &Self::Context, _: &Inventory) -> Result<()> {
        Ok(())
    }
    fn validate_page(&self, _: &Self::Context, _: &Inventory, _: &[ResourceMeasurement]) -> Result<()> {
        Ok(())
    }
    fn same_inventory(&self, _: &Self::Context, before: &Inventory, after: &Inventory) -> bool {
        before.same_rows(after)
    }
    async fn recheck<R: NativeRead>(&self, reader: &mut R, _: &Self::Context) -> Result<Self::Context> {
        self.identity(reader).await
    }

    async fn identity<R: NativeRead>(&self, reader: &mut R) -> Result<Self::Context>;
    async fn inventory<R: NativeRead>(&self, reader: &mut R, window: &Window) -> Result<Inventory>;
    async fn page<R: NativeRead>(
        &self,
        reader: &mut R,
        context: &Self::Context,
        window: &Window,
        cursor: Option<&str>,
        limit: u32,
    ) -> ReadResult<Vec<ResourceMeasurement>>;
}

pub(super) struct DatabaseScope {
    host: String,
    port: u16,
}
pub(super) enum DatabaseContext {
    Primary { database: DatabaseIdentity, physical: String },
    Readable(Box<crate::local_source::Context>),
}
impl PartialEq for DatabaseContext {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Primary { database, physical }, Self::Primary { database: other, physical: incarnation }) => {
                database == other && physical == incarnation
            }
            (Self::Readable(before), Self::Readable(after)) => before.same_observed_source(after),
            _ => false,
        }
    }
}
#[async_trait]
impl ResourceScope for DatabaseScope {
    type Context = DatabaseContext;
    fn source(&self, context: &Self::Context) -> &'static str {
        match context {
            DatabaseContext::Primary { .. } => culprit_core::source_id::RESOURCES,
            DatabaseContext::Readable(_) => MSSQL_READABLE_RESOURCE_HISTORY_SOURCE,
        }
    }
    fn target(&self, context: &Self::Context) -> CollectionTarget {
        match context {
            DatabaseContext::Primary { database, .. } => database.clone().into(),
            DatabaseContext::Readable(context) => context.activity.database.clone().into(),
        }
    }
    fn observation(&self, context: &Self::Context, inventory: &Inventory) -> Result<SourceObservation> {
        readable_member::observation(context, inventory)
    }
    fn validate_inventory(&self, context: &Self::Context, inventory: &Inventory) -> Result<()> {
        readable_member::validate_inventory(context, inventory)
    }
    fn validate_page(
        &self,
        context: &Self::Context,
        inventory: &Inventory,
        rows: &[ResourceMeasurement],
    ) -> Result<()> {
        readable_member::validate_page(context, inventory, rows)
    }
    fn same_inventory(&self, _: &Self::Context, before: &Inventory, after: &Inventory) -> bool {
        before.same_rows(after) && before.replica_role == after.replica_role
    }
    async fn identity<R: NativeRead>(&self, reader: &mut R) -> Result<Self::Context> {
        readable_member::identity(reader, &self.host, self.port).await
    }
    async fn recheck<R: NativeRead>(&self, reader: &mut R, context: &Self::Context) -> Result<Self::Context> {
        match context {
            DatabaseContext::Primary { .. } => self.identity(reader).await,
            DatabaseContext::Readable(before) => Ok(DatabaseContext::Readable(Box::new(
                readable_member::recheck(reader, before, &self.host, self.port).await?,
            ))),
        }
    }
    async fn inventory<R: NativeRead>(&self, reader: &mut R, window: &Window) -> Result<Inventory> {
        resources::inventory(reader, window).await
    }
    async fn page<R: NativeRead>(
        &self,
        reader: &mut R,
        context: &Self::Context,
        window: &Window,
        cursor: Option<&str>,
        limit: u32,
    ) -> ReadResult<Vec<ResourceMeasurement>> {
        match context {
            DatabaseContext::Primary { .. } => resources::windowed(reader, window, cursor, limit)
                .await?
                .iter()
                .map(|sample| {
                    if sample.replica_role != Some(MssqlResourceReplicaRole::Primary) {
                        return Err(CollectionStop::SourceChanged.error());
                    }
                    Ok(sample.exported())
                })
                .collect::<Result<Vec<_>>>()
                .map_err(ReadError::from),
            DatabaseContext::Readable(context) => {
                readable_member::page(reader, context, &self.host, self.port, window, cursor, limit).await
            }
        }
    }
}

pub(super) struct PoolScope;
#[async_trait]
impl ResourceScope for PoolScope {
    type Context = resources::pool::Context;
    fn source(&self, _: &Self::Context) -> &'static str {
        resources::pool::SOURCE
    }
    fn target(&self, context: &Self::Context) -> CollectionTarget {
        CollectionTarget::Shared(context.identity.clone())
    }
    async fn identity<R: NativeRead>(&self, reader: &mut R) -> Result<Self::Context> {
        resources::pool::identity(reader).await?.0.pool.ok_or_else(|| CollectionStop::SourceChanged.error())
    }
    async fn inventory<R: NativeRead>(&self, reader: &mut R, window: &Window) -> Result<Inventory> {
        resources::pool::inventory(reader, window).await
    }
    async fn page<R: NativeRead>(
        &self,
        reader: &mut R,
        context: &Self::Context,
        window: &Window,
        cursor: Option<&str>,
        limit: u32,
    ) -> ReadResult<Vec<ResourceMeasurement>> {
        if self.identity(reader).await? != *context {
            return Err(CollectionStop::SourceChanged.error().into());
        }
        resources::pool::windowed(reader, context, window, cursor, limit).await
    }
}

pub(super) struct ResourceSource<R = Session, S: ResourceScope = DatabaseScope> {
    request: CollectionRequest,
    session: R,
    scope: S,
    context: Option<S::Context>,
    coverage_stop: Option<CollectionStop>,
    inventory: Option<Inventory>,
    pager: ResourcePager,
    progress: super::progress::PagedProgress,
}
impl ResourceSource {
    pub(super) fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        Self::with_io(Session::collection(pool, request.budget.clone())?, request, host, port)
    }
}
impl<R: NativeRead> ResourceSource<R> {
    fn with_io(session: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        Self::with_scope(session, request, DatabaseScope { host, port })
    }
}
impl<R: NativeRead, S: ResourceScope> ResourceSource<R, S> {
    fn with_scope(session: R, request: &CollectionRequest, scope: S) -> Result<Self> {
        request.validate()?;
        if request.collection() != TelemetryCollection::ResourceHistory {
            return Err(decode::malformed());
        }
        Ok(Self {
            request: request.clone(),
            session,
            scope,
            context: None,
            coverage_stop: None,
            inventory: None,
            pager: ResourcePager::default(),
            progress: super::progress::PagedProgress::new(request.collection()),
        })
    }
    fn window(&self) -> Result<Window> {
        let (from, until) = self.request.bounds()?;
        Window::new(from, until)
    }
}

pub(super) fn pool_source(pool: Arc<Pool>, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
    Ok(Box::new(ResourceSource::with_scope(Session::collection(pool, request.budget.clone())?, request, PoolScope)?))
}

pub(crate) async fn discover_resources(
    pool: Arc<Pool>,
    request: &ResourceDiscoveryRequest,
) -> Result<Vec<CollectionResource>> {
    request.budget.validate()?;
    if !matches!(request.kind, SharedResourceKind::Pool | SharedResourceKind::Instance) {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    let mut reader = Session::collection(pool, request.budget.clone())?;
    let result = discover_with_io(&mut reader, request).await;
    reader.finish(result).await
}

async fn discover_with_io<R: NativeRead>(
    reader: &mut R,
    request: &ResourceDiscoveryRequest,
) -> Result<Vec<CollectionResource>> {
    let resources = match request.kind {
        SharedResourceKind::Pool => {
            description::ensure_supported(reader).await?;
            let (membership, observed_at) = resources::pool::identity(reader).await?;
            let pool = membership.pool;
            pool.into_iter()
                .map(|pool| CollectionResource {
                    collections: super::recipes::collections_for(
                        &pool.identity,
                        crate::probe::ENGINE_EDITION_AZURE_SQL_DATABASE,
                    ),
                    identity: pool.identity,
                    observed_at: observed_at.clone(),
                })
                .collect::<Vec<_>>()
        }
        SharedResourceKind::Instance => {
            let (instance, observed_at) = crate::instance::read(reader).await?;
            let mut collections = super::recipes::collections_for(&instance.identity, instance.engine_edition());
            if !crate::deadlocks::system_health::supported(&instance) {
                collections.retain(|collection| *collection != TelemetryCollection::DeadlockHistory);
            }
            vec![CollectionResource { identity: instance.identity, observed_at, collections }]
        }
        _ => {
            return Err(culprit_core::Error::new(
                culprit_core::ErrorCode::Unsupported,
                culprit_core::t!("collection.unsupported"),
            ));
        }
    };
    request.validate_result(&resources)?;
    Ok(resources)
}

#[async_trait]
impl<R: NativeRead, S: ResourceScope + Sync> TelemetrySource for ResourceSource<R, S> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.progress.describing()?;
        description::ensure_supported(&mut self.session).await?;
        let context = self.scope.identity(&mut self.session).await?;
        let target = self.scope.target(&context);
        if !self.request.subject.accepts(&target) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let window = self.window()?;
        let inventory = self.scope.inventory(&mut self.session, &window).await?;
        self.scope.validate_inventory(&context, &inventory)?;
        let observation = self.scope.observation(&context, &inventory)?;
        let source = self.scope.source(&context);
        let description = CollectionDescription {
            partition: culprit_core::telemetry::SourcePartition::Target,
            target,
            source: source.into(),
            observation,
            continuity: SourceContinuity::Unverified,
        };
        if !description.matches_request(&self.request) {
            return Err(decode::malformed());
        }
        self.context = Some(context);
        self.coverage_stop = inventory.history.coverage_stop();
        self.inventory = Some(inventory);
        self.progress.described();
        Ok(description)
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.session.read_metrics()
    }
    fn query_count(&self) -> u32 {
        self.session.queries()
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        self.progress.reading(stream, cursor)?;
        self.pager.check_cursor(cursor)?;
        if let Some(stop) = self.coverage_stop {
            return self.progress.read(stream, Ok(CollectionRead::Stopped(stop)));
        }
        let rows_per_page =
            self.request.budget.page_rows.min((self.request.budget.page_bytes / ENCODED_ROW_ALLOWANCE).max(1));
        let window = self.window()?;
        let context = self.context.as_ref().ok_or_else(decode::malformed)?;
        let result = match self.scope.page(&mut self.session, context, &window, cursor, rows_per_page).await {
            Ok(rows) => self
                .scope
                .validate_page(context, self.inventory.as_ref().ok_or_else(decode::malformed)?, &rows)
                .and_then(|_| self.pager.accept(rows, rows_per_page)),
            Err(ReadError::Stopped(reason)) => Ok(CollectionRead::Stopped(reason)),
            Err(ReadError::Source(error)) => Err(error),
        };
        self.progress.read(stream, result)
    }
    async fn finish(&mut self) -> Result<SourceCompletion> {
        self.progress.verifying()?;
        let before = self.context.as_ref().ok_or_else(decode::malformed)?;
        let context = self.scope.recheck(&mut self.session, before).await?;
        if before != &context {
            self.progress.verified();
            return Ok(SourceCompletion::Changed);
        }
        let window = self.window()?;
        let current = self.scope.inventory(&mut self.session, &window).await?;
        self.scope.validate_inventory(&context, &current)?;
        self.progress.verified();
        Ok(SourceCompletion::verified_if(self.inventory.as_ref().is_some_and(|before| {
            self.scope.same_inventory(&context, before, &current)
                && self.pager.complete(before.history.expected_samples)
        })))
    }
    async fn close(&mut self) -> SourceShutdown {
        self.progress.close();
        self.session.close().await
    }
}

#[cfg(test)]
mod pool_tests;
#[cfg(test)]
mod tests;
