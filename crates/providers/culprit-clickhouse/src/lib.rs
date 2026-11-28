//! Read-only ClickHouse HTTP source. Warehouse delivery belongs to the engine's selected store.
mod catalog;
mod client;
mod collect;
mod execution;
mod health;
mod identity;
mod live;
mod queries;
mod safety;
pub use safety::{telemetry_profile_selectors, verify_builtin_safety};
pub mod sources;
mod telemetry;
mod timeline;
mod workload;
use async_trait::async_trait;
use culprit_core::{
    ConnectionSpec, Engine, Environment, ExplainMode, LiveState, Plan, Provider, ProviderReadContext, Result,
    ResultSet, ServerInfo, Snapshot, SnapshotOptions, telemetry::*,
};
pub use telemetry::telemetry_recipes;

pub fn component_version() -> culprit_core::ComponentVersion {
    culprit_core::ComponentVersion::package(
        "culprit-clickhouse",
        env!("CARGO_PKG_VERSION"),
        option_env!("CULPRIT_BUILD_REVISION").or(option_env!("GITHUB_SHA")),
    )
}

pub struct ClickHouseProvider {
    client: client::Client,
    host: String,
    port: u16,
    environment: Environment,
}
impl ClickHouseProvider {
    pub fn new(spec: &ConnectionSpec, context: ProviderReadContext) -> Result<Self> {
        Ok(Self {
            client: client::Client::new(spec, context)?,
            host: spec.host.clone(),
            port: Engine::Clickhouse.network_port(spec.port)?,
            environment: spec.environment,
        })
    }
    fn require_settled(&self) -> Result<()> {
        match self.client.shutdown() {
            SourceShutdown::Quiescent => Ok(()),
            SourceShutdown::Unconfirmed => Err(CollectionStop::CancellationUnconfirmed.error()),
        }
    }
}
#[async_trait]
impl Provider for ClickHouseProvider {
    fn engine(&self) -> Engine {
        Engine::Clickhouse
    }
    fn collector_version(&self) -> culprit_core::ComponentVersion {
        component_version()
    }
    fn telemetry_source_for_safety(
        &self,
        request: &CollectionRequest,
        profile: &SourceProfileAdmission,
    ) -> Result<Box<dyn TelemetrySource>> {
        if !profile.matches_request(request)
            || profile.scope().is_some()
            || !telemetry_recipes()
                .iter()
                .any(|recipe| recipe.purpose == RecipePurpose::Baseline && profile.matches_recipe(recipe))
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        telemetry::source(self, request, Some(profile))
    }
    fn source_shutdown(&self) -> SourceShutdown {
        self.client.shutdown()
    }
    fn telemetry_source(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        telemetry::source(self, request, None)
    }
    async fn probe(&self) -> Result<ServerInfo> {
        Ok(identity::Identity::read(&self.client).await?.server())
    }
    async fn snapshot(&self, connection: &str, options: &SnapshotOptions) -> Result<Snapshot> {
        collect::snapshot(self, connection, options).await
    }
    async fn period_stats(
        &self,
        baseline: &culprit_core::changes::Period,
        recent: &culprit_core::changes::Period,
        population: Option<&NativeIdentity>,
    ) -> Result<Option<culprit_core::changes::PeriodData>> {
        if population.is_some() {
            return Err(culprit_core::Error::new(
                culprit_core::ErrorCode::Unsupported,
                culprit_core::t!("model.workload.population_unavailable"),
            ));
        }
        workload::periods(&self.client, baseline, recent).await.map(Some)
    }
    async fn live(&self) -> Result<LiveState> {
        live::read(&self.client).await
    }
    async fn explain(&self, sql: &str, mode: ExplainMode) -> Result<Plan> {
        execution::explain(&self.client, sql, mode).await
    }
    async fn run(&self, sql: &str, limit: usize) -> Result<ResultSet> {
        execution::run(&self.client, sql, limit).await
    }
}

#[cfg(test)]
mod tests;
