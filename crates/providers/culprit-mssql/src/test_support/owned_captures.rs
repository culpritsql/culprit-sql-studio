//! Provider-only adapter over the one shared native-fixture canonical capture owner.
use crate::conn::Pool;
use culprit_collector::{analytical::test_support::native_capture, capture::SourceFactory};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use std::sync::Arc;
mod diagnostics;
pub(crate) use native_capture::Captured;

struct Factory {
    pool: Arc<Pool>,
    port: u16,
    capture: CaptureIdentity,
}
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        let source = crate::telemetry::source(self.pool.clone(), request, "127.0.0.1".into(), self.port)?;
        Ok(if matches!(request.selection, CollectionSelection::Snapshot { dataset: SnapshotDataset::RetainedPlans }) {
            diagnostics::source(source, self.capture.clone())
        } else {
            source
        })
    }
}

pub(crate) struct OwnedCaptures {
    retained: native_capture::OwnedCaptures,
    pool: Arc<Pool>,
    port: u16,
}
impl OwnedCaptures {
    pub fn from_pool(pool: Arc<Pool>, port: u16) -> Result<Self> {
        Ok(Self { retained: native_capture::OwnedCaptures::new()?, pool, port })
    }
    pub async fn capture(&mut self, selection: CollectionSelection) -> Result<Captured> {
        self.capture_with_cancellation(selection, &CollectionCancellation::default()).await
    }
    pub async fn capture_with_cancellation(
        &mut self,
        selection: CollectionSelection,
        cancellation: &CollectionCancellation,
    ) -> Result<Captured> {
        self.capture_on_subject(selection, CollectionSubject::Database, cancellation).await
    }
    /// Use the same provider factory and canonical owner for an explicitly selected native resource.
    pub async fn capture_on_subject(
        &mut self,
        selection: CollectionSelection,
        subject: CollectionSubject,
        cancellation: &CollectionCancellation,
    ) -> Result<Captured> {
        self.capture_owned(selection, subject, cancellation, None).await
    }
    pub async fn capture_with_factory(
        &mut self,
        selection: CollectionSelection,
        cancellation: &CollectionCancellation,
        factory: &dyn SourceFactory,
    ) -> Result<Captured> {
        self.capture_owned(selection, CollectionSubject::Database, cancellation, Some(factory)).await
    }
    pub async fn capture_with_subject(
        &mut self,
        selection: CollectionSelection,
        subject: CollectionSubject,
        cancellation: &CollectionCancellation,
        factory: &dyn SourceFactory,
    ) -> Result<Captured> {
        self.capture_owned(selection, subject, cancellation, Some(factory)).await
    }
    async fn capture_owned(
        &mut self,
        selection: CollectionSelection,
        subject: CollectionSubject,
        cancellation: &CollectionCancellation,
        factory: Option<&dyn SourceFactory>,
    ) -> Result<Captured> {
        Box::pin(async {
            let provisional = CollectionRequest { subject, selection, budget: CollectionBudget::default() };
            let recipe = crate::telemetry::recipes()
                .into_iter()
                .find(|recipe| recipe.accepts(&provisional))
                .ok_or_else(|| culprit_core::Error::invalid("native fixture recipe not declared"))?;
            let request = CollectionRequest { budget: provisional.budget.intersection(&recipe.budget), ..provisional };
            let pool = self.pool.clone();
            let port = self.port;
            self.retained
                .capture("owned-native-capture-fixture", request, cancellation, |capture| SelectedFactory {
                    selected: factory,
                    default: Factory { pool, port, capture: capture.clone() },
                })
                .await
        })
        .await
    }
    pub async fn verify_retained(&mut self, capture: &Captured) -> Result<()> {
        self.retained.verify_retained(capture).await
    }
}
struct SelectedFactory<'a> {
    selected: Option<&'a dyn SourceFactory>,
    default: Factory,
}
#[async_trait::async_trait]
impl SourceFactory for SelectedFactory<'_> {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        match self.selected {
            Some(factory) => factory.open(request).await,
            None => self.default.open(request).await,
        }
    }
}
