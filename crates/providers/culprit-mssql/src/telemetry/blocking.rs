//! Frozen paging reuses the Activity reader, selected-frame owner and canonical blocking interpretation.
use crate::{
    activity::{ActivityReader, IdentityDetail, ReadOptions},
    conn::Pool,
    read::NativeRead,
};
use culprit_core::{Result, telemetry::*};
use std::sync::Arc;

// One SET + two three-query boxed authority bounds + one Activity frame + at most one batched current
// SQL enrichment + one exact original participant check. Only a selected schema-lock victim adds two
// bounded native endpoint snapshots on this same held reader; ordinary blocking still uses at most ten.
const MAX_BLOCKING_QUERIES: u32 = 12;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = super::activity::bounded_budget(requested);
    budget.max_queries = requested.max_queries.min(MAX_BLOCKING_QUERIES);
    budget
}
pub(super) fn source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    request.subject.require_database()?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    let options = ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint };
    let reader = ActivityReader::open_collection(pool, options, host, port)?;
    Ok(Box::new(from_reader(reader, &request)?))
}
fn from_reader<R: NativeRead>(
    reader: ActivityReader<R>,
    request: &CollectionRequest,
) -> Result<culprit_read::FrozenSource<BlockingReader<R>>> {
    request.validate()?;
    request.subject.require_database()?;
    if !matches!(request.selection, CollectionSelection::FocusedBlocking { .. }) {
        return Err(CollectionStop::InvalidPage.error());
    }
    culprit_read::FrozenSource::new(BlockingReader { reader, verified: None }, request)
}
struct BlockingReader<R: NativeRead> {
    reader: ActivityReader<R>,
    verified: Option<CollectionDescription>,
}
#[async_trait::async_trait]
impl<R: NativeRead> culprit_read::ObservationReader for BlockingReader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<culprit_read::FrozenObservation> {
        let CollectionSelection::FocusedBlocking { focus } = &request.selection else {
            return Err(CollectionStop::InvalidPage.error());
        };
        let observation = self.reader.capture_focused_blocking(focus).await?;
        if !observation.description.matches_request(request) {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.verified = Some(observation.description.clone());
        Ok(observation)
    }
    async fn verify(&mut self, source: &CollectionDescription) -> Result<bool> {
        // The existing PendingActivityFrame completed the mandatory held native final recheck.
        Ok(self.verified.as_ref() == Some(source))
    }
    fn query_count(&self) -> u32 {
        self.reader.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.reader.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.reader.close().await
    }
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
