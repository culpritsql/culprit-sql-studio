//! Manual/source-question acquisition only. Official-driver cancellation qualification is not asserted.
use super::*;
use crate::activity::{ActivityReader, IdentityDetail, ReadOptions};
use crate::read::NativeRead;
use crate::read::ReadSession;
use culprit_core::telemetry::*;
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};
const MAX_QUERIES: u32 = 15; // local Context3+3, actor1+1, selected AG layout/row2+2, redo1+1, SET1
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 5000;
const MAX_TOTAL_BYTES: u32 = 256 * 1024;
const MAX_PAGE_BYTES: u32 = 64 * 1024;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut value = requested.clone();
    value.max_queries = value.max_queries.min(MAX_QUERIES);
    value.max_snapshot_rows = value.max_snapshot_rows.min(MAX_BLOCKING_PARTICIPANTS);
    value.page_rows = value.page_rows.min(value.max_snapshot_rows);
    value.statement_timeout_ms = value.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS);
    value.total_timeout_ms = value.total_timeout_ms.min(TOTAL_TIMEOUT_MS);
    value.total_bytes = value.total_bytes.min(MAX_TOTAL_BYTES);
    value.page_bytes = value.page_bytes.min(MAX_PAGE_BYTES).min(value.total_bytes);
    value
}
fn selection(request: &CollectionRequest) -> Result<(&SourceReadFocus, &SourceReadExecutionScope)> {
    request.validate()?;
    request.subject.require_database()?;
    let CollectionSelection::SourceRead { focus, execution } = &request.selection else {
        return Err(crate::read::invalid_data());
    };
    let original = focus.readable_secondary_request().ok_or_else(crate::read::invalid_data)?;
    crate::activity::readable_secondary_recovery::validate_focus(original)?;
    if !execution.accepts_question(focus.question)
        || execution.database_writable() != Some(false)
        || !execution.matches_target(&focus.target)
    {
        return Err(crate::read::invalid_data());
    }
    Ok((focus, execution))
}
pub(super) fn source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
) -> Result<Box<dyn TelemetrySource>> {
    selection(request)?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request.budget.validate()?;
    let io = ReadSession::collection(pool, request.budget.clone())?;
    with_io_and_profile(io, &request, host, port, None)
}
pub(super) fn profile_source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    selection(request)?;
    if bounded_budget(&request.budget) != request.budget {
        return Err(crate::read::invalid_data());
    }
    let profile = culprit_read::SourceProfileGuard::new(request, admission)?;
    let io = ReadSession::collection(pool, request.budget.clone())?.with_profile(profile.clone());
    with_io_and_profile(io, request, host, port, Some(profile))
}
#[cfg(test)]
fn with_io<R: NativeRead + 'static>(
    io: R,
    request: &CollectionRequest,
    host: String,
    port: u16,
) -> Result<Box<dyn TelemetrySource>> {
    with_io_and_profile(io, request, host, port, None)
}
fn with_io_and_profile<R: NativeRead + 'static>(
    io: R,
    request: &CollectionRequest,
    host: String,
    port: u16,
    profile: Option<culprit_read::SourceProfileGuard>,
) -> Result<Box<dyn TelemetrySource>> {
    selection(request)?;
    let activity = ActivityReader::new(
        io,
        ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint },
        host,
        port,
    );
    Ok(Box::new(FrozenSource::new(Reader { activity, verified: None, generation: None, profile }, request)?))
}
struct Reader<R: NativeRead> {
    activity: ActivityReader<R>,
    verified: Option<CollectionDescription>,
    generation: Option<u64>,
    profile: Option<culprit_read::SourceProfileGuard>,
}
#[async_trait::async_trait]
impl<R: NativeRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let (focus, execution) = selection(request)?;
        let point = Box::pin(self.activity.capture_readable_secondary_recovery_with_profile(
            focus,
            execution,
            self.profile.as_ref(),
        ))
        .await?;
        let (description, record) = point.into_source_read(focus)?;
        if !description.matches_request(request) {
            return Err(crate::read::invalid_data());
        }
        self.generation = self.activity.connection_generation();
        self.verified = Some(description.clone());
        Ok(FrozenObservation { description, records: vec![record] })
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        if let Some(profile) = &self.profile {
            profile.verify_final(
                description.observation.execution_context().ok_or_else(crate::read::invalid_data)?,
                self.activity.connection_generation(),
            )?;
        }
        Ok(self.verified.as_ref() == Some(description)
            && self.generation.is_some()
            && self.generation == self.activity.connection_generation())
    }
    fn query_count(&self) -> u32 {
        self.activity.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.activity.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.activity.close().await
    }
}
#[cfg(test)]
mod tests;
