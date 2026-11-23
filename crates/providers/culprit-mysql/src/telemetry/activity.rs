//! A metadata baseline is verified and frozen before optional native investigations are scheduled.
use super::*;
use crate::read::QueryRead;
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};
#[cfg(test)]
pub(super) mod tests;
const MAX_BASELINE_ROWS: u32 = 500;
// PREPARE and EXECUTE each consume one native phase: seven owned setup phases, five metadata phases at
// each bound, one consumer read and two phases for the selected-database frame.
const MAX_BASELINE_QUERIES: u32 = 20;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_BASELINE_ROWS);
    budget.page_rows = budget.page_rows.min(MAX_BASELINE_ROWS);
    budget.max_queries = budget.max_queries.min(MAX_BASELINE_QUERIES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(1_000);
    budget.total_timeout_ms = budget.total_timeout_ms.min(5_000);
    budget
}
fn bounded_request(request: &CollectionRequest) -> CollectionRequest {
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request
}
struct ActivityReader<R = ReadSession> {
    io: R,
    connector: Arc<Connector>,
    verified: Option<CollectionDescription>,
    generation: Option<u64>,
}
pub(super) fn source(connector: Arc<Connector>, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
    open(connector, request, None)
}
pub(super) fn profile_source(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    profile: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    if bounded_budget(&request.budget) != request.budget {
        return Err(native::invalid());
    }
    open(connector, request, Some(culprit_read::SourceProfileGuard::new(request, profile)?))
}
fn open(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    profile: Option<culprit_read::SourceProfileGuard>,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    request.subject.require_database()?;
    if request.collection() != TelemetryCollection::Activity {
        return Err(native::invalid());
    }
    crate::probe::database_of(&connector)?;
    let request = bounded_request(request);
    let connector = profile.map_or_else(|| connector.clone(), |profile| Arc::new(connector.with_profile(profile)));
    Ok(Box::new(FrozenSource::new(
        ActivityReader {
            io: ReadSession::new(connector.clone(), request.budget.clone(), Scope::Server),
            connector,
            verified: None,
            generation: None,
        },
        &request,
    )?))
}
#[async_trait::async_trait]
impl<R: crate::read::OwnedRead> ObservationReader for ActivityReader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let capture = capture(&mut self.io, &self.connector, request).await?;
        self.generation = self.io.connection_generation();
        self.verified = Some(capture.description.clone());
        Ok(capture)
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        if let Some(profile) = self.connector.profile() {
            profile.verify_final(&NativeReadContext::from_activity(description)?, self.io.connection_generation())?;
        }
        Ok(self.verified.as_ref() == Some(description)
            && self.generation.is_some()
            && self.generation == self.io.connection_generation())
    }
    fn query_count(&self) -> u32 {
        self.io.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.io.close().await
    }
}

async fn capture<R: crate::read::QueryRead>(
    reader: &mut R,
    connector: &Connector,
    request: &CollectionRequest,
) -> Result<FrozenObservation> {
    connector.check_profile()?;
    let opening = reader.connection_generation();
    let before = metadata::activity(reader, connector).await?;
    let generation =
        reader.connection_generation().filter(|id| *id > 0).ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    if opening.is_some_and(|old| old != generation) {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut held = crate::read::SameConnection::new(reader, generation)?;
    let reader = &mut held;
    if let Some(profile) = connector.profile() {
        if let Some(policy) = profile.builtin_policy() {
            crate::builtin_safety::verify_metadata(policy, &before)?;
        }
        if !before.process_wide || !before.connection.performance_schema {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        profile.bind_native(&before.activity_context(&before)?, reader.connection_generation())?;
    }
    let frame = native::activity::observe(reader, &before, &request.budget).await?;
    let after = metadata::activity(reader, connector).await?;
    let context = before.activity_context(&after)?;
    if let Some(profile) = connector.profile() {
        profile.verify_final(&context, reader.connection_generation())?;
    }
    let mut coverage = frame.coverage;
    if !after.process_wide {
        coverage.state = culprit_core::SourceState::Partial;
        coverage.complete_scope = false;
        coverage.error_code = Some(ErrorCode::PermissionDenied);
        coverage.stopped.get_or_insert(CollectionStop::SourceUnavailable);
    }
    let epoch = context.partition.native_epoch().ok_or_else(native::invalid)?.to_owned();
    if !context.partition.is_qualified() {
        coverage.state = culprit_core::SourceState::Partial;
        coverage.complete_scope = false;
        coverage.stopped.get_or_insert(CollectionStop::SourceUnavailable);
    }
    let description = CollectionDescription {
        partition: context.partition,
        target: context.target,
        source: native::activity::SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            database_writable: context.database_writable,
            epoch: Some(epoch),
            observed_from: context.observed_from,
            observed_until: context.observed_until,
            at: frame.at,
            scope: MeasurementScope::Database,
            population: ActivityPopulation::DefaultDatabaseConnections,
            native_rows: frame.native_rows,
            observations: frame.observations.len() as u32,
            coverage,
        }),
    };
    if !description.observation.activity().is_some_and(ActivitySnapshotDescription::is_valid) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(FrozenObservation { description, records: frame.observations.into_iter().map(TelemetryRecord::from).collect() })
}
