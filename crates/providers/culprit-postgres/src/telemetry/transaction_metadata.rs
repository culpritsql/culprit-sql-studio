//! An explicit source question pays one held bounded acquisition; ordinary Activity never calls this reader.
use super::*;
use crate::native::{
    activity as native_activity,
    local_context::{self, Context, SameConnection},
};
use crate::read::QueryRead;
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};
use std::collections::BTreeSet;

const MAX_QUERIES: u32 = 12;
const MAX_BYTES: u32 = 1024 * 1024;
const MAX_SQL_POOL: usize = 64 * 1024;
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 5000;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_queries = budget.max_queries.min(MAX_QUERIES);
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_TRANSACTION_METADATA_BACKENDS as u32);
    budget.page_rows = budget.page_rows.min(budget.max_snapshot_rows);
    budget.total_bytes = budget.total_bytes.min(MAX_BYTES);
    budget.page_bytes = budget.page_bytes.min(budget.total_bytes);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(TOTAL_TIMEOUT_MS);
    budget
}
pub(super) fn source(connector: Connector, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
    open(connector, request, None)
}
pub(super) fn profile_source(
    connector: Connector,
    request: &CollectionRequest,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    if bounded_budget(&request.budget) != request.budget {
        return Err(native::invalid());
    }
    open(connector, request, Some(culprit_read::SourceProfileGuard::new(request, admission)?))
}
fn open(
    connector: Connector,
    request: &CollectionRequest,
    profile: Option<culprit_read::SourceProfileGuard>,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    request.subject.require_database()?;
    let (focus, _) = question(request)?;
    participant(focus)?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    let mut io = ReadSession::new(connector.clone(), request.budget.clone());
    if let Some(guard) = &profile {
        io = io.with_profile(guard.clone());
    }
    let reader = Reader { host: connector.host, port: connector.port, io, verified: None, generation: None };
    let frozen = FrozenSource::new(reader, &request)?;
    Ok(match profile {
        Some(guard) => Box::new(culprit_read::GuardedTelemetrySource::new(frozen, guard)),
        None => Box::new(frozen),
    })
}
struct Reader<R = ReadSession> {
    io: R,
    host: String,
    port: u16,
    verified: Option<CollectionDescription>,
    generation: Option<u64>,
}
#[async_trait::async_trait]
impl<R: NativeRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let observation = Box::pin(capture(&mut self.io, &self.host, self.port, request)).await?;
        self.generation = self.io.connection_generation();
        self.verified = Some(observation.description.clone());
        Ok(observation)
    }
    async fn verify(&mut self, source: &CollectionDescription) -> Result<bool> {
        Ok(self.verified.as_ref() == Some(source)
            && self.generation.is_some()
            && self.io.connection_generation() == self.generation)
    }
    fn query_count(&self) -> u32 {
        self.io.queries()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.io.close().await
    }
}
fn question(request: &CollectionRequest) -> Result<(&SourceReadFocus, &SourceReadExecutionScope)> {
    let CollectionSelection::SourceRead { focus, execution } = &request.selection else {
        return Err(native::invalid());
    };
    if focus.question != SourceReadQuestion::TransactionMetadata
        || focus.target.engine() != culprit_core::Engine::Postgres
        || !execution.accepts_question(focus.question)
        || !execution.matches_target(&focus.target)
    {
        return Err(native::invalid());
    }
    Ok((focus, execution))
}
fn participant(focus: &SourceReadFocus) -> Result<(&ActivityKey, &NativeIdentity)> {
    let (key, statement_identity, _) = focus.transaction_metadata_participant().ok_or_else(native::invalid)?;
    let pid = key.session.id.parse::<u32>().map_err(|_| native::invalid())?;
    if pid == 0 || pid > i32::MAX as u32 || pid.to_string() != key.session.id {
        return Err(native::invalid());
    }
    Ok((key, statement_identity))
}
fn same_request(actor: &ActivityObservation, key: &ActivityKey, statement: &NativeIdentity) -> bool {
    actor.key == *key && actor.statement_identity.as_ref() == Some(statement)
}
async fn actor<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    pid: u32,
    budget: &CollectionBudget,
) -> Result<native_activity::ActivityFrame> {
    let mut frame = native_activity::observe_rows(
        reader,
        &native_activity::target_query(true, pid, "'client backend'", ""),
        &meta.database_oid,
        budget,
    )
    .await?;
    native_activity::transaction_metadata::classify(&mut frame);
    Ok(frame)
}
fn current(frame: &native_activity::ActivityFrame) -> Result<Option<&ActivityObservation>> {
    if frame.records.len() > 1 {
        return Err(native::invalid());
    }
    frame
        .records
        .first()
        .map(|row| row.facts.as_ref().ok_or_else(|| CollectionStop::SourceUnavailable.error()))
        .transpose()
}
async fn capture<R: NativeRead>(
    reader: &mut R,
    host: &str,
    port: u16,
    request: &CollectionRequest,
) -> Result<FrozenObservation> {
    let (focus, execution) = question(request)?;
    let (key, statement) = participant(focus)?;
    let pid = key.session.id.parse::<u32>().map_err(|_| native::invalid())?;
    let opening_generation = reader.connection_generation();
    let before = Box::new(Box::pin(metadata::activity(reader, host, port)).await?);
    if before.version < 160_000 {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    if !before.full_visibility {
        return Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
    }
    let generation = reader.connection_generation().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    if opening_generation.is_some_and(|original| original != generation) {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut held = SameConnection::new(reader, generation);
    let reader = &mut held;
    let owner = Box::new(
        Box::pin(Context::observe(reader, &before)).await?.ok_or_else(|| CollectionStop::SourceUnavailable.error())?,
    );
    {
        let initial = owner.read_context()?;
        if !execution.matches_context(&initial) || !execution.matches_target(&focus.target) {
            return Err(CollectionStop::SourceChanged.error());
        }
        if let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &focus.prerequisite
            && ObservationBounds::new(&continuation.request.original_observed_at, &initial.observed_from).is_none()
        {
            return Err(CollectionStop::SourceChanged.error());
        }
    }
    let first = Box::pin(actor(reader, &before, pid, &request.budget)).await?;
    let mut point = None;
    if current(&first)?.is_some_and(|actor| {
        same_request(actor, key, statement)
            && match &focus.prerequisite {
                SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } => {
                    actor.wait.as_ref().and_then(|wait| wait.class) == Some(continuation.mechanism.class())
                }
                _ => true,
            }
    }) {
        point = Box::pin(observe_point(reader, &before, request, key, statement, pid)).await?;
    }
    let completion = Box::pin(local_context::complete(reader, host, port, &before, Some(&owner))).await?;
    let context = completion.context;
    if !completion.full_visibility
        || !execution.matches_context(&context)
        || reader.connection_generation() != Some(generation)
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    let count = u32::from(point.is_some());
    let complete = point.as_ref().is_none_or(|point| point.observation_is_complete());
    let mut coverage = StateSourceCoverage::observed(POSTGRES_TRANSACTION_METADATA_SOURCE.into(), count, complete);
    if point.as_ref().is_some_and(|point| point.cache_limited || point.retainer_limited) {
        coverage.stopped = Some(CollectionStop::RowLimit);
    }
    let description = CollectionDescription {
        target: context.target.clone(),
        partition: SourcePartition::Target,
        source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::TransactionMetadata,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: count,
                sources: vec![coverage],
            },
            focus_key: focus.key()?,
            execution: Box::new(context),
        },
    };
    let records = point
        .take()
        .map(|point| {
            if !point.fits_source_scope(&description) {
                return Err(native::invalid());
            }
            Ok(TelemetryRecord::Operational(OperationalObservation {
                source: POSTGRES_TRANSACTION_METADATA_SOURCE.into(),
                key: point.key()?,
                fact: OperationalFact::PostgresTransactionMetadata(point),
            }))
        })
        .transpose()?
        .into_iter()
        .collect();
    if !description.matches_request(request) {
        return Err(native::invalid());
    }
    Ok(FrozenObservation { description, records })
}
async fn observe_point<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    request: &CollectionRequest,
    key: &ActivityKey,
    statement: &NativeIdentity,
    pid: u32,
) -> Result<Option<Box<PostgresTransactionMetadata>>> {
    let limit = (request.budget.max_snapshot_rows as usize).min(MAX_TRANSACTION_METADATA_BACKENDS);
    let mut reading = native_activity::transaction_metadata::read(reader, meta, pid, limit).await?;
    if !reading.caches.iter().any(|cache| same_request(&cache.actor, key, statement)) {
        return Ok(None);
    }
    let retainers = native::transaction_metadata::read_retainers(
        reader,
        meta.version,
        meta.standby,
        (request.budget.max_snapshot_rows as usize).min(MAX_TRANSACTION_METADATA_RETAINERS),
    )
    .await?;
    let keys = reading
        .frame
        .records
        .iter()
        .filter_map(|row| row.facts.as_ref())
        .map(|actor| actor.key.clone())
        .collect::<BTreeSet<_>>();
    let pool = (request.budget.total_bytes as usize / 8).min(request.budget.page_bytes as usize / 4).min(MAX_SQL_POOL);
    let per_statement = (pool / keys.len().max(1)).min(request.budget.max_sql_bytes as usize);
    native_activity::enrich(reader, &mut reading.frame, &keys, per_statement, false).await?;
    native_activity::transaction_metadata::synchronize(&reading.frame, &mut reading.caches)?;
    let end = actor(reader, meta, pid, &request.budget).await?;
    let request_end = match current(&end)? {
        None => BlockingRequestState::Missing,
        Some(actor) if same_request(actor, key, statement) => BlockingRequestState::SameStatement,
        Some(_) => BlockingRequestState::Changed,
    };
    let point = PostgresTransactionMetadata {
        observed_at: reading.frame.at,
        database_oid: meta.database_oid.clone(),
        database: meta.database.database.clone(),
        focus: key.clone(),
        request_end,
        caches: reading.caches,
        cache_rows: reading.native_rows,
        cache_limited: reading.limited,
        slrus: reading.slrus,
        retention: retainers.snapshot,
        retainer_rows: retainers.native_rows,
        retainer_limited: retainers.limited,
    };
    if !point.is_valid() {
        return Err(native::invalid());
    }
    Ok(Some(Box::new(point)))
}
#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "native-test-support"))]
mod native_tests;
