//! One selected readonly request uses the existing held connection, recovery decoder and native bounds.
use super::*;
use crate::native::{
    activity as native_activity,
    local_context::{self, Context, SameConnection},
};
use crate::read::QueryRead;
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader, SourceProfileGuard};
const MAX_QUERIES: u32 = 9; // metadata/member2 + actor1 + endpoint2 + actor1 + final metadata/member2 + SET1
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 5000;
const MAX_TOTAL_BYTES: u32 = 256 * 1024;
const MAX_PAGE_BYTES: u32 = 64 * 1024;
const MAX_RECORDS: u32 = 1;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut value = requested.clone();
    value.max_queries = value.max_queries.min(MAX_QUERIES);
    value.max_snapshot_rows = value.max_snapshot_rows.min(MAX_RECORDS);
    value.page_rows = value.page_rows.min(MAX_RECORDS);
    value.statement_timeout_ms = value.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS);
    value.total_timeout_ms = value.total_timeout_ms.min(TOTAL_TIMEOUT_MS);
    value.total_bytes = value.total_bytes.min(MAX_TOTAL_BYTES);
    value.page_bytes = value.page_bytes.min(MAX_PAGE_BYTES).min(value.total_bytes);
    value
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
    open(connector, request, Some(SourceProfileGuard::new(request, admission)?))
}
fn selection(request: &CollectionRequest) -> Result<(&SourceReadFocus, &SourceReadExecutionScope)> {
    request.validate()?;
    request.subject.require_database()?;
    let CollectionSelection::SourceRead { focus, execution } = &request.selection else {
        return Err(native::invalid());
    };
    let original = focus.readable_secondary_request().ok_or_else(native::invalid)?;
    if original.target.engine() != culprit_core::Engine::Postgres
        || !execution.accepts_question(focus.question)
        || execution.database_writable() != Some(false)
        || !execution.matches_target(&focus.target)
    {
        return Err(native::invalid());
    }
    original
        .original
        .key
        .session
        .id
        .parse::<i32>()
        .ok()
        .filter(|id| *id > 0 && id.to_string() == original.original.key.session.id)
        .ok_or_else(native::invalid)?;
    Ok((focus, execution))
}
fn open(
    connector: Connector,
    request: &CollectionRequest,
    profile: Option<SourceProfileGuard>,
) -> Result<Box<dyn TelemetrySource>> {
    selection(request)?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request.budget.validate()?;
    let mut io = ReadSession::new(connector.clone(), request.budget.clone());
    if let Some(profile) = &profile {
        io = io.with_profile(profile.clone());
    }
    Ok(Box::new(FrozenSource::new(
        Reader { io, host: connector.host.clone(), port: connector.port, profile, verified: None, generation: None },
        &request,
    )?))
}
struct Reader<R = ReadSession> {
    io: R,
    host: String,
    port: u16,
    profile: Option<SourceProfileGuard>,
    verified: Option<CollectionDescription>,
    generation: Option<u64>,
}
#[async_trait::async_trait]
impl<R: NativeRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let observation =
            Box::pin(capture(&mut self.io, &self.host, self.port, request, self.profile.as_ref())).await?;
        self.generation = self.io.connection_generation();
        self.verified = Some(observation.description.clone());
        Ok(observation)
    }
    async fn verify(&mut self, source: &CollectionDescription) -> Result<bool> {
        if let Some(profile) = &self.profile {
            profile.verify_final(
                source.observation.execution_context().ok_or_else(native::invalid)?,
                self.io.connection_generation(),
            )?;
        }
        Ok(self.verified.as_ref() == Some(source)
            && self.generation.is_some()
            && self.generation == self.io.connection_generation())
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
async fn actor<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    original: &ActivityRequestFocus,
    budget: &CollectionBudget,
) -> Result<native_activity::ActivityFrame> {
    let pid = original.original.key.session.id.parse::<u32>().map_err(|_| native::invalid())?;
    native_activity::observe_rows(
        reader,
        &native_activity::target_query(true, pid, "'client backend'", ""),
        &meta.database_oid,
        budget,
    )
    .await
}
fn request_state(
    frame: &native_activity::ActivityFrame,
    original: &ActivityRequestFocus,
) -> Result<BlockingRequestState> {
    match frame.records.as_slice() {
        [] => Ok(BlockingRequestState::Missing),
        [record] => {
            let row = record.facts.as_ref().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
            Ok(
                if row.key == original.original.key
                    && row.statement_identity.as_ref() == Some(&original.statement_identity)
                {
                    BlockingRequestState::SameStatement
                } else {
                    BlockingRequestState::Changed
                },
            )
        }
        _ => Err(native::invalid()),
    }
}
async fn capture<R: NativeRead>(
    reader: &mut R,
    host: &str,
    port: u16,
    request: &CollectionRequest,
    profile: Option<&SourceProfileGuard>,
) -> Result<FrozenObservation> {
    let (focus, execution) = selection(request)?;
    let original = focus.readable_secondary_request().ok_or_else(native::invalid)?;
    if let Some(profile) = profile {
        profile.ensure_active()?;
    }
    let opening = reader.connection_generation();
    let meta = Box::new(Box::pin(metadata::activity(reader, host, port)).await?);
    if meta.version / 10000 != 18 || !meta.standby {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    if !meta.full_visibility {
        return Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
    }
    if let Some(policy) = profile.and_then(SourceProfileGuard::builtin_policy) {
        crate::builtin_safety::verify_native(policy, meta.version)?;
    }
    let generation =
        reader.connection_generation().filter(|id| *id > 0).ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    if opening.is_some_and(|id| id != generation) {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut held = SameConnection::new(reader, generation);
    let reader = &mut held;
    let owner = Box::new(
        Box::pin(Context::observe(reader, &meta)).await?.ok_or_else(|| CollectionStop::SourceUnavailable.error())?,
    );
    let initial = owner.read_context()?;
    if !execution.matches_context(&initial) || !focused_request_original_scope_matches(original, &initial) {
        return Err(CollectionStop::SourceChanged.error());
    }
    if let Some(profile) = profile {
        profile.bind_native(&initial, reader.connection_generation())?;
    }
    let first = Box::pin(actor(reader, &meta, original, &request.budget)).await?;
    let state = request_state(&first, original)?;
    let (before, after, end) = if state == BlockingRequestState::SameStatement {
        let before = Box::pin(native::readable_secondary_recovery::read(reader, &meta, original)).await?;
        let after = Box::pin(native::readable_secondary_recovery::read(reader, &meta, original)).await?;
        let end = Box::pin(actor(reader, &meta, original, &request.budget)).await?;
        (Some(before), Some(after), Some(end))
    } else {
        (None, None, None)
    };
    let completion = Box::pin(local_context::complete(reader, host, port, &meta, Some(&owner))).await?;
    if !completion.full_visibility
        || !execution.matches_context(&completion.context)
        || reader.connection_generation() != Some(generation)
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    if let Some(profile) = profile {
        profile.verify_final(&completion.context, reader.connection_generation())?;
    }
    let (binding, membership) = match state {
        BlockingRequestState::SameStatement => (FocusedRequestBinding::Matched, FocusedRequestMembership::Observed),
        BlockingRequestState::Missing => {
            (FocusedRequestBinding::ParticipantMissing, FocusedRequestMembership::NotObserved)
        }
        _ => (FocusedRequestBinding::ParticipantChanged, FocusedRequestMembership::NotObserved),
    };
    let point = ReadableSecondaryRecoveryPoint {
        context: completion.context,
        focus: Box::new(original.clone()),
        observed_at: canonical_time(&first.at)?,
        checked_until: canonical_time(end.as_ref().map_or(first.at.as_str(), |frame| frame.at.as_str()))?,
        binding,
        membership,
        request_end: end
            .as_ref()
            .map(|frame| request_state(frame, original))
            .transpose()?
            .unwrap_or(BlockingRequestState::NotRead),
        native: ReadableSecondaryRecoveryNative::Postgres { before, after },
    };
    let (description, record) = point.into_source_read(focus)?;
    if !description.matches_request(request) {
        return Err(native::invalid());
    }
    Ok(FrozenObservation { description, records: vec![record] })
}
#[cfg(test)]
mod tests;
