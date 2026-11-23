//! One selected lock point shares Activity/native lock decoders and the held read owner.
use super::*;
use crate::read::{OwnedRead, QueryRead, SameConnection};
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader, SourceProfileGuard};
use mysql_async::Value;
mod edges;
mod metadata_context;
mod participants;

const SOURCE: &str = "mysql-blocking";
// Seven owned setup phases, both metadata5 bounds, consumers+MDL2, exact root2,
// paired data/metadata edges8 and paired selected participants4. PREPARE is always charged.
const MAX_QUERIES: u32 = 33;
// Explicit deep ticket endpoints each pay PREPARE + execute. The builtin path remains 33.
const METADATA_MAX_QUERIES: u32 = 37;
#[derive(Clone, Copy)]
enum Detail {
    Blocking,
    MetadataLocks,
}
const MAX_BYTES: u32 = 1024 * 1024;
const MAX_SQL_BYTES: u32 = 64 * 1024;
const MAX_DEPTH: u32 = 1;
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 5000;

pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    bounded(requested, MAX_QUERIES)
}
pub(super) fn metadata_budget(requested: &CollectionBudget) -> CollectionBudget {
    bounded(requested, METADATA_MAX_QUERIES)
}
fn bounded(requested: &CollectionBudget, queries: u32) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_queries = budget.max_queries.min(queries);
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_BLOCKING_PARTICIPANTS);
    budget.page_rows = budget.page_rows.min(MAX_BLOCKING_PARTICIPANTS);
    budget.total_bytes = budget.total_bytes.min(MAX_BYTES);
    budget.page_bytes = budget.page_bytes.min(budget.total_bytes);
    budget.max_sql_bytes = budget.max_sql_bytes.min(MAX_SQL_BYTES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(TOTAL_TIMEOUT_MS);
    budget
}
fn focus(request: &CollectionRequest) -> Result<&ActivityRequestFocus> {
    request.validate()?;
    request.subject.require_database()?;
    let focus = match &request.selection {
        CollectionSelection::FocusedBlocking { focus } => focus.as_ref(),
        CollectionSelection::SourceRead { focus, .. } => focus.metadata_lock_request().ok_or_else(native::invalid)?,
        _ => return Err(native::invalid()),
    };
    if focus.target.engine() != culprit_core::Engine::Mysql
        || focus.source != native::activity::SOURCE
        || !focus.is_blocking_focus()
    {
        return Err(native::invalid());
    }
    participants::session_id(&focus.original.key.session.id)?;
    native_thread(focus)?;
    let request = focus.original.key.request.as_ref().ok_or_else(native::invalid)?;
    let canonical = |value: &str| value.parse::<u64>().is_ok_and(|number| number.to_string() == value);
    if !canonical(&request.id)
        || !canonical(&request.generation)
        || culprit_core::engines::mysql_activity_statement_identity(&focus.original.key.session, request).as_ref()
            != Some(&focus.statement_identity)
    {
        return Err(native::invalid());
    }
    Ok(focus)
}
fn native_thread(focus: &ActivityRequestFocus) -> Result<u64> {
    focus
        .original
        .key
        .session
        .generation
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0 && id.to_string() == focus.original.key.session.generation)
        .ok_or_else(native::invalid)
}
pub(super) fn source(connector: Arc<Connector>, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
    open(connector, request, None, Detail::Blocking)
}
pub(super) fn profile_source(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    if bounded_budget(&request.budget) != request.budget {
        return Err(native::invalid());
    }
    open(connector, request, Some(SourceProfileGuard::new(request, admission)?), Detail::Blocking)
}
pub(super) fn metadata_source(
    connector: Arc<Connector>,
    request: &CollectionRequest,
) -> Result<Box<dyn TelemetrySource>> {
    if !matches!(&request.selection,CollectionSelection::SourceRead {focus,..} if focus.question==SourceReadQuestion::MetadataLockContext)
    {
        return Err(native::invalid());
    }
    open(connector, request, None, Detail::MetadataLocks)
}
pub(super) fn metadata_profile_source(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    if metadata_budget(&request.budget) != request.budget
        || admission.selector() != RecipeSelector::FocusedMetadataLocks
    {
        return Err(native::invalid());
    }
    open(connector, request, Some(SourceProfileGuard::new(request, admission)?), Detail::MetadataLocks)
}
fn open(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    profile: Option<SourceProfileGuard>,
    detail: Detail,
) -> Result<Box<dyn TelemetrySource>> {
    focus(request)?;
    crate::probe::database_of(&connector)?;
    let mut request = request.clone();
    request.budget = match detail {
        Detail::Blocking => bounded_budget(&request.budget),
        Detail::MetadataLocks => metadata_budget(&request.budget),
    };
    let connector = profile.map_or_else(|| connector.clone(), |profile| Arc::new(connector.with_profile(profile)));
    let reader = Reader {
        io: ReadSession::new(connector.clone(), request.budget.clone(), Scope::Server),
        connector,
        verified: None,
        generation: None,
        detail,
    };
    Ok(Box::new(FrozenSource::new(reader, &request)?))
}
struct Reader<R = ReadSession> {
    io: R,
    connector: Arc<Connector>,
    verified: Option<CollectionDescription>,
    generation: Option<u64>,
    detail: Detail,
}
#[async_trait::async_trait]
impl<R: OwnedRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let observation = Box::pin(capture(&mut self.io, &self.connector, request, self.detail)).await?;
        self.generation = self.io.connection_generation();
        self.verified = Some(observation.description.clone());
        Ok(observation)
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
struct Observation {
    frame: participants::Frame,
    binding: FocusedRequestBinding,
    membership: FocusedRequestMembership,
    request_end: BlockingRequestState,
    status: BlockingClosureStatus,
}
async fn capture<R: QueryRead>(
    reader: &mut R,
    connector: &Connector,
    request: &CollectionRequest,
    detail: Detail,
) -> Result<FrozenObservation> {
    let focus = focus(request)?;
    connector.check_profile()?;
    let opening = reader.connection_generation();
    let before = Box::new(Box::pin(metadata::activity(reader, connector)).await?);
    if matches!(detail, Detail::MetadataLocks)
        && !crate::builtin_safety::supported(before.connection.flavor, &before.connection.version)
    {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    if !before.connection.performance_schema || !before.process_wide {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    if let Some(policy) = connector.profile().and_then(SourceProfileGuard::builtin_policy) {
        crate::builtin_safety::verify_metadata(policy, &before)?;
    }
    let generation =
        reader.connection_generation().filter(|id| *id > 0).ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    if opening.is_some_and(|old| old != generation) {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut held = SameConnection::new(reader, generation)?;
    let initial = before.activity_context(&before)?;
    if !focused_request_original_scope_matches(focus, &initial) {
        return Err(CollectionStop::SourceChanged.error());
    }
    if let Some(profile) = connector.profile() {
        profile.bind_native(&initial, held.connection_generation())?;
    }
    if !native::activity::instrumentation_enabled(&mut held).await?
        || !native::locks::selected::instrumented(&mut held).await?
    {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    if let CollectionSelection::SourceRead { execution, .. } = &request.selection
        && (!execution.accepts_question(SourceReadQuestion::MetadataLockContext)
            || !execution.matches_context(&initial))
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    let observed = match detail {
        Detail::Blocking => Box::pin(observe(&mut held, &before, focus, &request.budget)).await?,
        Detail::MetadataLocks => {
            Box::pin(metadata_context::observe(&mut held, &before, focus, &request.budget)).await?
        }
    };
    let after = Box::pin(metadata::activity(&mut held, connector)).await?;
    let context = before.activity_context(&after)?;
    if !after.process_wide
        || !focused_request_original_scope_matches(focus, &context)
        || ObservationBounds::new(&observed.frame.at, &context.observed_until).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    if let Some(profile) = connector.profile() {
        profile.verify_final(&context, held.connection_generation())?;
    }
    if let CollectionSelection::SourceRead { execution, .. } = &request.selection
        && !execution.matches_context(&context)
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    freeze(request, context, observed, detail)
}
async fn observe<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    focus: &ActivityRequestFocus,
    budget: &CollectionBudget,
) -> Result<Observation> {
    let sql_bytes = (budget.max_sql_bytes as usize).min(budget.total_bytes as usize / 8);
    let root = participants::root(reader, meta, focus, true, sql_bytes).await?;
    let state = participants::request_state(&root, focus);
    if state != BlockingRequestState::SameStatement {
        return Ok(Observation {
            frame: participants::Frame { rows: vec![], at: root.at, limited: root.limited },
            binding: if state == BlockingRequestState::Missing {
                FocusedRequestBinding::ParticipantMissing
            } else {
                FocusedRequestBinding::ParticipantChanged
            },
            membership: FocusedRequestMembership::NotObserved,
            request_end: BlockingRequestState::NotRead,
            status: BlockingClosureStatus::NotRead,
        });
    }
    let before = edges::read(reader, meta, focus, budget.max_snapshot_rows).await?;
    let selection = before.participants(focus, budget.max_snapshot_rows)?;
    let first = participants::selected(reader, meta, &selection.ids, true, selection.ids.len()).await?;
    let after = edges::read(reader, meta, focus, budget.max_snapshot_rows).await?;
    let end = participants::selected(reader, meta, &selection.ids, true, selection.ids.len()).await?;
    ordered(&root.at, &first.at)?;
    ordered(&first.at, &end.at)?;
    let request_end = participants::request_state(&end, focus);
    let (frame, status) =
        edges::correlate(root, first, &end, before, after, selection.limited, focus, budget.max_snapshot_rows)?;
    Ok(Observation {
        frame,
        status,
        request_end,
        binding: FocusedRequestBinding::Matched,
        membership: FocusedRequestMembership::Observed,
    })
}
fn ordered(before: &str, after: &str) -> Result<()> {
    ObservationBounds::new(before, after).map(|_| ()).ok_or_else(|| CollectionStop::SourceChanged.error())
}
fn freeze(
    request: &CollectionRequest,
    context: NativeReadContext,
    observed: Observation,
    detail: Detail,
) -> Result<FrozenObservation> {
    let focus = focus(request)?;
    let count = u32::try_from(observed.frame.rows.len()).map_err(|_| native::invalid())?;
    let source = match detail {
        Detail::Blocking => SOURCE,
        Detail::MetadataLocks => metadata_context::source(),
    };
    let mut coverage = StateSourceCoverage::observed(source.into(), count, false);
    let limited = match detail {
        Detail::Blocking => observed.status.is_limited(),
        Detail::MetadataLocks => metadata_context::limited(&observed.frame),
    };
    if limited {
        coverage.stopped = Some(CollectionStop::RowLimit);
    }
    let snapshot = ActivitySnapshotDescription {
        epoch: Some(focus.epoch.clone()),
        database_writable: context.database_writable,
        observed_from: context.observed_from.clone(),
        observed_until: context.observed_until.clone(),
        at: observed.frame.at.clone(),
        scope: MeasurementScope::Database,
        population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
        native_rows: count,
        observations: count,
        coverage,
    };
    let result = ActivityBlockingFocusResult {
        focus: focus.clone(),
        binding: observed.binding,
        membership: observed.membership,
        request_end: observed.request_end,
        closure: BlockingClosureDescription {
            status: observed.status,
            max_lookups: MAX_BLOCKING_LOOKUPS,
            max_depth: MAX_DEPTH,
            max_participants: request.budget.max_snapshot_rows,
        },
    };
    let observation = match detail {
        Detail::Blocking => SourceObservation::FocusedBlocking { snapshot, result: Box::new(result) },
        Detail::MetadataLocks => {
            metadata_context::observation(request, &context, snapshot, result, &observed.frame.rows)?
        }
    };
    let description = CollectionDescription {
        target: context.target,
        partition: context.partition,
        source: source.into(),
        continuity: SourceContinuity::Unverified,
        observation,
    };
    if !description.matches_request(request) {
        return Err(CollectionStop::SourceChanged.error());
    }
    let rows = observed
        .frame
        .rows
        .iter()
        .map(|row| ActivityFrameRow { observation: row, reference: None })
        .collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&description, &rows, true)?;
    let accepted = match &description.observation {
        SourceObservation::FocusedBlocking { result, .. } => result.accepts_frame(&frame),
        SourceObservation::MetadataLockContext { result, .. } => result.accepts_frame(&frame),
        _ => false,
    };
    if !accepted {
        return Err(native::invalid());
    }
    Ok(FrozenObservation { description, records: observed.frame.rows.into_iter().map(TelemetryRecord::from).collect() })
}

#[cfg(test)]
pub(super) mod tests;
