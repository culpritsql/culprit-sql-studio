//! A bounded retained-event observation, using the same native reader/parser as interactive deadlocks.
use super::description;
use crate::{
    conn::Pool,
    deadlocks::{native, system_health},
    read::{NativeRead, ReadError, ReadSession, invalid_data, text},
};
use async_trait::async_trait;
use culprit_core::{Result, telemetry::*};
use std::sync::Arc;
const ENVELOPE_ALLOWANCE: usize = 4096;
const SHARED_EVENT_ROWS: u32 = 50;
const SHARED_READER_QUERIES: u32 = 8;
const SHARED_QUERIES: u32 = SHARED_READER_QUERIES + crate::read::ReadSession::SESSION_SETUP_QUERIES;
const DATABASE_READER_QUERIES: u32 = 12;
const DATABASE_QUERIES: u32 = DATABASE_READER_QUERIES + crate::read::ReadSession::SESSION_SETUP_QUERIES;
const SHARED_TOTAL_BYTES: u32 = 16 * 1024 * 1024;
const SHARED_STATEMENT_TIMEOUT_MS: u32 = 1_000;
const SHARED_TOTAL_TIMEOUT_MS: u32 = 15_000;

pub(super) struct EventSource<R = ReadSession> {
    request: CollectionRequest,
    host: String,
    port: u16,
    io: R,
    profile: Option<culprit_read::SourceProfileGuard>,
    builtin_policy: Option<BuiltinRecipeSafety>,
    initial_at: Option<String>,
    description: Option<CollectionDescription>,
    owner: Option<EventOwner>,
    positions: Option<Vec<String>>,
    records: Vec<TelemetryRecord>,
    offset: usize,
    progress: super::progress::PagedProgress,
}
enum EventOwner {
    Database { physical: String, session: Option<crate::deadlocks::ConfiguredDatabaseSession> },
    Instance { instance: Box<crate::instance::Context>, target: system_health::ConfiguredTarget },
}

pub(super) fn shared_budget(requested: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        page_rows: requested.page_rows.min(SHARED_EVENT_ROWS),
        max_snapshot_rows: requested.max_snapshot_rows.min(SHARED_EVENT_ROWS),
        max_pages: requested.max_pages.min(SHARED_EVENT_ROWS),
        total_bytes: requested.total_bytes.min(SHARED_TOTAL_BYTES),
        max_queries: requested.max_queries.min(SHARED_QUERIES),
        statement_timeout_ms: requested.statement_timeout_ms.min(SHARED_STATEMENT_TIMEOUT_MS),
        total_timeout_ms: requested.total_timeout_ms.min(SHARED_TOTAL_TIMEOUT_MS),
        ..requested.clone()
    }
}

pub(super) fn database_budget(requested: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        page_rows: requested.page_rows.min(SHARED_EVENT_ROWS),
        max_snapshot_rows: requested.max_snapshot_rows.min(SHARED_EVENT_ROWS),
        max_pages: requested.max_pages.min(SHARED_EVENT_ROWS),
        total_bytes: requested.total_bytes.min(SHARED_TOTAL_BYTES),
        max_queries: requested.max_queries.min(DATABASE_QUERIES),
        statement_timeout_ms: requested.statement_timeout_ms.min(SHARED_STATEMENT_TIMEOUT_MS),
        total_timeout_ms: requested.total_timeout_ms.min(SHARED_TOTAL_TIMEOUT_MS),
        ..requested.clone()
    }
}

fn bounded_request(request: &CollectionRequest) -> Result<CollectionRequest> {
    request.validate()?;
    let mut request = request.clone();
    match &request.subject {
        CollectionSubject::Shared { resource } => {
            if resource.engine != culprit_core::Engine::Mssql || resource.kind != SharedResourceKind::Instance {
                return Err(culprit_core::Error::new(
                    culprit_core::ErrorCode::Unsupported,
                    culprit_core::t!("collection.unsupported"),
                ));
            }
            request.budget = shared_budget(&request.budget);
            request.budget.validate()?;
        }
        CollectionSubject::Database => {
            request.budget = database_budget(&request.budget);
            request.budget.validate()?;
        }
        CollectionSubject::ConfiguredConnection { .. } => {}
    }
    Ok(request)
}

pub(super) fn profile_source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
    profile: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    use culprit_read::SourceProfileGuard;
    let bounded = bounded_request(request)?;
    if bounded.budget != request.budget {
        return Err(invalid_data());
    }
    let guard = SourceProfileGuard::new(request, profile)?;
    let builtin_policy = guard.builtin_policy().cloned();
    let io = ReadSession::collection(pool, request.budget.clone())?.with_profile(guard.clone());
    let mut inner = EventSource::with_io(io, request, host, port)?;
    inner.profile = Some(guard);
    inner.builtin_policy = builtin_policy;
    Ok(Box::new(inner))
}

impl EventSource {
    pub(super) fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        Self::with_io(ReadSession::collection(pool, request.budget.clone())?, &request, host, port)
    }
}
impl<R: NativeRead> EventSource<R> {
    fn with_io(io: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        if request.collection() != TelemetryCollection::DeadlockHistory {
            return Err(invalid_data());
        }
        Ok(Self {
            request: request.clone(),
            host,
            port,
            io,
            profile: None,
            builtin_policy: None,
            initial_at: None,
            description: None,
            owner: None,
            positions: None,
            records: vec![],
            offset: 0,
            progress: super::progress::PagedProgress::new(request.collection()),
        })
    }
    async fn page(&mut self) -> Result<CollectionRead> {
        if self.positions.is_none() {
            self.load().await?;
        }
        let mut end = self.offset;
        let mut bytes = 0;
        while end < self.records.len() && end - self.offset < self.request.budget.page_rows as usize {
            let size = serde_json::to_vec(&self.records[end]).map_err(|_| invalid_data())?.len() + ENVELOPE_ALLOWANCE;
            if bytes + size > self.request.budget.page_bytes as usize {
                break;
            }
            bytes += size;
            end += 1;
        }
        if end == self.offset && end < self.records.len() {
            return Ok(CollectionRead::Stopped(CollectionStop::ByteLimit));
        }
        let records = self.records[self.offset..end].to_vec();
        self.offset = end;
        Ok(CollectionRead::Page { records, next_cursor: (end < self.records.len()).then(|| end.to_string()) })
    }

    async fn read_native(&mut self) -> Result<Vec<native::Event>> {
        let description = self.description.as_ref().ok_or_else(invalid_data)?;
        let (from, until) = self.request.bounds()?;
        let limit = self.request.budget.max_snapshot_rows as usize + 1;
        let events = match self.owner.as_ref().ok_or_else(invalid_data)? {
            EventOwner::Database { session: Some(session), .. } => {
                let session = session.clone();
                let database = description.target.database().ok_or_else(invalid_data)?.database.clone();
                crate::resources::admit_resources(&mut self.io).await.map_err(ReadError::into_error)?;
                session.events(&mut self.io, &database, &from.to_rfc3339(), &until.to_rfc3339(), limit, false).await?
            }
            EventOwner::Database { session: None, .. } => vec![],
            EventOwner::Instance { target, .. } => {
                target.events(&mut self.io, &from.to_rfc3339(), &until.to_rfc3339(), limit, false).await?
            }
        };
        if events.len() > self.request.budget.max_snapshot_rows as usize {
            return Err(CollectionStop::RowLimit.error());
        }
        Ok(events)
    }
    async fn load(&mut self) -> Result<()> {
        let events = self.read_native().await?;
        self.positions = Some(events.iter().map(|event| event.position.clone()).collect());
        for event in events {
            for (ordinal, graph) in event.graphs.into_iter().enumerate() {
                let mut row = DeadlockEvent {
                    position: event.position.clone(),
                    file_id: event.file_id.clone(),
                    file_offset: event.offset.clone(),
                    graph_ordinal: ordinal as u32,
                    at: event.at.clone(),
                    processes: graph.processes,
                    resources: graph.resources,
                    pattern: graph.pattern,
                };
                row.redact(culprit_core::Engine::Mssql);
                self.records.push(TelemetryRecord::Deadlock(row));
            }
        }
        if self.records.len() > self.request.budget.max_snapshot_rows as usize {
            return Err(CollectionStop::RowLimit.error());
        }
        Ok(())
    }
}
#[async_trait]
impl<R: NativeRead> TelemetrySource for EventSource<R> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.progress.describing()?;
        if let Some(profile) = &self.profile {
            profile.ensure_active()?;
        }
        let (target, partition, initial_from, at, unavailable_error, owner) = match &self.request.subject {
            CollectionSubject::ConfiguredConnection { .. } => return Err(invalid_data()),
            CollectionSubject::Database => {
                description::ensure_supported(&mut self.io).await?;
                if let Some(policy) = &self.builtin_policy {
                    crate::builtin_safety::verify_activity(policy, 5, 12)?;
                }
                let (database, physical) = description::identity(&mut self.io, &self.host, self.port).await?;
                let server = self.io.query(native::SERVER, vec![], 1).await.map_err(ReadError::into_error)?;
                let row = server.first().ok_or_else(invalid_data)?;
                if crate::read::integer(row, "edition")? != 5 || text(row, "db")? != database.database {
                    return Err(CollectionStop::SourceChanged.error());
                }
                let at = canonical_time(&text(row, "observed_at")?)?;
                let target: CollectionTarget = database.into();
                if let Some(profile) = &self.profile {
                    profile.bind_native(
                        &NativeReadContext {
                            target: target.clone(),
                            partition: SourcePartition::Target,
                            observed_from: at.clone(),
                            observed_until: at.clone(),
                            database_writable: Some(true),
                        },
                        self.io.connection_generation(),
                    )?;
                }
                let session = crate::deadlocks::ConfiguredDatabaseSession::probe(&mut self.io).await?;
                let unavailable_error = session.is_none().then_some(culprit_core::ErrorCode::Unsupported);
                (
                    target,
                    SourcePartition::Target,
                    at.clone(),
                    at,
                    unavailable_error,
                    EventOwner::Database { physical, session },
                )
            }
            CollectionSubject::Shared { .. } => {
                let (instance, initial_at) = crate::instance::read(&mut self.io).await?;
                if !system_health::supported(&instance) {
                    return Err(culprit_core::Error::new(
                        culprit_core::ErrorCode::Unsupported,
                        culprit_core::t!("collection.unsupported"),
                    ));
                }
                if let Some(policy) = &self.builtin_policy {
                    crate::builtin_safety::verify_activity(policy, instance.engine_edition(), instance.major)?;
                }
                let resource = CollectionTarget::Shared(instance.identity.clone());
                if !self.request.subject.accepts(&resource) {
                    return Err(CollectionStop::SourceChanged.error());
                }
                let target = system_health::ConfiguredTarget::read(&mut self.io).await?;
                if self.builtin_policy.is_some() {
                    target.ensure_bounded_footprint()?;
                }
                if ObservationBounds::new(&initial_at, target.observed_at()).is_none() {
                    return Err(CollectionStop::SourceChanged.error());
                }
                let partition = SourcePartition::InstanceLocal {
                    member: Some(instance.identity.clone()),
                    epoch: Some(target.capture_epoch(&instance)?),
                };
                let at = target.observed_at().to_owned();
                if let Some(profile) = &self.profile {
                    profile.bind_native(
                        &NativeReadContext {
                            target: resource.clone(),
                            partition: partition.clone(),
                            observed_from: initial_at.clone(),
                            observed_until: at.clone(),
                            database_writable: None,
                        },
                        self.io.connection_generation(),
                    )?;
                }
                (
                    resource,
                    partition,
                    initial_at,
                    at,
                    None,
                    EventOwner::Instance { instance: Box::new(instance), target },
                )
            }
        };
        let (from, until) = self.request.bounds()?;
        let value = CollectionDescription {
            partition,
            target,
            source: culprit_core::source_id::DEADLOCKS.into(),
            observation: SourceObservation::RetainedEvents(EventHistoryDescription {
                from: from.to_rfc3339(),
                until: until.to_rfc3339(),
                observed_at: at,
                retained_from: None,
                history_complete: false,
                unavailable_error,
            }),
            continuity: SourceContinuity::Unverified,
        };
        if !value.matches_request(&self.request) {
            return Err(invalid_data());
        }
        self.initial_at = Some(initial_from);
        self.owner = Some(owner);
        self.description = Some(value.clone());
        self.progress.described();
        Ok(value)
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.read_metrics()
    }
    fn query_count(&self) -> u32 {
        self.io.queries()
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        if let Some(profile) = &self.profile {
            profile.check_connection(self.io.connection_generation())?;
        }
        self.progress.reading(stream, cursor)?;
        let result = self.page().await;
        self.progress.read(stream, result)
    }

    async fn finish(&mut self) -> Result<culprit_core::telemetry::SourceCompletion> {
        self.progress.verifying()?;
        if let Some(profile) = &self.profile {
            profile.ensure_active()?;
            profile.check_connection(self.io.connection_generation())?;
        }
        let database_owner = match self.owner.as_ref().ok_or_else(invalid_data)? {
            EventOwner::Database { physical, session } => Some((physical.clone(), session.clone())),
            EventOwner::Instance { .. } => None,
        };
        if let Some((expected, original_session)) = database_owner {
            let (database, physical) = description::identity(&mut self.io, &self.host, self.port).await?;
            if expected != physical
                || self.description.as_ref().is_none_or(|description| description.target.database() != Some(&database))
            {
                self.progress.verified();
                return Ok(SourceCompletion::Changed);
            }
            if let Some(profile) = &self.profile {
                let server = self.io.query(native::SERVER, vec![], 1).await.map_err(ReadError::into_error)?;
                let row = server.first().ok_or_else(invalid_data)?;
                if crate::read::integer(row, "edition")? != 5 || text(row, "db")? != database.database {
                    self.progress.verified();
                    return Ok(SourceCompletion::Changed);
                }
                let final_at = canonical_time(&text(row, "observed_at")?)?;
                let initial_at = self.initial_at.as_ref().ok_or_else(invalid_data)?;
                if ObservationBounds::new(initial_at, &final_at).is_none() {
                    self.progress.verified();
                    return Ok(SourceCompletion::Changed);
                }
                profile.verify_final(
                    &NativeReadContext {
                        target: database.clone().into(),
                        partition: SourcePartition::Target,
                        observed_from: initial_at.clone(),
                        observed_until: final_at,
                        database_writable: Some(true),
                    },
                    self.io.connection_generation(),
                )?;
            }
            return match original_session {
                Some(original_session) => {
                    crate::resources::admit_resources(&mut self.io).await.map_err(ReadError::into_error)?;
                    let Some(current_session) =
                        crate::deadlocks::ConfiguredDatabaseSession::probe(&mut self.io).await?
                    else {
                        self.progress.verified();
                        return Ok(SourceCompletion::Changed);
                    };
                    if current_session != original_session {
                        self.progress.verified();
                        return Ok(SourceCompletion::Changed);
                    }
                    let (from, until) = self.request.bounds()?;
                    let limit = self.request.budget.max_snapshot_rows as usize + 1;
                    let events = current_session
                        .events(&mut self.io, &database.database, &from.to_rfc3339(), &until.to_rfc3339(), limit, false)
                        .await?;
                    if events.len() > self.request.budget.max_snapshot_rows as usize {
                        return Err(CollectionStop::RowLimit.error());
                    }
                    self.progress.verified();
                    Ok(culprit_core::telemetry::SourceCompletion::verified_if(
                        self.positions.as_ref()
                            == Some(&events.into_iter().map(|event| event.position).collect::<Vec<_>>()),
                    ))
                }
                None => {
                    let current_session = crate::deadlocks::ConfiguredDatabaseSession::probe(&mut self.io).await?;
                    self.progress.verified();
                    Ok(culprit_core::telemetry::SourceCompletion::verified_if(
                        current_session.is_none() && self.positions.as_deref() == Some(&[]),
                    ))
                }
            };
        }
        let events = self.read_native().await?;
        let EventOwner::Instance { instance: original, target: original_target } =
            self.owner.as_ref().ok_or_else(invalid_data)?
        else {
            return Err(invalid_data());
        };
        let current_target = system_health::ConfiguredTarget::read(&mut self.io).await?;
        if self.builtin_policy.is_some() {
            current_target.ensure_bounded_footprint()?;
        }
        let (current, at) = crate::instance::read(&mut self.io).await?;
        let owner_matches = original.as_ref() == &current
            && original_target.confirmed_by(&current_target)
            && ObservationBounds::new(current_target.observed_at(), &at).is_some();
        if owner_matches && let Some(profile) = &self.profile {
            let initial_at = self.initial_at.as_ref().ok_or_else(invalid_data)?;
            let partition = SourcePartition::InstanceLocal {
                member: Some(current.identity.clone()),
                epoch: Some(current_target.capture_epoch(&current)?),
            };
            profile.verify_final(
                &NativeReadContext {
                    target: CollectionTarget::Shared(current.identity),
                    partition,
                    observed_from: initial_at.clone(),
                    observed_until: at,
                    database_writable: None,
                },
                self.io.connection_generation(),
            )?;
        }
        self.progress.verified();
        Ok(culprit_core::telemetry::SourceCompletion::verified_if(
            owner_matches
                && self.positions.as_ref() == Some(&events.into_iter().map(|event| event.position).collect::<Vec<_>>()),
        ))
    }
    async fn close(&mut self) -> SourceShutdown {
        self.progress.close();
        self.io.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        resources::test_support::{FakeRead, latest_row, replace},
        value::{Rec, Val},
    };
    fn request() -> CollectionRequest {
        CollectionRequest {
            subject: culprit_core::telemetry::CollectionSubject::Database,
            selection: CollectionSelection::DeadlockHistory {
                from: "2020-01-01T06:00:00Z".into(),
                until: "2020-01-01T06:01:00Z".into(),
            },
            budget: CollectionBudget { page_rows: 1, ..Default::default() },
        }
    }
    fn identity() -> Rec {
        Rec::of(&[
            ("database_name", Val::Text("db".into())),
            ("incarnation", Val::Text("logical".into())),
            ("physical_incarnation", Val::Text("physical".into())),
            ("updateability", Val::Text("READ_WRITE".into())),
        ])
    }
    fn server() -> Rec {
        Rec::of(&[
            ("edition", Val::Int(5)),
            ("db", Val::Text("db".into())),
            ("observed_at", Val::Text("2020-01-01T06:02:00Z".into())),
        ])
    }
    fn event(offset: u64) -> String {
        crate::deadlocks::database_test_support::event(offset)
    }
    fn pressure() -> Rec {
        replace(
            &latest_row("2020-01-01T06:01:45Z"),
            &[
                ("avg_instance_cpu_percent", Val::Float(10.0)),
                ("max_worker_percent", Val::Float(10.0)),
                ("avg_log_write_percent", Val::Float(10.0)),
            ],
        )
    }
    fn replies(events: Vec<String>, verified: Vec<String>) -> FakeRead {
        use crate::deadlocks::database_test_support::{configuration, target};
        FakeRead::new(vec![
            Ok(vec![server()]),
            Ok(vec![identity()]),
            Ok(vec![server()]),
            Ok(vec![configuration()]),
            Ok(vec![pressure()]),
            Ok(vec![target(&events)]),
            Ok(vec![identity()]),
            Ok(vec![pressure()]),
            Ok(vec![configuration()]),
            Ok(vec![target(&verified)]),
        ])
    }
    #[tokio::test]
    async fn bounded_event_pages_recheck_native_membership_and_never_claim_complete_retention() {
        let mut source = EventSource::with_io(
            replies(vec![event(1), event(2)], vec![event(1), event(2)]),
            &request(),
            "host".into(),
            1433,
        )
        .unwrap();
        let description = source.describe().await.unwrap();
        assert!(description.matches_request(&request()));
        assert!(!description.observation.events().unwrap().history_complete);
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::DeadlockEvents, None).await.unwrap()
        else {
            panic!("page")
        };
        assert_eq!(records.len(), 1);
        let encoded = serde_json::to_string(&records).unwrap();
        for canary in ["private-deadlock-canary", "private-host", "private-user"] {
            assert!(!encoded.contains(canary));
        }
        let row = ExportedRecord::new(
            &description,
            &CaptureIdentity { observation_id: "observation".into(), revision: None },
            records[0].clone(),
        )
        .unwrap();
        assert!(row.record_id.is_some());
        assert!(next_cursor.is_some());
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::DeadlockEvents, next_cursor.as_deref()).await.unwrap()
        else {
            panic!("page")
        };
        assert_eq!(records.len(), 1);
        assert!(next_cursor.is_none());
        assert_eq!(source.finish().await.unwrap(), culprit_core::telemetry::SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    #[tokio::test]
    async fn late_or_changed_events_prevent_complete_observation_commit() {
        let mut source =
            EventSource::with_io(replies(vec![event(1)], vec![event(1), event(2)]), &request(), "host".into(), 1433)
                .unwrap();
        source.describe().await.unwrap();
        source.read_page(TelemetryStream::DeadlockEvents, None).await.unwrap();
        assert_eq!(source.finish().await.unwrap(), culprit_core::telemetry::SourceCompletion::Changed);
    }
    #[tokio::test]
    async fn exceeding_the_native_event_cap_does_not_turn_latest_n_into_successful_history() {
        let mut request = request();
        request.budget.max_snapshot_rows = 1;
        let mut source =
            EventSource::with_io(replies(vec![event(1), event(2)], vec![]), &request, "host".into(), 1433).unwrap();
        source.describe().await.unwrap();
        let error = source.read_page(TelemetryStream::DeadlockEvents, None).await.err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::RowLimit);
        assert!(source.finish().await.is_err());
    }

    #[tokio::test]
    async fn cancelling_a_target_read_retains_the_native_owner_until_explicit_close() {
        let io = replies(vec![event(1)], vec![]).pause_on_query(6);
        let mut source = EventSource::with_io(io, &request(), "host".into(), 1433).unwrap();
        source.describe().await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                source.read_page(TelemetryStream::DeadlockEvents, None),
            )
            .await
            .is_err()
        );
        assert_eq!(source.io.calls.len(), 6);
        assert_eq!(source.io.closed, 0);
        source.io.shutdown = SourceShutdown::Unconfirmed;
        assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
        assert_eq!(source.io.closed, 1);
        assert!(source.read_page(TelemetryStream::DeadlockEvents, None).await.is_err());
        assert_eq!(source.io.calls.len(), 6);
    }

    #[tokio::test]
    async fn unconfigured_database_session_records_unavailable_coverage_under_standard_without_claiming_empty_history()
    {
        let recipe = crate::telemetry::recipes().into_iter().find(|r| r.id == "mssql.deadlock-history").unwrap();
        let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else { panic!() };
        let mut bounded = request();
        bounded.budget = recipe.budget.clone();
        let admission = SourceProfileAdmission::for_builtin(
            digest(b"azure-deadlock-route"),
            *policy,
            None,
            bounded.budget.clone(),
            || true,
        )
        .unwrap();
        let guard = culprit_read::SourceProfileGuard::new(&bounded, &admission).unwrap();
        let io = FakeRead::new(vec![
            Ok(vec![server()]),
            Ok(vec![identity()]),
            Ok(vec![server()]),
            Ok(vec![]),
            Ok(vec![identity()]),
            Ok(vec![replace(&server(), &[("observed_at", Val::Text("2020-01-01T06:02:05Z".into()))])]),
            Ok(vec![]),
        ])
        .with_generation(1);
        let mut source = EventSource::with_io(io, &bounded, "host".into(), 1433).unwrap();
        source.builtin_policy = guard.builtin_policy().cloned();
        source.profile = Some(guard);
        let description = source.describe().await.unwrap();
        assert_eq!(
            description.observation.events().unwrap().unavailable_error,
            Some(culprit_core::ErrorCode::Unsupported)
        );
        assert_eq!(
            description.observation.native_coverage_state().unwrap(),
            Some(culprit_core::SourceState::Unavailable)
        );
        assert_eq!(
            diagnostic_original_input_qualification(Some(&description)),
            DiagnosticInputQualification::Ineligible {
                reason: DiagnosticInputIneligibility::OriginalSourceUnavailable,
            }
        );
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::DeadlockEvents, None).await.unwrap()
        else {
            panic!("empty unavailable page")
        };
        assert!(records.is_empty());
        assert!(next_cursor.is_none());
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[cfg(test)]
mod shared_tests;
