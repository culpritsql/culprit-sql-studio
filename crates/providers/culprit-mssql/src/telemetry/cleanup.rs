//! Local cleanup facts use the existing database readers and one held native owner. Paging and close
//! remain in FrozenSource; independently unavailable families retain their original coverage.
use super::{point, state};
use crate::{
    conn::Pool,
    local_source::Context,
    read::{NativeRead, ReadSession, invalid_data},
};
use culprit_core::{Result, telemetry::*};
use std::sync::Arc;

const FAMILIES: [point::Family; 3] =
    [point::Family::CleanupHorizon, point::Family::VersionStore, point::Family::PersistentVersions];
const MAX_RECORDS: u32 = FAMILIES.len() as u32;
// Before/final authority use platform + process + database on this same connection. The family work
// is horizon + tempdb version-store + PVS layout/ADR/optional stats, followed by one native clock.
const AUTHORITY_QUERIES: u32 = 3;
const MAX_FAMILY_QUERIES: u32 = 1 + 1 + 3;
const SNAPSHOT_CLOCK_QUERIES: u32 = 1;
const MAX_READER_QUERIES: u32 = 2 * AUTHORITY_QUERIES + MAX_FAMILY_QUERIES + SNAPSHOT_CLOCK_QUERIES;
pub(super) const MAX_QUERIES: u32 = MAX_READER_QUERIES + ReadSession::SESSION_SETUP_QUERIES;
const MAX_PAGE_BYTES: u32 = 32 * 1024;

pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        max_snapshot_rows: requested.max_snapshot_rows.min(MAX_RECORDS),
        page_rows: requested.page_rows.min(MAX_RECORDS),
        page_bytes: requested.page_bytes.min(MAX_PAGE_BYTES),
        total_bytes: requested.total_bytes.min(MAX_PAGE_BYTES),
        max_pages: requested.max_pages.min(MAX_RECORDS),
        max_queries: requested.max_queries.min(MAX_QUERIES),
        statement_timeout_ms: requested.statement_timeout_ms.min(1_000),
        total_timeout_ms: requested.total_timeout_ms.min(15_000),
        ..requested.clone()
    }
}

pub(super) struct CleanupSource<R: NativeRead = ReadSession> {
    inner: culprit_read::FrozenSource<CleanupReader<R>>,
}
struct CleanupReader<R: NativeRead> {
    io: R,
    host: String,
    port: u16,
    context: Option<Context>,
    profile: Option<culprit_read::SourceProfileGuard>,
    builtin_policy: Option<BuiltinRecipeSafety>,
}
impl CleanupSource {
    pub fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        Self::with_io(ReadSession::collection(pool, bounded_budget(&request.budget))?, request, host, port)
    }
}
pub(super) fn profile_source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    request.subject.require_database()?;
    if request.selection != (CollectionSelection::Snapshot { dataset: SnapshotDataset::Cleanup }) {
        return Err(invalid_data());
    }
    if bounded_budget(&request.budget) != request.budget {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    let guard = culprit_read::SourceProfileGuard::new(request, admission)?;
    let io = ReadSession::collection(pool, request.budget.clone())?.with_profile(guard.clone());
    let inner =
        CleanupSource::with_io_and_policy(io, request, host, port, Some(guard), admission.builtin_policy().cloned())?;
    Ok(Box::new(inner))
}
impl<R: NativeRead> CleanupSource<R> {
    fn with_io(io: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        Self::with_io_and_policy(io, request, host, port, None, None)
    }
    fn with_io_and_policy(
        io: R,
        request: &CollectionRequest,
        host: String,
        port: u16,
        profile: Option<culprit_read::SourceProfileGuard>,
        builtin_policy: Option<BuiltinRecipeSafety>,
    ) -> Result<Self> {
        request.validate()?;
        request.subject.require_database()?;
        if request.selection != (CollectionSelection::Snapshot { dataset: SnapshotDataset::Cleanup }) {
            return Err(invalid_data());
        }
        let mut request = request.clone();
        request.budget = bounded_budget(&request.budget);
        request.budget.validate()?;
        let reader = CleanupReader { io, host, port, context: None, profile, builtin_policy };
        Ok(Self { inner: culprit_read::FrozenSource::new(reader, &request)? })
    }
}

#[async_trait::async_trait]
impl<R: NativeRead> culprit_read::ObservationReader for CleanupReader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<culprit_read::FrozenObservation> {
        if let Some(profile) = &self.profile {
            profile.ensure_active()?;
        }
        let context = Context::read(&mut self.io, &self.host, self.port).await?;
        require_cleanup_owner(&context)?;
        if let Some(policy) = &self.builtin_policy {
            crate::builtin_safety::verify_activity(policy, context.activity.edition, context.activity.major)?;
        }
        if let Some(profile) = &self.profile {
            profile
                .bind_native(&cleanup_read_context(&context, &context.activity.at)?, context.connection_generation())?;
        }
        let mut records = Vec::new();
        let mut sources = Vec::new();
        for family in FAMILIES {
            if let Some(profile) = &self.profile {
                profile.check_connection(self.io.connection_generation())?;
            }
            let remaining = (request.budget.max_snapshot_rows as usize).saturating_sub(records.len());
            if remaining == 0 {
                return Err(CollectionStop::RowLimit.error());
            }
            let result = family.read(&mut self.io, remaining, None, None).await;
            let mut coverage = state::record_family(family, result, remaining, &mut records)?;
            if matches!(family, point::Family::CleanupHorizon)
                && !context.database_wide
                && matches!(coverage.state, culprit_core::SourceState::Collected | culprit_core::SourceState::Empty)
            {
                coverage.state = culprit_core::SourceState::Partial;
                coverage.complete_scope = false;
            }
            sources.push(coverage);
        }
        let (until, _) = state::clock(&mut self.io).await?;
        if ObservationBounds::new(&context.activity.at, &until).is_none() {
            return Err(CollectionStop::SourceChanged.error());
        }
        let snapshot = StateSnapshotDescription {
            dataset: SnapshotDataset::Cleanup,
            observed_from: context.activity.at.clone(),
            observed_until: until,
            records: records.len().try_into().map_err(|_| invalid_data())?,
            sources,
        };
        let (partition, observation) = if context.is_qualified() {
            (
                context.source_partition()?,
                SourceObservation::LocalResource {
                    snapshot,
                    database_writable: context.activity.writable.ok_or_else(invalid_data)?,
                },
            )
        } else {
            (SourcePartition::Target, SourceObservation::Snapshot(snapshot))
        };
        let description = CollectionDescription {
            target: context.activity.database.clone().into(),
            partition,
            source: "mssql-cleanup".into(),
            continuity: SourceContinuity::Unverified,
            observation,
        };
        self.context = Some(context);
        Ok(culprit_read::FrozenObservation { description, records })
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        if let Some(profile) = &self.profile {
            profile.ensure_active()?;
            profile.check_connection(self.io.connection_generation())?;
        }
        let original = self.context.as_ref().ok_or_else(invalid_data)?;
        let after = original.recheck(&mut self.io, &self.host, self.port).await?;
        if require_cleanup_owner(&after).is_err() {
            return Ok(false);
        }
        let until = &description.observation.snapshot().ok_or_else(invalid_data)?.observed_until;
        let same_owner = if original.is_qualified() {
            original.same_source(&after) && original.completed(&after).is_ok()
        } else {
            !after.is_qualified() && original.same_observed_source(&after)
        };
        if original.database_wide != after.database_wide
            || !same_owner
            || ObservationBounds::new(until, &after.activity.at).is_none()
        {
            return Ok(false);
        }
        if let Some(profile) = &self.profile {
            profile
                .verify_final(&cleanup_read_context(original, &after.activity.at)?, after.connection_generation())?;
        }
        Ok(true)
    }
    fn query_count(&self) -> u32 {
        self.io.queries()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.io.close().await
    }
}

fn cleanup_read_context(before: &Context, observed_until: &str) -> Result<NativeReadContext> {
    let partition = if before.is_qualified() { before.source_partition()? } else { SourcePartition::Target };
    let context = NativeReadContext {
        target: before.activity.database.clone().into(),
        partition,
        observed_from: before.activity.at.clone(),
        observed_until: observed_until.into(),
        database_writable: before.activity.writable,
    };
    context.is_valid().then_some(context).ok_or_else(invalid_data)
}

fn require_cleanup_owner(context: &Context) -> Result<()> {
    if context.connection_generation().is_none_or(|generation| generation == 0) {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    if context.activity.edition == crate::probe::ENGINE_EDITION_AZURE_SQL_DATABASE {
        if context.is_qualified() {
            return Ok(());
        }
        if context.activity.writable == Some(false) {
            return Err(culprit_core::Error::new(
                culprit_core::ErrorCode::Unsupported,
                culprit_core::t!("collection.unsupported"),
            ));
        }
        if context.qualification_error.is_none()
            && context.activity.writable == Some(true)
            && context.activity.database.incarnation.is_some()
            && context.physical_database.is_some()
        {
            return Ok(());
        }
        return Err(CollectionStop::SourceUnavailable.error());
    }
    if !matches!(context.source_instance(), Some(member) if member.kind == SharedResourceKind::Instance) {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    if !context.is_qualified() {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    Ok(())
}

#[async_trait::async_trait]
impl<R: NativeRead> TelemetrySource for CleanupSource<R> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.inner.describe().await
    }
    fn query_count(&self) -> u32 {
        self.inner.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.inner.read_metrics()
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        self.inner.read_page(stream, cursor).await
    }
    async fn finish(&mut self) -> Result<SourceCompletion> {
        self.inner.finish().await
    }
    async fn close(&mut self) -> SourceShutdown {
        self.inner.close().await
    }
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
