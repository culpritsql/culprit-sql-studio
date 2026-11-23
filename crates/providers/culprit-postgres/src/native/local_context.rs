use crate::{native, read::QueryRead, rows::Row};
use culprit_core::{ErrorCode, Result, telemetry::*};

pub(crate) struct Context {
    metadata: native::metadata::Metadata,
    member: SharedResourceIdentity,
    member_observed_at: String,
}

pub(crate) struct Completion {
    pub(crate) context: NativeReadContext,
    pub(crate) full_visibility: bool,
}

impl Context {
    pub(crate) async fn observe<R: QueryRead>(
        reader: &mut R,
        metadata: &native::metadata::Metadata,
    ) -> Result<Option<Self>> {
        let instance = match native::instance::read(reader, metadata.version).await {
            Ok(instance) => instance,
            Err(error) if optional(&error) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if ObservationBounds::new(&metadata.at, &instance.observed_at).is_none() {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(Some(Self {
            metadata: metadata.clone(),
            member: instance.identity,
            member_observed_at: instance.observed_at,
        }))
    }

    pub(crate) fn read_context(&self) -> Result<NativeReadContext> {
        self.context_until(&self.member_observed_at)
    }

    pub(crate) async fn recheck<R: QueryRead>(
        &self,
        reader: &mut R,
        host: &str,
        port: u16,
    ) -> Result<Option<NativeReadContext>> {
        Ok(self.recheck_completion(reader, host, port).await?.map(|completion| completion.context))
    }

    fn matches_metadata(&self, metadata: &native::metadata::Metadata) -> bool {
        self.metadata.same_activity_source(metadata)
            && ObservationBounds::new(&self.metadata.at, &metadata.at).is_some()
    }

    /// Complete already-observed paired metadata/member bounds. Snapshot readers retain their own native
    /// layout checks and never pay for a duplicate Activity metadata recheck.
    pub(crate) fn completed(&self, after: &Self) -> Result<Completion> {
        if !self.matches_metadata(&after.metadata) || self.member != after.member {
            return Err(CollectionStop::SourceChanged.error());
        }
        let context = self.context_until(&after.member_observed_at)?;
        if !context.completes_read(&self.read_context()?) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(Completion { context, full_visibility: self.metadata.full_visibility && after.metadata.full_visibility })
    }

    async fn recheck_completion<R: QueryRead>(
        &self,
        reader: &mut R,
        host: &str,
        port: u16,
    ) -> Result<Option<Completion>> {
        let after = match native::metadata::activity(reader, host, port).await {
            Ok(metadata) => metadata,
            Err(error) if optional(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        complete_observed(reader, &self.metadata, Some(self), &after).await
    }

    fn context_until(&self, until: &str) -> Result<NativeReadContext> {
        let context = NativeReadContext {
            target: self.metadata.database.clone().into(),
            partition: SourcePartition::InstanceLocal {
                member: Some(self.member.clone()),
                epoch: Some(self.metadata.activity_epoch()),
            },
            observed_from: canonical_time(&self.metadata.at)?,
            observed_until: canonical_time(until)?,
            database_writable: Some(self.metadata.writable_role()),
        };
        if !context.is_valid() {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(context)
    }
}

pub(crate) fn unknown(metadata: &native::metadata::Metadata, until: &str) -> Result<NativeReadContext> {
    let context = NativeReadContext {
        target: metadata.database.clone().into(),
        partition: SourcePartition::InstanceLocal { member: None, epoch: Some(metadata.activity_epoch()) },
        observed_from: canonical_time(&metadata.at)?,
        observed_until: canonical_time(until)?,
        database_writable: Some(metadata.writable_role()),
    };
    if !context.is_valid() {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(context)
}

/// Complete the same held read. A later permission grant cannot fill missing initial member authority.
pub(crate) async fn complete<R: QueryRead>(
    reader: &mut R,
    host: &str,
    port: u16,
    before: &native::metadata::Metadata,
    owner: Option<&Context>,
) -> Result<Completion> {
    if let Some(owner) = owner {
        return owner
            .recheck_completion(reader, host, port)
            .await?
            .ok_or_else(|| CollectionStop::SourceChanged.error());
    }
    let after = native::metadata::activity(reader, host, port).await?;
    complete_observed(reader, before, None, &after).await?.ok_or_else(|| CollectionStop::SourceChanged.error())
}

/// Settle already-paid paired native bounds once. Snapshot profiles and ordinary Settings reuse their
/// own initial metadata/context here; no second metadata query, role inference or member repair is performed.
pub(crate) async fn complete_observed<R: QueryRead>(
    reader: &mut R,
    before: &native::metadata::Metadata,
    owner: Option<&Context>,
    after: &native::metadata::Metadata,
) -> Result<Option<Completion>> {
    if !before.same_activity_source(after) || ObservationBounds::new(&before.at, &after.at).is_none() {
        return Ok(None);
    }
    let observed = Context::observe(reader, after).await?;
    if let Some(owner) = owner {
        return Ok(observed.as_ref().and_then(|other| owner.completed(other).ok()));
    }
    let until = match observed {
        Some(owner) => owner.read_context()?.observed_until,
        None => after.at.clone(),
    };
    Ok(Some(Completion {
        context: unknown(before, &until)?,
        full_visibility: before.full_visibility && after.full_visibility,
    }))
}

fn optional(error: &culprit_core::Error) -> bool {
    (error.detail.is_none() && matches!(error.code, ErrorCode::PermissionDenied | ErrorCode::Unsupported))
        || error.detail == Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::InvalidPage))
}

/// A borrowed transport witness owns no login, cancellation, close or retry policy.
pub(crate) struct SameConnection<'a, R> {
    reader: &'a mut R,
    generation: u64,
}
#[async_trait::async_trait]
impl<R: QueryRead> QueryRead for SameConnection<'_, R> {
    fn connection_generation(&self) -> Option<u64> {
        self.reader.connection_generation()
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        if self.reader.connection_generation() != Some(self.generation) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let rows = self.reader.query(sql, limit).await?;
        if self.reader.connection_generation() != Some(self.generation) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(rows)
    }
}
impl<'a, R> SameConnection<'a, R> {
    pub(crate) fn new(reader: &'a mut R, generation: u64) -> Self {
        Self { reader, generation }
    }
}
