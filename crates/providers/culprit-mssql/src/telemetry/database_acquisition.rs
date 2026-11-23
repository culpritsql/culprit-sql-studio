//! One held native database owner supplies acquisition and pressure admission; each source keeps its data semantics.
use crate::{
    local_source::Context,
    read::{NativeRead, ReadError, invalid_data},
};
use culprit_core::{Error, ErrorCode, Result, telemetry::*};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Profile {
    AzureLogical,
    BoxedSql2022,
    ManagedInstance,
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct DatabaseAcquisition {
    profile: Profile,
    native: Box<Context>,
    connection_generation: Option<u64>,
}
impl DatabaseAcquisition {
    pub(super) async fn read<R: NativeRead>(reader: &mut R, host: &str, port: u16) -> Result<Self> {
        let context = Context::read(reader, host, port).await?;
        Self::observed(context, reader.connection_generation())
    }
    /// Catalogs keep their native local DB owner, not Query Store's logical population namespace.
    pub(super) async fn read_catalog<R: NativeRead>(
        reader: &mut R,
        host: &str,
        port: u16,
        edition: i64,
    ) -> Result<Self> {
        let generation = reader.connection_generation();
        let acquisition = Self::read(reader, host, port).await?;
        if acquisition.edition() != edition || acquisition.connection_generation != generation {
            return Err(CollectionStop::SourceChanged.error());
        }
        if generation.is_none_or(|generation| generation == 0) {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        Ok(acquisition)
    }
    pub(super) fn database_wide(&self) -> bool {
        self.native.database_wide
    }
    pub(super) fn database_id(&self) -> u32 {
        self.native.database_id
    }
    pub(super) fn require_database_id(&self, database_id: u32) -> Result<()> {
        if database_id != self.native.database_id {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
    fn observed(context: Context, connection_generation: Option<u64>) -> Result<Self> {
        let profile = match (context.activity.edition, context.activity.major) {
            (5, _) => Profile::AzureLogical,
            (2..=4, 16) => Profile::BoxedSql2022,
            // Managed version numbers do not establish a boxed engine's compatibility level. Optional Query Store
            // fields remain source-observed; this profile establishes acquisition authority, not native/cost approval.
            (8, _) => Profile::ManagedInstance,
            // Other boxed native versions retain their separate compatibility boundary.
            _ => return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported"))),
        };
        if !context.is_qualified() {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        let owner = context.completed(&context)?;
        if !owner.has_qualified_local_owner() || owner.database_writable.is_none() {
            return Err(invalid_data());
        }
        Ok(Self { profile, native: Box::new(context), connection_generation })
    }
    pub(super) fn owner(&self) -> Result<NativeReadContext> {
        self.native.completed(&self.native)
    }
    pub(super) fn target(&self) -> CollectionTarget {
        self.native.activity.database.clone().into()
    }
    pub(super) fn edition(&self) -> i64 {
        self.native.activity.edition
    }
    pub(super) async fn admit<R: NativeRead>(&self, reader: &mut R) -> Result<()> {
        if self.connection_generation != reader.connection_generation() {
            return Err(CollectionStop::SourceChanged.error());
        }
        match self.profile {
            Profile::AzureLogical => crate::resources::admit_resources(reader).await,
            Profile::BoxedSql2022 | Profile::ManagedInstance => {
                crate::resources::admit_instance_resources(reader).await
            }
        }
        .map_err(ReadError::into_error)
    }
    pub(super) async fn recheck<R: NativeRead>(&self, reader: &mut R, host: &str, port: u16) -> Result<Self> {
        if self.connection_generation != reader.connection_generation() {
            return Err(CollectionStop::SourceChanged.error());
        }
        let after = self.native.recheck(reader, host, port).await?;
        let value = Self::observed(after, reader.connection_generation())?;
        if self.profile != value.profile
            || self.connection_generation != value.connection_generation
            || !self.native.same_source(&value.native)
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(value)
    }
    pub(super) fn completed(&self, after: &Self) -> Result<NativeReadContext> {
        let original = self.owner()?;
        if self.profile != after.profile
            || self.connection_generation != after.connection_generation
            || !after.owner()?.matches_earlier_scope(&original)
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        let completed = self.native.completed(&after.native)?;
        if !completed.completes_read(&original) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(completed)
    }
}

#[cfg(test)]
mod tests;
