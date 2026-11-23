//! One selected census population shares the native query, row decoder and before/final authority.
use super::{ActivityFrame, ActivityRecord, database_context::Context};
use culprit_core::{Error, ErrorCode, Result, telemetry::*};

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum CensusScope {
    Database,
    Instance(Box<SharedResourceIdentity>),
}
impl CensusScope {
    pub(crate) fn from_request(request: &CollectionRequest) -> Result<Self> {
        request.validate()?;
        if request.selection != CollectionSelection::Activity {
            return Err(crate::read::invalid_data());
        }
        match &request.subject {
            CollectionSubject::Database => Ok(Self::Database),
            CollectionSubject::Shared { resource }
                if resource.engine == culprit_core::Engine::Mssql && resource.kind == SharedResourceKind::Instance =>
            {
                Ok(Self::Instance(Box::new(resource.clone())))
            }
            _ => Err(unsupported()),
        }
    }
    pub(super) fn is_database(&self) -> bool {
        matches!(self, Self::Database)
    }
    pub(super) fn native_parameter(&self) -> i64 {
        if self.is_database() { super::CURRENT_DATABASE_SCOPE } else { 0 }
    }
    pub(super) fn validate_authority(&self, context: Option<&Context>, generation: Option<u64>) -> Result<()> {
        let Self::Instance(expected) = self else { return Ok(()) };
        let context = context.ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        if !crate::instance::supports_profile(context.activity.edition, context.activity.major) {
            return Err(unsupported());
        }
        if !context.is_qualified() || context.connection_generation().is_none_or(|generation| generation == 0) {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        if context.connection_generation() != generation
            || context.instance.as_ref().is_none_or(|observed| observed.identity != **expected)
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
    pub(super) fn validate_rows(&self, rows: &[ActivityRecord]) -> Result<()> {
        let instance_population = !self.is_database();
        if rows.iter().any(|row| (row.facts.scope == ActivityScopeBasis::InstanceMember) != instance_population) {
            return Err(crate::read::invalid_data());
        }
        Ok(())
    }
    pub(super) fn validate_initial_frame(&self, context: Option<&Context>, frame: &ActivityFrame) -> Result<()> {
        if self.is_database() {
            return Ok(());
        }
        let context = context.ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        if !context.matches_frame(&frame.context) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
    pub(super) fn native_context(&self, before: &Context, after: &Context) -> Result<NativeReadContext> {
        let mut context = before.completed(after)?;
        let Self::Instance(expected) = self else { return Ok(context) };
        self.validate_authority(Some(before), before.connection_generation())?;
        self.validate_authority(Some(after), after.connection_generation())?;
        let original = before.instance.as_ref().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        let completed = after.instance.as_ref().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        if original != completed {
            return Err(CollectionStop::SourceChanged.error());
        }
        context.target = CollectionTarget::Shared((**expected).clone());
        context.partition = SourcePartition::InstanceLocal {
            member: Some((**expected).clone()),
            epoch: Some(original.capture_epoch()?),
        };
        context.database_writable = None;
        if !context.has_qualified_local_owner() {
            return Err(crate::read::invalid_data());
        }
        Ok(context)
    }
}
fn unsupported() -> Error {
    Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported"))
}
