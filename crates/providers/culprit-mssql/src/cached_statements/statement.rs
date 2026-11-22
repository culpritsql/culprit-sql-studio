//! Adapt the one pure UTF-16 statement slicer to the provider's private native payload.
use crate::query_store::payload::Payload;
use culprit_core::telemetry::{CachedStatementContext, DefinitionPayload};

pub(crate) fn statement_from_prefix(context: &CachedStatementContext, prefix: Option<&str>) -> DefinitionPayload {
    culprit_plans::mssql::statement_from_prefix(context, prefix)
}

pub(crate) fn native_statement_from_prefix(context: &CachedStatementContext, prefix: Option<&str>) -> Payload {
    match culprit_plans::mssql::native_statement_from_prefix(context, prefix) {
        Ok(text) => Payload::Text(text),
        Err(reason) => Payload::Unavailable(reason),
    }
}
