//! Guarded ordinary configuration uses the existing SHOW/global context/pager owners.
use super::*;
const MAX_SETTINGS_ROWS: u32 = 23; // The one Core native global-variable inventory, not arbitrary server variables.
const MAX_SETTINGS_QUERIES: u32 = 18; // Setup7 + initial native context5 + SHOW1 + final native context5.
const MAX_PAGE_BYTES: u32 = 64 * 1024;
const MAX_TOTAL_BYTES: u32 = 256 * 1024;
const MAX_STATEMENT_MS: u32 = 1000;
const MAX_SOURCE_MS: u32 = 5000;
pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_SETTINGS_ROWS);
    budget.page_rows = budget.page_rows.min(MAX_SETTINGS_ROWS).min(budget.max_snapshot_rows);
    budget.page_bytes = budget.page_bytes.min(MAX_PAGE_BYTES);
    budget.total_bytes = budget.total_bytes.min(MAX_TOTAL_BYTES);
    budget.page_bytes = budget.page_bytes.min(budget.total_bytes);
    budget.max_sql_bytes = budget.max_sql_bytes.min(budget.total_bytes);
    budget.max_plan_bytes = budget.max_plan_bytes.min(budget.total_bytes);
    budget.max_queries = budget.max_queries.min(MAX_SETTINGS_QUERIES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(MAX_STATEMENT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(MAX_SOURCE_MS);
    budget
}
pub(super) fn source(
    connector: Arc<Connector>,
    request: &CollectionRequest,
    admission: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    if request.selection != (CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings })
        || bounded_budget(&request.budget) != request.budget
    {
        return Err(native::invalid());
    }
    let guard = culprit_read::SourceProfileGuard::new(request, admission)?;
    snapshot_source(Arc::new(connector.with_profile(guard)), request, Recipe::Settings)
}
pub(super) fn verify_metadata(metadata: &Metadata, profile: Option<&culprit_read::SourceProfileGuard>) -> Result<()> {
    if !crate::builtin_safety::supported(metadata.connection.flavor, &metadata.connection.version) {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    if let Some(policy) = profile.and_then(culprit_read::SourceProfileGuard::builtin_policy) {
        // Preserve the current writable-only normal policy; a future role needs actual native acceptance.
        crate::builtin_safety::verify_metadata(policy, metadata)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_tests;
