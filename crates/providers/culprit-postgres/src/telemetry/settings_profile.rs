//! A finite guarded route over the existing native Settings query and shared snapshot lifecycle.
use super::*;

const MAX_SETTINGS_QUERIES: u32 = 6; // SET1 +metadata/member2 +allowlist1 +metadata/member2.
const MAX_SETTINGS_STATEMENT_MS: u32 = 1_000;
const MAX_SETTINGS_DURATION_MS: u32 = 5_000;

pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    let rows = native::settings::capture_rows();
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(rows);
    budget.page_rows = budget.page_rows.min(rows);
    budget.max_queries = budget.max_queries.min(MAX_SETTINGS_QUERIES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(MAX_SETTINGS_STATEMENT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(MAX_SETTINGS_DURATION_MS);
    budget
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
